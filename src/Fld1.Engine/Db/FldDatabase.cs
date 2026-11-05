using Fld1.Core.Hlc;
using Fld1.Core.Hash;
using Fld1.Core.Uuid;
using Fld1.Engine.Storage;
using Fld1.Engine.Outbox;
using Fld1.Engine.Sync;
using Fld1.Manifest.Format;
using Fld1.Manifest.IO;
using Fld1.Run.Builder;
using Fld1.Run.Reader;
using Fld1.Wal.Codec;
using Fld1.Wal.Format;
using Fld1.Wal.IO;
using Fld1.Wal.Recovery;

namespace Fld1.Engine.Db;

/// <summary>
/// Single-process FLD database.
/// Put → mem overlay; Commit → WAL; Checkpoint → run + clear sealed prefix;
/// Open recovers WAL records after last sealed SeqHi into overlay.
/// </summary>
public sealed class FldDatabase : IDisposable
{
    private readonly string _dbDir;
    private readonly uint _namespaceId;
    private readonly byte[] _localAuthor;
    private readonly HlcClock _clock;
    private readonly MemOverlay _overlay = new();
    private readonly HashSet<MemFieldKey> _dirty = new();
    private readonly AtomicManifestStore _manifestStore;
    private readonly object _commitLock = new();
    private WalManager _wal;
    private OutboxLog _outbox;
    private readonly List<(ulong runId, string path, byte[]? cached)> _runs = new();
    private ulong _nextRunId = 1;
    private ulong _clientRequestSeq;
    private readonly byte[] _clientInstanceId = new byte[16];
    private ulong _lastSealedCommitSeq;
    private ulong _pendingFlushedCommitSeq; // deferred manifest watermark
    private bool _manifestDirty;
    private readonly int _autoCheckpointOverlayFields;
    private readonly bool _truncateSealedWal;
    private readonly int _autoCompactRunCount;
    private bool _disposed;
    private bool _faulted;

    public string DbDir => _dbDir;
    public uint NamespaceId => _namespaceId;
    public int OverlayCount => _overlay.Count;
    public ulong LastSealedCommitSeq => _lastSealedCommitSeq;
    public ulong PendingFlushedCommitSeq => _pendingFlushedCommitSeq;
    public ReadOnlySpan<byte> LocalAuthor => _localAuthor;

    private FldDatabase(
        string dbDir,
        uint namespaceId,
        byte[] localAuthor,
        WalManager wal,
        OutboxLog outbox,
        AtomicManifestStore manifestStore,
        int autoCheckpointOverlayFields = 0,
        bool truncateSealedWal = true,
        int autoCompactRunCount = 0)
    {
        _dbDir = dbDir;
        _namespaceId = namespaceId;
        _localAuthor = localAuthor;
        _clock = new HlcClock(localAuthor);
        _wal = wal;
        _outbox = outbox;
        _manifestStore = manifestStore;
        _autoCheckpointOverlayFields = autoCheckpointOverlayFields;
        _truncateSealedWal = truncateSealedWal;
        _autoCompactRunCount = autoCompactRunCount;
        Uuid7.NewUuid7(_clientInstanceId);
    }

    public static FldDatabase Open(string dbDir, uint namespaceId = 0, byte[]? localAuthor = null)
        => Open(new FldOpenOptions { Path = dbDir, NamespaceId = namespaceId, LocalAuthor = localAuthor });

    public static FldDatabase Open(FldOpenOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        string dbDir = options.Path;
        uint namespaceId = options.NamespaceId;
        byte[]? localAuthor = options.LocalAuthor;
        if (namespaceId > 0xFFFF) throw new ArgumentOutOfRangeException(nameof(options.NamespaceId));
        if (options.AutoCheckpointOverlayFields < 0)
            throw new ArgumentOutOfRangeException(nameof(options.AutoCheckpointOverlayFields));
        Directory.CreateDirectory(dbDir);
        Directory.CreateDirectory(Path.Combine(dbDir, "wal"));
        Directory.CreateDirectory(Path.Combine(dbDir, "runs"));
        Directory.CreateDirectory(Path.Combine(dbDir, "tmp"));

        var author = localAuthor ?? new byte[16];
        if (localAuthor is null) Uuid7.NewUuid7(author);
        if (author.Length != 16) throw new ArgumentException("author must be 16 bytes");

        var manifestStore = new AtomicManifestStore(dbDir);
        var manifest = manifestStore.Load();

        if (!manifest.Namespaces.Contains(namespaceId))
        {
            manifest = manifestStore.Update(m => CloneManifest(m, nss: m.Namespaces
                .Append(namespaceId).Distinct().OrderBy(x => x).ToList()));
        }

        ulong checkpointedThroughCommitSeq = manifest.Runs
            .Where(run => run.Ns == namespaceId)
            .Select(run => run.SeqHi)
            .DefaultIfEmpty(0UL)
            .Max();

        string walDir = Path.Combine(dbDir, "wal");
        var (wal, walRecords) = WalManager.Open(walDir, namespaceId, options.WalRotateAfterBytes);
        OutboxLog? outbox = null;
        try
        {
            outbox = OutboxLog.Open(
                Path.Combine(dbDir, "outbox"), namespaceId, checkpointedThroughCommitSeq);

            var db = new FldDatabase(
                dbDir, namespaceId, author, wal, outbox, manifestStore,
                options.AutoCheckpointOverlayFields,
                options.TruncateSealedWal,
                options.AutoCompactRunCount);

            db.LoadRunsFromManifest(manifest);
            db.GcOrphanRuns();
            db.ReplayWalIntoOverlay(walRecords);
            return db;
        }
        catch
        {
            // Open must not leak file handles when Manifest/Run/Outbox recovery fails.
            try { outbox?.Dispose(); } catch { /* preserve original open error */ }
            try { wal.Dispose(); } catch { /* preserve original open error */ }
            throw;
        }
    }

    private void LoadRunsFromManifest(ManifestDocument manifest)
    {
        ulong sealedSeq = 0;
        foreach (var entry in manifest.Runs)
        {
            if (entry.RunId >= _nextRunId)
                _nextRunId = checked(entry.RunId + 1);
        }

        foreach (var re in manifest.Runs.Where(r => r.Ns == _namespaceId).OrderBy(r => r.RunId))
        {
            var path = Path.Combine(_dbDir, "runs", $"{re.RunId}.fld1");
            if (!File.Exists(path))
                throw new InvalidDataException(
                    $"Manifest references missing Run {re.RunId} for namespace {_namespaceId}.");

            _runs.Add((re.RunId, path, null));
            if (re.SeqHi > sealedSeq) sealedSeq = re.SeqHi;
        }
        _lastSealedCommitSeq = sealedSeq;
    }

    /// <summary>
    /// Recover the storage overlay and idempotently finish Outbox materialization.
    /// The Outbox watermark is independent of Run sealing and prevents ACKed entries
    /// removed by CompactOutbox from being resurrected during WAL replay.
    /// </summary>
    private void ReplayWalIntoOverlay(List<WalRecoveredRecord> records)
    {
        bool needsOutboxFlush = false;
        ulong materializedThrough = _outbox.MaterializedThroughCommitSeq;

        foreach (var record in records)
        {
            if (record.Kind != WalConstants.KindGroupCommit)
                continue;

            var payload = GroupCommitCodec.Decode(record.Payload);
            foreach (var mutation in payload.Mutations)
            {
                if (mutation.Ns != _namespaceId)
                    continue;

                foreach (var field in mutation.Fields)
                {
                    byte[] changeId = GetDurableChangeId(_namespaceId, record.CommitSeq, field);
                    if (record.CommitSeq > materializedThrough)
                    {
                        needsOutboxFlush = true;
                        _outbox.AppendIfMissing(new OutboxEntry
                        {
                            OutboxSeq = 0,
                            CommitSeq = record.CommitSeq,
                            ChangeId = changeId,
                            RecordId = field.RecordId,
                            FieldId = field.FieldId,
                            OpKind = field.OpKind,
                            AuthorNodeId = field.AuthorNodeId,
                            Wall = field.Wall,
                            Logical = field.Logical,
                            KeyEpoch = field.KeyEpoch,
                            Value = field.OpKind == 2 ? Array.Empty<byte>() : field.Value
                        });
                    }

                    if (record.CommitSeq <= _lastSealedCommitSeq)
                        continue;

                    _overlay.Put(new MemFieldKey(field.RecordId, field.FieldId), new MemFieldValue
                    {
                        Value = field.OpKind == 2 ? Array.Empty<byte>() : field.Value,
                        Wall = field.Wall,
                        Logical = field.Logical,
                        AuthorNodeId = field.AuthorNodeId,
                        KeyEpoch = field.KeyEpoch,
                        IsTombstone = field.OpKind == 2,
                        ChangeId = changeId
                    });
                    _pendingFlushedCommitSeq = Math.Max(_pendingFlushedCommitSeq, record.CommitSeq);
                    _manifestDirty = true;
                }
            }
        }

        if (needsOutboxFlush)
            _outbox.Flush();
    }

    private static byte[] GetDurableChangeId(uint namespaceId, ulong commitSeq, WalFieldMutation field)
    {
        if (field.ChangeId is { Length: 16 })
        {
            bool allZero = true;
            for (int i = 0; i < 16; i++)
                allZero &= field.ChangeId[i] == 0;
            if (!allZero)
                return field.ChangeId;
        }

        // Legacy WAL body v1 did not carry ChangeId. Derive a deterministic identity
        // from the stable WAL field mutation so every replay produces the same ID.
        Span<byte> input = stackalloc byte[4 + 8 + 16 + 2 + 1 + 2 + 8 + 4 + 16];
        int offset = 0;
        System.Buffers.Binary.BinaryPrimitives.WriteUInt32LittleEndian(input.Slice(offset, 4), namespaceId); offset += 4;
        System.Buffers.Binary.BinaryPrimitives.WriteUInt64LittleEndian(input.Slice(offset, 8), commitSeq); offset += 8;
        field.RecordId.CopyTo(input.Slice(offset, 16)); offset += 16;
        System.Buffers.Binary.BinaryPrimitives.WriteUInt16LittleEndian(input.Slice(offset, 2), field.FieldId); offset += 2;
        input[offset++] = field.OpKind;
        System.Buffers.Binary.BinaryPrimitives.WriteUInt16LittleEndian(input.Slice(offset, 2), field.KeyEpoch); offset += 2;
        System.Buffers.Binary.BinaryPrimitives.WriteUInt64LittleEndian(input.Slice(offset, 8), field.Wall); offset += 8;
        System.Buffers.Binary.BinaryPrimitives.WriteUInt32LittleEndian(input.Slice(offset, 4), field.Logical); offset += 4;
        field.AuthorNodeId.CopyTo(input.Slice(offset, 16));

        Span<byte> digest = stackalloc byte[32];
        Hashes.Blake3_256Tagged("FLD1/OUTBOX-CHANGE-ID/v1"u8, input, digest);
        return digest.Slice(0, 16).ToArray();
    }

    private static byte[] NewChangeId()
    {
        var changeId = new byte[16];
        Uuid7.NewUuid7(changeId);
        return changeId;
    }

    public void Put(
        ReadOnlySpan<byte> recordId,
        ushort fieldId,
        ReadOnlySpan<byte> value,
        ushort keyEpoch = 0,
        bool isTombstone = false)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        EnsureHealthy();
        if (recordId.Length != 16) throw new ArgumentException("RecordId");

        var hlc = _clock.Now();
        var key = new MemFieldKey(recordId, fieldId);
        var valueState = new MemFieldValue
        {
            Value = isTombstone ? Array.Empty<byte>() : value.ToArray(),
            Wall = hlc.Stamp.Wall,
            Logical = hlc.Stamp.Logical,
            AuthorNodeId = _localAuthor,
            KeyEpoch = keyEpoch,
            IsTombstone = isTombstone,
            ChangeId = NewChangeId()
        };

        lock (_commitLock)
        {
            EnsureHealthy();
            _overlay.Put(key, valueState);
            _dirty.Add(key);
        }
    }

    /// <summary>Put taking ownership of value without copying the payload.</summary>
    public void PutOwned(ReadOnlySpan<byte> recordId, ushort fieldId, byte[] value, ushort keyEpoch = 0)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        EnsureHealthy();
        if (recordId.Length != 16) throw new ArgumentException("RecordId");
        ArgumentNullException.ThrowIfNull(value);

        var hlc = _clock.Now();
        var key = new MemFieldKey(recordId, fieldId);
        var valueState = new MemFieldValue
        {
            Value = value,
            Wall = hlc.Stamp.Wall,
            Logical = hlc.Stamp.Logical,
            AuthorNodeId = _localAuthor,
            KeyEpoch = keyEpoch,
            IsTombstone = false,
            ChangeId = NewChangeId()
        };

        lock (_commitLock)
        {
            EnsureHealthy();
            _overlay.Put(key, valueState);
            _dirty.Add(key);
        }
    }

    /// <summary>Tombstone one field. The delete is durable after Commit.</summary>
    public void Delete(ReadOnlySpan<byte> recordId, ushort fieldId, ushort keyEpoch = 0)
        => Put(recordId, fieldId, ReadOnlySpan<byte>.Empty, keyEpoch, isTombstone: true);

    /// <summary>Apply batch into overlay (not durable until Commit).</summary>
    public void Apply(FldWriteBatch batch)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentNullException.ThrowIfNull(batch);

        // The batch is one overlay transaction with respect to Commit/Checkpoint.
        // Monitor is re-entrant, so PutOwned/Delete can retain their own safety checks.
        lock (_commitLock)
        {
            EnsureHealthy();
            foreach (var entry in batch.Entries)
            {
                if (entry.Tombstone)
                    Delete(entry.RecordId, entry.FieldId);
                else
                    PutOwned(entry.RecordId, entry.FieldId, entry.Value);
            }
        }
    }

    /// <summary>Allocate a new UUIDv7 record id into <paramref name="dest"/> (16 bytes).</summary>
    public static void NewRecordId(Span<byte> dest)
    {
        if (dest.Length != 16) throw new ArgumentException("need 16 bytes");
        Uuid7.NewUuid7(dest);
    }

    public static byte[] NewRecordId()
    {
        var id = new byte[16];
        Uuid7.NewUuid7(id);
        return id;
    }

    /// <summary>Durable group commit of current overlay into WAL. Overlay kept for reads.</summary>
    public ulong Commit()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        lock (_commitLock)
        {
            EnsureHealthy();
            if (_dirty.Count == 0) return 0;

            // Only keys written since last Commit (delta)
            var snap = new List<KeyValuePair<MemFieldKey, MemFieldValue>>(_dirty.Count);
            foreach (var key in _dirty)
            {
                if (_overlay.TryGet(key, out var v) && v is not null)
                    snap.Add(new KeyValuePair<MemFieldKey, MemFieldValue>(key, v));
            }
            if (snap.Count == 0) { _dirty.Clear(); return 0; }

            _clientRequestSeq++;
            var fields = new List<WalFieldMutation>(snap.Count);
            Span<byte> recBuf = stackalloc byte[16];
            foreach (var kv in snap)
            {
                kv.Key.WriteRecordId(recBuf);
                fields.Add(new WalFieldMutation
                {
                    RecordId = recBuf.ToArray(),
                    FieldId = kv.Key.FieldId,
                    OpKind = kv.Value.IsTombstone ? (byte)2 : (byte)1,
                    KeyEpoch = kv.Value.KeyEpoch,
                    Wall = kv.Value.Wall,
                    Logical = kv.Value.Logical,
                    AuthorNodeId = kv.Value.AuthorNodeId,
                    ChangeId = kv.Value.ChangeId,
                    Value = kv.Value.IsTombstone ? Array.Empty<byte>() : kv.Value.Value
                });
            }

            var payload = GroupCommitCodec.Encode(new GroupCommitPayload
            {
                Mutations =
                [
                    new WalMutation
                    {
                        ClientInstanceId = _clientInstanceId,
                        RequestSeq = _clientRequestSeq,
                        Ns = (ushort)_namespaceId,
                        Fields = fields
                    }
                ],
                Results =
                [
                    new WalResult
                    {
                        ClientInstanceId = _clientInstanceId,
                        RequestSeq = _clientRequestSeq,
                        Status = WalConstants.StatusCommitted,
                        ErrorCode = 0,
                        CommitSeq = 0,
                        ProvenanceIdx = 0xFFFFFFFF
                    }
                ],
                Provenance = []
            });

            ulong commitSeq;
            try
            {
                commitSeq = _wal.Append(WalConstants.KindGroupCommit, payload);
                _wal.Flush();

                Span<byte> recordId = stackalloc byte[16];
                foreach (var item in snap)
                {
                    item.Key.WriteRecordId(recordId);
                    _outbox.AppendIfMissing(new OutboxEntry
                    {
                        OutboxSeq = 0,
                        CommitSeq = commitSeq,
                        ChangeId = item.Value.ChangeId,
                        RecordId = recordId.ToArray(),
                        FieldId = item.Key.FieldId,
                        OpKind = item.Value.IsTombstone ? (byte)2 : (byte)1,
                        AuthorNodeId = item.Value.AuthorNodeId,
                        Wall = item.Value.Wall,
                        Logical = item.Value.Logical,
                        KeyEpoch = item.Value.KeyEpoch,
                        Value = item.Value.IsTombstone ? Array.Empty<byte>() : item.Value.Value
                    });
                }

                _outbox.Flush();
            }
            catch
            {
                // WAL may already be durable. Do not append another logical commit in
                // this process; startup replay will finish the idempotent Outbox append.
                _faulted = true;
                throw;
            }

            _dirty.Clear();

            // Defer manifest disk write — WAL is source of truth until Checkpoint/FlushManifest
            _pendingFlushedCommitSeq = commitSeq;
            _manifestDirty = true;
            bool autoCkpt = _autoCheckpointOverlayFields > 0
                            && _overlay.Count >= _autoCheckpointOverlayFields;
            if (!autoCkpt)
                return commitSeq;
            CheckpointUnderLock();
            return commitSeq;
        }
    }

    public ulong Checkpoint()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        lock (_commitLock)
            return CheckpointUnderLock();
    }

    private ulong CheckpointUnderLock()
    {
            EnsureHealthy();
            if (_dirty.Count != 0)
                throw new InvalidOperationException("Checkpoint requires all overlay writes to be committed first.");

            var snap = _overlay.Snapshot();
            if (snap.Count == 0) return 0;

            var builder = new RunBuilder(_namespaceId);
            Span<byte> rec = stackalloc byte[16];
            foreach (var kv in snap)
            {
                kv.Key.WriteRecordId(rec);
                builder.Add(
                    rec, kv.Key.FieldId,
                    kv.Value.IsTombstone ? ReadOnlySpan<byte>.Empty : kv.Value.Value,
                    kv.Value.Wall, kv.Value.Logical, kv.Value.AuthorNodeId,
                    kv.Value.KeyEpoch, kv.Value.IsTombstone);
            }

            byte[] runBytes = builder.Build();
            ulong runId = _nextRunId++;
            string path = Path.Combine(_dbDir, "runs", $"{runId}.fld1");
            var builtReader = new RunReader(runBytes);
            if (!builtReader.TryValidate(out var builtVerify) || builtReader.NamespaceId != _namespaceId)
                throw new InvalidDataException(
                    $"Checkpoint produced invalid Run: {builtVerify.Status} {builtVerify.Detail}");
            WriteRunAtomically(path, runBytes);

            var recs = new List<byte[]>(snap.Count);
            foreach (var kv in snap)
            {
                var b = new byte[16];
                kv.Key.WriteRecordId(b);
                recs.Add(b);
            }
            recs.Sort((x, y) => Fld1.Core.Util.ByteUtil.CompareUnsigned16(x, y));

            ulong seqHi = _pendingFlushedCommitSeq;
            if (seqHi == 0)
                seqHi = _manifestStore.Load().LastFlushedCommitSeq;
            if (seqHi == 0) seqHi = _lastSealedCommitSeq;

            _manifestStore.Update(m =>
            {
                var runs = m.Runs.ToList();
                runs.Add(new RunEntry
                {
                    RunId = runId,
                    Level = 0,
                    Ns = _namespaceId,
                    MinRec = recs[0],
                    MaxRec = recs[^1],
                    SeqLo = _lastSealedCommitSeq + 1,
                    SeqHi = seqHi
                });
                return CloneManifest(m, runs: runs, lastFlushed: seqHi);
            });

            try
            {
                _outbox.MarkMaterialized(seqHi);
            }
            catch
            {
                _faulted = true;
                throw;
            }

            _manifestDirty = false;
            _pendingFlushedCommitSeq = seqHi;

            _runs.Add((runId, path, runBytes));
            _lastSealedCommitSeq = seqHi;
            _overlay.Clear();
            _dirty.Clear();
            if (_truncateSealedWal)
                _wal.TruncateSealedSegments(seqHi);
            if (_autoCompactRunCount > 0 && _runs.Count >= _autoCompactRunCount)
                CompactUnderLock();
            return runId;
    }

    /// <summary>
    /// Point get with full LWW across overlay + all runs (not "first hit in newest run").
    /// </summary>
    public bool TryGet(ReadOnlySpan<byte> recordId, ushort fieldId, out ReadOnlySpan<byte> value)
    {
        value = default;
        if (!TryResolve(recordId, fieldId, out var r) || r.IsTombstone || r.Value is null)
            return false;
        value = r.Value;
        return true;
    }

    public bool TryGetBytes(ReadOnlySpan<byte> recordId, ushort fieldId, out byte[]? value)
    {
        if (!TryResolve(recordId, fieldId, out var r) || r.IsTombstone)
        {
            value = null;
            return false;
        }
        value = r.Value;
        return value is not null;
    }

    /// <summary>LWW resolve including tombstones (Found + IsTombstone).</summary>
    public bool TryResolve(ReadOnlySpan<byte> recordId, ushort fieldId, out FieldResolver.Resolved resolved)
    {
        resolved = FieldResolver.Resolved.Miss;
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (recordId.Length != 16) return false;

        lock (_commitLock)
            return TryResolveUnlocked(recordId, fieldId, out resolved);
    }

    /// <summary>Mark record dead via lifecycle field 0x0000 = Dead.</summary>
    public void DeleteRecord(ReadOnlySpan<byte> recordId)
        => Put(recordId, FldLifecycle.FieldId, FldLifecycle.DeadValue);

    public void UndeleteRecord(ReadOnlySpan<byte> recordId)
        => Put(recordId, FldLifecycle.FieldId, FldLifecycle.AliveValue);

    public bool IsRecordDead(ReadOnlySpan<byte> recordId)
        => TryResolve(recordId, FldLifecycle.FieldId, out var r) && !r.IsTombstone
           && r.Value is { Length: 1 } && r.Value[0] == FldLifecycle.Dead;

    /// <summary>
    /// Compact all runs (+ overlay if any) for this namespace into one L0 run.
    /// LWW-merges every field; writes new run, updates manifest, removes old run files.
    /// </summary>
    public ulong Compact()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        lock (_commitLock)
            return CompactUnderLock();
    }

    private ulong CompactUnderLock()
    {
        EnsureHealthy();
        if (_dirty.Count != 0)
            throw new InvalidOperationException("Compact requires all overlay writes to be committed first.");

        // Index overlay keys once. ScanFieldIds includes tombstones; ScanRecord intentionally does not.
        var fieldsByRecord = new Dictionary<(ulong Lo, ulong Hi), HashSet<ushort>>();
        var recordIds = new List<byte[]>();

        void AddRecord(ReadOnlySpan<byte> recordId)
        {
            var recordKey = (
                System.Buffers.Binary.BinaryPrimitives.ReadUInt64LittleEndian(recordId),
                System.Buffers.Binary.BinaryPrimitives.ReadUInt64LittleEndian(recordId.Slice(8)));
            if (fieldsByRecord.ContainsKey(recordKey))
                return;

            fieldsByRecord.Add(recordKey, new HashSet<ushort>());
            recordIds.Add(recordId.ToArray());
        }

        Span<byte> overlayRecordId = stackalloc byte[16];
        foreach (var item in _overlay.Snapshot())
        {
            item.Key.WriteRecordId(overlayRecordId);
            AddRecord(overlayRecordId);
            fieldsByRecord[(item.Key.RecLo, item.Key.RecHi)].Add(item.Key.FieldId);
        }

        for (int runIndex = 0; runIndex < _runs.Count; runIndex++)
        {
            var bytes = EnsureRunCached(runIndex);
            new RunReader(bytes).ScanRecordIds(AddRecord);
        }

        if (recordIds.Count == 0)
            return 0;

        var builder = new RunBuilder(_namespaceId);
        foreach (var recordId in recordIds)
        {
            var recordKey = (
                System.Buffers.Binary.BinaryPrimitives.ReadUInt64LittleEndian(recordId),
                System.Buffers.Binary.BinaryPrimitives.ReadUInt64LittleEndian(recordId.AsSpan(8)));
            HashSet<ushort> fieldIds = fieldsByRecord[recordKey];

            for (int runIndex = 0; runIndex < _runs.Count; runIndex++)
            {
                var bytes = EnsureRunCached(runIndex);
                new RunReader(bytes).ScanFieldIds(recordId, fieldId => { fieldIds.Add(fieldId); });
            }

            foreach (ushort fieldId in fieldIds)
            {
                if (!TryResolveUnlocked(recordId, fieldId, out var resolved) || !resolved.Found)
                    continue;

                if (resolved.IsTombstone)
                {
                    builder.Add(recordId, fieldId, ReadOnlySpan<byte>.Empty,
                        resolved.Wall, resolved.Logical,
                        resolved.AuthorNodeId ?? _localAuthor, resolved.KeyEpoch, isTombstone: true);
                }
                else if (resolved.Value is not null)
                {
                    builder.Add(recordId, fieldId, resolved.Value,
                        resolved.Wall, resolved.Logical,
                        resolved.AuthorNodeId ?? _localAuthor, resolved.KeyEpoch, isTombstone: false);
                }
            }
        }

        byte[] runBytes = builder.Build();
        var newReader = new RunReader(runBytes);
        if (!newReader.TryValidate(out var verify) || newReader.NamespaceId != _namespaceId)
            throw new InvalidDataException($"Compaction produced invalid Run: {verify.Status} {verify.Detail}");

        ulong runId = _nextRunId++;
        string path = Path.Combine(_dbDir, "runs", $"{runId}.fld1");
        WriteRunAtomically(path, runBytes);

        recordIds.Sort((x, y) => Fld1.Core.Util.ByteUtil.CompareUnsigned16(x, y));
        ulong seqHi = _pendingFlushedCommitSeq;
        if (seqHi == 0)
            seqHi = _manifestStore.Load().LastFlushedCommitSeq;
        if (seqHi == 0)
            seqHi = _lastSealedCommitSeq;

        var oldPaths = _runs.Select(run => run.path).ToList();
        _manifestStore.Update(manifest =>
        {
            var runs = manifest.Runs.Where(run => run.Ns != _namespaceId).ToList();
            runs.Add(new RunEntry
            {
                RunId = runId,
                Level = 0,
                Ns = _namespaceId,
                MinRec = recordIds[0],
                MaxRec = recordIds[^1],
                SeqLo = 1,
                SeqHi = seqHi
            });
            return CloneManifest(manifest, runs: runs, lastFlushed: seqHi);
        });

        try
        {
            _outbox.MarkMaterialized(seqHi);
        }
        catch
        {
            _faulted = true;
            throw;
        }

        _manifestDirty = false;
        _pendingFlushedCommitSeq = seqHi;
        _runs.Clear();
        _runs.Add((runId, path, runBytes));
        _overlay.Clear();
        _dirty.Clear();
        _lastSealedCommitSeq = seqHi;

        foreach (string oldPath in oldPaths)
        {
            try
            {
                if (oldPath != path)
                    File.Delete(oldPath);
            }
            catch
            {
                // Manifest already points at the replacement Run; stale files are collected on next open.
            }
        }

        if (_truncateSealedWal)
            _wal.TruncateSealedSegments(seqHi);

        return runId;
    }

    /// <summary>Remove run files on disk not referenced by in-memory run list.</summary>
    private void GcOrphanRuns()
    {
        string runsDir = Path.Combine(_dbDir, "runs");
        if (!Directory.Exists(runsDir)) return;

        // A database directory may contain several namespaces. Only runs absent from
        // the complete manifest are orphaned; the current FldDatabase loads one namespace.
        var manifest = _manifestStore.Load();
        var liveRunIds = manifest.Runs.Select(run => run.RunId).ToHashSet();

        foreach (string file in Directory.EnumerateFiles(runsDir, "*.fld1"))
        {
            string name = Path.GetFileNameWithoutExtension(file);
            if (!ulong.TryParse(name, out ulong runId) || liveRunIds.Contains(runId))
                continue;

            try { File.Delete(file); }
            catch { /* stale orphan is harmless and will be retried on next open */ }
        }
    }

    /// <summary>Resolve without taking _commitLock (caller may hold it).</summary>
    private bool TryResolveUnlocked(ReadOnlySpan<byte> recordId, ushort fieldId, out FieldResolver.Resolved resolved)
    {
        resolved = FieldResolver.Resolved.Miss;
        bool have = false;
        ulong bestWall = 0; uint bestLog = 0;
        byte[]? bestAuthor = null;
        byte[]? bestValue = null;
        bool bestTomb = false;
        ushort bestEpoch = 0;

        var key = new MemFieldKey(recordId, fieldId);
        if (_overlay.TryGet(key, out var mem) && mem is not null)
        {
            have = true;
            bestWall = mem.Wall; bestLog = mem.Logical;
            bestAuthor = mem.AuthorNodeId; bestValue = mem.Value;
            bestTomb = mem.IsTombstone; bestEpoch = mem.KeyEpoch;
        }

        for (int i = 0; i < _runs.Count; i++)
        {
            var bytes = EnsureRunCached(i);
            if (!new RunReader(bytes).TryGetField(recordId, fieldId, out var st) || !st.Found)
                continue;
            var author = st.AuthorNodeId.ToArray();
            if (!have || FieldResolver.IsNewer(st.Wall, st.Logical, author, bestWall, bestLog, bestAuthor ?? ReadOnlySpan<byte>.Empty))
            {
                have = true;
                bestWall = st.Wall; bestLog = st.Logical;
                bestAuthor = author;
                bestValue = st.IsTombstone ? Array.Empty<byte>() : st.Value.ToArray();
                bestTomb = st.IsTombstone; bestEpoch = st.KeyEpoch;
            }
        }
        if (!have) return false;
        resolved = new FieldResolver.Resolved(true, bestTomb, bestEpoch, bestWall, bestLog, bestAuthor, bestValue);
        return true;
    }

    private static ManifestDocument CloneManifest(
        ManifestDocument m,
        ulong? lastFlushed = null,
        List<uint>? nss = null,
        List<RunEntry>? runs = null)
        => new()
        {
            Generation = m.Generation,
            LastFlushedCommitSeq = lastFlushed ?? m.LastFlushedCommitSeq,
            CertifiedWatermark = m.CertifiedWatermark,
            Namespaces = nss ?? m.Namespaces,
            Runs = runs ?? m.Runs,
            Retains = m.Retains,
            ResultClients = m.ResultClients,
            ProvenanceIndex = m.ProvenanceIndex,
            CoordinatorLeaseEpoch = m.CoordinatorLeaseEpoch,
            CoordinatorSessionId = m.CoordinatorSessionId
        };

    /// <summary>Persist deferred LastFlushedCommitSeq to manifest (fsync).</summary>
    public void FlushManifest()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        lock (_commitLock)
        {
            if (!_manifestDirty) return;
            _manifestStore.Update(m => CloneManifest(m, lastFlushed: _pendingFlushedCommitSeq));
            _manifestDirty = false;
        }
    }

    /// <summary>
    /// Scan live fields for a record after LWW merge across overlay + all runs.
    /// </summary>
    public void ScanRecord(ReadOnlySpan<byte> recordId, Action<ushort, byte[]> onField)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (recordId.Length != 16) return;
        ArgumentNullException.ThrowIfNull(onField);

        var result = new List<(ushort FieldId, byte[] Value)>();
        lock (_commitLock)
        {
            var fieldIds = new HashSet<ushort>();
            Span<byte> overlayRecordId = stackalloc byte[16];

            foreach (var item in _overlay.Snapshot())
            {
                item.Key.WriteRecordId(overlayRecordId);
                if (overlayRecordId.SequenceEqual(recordId))
                    fieldIds.Add(item.Key.FieldId);
            }

            for (int i = 0; i < _runs.Count; i++)
            {
                var bytes = EnsureRunCached(i);
                new RunReader(bytes).ScanRecord(recordId, (fieldId, _) => fieldIds.Add(fieldId));
            }

            foreach (ushort fieldId in fieldIds)
            {
                if (TryResolveUnlocked(recordId, fieldId, out var resolved)
                    && resolved.Found && !resolved.IsTombstone && resolved.Value is not null)
                {
                    result.Add((fieldId, resolved.Value));
                }
            }
        }

        foreach (var item in result.OrderBy(x => x.FieldId))
            onField(item.FieldId, item.Value);
    }

    /// <summary>Enumerate distinct RecordIds from overlay + all runs (BE order not guaranteed).</summary>
    public void ScanRecordIds(Action<byte[]> onRecordId)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentNullException.ThrowIfNull(onRecordId);

        var result = new List<byte[]>();
        lock (_commitLock)
        {
            var ids = new HashSet<(ulong Lo, ulong Hi)>();

            foreach (var item in _overlay.Snapshot())
            {
                if (!ids.Add((item.Key.RecLo, item.Key.RecHi)))
                    continue;

                var recordId = new byte[16];
                item.Key.WriteRecordId(recordId);
                result.Add(recordId);
            }

            for (int i = 0; i < _runs.Count; i++)
            {
                var bytes = EnsureRunCached(i);
                new RunReader(bytes).ScanRecordIds(recordId =>
                {
                    ulong lo = System.Buffers.Binary.BinaryPrimitives.ReadUInt64LittleEndian(recordId);
                    ulong hi = System.Buffers.Binary.BinaryPrimitives.ReadUInt64LittleEndian(recordId.Slice(8));
                    if (ids.Add((lo, hi)))
                        result.Add(recordId.ToArray());
                });
            }
        }

        foreach (var recordId in result)
            onRecordId(recordId);
    }

    private byte[] EnsureRunCached(int index)
    {
        var (runId, path, cached) = _runs[index];
        if (cached is not null) return cached;

        var bytes = File.ReadAllBytes(path);
        var reader = new RunReader(bytes);
        if (!reader.TryValidate(out var verify) || reader.NamespaceId != _namespaceId)
            throw new InvalidDataException(
                $"Run {runId} is invalid for namespace {_namespaceId}: {verify.Status} {verify.Detail}");

        _runs[index] = (runId, path, bytes);
        return bytes;
    }



    /// <summary>
    /// Apply remote fields with LWW (remote HLC/author). Marks dirty for next Commit.
    /// Transport/BIN-SYNC decode happens outside; this is the storage apply edge.
    /// Returns number of fields that became the LWW winner in overlay.
    /// </summary>
    public int ApplyRemote(ReadOnlySpan<RemoteFieldWrite> writes)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        lock (_commitLock)
        {
            EnsureHealthy();
            int applied = 0;

            foreach (ref readonly var write in writes)
            {
                var key = new MemFieldKey(write.RecordId, write.FieldId);
                if (TryResolveUnlocked(write.RecordId, write.FieldId, out var resolved) && resolved.Found)
                {
                    if (!FieldResolver.IsNewer(
                            write.Wall, write.Logical, write.AuthorNodeId,
                            resolved.Wall, resolved.Logical,
                            resolved.AuthorNodeId ?? ReadOnlySpan<byte>.Empty))
                        continue;
                }

                byte[] changeId = write.ChangeId is { Length: 16 }
                    ? write.ChangeId.ToArray()
                    : NewChangeId();

                _overlay.Put(key, new MemFieldValue
                {
                    Value = write.IsTombstone ? Array.Empty<byte>() : write.Value.ToArray(),
                    Wall = write.Wall,
                    Logical = write.Logical,
                    AuthorNodeId = write.AuthorNodeId.ToArray(),
                    KeyEpoch = write.KeyEpoch,
                    IsTombstone = write.IsTombstone,
                    ChangeId = changeId
                });

                _dirty.Add(key);
                applied++;
            }

            return applied;
        }
    }

    public int CompactOutbox()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        lock (_commitLock)
        {
            EnsureHealthy();
            return _outbox.Compact();
        }
    }

    public IReadOnlyList<OutboxEntry> ReadOutboxBatch(ulong afterSeq, int limit = 256)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        lock (_commitLock)
        {
            EnsureHealthy();
            return _outbox.ReadBatch(afterSeq, limit);
        }
    }

    public void AcknowledgeOutbox(ulong throughSeq)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        lock (_commitLock)
        {
            EnsureHealthy();
            _outbox.Acknowledge(throughSeq);
        }
    }

    public ulong GetOutboxWatermark()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        return _outbox.Watermark;
    }

    public void RotateWal()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        lock (_commitLock)
            _wal.Rotate();
    }

    private void EnsureHealthy()
    {
        if (_faulted)
            throw new InvalidOperationException("Database requires reopen after a failed durable operation.");
    }

    private static void WriteRunAtomically(string path, byte[] runBytes)
    {
        string tempPath = path + ".tmp";
        string directory = Path.GetDirectoryName(path)!;

        using (var fs = new FileStream(
            tempPath, FileMode.Create, FileAccess.Write, FileShare.None,
            bufferSize: 64 * 1024, FileOptions.WriteThrough))
        {
            fs.Write(runBytes);
            fs.Flush(flushToDisk: true);
        }

        File.Move(tempPath, path, overwrite: true);
        TryFsyncDirectory(directory);
    }

    private static void TryFsyncDirectory(string directory)
    {
        try
        {
            if (OperatingSystem.IsLinux() || OperatingSystem.IsMacOS())
            {
                using var fs = new FileStream(directory, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
                fs.Flush(flushToDisk: true);
            }
        }
        catch
        {
            // Best-effort; directory handles are not flushable on every supported filesystem.
        }
    }

    public void Dispose()
    {
        if (_disposed) return;
        lock (_commitLock)
        {
            if (_manifestDirty)
            {
                try
                {
                    _manifestStore.Update(m => CloneManifest(m, lastFlushed: _pendingFlushedCommitSeq));
                    _manifestDirty = false;
                }
                catch { /* best-effort on dispose */ }
            }
            _disposed = true;
            _outbox.Dispose();
            _wal.Dispose();
        }
    }
}
