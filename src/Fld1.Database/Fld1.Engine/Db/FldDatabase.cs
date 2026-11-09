using System.Buffers.Binary;
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
using Fld1.Storage.Engine;
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
/// <summary>Conservative count and maximum HLC of unpublished FLD local operations.</summary>
public readonly record struct FldDraftHorizon(ulong Count, ulong MaxHlcWall, uint MaxHlcLogical);

/// <summary>
/// Neutral acknowledgement that a specific FLD field operation has been frozen into
/// an immutable downstream stream. It assigns identity only; it never carries payload.
/// </summary>
public sealed class FldPublishedOrigin
{
    public required byte[] RecordId { get; init; }
    public required ushort FieldId { get; init; }
    public required byte[] ChangeId { get; init; }
    public required uint OriginAuthorEpoch { get; init; }
    public required ulong OriginSequence { get; init; }
}

public sealed partial class FldDatabase : IDisposable
{
    // One root-wide lock: manifest, namespace WALs, runs and outboxes share this database owner.
    private const ulong StoragePartitionId = 0;
    private readonly string _dbDir;
    private readonly Fld1.Storage.Engine.StoragePartitionLease _storageLease;
    private readonly Fld1.Storage.Engine.StorageDatabase _runStorage;
    private readonly Fld1.Storage.Engine.StorageBlobStore _runBlobStore;
    private readonly uint _namespaceId;
    private readonly byte[] _localAuthor;
    private readonly HlcClock _clock;
    private readonly MemOverlay _overlay = new();
    private readonly Dictionary<MemFieldKey, MemFieldValue> _dirty = new();
    private ApplyJournal _applyJournal;
    // Rebuildable in-memory author discovery cache; no duplicate durable author index.
    private Dictionary<string, (byte[] Author, ulong MaxUnackedJournalSeq)>? _unacknowledgedRelayAuthors;
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
        ApplyJournal applyJournal,
        Fld1.Storage.Engine.StoragePartitionLease storageLease,
        Fld1.Storage.Engine.StorageDatabase runStorage,
        Fld1.Storage.Engine.StorageBlobStore runBlobStore,
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
        _applyJournal = applyJournal;
        _storageLease = storageLease ?? throw new ArgumentNullException(nameof(storageLease));
        _runStorage = runStorage ?? throw new ArgumentNullException(nameof(runStorage));
        _runBlobStore = runBlobStore ?? throw new ArgumentNullException(nameof(runBlobStore));
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

        Fld1.Storage.Engine.StoragePartitionLease? storageLease = null;
        Fld1.Storage.Engine.StorageDatabase? runStorage = null;
        bool leaseTransferred = false;
        bool runStorageTransferred = false;
        try
        {
            storageLease = Fld1.Storage.Engine.StorageDatabase.AcquirePartitionLease(
                dbDir, StoragePartitionId);
            runStorage = Fld1.Storage.Engine.StorageDatabase.Open(
                Path.Combine(dbDir, "storage-core"), partitionId: 0);
            var runBlobStore = new Fld1.Storage.Engine.StorageBlobStore(
                runStorage, "FLDRUNB1"u8);

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
            string outboxDir = Path.Combine(dbDir, "outbox");
            string outboxPath = Path.Combine(outboxDir, $"ns_{namespaceId}.obx");
            bool hasOutboxHistory = File.Exists(outboxPath) && new FileInfo(outboxPath).Length > 16
                                    || File.Exists(outboxPath + ".ack")
                                    || File.Exists(outboxPath + ".state");
            bool hasWalData = Directory.EnumerateFiles(walDir, $"ns_{namespaceId}.*.log")
                .Any(path => new FileInfo(path).Length > WalHeader.Size);
            string applyJournalPath = Path.Combine(outboxDir, $"ns_{namespaceId}.apply");
            bool hasApplyHistory = (File.Exists(applyJournalPath)
                                    && new FileInfo(applyJournalPath).Length > 16)
                                   || File.Exists(applyJournalPath + ".relayack");
            bool hasDurableData = manifest.Runs.Any(run => run.Ns == namespaceId)
                                  || hasWalData
                                  || hasOutboxHistory
                                  || hasApplyHistory;
            byte[] author = LocalAuthorStore.LoadOrCreate(dbDir, localAuthor, hasDurableData);

            var (wal, walRecords) = WalManager.Open(walDir, namespaceId, options.WalRotateAfterBytes);
            OutboxLog? outbox = null;
            ApplyJournal? applyJournal = null;
            try
            {
                outbox = OutboxLog.Open(
                    outboxDir, namespaceId, checkpointedThroughCommitSeq);
                applyJournal = ApplyJournal.Open(outboxDir, namespaceId);

                var db = new FldDatabase(
                    dbDir, namespaceId, author, wal, outbox, applyJournal, storageLease,
                    runStorage, runBlobStore, manifestStore,
                    options.AutoCheckpointOverlayFields,
                    options.TruncateSealedWal,
                    options.AutoCompactRunCount);

                db.LoadRunsFromManifest(manifest);
                db.RecoverFullReplaceStageCommitMarkers();
                db.GcOrphanRuns();
                db.ReplayWalIntoOverlay(walRecords);
                db._applyJournal.ValidateRelayAckWatermarks();
                if (db._truncateSealedWal && db._lastSealedCommitSeq != 0)
                {
                    db._wal.TruncateSealedSegments(db._lastSealedCommitSeq);
                    db._applyJournal.CompactAcknowledgedRelayPayloads(db._lastSealedCommitSeq);
                }
                leaseTransferred = true;
                runStorageTransferred = true;
                return db;
            }
            catch
            {
                // Open must not leak file handles when Manifest/Run/Outbox recovery fails.
                try { applyJournal?.Dispose(); } catch { /* preserve original open error */ }
                try { outbox?.Dispose(); } catch { /* preserve original open error */ }
                try { wal.Dispose(); } catch { /* preserve original open error */ }
                throw;
            }
        }
        finally
        {
            if (!runStorageTransferred)
                runStorage?.Dispose();
            if (!leaseTransferred)
                storageLease?.Dispose();
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
            Guid blobId = RunBlobId(re.RunId);
            if (_runBlobStore.ContainsBlob(blobId))
            {
                if (!_runBlobStore.TryOpenRead(blobId, out StorageBlobReader? blobReader) || blobReader is null)
                    throw new InvalidDataException($"Manifest references an incomplete Storage blob for Run {re.RunId}.");
                blobReader.Dispose();
            }
            else if (!File.Exists(path))
            {
                throw new InvalidDataException(
                    $"Manifest references missing Run {re.RunId} in both Storage blobs and legacy Run files.");
            }

            _runs.Add((re.RunId, path, null));
            if (re.SeqHi > sealedSeq) sealedSeq = re.SeqHi;
        }
        _lastSealedCommitSeq = Math.Max(sealedSeq, _outbox.MaterializedThroughCommitSeq);
    }

    /// <summary>
    /// Recover the storage overlay and idempotently finish Outbox materialization.
    /// The Outbox watermark is independent of Run sealing and prevents ACKed entries
    /// removed by CompactOutbox from being resurrected during WAL replay.
    /// </summary>
    private void ReplayWalIntoOverlay(List<WalRecoveredRecord> records)
    {
        bool needsOutboxFlush = false;
        bool needsApplyJournalFlush = false;
        ulong materializedThrough = _outbox.MaterializedThroughCommitSeq;

        foreach (var record in records)
        {
            if (record.Kind == WalConstants.KindIdentityStamp)
            {
                if (record.CommitSeq > _lastSealedCommitSeq)
                {
                    IReadOnlyList<WalIdentityStamp> stamps = IdentityStampCodec.Decode(record.Payload);
                    ApplyIdentityStampsUnlocked(stamps);
                    _pendingFlushedCommitSeq = Math.Max(_pendingFlushedCommitSeq, record.CommitSeq);
                    _manifestDirty = true;
                }
                continue;
            }

            if (record.Kind == WalConstants.KindApplyCommit)
            {
                var apply = ApplyCommitCodec.Decode(record.Payload);
                if (apply.NamespaceId != _namespaceId)
                    throw new InvalidDataException("APPLY_COMMIT namespace does not match database.");

                if (record.CommitSeq <= _lastSealedCommitSeq)
                {
                    // A sealed journal record may already have had ACKed relay
                    // payload reclaimed. Its durable receipt is authoritative;
                    // do not compare it with the full pre-checkpoint WAL payload.
                    if (_applyJournal.TryFindBatch(
                            apply.IssuerNodeId, apply.IssuerEpoch, apply.NamespaceId,
                            apply.BatchId, out var existingReceipt))
                    {
                        if (existingReceipt is null ||
                            !_applyJournal.MatchesReceiptMetadata(existingReceipt.Commit, apply))
                            throw new InvalidDataException("Sealed APPLY WAL metadata conflicts with its durable batch receipt.");
                    }
                    else if (_applyJournal.AppendIfMissing(record.CommitSeq, record.Payload))
                    {
                        needsApplyJournalFlush = true;
                    }
                    continue;
                }

                if (_applyJournal.AppendIfMissing(record.CommitSeq, record.Payload))
                    needsApplyJournalFlush = true;

                foreach (var field in apply.Fields)
                {
                    var key = new MemFieldKey(field.RecordId, field.FieldId);
                    if (TryResolveUnlocked(field.RecordId, field.FieldId, out var resolved)
                        && resolved.Found
                        && !FieldResolver.IsNewer(
                            field.Wall, field.Logical, field.AuthorNodeId,
                            resolved.Wall, resolved.Logical,
                            resolved.AuthorNodeId ?? ReadOnlySpan<byte>.Empty))
                        continue;

                    _overlay.Put(key, new MemFieldValue
                    {
                        Value = field.OpKind == 2 ? Array.Empty<byte>() : field.Value,
                        Wall = field.Wall,
                        Logical = field.Logical,
                        AuthorNodeId = field.AuthorNodeId,
                        KeyEpoch = field.KeyEpoch,
                        IsTombstone = field.OpKind == 2,
                        ChangeId = field.ChangeId,
                        OriginAuthorEpoch = field.OriginAuthorEpoch,
                        OriginSequence = field.OriginSequence
                    });
                }

                // Even an all-LWW-loser APPLY is durable work: its relay journal
                // record is already materialized and its WAL prefix can be sealed.
                _pendingFlushedCommitSeq = Math.Max(_pendingFlushedCommitSeq, record.CommitSeq);
                _manifestDirty = true;
                continue;
            }

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
                        ChangeId = changeId,
                        OriginAuthorEpoch = field.OriginAuthorEpoch,
                        OriginSequence = field.OriginSequence
                    });
                    _pendingFlushedCommitSeq = Math.Max(_pendingFlushedCommitSeq, record.CommitSeq);
                    _manifestDirty = true;
                }
            }
        }

        if (needsOutboxFlush)
            _outbox.Flush();
        if (needsApplyJournalFlush)
            _applyJournal.Flush();
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
            _dirty[key] = valueState;
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
            _dirty[key] = valueState;
        }
    }

    /// <summary>
    /// Append a provider-owned local field draft with its stable source identity.
    /// The author epoch is assigned by the host before the draft enters the feed;
    /// OriginSequence must remain zero until BIN-SYNC freezes the operation.
    /// This API is protocol-neutral and persists the supplied ChangeId unchanged.
    /// </summary>
    public void PutWithIdentity(
        ReadOnlySpan<byte> recordId,
        ushort fieldId,
        ReadOnlySpan<byte> value,
        ReadOnlySpan<byte> changeId,
        uint originAuthorEpoch,
        ulong wall,
        uint logical,
        ushort keyEpoch = 0,
        bool isTombstone = false)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        EnsureHealthy();
        if (recordId.Length != 16)
            throw new ArgumentException("RecordId must be 16 bytes.", nameof(recordId));
        if (changeId.Length != 16)
            throw new ArgumentException("ChangeId must be 16 bytes.", nameof(changeId));
        byte aggregate = 0;
        foreach (byte part in changeId) aggregate |= part;
        if (aggregate == 0)
            throw new ArgumentException("ChangeId must be nonzero.", nameof(changeId));
        if (originAuthorEpoch == 0)
            throw new ArgumentOutOfRangeException(nameof(originAuthorEpoch));
        if (wall == 0)
            throw new ArgumentOutOfRangeException(nameof(wall));
        if (isTombstone && !value.IsEmpty)
            throw new ArgumentException("Tombstone payload must be empty.", nameof(value));

        var key = new MemFieldKey(recordId, fieldId);
        var valueState = new MemFieldValue
        {
            Value = isTombstone ? Array.Empty<byte>() : value.ToArray(),
            Wall = wall,
            Logical = logical,
            AuthorNodeId = _localAuthor,
            KeyEpoch = keyEpoch,
            IsTombstone = isTombstone,
            ChangeId = changeId.ToArray(),
            OriginAuthorEpoch = originAuthorEpoch,
            OriginSequence = 0
        };

        lock (_commitLock)
        {
            EnsureHealthy();
            _overlay.Put(key, valueState);
            _dirty[key] = valueState;
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

            // Capture the actual local operation at write time. A later remote merge may
            // win the visible overlay, but must not rewrite the local operation's identity.
            var snap = new List<KeyValuePair<MemFieldKey, MemFieldValue>>(_dirty);

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
                    OriginAuthorEpoch = kv.Value.OriginAuthorEpoch,
                    OriginSequence = kv.Value.OriginSequence,
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
            if (snap.Count == 0)
            {
                // APPLY commits can contain only LWW losers. Their durable relay
                // journal is independent of the field Run, so seal the WAL prefix
                // without manufacturing an empty Run.
                ulong applyOnlySeq = _pendingFlushedCommitSeq;
                if (applyOnlySeq <= _lastSealedCommitSeq)
                {
                    if (_truncateSealedWal)
                    {
                        _wal.TruncateSealedSegments(_lastSealedCommitSeq);
                        _applyJournal.CompactAcknowledgedRelayPayloads(_lastSealedCommitSeq);
                    }
                    return 0;
                }

                try
                {
                    _outbox.MarkMaterialized(applyOnlySeq);
                    _manifestStore.Update(m => CloneManifest(m, lastFlushed: applyOnlySeq));
                }
                catch
                {
                    _faulted = true;
                    throw;
                }

                _lastSealedCommitSeq = applyOnlySeq;
                _pendingFlushedCommitSeq = applyOnlySeq;
                _manifestDirty = false;
                if (_truncateSealedWal)
                {
                    _wal.TruncateSealedSegments(applyOnlySeq);
                    _applyJournal.CompactAcknowledgedRelayPayloads(applyOnlySeq);
                }
                return 0;
            }

            var builder = new RunBuilder(_namespaceId);
            Span<byte> rec = stackalloc byte[16];
            foreach (var kv in snap)
            {
                kv.Key.WriteRecordId(rec);
                builder.Add(
                    rec, kv.Key.FieldId,
                    kv.Value.IsTombstone ? ReadOnlySpan<byte>.Empty : kv.Value.Value,
                    kv.Value.Wall, kv.Value.Logical, kv.Value.AuthorNodeId,
                    kv.Value.KeyEpoch, kv.Value.IsTombstone,
                    kv.Value.ChangeId, kv.Value.OriginAuthorEpoch, kv.Value.OriginSequence);
            }

            byte[] runBytes = builder.Build();
            ulong runId = AllocateRunId();
            string path = Path.Combine(_dbDir, "runs", $"{runId}.fld1");
            var builtReader = new RunReader(runBytes);
            if (!builtReader.TryValidate(out var builtVerify) || builtReader.NamespaceId != _namespaceId)
                throw new InvalidDataException(
                    $"Checkpoint produced invalid Run: {builtVerify.Status} {builtVerify.Detail}");
            _runBlobStore.PutImmutableBlob(RunBlobId(runId), runBytes);
            WriteRunAtomically(path, runBytes); // Compatibility mirror for legacy FLD1 readers.

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
            {
                _wal.TruncateSealedSegments(seqHi);
                _applyJournal.CompactAcknowledgedRelayPayloads(seqHi);
            }
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
        {
            EnsureHealthy();
            return TryResolveUnlocked(recordId, fieldId, out resolved);
        }
    }

    /// <summary>
    /// Persist immutable origins assigned by the downstream publication layer.
    /// The operation is identity-only: it does not append another Outbox row,
    /// alter HLC/value, or create a second pending journal. Repeated exact stamps
    /// are idempotent; conflicting origins for a still-current ChangeId fail closed.
    /// </summary>
    public int CommitPublishedOrigins(IReadOnlyList<FldPublishedOrigin> publishedOrigins)
    {
        ArgumentNullException.ThrowIfNull(publishedOrigins);
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (publishedOrigins.Count == 0)
            return 0;

        lock (_commitLock)
        {
            EnsureHealthy();
            var pending = new List<WalIdentityStamp>(Math.Min(publishedOrigins.Count, 1024));
            var seenFields = new HashSet<(ulong RecordLo, ulong RecordHi, ushort FieldId)>();

            foreach (FldPublishedOrigin item in publishedOrigins)
            {
                ArgumentNullException.ThrowIfNull(item);
                if (item.RecordId is null || item.RecordId.Length != 16)
                    throw new ArgumentException("Published-origin RecordId must be 16 bytes.", nameof(publishedOrigins));
                if (item.ChangeId is null || item.ChangeId.Length != 16 || IsZeroId(item.ChangeId))
                    throw new ArgumentException("Published-origin ChangeId must be nonzero and 16 bytes.", nameof(publishedOrigins));
                if (item.OriginAuthorEpoch == 0 || item.OriginSequence == 0)
                    throw new ArgumentException("Published origin epoch and sequence must be nonzero.", nameof(publishedOrigins));

                var fieldKey = (
                    System.Buffers.Binary.BinaryPrimitives.ReadUInt64LittleEndian(item.RecordId),
                    System.Buffers.Binary.BinaryPrimitives.ReadUInt64LittleEndian(item.RecordId.AsSpan(8)),
                    item.FieldId);
                if (!seenFields.Add(fieldKey))
                    throw new ArgumentException("Published-origin batch contains a duplicate field key.", nameof(publishedOrigins));

                if (!TryResolveUnlocked(item.RecordId, item.FieldId, out FieldResolver.Resolved current) ||
                    !current.Found || current.ChangeId is not { Length: 16 } ||
                    !current.ChangeId.AsSpan().SequenceEqual(item.ChangeId))
                {
                    // The operation was superseded. Its immutable outbound identity is
                    // retained by the publication store; it is no longer current state.
                    continue;
                }

                if (current.OriginAuthorEpoch != 0 || current.OriginSequence != 0)
                {
                    if (current.OriginAuthorEpoch != item.OriginAuthorEpoch ||
                        current.OriginSequence != item.OriginSequence)
                        throw new InvalidDataException("A current ChangeId already has a conflicting immutable OperationOrigin.");
                    continue;
                }

                pending.Add(new WalIdentityStamp
                {
                    RecordId = item.RecordId.ToArray(),
                    FieldId = item.FieldId,
                    ChangeId = item.ChangeId.ToArray(),
                    OriginAuthorEpoch = item.OriginAuthorEpoch,
                    OriginSequence = item.OriginSequence
                });
            }

            if (pending.Count == 0)
                return 0;

            int committed = 0;
            const int maximumPerRecord = WalConstants.MaxMutations;
            for (int offset = 0; offset < pending.Count; offset += maximumPerRecord)
            {
                int count = Math.Min(maximumPerRecord, pending.Count - offset);
                var page = pending.GetRange(offset, count);
                byte[] payload = IdentityStampCodec.Encode(page);
                try
                {
                    ulong commitSeq = _wal.Append(WalConstants.KindIdentityStamp, payload);
                    _wal.Flush();
                    ApplyIdentityStampsUnlocked(page);
                    _pendingFlushedCommitSeq = Math.Max(_pendingFlushedCommitSeq, commitSeq);
                    _manifestDirty = true;
                    committed += count;
                }
                catch
                {
                    // A durable stamp may already be present. Reopen replays the record
                    // idempotently before any further database writes are accepted.
                    _faulted = true;
                    throw;
                }
            }

            return committed;
        }
    }

    private void ApplyIdentityStampsUnlocked(IReadOnlyList<WalIdentityStamp> stamps)
    {
        foreach (WalIdentityStamp stamp in stamps)
        {
            if (!TryResolveUnlocked(stamp.RecordId, stamp.FieldId, out FieldResolver.Resolved current) ||
                !current.Found || current.ChangeId is not { Length: 16 } ||
                !current.ChangeId.AsSpan().SequenceEqual(stamp.ChangeId))
                continue;

            if (current.OriginAuthorEpoch != 0 || current.OriginSequence != 0)
            {
                if (current.OriginAuthorEpoch != stamp.OriginAuthorEpoch ||
                    current.OriginSequence != stamp.OriginSequence)
                    throw new InvalidDataException("Identity-stamp WAL conflicts with the current field OperationOrigin.");
                continue;
            }

            var value = new MemFieldValue
            {
                Value = current.IsTombstone ? Array.Empty<byte>() : current.Value?.ToArray() ?? Array.Empty<byte>(),
                Wall = current.Wall,
                Logical = current.Logical,
                AuthorNodeId = current.AuthorNodeId?.ToArray() ?? throw new InvalidDataException("Current field has no author identity."),
                KeyEpoch = current.KeyEpoch,
                IsTombstone = current.IsTombstone,
                ChangeId = current.ChangeId.ToArray(),
                OriginAuthorEpoch = stamp.OriginAuthorEpoch,
                OriginSequence = stamp.OriginSequence
            };
            _overlay.SetResolved(new MemFieldKey(stamp.RecordId, stamp.FieldId), value);
        }
    }

    private static bool IsZeroId(ReadOnlySpan<byte> value)
    {
        byte aggregate = 0;
        foreach (byte part in value) aggregate |= part;
        return aggregate == 0;
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
                        resolved.AuthorNodeId ?? _localAuthor, resolved.KeyEpoch, isTombstone: true,
                        changeId: resolved.ChangeId ?? Array.Empty<byte>(),
                        originAuthorEpoch: resolved.OriginAuthorEpoch,
                        originSequence: resolved.OriginSequence);
                }
                else if (resolved.Value is not null)
                {
                    builder.Add(recordId, fieldId, resolved.Value,
                        resolved.Wall, resolved.Logical,
                        resolved.AuthorNodeId ?? _localAuthor, resolved.KeyEpoch, isTombstone: false,
                        changeId: resolved.ChangeId ?? Array.Empty<byte>(),
                        originAuthorEpoch: resolved.OriginAuthorEpoch,
                        originSequence: resolved.OriginSequence);
                }
            }
        }

        byte[] runBytes = builder.Build();
        var newReader = new RunReader(runBytes);
        if (!newReader.TryValidate(out var verify) || newReader.NamespaceId != _namespaceId)
            throw new InvalidDataException($"Compaction produced invalid Run: {verify.Status} {verify.Detail}");

        ulong runId = _nextRunId++;
        string path = Path.Combine(_dbDir, "runs", $"{runId}.fld1");
        _runBlobStore.PutImmutableBlob(RunBlobId(runId), runBytes);
        WriteRunAtomically(path, runBytes); // Compatibility mirror for legacy FLD1 readers.

        recordIds.Sort((x, y) => Fld1.Core.Util.ByteUtil.CompareUnsigned16(x, y));
        ulong seqHi = _pendingFlushedCommitSeq;
        if (seqHi == 0)
            seqHi = _manifestStore.Load().LastFlushedCommitSeq;
        if (seqHi == 0)
            seqHi = _lastSealedCommitSeq;

        var oldRuns = _runs.Select(run => (runId: run.runId, path: run.path)).ToList();
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

        HashSet<ulong>? pinnedRunIds = GetPinnedProviderSnapshotRunIds();
        foreach (var oldRun in oldRuns)
        {
            try
            {
                if (oldRun.runId == runId || pinnedRunIds is null || pinnedRunIds.Contains(oldRun.runId))
                    continue;
                _runBlobStore.DeleteBlob(RunBlobId(oldRun.runId));
                if (File.Exists(oldRun.path)) File.Delete(oldRun.path);
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

    /// <summary>Remove run files absent from both the active manifest and durable provider snapshot pins.</summary>
    private void GcOrphanRuns()
    {
        string runsDir = Path.Combine(_dbDir, "runs");

        // A database directory may contain several namespaces. Only runs absent from
        // the complete manifest, provider snapshot pins and active staging generations
        // are orphaned; the current FldDatabase loads one namespace.
        var manifest = _manifestStore.Load();
        var liveRunIds = manifest.Runs.Select(run => run.RunId).ToHashSet();
        HashSet<ulong>? snapshotPinnedRunIds = GetPinnedProviderSnapshotRunIds();
        if (snapshotPinnedRunIds is null)
            return; // Fail closed: never delete Runs if a snapshot pin manifest is corrupt.
        liveRunIds.UnionWith(snapshotPinnedRunIds);
        HashSet<ulong>? stagedRunIds = GetUncommittedFullReplaceRunIds();
        if (stagedRunIds is null)
            return; // Corrupt staging metadata cannot authorize deletion of its Run files.
        liveRunIds.UnionWith(stagedRunIds);

        if (Directory.Exists(runsDir))
        {
            foreach (string file in Directory.EnumerateFiles(runsDir, "*.fld1"))
            {
                string name = Path.GetFileNameWithoutExtension(file);
                if (!ulong.TryParse(name, out ulong runId) || liveRunIds.Contains(runId))
                    continue;

                try { File.Delete(file); }
                catch { /* stale legacy orphan is harmless and will be retried on next open */ }
            }
        }

        // Run IDs are globally monotonic within the root, so the same live set protects
        // active, snapshot-pinned and in-progress stage blobs across every namespace.
        HashSet<Guid> liveBlobIds = liveRunIds.Select(RunBlobId).ToHashSet();
        int prunedBlobs;
        do { prunedBlobs = _runBlobStore.PruneUnreferencedBlobs(liveBlobIds, 4096); }
        while (prunedBlobs == 4096);
        int prunedPages;
        do { prunedPages = _runBlobStore.PruneOrphanPages(16_384); }
        while (prunedPages == 16_384);
    }

    /// <summary>Resolve without taking _commitLock (caller may hold it).</summary>
    private bool TryResolveUnlocked(ReadOnlySpan<byte> recordId, ushort fieldId, out FieldResolver.Resolved resolved)
    {
        resolved = FieldResolver.Resolved.Miss;
        bool have = false;
        ulong bestWall = 0;
        uint bestLog = 0;
        byte[]? bestAuthor = null;
        byte[]? bestValue = null;
        byte[]? bestChangeId = null;
        uint bestOriginAuthorEpoch = 0;
        ulong bestOriginSequence = 0;
        bool bestTomb = false;
        ushort bestEpoch = 0;

        var key = new MemFieldKey(recordId, fieldId);
        if (_overlay.TryGet(key, out var mem) && mem is not null)
        {
            have = true;
            bestWall = mem.Wall;
            bestLog = mem.Logical;
            bestAuthor = mem.AuthorNodeId;
            bestValue = mem.Value;
            bestTomb = mem.IsTombstone;
            bestEpoch = mem.KeyEpoch;
            bestChangeId = mem.ChangeId;
            bestOriginAuthorEpoch = mem.OriginAuthorEpoch;
            bestOriginSequence = mem.OriginSequence;
        }

        for (int i = 0; i < _runs.Count; i++)
        {
            var bytes = EnsureRunCached(i);
            if (!new RunReader(bytes).TryGetField(recordId, fieldId, out var state) || !state.Found)
                continue;

            byte[] author = state.AuthorNodeId.ToArray();
            if (!have || FieldResolver.IsNewer(
                    state.Wall, state.Logical, author,
                    bestWall, bestLog, bestAuthor ?? ReadOnlySpan<byte>.Empty))
            {
                have = true;
                bestWall = state.Wall;
                bestLog = state.Logical;
                bestAuthor = author;
                bestValue = state.IsTombstone ? Array.Empty<byte>() : state.Value.ToArray();
                bestTomb = state.IsTombstone;
                bestEpoch = state.KeyEpoch;
                bestChangeId = state.ChangeId.IsEmpty ? Array.Empty<byte>() : state.ChangeId.ToArray();
                bestOriginAuthorEpoch = state.OriginAuthorEpoch;
                bestOriginSequence = state.OriginSequence;
            }
        }

        if (!have)
            return false;

        resolved = new FieldResolver.Resolved(
            true, bestTomb, bestEpoch, bestWall, bestLog,
            bestAuthor, bestValue, bestChangeId,
            bestOriginAuthorEpoch, bestOriginSequence);
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
            EnsureHealthy();
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
            EnsureHealthy();
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
            EnsureHealthy();
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

        byte[] bytes;
        Guid blobId = RunBlobId(runId);
        if (_runBlobStore.ContainsBlob(blobId))
        {
            if (!_runBlobStore.TryOpenRead(blobId, out StorageBlobReader? blobReader) || blobReader is null)
                throw new InvalidDataException($"Run {runId} references an incomplete Storage blob.");
            using (blobReader)
                bytes = blobReader.ReadAllBytes();
        }
        else
        {
            bytes = File.ReadAllBytes(path);
        }

        var reader = new RunReader(bytes);
        if (!reader.TryValidate(out var verify) || reader.NamespaceId != _namespaceId)
            throw new InvalidDataException(
                $"Run {runId} is invalid for namespace {_namespaceId}: {verify.Status} {verify.Detail}");

        _runs[index] = (runId, path, bytes);
        return bytes;
    }

    /// <summary>
    /// Allocates a root-global Run ID without colliding with active, staged or
    /// crash-left immutable blobs whose IDs are not yet present in the manifest.
    /// </summary>
    private ulong AllocateRunId()
    {
        while (true)
        {
            ulong candidate = _nextRunId;
            if (candidate == 0 || candidate == ulong.MaxValue)
                throw new InvalidOperationException("Root-global FLD Run ID space is exhausted.");
            _nextRunId = candidate + 1;
            string path = Path.Combine(_dbDir, "runs", $"{candidate}.fld1");
            if (!File.Exists(path) && !_runBlobStore.ContainsBlob(RunBlobId(candidate)))
                return candidate;
        }
    }

    private static Guid RunBlobId(ulong runId)
    {
        if (runId == 0) throw new ArgumentOutOfRangeException(nameof(runId));
        Span<byte> id = stackalloc byte[16];
        System.Buffers.Binary.BinaryPrimitives.WriteUInt64LittleEndian(id, runId);
        System.Buffers.Binary.BinaryPrimitives.WriteUInt32LittleEndian(id.Slice(8), 0x314E5552); // RUN1
        System.Buffers.Binary.BinaryPrimitives.WriteUInt32LittleEndian(id.Slice(12), 0x31444C46); // FLD1
        return new Guid(id);
    }



    /// <summary>
    /// Legacy volatile LWW merge only. This method is not durable and MUST NOT be used
    /// by BIN-SYNC receive; use ApplyRemoteBatch for atomic durable receive.
    /// Returns number of fields that became the LWW winner in overlay.
    /// </summary>
    [Obsolete("Non-durable merge-only compatibility API. Use ApplyRemoteBatch for synchronization.")]
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

                byte[] changeId = write.ChangeId.ToArray();

                _overlay.Put(key, new MemFieldValue
                {
                    Value = write.IsTombstone ? Array.Empty<byte>() : write.Value.ToArray(),
                    Wall = write.Wall,
                    Logical = write.Logical,
                    AuthorNodeId = write.AuthorNodeId.ToArray(),
                    KeyEpoch = write.KeyEpoch,
                    IsTombstone = write.IsTombstone,
                    ChangeId = changeId,
                    OriginAuthorEpoch = write.OriginAuthorEpoch,
                    OriginSequence = write.OriginSequence
                });

                applied++;
            }

            return applied;
        }
    }

    /// <summary>
    /// Atomically persist a validated remote batch, its opaque BIN-SYNC protocol delta,
    /// and all relay-eligible source mutations. This is the only supported durable
    /// receive boundary; it never creates local-author Outbox entries.
    /// </summary>
    public RemoteBatchReceipt ApplyRemoteBatch(RemoteBatchCommit batch)
    {
        ArgumentNullException.ThrowIfNull(batch);
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (batch.NamespaceId != _namespaceId)
            throw new ArgumentException("Remote batch namespace does not match this database.", nameof(batch));

        lock (_commitLock)
        {
            EnsureHealthy();

            if (_applyJournal.TryFindBatch(
                    batch.IssuerNodeId, batch.IssuerEpoch, batch.NamespaceId, batch.BatchId,
                    out var existing))
            {
                var prior = existing!.Commit;
                if (prior.CursorStart != batch.CursorStart
                    || prior.CursorEnd != batch.CursorEnd
                    || !prior.BatchDigest.AsSpan().SequenceEqual(batch.BatchDigest)
                    || !prior.RecordsRoot.AsSpan().SequenceEqual(batch.RecordsRoot)
                    || !prior.PreviousBatchDigest.AsSpan().SequenceEqual(batch.PreviousBatchDigest))
                    throw new InvalidDataException("Conflicting metadata for an already committed remote batch.");

                return new RemoteBatchReceipt
                {
                    CommitSeq = existing.CommitSeq,
                    CursorStart = prior.CursorStart,
                    CursorEnd = prior.CursorEnd,
                    BatchId = prior.BatchId.ToArray(),
                    ProtocolStateDelta = prior.ProtocolStateDelta.ToArray(),
                    AppliedCount = checked((int)prior.AppliedCount),
                    IsDuplicate = true
                };
            }

            var fields = new List<WalFieldMutation>(batch.Writes.Count);
            var winners = new Dictionary<MemFieldKey, RemoteFieldWrite>(batch.Writes.Count);
            foreach (var write in batch.Writes)
            {
                if (write.IsTombstone && write.Value.Length != 0)
                    throw new ArgumentException("Remote tombstone payload must be empty.", nameof(batch));

                fields.Add(new WalFieldMutation
                {
                    RecordId = write.RecordId,
                    FieldId = write.FieldId,
                    OpKind = write.IsTombstone ? (byte)2 : (byte)1,
                    KeyEpoch = write.KeyEpoch,
                    Wall = write.Wall,
                    Logical = write.Logical,
                    AuthorNodeId = write.AuthorNodeId,
                    ChangeId = write.ChangeId,
                    OriginAuthorEpoch = write.OriginAuthorEpoch,
                    OriginSequence = write.OriginSequence,
                    Value = write.IsTombstone ? Array.Empty<byte>() : write.Value
                });

                var key = new MemFieldKey(write.RecordId, write.FieldId);
                ulong currentWall;
                uint currentLogical;
                ReadOnlySpan<byte> currentAuthor;
                if (winners.TryGetValue(key, out var staged))
                {
                    currentWall = staged.Wall;
                    currentLogical = staged.Logical;
                    currentAuthor = staged.AuthorNodeId;
                }
                else if (TryResolveUnlocked(write.RecordId, write.FieldId, out var resolved) && resolved.Found)
                {
                    currentWall = resolved.Wall;
                    currentLogical = resolved.Logical;
                    currentAuthor = resolved.AuthorNodeId ?? ReadOnlySpan<byte>.Empty;
                }
                else
                {
                    winners[key] = write;
                    continue;
                }

                if (FieldResolver.IsNewer(
                    write.Wall, write.Logical, write.AuthorNodeId,
                    currentWall, currentLogical, currentAuthor))
                    winners[key] = write;
            }

            int appliedCount = winners.Count;
            var relayFields = new List<WalFieldMutation>(batch.RelayWrites.Count);
            foreach (var write in batch.RelayWrites)
            {
                relayFields.Add(new WalFieldMutation
                {
                    RecordId = write.RecordId,
                    FieldId = write.FieldId,
                    OpKind = write.IsTombstone ? (byte)2 : (byte)1,
                    KeyEpoch = write.KeyEpoch,
                    Wall = write.Wall,
                    Logical = write.Logical,
                    AuthorNodeId = write.AuthorNodeId,
                    ChangeId = write.ChangeId,
                    OriginAuthorEpoch = write.OriginAuthorEpoch,
                    OriginSequence = write.OriginSequence,
                    Value = write.IsTombstone ? Array.Empty<byte>() : write.Value
                });
            }

            var walCommit = new WalApplyCommit
            {
                SenderPeer = batch.SenderPeer,
                SenderEpoch = batch.SenderEpoch,
                NamespaceId = batch.NamespaceId,
                IssuerNodeId = batch.IssuerNodeId,
                IssuerEpoch = batch.IssuerEpoch,
                BatchId = batch.BatchId,
                CursorStart = batch.CursorStart,
                CursorEnd = batch.CursorEnd,
                PreviousBatchDigest = batch.PreviousBatchDigest,
                BatchDigest = batch.BatchDigest,
                RecordsRoot = batch.RecordsRoot,
                ProtocolStateDelta = batch.ProtocolStateDelta,
                AppliedCount = checked((uint)appliedCount),
                Fields = fields,
                RelayFields = relayFields
            };
            byte[] walPayload = ApplyCommitCodec.Encode(walCommit);

            ulong commitSeq;
            try
            {
                commitSeq = _wal.Append(WalConstants.KindApplyCommit, walPayload);
                _wal.Flush();

                if (!_applyJournal.AppendIfMissing(commitSeq, walPayload))
                    throw new InvalidDataException("Remote batch appeared in apply journal during serialized commit.");
                _applyJournal.Flush();
                AddUnacknowledgedRelayAuthorsLocked(walCommit.RelayFields, commitSeq);

                foreach (var pair in winners)
                {
                    var write = pair.Value;
                    _overlay.Put(pair.Key, new MemFieldValue
                    {
                        Value = write.IsTombstone ? Array.Empty<byte>() : write.Value,
                        Wall = write.Wall,
                        Logical = write.Logical,
                        AuthorNodeId = write.AuthorNodeId,
                        KeyEpoch = write.KeyEpoch,
                        IsTombstone = write.IsTombstone,
                        ChangeId = write.ChangeId,
                        OriginAuthorEpoch = write.OriginAuthorEpoch,
                        OriginSequence = write.OriginSequence
                    });
                }
            }
            catch
            {
                // WAL may already be durable. Recovery will finish journal materialization.
                _faulted = true;
                throw;
            }

            _pendingFlushedCommitSeq = Math.Max(_pendingFlushedCommitSeq, commitSeq);
            _manifestDirty = true;
            return new RemoteBatchReceipt
            {
                CommitSeq = commitSeq,
                CursorStart = batch.CursorStart,
                CursorEnd = batch.CursorEnd,
                BatchId = batch.BatchId.ToArray(),
                ProtocolStateDelta = batch.ProtocolStateDelta.ToArray(),
                AppliedCount = appliedCount,
                IsDuplicate = false
            };
        }
    }

    /// <summary>
    /// Enumerate original authors with at least one unacknowledged relay operation.
    /// This is a startup/discovery scan; it returns identities only and persists no
    /// second author index. Include the RPM3 map's own author list as well when
    /// recovering, because a crash may occur after this journal ACK but before map prune.
    /// </summary>
    public IReadOnlyList<byte[]> ReadUnacknowledgedRelayAuthors()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        lock (_commitLock)
        {
            EnsureHealthy();
            if (_unacknowledgedRelayAuthors is null)
                RebuildUnacknowledgedRelayAuthorsLocked();

            return _unacknowledgedRelayAuthors!
                .OrderBy(pair => pair.Key, StringComparer.Ordinal)
                .Select(pair => pair.Value.Author.ToArray())
                .ToArray();
        }
    }

    private void RebuildUnacknowledgedRelayAuthorsLocked()
    {
        var authors = new Dictionary<string, (byte[] Author, ulong MaxUnackedJournalSeq)>(StringComparer.Ordinal);
        ulong cursor = 0;
        ulong highWatermark = _applyJournal.HighWatermark;
        while (cursor < highWatermark)
        {
            var page = _applyJournal.ReadBatches(cursor, 128);
            if (page.Count == 0)
                throw new InvalidDataException("ApplyJournal ended before its durable relay high watermark.");

            foreach (var item in page)
            {
                cursor = item.JournalSeq;
                var batchAuthors = new Dictionary<string, byte[]>(StringComparer.Ordinal);
                foreach (WalFieldMutation field in item.Commit.RelayFields)
                {
                    if (field.AuthorNodeId.AsSpan().SequenceEqual(_localAuthor))
                        continue;
                    string authorKey = Convert.ToHexString(field.AuthorNodeId);
                    batchAuthors.TryAdd(authorKey, field.AuthorNodeId);
                }

                foreach (var pair in batchAuthors)
                {
                    if (_applyJournal.IsRelayAcknowledged(pair.Value, item.JournalSeq))
                        continue;
                    if (authors.TryGetValue(pair.Key, out var prior))
                        authors[pair.Key] = (prior.Author, Math.Max(prior.MaxUnackedJournalSeq, item.JournalSeq));
                    else
                        authors.Add(pair.Key, (pair.Value.ToArray(), item.JournalSeq));
                }
            }

            if (page.Count < 128)
                break;
        }

        if (cursor < highWatermark)
            throw new InvalidDataException("ApplyJournal relay-author scan did not reach its durable high watermark.");
        _unacknowledgedRelayAuthors = authors;
    }

    private void AddUnacknowledgedRelayAuthorsLocked(
        IReadOnlyList<WalFieldMutation> relayFields, ulong journalSeq)
    {
        if (_unacknowledgedRelayAuthors is null)
            return;

        foreach (WalFieldMutation field in relayFields)
        {
            if (field.AuthorNodeId.AsSpan().SequenceEqual(_localAuthor))
                continue;
            string authorKey = Convert.ToHexString(field.AuthorNodeId);
            if (_unacknowledgedRelayAuthors.TryGetValue(authorKey, out var prior))
                _unacknowledgedRelayAuthors[authorKey] =
                    (prior.Author, Math.Max(prior.MaxUnackedJournalSeq, journalSeq));
            else
                _unacknowledgedRelayAuthors.Add(authorKey, (field.AuthorNodeId.ToArray(), journalSeq));
        }
    }

    /// <summary>
    /// Read durable relay batches for one original author. JournalSeq is only an
    /// internal paging token; BIN-SYNC owns the author-stream sequence.
    /// </summary>
    public IReadOnlyList<RelayJournalBatch> ReadRelayBatches(
        ReadOnlySpan<byte> authorNodeId, ulong afterJournalSeq = 0, int limit = 256)
        => ReadRelayBatchPage(authorNodeId, afterJournalSeq, limit).Batches;

    /// <summary>
    /// Reads relay batches and returns the last journal sequence scanned, including
    /// batches that contained no relay operation for the requested author. The caller
    /// may persist this scan cursor only atomically with durable publication of every
    /// returned batch; it is not a downstream-safe ACK watermark.
    /// </summary>
    public RelayBatchPage ReadRelayBatchPage(
        ReadOnlySpan<byte> authorNodeId, ulong afterJournalSeq = 0, int limit = 256)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (authorNodeId.Length != 16)
            throw new ArgumentException("AuthorNodeId must be 16 bytes.", nameof(authorNodeId));
        if (limit <= 0)
            return new RelayBatchPage
            {
                Batches = Array.Empty<RelayJournalBatch>(),
                ScannedThroughJournalSeq = afterJournalSeq
            };
        byte[] authorFilter = authorNodeId.ToArray();

        lock (_commitLock)
        {
            EnsureHealthy();
            ulong highWatermark = _applyJournal.HighWatermark;
            if (authorNodeId.SequenceEqual(_localAuthor))
                return new RelayBatchPage
                {
                    Batches = Array.Empty<RelayJournalBatch>(),
                    ScannedThroughJournalSeq = highWatermark
                };

            var result = new List<RelayJournalBatch>(Math.Min(limit, 256));
            ulong cursor = afterJournalSeq;
            int operationCount = 0;
            while (result.Count < limit && cursor < highWatermark)
            {
                var page = _applyJournal.ReadBatches(cursor, 128);
                if (page.Count == 0) break;
                bool operationLimitReached = false;
                foreach (var item in page)
                {
                    if (_applyJournal.IsRelayAcknowledged(authorFilter, item.JournalSeq))
                    {
                        cursor = item.JournalSeq;
                        continue;
                    }
                    int matchingCount = 0;
                    foreach (WalFieldMutation field in item.Commit.RelayFields)
                    {
                        if (field.AuthorNodeId.AsSpan().SequenceEqual(authorFilter))
                            matchingCount++;
                    }
                    if (matchingCount == 0)
                    {
                        cursor = item.JournalSeq;
                        continue;
                    }
                    if (matchingCount > RemoteBatchCommit.MaximumOperationsPerBatch - operationCount)
                    {
                        operationLimitReached = true;
                        break;
                    }

                    var matching = new RemoteFieldWrite[matchingCount];
                    int matchingIndex = 0;
                    foreach (WalFieldMutation field in item.Commit.RelayFields)
                    {
                        if (!field.AuthorNodeId.AsSpan().SequenceEqual(authorFilter))
                            continue;
                        matching[matchingIndex++] = new RemoteFieldWrite(
                            field.RecordId, field.FieldId, field.Value, field.Wall, field.Logical,
                            field.AuthorNodeId, field.ChangeId, field.KeyEpoch, field.OpKind == 2);
                    }

                    var source = item.Commit;
                    var batchCopy = new RemoteBatchCommit(
                        source.SenderPeer, source.SenderEpoch, source.NamespaceId,
                        source.IssuerNodeId, source.IssuerEpoch, source.BatchId,
                        source.CursorStart, source.CursorEnd, source.PreviousBatchDigest,
                        source.BatchDigest, source.RecordsRoot, matching, matching, source.ProtocolStateDelta);
                    result.Add(new RelayJournalBatch
                    {
                        JournalSeq = item.JournalSeq,
                        CommitSeq = item.CommitSeq,
                        Batch = batchCopy
                    });
                    operationCount += matching.Length;
                    cursor = item.JournalSeq;
                    if (result.Count == limit) break;
                }
                if (operationLimitReached || page.Count < 128) break;
            }

            return new RelayBatchPage
            {
                Batches = result,
                ScannedThroughJournalSeq = cursor
            };
        }
    }

    /// <summary>
    /// Persist a per-author safe relay ACK watermark. The caller must compute this
    /// as the minimum contiguous downstream ACK across all interested peers. This
    /// records read-side retention safety; physical payload compaction waits until
    /// the corresponding APPLY WAL commit sequence has been sealed and truncated.
    /// </summary>
    public void AcknowledgeRelayBatches(
        ReadOnlySpan<byte> authorNodeId, ulong safeThroughJournalSeq)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        lock (_commitLock)
        {
            EnsureHealthy();
            _applyJournal.AcknowledgeRelayBatches(authorNodeId, safeThroughJournalSeq);
            if (_unacknowledgedRelayAuthors is { } authors)
            {
                string authorKey = Convert.ToHexString(authorNodeId);
                if (authors.TryGetValue(authorKey, out var pending) &&
                    safeThroughJournalSeq >= pending.MaxUnackedJournalSeq)
                    authors.Remove(authorKey);
            }
        }
    }

    public ulong GetRelayJournalHighWatermark()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        lock (_commitLock)
        {
            EnsureHealthy();
            return _applyJournal.HighWatermark;
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

    /// <summary>Acquire a durable Outbox consumer lease; acquiring a new one fences older consumers.</summary>
    public OutboxLease AcquireOutboxLease(ReadOnlySpan<byte> ownerId)
    {
        lock (_commitLock)
        {
            EnsureHealthy();
            return _outbox.AcquireLease(ownerId);
        }
    }

    /// <summary>Release a lease and immediately invalidate its token.</summary>
    public void ReleaseOutboxLease(OutboxLease lease)
    {
        lock (_commitLock)
        {
            EnsureHealthy();
            _outbox.ReleaseLease(lease);
        }
    }

    /// <summary>
    /// Fenced borrowed-span enumeration for an external BIN-SYNC consumer. The callback
    /// runs under the Outbox lock; it must not retain the view or re-enter this database.
    /// </summary>
    public int VisitOutboxBatch(
        ulong afterSeq, int limit, OutboxLease lease, OutboxEntryVisitor visitor)
    {
        lock (_commitLock)
            EnsureHealthy();
        return _outbox.VisitBatch(afterSeq, limit, lease, visitor);
    }

    /// <summary>Fenced ACK for the safe contiguous prefix computed by BIN-SYNC.</summary>
    public void AcknowledgeOutbox(ulong throughSeq, OutboxLease lease)
    {
        lock (_commitLock)
        {
            EnsureHealthy();
            _outbox.Acknowledge(throughSeq, lease);
        }
    }

    public ulong GetOutboxHighWatermark()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        lock (_commitLock)
        {
            EnsureHealthy();
            return _outbox.HighWatermark;
        }
    }

    /// <summary>Legacy unfenced ACK. External BIN-SYNC consumers must use the lease overload.</summary>
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

    /// <summary>
    /// Acquire the FLD application commit gate. Compose this after BIN-SYNC coordinator
    /// and Node gates, but before the shared BIN-SYNC Storage transaction gate.
    /// Dispose on the acquiring thread.
    /// </summary>
    public IDisposable AcquireCommitGuard()
    {
        Monitor.Enter(_commitLock);
        return new CommitLease(_commitLock);
    }

    private sealed class CommitLease : IDisposable
    {
        private object? _gate;
        public CommitLease(object gate) => _gate = gate;
        public void Dispose()
        {
            object? gate = Interlocked.Exchange(ref _gate, null);
            if (gate is not null) Monitor.Exit(gate);
        }
    }

    /// <summary>
    /// Snapshot the durable local draft horizon while the caller holds AcquireCommitGuard.
    /// A healthy database has recovered every WAL→Outbox gap before Open returns.
    /// Dirty overlay edits and the unacknowledged Outbox suffix are counted directly;
    /// an unsealed WAL prefix also forces a non-zero conservative barrier because
    /// recovery can reconstruct acknowledged/compacted Outbox rows after restart.
    /// </summary>
    public FldDraftHorizon ReadUnpublishedLocalDraftHorizon()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        lock (_commitLock)
        {
            EnsureHealthy();
            ulong count = checked((ulong)_dirty.Count);
            ulong maxWall = 0;
            uint maxLogical = 0;
            foreach (MemFieldValue value in _dirty.Values)
                UpdateMaximumHlc(value.Wall, value.Logical, ref maxWall, ref maxLogical);

            ulong cursor = _outbox.AckedThrough;
            const int pageSize = 512;
            while (true)
            {
                int visited = _outbox.VisitUnackedBatch(cursor, pageSize, Visit);
                if (visited < pageSize)
                    break;
            }
            // Until WAL is sealed, recovery can reconstruct an acknowledged/compacted
            // Outbox suffix. Keep the horizon non-zero so final-author closure cannot
            // strand replayable local commits behind a retired epoch.
            if (_pendingFlushedCommitSeq > _lastSealedCommitSeq && count == 0)
                count = 1;
            return new FldDraftHorizon(count, maxWall, maxLogical);

            bool Visit(in OutboxEntryView entry)
            {
                count = checked(count + 1);
                UpdateMaximumHlc(entry.Wall, entry.Logical, ref maxWall, ref maxLogical);
                cursor = entry.OutboxSeq;
                return true;
            }
        }
    }

    /// <summary>
    /// Acquire the application commit guard and capture the immutable reference snapshot
    /// under that guard. The caller owns both resources: dispose the snapshot and then
    /// the returned guard on the acquiring thread after the GC scan is complete.
    /// </summary>
    public IDisposable AcquireReferenceSnapshotGuard(out FldReferenceSnapshot snapshot)
    {
        IDisposable guard = AcquireCommitGuard();
        try
        {
            snapshot = OpenReferenceSnapshot();
            return guard;
        }
        catch
        {
            guard.Dispose();
            throw;
        }
    }

    /// <summary>
    /// Build one immutable index of host-side ChangeId, RecordId and receipt references.
    /// Call under the composed application commit lease and reuse the result for all
    /// candidate checks in that GC pass.
    /// </summary>
    public FldReferenceSnapshot OpenReferenceSnapshot()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        lock (_commitLock)
        {
            EnsureHealthy();
            var changeIds = new HashSet<FldId128>();
            var recordIds = new HashSet<FldId128>();
            var receipts = new HashSet<FldReceiptReferenceKey>();

            ulong draftCount = checked((ulong)_dirty.Count);
            ulong maxWall = 0;
            uint maxLogical = 0;
            Span<byte> recordIdBuffer = stackalloc byte[16];
            foreach (var pair in _dirty)
            {
                UpdateMaximumHlc(pair.Value.Wall, pair.Value.Logical, ref maxWall, ref maxLogical);
                if (pair.Value.ChangeId.Length != 16)
                    throw new InvalidDataException("Dirty FLD mutation has no canonical ChangeId.");
                changeIds.Add(FldId128.From(pair.Value.ChangeId));
                pair.Key.WriteRecordId(recordIdBuffer);
                recordIds.Add(FldId128.From(recordIdBuffer));
            }

            ulong outboxCursor = _outbox.AckedThrough;
            const int outboxPageSize = 512;
            while (true)
            {
                int visited = _outbox.VisitUnackedBatch(outboxCursor, outboxPageSize, VisitOutbox);
                if (visited < outboxPageSize)
                    break;
            }

            ulong journalCursor = 0;
            const int journalPageSize = 256;
            while (true)
            {
                IReadOnlyList<ApplyJournalRecord> page =
                    _applyJournal.ReadBatches(journalCursor, journalPageSize);
                foreach (ApplyJournalRecord record in page)
                {
                    journalCursor = record.JournalSeq;
                    WalApplyCommit commit = record.Commit;
                    if (commit.NamespaceId != _namespaceId)
                        continue;
                    receipts.Add(new FldReceiptReferenceKey(
                        FldId128.From(commit.SenderPeer), commit.SenderEpoch,
                        commit.IssuerEpoch, commit.NamespaceId, FldId128.From(commit.BatchId)));
                    AddFields(commit.Fields);
                    AddFields(commit.RelayFields);
                }
                if (page.Count < journalPageSize)
                    break;
            }

            bool unsealedWal = _pendingFlushedCommitSeq > _lastSealedCommitSeq;
            // Unsealed WAL may replay an acknowledged/compacted Outbox suffix after
            // restart. It is a conservative draft barrier until checkpoint/seal.
            if (unsealedWal && draftCount == 0)
                draftCount = 1;
            return new FldReferenceSnapshot(
                new FldDraftHorizon(draftCount, maxWall, maxLogical),
                changeIds, recordIds, receipts, unsealedWal);

            bool VisitOutbox(in OutboxEntryView entry)
            {
                draftCount = checked(draftCount + 1);
                UpdateMaximumHlc(entry.Wall, entry.Logical, ref maxWall, ref maxLogical);
                outboxCursor = entry.OutboxSeq;
                changeIds.Add(FldId128.From(entry.ChangeId));
                recordIds.Add(FldId128.From(entry.RecordId));
                return true;
            }

            void AddFields(IReadOnlyList<WalFieldMutation> fields)
            {
                foreach (WalFieldMutation field in fields)
                {
                    changeIds.Add(FldId128.From(field.ChangeId));
                    recordIds.Add(FldId128.From(field.RecordId));
                }
            }
        }
    }

    private static void UpdateMaximumHlc(
        ulong wall, uint logical, ref ulong maxWall, ref uint maxLogical)
    {
        if (wall > maxWall || (wall == maxWall && logical > maxLogical))
        {
            maxWall = wall;
            maxLogical = logical;
        }
    }

    /// <summary>
    /// Check whether a durable FLD APPLY journal entry still retains this incoming
    /// transport receipt identity. Any retained entry is treated as a live reference,
    /// including entries whose relay payload is already acknowledged but not compacted.
    /// </summary>
    public bool HasPendingReceiptReference(
        ReadOnlySpan<byte> senderPeer, uint authorEpoch, uint transportPeerEpoch,
        uint namespaceId, ReadOnlySpan<byte> batchId)
    {
        if (senderPeer.Length != 16 || batchId.Length != 16)
            throw new ArgumentException("Receipt identity values must be 16 bytes.");
        if (namespaceId != _namespaceId)
            return false;
        byte[] sender = senderPeer.ToArray();
        byte[] batch = batchId.ToArray();
        lock (_commitLock)
        {
            EnsureHealthy();
            if (_pendingFlushedCommitSeq > _lastSealedCommitSeq)
                return true; // An unsealed WAL prefix may still replay an APPLY receipt.
            return AnyApplyJournal(commit =>
                commit.NamespaceId == namespaceId &&
                commit.SenderEpoch == transportPeerEpoch &&
                commit.IssuerEpoch == authorEpoch &&
                commit.SenderPeer.AsSpan().SequenceEqual(sender) &&
                commit.BatchId.AsSpan().SequenceEqual(batch));
        }
    }

    /// <summary>
    /// Check pending FLD source/relay work for a ChangeId. Unsealed WAL is a
    /// conservative global blocker because it may replay any operation after restart.
    /// </summary>
    public bool HasPendingChangeIdReference(ReadOnlySpan<byte> changeId)
    {
        if (changeId.Length != 16)
            throw new ArgumentException("ChangeId must be 16 bytes.", nameof(changeId));
        byte[] id = changeId.ToArray();
        lock (_commitLock)
        {
            EnsureHealthy();
            if (_dirty.Values.Any(value => value.ChangeId.AsSpan().SequenceEqual(id)) ||
                _pendingFlushedCommitSeq > _lastSealedCommitSeq)
                return true;
            bool MatchesChangeId(in OutboxEntryView entry)
                => entry.ChangeId.SequenceEqual(id);
            if (AnyOutboxEntry(MatchesChangeId))
                return true;
            return AnyApplyJournal(commit =>
                commit.Fields.Any(field => field.ChangeId.AsSpan().SequenceEqual(id)) ||
                commit.RelayFields.Any(field => field.ChangeId.AsSpan().SequenceEqual(id)));
        }
    }

    /// <summary>
    /// Check pending FLD source/relay work for a RecordId or tombstone ChangeId.
    /// Any unsealed WAL prefix blocks conservatively because recovery may replay it.
    /// </summary>
    public bool HasPendingRecordReference(
        ReadOnlySpan<byte> recordId, ReadOnlySpan<byte> tombstoneChangeId)
    {
        if (recordId.Length != 16 || tombstoneChangeId.Length != 16)
            throw new ArgumentException("RecordId and ChangeId must be 16 bytes.");
        byte[] record = recordId.ToArray();
        byte[] tombstone = tombstoneChangeId.ToArray();
        ulong recordLo = BinaryPrimitives.ReadUInt64LittleEndian(record);
        ulong recordHi = BinaryPrimitives.ReadUInt64LittleEndian(record.AsSpan(8));
        lock (_commitLock)
        {
            EnsureHealthy();
            if (_dirty.Keys.Any(key => key.RecLo == recordLo && key.RecHi == recordHi) ||
                _pendingFlushedCommitSeq > _lastSealedCommitSeq)
                return true;
            bool MatchesRecord(in OutboxEntryView entry)
                => entry.RecordId.SequenceEqual(record) ||
                   entry.ChangeId.SequenceEqual(tombstone);
            if (AnyOutboxEntry(MatchesRecord))
                return true;
            return AnyApplyJournal(commit =>
                commit.Fields.Any(field =>
                    field.RecordId.AsSpan().SequenceEqual(record) ||
                    field.ChangeId.AsSpan().SequenceEqual(tombstone)) ||
                commit.RelayFields.Any(field =>
                    field.RecordId.AsSpan().SequenceEqual(record) ||
                    field.ChangeId.AsSpan().SequenceEqual(tombstone)));
        }
    }

    private bool AnyOutboxEntry(OutboxEntryVisitor predicate)
    {
        ulong cursor = _outbox.AckedThrough;
        const int pageSize = 512;
        while (true)
        {
            bool found = false;
            int visited = _outbox.VisitUnackedBatch(cursor, pageSize, Visit);
            if (found)
                return true;
            if (visited < pageSize)
                return false;

            bool Visit(in OutboxEntryView entry)
            {
                cursor = entry.OutboxSeq;
                if (predicate(in entry))
                {
                    found = true;
                    return false;
                }
                return true;
            }
        }
    }

    private bool AnyApplyJournal(Func<WalApplyCommit, bool> predicate)
    {
        ulong cursor = 0;
        const int pageSize = 256;
        while (true)
        {
            IReadOnlyList<ApplyJournalRecord> page = _applyJournal.ReadBatches(cursor, pageSize);
            foreach (ApplyJournalRecord record in page)
            {
                cursor = record.JournalSeq;
                if (predicate(record.Commit))
                    return true;
            }
            if (page.Count < pageSize)
                return false;
        }
    }

    public void RotateWal()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        lock (_commitLock)
        {
            EnsureHealthy();
            _wal.Rotate();
        }
    }

    private void EnsureHealthy()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (_faulted)
            throw new InvalidOperationException("Database requires reopen after a failed durable operation.");
    }

    private static void WriteRunAtomically(string path, byte[] runBytes)
    {
        string tempPath = path + ".tmp";
        string directory = Path.GetDirectoryName(path)!;
        StorageDirectorySync.CreateDirectoryDurable(directory);

        using (var fs = new FileStream(
            tempPath, FileMode.Create, FileAccess.Write, FileShare.None,
            bufferSize: 64 * 1024, FileOptions.WriteThrough))
        {
            fs.Write(runBytes);
            fs.Flush(flushToDisk: true);
        }

        File.Move(tempPath, path, overwrite: true);
        // The compatibility mirror must be durable before the FLD manifest or
        // FULL_REPLACE stage cursor can publish a reference to it.
        StorageDirectorySync.Flush(directory);
    }

    public void Dispose()
    {
        lock (_commitLock)
        {
            if (_disposed) return;
            if (_manifestDirty)
            {
                try
                {
                    _manifestStore.Update(m => CloneManifest(m, lastFlushed: _pendingFlushedCommitSeq));
                    _manifestDirty = false;
                }
                catch { /* best-effort on dispose */ }
            }
            DisposeProviderSnapshotsUnderLock();
            _disposed = true;
            try
            {
                _applyJournal.Dispose();
            }
            finally
            {
                try
                {
                    _outbox.Dispose();
                }
                finally
                {
                    try
                    {
                        _wal.Dispose();
                    }
                    finally
                    {
                        try { _runStorage.Dispose(); }
                        finally { _storageLease.Dispose(); }
                    }
                }
            }
        }
    }
}
