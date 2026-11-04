using Fld1.Core.Hlc;
using Fld1.Core.Uuid;
using Fld1.Engine.Storage;
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
    private readonly AtomicManifestStore _manifestStore;
    private readonly object _commitLock = new();
    private WalWriter _wal;
    private readonly List<(ulong runId, string path, byte[]? cached)> _runs = new();
    private ulong _nextRunId = 1;
    private ulong _clientRequestSeq;
    private readonly byte[] _clientInstanceId = new byte[16];
    private ulong _lastSealedCommitSeq;
    private bool _disposed;

    public string DbDir => _dbDir;
    public uint NamespaceId => _namespaceId;
    public int OverlayCount => _overlay.Count;
    public ulong LastSealedCommitSeq => _lastSealedCommitSeq;

    private FldDatabase(
        string dbDir,
        uint namespaceId,
        byte[] localAuthor,
        WalWriter wal,
        AtomicManifestStore manifestStore)
    {
        _dbDir = dbDir;
        _namespaceId = namespaceId;
        _localAuthor = localAuthor;
        _clock = new HlcClock(localAuthor);
        _wal = wal;
        _manifestStore = manifestStore;
        Uuid7.NewUuid7(_clientInstanceId);
    }

    public static FldDatabase Open(string dbDir, uint namespaceId = 0, byte[]? localAuthor = null)
    {
        if (namespaceId > 0xFFFF) throw new ArgumentOutOfRangeException(nameof(namespaceId));
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

        string walPath = Path.Combine(dbDir, "wal", $"ns_{namespaceId}.0.log");
        WalWriter wal;
        WalRecoveryResult recovered;
        if (!File.Exists(walPath))
        {
            wal = WalWriter.Create(walPath, namespaceId, firstCommitSeq: 1);
            recovered = new WalRecoveryResult
            {
                Status = WalRecoveryStatus.Ok,
                Records = [],
                ValidEndOffset = WalHeader.Size
            };
        }
        else
        {
            recovered = WalRecovery.Recover(walPath);
            if (recovered.Status is WalRecoveryStatus.CorruptHeader or WalRecoveryStatus.CorruptMiddle
                or WalRecoveryStatus.UnknownKind or WalRecoveryStatus.OrderViolation)
                throw new InvalidDataException($"WAL recovery failed: {recovered.Status} {recovered.Detail}");

            if (recovered.Status is WalRecoveryStatus.TruncatedTorn
                or WalRecoveryStatus.CorruptTailAmbiguous)
            {
                using var fs = new FileStream(walPath, FileMode.Open, FileAccess.Write, FileShare.Read);
                fs.SetLength(recovered.ValidEndOffset);
                fs.Flush(flushToDisk: true);
            }

            ulong nextSeq = (recovered.LastCommitSeq ?? 0) + 1;
            if (nextSeq == 0) nextSeq = 1;
            wal = WalWriter.OpenForAppend(walPath, namespaceId, (ulong)recovered.ValidEndOffset, nextSeq);
        }

        var db = new FldDatabase(dbDir, namespaceId, author, wal, manifestStore);
        db.LoadRunsFromManifest(manifest);
        db.ReplayWalIntoOverlay(recovered.Records);
        return db;
    }

    private void LoadRunsFromManifest(ManifestDocument manifest)
    {
        ulong sealedSeq = 0;
        foreach (var re in manifest.Runs.Where(r => r.Ns == _namespaceId).OrderBy(r => r.RunId))
        {
            var path = Path.Combine(_dbDir, "runs", $"{re.RunId}.fld1");
            if (File.Exists(path))
            {
                _runs.Add((re.RunId, path, null));
                if (re.RunId >= _nextRunId) _nextRunId = re.RunId + 1;
                if (re.SeqHi > sealedSeq) sealedSeq = re.SeqHi;
            }
        }
        _lastSealedCommitSeq = sealedSeq;
    }

    /// <summary>Apply durable WAL mutations with CommitSeq &gt; last sealed run into overlay.</summary>
    private void ReplayWalIntoOverlay(List<WalRecoveredRecord> records)
    {
        foreach (var rec in records)
        {
            if (rec.CommitSeq <= _lastSealedCommitSeq) continue;
            if (rec.Kind != WalConstants.KindGroupCommit) continue;
            var payload = GroupCommitCodec.Decode(rec.Payload);
            foreach (var mut in payload.Mutations)
            {
                if (mut.Ns != (ushort)_namespaceId && mut.Ns != _namespaceId) continue;
                foreach (var f in mut.Fields)
                {
                    var key = new MemFieldKey(f.RecordId, f.FieldId);
                    _overlay.Put(key, new MemFieldValue
                    {
                        Value = f.Value,
                        Wall = f.Wall,
                        Logical = f.Logical,
                        AuthorNodeId = f.AuthorNodeId,
                        KeyEpoch = f.KeyEpoch,
                        IsTombstone = f.OpKind == 2
                    });
                }
            }
        }
    }

    public void Put(
        ReadOnlySpan<byte> recordId,
        ushort fieldId,
        ReadOnlySpan<byte> value,
        ushort keyEpoch = 0,
        bool isTombstone = false)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (recordId.Length != 16) throw new ArgumentException("RecordId");
        var hlc = _clock.Now();
        var key = new MemFieldKey(recordId, fieldId);
        // Single alloc for value; author is shared local ref (immutable 16B)
        _overlay.Put(key, new MemFieldValue
        {
            Value = isTombstone ? Array.Empty<byte>() : value.ToArray(),
            Wall = hlc.Stamp.Wall,
            Logical = hlc.Stamp.Logical,
            AuthorNodeId = _localAuthor,
            KeyEpoch = keyEpoch,
            IsTombstone = isTombstone
        });
    }

    /// <summary>Durable group commit of current overlay into WAL. Overlay kept for reads.</summary>
    public ulong Commit()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        lock (_commitLock)
        {
            var snap = _overlay.Snapshot();
            if (snap.Count == 0) return 0;

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

            ulong commitSeq = _wal.Append(WalConstants.KindGroupCommit, payload);
            _wal.Flush();

            // Manifest watermark only (cheap); full client log later
            _manifestStore.Update(m => CloneManifest(m, lastFlushed: commitSeq));
            return commitSeq;
        }
    }

    public ulong Checkpoint()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        lock (_commitLock)
        {
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
            string tmp = path + ".tmp";
            File.WriteAllBytes(tmp, runBytes);
            using (var fs = new FileStream(tmp, FileMode.Open, FileAccess.Read, FileShare.Read))
                fs.Flush(flushToDisk: true);
            File.Move(tmp, path, overwrite: true);

            var recs = new List<byte[]>(snap.Count);
            foreach (var kv in snap)
            {
                var b = new byte[16];
                kv.Key.WriteRecordId(b);
                recs.Add(b);
            }
            recs.Sort((x, y) => Fld1.Core.Util.ByteUtil.CompareUnsigned16(x, y));

            ulong seqHi = _manifestStore.Load().LastFlushedCommitSeq;
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
                return CloneManifest(m, runs: runs, lastFlushed: m.LastFlushedCommitSeq);
            });

            _runs.Add((runId, path, runBytes));
            _lastSealedCommitSeq = seqHi;
            _overlay.Clear();
            return runId;
        }
    }

    public bool TryGet(ReadOnlySpan<byte> recordId, ushort fieldId, out ReadOnlySpan<byte> value)
    {
        value = default;
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (recordId.Length != 16) return false;

        var key = new MemFieldKey(recordId, fieldId);
        if (_overlay.TryGet(key, out var mem) && mem is not null)
        {
            if (mem.IsTombstone) return false;
            value = mem.Value;
            return true;
        }

        for (int i = _runs.Count - 1; i >= 0; i--)
        {
            var (_, path, cached) = _runs[i];
            byte[] bytes = cached ?? File.ReadAllBytes(path);
            if (cached is null)
            {
                lock (_commitLock)
                {
                    for (int j = 0; j < _runs.Count; j++)
                    {
                        if (_runs[j].path == path && _runs[j].cached is null)
                        {
                            _runs[j] = (_runs[j].runId, path, bytes);
                            break;
                        }
                    }
                }
            }

            if (new RunReader(bytes).TryGet(recordId, fieldId, out value))
                return true;
        }
        return false;
    }

    public bool TryGetBytes(ReadOnlySpan<byte> recordId, ushort fieldId, out byte[]? value)
    {
        if (TryGet(recordId, fieldId, out var span))
        {
            value = span.ToArray();
            return true;
        }
        value = null;
        return false;
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

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        lock (_commitLock) _wal.Dispose();
    }
}
