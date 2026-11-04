using System.Buffers.Binary;
using Fld1.Core.Hlc;
using Fld1.Core.Uuid;
using Fld1.Engine.Storage;
using Fld1.Manifest.Format;
using Fld1.Manifest.IO;
using Fld1.Run.Builder;
using Fld1.Run.Format;
using Fld1.Run.Reader;
using Fld1.Wal.Codec;
using Fld1.Wal.Format;
using Fld1.Wal.IO;
using Fld1.Wal.Recovery;

namespace Fld1.Engine.Db;

/// <summary>
/// Single-process FLD database (Engine path without SHM/IPC yet).
/// Write → mem overlay + WAL group commit; Checkpoint → immutable run + manifest.
/// Read: overlay first, then runs newest-first.
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
    private bool _disposed;

    public string DbDir => _dbDir;
    public uint NamespaceId => _namespaceId;
    public int OverlayCount => _overlay.Count;

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

        // Ensure ns registered
        if (!manifest.Namespaces.Contains(namespaceId))
        {
            manifest = manifestStore.Update(m => new ManifestDocument
            {
                Generation = m.Generation,
                LastFlushedCommitSeq = m.LastFlushedCommitSeq,
                CertifiedWatermark = m.CertifiedWatermark,
                Namespaces = m.Namespaces.Append(namespaceId).Distinct().OrderBy(x => x).ToList(),
                Runs = m.Runs,
                Retains = m.Retains,
                ResultClients = m.ResultClients,
                ProvenanceIndex = m.ProvenanceIndex,
                CoordinatorLeaseEpoch = m.CoordinatorLeaseEpoch,
                CoordinatorSessionId = m.CoordinatorSessionId
            });
        }

        string walPath = Path.Combine(dbDir, "wal", $"ns_{namespaceId}.0.log");
        WalWriter wal;
        if (!File.Exists(walPath))
        {
            wal = WalWriter.Create(walPath, namespaceId, firstCommitSeq: 1);
        }
        else
        {
            var recovered = WalRecovery.Recover(walPath);
            if (recovered.Status is WalRecoveryStatus.CorruptHeader or WalRecoveryStatus.CorruptMiddle
                or WalRecoveryStatus.UnknownKind or WalRecoveryStatus.OrderViolation)
                throw new InvalidDataException($"WAL recovery failed: {recovered.Status} {recovered.Detail}");

            // Truncate torn tail if needed
            if (recovered.Status == WalRecoveryStatus.TruncatedTorn
                || recovered.Status == WalRecoveryStatus.CorruptTailAmbiguous)
            {
                using (var fs = new FileStream(walPath, FileMode.Open, FileAccess.Write, FileShare.Read))
                {
                    fs.SetLength(recovered.ValidEndOffset);
                    fs.Flush(flushToDisk: true);
                }
            }

            ulong nextSeq = (recovered.LastCommitSeq ?? 0) + 1;
            if (nextSeq == 0) nextSeq = 1;
            wal = WalWriter.OpenForAppend(walPath, namespaceId, (ulong)recovered.ValidEndOffset, nextSeq);
        }

        var db = new FldDatabase(dbDir, namespaceId, author, wal, manifestStore);
        db.LoadRunsFromManifest(manifest);
        return db;
    }

    private void LoadRunsFromManifest(ManifestDocument manifest)
    {
        foreach (var re in manifest.Runs.Where(r => r.Ns == _namespaceId).OrderBy(r => r.RunId))
        {
            var path = Path.Combine(_dbDir, "runs", $"{re.RunId}.fld1");
            if (File.Exists(path))
            {
                _runs.Add((re.RunId, path, null));
                if (re.RunId >= _nextRunId) _nextRunId = re.RunId + 1;
            }
        }
    }

    /// <summary>Local field write into overlay (not durable until Commit).</summary>
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
        _overlay.Put(key, new MemFieldValue
        {
            Value = value.ToArray(),
            Wall = hlc.Stamp.Wall,
            Logical = hlc.Stamp.Logical,
            AuthorNodeId = _localAuthor,
            KeyEpoch = keyEpoch,
            IsTombstone = isTombstone
        });
    }

    /// <summary>
    /// Durable group commit of current overlay snapshot into WAL.
    /// Does not clear overlay (still readable); Checkpoint seals to run.
    /// </summary>
    public ulong Commit()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        lock (_commitLock)
        {
            var snap = _overlay.Snapshot();
            if (snap.Count == 0)
                return 0;

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
                        CommitSeq = 0, // filled conceptually by WAL seq
                        ProvenanceIdx = 0xFFFFFFFF
                    }
                ],
                Provenance = []
            });

            ulong commitSeq = _wal.Append(WalConstants.KindGroupCommit, payload);
            _wal.Flush();

            // Patch result CommitSeq in-memory only; durable seq is WAL envelope
            _manifestStore.Update(m => new ManifestDocument
            {
                Generation = m.Generation,
                LastFlushedCommitSeq = commitSeq,
                CertifiedWatermark = m.CertifiedWatermark,
                Namespaces = m.Namespaces,
                Runs = m.Runs,
                Retains = m.Retains,
                ResultClients = m.ResultClients,
                ProvenanceIndex = m.ProvenanceIndex,
                CoordinatorLeaseEpoch = m.CoordinatorLeaseEpoch,
                CoordinatorSessionId = m.CoordinatorSessionId
            });

            return commitSeq;
        }
    }

    /// <summary>
    /// Seal overlay into an immutable FLD1 run, publish in manifest, clear overlay.
    /// </summary>
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
            // atomic write
            string tmp = path + ".tmp";
            File.WriteAllBytes(tmp, runBytes);
            using (var fs = new FileStream(tmp, FileMode.Open, FileAccess.Read, FileShare.Read))
                fs.Flush(flushToDisk: true);
            File.Move(tmp, path, overwrite: true);

            // bounds from first/last record in run — approximate from snap keys
            var recs = snap.Select(kv =>
            {
                var b = new byte[16];
                kv.Key.WriteRecordId(b);
                return b;
            }).OrderBy(b => b, Comparer<byte[]>.Create((x, y) =>
                Fld1.Core.Util.ByteUtil.CompareUnsigned16(x, y))).ToList();

            var minRec = recs[0];
            var maxRec = recs[^1];
            ulong seqHi = _manifestStore.Load().LastFlushedCommitSeq;

            _manifestStore.Update(m =>
            {
                var runs = m.Runs.ToList();
                runs.Add(new RunEntry
                {
                    RunId = runId,
                    Level = 0,
                    Ns = _namespaceId,
                    MinRec = minRec,
                    MaxRec = maxRec,
                    SeqLo = 1,
                    SeqHi = seqHi
                });
                return new ManifestDocument
                {
                    Generation = m.Generation,
                    LastFlushedCommitSeq = m.LastFlushedCommitSeq,
                    CertifiedWatermark = m.CertifiedWatermark,
                    Namespaces = m.Namespaces,
                    Runs = runs,
                    Retains = m.Retains,
                    ResultClients = m.ResultClients,
                    ProvenanceIndex = m.ProvenanceIndex,
                    CoordinatorLeaseEpoch = m.CoordinatorLeaseEpoch,
                    CoordinatorSessionId = m.CoordinatorSessionId
                };
            });

            _runs.Add((runId, path, runBytes)); // cache for fast read
            _overlay.Clear();
            return runId;
        }
    }

    /// <summary>Point get: overlay then runs newest-first. Prefer zero-copy from cached run.</summary>
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

        // Newest run first
        for (int i = _runs.Count - 1; i >= 0; i--)
        {
            var (_, path, cached) = _runs[i];
            byte[] bytes = cached ?? File.ReadAllBytes(path);
            if (cached is null)
            {
                lock (_commitLock)
                {
                    // store cache
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

            var reader = new RunReader(bytes);
            if (reader.TryGet(recordId, fieldId, out value))
                return true;
        }

        return false;
    }

    /// <summary>Copy-out variant for callers that need owned arrays.</summary>
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

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        lock (_commitLock)
        {
            _wal.Dispose();
        }
    }
}
