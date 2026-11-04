using Fld1.Wal.Format;
using Fld1.Wal.Recovery;

namespace Fld1.Wal.IO;

/// <summary>
/// Multi-segment WAL for one namespace. Rotation: fsync active → new segment file.
/// Segment names: ns_{namespaceId}.{seg}.log
/// </summary>
public sealed class WalManager : IDisposable
{
    private readonly string _walDir;
    private readonly ulong _namespaceId;
    private readonly long _rotateAfterBytes;
    private readonly object _lock = new();
    private WalWriter _active;
    private int _activeSeg;
    private bool _disposed;

    public ulong NamespaceId => _namespaceId;
    public int ActiveSegment => _activeSeg;
    public ulong NextCommitSeq
    {
        get { lock (_lock) return _active.NextCommitSeq; }
    }
    public long ActiveLength
    {
        get { lock (_lock) return (long)_active.WriteOffset; }
    }

    private WalManager(string walDir, ulong namespaceId, long rotateAfterBytes, WalWriter active, int activeSeg)
    {
        _walDir = walDir;
        _namespaceId = namespaceId;
        _rotateAfterBytes = rotateAfterBytes;
        _active = active;
        _activeSeg = activeSeg;
    }

    public static string SegmentPath(string walDir, ulong namespaceId, int seg)
        => Path.Combine(walDir, $"ns_{namespaceId}.{seg}.log");

    /// <summary>
    /// Open or create WAL set. Recovers all segments in order; truncates torn tails.
    /// Returns manager + ordered recovered records across segments.
    /// </summary>
    public static (WalManager manager, List<WalRecoveredRecord> records) Open(
        string walDir,
        ulong namespaceId,
        long rotateAfterBytes = 64L * 1024 * 1024)
    {
        Directory.CreateDirectory(walDir);
        var segs = ListSegments(walDir, namespaceId);
        var allRecords = new List<WalRecoveredRecord>();

        if (segs.Count == 0)
        {
            string path = SegmentPath(walDir, namespaceId, 0);
            var w = WalWriter.Create(path, namespaceId, firstCommitSeq: 1);
            return (new WalManager(walDir, namespaceId, rotateAfterBytes, w, 0), allRecords);
        }

        ulong nextSeq = 1;
        long lastValidEnd = WalHeader.Size;
        string lastPath = SegmentPath(walDir, namespaceId, segs[^1]);
        int lastSeg = segs[^1];

        foreach (int seg in segs)
        {
            string path = SegmentPath(walDir, namespaceId, seg);
            var recovered = WalRecovery.Recover(path);
            if (recovered.Status is WalRecoveryStatus.CorruptHeader or WalRecoveryStatus.CorruptMiddle
                or WalRecoveryStatus.UnknownKind or WalRecoveryStatus.OrderViolation)
                throw new InvalidDataException($"WAL {path}: {recovered.Status} {recovered.Detail}");

            if (recovered.Status is WalRecoveryStatus.TruncatedTorn
                or WalRecoveryStatus.CorruptTailAmbiguous)
            {
                using var fs = new FileStream(path, FileMode.Open, FileAccess.Write, FileShare.Read);
                fs.SetLength(recovered.ValidEndOffset);
                fs.Flush(flushToDisk: true);
            }

            allRecords.AddRange(recovered.Records);
            if (recovered.LastCommitSeq is ulong s)
                nextSeq = s + 1;
            lastValidEnd = recovered.ValidEndOffset;
            lastPath = path;
            lastSeg = seg;
        }

        var writer = WalWriter.OpenForAppend(lastPath, namespaceId, (ulong)lastValidEnd, nextSeq);
        return (new WalManager(walDir, namespaceId, rotateAfterBytes, writer, lastSeg), allRecords);
    }

    private static List<int> ListSegments(string walDir, ulong namespaceId)
    {
        string prefix = $"ns_{namespaceId}.";
        const string suffix = ".log";
        var list = new List<int>();
        if (!Directory.Exists(walDir)) return list;
        foreach (var f in Directory.EnumerateFiles(walDir, $"ns_{namespaceId}.*.log"))
        {
            var name = Path.GetFileName(f);
            if (!name.StartsWith(prefix, StringComparison.Ordinal) || !name.EndsWith(suffix, StringComparison.Ordinal))
                continue;
            var mid = name.AsSpan(prefix.Length, name.Length - prefix.Length - suffix.Length);
            if (int.TryParse(mid, out int seg) && seg >= 0)
                list.Add(seg);
        }
        list.Sort();
        return list;
    }

    /// <summary>Append; rotates if active segment exceeds threshold after write.</summary>
    public ulong Append(byte kind, ReadOnlySpan<byte> payload, byte flags = 0)
    {
        lock (_lock)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            ulong seq = _active.Append(kind, payload, flags);
            if (_rotateAfterBytes > 0 && (long)_active.WriteOffset >= _rotateAfterBytes)
                RotateUnderLock();
            return seq;
        }
    }

    public ulong Flush()
    {
        lock (_lock)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            return _active.Flush();
        }
    }

    /// <summary>Force rotation (fsync active first, then new empty segment).</summary>
    public void Rotate()
    {
        lock (_lock)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            RotateUnderLock();
        }
    }

    private void RotateUnderLock()
    {
        ulong nextSeq = _active.NextCommitSeq;
        _active.StopAccepting();
        _active.Flush();
        _active.Dispose();

        int newSeg = _activeSeg + 1;
        string path = SegmentPath(_walDir, _namespaceId, newSeg);
        _active = WalWriter.Create(path, _namespaceId, firstCommitSeq: nextSeq);
        _activeSeg = newSeg;
    }

    /// <summary>
    /// Delete sealed segments whose max recovered CommitSeq ≤ sealedThroughSeq.
    /// Keeps active segment. Spec: only drop fully covered history.
    /// </summary>
    public int TruncateSealedSegments(ulong sealedThroughSeq)
    {
        lock (_lock)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            var segs = ListSegments(_walDir, _namespaceId);
            int removed = 0;
            foreach (int seg in segs)
            {
                if (seg >= _activeSeg) break;
                string path = SegmentPath(_walDir, _namespaceId, seg);
                var r = WalRecovery.Recover(path);
                ulong maxSeq = r.LastCommitSeq ?? 0;
                if (maxSeq <= sealedThroughSeq && r.Status is WalRecoveryStatus.Ok or WalRecoveryStatus.TruncatedTorn)
                {
                    try
                    {
                        File.Delete(path);
                        removed++;
                    }
                    catch { /* ignore */ }
                }
            }
            return removed;
        }
    }

    public void Dispose()
    {
        lock (_lock)
        {
            if (_disposed) return;
            _disposed = true;
            _active.Dispose();
        }
    }
}
