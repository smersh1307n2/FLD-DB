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
    private bool _faulted;

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

        ulong? expectedNextSeq = null;
        Span<byte> segmentHeader = stackalloc byte[WalHeader.Size];
        long lastValidEnd = WalHeader.Size;
        string lastPath = SegmentPath(walDir, namespaceId, segs[^1]);
        int lastSeg = segs[^1];

        for (int index = 0; index < segs.Count; index++)
        {
            int segment = segs[index];
            if (index > 0 && segment != segs[index - 1] + 1)
                throw new InvalidDataException(
                    $"WAL segment gap: {segs[index - 1]} -> {segment}");

            string path = SegmentPath(walDir, namespaceId, segment);
            ulong firstCommitSeq;
            using (var headerStream = new FileStream(
                path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
            {
                if (headerStream.Length < WalHeader.Size)
                    throw new InvalidDataException($"WAL {path}: truncated header.");

                headerStream.ReadExactly(segmentHeader);

                if (!WalHeader.CheckMagic(segmentHeader)
                    || !WalHeader.VerifyCrc(segmentHeader)
                    || WalHeader.ReadVersion(segmentHeader) != WalConstants.WalVersion)
                    throw new InvalidDataException($"WAL {path}: invalid header.");
                if (WalHeader.ReadNamespaceId(segmentHeader) != namespaceId)
                    throw new InvalidDataException($"WAL {path}: namespace mismatch.");
                if (WalHeader.ReadFlags(segmentHeader) != 0)
                    throw new InvalidDataException($"WAL {path}: reserved flags are non-zero.");

                firstCommitSeq = WalHeader.ReadFirstCommitSeq(segmentHeader);
                if (firstCommitSeq == 0)
                    throw new InvalidDataException($"WAL {path}: FirstCommitSeq is zero.");
                if (expectedNextSeq.HasValue && firstCommitSeq != expectedNextSeq.Value)
                    throw new InvalidDataException(
                        $"WAL {path}: FirstCommitSeq {firstCommitSeq}, expected {expectedNextSeq.Value}.");

                expectedNextSeq ??= firstCommitSeq;
            }

            WalRecoveryResult recovered = WalRecovery.Recover(path);
            if (recovered.Status == WalRecoveryStatus.TruncatedTorn && index != segs.Count - 1)
                throw new InvalidDataException($"WAL {path}: torn tail in a non-final segment.");
            if (recovered.Status is not (WalRecoveryStatus.Ok or WalRecoveryStatus.TruncatedTorn))
                throw new InvalidDataException($"WAL {path}: {recovered.Status} {recovered.Detail}");

            foreach (var record in recovered.Records)
            {
                if (record.CommitSeq != expectedNextSeq!.Value)
                    throw new InvalidDataException(
                        $"WAL {path}: CommitSeq {record.CommitSeq}, expected {expectedNextSeq.Value}.");
                expectedNextSeq = checked(expectedNextSeq.Value + 1);
            }

            if (recovered.Status == WalRecoveryStatus.TruncatedTorn)
            {
                using var writable = new FileStream(path, FileMode.Open, FileAccess.Write, FileShare.Read);
                writable.SetLength(recovered.ValidEndOffset);
                writable.Flush(flushToDisk: true);
            }

            allRecords.AddRange(recovered.Records);
            lastValidEnd = recovered.ValidEndOffset;
            lastPath = path;
            lastSeg = segment;
        }

        ulong nextSeq = expectedNextSeq!.Value;
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
            EnsureUsable();
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
            EnsureUsable();
            return _active.Flush();
        }
    }

    /// <summary>Force rotation (fsync active first, then new empty segment).</summary>
    public void Rotate()
    {
        lock (_lock)
        {
            EnsureUsable();
            RotateUnderLock();
        }
    }

    private void RotateUnderLock()
    {
        ulong nextSeq = _active.NextCommitSeq;
        _active.Flush();

        int newSegment = checked(_activeSeg + 1);
        string finalPath = SegmentPath(_walDir, _namespaceId, newSegment);
        string tempPath = finalPath + ".tmp";
        if (File.Exists(finalPath))
            throw new IOException($"WAL segment already exists: {finalPath}");
        if (File.Exists(tempPath))
            File.Delete(tempPath); // orphaned temp from an interrupted previous rotation

        bool published = false;
        try
        {
            // New segment contents and header become durable before the segment is published.
            using (WalWriter.Create(tempPath, _namespaceId, firstCommitSeq: nextSeq))
            {
            }

            File.Move(tempPath, finalPath);
            published = true;
            TryFsyncDirectory(_walDir);

            var nextWriter = WalWriter.OpenForAppend(
                finalPath, _namespaceId, WalHeader.Size, nextSeq);

            WalWriter previous = _active;
            _active = nextWriter;
            _activeSeg = newSegment;
            previous.StopAccepting();
            previous.Dispose();
        }
        catch
        {
            if (published)
                _faulted = true; // segment set changed; reopen performs authoritative recovery
            else if (File.Exists(tempPath))
            {
                try { File.Delete(tempPath); } catch { /* best effort */ }
            }
            throw;
        }
    }

    /// <summary>
    /// Delete sealed segments whose max recovered CommitSeq ≤ sealedThroughSeq.
    /// Keeps active segment. Spec: only drop fully covered history.
    /// </summary>
    public int TruncateSealedSegments(ulong sealedThroughSeq)
    {
        lock (_lock)
        {
            EnsureUsable();
            var segments = ListSegments(_walDir, _namespaceId);
            int removed = 0;
            Span<byte> segmentHeader = stackalloc byte[WalHeader.Size];

            // Only delete a contiguous sealed prefix. Leaving a hole between retained
            // segment numbers would make recovery order ambiguous.
            foreach (int segment in segments)
            {
                if (segment >= _activeSeg)
                    break;

                string path = SegmentPath(_walDir, _namespaceId, segment);
                WalRecoveryResult recovered = WalRecovery.Recover(path);
                if (recovered.Status is not (WalRecoveryStatus.Ok or WalRecoveryStatus.TruncatedTorn))
                    break;

                ulong firstSeq;
                using (var fs = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
                {
                    fs.ReadExactly(segmentHeader);
                    firstSeq = WalHeader.ReadFirstCommitSeq(segmentHeader);
                }

                if (firstSeq == 0)
                    break;

                ulong maxSeq = recovered.LastCommitSeq ?? (firstSeq - 1);
                if (maxSeq > sealedThroughSeq)
                    break;

                try
                {
                    File.Delete(path);
                    removed++;
                }
                catch
                {
                    break;
                }
            }

            if (removed > 0)
                TryFsyncDirectory(_walDir);

            return removed;
        }
    }

    private void EnsureUsable()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (_faulted)
            throw new InvalidOperationException("WAL manager requires reopen after a failed rotation.");
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
        lock (_lock)
        {
            if (_disposed) return;
            _disposed = true;
            _active.Dispose();
        }
    }
}
