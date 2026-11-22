using System.Buffers.Binary;
using Fld1.Storage.Integrity;
using Fld1.Storage.Wal.Format;

namespace Fld1.Storage.Wal.Recovery;

internal enum WalRecoveryStatus
{
    Ok = 0,
    CorruptHeader,
    CorruptEnvelope,
    UnknownKind,
    CorruptPadding,
    OrderViolation,
    CorruptMiddle,
    CorruptTailAmbiguous,
    TruncatedTorn,
}

internal sealed class WalRecoveredRecord
{
    public required ulong CommitSeq { get; init; }
    public required byte Kind { get; init; }
    public required byte Flags { get; init; }
    public required byte[] Payload { get; init; }
    public required long FileOffset { get; init; }
}

internal sealed class WalRecoveryResult
{
    public required WalRecoveryStatus Status { get; init; }
    public string? Detail { get; init; }
    public List<WalRecoveredRecord> Records { get; init; } = new();
    public int RecordCount { get; init; }
    /// <summary>Valid prefix end offset (header + complete records). Safe to truncate here on torn tail.</summary>
    public long ValidEndOffset { get; init; }
    public ulong? LastCommitSeq { get; init; }
}

internal sealed record WalRecoverySegment(
    string Path,
    ulong FirstCommitSeq,
    int RecordCount,
    ulong? LastCommitSeq,
    long ValidEndOffset);

/// <summary>
/// A two-pass, bounded-memory recovery plan. Open validates every segment and records
/// only its small range summary; Replay decodes one WAL record at a time. The visitor
/// must consume each payload synchronously and must not retain it.
/// </summary>
internal sealed class WalRecoveryPlan
{
    private readonly WalRecoverySegment[] _segments;

    public ulong NextCommitSeq { get; }
    public bool HasRecords => _segments.Any(segment => segment.RecordCount != 0);

    internal WalRecoveryPlan(WalRecoverySegment[] segments, ulong nextCommitSeq)
    {
        _segments = segments ?? throw new ArgumentNullException(nameof(segments));
        if (nextCommitSeq == 0) throw new ArgumentOutOfRangeException(nameof(nextCommitSeq));
        NextCommitSeq = nextCommitSeq;
    }

    public void Replay(Action<WalRecoveredRecord> visitor)
    {
        ArgumentNullException.ThrowIfNull(visitor);
        ulong? expectedGlobalNext = null;

        foreach (WalRecoverySegment segment in _segments)
        {
            if (expectedGlobalNext.HasValue && segment.FirstCommitSeq != expectedGlobalNext.Value)
                throw new InvalidDataException(
                    $"WAL recovery plan segment gap: expected FirstCommitSeq {expectedGlobalNext.Value}, found {segment.FirstCommitSeq}.");

            expectedGlobalNext ??= segment.FirstCommitSeq;
            ulong segmentNext = segment.FirstCommitSeq;

            WalRecoveryResult scan = WalRecovery.RecoverStreaming(segment.Path, record =>
            {
                if (record.CommitSeq != segmentNext)
                    throw new InvalidDataException(
                        $"WAL replay sequence mismatch in '{segment.Path}': expected {segmentNext}, found {record.CommitSeq}.");
                segmentNext = checked(segmentNext + 1);
                visitor(record);
            });

            if (scan.Status != WalRecoveryStatus.Ok)
                throw new InvalidDataException(
                    $"WAL changed or became invalid between validation and replay: {segment.Path}: {scan.Status} {scan.Detail}");
            if (scan.RecordCount != segment.RecordCount ||
                scan.LastCommitSeq != segment.LastCommitSeq ||
                scan.ValidEndOffset != segment.ValidEndOffset)
                throw new InvalidDataException(
                    $"WAL segment changed between validation and replay: {segment.Path}");

            expectedGlobalNext = segmentNext;
        }

        if ((expectedGlobalNext ?? 1UL) != NextCommitSeq)
            throw new InvalidDataException("WAL recovery plan did not replay through its validated next CommitSeq.");
    }
}

/// <summary>
/// WAL recovery algorithm. EnvelopeCrc first; full CRC-invalid never auto-truncate; torn tail truncates.
/// The streaming path keeps only one decoded record payload alive at a time.
/// </summary>
internal static class WalRecovery
{
    public static WalRecoveryResult Recover(string path)
    {
        using var fs = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
        return Recover(fs);
    }

    public static WalRecoveryResult Recover(Stream stream)
    {
        ArgumentNullException.ThrowIfNull(stream);
        var records = new List<WalRecoveredRecord>();
        return RecoverCore(stream, records, visitor: null);
    }

    /// <summary>
    /// Validates a WAL segment and synchronously visits each complete record without
    /// retaining the decoded record collection. The callback must not retain Payload.
    /// </summary>
    internal static WalRecoveryResult RecoverStreaming(string path, Action<WalRecoveredRecord> visitor)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(path);
        ArgumentNullException.ThrowIfNull(visitor);
        using var fs = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
        return RecoverCore(fs, materialized: null, visitor);
    }

    private static WalRecoveryResult RecoverCore(
        Stream stream,
        List<WalRecoveredRecord>? materialized,
        Action<WalRecoveredRecord>? visitor)
    {
        if (stream.Length < WalHeader.Size)
            return Fail(WalRecoveryStatus.CorruptHeader, "file shorter than header", 0, materialized);

        var header = new byte[WalHeader.Size];
        stream.Position = 0;
        stream.ReadExactly(header);

        if (!WalHeader.CheckMagic(header))
            return Fail(WalRecoveryStatus.CorruptHeader, "bad magic", 0, materialized);
        if (!WalHeader.VerifyCrc(header))
            return Fail(WalRecoveryStatus.CorruptHeader, "header crc", 0, materialized);
        if (WalHeader.ReadVersion(header) != WalConstants.WalVersion)
            return Fail(WalRecoveryStatus.CorruptHeader, "version", 0, materialized);
        if (WalHeader.ReadFlags(header) != 0)
            return Fail(WalRecoveryStatus.CorruptHeader, "reserved flags", 0, materialized);
        if (WalHeader.ReadFirstCommitSeq(header) == 0)
            return Fail(WalRecoveryStatus.CorruptHeader, "FirstCommitSeq is zero", 0, materialized);
        for (int i = 0x1C; i < header.Length; i++)
        {
            if (header[i] != 0)
                return Fail(WalRecoveryStatus.CorruptHeader, $"reserved byte {i}", 0, materialized);
        }

        int recordCount = 0;
        long pos = WalHeader.Size;
        long fileSize = stream.Length;
        ulong? prevSeq = null;
        Span<byte> env = stackalloc byte[WalEnvelope.Size];
        Span<byte> crcBuf = stackalloc byte[4];
        Span<byte> padBuf = stackalloc byte[7];

        while (pos < fileSize)
        {
            if (fileSize - pos < 24)
                return Torn("short tail");

            stream.Position = pos;
            stream.ReadExactly(env);

            if (!WalEnvelope.VerifyEnvelopeCrc(env))
                return Fail(WalRecoveryStatus.CorruptEnvelope, $"pos={pos}", pos, materialized, prevSeq, recordCount);

            if (env[WalEnvelope.OffReserved] != 0 || env[WalEnvelope.OffPad] != 0)
                return Fail(WalRecoveryStatus.CorruptEnvelope,
                    $"reserved envelope byte pos={pos}", pos, materialized, prevSeq, recordCount);

            byte kind = WalEnvelope.ReadKind(env);
            if (kind == 0)
                return Fail(WalRecoveryStatus.UnknownKind, $"reserved kind=0 pos={pos}", pos, materialized, prevSeq, recordCount);

            uint length = WalEnvelope.ReadLength(env);
            if (length > WalConstants.MaxPayloadLen)
                return Fail(WalRecoveryStatus.CorruptEnvelope, $"len={length}", pos, materialized, prevSeq, recordCount);

            int rawSize = WalEnvelope.RawRecordSize(length);
            int recordSize = WalEnvelope.AlignedRecordSize(length);

            if (pos + recordSize > fileSize)
                return Torn("incomplete record");

            var payload = new byte[length];
            stream.Position = pos + WalEnvelope.Size;
            if (length > 0) stream.ReadExactly(payload);

            stream.ReadExactly(crcBuf);
            uint storedCrc = BinaryPrimitives.ReadUInt32LittleEndian(crcBuf);

            if (!StorageChecksums.IsCrc32COrLegacy(env.Slice(0, 16), payload, storedCrc))
            {
                bool isLast = pos + recordSize >= fileSize;
                return Fail(
                    isLast ? WalRecoveryStatus.CorruptTailAmbiguous : WalRecoveryStatus.CorruptMiddle,
                    $"record crc pos={pos}",
                    pos, materialized, prevSeq, recordCount);
            }

            int padLen = recordSize - rawSize;
            if (padLen > 0)
            {
                Span<byte> pad = padBuf.Slice(0, padLen);
                stream.ReadExactly(pad);
                for (int i = 0; i < pad.Length; i++)
                {
                    if (pad[i] != 0)
                        return Fail(WalRecoveryStatus.CorruptPadding, $"pos={pos}", pos, materialized, prevSeq, recordCount);
                }
            }

            ulong commitSeq = WalEnvelope.ReadCommitSeq(env);
            if (prevSeq.HasValue && commitSeq <= prevSeq.Value)
                return Fail(WalRecoveryStatus.OrderViolation, $"seq={commitSeq} prev={prevSeq}", pos,
                    materialized, prevSeq, recordCount);

            var recoveredRecord = new WalRecoveredRecord
            {
                CommitSeq = commitSeq,
                Kind = kind,
                Flags = env[WalEnvelope.OffFlags],
                Payload = payload,
                FileOffset = pos
            };
            materialized?.Add(recoveredRecord);
            visitor?.Invoke(recoveredRecord);
            recordCount++;

            prevSeq = commitSeq;
            pos += recordSize;
        }

        return new WalRecoveryResult
        {
            Status = WalRecoveryStatus.Ok,
            Records = materialized ?? new List<WalRecoveredRecord>(),
            RecordCount = recordCount,
            ValidEndOffset = pos,
            LastCommitSeq = prevSeq
        };

        WalRecoveryResult Torn(string detail)
            => new()
            {
                Status = WalRecoveryStatus.TruncatedTorn,
                Detail = detail,
                Records = materialized ?? new List<WalRecoveredRecord>(),
                RecordCount = recordCount,
                ValidEndOffset = pos,
                LastCommitSeq = prevSeq
            };
    }

    private static WalRecoveryResult Fail(
        WalRecoveryStatus status,
        string detail,
        long validEnd,
        List<WalRecoveredRecord>? records = null,
        ulong? lastSeq = null,
        int recordCount = 0)
        => new()
        {
            Status = status,
            Detail = detail,
            Records = records ?? new List<WalRecoveredRecord>(),
            RecordCount = recordCount,
            ValidEndOffset = validEnd,
            LastCommitSeq = lastSeq
        };
}
