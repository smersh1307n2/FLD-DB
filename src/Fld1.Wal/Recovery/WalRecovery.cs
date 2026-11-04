using System.Buffers.Binary;
using Fld1.Core.Hash;
using Fld1.Wal.Format;

namespace Fld1.Wal.Recovery;

public enum WalRecoveryStatus
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

public sealed class WalRecoveredRecord
{
    public required ulong CommitSeq { get; init; }
    public required byte Kind { get; init; }
    public required byte Flags { get; init; }
    public required byte[] Payload { get; init; }
    public required long FileOffset { get; init; }
}

public sealed class WalRecoveryResult
{
    public required WalRecoveryStatus Status { get; init; }
    public string? Detail { get; init; }
    public List<WalRecoveredRecord> Records { get; init; } = new();
    /// <summary>Valid prefix end offset (header + complete records). Safe to truncate here on torn tail.</summary>
    public long ValidEndOffset { get; init; }
    public ulong? LastCommitSeq { get; init; }
}

/// <summary>
/// WAL recovery algorithm. Spec §5.8.
/// EnvelopeCrc first; full CRC-invalid never auto-truncate; torn tail truncates.
/// </summary>
public static class WalRecovery
{
    public static WalRecoveryResult Recover(string path)
    {
        using var fs = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
        return Recover(fs);
    }

    public static WalRecoveryResult Recover(Stream stream)
    {
        if (stream.Length < WalHeader.Size)
            return Fail(WalRecoveryStatus.CorruptHeader, "file shorter than header", 0);

        var header = new byte[WalHeader.Size];
        stream.Position = 0;
        stream.ReadExactly(header);

        if (!WalHeader.CheckMagic(header))
            return Fail(WalRecoveryStatus.CorruptHeader, "bad magic", 0);
        if (!WalHeader.VerifyCrc(header))
            return Fail(WalRecoveryStatus.CorruptHeader, "header crc", 0);
        if (WalHeader.ReadVersion(header) != WalConstants.WalVersion)
            return Fail(WalRecoveryStatus.CorruptHeader, "version", 0);

        var records = new List<WalRecoveredRecord>();
        long pos = WalHeader.Size;
        long fileSize = stream.Length;
        ulong? prevSeq = null;

        while (pos < fileSize)
        {
            if (fileSize - pos < 24)
            {
                // torn: not enough for min record → truncate at pos
                return new WalRecoveryResult
                {
                    Status = WalRecoveryStatus.TruncatedTorn,
                    Detail = "short tail",
                    Records = records,
                    ValidEndOffset = pos,
                    LastCommitSeq = prevSeq
                };
            }

            var env = new byte[WalEnvelope.Size];
            stream.Position = pos;
            stream.ReadExactly(env);

            if (!WalEnvelope.VerifyEnvelopeCrc(env))
            {
                // Could be torn overwrite of envelope or corruption
                // Spec: EnvelopeCrc invalid → STOP (not truncate if we can't tell)
                // For recovery of torn at end: if this is the last incomplete attempt, treat as torn
                if (pos + 24 > fileSize)
                {
                    return new WalRecoveryResult
                    {
                        Status = WalRecoveryStatus.TruncatedTorn,
                        Detail = "envelope crc + short",
                        Records = records,
                        ValidEndOffset = pos,
                        LastCommitSeq = prevSeq
                    };
                }
                return Fail(WalRecoveryStatus.CorruptEnvelope, $"pos={pos}", pos, records, prevSeq);
            }

            byte kind = WalEnvelope.ReadKind(env);
            if (kind is not (WalConstants.KindGroupCommit or WalConstants.KindNsCreate
                or WalConstants.KindSnapshotInstall))
                return Fail(WalRecoveryStatus.UnknownKind, $"kind={kind} pos={pos}", pos, records, prevSeq);

            uint length = WalEnvelope.ReadLength(env);
            if (length > WalConstants.MaxPayloadLen)
                return Fail(WalRecoveryStatus.CorruptEnvelope, $"len={length}", pos, records, prevSeq);

            int rawSize = WalEnvelope.RawRecordSize(length);
            int recordSize = WalEnvelope.AlignedRecordSize(length);

            if (pos + recordSize > fileSize)
            {
                // torn — physical bytes incomplete
                return new WalRecoveryResult
                {
                    Status = WalRecoveryStatus.TruncatedTorn,
                    Detail = "incomplete record",
                    Records = records,
                    ValidEndOffset = pos,
                    LastCommitSeq = prevSeq
                };
            }

            var payload = new byte[length];
            stream.Position = pos + WalEnvelope.Size;
            if (length > 0) stream.ReadExactly(payload);

            var crcBuf = new byte[4];
            stream.ReadExactly(crcBuf);
            uint storedCrc = BinaryPrimitives.ReadUInt32LittleEndian(crcBuf);

            // Verify RecordCrc = CRC32C(Envelope[0..16) ‖ Payload)
            var crcInput = new byte[16 + length];
            env.AsSpan(0, 16).CopyTo(crcInput);
            payload.CopyTo(crcInput.AsSpan(16));
            uint computed = Hashes.Crc32C(crcInput);
            if (computed != storedCrc)
            {
                bool isLast = pos + recordSize >= fileSize;
                return Fail(
                    isLast ? WalRecoveryStatus.CorruptTailAmbiguous : WalRecoveryStatus.CorruptMiddle,
                    $"record crc pos={pos}",
                    pos, records, prevSeq);
            }

            // Padding must be zero
            int padLen = recordSize - rawSize;
            if (padLen > 0)
            {
                var pad = new byte[padLen];
                stream.ReadExactly(pad);
                for (int i = 0; i < pad.Length; i++)
                {
                    if (pad[i] != 0)
                        return Fail(WalRecoveryStatus.CorruptPadding, $"pos={pos}", pos, records, prevSeq);
                }
            }

            ulong commitSeq = WalEnvelope.ReadCommitSeq(env);
            if (prevSeq.HasValue && commitSeq <= prevSeq.Value)
                return Fail(WalRecoveryStatus.OrderViolation, $"seq={commitSeq} prev={prevSeq}", pos, records, prevSeq);

            records.Add(new WalRecoveredRecord
            {
                CommitSeq = commitSeq,
                Kind = kind,
                Flags = env[WalEnvelope.OffFlags],
                Payload = payload,
                FileOffset = pos
            });

            prevSeq = commitSeq;
            pos += recordSize;
        }

        return new WalRecoveryResult
        {
            Status = WalRecoveryStatus.Ok,
            Records = records,
            ValidEndOffset = pos,
            LastCommitSeq = prevSeq
        };
    }

    private static WalRecoveryResult Fail(
        WalRecoveryStatus s, string detail, long validEnd,
        List<WalRecoveredRecord>? records = null, ulong? lastSeq = null)
        => new()
        {
            Status = s,
            Detail = detail,
            Records = records ?? new(),
            ValidEndOffset = validEnd,
            LastCommitSeq = lastSeq
        };
}
