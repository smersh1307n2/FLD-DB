using System.Buffers.Binary;
using Fld1.Core.Hash;

namespace Fld1.Wal.Format;

/// <summary>
/// Record envelope 20 B. Spec §5.2.
/// EnvelopeCrc covers bytes[0..0x10). Recovery checks it first.
/// </summary>
public static class WalEnvelope
{
    public const int Size = WalConstants.EnvelopeSize;
    public const int OffCommitSeq = 0x00;
    public const int OffLength = 0x08;
    public const int OffKind = 0x0C;
    public const int OffReserved = 0x0D;
    public const int OffFlags = 0x0E;
    public const int OffPad = 0x0F;
    public const int OffEnvelopeCrc = 0x10;

    public static void Write(
        Span<byte> dest, ulong commitSeq, uint length, byte kind, byte flags = 0)
    {
        if (dest.Length < Size) throw new ArgumentException("envelope too small");
        dest.Clear();
        BinaryPrimitives.WriteUInt64LittleEndian(dest.Slice(OffCommitSeq), commitSeq);
        BinaryPrimitives.WriteUInt32LittleEndian(dest.Slice(OffLength), length);
        dest[OffKind] = kind;
        dest[OffReserved] = 0;
        dest[OffFlags] = flags;
        dest[OffPad] = 0;
        uint crc = Hashes.Crc32C(dest.Slice(0, OffEnvelopeCrc));
        BinaryPrimitives.WriteUInt32LittleEndian(dest.Slice(OffEnvelopeCrc), crc);
    }

    public static ulong ReadCommitSeq(ReadOnlySpan<byte> e)
        => BinaryPrimitives.ReadUInt64LittleEndian(e.Slice(OffCommitSeq));

    public static uint ReadLength(ReadOnlySpan<byte> e)
        => BinaryPrimitives.ReadUInt32LittleEndian(e.Slice(OffLength));

    public static byte ReadKind(ReadOnlySpan<byte> e) => e[OffKind];

    public static bool VerifyEnvelopeCrc(ReadOnlySpan<byte> e)
    {
        if (e.Length < Size) return false;
        uint actual = BinaryPrimitives.ReadUInt32LittleEndian(e.Slice(OffEnvelopeCrc));
        return Hashes.IsCrc32COrLegacy(e.Slice(0, OffEnvelopeCrc), actual);
    }

    /// <summary>RawRecordSize = 24 + Length; RecordSize = align8(Raw).</summary>
    public static int RawRecordSize(uint payloadLen) => 24 + (int)payloadLen;

    public static int AlignedRecordSize(uint payloadLen)
    {
        int raw = RawRecordSize(payloadLen);
        return (raw + 7) & ~7;
    }
}
