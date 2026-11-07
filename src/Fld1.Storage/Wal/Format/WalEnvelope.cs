using System.Buffers.Binary;
using Fld1.Storage.Integrity;

namespace Fld1.Storage.Wal.Format;

/// <summary>Generic WAL record envelope. Payload and non-zero kind are opaque to Storage.</summary>
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

    public static void Write(Span<byte> dest, ulong commitSeq, uint length, byte kind, byte flags = 0)
    {
        if (dest.Length < Size) throw new ArgumentException("envelope too small");
        if (kind == 0) throw new ArgumentOutOfRangeException(nameof(kind));
        dest.Clear();
        BinaryPrimitives.WriteUInt64LittleEndian(dest.Slice(OffCommitSeq), commitSeq);
        BinaryPrimitives.WriteUInt32LittleEndian(dest.Slice(OffLength), length);
        dest[OffKind] = kind;
        dest[OffFlags] = flags;
        uint crc = StorageChecksums.Crc32C(dest.Slice(0, OffEnvelopeCrc));
        BinaryPrimitives.WriteUInt32LittleEndian(dest.Slice(OffEnvelopeCrc), crc);
    }

    public static ulong ReadCommitSeq(ReadOnlySpan<byte> e) => BinaryPrimitives.ReadUInt64LittleEndian(e.Slice(OffCommitSeq));
    public static uint ReadLength(ReadOnlySpan<byte> e) => BinaryPrimitives.ReadUInt32LittleEndian(e.Slice(OffLength));
    public static byte ReadKind(ReadOnlySpan<byte> e) => e[OffKind];
    public static bool VerifyEnvelopeCrc(ReadOnlySpan<byte> e)
    {
        if (e.Length < Size) return false;
        uint actual = BinaryPrimitives.ReadUInt32LittleEndian(e.Slice(OffEnvelopeCrc));
        return StorageChecksums.IsCrc32COrLegacy(e.Slice(0, OffEnvelopeCrc), actual);
    }
    public static int RawRecordSize(uint payloadLen) => 24 + (int)payloadLen;
    public static int AlignedRecordSize(uint payloadLen)
    {
        int raw = RawRecordSize(payloadLen);
        return (raw + 7) & ~7;
    }
}