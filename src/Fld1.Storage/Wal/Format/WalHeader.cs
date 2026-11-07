using System.Buffers.Binary;
using Fld1.Storage.Integrity;

namespace Fld1.Storage.Wal.Format;

/// <summary>Storage WAL file header — 64 bytes.</summary>
public static class WalHeader
{
    public const int Size = WalConstants.HeaderSize;
    public const int OffMagic = 0x00;
    public const int OffWalVersion = 0x04;
    public const int OffFlags = 0x06;
    public const int OffPartitionId = 0x08;
    public const int OffFirstCommitSeq = 0x10;
    public const int OffHeaderCrc32C = 0x18;

    public static bool CheckMagic(ReadOnlySpan<byte> h)
        => h.Length >= 4 && h[0] == (byte)'F' && h[1] == (byte)'L' && h[2] == (byte)'D' && h[3] == (byte)'W';

    public static void Write(Span<byte> dest, ulong partitionId, ulong firstCommitSeq)
    {
        if (dest.Length < Size) throw new ArgumentException("header buffer too small");
        dest.Clear();
        WalConstants.Magic.CopyTo(dest);
        BinaryPrimitives.WriteUInt16LittleEndian(dest.Slice(OffWalVersion), WalConstants.WalVersion);
        BinaryPrimitives.WriteUInt16LittleEndian(dest.Slice(OffFlags), 0);
        BinaryPrimitives.WriteUInt64LittleEndian(dest.Slice(OffPartitionId), partitionId);
        BinaryPrimitives.WriteUInt64LittleEndian(dest.Slice(OffFirstCommitSeq), firstCommitSeq);
        uint crc = StorageChecksums.Crc32C(dest.Slice(0, OffHeaderCrc32C));
        BinaryPrimitives.WriteUInt32LittleEndian(dest.Slice(OffHeaderCrc32C), crc);
    }

    public static ushort ReadVersion(ReadOnlySpan<byte> h) => BinaryPrimitives.ReadUInt16LittleEndian(h.Slice(OffWalVersion));
    public static ushort ReadFlags(ReadOnlySpan<byte> h) => BinaryPrimitives.ReadUInt16LittleEndian(h.Slice(OffFlags));
    public static ulong ReadPartitionId(ReadOnlySpan<byte> h) => BinaryPrimitives.ReadUInt64LittleEndian(h.Slice(OffPartitionId));
    public static ulong ReadFirstCommitSeq(ReadOnlySpan<byte> h) => BinaryPrimitives.ReadUInt64LittleEndian(h.Slice(OffFirstCommitSeq));
    public static uint ReadHeaderCrc(ReadOnlySpan<byte> h) => BinaryPrimitives.ReadUInt32LittleEndian(h.Slice(OffHeaderCrc32C));
    public static bool VerifyCrc(ReadOnlySpan<byte> h)
        => h.Length >= Size && StorageChecksums.IsCrc32COrLegacy(h.Slice(0, OffHeaderCrc32C), ReadHeaderCrc(h));
}