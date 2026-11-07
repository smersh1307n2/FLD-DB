using System.IO.Hashing;

namespace Fld1.Storage.Integrity;

/// <summary>Storage-format checksums. No FLD Database or BIN-SYNC semantics.</summary>
internal static class StorageChecksums
{
    public static uint Crc32C(ReadOnlySpan<byte> data) => System.IO.Hashing.Crc32C.HashToUInt32(data);

    public static uint Crc32C(ReadOnlySpan<byte> first, ReadOnlySpan<byte> second)
    {
        var hash = new System.IO.Hashing.Crc32C();
        hash.Append(first);
        hash.Append(second);
        return hash.GetCurrentHashAsUInt32();
    }

    public static uint Crc32Ieee(ReadOnlySpan<byte> data) => Crc32.HashToUInt32(data);

    public static uint Crc32Ieee(ReadOnlySpan<byte> first, ReadOnlySpan<byte> second)
    {
        var hash = new Crc32();
        hash.Append(first);
        hash.Append(second);
        return hash.GetCurrentHashAsUInt32();
    }

    public static bool IsCrc32COrLegacy(ReadOnlySpan<byte> data, uint stored)
        => stored == Crc32C(data) || stored == Crc32Ieee(data);

    public static bool IsCrc32COrLegacy(ReadOnlySpan<byte> first, ReadOnlySpan<byte> second, uint stored)
        => stored == Crc32C(first, second) || stored == Crc32Ieee(first, second);
}