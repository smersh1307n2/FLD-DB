namespace Fld1.Storage.Integrity;

/// <summary>
/// Domain-neutral CRC implementations for the Storage file format.
/// The implementation is intentionally BCL-only and accepts segmented input
/// without allocating a concatenated payload buffer.
/// </summary>
internal static class StorageChecksums
{
    public static uint Crc32C(ReadOnlySpan<byte> data) => ~Append(uint.MaxValue, data, 0x82F63B78u);

    public static uint Crc32C(ReadOnlySpan<byte> first, ReadOnlySpan<byte> second)
        => ~Append(Append(uint.MaxValue, first, 0x82F63B78u), second, 0x82F63B78u);

    public static uint Crc32Ieee(ReadOnlySpan<byte> data) => ~Append(uint.MaxValue, data, 0xEDB88320u);

    public static uint Crc32Ieee(ReadOnlySpan<byte> first, ReadOnlySpan<byte> second)
        => ~Append(Append(uint.MaxValue, first, 0xEDB88320u), second, 0xEDB88320u);

    public static bool IsCrc32COrLegacy(ReadOnlySpan<byte> data, uint stored)
        => stored == Crc32C(data) || stored == Crc32Ieee(data);

    public static bool IsCrc32COrLegacy(ReadOnlySpan<byte> first, ReadOnlySpan<byte> second, uint stored)
        => stored == Crc32C(first, second) || stored == Crc32Ieee(first, second);

    private static uint Append(uint crc, ReadOnlySpan<byte> data, uint polynomial)
    {
        foreach (byte value in data)
        {
            crc ^= value;
            for (int bit = 0; bit < 8; bit++)
                crc = (crc >> 1) ^ ((crc & 1) == 0 ? 0u : polynomial);
        }

        return crc;
    }
}