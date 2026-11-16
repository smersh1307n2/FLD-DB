namespace Fld1.Storage.Integrity;

/// <summary>
/// Domain-neutral CRC implementations for the Storage file format.
/// Uses table-driven CRC to avoid the former eight-bit-steps-per-byte hot path
/// while remaining BCL-only and supporting segmented input without concatenation.
/// </summary>
internal static class StorageChecksums
{
    private const uint Crc32CPolynomial = 0x82F63B78u;
    private const uint Crc32IeeePolynomial = 0xEDB88320u;
    private static readonly uint[] Crc32CTable = CreateTable(Crc32CPolynomial);
    private static readonly uint[] Crc32IeeeTable = CreateTable(Crc32IeeePolynomial);

    public static uint Crc32C(ReadOnlySpan<byte> data)
        => ~Append(uint.MaxValue, data, Crc32CTable);

    public static uint Crc32C(ReadOnlySpan<byte> first, ReadOnlySpan<byte> second)
        => ~Append(Append(uint.MaxValue, first, Crc32CTable), second, Crc32CTable);

    public static uint Crc32Ieee(ReadOnlySpan<byte> data)
        => ~Append(uint.MaxValue, data, Crc32IeeeTable);

    public static uint Crc32Ieee(ReadOnlySpan<byte> first, ReadOnlySpan<byte> second)
        => ~Append(Append(uint.MaxValue, first, Crc32IeeeTable), second, Crc32IeeeTable);

    public static bool IsCrc32COrLegacy(ReadOnlySpan<byte> data, uint stored)
        => stored == Crc32C(data) || stored == Crc32Ieee(data);

    public static bool IsCrc32COrLegacy(ReadOnlySpan<byte> first, ReadOnlySpan<byte> second, uint stored)
        => stored == Crc32C(first, second) || stored == Crc32Ieee(first, second);

    private static uint Append(uint crc, ReadOnlySpan<byte> data, uint[] table)
    {
        foreach (byte value in data)
            crc = table[(byte)(crc ^ value)] ^ (crc >> 8);
        return crc;
    }

    private static uint[] CreateTable(uint polynomial)
    {
        var table = new uint[256];
        for (uint i = 0; i < table.Length; i++)
        {
            uint value = i;
            for (int bit = 0; bit < 8; bit++)
                value = (value >> 1) ^ ((value & 1) == 0 ? 0u : polynomial);
            table[i] = value;
        }
        return table;
    }
}
