using System.Buffers.Binary;
using System.Runtime.Intrinsics.X86;

namespace Fld1.Storage.Integrity;

/// <summary>
/// Domain-neutral CRC implementations for the Storage file format.
/// CRC32C uses SSE4.2 when available and an allocation-free table fallback;
/// IEEE CRC remains table-driven. Segmented input is processed without concatenation.
/// </summary>
internal static class StorageChecksums
{
    private const uint Crc32CPolynomial = 0x82F63B78u;
    private const uint Crc32IeeePolynomial = 0xEDB88320u;
    private static readonly uint[] Crc32CTable = CreateTable(Crc32CPolynomial);
    private static readonly uint[] Crc32IeeeTable = CreateTable(Crc32IeeePolynomial);

    public static uint Crc32C(ReadOnlySpan<byte> data)
        => ~AppendCrc32C(uint.MaxValue, data);

    public static uint Crc32C(ReadOnlySpan<byte> first, ReadOnlySpan<byte> second)
        => ~AppendCrc32C(AppendCrc32C(uint.MaxValue, first), second);

    public static uint Crc32Ieee(ReadOnlySpan<byte> data)
        => ~AppendTable(uint.MaxValue, data, Crc32IeeeTable);

    public static uint Crc32Ieee(ReadOnlySpan<byte> first, ReadOnlySpan<byte> second)
        => ~AppendTable(AppendTable(uint.MaxValue, first, Crc32IeeeTable), second, Crc32IeeeTable);

    public static bool IsCrc32COrLegacy(ReadOnlySpan<byte> data, uint stored)
        => stored == Crc32C(data) || stored == Crc32Ieee(data);

    public static bool IsCrc32COrLegacy(ReadOnlySpan<byte> first, ReadOnlySpan<byte> second, uint stored)
        => stored == Crc32C(first, second) || stored == Crc32Ieee(first, second);

    private static uint AppendCrc32C(uint crc, ReadOnlySpan<byte> data)
    {
        if (!Sse42.IsSupported)
            return AppendTable(crc, data, Crc32CTable);

        int offset = 0;
        if (Sse42.X64.IsSupported)
        {
            ulong crc64 = crc;
            while (data.Length - offset >= sizeof(ulong))
            {
                ulong value = BinaryPrimitives.ReadUInt64LittleEndian(data.Slice(offset, sizeof(ulong)));
                crc64 = Sse42.X64.Crc32(crc64, value);
                offset += sizeof(ulong);
            }
            crc = (uint)crc64;
        }

        while (data.Length - offset >= sizeof(uint))
        {
            uint value = BinaryPrimitives.ReadUInt32LittleEndian(data.Slice(offset, sizeof(uint)));
            crc = Sse42.Crc32(crc, value);
            offset += sizeof(uint);
        }

        if (data.Length - offset >= sizeof(ushort))
        {
            ushort value = BinaryPrimitives.ReadUInt16LittleEndian(data.Slice(offset, sizeof(ushort)));
            crc = Sse42.Crc32(crc, value);
            offset += sizeof(ushort);
        }

        if (offset < data.Length)
            crc = Sse42.Crc32(crc, data[offset]);

        return crc;
    }

    private static uint AppendTable(uint crc, ReadOnlySpan<byte> data, uint[] table)
    {
        foreach (byte value in data)
            crc = table[(byte)(crc ^ value)] ^ (crc >> 8);
        return crc;
    }

    private static uint[] CreateTable(uint polynomial)
    {
        var table = new uint[256];
        for (int i = 0; i < table.Length; i++)
        {
            uint value = (uint)i;
            for (int bit = 0; bit < 8; bit++)
                value = (value >> 1) ^ ((value & 1) == 0 ? 0u : polynomial);
            table[i] = value;
        }
        return table;
    }
}
