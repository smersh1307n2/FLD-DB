using Fld1.Storage.Integrity;
using Xunit;

namespace Fld1.Storage.Tests;

public sealed class StorageChecksumsTests
{
    [Fact]
    public void Crc32c_and_ieee_match_standard_check_vectors()
    {
        ReadOnlySpan<byte> input = "123456789"u8;
        Assert.Equal(0xE3069283u, StorageChecksums.Crc32C(input));
        Assert.Equal(0xCBF43926u, StorageChecksums.Crc32Ieee(input));
        Assert.Equal(0u, StorageChecksums.Crc32C(ReadOnlySpan<byte>.Empty));
        Assert.Equal(0u, StorageChecksums.Crc32Ieee(ReadOnlySpan<byte>.Empty));
    }

    [Fact]
    public void Segmented_checksums_match_contiguous_checksums()
    {
        byte[] first = "Storage CRC32C segmented "u8.ToArray();
        byte[] second = Enumerable.Range(0, 4097).Select(i => (byte)(i * 31)).ToArray();
        byte[] combined = first.Concat(second).ToArray();

        Assert.Equal(StorageChecksums.Crc32C(combined), StorageChecksums.Crc32C(first, second));
        Assert.Equal(StorageChecksums.Crc32Ieee(combined), StorageChecksums.Crc32Ieee(first, second));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(7)]
    [InlineData(8)]
    [InlineData(9)]
    [InlineData(31)]
    [InlineData(255)]
    [InlineData(4096)]
    [InlineData(16384)]
    public void Table_crc_matches_independent_bitwise_reference(int length)
    {
        byte[] bytes = new byte[length];
        new Random(731991 + length).NextBytes(bytes);

        Assert.Equal(Reference(bytes, 0x82F63B78u), StorageChecksums.Crc32C(bytes));
        Assert.Equal(Reference(bytes, 0xEDB88320u), StorageChecksums.Crc32Ieee(bytes));
    }

    private static uint Reference(ReadOnlySpan<byte> data, uint polynomial)
    {
        uint crc = uint.MaxValue;
        foreach (byte value in data)
        {
            crc ^= value;
            for (int bit = 0; bit < 8; bit++)
                crc = (crc >> 1) ^ ((crc & 1) == 0 ? 0u : polynomial);
        }
        return ~crc;
    }
}
