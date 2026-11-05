using Fld1.Core.Canon;
using Fld1.Core.Hash;
using Xunit;

namespace Fld1.Core.Tests;

public class HashTests
{
    [Fact]
    public void XxHash64_deterministic()
    {
        ReadOnlySpan<byte> data = "FLD1"u8;
        ulong h1 = Hashes.XxHash64(data);
        ulong h2 = Hashes.XxHash64(data);
        Assert.Equal(h1, h2);
    }

    [Fact]
    public void Crc32C_known()
    {
        // Empty → 0
        Assert.Equal(0u, Hashes.Crc32C(ReadOnlySpan<byte>.Empty));
    }

    [Fact]
    public void Crc32C_matches_castagnoli_check_value()
    {
        // CRC-32C/Castagnoli check("123456789") = 0xE3069283.
        Assert.Equal(0xE3069283u, Hashes.Crc32C("123456789"u8));
    }

    [Fact]
    public void Crc32C_two_spans_match_single_span()
    {
        Assert.Equal(
            Hashes.Crc32C("123456789"u8),
            Hashes.Crc32C("1234"u8, "56789"u8));
    }

    [Fact]
    public void Legacy_ieee_crc_is_readable_but_not_used_for_new_writes()
    {
        // CRC-32/IEEE 802.3 check("123456789") = 0xCBF43926.
        const uint legacy = 0xCBF43926;
        Assert.Equal(legacy, Hashes.Crc32Ieee("123456789"u8));
        Assert.True(Hashes.IsCrc32COrLegacy("123456789"u8, legacy));
        Assert.Equal(0xE3069283u, Hashes.Crc32C("123456789"u8));
    }

    [Fact]
    public void Blake3_tagged_runstate()
    {
        Span<byte> dig = stackalloc byte[32];
        Hashes.Blake3_256Tagged(Tags.RunStateV1, "ns0"u8, dig);
        var dig2 = Hashes.Blake3_256(Concat(Tags.RunStateV1, "ns0"u8));
        Assert.True(dig.SequenceEqual(dig2));
    }

    private static byte[] Concat(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b)
    {
        var r = new byte[a.Length + b.Length];
        a.CopyTo(r);
        b.CopyTo(r.AsSpan(a.Length));
        return r;
    }
}
