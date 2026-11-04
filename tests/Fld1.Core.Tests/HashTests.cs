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
