using Fld1.Core.Util;
using Xunit;

namespace Fld1.Core.Tests;

public class ByteUtilTests
{
    [Fact]
    public void CompareUnsigned_Equal()
    {
        var a = new byte[] { 1, 2, 3 };
        Assert.Equal(0, ByteUtil.CompareUnsigned(a, a));
    }

    [Fact]
    public void CompareUnsigned_less()
    {
        ReadOnlySpan<byte> a = [0x01, 0x00];
        ReadOnlySpan<byte> b = [0x01, 0x01];
        Assert.True(ByteUtil.CompareUnsigned(a, b) < 0);
    }

    [Fact]
    public void CompareUnsigned16_BE_order()
    {
        // First byte differs → higher first byte is greater
        Span<byte> a = stackalloc byte[16];
        Span<byte> b = stackalloc byte[16];
        a.Clear(); b.Clear();
        a[0] = 0x01;
        b[0] = 0x02;
        Assert.True(ByteUtil.CompareUnsigned16(a, b) < 0);
        Assert.True(ByteUtil.CompareUnsigned16(b, a) > 0);
    }

    [Fact]
    public void Equals16_true_false()
    {
        Span<byte> a = stackalloc byte[16];
        Span<byte> b = stackalloc byte[16];
        a.Fill(0xAB); b.Fill(0xAB);
        Assert.True(ByteUtil.Equals16(a, b));
        b[15] = 0x00;
        Assert.False(ByteUtil.Equals16(a, b));
    }

    [Fact]
    public void LE_roundtrip()
    {
        Span<byte> buf = stackalloc byte[8];
        ByteUtil.WriteUInt64LE(buf, 0x0102030405060708UL);
        Assert.Equal(0x0102030405060708UL, ByteUtil.ReadUInt64LE(buf));
        ByteUtil.WriteUInt32LE(buf, 0xAABBCCDD);
        Assert.Equal(0xAABBCCDDu, ByteUtil.ReadUInt32LE(buf));
        ByteUtil.WriteUInt16LE(buf, 0x1234);
        Assert.Equal((ushort)0x1234, ByteUtil.ReadUInt16LE(buf));
    }
}
