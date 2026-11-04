using Fld1.Core.Ids;
using Xunit;

namespace Fld1.Core.Tests;

public class Id16Tests
{
    [Fact]
    public void Compare_and_equals()
    {
        Span<byte> ba = stackalloc byte[16];
        Span<byte> bb = stackalloc byte[16];
        ba.Clear(); bb.Clear();
        ba[0] = 1;
        bb[0] = 2;
        var a = new Id16(ba);
        var b = new Id16(bb);
        Assert.True(a < b);
        Assert.False(a.Equals(b));
        Assert.Equal(a, new Id16(ba));
    }

    [Fact]
    public void NamespaceId_rejects_large()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new NamespaceId(0x1_0000u));
        _ = new NamespaceId(0xFFFF);
    }
}
