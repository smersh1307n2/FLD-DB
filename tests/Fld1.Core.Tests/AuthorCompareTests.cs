using Fld1.Core.Util;
using Xunit;

namespace Fld1.Core.Tests;

/// <summary>
/// AuthorNodeId order must match BinSync.NodeId: Lo then Hi as LE u64.
/// </summary>
public class AuthorCompareTests
{
    [Fact]
    public void Author_order_matches_nodeid_limbs()
    {
        var a = new byte[16]; // Lo=0, Hi=0
        var b = new byte[16];
        b[0] = 1; // Lo=1
        Assert.True(ByteUtil.CompareAuthorNodeId(a, b) < 0);

        var c = new byte[16];
        c[8] = 1; // Hi=1, Lo=0
        Assert.True(ByteUtil.CompareAuthorNodeId(a, c) < 0);
        // b.Lo=1 > c.Lo=0 → c < b
        Assert.True(ByteUtil.CompareAuthorNodeId(c, b) < 0);
        Assert.True(ByteUtil.CompareAuthorNodeId(b, c) > 0);
    }

    [Fact]
    public void Author_order_differs_from_be_memcmp_on_some_inputs()
    {
        var x = new byte[16];
        x[0] = 0x02; // Lo=2
        var y = new byte[16];
        y[7] = 0x01; // Lo = 1<<56
        Assert.True(ByteUtil.CompareUnsigned16(x, y) > 0);
        Assert.True(ByteUtil.CompareAuthorNodeId(x, y) < 0);
    }
}
