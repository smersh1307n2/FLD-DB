using Fld1.Core.Hlc;
using Xunit;

namespace Fld1.Core.Tests;

public class HlcTests
{
    private static byte[] Author(byte b)
    {
        var a = new byte[16];
        a[15] = b;
        return a;
    }

    [Fact]
    public void Clock_monotonic()
    {
        var clock = new HlcClock(Author(1));
        var a = clock.Now();
        var b = clock.Now();
        Assert.True(b.CompareTo(a) > 0);
    }

    [Fact]
    public void Observe_advances()
    {
        var clock = new HlcClock(Author(1));
        var remote = new HlcStamp(clock.Now().Stamp.Wall + 1_000_000, 5);
        clock.Observe(remote);
        var next = clock.Now();
        Assert.True(next.Stamp.Wall >= remote.Wall);
    }

    [Fact]
    public void LWW_tiebreak_by_author()
    {
        var a1 = new HlcFull(100, 0, Author(1));
        var a2 = new HlcFull(100, 0, Author(2));
        Assert.True(a1.CompareTo(a2) < 0);
        Assert.True(a2.CompareTo(a1) > 0);
    }

    [Fact]
    public void Logical_overflow_throws()
    {
        var clock = new HlcClock(Author(1));
        // Set wall to far future so physical time cannot advance past it in the test,
        // and logical to MaxValue. Next Now() must tick logical → overflow.
        ulong future = (ulong)(DateTimeOffset.UtcNow.Ticks / 10) + 10_000_000UL;
        clock.Observe(new HlcStamp(future, uint.MaxValue));
        Assert.Throws<InvalidOperationException>(() => clock.Now());
    }
}
