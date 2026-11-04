using Fld1.Run.Builder;
using Fld1.Run.Format;
using Fld1.Run.Reader;
using Fld1.Run.Verify;
using Xunit;

namespace Fld1.Run.Tests;

public class ExternalValueTests
{
    private static byte[] Rec(byte b)
    {
        var id = new byte[16];
        id[0] = b;
        id[6] = 0x70;
        id[8] = 0x80;
        return id;
    }

    private static byte[] Author(byte b)
    {
        var a = new byte[16];
        a[15] = b;
        return a;
    }

    [Fact]
    public void External_value_roundtrip()
    {
        var rec = Rec(0x50);
        var a = Author(1);
        var big = new byte[1000];
        for (int i = 0; i < big.Length; i++) big[i] = (byte)(i & 0xFF);

        // policy 256 → 1000B is external
        var run = new RunBuilder(0, inlinePolicy: 256)
            .Add(rec, 1, big, 100, 0, a)
            .Build();

        var vr = RunVerify.Verify(run);
        Assert.True(vr.IsOk, vr.Detail);
        Assert.True((RunHeader.ReadFlags(run) & RunFlags.HasValues) != 0);

        var reader = new RunReader(run);
        Assert.True(reader.TryGet(rec, 1, out var got));
        Assert.Equal(1000, got.Length);
        Assert.True(got.SequenceEqual(big));
    }

    [Fact]
    public void Mixed_inline_and_external()
    {
        var rec = Rec(0x51);
        var a = Author(1);
        var small = "tiny"u8.ToArray();
        var big = new byte[512];
        big.AsSpan().Fill(0xAB);

        var run = new RunBuilder(0)
            .Add(rec, 1, small, 10, 0, a)
            .Add(rec, 2, big, 10, 0, a)
            .Build();

        Assert.True(RunVerify.Verify(run).IsOk);
        var reader = new RunReader(run);
        Assert.True(reader.TryGet(rec, 1, out var v1) && v1.SequenceEqual(small));
        Assert.True(reader.TryGet(rec, 2, out var v2) && v2.SequenceEqual(big));
    }

    [Fact]
    public void External_deterministic()
    {
        var rec = Rec(0x52);
        var a = Author(2);
        var big = new byte[300];
        Random.Shared.NextBytes(big);

        byte[] Once() => new RunBuilder(3)
            .Add(rec, 9, big, 1, 0, a)
            .Build();

        Assert.True(Once().AsSpan().SequenceEqual(Once()));
    }

    [Fact]
    public void Reject_over_4mb()
    {
        var rec = Rec(0x53);
        var a = Author(1);
        Assert.Throws<ArgumentException>(() =>
            new FieldWrite(rec, 1, new byte[4 * 1024 * 1024 + 1], 1, 0, a));
    }
}
