using Fld1.Core.Uuid;
using Fld1.Run.Builder;
using Fld1.Run.Format;
using Fld1.Run.Reader;
using Fld1.Run.Verify;
using Xunit;

namespace Fld1.Run.Tests;

public class RunBuilderTests
{
    private static byte[] Rec(byte b)
    {
        var id = new byte[16];
        Uuid7.NewUuid7(id);
        id[15] = b; // make distinct while keeping v7-ish
        return id;
    }

    private static byte[] Author(byte b)
    {
        var a = new byte[16];
        a[15] = b;
        return a;
    }

    [Fact]
    public void Single_field_roundtrip()
    {
        var rec = Rec(1);
        var author = Author(1);
        var value = "hello"u8.ToArray();

        var run = new RunBuilder(1)
            .Add(rec, 10, value, wall: 1000, logical: 0, author)
            .Build();

        var vr = RunVerify.Verify(run);
        Assert.True(vr.IsOk, vr.Detail);

        var reader = new RunReader(run);
        Assert.False(reader.IsEmpty);
        Assert.Equal(1u, reader.NumClusters);
        Assert.Equal(1u, reader.NumFields);
        Assert.True(reader.TryGet(rec, 10, out var got));
        Assert.True(got.SequenceEqual(value));
    }

    [Fact]
    public void Two_records_sorted_and_get()
    {
        // Ensure record order independent of insert order
        var r1 = new byte[16];
        var r2 = new byte[16];
        r1[0] = 0x01; // smaller BE
        r2[0] = 0x02;
        // make valid-looking
        r1[6] = 0x70; r1[8] = 0x80;
        r2[6] = 0x70; r2[8] = 0x80;

        var a = Author(1);
        var run = new RunBuilder(0)
            .Add(r2, 1, "B"u8, 100, 0, a)
            .Add(r1, 1, "A"u8, 100, 0, a)
            .Build();

        Assert.True(RunVerify.Verify(run).IsOk);
        var reader = new RunReader(run);
        Assert.Equal(2u, reader.NumClusters);
        Assert.True(reader.TryGet(r1, 1, out var v1));
        Assert.True(v1.SequenceEqual("A"u8));
        Assert.True(reader.TryGet(r2, 1, out var v2));
        Assert.True(v2.SequenceEqual("B"u8));
        Assert.False(reader.TryGet(r1, 99, out _));
    }

    [Fact]
    public void LWW_keeps_winner()
    {
        var rec = new byte[16];
        rec[0] = 0xAA; rec[6] = 0x70; rec[8] = 0x80;
        var a1 = Author(1);
        var a2 = Author(2);

        var run = new RunBuilder(0)
            .Add(rec, 5, "old"u8, wall: 100, logical: 0, a1)
            .Add(rec, 5, "new"u8, wall: 200, logical: 0, a1)
            .Build();

        var reader = new RunReader(run);
        Assert.Equal(1u, reader.NumFields);
        Assert.True(reader.TryGet(rec, 5, out var v));
        Assert.True(v.SequenceEqual("new"u8));
    }

    [Fact]
    public void Deterministic_same_input()
    {
        var rec = new byte[16];
        rec[0] = 0x11; rec[6] = 0x70; rec[8] = 0x80;
        var a = Author(3);

        byte[] BuildOnce() => new RunBuilder(5)
            .Add(rec, 1, "x"u8, 50, 1, a)
            .Add(rec, 2, "y"u8, 50, 1, a)
            .Build();

        var a1 = BuildOnce();
        var a2 = BuildOnce();
        Assert.True(a1.AsSpan().SequenceEqual(a2));
    }

    [Fact]
    public void Multi_field_same_record()
    {
        var rec = new byte[16];
        rec[0] = 0x22; rec[6] = 0x70; rec[8] = 0x80;
        var a = Author(1);

        var run = new RunBuilder(0)
            .Add(rec, 3, "c"u8, 10, 0, a)
            .Add(rec, 1, "a"u8, 10, 0, a)
            .Add(rec, 2, "b"u8, 10, 0, a)
            .Build();

        var reader = new RunReader(run);
        Assert.Equal(1u, reader.NumClusters);
        Assert.Equal(3u, reader.NumFields);
        Assert.True(reader.TryGet(rec, 1, out var v1) && v1.SequenceEqual("a"u8));
        Assert.True(reader.TryGet(rec, 2, out var v2) && v2.SequenceEqual("b"u8));
        Assert.True(reader.TryGet(rec, 3, out var v3) && v3.SequenceEqual("c"u8));
    }

    [Fact]
    public void Tombstone_hides_value()
    {
        var rec = new byte[16];
        rec[0] = 0x33; rec[6] = 0x70; rec[8] = 0x80;
        var a = Author(1);

        var run = new RunBuilder(0)
            .Add(rec, 7, ReadOnlySpan<byte>.Empty, 100, 0, a, isTombstone: true)
            .Build();

        var reader = new RunReader(run);
        Assert.False(reader.TryGet(rec, 7, out _));
    }

    [Fact]
    public void Alignment_and_footer()
    {
        var rec = new byte[16];
        rec[0] = 0x44; rec[6] = 0x70; rec[8] = 0x80;
        var a = Author(1);
        var run = new RunBuilder(0)
            .Add(rec, 1, new byte[100], 1, 0, a)
            .Build();
        Assert.Equal(0, run.Length % 8);
        Assert.True(RunVerify.Verify(run).IsOk);
    }
}
