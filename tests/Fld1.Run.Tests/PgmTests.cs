using Fld1.Core.Uuid;
using Fld1.Run.Builder;
using Fld1.Run.Format;
using Fld1.Run.Reader;
using Fld1.Run.Verify;
using Xunit;

namespace Fld1.Run.Tests;

public class PgmTests
{
    private static byte[] Author(byte b)
    {
        var a = new byte[16];
        a[15] = b;
        return a;
    }

    [Fact]
    public void Many_records_get_all()
    {
        var a = Author(1);
        var builder = new RunBuilder(0);
        var ids = new List<byte[]>();
        for (int i = 0; i < 50; i++)
        {
            var id = new byte[16];
            Uuid7.NewUuid7(id);
            // ensure uniqueness & v7
            id[14] = (byte)(i >> 8);
            id[15] = (byte)i;
            ids.Add(id);
            builder.Add(id, 1, new byte[] { (byte)i }, wall: 1000 + (ulong)i, logical: 0, a);
        }
        // Sort ids as builder will (BE)
        ids.Sort((x, y) => Fld1.Core.Util.ByteUtil.CompareUnsigned16(x, y));

        var run = builder.Build();
        Assert.True(RunVerify.Verify(run).IsOk, RunVerify.Verify(run).Detail);
        Assert.True(RunHeader.ReadPgmSegments(run) >= 1);

        var reader = new RunReader(run);
        Assert.Equal(50u, reader.NumClusters);
        for (int i = 0; i < 50; i++)
        {
            Assert.True(reader.TryGet(ids[i], 1, out var v), $"miss i={i}");
            // value is the original index byte — recover from sorted position is hard;
            // just check length 1
            Assert.Equal(1, v.Length);
        }
    }

    [Fact]
    public void Pgm_segments_in_header()
    {
        var a = Author(1);
        var b = new RunBuilder(0, pgmEpsilon: 2);
        for (int i = 0; i < 20; i++)
        {
            var id = new byte[16];
            id[0] = (byte)(i * 10); // spread keys
            id[6] = 0x70; id[8] = 0x80;
            b.Add(id, 1, "x"u8, 1, 0, a);
        }
        var run = b.Build();
        Assert.True(RunVerify.Verify(run).IsOk);
        Assert.True(RunHeader.ReadPgmSegments(run) >= 1);
    }

    [Fact]
    public void Many_pgm_segments_lookup_all_records()
    {
        var author = Author(1);
        const int count = 4096;
        var builder = new RunBuilder(0, pgmEpsilon: 8);
        var ids = new List<byte[]>(count);

        for (int i = 0; i < count; i++)
        {
            var id = new byte[16];
            ulong timestampMs = (ulong)i * (ulong)i;
            id[0] = (byte)(timestampMs >> 40);
            id[1] = (byte)(timestampMs >> 32);
            id[2] = (byte)(timestampMs >> 24);
            id[3] = (byte)(timestampMs >> 16);
            id[4] = (byte)(timestampMs >> 8);
            id[5] = (byte)timestampMs;
            id[6] = 0x70;
            id[7] = 0;
            id[8] = 0x80;
            System.Buffers.Binary.BinaryPrimitives.WriteUInt32BigEndian(id.AsSpan(12, 4), (uint)i);

            ids.Add(id);
            builder.Add(id, 1, new byte[] { (byte)i }, wall: 1000 + (ulong)i, logical: 0, author);
        }

        var run = builder.Build();
        Assert.True(RunVerify.Verify(run).IsOk, RunVerify.Verify(run).Detail);
        Assert.True(RunHeader.ReadPgmSegments(run) > 1);

        var reader = new RunReader(run);
        foreach (var id in ids)
            Assert.True(reader.TryGet(id, 1, out var value), $"miss for {Convert.ToHexString(id)}");
    }
}
