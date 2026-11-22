using Fld1.Run.Builder;
using Fld1.Run.Reader;
using Xunit;

namespace Fld1.Run.Tests;

public class ScanTests
{
    private static byte[] Rec(byte b)
    {
        var id = new byte[16];
        id[0] = b; id[6] = 0x70; id[8] = 0x80;
        return id;
    }
    private static byte[] Author(byte b)
    {
        var a = new byte[16]; a[15] = b; return a;
    }

    [Fact]
    public void ScanRecord_all_fields()
    {
        var rec = Rec(1);
        var a = Author(1);
        var run = new RunBuilder(0)
            .Add(rec, 1, "one"u8, 1, 0, a)
            .Add(rec, 2, "two"u8, 1, 0, a)
            .Add(rec, 3, "three"u8, 1, 0, a)
            .Build();

        var found = new Dictionary<ushort, byte[]>();
        new RunReader(run).ScanRecord(rec, (fid, val) => found[fid] = val.ToArray());
        Assert.Equal(3, found.Count);
        Assert.Equal("one"u8.ToArray(), found[1]);
        Assert.Equal("two"u8.ToArray(), found[2]);
        Assert.Equal("three"u8.ToArray(), found[3]);
    }

    [Fact]
    public void ScanRecordIds_lists_all()
    {
        var a = Author(1);
        var run = new RunBuilder(0)
            .Add(Rec(1), 1, "a"u8, 1, 0, a)
            .Add(Rec(2), 1, "b"u8, 1, 0, a)
            .Build();
        var ids = new List<byte>();
        new RunReader(run).ScanRecordIds(rid => ids.Add(rid[0]));
        Assert.Equal(2, ids.Count);
        Assert.Contains((byte)1, ids);
        Assert.Contains((byte)2, ids);
    }
    [Fact]
    public void ScanAllFieldIds_lists_every_record_and_includes_tombstone_descriptors()
    {
        var first = Rec(1);
        var second = Rec(2);
        var author = Author(1);
        var run = new RunBuilder(0)
            .Add(first, 1, "live"u8, 1, 0, author)
            .Add(first, 2, "deleted"u8, 2, 0, author, isTombstone: true)
            .Add(second, 7, "other"u8, 3, 0, author)
            .Build();

        var found = new Dictionary<byte, List<ushort>>();
        new RunReader(run).ScanAllFieldIds((recordId, fieldId) =>
        {
            byte record = recordId.Span[0];
            if (!found.TryGetValue(record, out List<ushort>? fields))
            {
                fields = new List<ushort>();
                found.Add(record, fields);
            }
            fields.Add(fieldId);
        });

        Assert.Equal(2, found.Count);
        Assert.Equal(new ushort[] { 1, 2 }, found[1]);
        Assert.Equal(new ushort[] { 7 }, found[2]);
    }
}
