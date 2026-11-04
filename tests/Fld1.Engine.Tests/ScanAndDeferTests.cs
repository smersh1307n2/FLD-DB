using Fld1.Engine.Db;
using Xunit;

namespace Fld1.Engine.Tests;

public class ScanAndDeferTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "fld-scan-" + Guid.NewGuid().ToString("N"));
    public ScanAndDeferTests() => Directory.CreateDirectory(_dir);
    public void Dispose() { try { Directory.Delete(_dir, true); } catch { } }

    private static byte[] Rec(byte b)
    {
        var id = new byte[16]; id[0] = b; id[6] = 0x70; id[8] = 0x80; return id;
    }

    [Fact]
    public void Commit_without_FlushManifest_still_recovers_via_wal()
    {
        var rec = Rec(20);
        using (var db = FldDatabase.Open(_dir, 0))
        {
            db.Put(rec, 1, "x"u8);
            db.Commit();
            // no FlushManifest, no Checkpoint — dispose still best-effort flushes manifest
        }
        using (var db2 = FldDatabase.Open(_dir, 0))
        {
            Assert.True(db2.TryGetBytes(rec, 1, out var v) && v!.SequenceEqual("x"u8.ToArray()));
        }
    }

    [Fact]
    public void ScanRecord_merges_overlay_and_run()
    {
        var rec = Rec(21);
        using var db = FldDatabase.Open(_dir, 0);
        db.Put(rec, 1, "old"u8);
        db.Put(rec, 2, "keep"u8);
        db.Commit();
        db.Checkpoint();
        db.Put(rec, 1, "new"u8); // overlay wins

        var map = new Dictionary<ushort, byte[]>();
        db.ScanRecord(rec, (fid, val) => map[fid] = val);
        Assert.Equal("new"u8.ToArray(), map[1]);
        Assert.Equal("keep"u8.ToArray(), map[2]);
    }

    [Fact]
    public void ScanRecordIds_includes_overlay_and_runs()
    {
        using var db = FldDatabase.Open(_dir, 0);
        db.Put(Rec(1), 1, "a"u8);
        db.Commit();
        db.Checkpoint();
        db.Put(Rec(2), 1, "b"u8);
        var ids = new List<byte>();
        db.ScanRecordIds(rid => ids.Add(rid[0]));
        Assert.Contains((byte)1, ids);
        Assert.Contains((byte)2, ids);
    }
}
