using Fld1.Engine.Db;
using Xunit;

namespace Fld1.Engine.Tests;

public class LwwAndCompactTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "fld-lww-" + Guid.NewGuid().ToString("N"));
    public LwwAndCompactTests() => Directory.CreateDirectory(_dir);
    public void Dispose() { try { Directory.Delete(_dir, true); } catch { } }

    [Fact]
    public void Lww_newer_checkpoint_wins_over_older_run()
    {
        using var db = FldDatabase.Open(_dir);
        var rec = FldDatabase.NewRecordId();
        db.Put(rec, 1, "v1"u8);
        db.Commit();
        db.Checkpoint();
        db.Put(rec, 1, "v2"u8);
        db.Commit();
        db.Checkpoint();
        Assert.True(db.TryGetBytes(rec, 1, out var v) && v!.SequenceEqual("v2"u8.ToArray()));
    }

    [Fact]
    public void Lww_tombstone_hides_older_value()
    {
        using var db = FldDatabase.Open(_dir);
        var rec = FldDatabase.NewRecordId();
        db.Put(rec, 1, "alive"u8);
        db.Commit();
        db.Checkpoint();
        db.Delete(rec, 1);
        db.Commit();
        Assert.False(db.TryGetBytes(rec, 1, out _));
        db.Checkpoint();
        Assert.False(db.TryGetBytes(rec, 1, out _));
    }

    [Fact]
    public void Lifecycle_delete_record()
    {
        using var db = FldDatabase.Open(_dir);
        var rec = FldDatabase.NewRecordId();
        db.Put(rec, 1, "x"u8);
        db.DeleteRecord(rec);
        Assert.True(db.IsRecordDead(rec));
        db.UndeleteRecord(rec);
        Assert.False(db.IsRecordDead(rec));
    }

    [Fact]
    public void Compact_merges_runs()
    {
        using var db = FldDatabase.Open(_dir);
        var r1 = FldDatabase.NewRecordId();
        var r2 = FldDatabase.NewRecordId();
        db.Put(r1, 1, "a"u8);
        db.Commit();
        db.Checkpoint();
        db.Put(r2, 1, "b"u8);
        db.Commit();
        db.Checkpoint();
        db.Put(r1, 1, "a2"u8);
        db.Commit();
        db.Checkpoint();

        ulong id = db.Compact();
        Assert.True(id >= 1);
        Assert.Equal(0, db.OverlayCount);
        Assert.True(db.TryGetBytes(r1, 1, out var a) && a!.SequenceEqual("a2"u8.ToArray()));
        Assert.True(db.TryGetBytes(r2, 1, out var b) && b!.SequenceEqual("b"u8.ToArray()));
        // single run file preferred
        var runs = Directory.GetFiles(Path.Combine(_dir, "runs"), "*.fld1");
        Assert.Single(runs);
    }
}
