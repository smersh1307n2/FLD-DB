using Fld1.Engine.Db;
using Xunit;

namespace Fld1.Engine.Tests;

public class SimpleApiTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "fld-api-" + Guid.NewGuid().ToString("N"));
    public SimpleApiTests() => Directory.CreateDirectory(_dir);
    public void Dispose() { try { Directory.Delete(_dir, true); } catch { } }

    [Fact]
    public void Open_with_options()
    {
        using var db = FldDatabase.Open(new FldOpenOptions
        {
            Path = _dir,
            NamespaceId = 3,
            AutoCheckpointOverlayFields = 0
        });
        Assert.Equal(3u, db.NamespaceId);
        Assert.Equal(16, db.LocalAuthor.Length);
    }

    [Fact]
    public void WriteBatch_put_and_delete()
    {
        using var db = FldDatabase.Open(_dir);
        var rec = FldDatabase.NewRecordId();
        var batch = new FldWriteBatch()
            .Put(rec, 1, "a"u8.ToArray())
            .Put(rec, 2, "b"u8.ToArray())
            .Delete(rec, 2);
        db.Apply(batch);
        db.Commit();

        Assert.True(db.TryGetBytes(rec, 1, out var a) && a!.SequenceEqual("a"u8.ToArray()));
        Assert.False(db.TryGetBytes(rec, 2, out _));
    }

    [Fact]
    public void Delete_hides_field()
    {
        using var db = FldDatabase.Open(_dir);
        var rec = FldDatabase.NewRecordId();
        db.Put(rec, 1, "x"u8);
        db.Commit();
        db.Checkpoint();
        db.Delete(rec, 1);
        Assert.False(db.TryGetBytes(rec, 1, out _));
        db.Commit();
        // after reopen overlay tombstone from wal
        db.Dispose();
        using var db2 = FldDatabase.Open(_dir);
        Assert.False(db2.TryGetBytes(rec, 1, out _));
    }

    [Fact]
    public void PutOwned_no_extra_semantics()
    {
        using var db = FldDatabase.Open(_dir);
        var rec = FldDatabase.NewRecordId();
        var buf = "owned"u8.ToArray();
        db.PutOwned(rec, 1, buf);
        Assert.True(db.TryGetBytes(rec, 1, out var v) && v!.SequenceEqual(buf));
    }

    [Fact]
    public void AutoCheckpoint_on_commit()
    {
        using var db = FldDatabase.Open(new FldOpenOptions
        {
            Path = _dir,
            AutoCheckpointOverlayFields = 2
        });
        var r1 = FldDatabase.NewRecordId();
        var r2 = FldDatabase.NewRecordId();
        db.Put(r1, 1, "1"u8);
        db.Put(r2, 1, "2"u8);
        db.Commit();
        Assert.Equal(0, db.OverlayCount); // sealed by auto checkpoint
        Assert.True(db.TryGetBytes(r1, 1, out var v) && v!.SequenceEqual("1"u8.ToArray()));
    }
}
