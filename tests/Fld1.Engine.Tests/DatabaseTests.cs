using Fld1.Core.Uuid;
using Fld1.Engine.Db;
using Xunit;

namespace Fld1.Engine.Tests;

public class DatabaseTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "fld-db-" + Guid.NewGuid().ToString("N"));
    public DatabaseTests() => Directory.CreateDirectory(_dir);
    public void Dispose() { try { Directory.Delete(_dir, true); } catch { } }

    private static byte[] Rec(byte b)
    {
        var id = new byte[16];
        id[0] = b;
        id[6] = 0x70;
        id[8] = 0x80;
        return id;
    }

    [Fact]
    public void Put_get_from_overlay()
    {
        using var db = FldDatabase.Open(_dir, namespaceId: 0);
        var rec = Rec(1);
        db.Put(rec, 10, "hello"u8);
        Assert.True(db.TryGetBytes(rec, 10, out var v));
        Assert.Equal("hello"u8.ToArray(), v);
    }

    [Fact]
    public void Commit_writes_wal()
    {
        using var db = FldDatabase.Open(_dir, 0);
        db.Put(Rec(2), 1, "x"u8);
        ulong seq = db.Commit();
        Assert.True(seq >= 1);
        Assert.True(File.Exists(Path.Combine(_dir, "wal", "ns_0.0.log")));
    }

    [Fact]
    public void Checkpoint_run_and_get()
    {
        using var db = FldDatabase.Open(_dir, 0);
        var rec = Rec(3);
        db.Put(rec, 5, "sealed"u8);
        db.Commit();
        ulong runId = db.Checkpoint();
        Assert.True(runId >= 1);
        Assert.Equal(0, db.OverlayCount);
        Assert.True(db.TryGetBytes(rec, 5, out var v));
        Assert.Equal("sealed"u8.ToArray(), v);
        Assert.True(File.Exists(Path.Combine(_dir, "runs", $"{runId}.fld1")));
    }

    [Fact]
    public void Reopen_reads_run()
    {
        var rec = Rec(4);
        ulong runId;
        using (var db = FldDatabase.Open(_dir, 0))
        {
            db.Put(rec, 1, "persist"u8);
            db.Commit();
            runId = db.Checkpoint();
        }
        using (var db2 = FldDatabase.Open(_dir, 0))
        {
            Assert.True(db2.TryGetBytes(rec, 1, out var v));
            Assert.Equal("persist"u8.ToArray(), v);
        }
    }

    [Fact]
    public void Overlay_wins_over_run()
    {
        var rec = Rec(5);
        using var db = FldDatabase.Open(_dir, 0);
        db.Put(rec, 1, "old"u8);
        db.Commit();
        db.Checkpoint();
        db.Put(rec, 1, "new"u8);
        Assert.True(db.TryGetBytes(rec, 1, out var v));
        Assert.Equal("new"u8.ToArray(), v);
    }

    [Fact]
    public void Multi_field_checkpoint()
    {
        using var db = FldDatabase.Open(_dir, 0);
        var rec = Rec(6);
        db.Put(rec, 1, "a"u8);
        db.Put(rec, 2, "b"u8);
        db.Commit();
        db.Checkpoint();
        Assert.True(db.TryGetBytes(rec, 1, out var a) && a!.SequenceEqual("a"u8.ToArray()));
        Assert.True(db.TryGetBytes(rec, 2, out var b) && b!.SequenceEqual("b"u8.ToArray()));
    }
}
