using Fld1.Engine.Db;
using Xunit;

namespace Fld1.Engine.Tests;

public class WalRotationTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "fld-rot-" + Guid.NewGuid().ToString("N"));
    public WalRotationTests() => Directory.CreateDirectory(_dir);
    public void Dispose() { try { Directory.Delete(_dir, true); } catch { } }

    [Fact]
    public void Manual_rotate_and_recover()
    {
        var rec = FldDatabase.NewRecordId();
        using (var db = FldDatabase.Open(new FldOpenOptions
        {
            Path = _dir,
            WalRotateAfterBytes = 0,
            TruncateSealedWal = false
        }))
        {
            db.Put(rec, 1, "a"u8);
            db.Commit();
            db.RotateWal();
            db.Put(rec, 1, "b"u8);
            db.Commit();
        }
        using (var db2 = FldDatabase.Open(_dir))
        {
            Assert.True(db2.TryGetBytes(rec, 1, out var v) && v!.SequenceEqual("b"u8.ToArray()));
        }
    }

    [Fact]
    public void Checkpoint_can_truncate_old_wal()
    {
        var rec = FldDatabase.NewRecordId();
        using var db = FldDatabase.Open(new FldOpenOptions
        {
            Path = _dir,
            WalRotateAfterBytes = 0,
            TruncateSealedWal = true
        });
        db.Put(rec, 1, "x"u8);
        db.Commit();
        db.RotateWal();
        db.Put(rec, 2, "y"u8);
        db.Commit();
        db.Checkpoint();
        // sealed covers commits; segment 0 may be gone
        var walFiles = Directory.GetFiles(Path.Combine(_dir, "wal"), "*.log");
        Assert.NotEmpty(walFiles);
        Assert.True(db.TryGetBytes(rec, 1, out var a) && a!.SequenceEqual("x"u8.ToArray()));
    }
}
