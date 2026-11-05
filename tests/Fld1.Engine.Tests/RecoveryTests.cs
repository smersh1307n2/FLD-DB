using Fld1.Engine.Db;
using Xunit;

namespace Fld1.Engine.Tests;

public class RecoveryTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "fld-rec-" + Guid.NewGuid().ToString("N"));
    public RecoveryTests() => Directory.CreateDirectory(_dir);
    public void Dispose() { try { Directory.Delete(_dir, true); } catch { } }

    private static byte[] Rec(byte b)
    {
        var id = new byte[16];
        id[0] = b; id[6] = 0x70; id[8] = 0x80;
        return id;
    }

    [Fact]
    public void Crash_after_commit_before_checkpoint_recovers_overlay()
    {
        var rec = Rec(10);
        using (var db = FldDatabase.Open(_dir, 0))
        {
            db.Put(rec, 1, "durable"u8);
            Assert.True(db.Commit() >= 1);
            // no Checkpoint — process "dies"
        }

        using (var db2 = FldDatabase.Open(_dir, 0))
        {
            Assert.True(db2.OverlayCount >= 1);
            Assert.True(db2.TryGetBytes(rec, 1, out var v));
            Assert.Equal("durable"u8.ToArray(), v);
        }
    }

    [Fact]
    public void Missing_outbox_after_checkpoint_fails_closed()
    {
        using (var db = FldDatabase.Open(_dir, 0))
        {
            db.Put(Rec(43), 1, "outbox-is-durable"u8);
            db.Commit();
            db.Checkpoint();
        }

        string outboxPath = Path.Combine(_dir, "outbox", "ns_0.obx");
        Assert.True(File.Exists(outboxPath));
        File.Delete(outboxPath);

        Assert.Throws<InvalidDataException>(() => FldDatabase.Open(_dir, 0));
        Assert.False(File.Exists(outboxPath));
    }

    [Fact]
    public void Corrupt_manifest_fails_open_without_deleting_runs()
    {
        var recordId = Rec(42);
        ulong runId;

        using (var db = FldDatabase.Open(_dir, 0))
        {
            db.Put(recordId, 1, "must-survive"u8);
            db.Commit();
            runId = db.Checkpoint();
        }

        string runPath = Path.Combine(_dir, "runs", $"{runId}.fld1");
        Assert.True(File.Exists(runPath));

        string manifestPath = Path.Combine(_dir, "manifest.fldm");
        byte[] manifest = File.ReadAllBytes(manifestPath);
        manifest[^1] ^= 0x80;
        File.WriteAllBytes(manifestPath, manifest);

        Assert.Throws<InvalidDataException>(() => FldDatabase.Open(_dir, 0));
        Assert.True(File.Exists(runPath));
    }

    [Fact]
    public void Checkpoint_then_commit_reopen_keeps_both()
    {
        var r1 = Rec(11);
        var r2 = Rec(12);
        using (var db = FldDatabase.Open(_dir, 0))
        {
            db.Put(r1, 1, "sealed"u8);
            db.Commit();
            db.Checkpoint();
            db.Put(r2, 1, "wal-only"u8);
            db.Commit();
        }
        using (var db2 = FldDatabase.Open(_dir, 0))
        {
            Assert.True(db2.TryGetBytes(r1, 1, out var a) && a!.SequenceEqual("sealed"u8.ToArray()));
            Assert.True(db2.TryGetBytes(r2, 1, out var b) && b!.SequenceEqual("wal-only"u8.ToArray()));
        }
    }
}
