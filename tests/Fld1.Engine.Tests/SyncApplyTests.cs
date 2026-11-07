using Fld1.Engine.Db;
using Fld1.Engine.Sync;
using Xunit;

namespace Fld1.Engine.Tests;

public class SyncApplyTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "fld-sync-" + Guid.NewGuid().ToString("N"));
    public SyncApplyTests() => Directory.CreateDirectory(_dir);
    public void Dispose() { try { Directory.Delete(_dir, true); } catch { } }

    private static byte[] Author(byte b)
    {
        var a = new byte[16];
        a[15] = b;
        return a;
    }

    [Fact]
    public void ApplyRemote_wins_with_newer_hlc()
    {
        using var db = FldDatabase.Open(_dir);
        var rec = FldDatabase.NewRecordId();
        db.Put(rec, 1, "local"u8);
        db.Commit();
        db.Checkpoint();

        int n = db.ApplyRemote([
            new RemoteFieldWrite(rec, 1, "remote"u8.ToArray(), wall: ulong.MaxValue / 2, logical: 0, Author(9))
        ]);
        Assert.Equal(1, n);
        Assert.True(db.TryGetBytes(rec, 1, out var v) && v!.SequenceEqual("remote"u8.ToArray()));
    }

    [Fact]
    public void ApplyRemote_loses_to_newer_local()
    {
        using var db = FldDatabase.Open(_dir);
        var rec = FldDatabase.NewRecordId();
        db.Put(rec, 1, "local-new"u8);
        // remote with wall=1 loses to local HLC from clock
        int n = db.ApplyRemote([
            new RemoteFieldWrite(rec, 1, "old"u8.ToArray(), wall: 1, logical: 0, Author(1))
        ]);
        Assert.Equal(0, n);
        Assert.True(db.TryGetBytes(rec, 1, out var v) && v!.SequenceEqual("local-new"u8.ToArray()));
    }

    [Fact]
    public void Outbox_visit_batch_exposes_borrowed_entries_and_can_stop()
    {
        using var db = FldDatabase.Open(_dir);
        var rec = FldDatabase.NewRecordId();
        db.Put(rec, 7, "borrowed"u8);
        db.Commit();

        int callbacks = 0;
        bool Visit(in Fld1.Engine.Outbox.OutboxEntryView entry)
        {
            callbacks++;
            Assert.Equal(1UL, entry.OutboxSeq);
            Assert.Equal(7, entry.FieldId);
            Assert.Equal(rec, entry.RecordId.ToArray());
            Assert.Equal("borrowed"u8.ToArray(), entry.Value.ToArray());
            Assert.Equal(16, entry.ChangeId.Length);
            return false;
        }

        int visited = db.VisitOutboxBatch(0, 10, Visit);
        Assert.Equal(1, visited);
        Assert.Equal(1, callbacks);
    }

    [Fact]
    public void Outbox_visit_does_not_hold_database_commit_lock()
    {
        using var db = FldDatabase.Open(_dir);
        var rec = FldDatabase.NewRecordId();
        db.Put(rec, 1, "seed"u8);
        db.Commit();

        using var entered = new ManualResetEventSlim();
        using var release = new ManualResetEventSlim();
        bool Block(in Fld1.Engine.Outbox.OutboxEntryView entry)
        {
            entered.Set();
            release.Wait(TimeSpan.FromSeconds(5));
            return false;
        }

        var visitTask = Task.Run(() => db.VisitOutboxBatch(0, 1, Block));
        Assert.True(entered.Wait(TimeSpan.FromSeconds(5)));
        try
        {
            var writeTask = Task.Run(() => db.Put(rec, 2, "parallel"u8));
            Assert.True(writeTask.Wait(TimeSpan.FromSeconds(5)),
                "Outbox encoding must not hold the database commit lock.");
        }
        finally
        {
            release.Set();
        }

        Assert.True(visitTask.Wait(TimeSpan.FromSeconds(5)));
    }

    [Fact]
    public void Outbox_compact_after_ack()
    {
        using var db = FldDatabase.Open(_dir);
        var rec = FldDatabase.NewRecordId();
        db.Put(rec, 1, "a"u8);
        db.Commit();
        db.Put(rec, 2, "b"u8);
        db.Commit();
        var batch = db.ReadOutboxBatch(0, 10);
        Assert.Equal(2, batch.Count);
        db.AcknowledgeOutbox(batch[0].OutboxSeq);
        int kept = db.CompactOutbox();
        Assert.Equal(1, kept);
        var rest = db.ReadOutboxBatch(0, 10);
        Assert.Single(rest);
        Assert.Equal(batch[1].OutboxSeq, rest[0].OutboxSeq);
    }
}
