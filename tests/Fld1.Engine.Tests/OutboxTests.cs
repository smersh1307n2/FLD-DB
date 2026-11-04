using Fld1.Engine.Db;
using Xunit;

namespace Fld1.Engine.Tests;

public class OutboxTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "fld-obx-" + Guid.NewGuid().ToString("N"));
    public OutboxTests() => Directory.CreateDirectory(_dir);
    public void Dispose() { try { Directory.Delete(_dir, true); } catch { } }

    [Fact]
    public void Commit_fills_outbox()
    {
        using var db = FldDatabase.Open(_dir);
        var rec = FldDatabase.NewRecordId();
        db.Put(rec, 1, "hi"u8);
        db.Commit();
        var batch = db.ReadOutboxBatch(0, 10);
        Assert.Single(batch);
        Assert.Equal(1ul, batch[0].OutboxSeq);
        Assert.Equal(1, batch[0].FieldId);
        Assert.Equal("hi"u8.ToArray(), batch[0].Value);
        Assert.Equal(16, batch[0].ChangeId.Length);
    }

    [Fact]
    public void Acknowledge_advances_watermark()
    {
        using var db = FldDatabase.Open(_dir);
        var rec = FldDatabase.NewRecordId();
        db.Put(rec, 1, "a"u8);
        db.Commit();
        db.Put(rec, 2, "b"u8);
        db.Commit();
        var all = db.ReadOutboxBatch(0, 10);
        Assert.Equal(2, all.Count);
        db.AcknowledgeOutbox(all[0].OutboxSeq);
        Assert.Equal(all[0].OutboxSeq, db.GetOutboxWatermark());
        var rest = db.ReadOutboxBatch(0, 10);
        Assert.Single(rest);
        Assert.Equal(all[1].OutboxSeq, rest[0].OutboxSeq);
    }

    [Fact]
    public void Outbox_survives_reopen()
    {
        var rec = FldDatabase.NewRecordId();
        byte[] changeId;
        using (var db = FldDatabase.Open(_dir))
        {
            db.Put(rec, 1, "x"u8);
            db.Commit();
            changeId = db.ReadOutboxBatch(0, 1)[0].ChangeId;
        }
        using (var db2 = FldDatabase.Open(_dir))
        {
            var batch = db2.ReadOutboxBatch(0, 1);
            Assert.Single(batch);
            Assert.Equal(changeId, batch[0].ChangeId);
            Assert.Equal("x"u8.ToArray(), batch[0].Value);
        }
    }
}
