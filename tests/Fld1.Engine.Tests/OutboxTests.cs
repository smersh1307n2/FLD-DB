using System.Buffers.Binary;
using Fld1.Engine.Db;
using Fld1.Engine.Outbox;
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
    public void Acknowledge_rejects_unassigned_sequence()
    {
        using var db = FldDatabase.Open(_dir);
        var recordId = FldDatabase.NewRecordId();
        db.Put(recordId, 1, "value"u8);
        db.Commit();

        Assert.Throws<ArgumentOutOfRangeException>(() => db.AcknowledgeOutbox(2));
        Assert.Equal(0ul, db.GetOutboxWatermark());
        Assert.Single(db.ReadOutboxBatch(0, 10));
    }

    [Fact]
    public void Empty_compaction_preserves_next_sequence_after_reopen()
    {
        var recordId = FldDatabase.NewRecordId();
        using (var db = FldDatabase.Open(_dir))
        {
            db.Put(recordId, 1, "first"u8);
            db.Commit();
            var first = Assert.Single(db.ReadOutboxBatch(0, 10));
            Assert.Equal(1ul, first.OutboxSeq);

            db.AcknowledgeOutbox(first.OutboxSeq);
            Assert.Equal(0, db.CompactOutbox());
        }

        using (var db = FldDatabase.Open(_dir))
        {
            db.Put(recordId, 2, "second"u8);
            db.Commit();

            var second = Assert.Single(db.ReadOutboxBatch(0, 10));
            Assert.Equal(2ul, second.OutboxSeq);
            Assert.Equal(1ul, db.GetOutboxWatermark());
            Assert.Equal("second"u8.ToArray(), second.Value);
        }
    }

    [Fact]
    public void Recovery_does_not_resurrect_acknowledged_compacted_entries()
    {
        var recordId = FldDatabase.NewRecordId();
        using (var db = FldDatabase.Open(_dir))
        {
            db.Put(recordId, 1, "already-synced"u8);
            db.Commit();
            var entry = Assert.Single(db.ReadOutboxBatch(0, 10));
            db.AcknowledgeOutbox(entry.OutboxSeq);
            Assert.Equal(0, db.CompactOutbox());
        }

        // WAL is still present because no Run checkpoint was made. Recovery must honor
        // the durable Outbox materialization watermark instead of recreating ACKed data.
        using (var db = FldDatabase.Open(_dir))
        {
            Assert.Empty(db.ReadOutboxBatch(0, 10));
            Assert.Equal(1ul, db.GetOutboxWatermark());
        }
    }

    [Fact]
    public void Recovery_rebuilds_missing_outbox_from_wal_with_stable_change_id()
    {
        var recordId = FldDatabase.NewRecordId();
        byte[] expectedChangeId;
        using (var db = FldDatabase.Open(_dir))
        {
            db.Put(recordId, 7, "recover-me"u8);
            db.Commit();
            expectedChangeId = Assert.Single(db.ReadOutboxBatch(0, 10)).ChangeId;
        }

        File.Delete(Path.Combine(_dir, "outbox", "ns_0.obx"));

        using (var db = FldDatabase.Open(_dir))
        {
            var rebuilt = Assert.Single(db.ReadOutboxBatch(0, 10));
            Assert.Equal(expectedChangeId, rebuilt.ChangeId);
            Assert.Equal("recover-me"u8.ToArray(), rebuilt.Value);
        }
    }

    [Fact]
    public void Sparse_index_reads_from_the_requested_cursor()
    {
        string outboxDir = Path.Combine(_dir, "direct-outbox");
        using var outbox = OutboxLog.Open(outboxDir, namespaceId: 9);
        var author = new byte[16];
        author[0] = 0xA5;

        for (uint i = 0; i < 256; i++)
        {
            var recordId = new byte[16];
            BinaryPrimitives.WriteUInt32LittleEndian(recordId, i + 1);
            var changeId = new byte[16];
            BinaryPrimitives.WriteUInt32LittleEndian(changeId, i + 1);
            outbox.Append(new OutboxEntry
            {
                OutboxSeq = 0,
                CommitSeq = i + 1,
                ChangeId = changeId,
                RecordId = recordId,
                FieldId = 1,
                OpKind = 1,
                AuthorNodeId = author,
                Wall = i + 1,
                Logical = 0,
                KeyEpoch = 0,
                Value = new byte[] { (byte)i }
            });
        }
        outbox.Flush();

        var batch = outbox.ReadBatch(afterSeq: 129, limit: 10);
        Assert.Equal(10, batch.Count);
        Assert.Equal(130ul, batch[0].OutboxSeq);
        Assert.Equal(139ul, batch[^1].OutboxSeq);
        Assert.Equal((byte)129, batch[0].Value[0]);
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
