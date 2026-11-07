using System.Buffers.Binary;
using Fld1.Core.Hash;
using Fld1.Engine.Db;
using Fld1.Engine.Sync;
using Fld1.Engine.Outbox;
using Xunit;

namespace Fld1.Engine.Tests;

public class SyncApplyTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "fld-sync-" + Guid.NewGuid().ToString("N"));
    public SyncApplyTests() => Directory.CreateDirectory(_dir);
    public void Dispose() { try { Directory.Delete(_dir, true); } catch { } }

    private static byte[] Change(byte b)
    {
        var id = new byte[16];
        id[15] = b;
        return id;
    }

    private static byte[] Author(byte b)
    {
        var a = new byte[16];
        a[15] = b;
        return a;
    }

    [Fact]
    public void RemoteFieldWrite_requires_nonzero_stable_change_id()
    {
        var recordId = FldDatabase.NewRecordId();
        Assert.Throws<ArgumentException>(() =>
            new RemoteFieldWrite(recordId, 1, Array.Empty<byte>(), 1, 0,
                Author(1), Change(0)));

        Assert.Throws<ArgumentNullException>(() =>
            new RemoteFieldWrite(recordId, 1, Array.Empty<byte>(), 1, 0,
                Author(1), null!));
    }

    private static RemoteBatchCommit Batch(byte[] recordId, byte[] value, ulong wall, byte change, bool relay = true, ushort fieldId = 5)
    {
        return new RemoteBatchCommit(
            senderPeer: Author(8),
            senderEpoch: 1,
            namespaceId: 0,
            issuerNodeId: Author(8),
            issuerEpoch: 2,
            batchId: Change(20),
            cursorStart: 1,
            cursorEnd: 1,
            previousBatchDigest: new byte[16],
            batchDigest: Change(21),
            recordsRoot: Change(22),
            writes:
            [
                new RemoteFieldWrite(recordId, fieldId, value, wall, 0,
                    Author(9), Change(change))
            ],
            relayWrites: relay
                ? [new RemoteFieldWrite(recordId, fieldId, value, wall, 0, Author(9), Change(change))]
                : Array.Empty<RemoteFieldWrite>(),
            protocolStateDelta: [0xA1, 0xB2, 0xC3]);
    }

    [Fact]
    public void ApplyRemoteBatch_is_durable_idempotent_and_never_enters_local_outbox()
    {
        var recordId = FldDatabase.NewRecordId();
        var batch = Batch(recordId, "remote"u8.ToArray(), 100, 1);
        ulong commitSeq;

        using (var db = FldDatabase.Open(_dir))
        {
            var receipt = db.ApplyRemoteBatch(batch);
            Assert.False(receipt.IsDuplicate);
            Assert.Equal(1, receipt.AppliedCount);
            Assert.Equal(1UL, receipt.CursorEnd);
            Assert.Equal(new byte[] { 0xA1, 0xB2, 0xC3 }, receipt.ProtocolStateDelta);
            commitSeq = receipt.CommitSeq;

            Assert.True(db.TryGetBytes(recordId, 5, out var value));
            Assert.Equal("remote"u8.ToArray(), value);
            Assert.Equal(0UL, db.GetOutboxHighWatermark());

            var relay = db.ReadRelayBatches(Author(9), 0, 10);
            Assert.Single(relay);
            Assert.Equal(1UL, relay[0].JournalSeq);
            Assert.Single(relay[0].Batch.Writes);
            Assert.Equal(Change(1), relay[0].Batch.Writes[0].ChangeId);

            var duplicate = db.ApplyRemoteBatch(batch);
            Assert.True(duplicate.IsDuplicate);
            Assert.Equal(commitSeq, duplicate.CommitSeq);
            Assert.Equal(1UL, db.GetRelayJournalHighWatermark());
            Assert.Equal(0UL, db.GetOutboxHighWatermark());

            db.Checkpoint();
        }

        using (var reopened = FldDatabase.Open(_dir))
        {
            Assert.True(reopened.TryGetBytes(recordId, 5, out var value));
            Assert.Equal("remote"u8.ToArray(), value);
            Assert.Equal(0UL, reopened.GetOutboxHighWatermark());
            Assert.Single(reopened.ReadRelayBatches(Author(9), 0, 10));

            var duplicate = reopened.ApplyRemoteBatch(batch);
            Assert.True(duplicate.IsDuplicate);
            Assert.Equal(commitSeq, duplicate.CommitSeq);
        }
    }

    [Fact]
    public void ApplyJournal_stores_only_relay_payload_not_all_incoming_fields()
    {
        var writes = new List<RemoteFieldWrite>(8);
        for (byte i = 1; i <= 8; i++)
        {
            writes.Add(new RemoteFieldWrite(
                FldDatabase.NewRecordId(), (ushort)i,
                Enumerable.Repeat(i, 4096).ToArray(),
                (ulong)(100 + i), 0, Author(9), Change(i)));
        }

        var batch = new RemoteBatchCommit(
            senderPeer: Author(8),
            senderEpoch: 1,
            namespaceId: 0,
            issuerNodeId: Author(8),
            issuerEpoch: 2,
            batchId: Change(30),
            cursorStart: 1,
            cursorEnd: 8,
            previousBatchDigest: new byte[16],
            batchDigest: Change(31),
            recordsRoot: Change(32),
            writes: writes,
            relayWrites: [writes[3]],
            protocolStateDelta: [0xA1, 0xB2]);

        using (var db = FldDatabase.Open(_dir))
        {
            db.ApplyRemoteBatch(batch);

            long walBytes = Directory.EnumerateFiles(Path.Combine(_dir, "wal"), "ns_0.*.log")
                .Sum(path => new FileInfo(path).Length);
            long journalBytes = new FileInfo(Path.Combine(_dir, "outbox", "ns_0.apply")).Length;
            Assert.True(walBytes > 0);
            Assert.True(journalBytes < walBytes / 2,
                $"Relay-only journal should be much smaller than the full WAL commit (journal={journalBytes}, WAL={walBytes}).");

            var relay = Assert.Single(db.ReadRelayBatches(Author(9), 0, 10));
            var relayedWrite = Assert.Single(relay.Batch.Writes);
            Assert.Equal(Change(4), relayedWrite.ChangeId);
            Assert.Equal(writes[3].Value, relayedWrite.Value);
        }

        using var reopened = FldDatabase.Open(_dir);
        var restoredRelay = Assert.Single(reopened.ReadRelayBatches(Author(9), 0, 10));
        Assert.Single(restoredRelay.Batch.Writes);
        Assert.Equal(Change(4), restoredRelay.Batch.Writes[0].ChangeId);
    }

    [Fact]
    public void ReadRelayBatches_seeks_from_sparse_anchor_after_restart()
    {
        using (var db = FldDatabase.Open(_dir))
        {
            byte[] previousDigest = new byte[16];
            for (int i = 1; i <= 260; i++)
            {
                var write = new RemoteFieldWrite(
                    FldDatabase.NewRecordId(), 5, [(byte)i],
                    (ulong)(100 + i), 0, Author(9), FldDatabase.NewRecordId());
                var batch = new RemoteBatchCommit(
                    senderPeer: Author(8), senderEpoch: 1, namespaceId: 0,
                    issuerNodeId: Author(8), issuerEpoch: 2,
                    batchId: FldDatabase.NewRecordId(),
                    cursorStart: (ulong)i, cursorEnd: (ulong)i,
                    previousBatchDigest: previousDigest,
                    batchDigest: FldDatabase.NewRecordId(),
                    recordsRoot: FldDatabase.NewRecordId(),
                    writes: [write], relayWrites: [write],
                    protocolStateDelta: [(byte)i]);
                db.ApplyRemoteBatch(batch);
                previousDigest = batch.BatchDigest;
            }

            var page = db.ReadRelayBatches(Author(9), afterJournalSeq: 257, limit: 10);
            Assert.Equal(new ulong[] { 258, 259, 260 }, page.Select(item => item.JournalSeq));
            Assert.Equal((byte)4, Assert.Single(page[^1].Batch.Writes).Value[0]);
        }

        using var reopened = FldDatabase.Open(_dir);
        var restoredPage = reopened.ReadRelayBatches(Author(9), afterJournalSeq: 257, limit: 10);
        Assert.Equal(new ulong[] { 258, 259, 260 },
            restoredPage.Select(item => item.JournalSeq));

        var firstPage = reopened.ReadRelayBatchPage(Author(9), afterJournalSeq: 0, limit: 1);
        Assert.Single(firstPage.Batches);
        Assert.Equal(1UL, firstPage.ScannedThroughJournalSeq);

        var noMatches = reopened.ReadRelayBatchPage(Author(10), afterJournalSeq: 0, limit: 10);
        Assert.Empty(noMatches.Batches);
        Assert.Equal(260UL, noMatches.ScannedThroughJournalSeq);
    }

    private static byte[] RelayAckSidecar(byte[] author, ulong journalSeq)
    {
        var bytes = new byte[12 + 24 + 4];
        "FLRA"u8.CopyTo(bytes);
        BinaryPrimitives.WriteUInt16LittleEndian(bytes.AsSpan(4, 2), 1);
        BinaryPrimitives.WriteUInt16LittleEndian(bytes.AsSpan(6, 2), 0);
        BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan(8, 4), 1);
        author.CopyTo(bytes.AsSpan(12, 16));
        BinaryPrimitives.WriteUInt64LittleEndian(bytes.AsSpan(28, 8), journalSeq);
        uint crc = Hashes.Crc32C(bytes.AsSpan(0, 12), bytes.AsSpan(12, 24));
        BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan(36, 4), crc);
        return bytes;
    }

    [Fact]
    public void ApplyJournal_rejects_relay_ack_sidecar_when_journal_is_missing()
    {
        var outboxDir = Path.Combine(_dir, "outbox");
        Directory.CreateDirectory(outboxDir);
        File.WriteAllBytes(Path.Combine(outboxDir, "ns_0.apply.relayack"),
            RelayAckSidecar(Author(9), 1));

        Assert.Throws<InvalidDataException>(() =>
            FldDatabase.Open(_dir, localAuthor: Author(1)));
    }

    [Fact]
    public void ApplyJournal_rejects_relay_ack_watermark_beyond_journal_high_watermark()
    {
        using (FldDatabase.Open(_dir, localAuthor: Author(1))) { }
        File.WriteAllBytes(Path.Combine(_dir, "outbox", "ns_0.apply.relayack"),
            RelayAckSidecar(Author(9), 1));

        Assert.Throws<InvalidDataException>(() =>
            FldDatabase.Open(_dir, localAuthor: Author(1)));
    }

    [Fact]
    public void Relay_ack_sidecar_survives_restart_and_compacts_only_acknowledged_author()
    {
        var authorA = Author(9);
        var authorB = Author(10);
        var writeA = new RemoteFieldWrite(
            FldDatabase.NewRecordId(), 1, [0xA1], 100, 0, authorA, Change(41));
        var writeB = new RemoteFieldWrite(
            FldDatabase.NewRecordId(), 2, [0xB2], 101, 0, authorB, Change(42));
        var batch = new RemoteBatchCommit(
            senderPeer: Author(8), senderEpoch: 1, namespaceId: 0,
            issuerNodeId: Author(8), issuerEpoch: 2, batchId: Change(43),
            cursorStart: 1, cursorEnd: 1,
            previousBatchDigest: new byte[16], batchDigest: Change(44),
            recordsRoot: Change(45), writes: [writeA, writeB],
            relayWrites: [writeA, writeB], protocolStateDelta: [0xC1]);

        using (var db = FldDatabase.Open(_dir))
        {
            db.ApplyRemoteBatch(batch);
            db.AcknowledgeRelayBatches(authorA, safeThroughJournalSeq: 1);

            var ackedBeforeRestart = db.ReadRelayBatchPage(authorA, 0, 10);
            Assert.Empty(ackedBeforeRestart.Batches);
            Assert.Equal(1UL, ackedBeforeRestart.ScannedThroughJournalSeq);
            Assert.Single(db.ReadRelayBatches(authorB, 0, 10));
        }

        using (var reopened = FldDatabase.Open(_dir))
        {
            // WAL replay sees the original full commit, but the durable receipt and
            // ACK sidecar suppress only author A; author B's relay payload remains.
            Assert.Empty(reopened.ReadRelayBatches(authorA, 0, 10));
            Assert.Single(reopened.ReadRelayBatches(authorB, 0, 10));

            long journalBeforeCompact = new FileInfo(Path.Combine(_dir, "outbox", "ns_0.apply")).Length;
            reopened.Checkpoint();
            long journalAfterCompact = new FileInfo(Path.Combine(_dir, "outbox", "ns_0.apply")).Length;
            Assert.True(journalAfterCompact < journalBeforeCompact,
                $"Acknowledged relay payload should be reclaimed after WAL seal (before={journalBeforeCompact}, after={journalAfterCompact}).");
            var duplicate = reopened.ApplyRemoteBatch(batch);
            Assert.True(duplicate.IsDuplicate);
        }

        using var compacted = FldDatabase.Open(_dir);
        Assert.Empty(compacted.ReadRelayBatches(authorA, 0, 10));
        var remaining = Assert.Single(compacted.ReadRelayBatches(authorB, 0, 10));
        Assert.Single(remaining.Batch.Writes);
        Assert.Equal(Change(42), remaining.Batch.Writes[0].ChangeId);
        Assert.True(compacted.ApplyRemoteBatch(batch).IsDuplicate);
    }

    [Fact]
    public void ApplyJournal_is_rebuilt_from_wal_if_materialization_is_missing()
    {
        var recordId = FldDatabase.NewRecordId();
        var batch = Batch(recordId, "recover-me"u8.ToArray(), 100, 5);
        using (var db = FldDatabase.Open(_dir))
            db.ApplyRemoteBatch(batch);

        File.Delete(Path.Combine(_dir, "outbox", "ns_0.apply"));

        using var reopened = FldDatabase.Open(_dir);
        Assert.True(reopened.TryGetBytes(recordId, 5, out var value));
        Assert.Equal("recover-me"u8.ToArray(), value);
        Assert.Single(reopened.ReadRelayBatches(Author(9), 0, 10));
        Assert.Equal(1UL, reopened.GetRelayJournalHighWatermark());
    }

    [Fact]
    public void ApplyRemoteBatch_persists_batch_without_enqueuing_non_relayable_change()
    {
        using var db = FldDatabase.Open(_dir);
        var recordId = FldDatabase.NewRecordId();
        var receipt = db.ApplyRemoteBatch(
            Batch(recordId, "accepted"u8.ToArray(), 100, 4, relay: false));

        Assert.Equal(1, receipt.AppliedCount);
        Assert.True(db.TryGetBytes(recordId, 5, out var value));
        Assert.Equal("accepted"u8.ToArray(), value);
        Assert.Empty(db.ReadRelayBatches(Author(9), 0, 10));
        Assert.Equal(1UL, db.GetRelayJournalHighWatermark());
    }

    [Fact]
    public void ApplyRemoteBatch_retains_lww_loser_for_relay()
    {
        using var db = FldDatabase.Open(_dir);
        var recordId = FldDatabase.NewRecordId();
        db.Put(recordId, 5, "local-newer"u8);
        db.Commit();
        db.Checkpoint();

        var receipt = db.ApplyRemoteBatch(Batch(recordId, "remote-older"u8.ToArray(), 1, 2));
        Assert.Equal(0, receipt.AppliedCount);
        Assert.True(db.TryGetBytes(recordId, 5, out var value));
        Assert.Equal("local-newer"u8.ToArray(), value);
        Assert.Single(db.ReadRelayBatches(Author(9), 0, 10));
        Assert.Equal(1UL, db.GetOutboxHighWatermark());

        // The remote operation loses LWW, so there is no overlay delta to write.
        // Checkpoint must still seal its WAL commit without discarding relay history.
        db.Checkpoint();
        db.Dispose();
        using var reopened = FldDatabase.Open(_dir);
        Assert.True(reopened.TryGetBytes(recordId, 5, out var afterRestart));
        Assert.Equal("local-newer"u8.ToArray(), afterRestart);
        Assert.Single(reopened.ReadRelayBatches(Author(9), 0, 10));
    }

    [Fact]
    public void ApplyRemoteBatch_does_not_reclassify_pending_local_write()
    {
        using var db = FldDatabase.Open(_dir);
        var recordId = FldDatabase.NewRecordId();
        db.Put(recordId, 5, "local"u8);
        db.ApplyRemoteBatch(Batch(recordId, "remote-winner"u8.ToArray(), ulong.MaxValue, 3));

        db.Commit();

        var outbox = db.ReadOutboxBatch(0, 10);
        Assert.Single(outbox);
        Assert.Equal(db.LocalAuthor.ToArray(), outbox[0].AuthorNodeId);
        Assert.Equal("local"u8.ToArray(), outbox[0].Value);
        Assert.True(db.TryGetBytes(recordId, 5, out var visible));
        Assert.Equal("remote-winner"u8.ToArray(), visible);
    }

    [Fact]
    public void ApplyRemote_wins_with_newer_hlc()
    {
        using var db = FldDatabase.Open(_dir);
        var rec = FldDatabase.NewRecordId();
        db.Put(rec, 1, "local"u8);
        db.Commit();
        db.Checkpoint();

        var receipt = db.ApplyRemoteBatch(Batch(
            rec, "remote"u8.ToArray(), ulong.MaxValue / 2, 1, fieldId: 1));
        Assert.Equal(1, receipt.AppliedCount);
        Assert.True(db.TryGetBytes(rec, 1, out var v) && v!.SequenceEqual("remote"u8.ToArray()));
    }

    [Fact]
    public void ApplyRemote_loses_to_newer_local()
    {
        using var db = FldDatabase.Open(_dir);
        var rec = FldDatabase.NewRecordId();
        db.Put(rec, 1, "local-new"u8);
        // remote with wall=1 loses to local HLC from clock
        var receipt = db.ApplyRemoteBatch(Batch(
            rec, "old"u8.ToArray(), 1, 2, fieldId: 1));
        Assert.Equal(0, receipt.AppliedCount);
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

        var lease = db.AcquireOutboxLease(Author(8));
        int visited = db.VisitOutboxBatch(0, 10, lease, Visit);
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
            release.Wait();
            return false;
        }

        var lease = db.AcquireOutboxLease(Author(9));
        var visitTask = Task.Run(() => db.VisitOutboxBatch(0, 1, lease, Block));
        try
        {
            Assert.True(entered.Wait(TimeSpan.FromSeconds(5)));
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
    public void Outbox_lease_fences_previous_consumers_and_ack()
    {
        using var db = FldDatabase.Open(_dir);
        var rec = FldDatabase.NewRecordId();
        db.Put(rec, 1, "leased"u8);
        db.Commit();

        var first = db.AcquireOutboxLease(Author(1));
        var second = db.AcquireOutboxLease(Author(2));

        bool Visit(in Fld1.Engine.Outbox.OutboxEntryView entry) => true;
        Assert.Throws<InvalidOperationException>(
            () => db.VisitOutboxBatch(0, 10, first, Visit));
        Assert.Throws<InvalidOperationException>(() => db.AcknowledgeOutbox(1, first));

        Assert.Equal(1, db.VisitOutboxBatch(0, 10, second, Visit));
        db.AcknowledgeOutbox(1, second);
        Assert.Equal(1UL, db.GetOutboxWatermark());

        db.ReleaseOutboxLease(second);
        Assert.Throws<InvalidOperationException>(
            () => db.VisitOutboxBatch(0, 10, second, Visit));
    }

    [Fact]
    public void Outbox_lease_epoch_survives_reopen()
    {
        OutboxLease first;
        using (var db = FldDatabase.Open(_dir))
            first = db.AcquireOutboxLease(Author(3));

        using var reopened = FldDatabase.Open(_dir);
        bool Visit(in Fld1.Engine.Outbox.OutboxEntryView entry) => true;
        Assert.Throws<InvalidOperationException>(
            () => reopened.VisitOutboxBatch(0, 10, first, Visit));

        var second = reopened.AcquireOutboxLease(Author(4));
        Assert.True(second.Epoch > first.Epoch);
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
        Assert.Equal(batch[1].OutboxSeq, db.GetOutboxHighWatermark());
        db.AcknowledgeOutbox(batch[0].OutboxSeq);
        Assert.Equal(batch[0].OutboxSeq, db.GetOutboxWatermark());
        int kept = db.CompactOutbox();
        Assert.Equal(1, kept);
        var rest = db.ReadOutboxBatch(0, 10);
        Assert.Single(rest);
        Assert.Equal(batch[1].OutboxSeq, rest[0].OutboxSeq);
    }
}
