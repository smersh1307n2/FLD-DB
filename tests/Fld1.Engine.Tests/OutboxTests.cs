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
    public void Local_operation_receipt_is_atomic_idempotent_and_survives_checkpoint_wal_truncation()
    {
        byte[] recordId = FldDatabase.NewRecordId();
        byte[] clientId = new byte[16]; clientId[0] = 0x51;
        byte[] operationId = new byte[16]; operationId[0] = 0x52;
        byte[] fingerprint = Enumerable.Range(0, 32).Select(value => (byte)(value + 1)).ToArray();
        byte[] response = "stable-original-result"u8.ToArray();
        var receipt = new FldLocalOperationReceipt
        {
            ClientId = clientId,
            OperationId = operationId,
            ClientSequence = 1,
            Fingerprint = fingerprint,
            Result = response
        };
        var options = new FldOpenOptions
        {
            Path = _dir,
            WalRotateAfterBytes = 1
        };

        ulong originalCommitSeq;
        using (var db = FldDatabase.Open(options))
        {
            db.Put(recordId, 4, "atomic-field-value"u8);
            FldLocalOperationCommitResult committed = db.CommitWithReceipt(receipt);
            originalCommitSeq = committed.CommitSeq;
            Assert.Equal(1ul, originalCommitSeq);
            Assert.False(committed.IsDuplicate);
            Assert.Equal(response, committed.Result);
            Assert.Single(db.ReadOutboxBatch(0, 10));
            Assert.True(db.TryGetLocalOperationReceipt(
                clientId, operationId, 1, fingerprint, out var found));
            Assert.Equal(originalCommitSeq, found!.CommitSeq);
            Assert.Equal(response, found.Result);

            // Force the receipt into the manifest before the covered WAL segment is removed.
            db.Checkpoint();
        }

        string[] remainingWal = Directory.GetFiles(
            Path.Combine(_dir, "wal"), "ns_0.*.log");
        Assert.Single(remainingWal);

        using (var reopened = FldDatabase.Open(options))
        {
            Assert.True(reopened.TryGetLocalOperationReceipt(
                clientId, operationId, 1, fingerprint, out var found));
            Assert.Equal(originalCommitSeq, found!.CommitSeq);
            Assert.Equal(response, found.Result);

            FldLocalOperationCommitResult replay = reopened.CommitWithReceipt(receipt);
            Assert.True(replay.IsDuplicate);
            Assert.Equal(originalCommitSeq, replay.CommitSeq);
            Assert.Equal(response, replay.Result);
            Assert.Empty(reopened.ReadOutboxBatch(1, 10));

            byte[] conflictingFingerprint = fingerprint.ToArray();
            conflictingFingerprint[0] ^= 0x80;
            Assert.Throws<InvalidDataException>(() => reopened.TryGetLocalOperationReceipt(
                clientId, operationId, 1, conflictingFingerprint, out _));
        }
    }

    [Fact]
    public void Local_operation_receipt_can_commit_a_read_only_operation_without_field_mutations()
    {
        byte[] clientId = new byte[16]; clientId[0] = 0x61;
        byte[] operationId = new byte[16]; operationId[0] = 0x62;
        byte[] fingerprint = new byte[32]; fingerprint[0] = 0x63;
        byte[] response = "read-result"u8.ToArray();

        using (var db = FldDatabase.Open(_dir))
        {
            FldLocalOperationCommitResult committed = db.CommitWithReceipt(new FldLocalOperationReceipt
            {
                ClientId = clientId,
                OperationId = operationId,
                ClientSequence = 1,
                Fingerprint = fingerprint,
                Result = response
            });
            Assert.Equal(1ul, committed.CommitSeq);
            Assert.Empty(db.ReadOutboxBatch(0, 10));
        }

        using (var reopened = FldDatabase.Open(_dir))
        {
            Assert.True(reopened.TryGetLocalOperationReceipt(
                clientId, operationId, 1, fingerprint, out var replay));
            Assert.Equal(response, replay!.Result);
            Assert.Equal(1ul, replay.CommitSeq);
        }
    }

    [Fact]
    public void Client_acknowledgement_retires_only_a_contiguous_receipt_prefix_and_survives_recovery()
    {
        byte[] clientId = new byte[16]; clientId[0] = 0x71;
        byte[] operation1 = new byte[16]; operation1[0] = 0x72;
        byte[] operation2 = new byte[16]; operation2[0] = 0x73;
        byte[] fingerprint1 = new byte[32]; fingerprint1[0] = 0x74;
        byte[] fingerprint2 = new byte[32]; fingerprint2[0] = 0x75;
        var first = new FldLocalOperationReceipt
        {
            ClientId = clientId, OperationId = operation1, ClientSequence = 1,
            Fingerprint = fingerprint1, Result = "one"u8.ToArray()
        };
        var second = new FldLocalOperationReceipt
        {
            ClientId = clientId, OperationId = operation2, ClientSequence = 2,
            Fingerprint = fingerprint2, Result = "two"u8.ToArray()
        };

        using (var db = FldDatabase.Open(_dir))
        {
            db.Put(FldDatabase.NewRecordId(), 1, "first"u8);
            db.CommitWithReceipt(first);
            db.Put(FldDatabase.NewRecordId(), 1, "second"u8);
            db.CommitWithReceipt(second);

            Assert.True(db.AcknowledgeLocalOperationReceipts(clientId, 1));
            Assert.False(db.AcknowledgeLocalOperationReceipts(clientId, 1));
            Assert.Throws<ArgumentOutOfRangeException>(
                () => db.AcknowledgeLocalOperationReceipts(clientId, 3));
            Assert.Throws<InvalidOperationException>(() => db.TryGetLocalOperationReceipt(
                clientId, operation1, 1, fingerprint1, out _));
            Assert.True(db.TryGetLocalOperationReceipt(
                clientId, operation2, 2, fingerprint2, out var retained));
            Assert.Equal("two"u8.ToArray(), retained!.Result);
            Assert.Equal(2, db.ReadOutboxBatch(0, 10).Count);
        }

        // Dispose persisted the watermark, while the WAL still contains both commits and ACK.
        using (var reopened = FldDatabase.Open(_dir))
        {
            Assert.Throws<InvalidOperationException>(() => reopened.TryGetLocalOperationReceipt(
                clientId, operation1, 1, fingerprint1, out _));
            Assert.True(reopened.TryGetLocalOperationReceipt(
                clientId, operation2, 2, fingerprint2, out var retained));
            Assert.Equal("two"u8.ToArray(), retained!.Result);

            Assert.True(reopened.AcknowledgeLocalOperationReceipts(clientId, 2));
            Assert.False(reopened.AcknowledgeLocalOperationReceipts(clientId, 2));
            reopened.Checkpoint();
        }

        using (var compacted = FldDatabase.Open(_dir))
        {
            Assert.Throws<InvalidOperationException>(() => compacted.TryGetLocalOperationReceipt(
                clientId, operation1, 1, fingerprint1, out _));
            Assert.Throws<InvalidOperationException>(() => compacted.TryGetLocalOperationReceipt(
                clientId, operation2, 2, fingerprint2, out _));

            byte[] operation3 = new byte[16]; operation3[0] = 0x76;
            byte[] fingerprint3 = new byte[32]; fingerprint3[0] = 0x77;
            FldLocalOperationCommitResult next = compacted.CommitWithReceipt(new FldLocalOperationReceipt
            {
                ClientId = clientId,
                OperationId = operation3,
                ClientSequence = 3,
                Fingerprint = fingerprint3,
                Result = "three"u8.ToArray()
            });
            Assert.Equal(5ul, next.CommitSeq); // two field commits + two durable ACK records
            Assert.Equal(2, compacted.ReadOutboxBatch(0, 10).Count);
        }
    }

    [Fact]
    public void Local_identity_draft_with_unassigned_origin_sequence_survives_wal_recovery()
    {
        var recordId = FldDatabase.NewRecordId();
        var changeId = new byte[16];
        changeId[0] = 0xA7;

        using (var db = FldDatabase.Open(_dir))
        {
            db.PutWithIdentity(
                recordId, 7, "draft"u8, changeId,
                originAuthorEpoch: 6, wall: 123, logical: 0);
            db.Commit();
        }

        using (var reopened = FldDatabase.Open(_dir))
        {
            Assert.True(reopened.TryResolve(recordId, 7, out var resolved));
            Assert.Equal(changeId, resolved.ChangeId);
            Assert.Equal(6U, resolved.OriginAuthorEpoch);
            Assert.Equal(0UL, resolved.OriginSequence);
            Assert.Single(reopened.ReadOutboxBatch(0, 10));
        }
    }

    [Fact]
    public void Provider_snapshot_preserves_unpublished_identity_draft_with_zero_origin_sequence()
    {
        byte[] recordId = FldDatabase.NewRecordId();
        byte[] changeId = new byte[16];
        changeId[0] = 0xA8;

        using (var db = FldDatabase.Open(_dir))
        {
            db.PutWithIdentity(
                recordId, 7, "snapshot-draft"u8, changeId,
                originAuthorEpoch: 6, wall: 124, logical: 0);
            db.Commit();

            using var snapshot = db.OpenProviderSnapshot();
            FldProviderSnapshotEntry entry = Assert.Single(snapshot.ReadPage(0, 10).Entries);
            Assert.Equal(changeId, entry.ChangeId);
            Assert.Equal(6U, entry.OriginAuthorEpoch);
            Assert.Equal(0UL, entry.OriginSequence);
            Assert.Equal("snapshot-draft"u8.ToArray(), entry.Value);
        }

        // Reopen the persisted snapshot manifest and its overlay Run. Draft identity
        // must remain present without inventing a publication sequence.
        using (var reopened = FldDatabase.Open(_dir))
        using (var snapshot = reopened.OpenProviderSnapshot())
        {
            FldProviderSnapshotEntry entry = Assert.Single(snapshot.ReadPage(0, 10).Entries);
            Assert.Equal(changeId, entry.ChangeId);
            Assert.Equal(6U, entry.OriginAuthorEpoch);
            Assert.Equal(0UL, entry.OriginSequence);
            Assert.Equal("snapshot-draft"u8.ToArray(), entry.Value);
        }
    }

    [Fact]
    public void Commit_published_origin_assigns_sequence_to_epoch_stamped_local_draft()
    {
        byte[] recordId = FldDatabase.NewRecordId();
        byte[] changeId = new byte[16];
        changeId[0] = 0xA9;
        var published = new FldPublishedOrigin
        {
            RecordId = recordId,
            FieldId = 7,
            ChangeId = changeId,
            OriginAuthorEpoch = 6,
            OriginSequence = 1
        };

        using (var db = FldDatabase.Open(_dir))
        {
            db.PutWithIdentity(
                recordId, 7, "publish-draft"u8, changeId,
                originAuthorEpoch: 6, wall: 125, logical: 0);
            db.Commit();

            Assert.Equal(1, db.CommitPublishedOrigins(new[] { published }));
            Assert.Equal(0, db.CommitPublishedOrigins(new[] { published }));
            Assert.Throws<InvalidDataException>(() => db.CommitPublishedOrigins(new[]
            {
                new FldPublishedOrigin
                {
                    RecordId = recordId,
                    FieldId = 7,
                    ChangeId = changeId,
                    OriginAuthorEpoch = 6,
                    OriginSequence = 2
                }
            }));
        }

        using (var reopened = FldDatabase.Open(_dir))
        using (var snapshot = reopened.OpenProviderSnapshot())
        {
            FldProviderSnapshotEntry entry = Assert.Single(snapshot.ReadPage(0, 10).Entries);
            Assert.Equal(changeId, entry.ChangeId);
            Assert.Equal(6U, entry.OriginAuthorEpoch);
            Assert.Equal(1UL, entry.OriginSequence);
        }
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
    public void Torn_tail_after_materialization_fails_closed_without_truncating()
    {
        string outboxDir = Path.Combine(_dir, "materialized-outbox");
        string outboxPath = Path.Combine(outboxDir, "ns_11.obx");
        var author = new byte[16];
        author[0] = 0xA1;
        var changeId = new byte[16];
        changeId[0] = 1;
        var recordId = new byte[16];
        recordId[0] = 2;

        using (var outbox = OutboxLog.Open(outboxDir, namespaceId: 11))
        {
            outbox.Append(new OutboxEntry
            {
                OutboxSeq = 0,
                CommitSeq = 1,
                ChangeId = changeId,
                RecordId = recordId,
                FieldId = 3,
                OpKind = 1,
                AuthorNodeId = author,
                Wall = 1,
                Logical = 0,
                KeyEpoch = 0,
                Value = "durable"u8.ToArray()
            });
            outbox.Flush();
            outbox.MarkMaterialized(1);
        }

        using (var stream = new FileStream(outboxPath, FileMode.Append, FileAccess.Write, FileShare.None))
            stream.Write(new byte[] { 0x01, 0x02, 0x03 });

        long damagedLength = new FileInfo(outboxPath).Length;
        Assert.Throws<InvalidDataException>(() => OutboxLog.Open(outboxDir, namespaceId: 11));
        Assert.Equal(damagedLength, new FileInfo(outboxPath).Length);
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
    public void Sequence_exhaustion_is_rejected_before_log_is_modified()
    {
        string outboxDir = Path.Combine(_dir, "sequence-exhaustion");
        string outboxPath = Path.Combine(outboxDir, "ns_17.obx");
        using (var outbox = OutboxLog.Open(outboxDir, namespaceId: 17))
        {
            var nextSeq = typeof(OutboxLog).GetField(
                "_nextSeq",
                System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic)!;
            nextSeq.SetValue(outbox, ulong.MaxValue);

            long originalLength = new FileInfo(outboxPath).Length;
            var author = new byte[16];
            var changeId = new byte[16];
            changeId[0] = 1;
            var recordId = new byte[16];
            recordId[0] = 2;

            Assert.Throws<InvalidOperationException>(() => outbox.Append(new OutboxEntry
            {
                OutboxSeq = 0,
                CommitSeq = 1,
                ChangeId = changeId,
                RecordId = recordId,
                FieldId = 1,
                OpKind = 1,
                AuthorNodeId = author,
                Wall = 1,
                Logical = 0,
                KeyEpoch = 0,
                Value = "value"u8.ToArray()
            }));

            Assert.Equal(originalLength, new FileInfo(outboxPath).Length);
        }

        // The failed append did not leave a terminal sequence record or torn tail.
        using var reopened = OutboxLog.Open(outboxDir, namespaceId: 17);
        Assert.Empty(reopened.ReadBatch(0, 10));
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
