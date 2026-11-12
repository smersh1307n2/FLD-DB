using Fld1.Engine.Db;
using Fld1.Engine.Sync;
using Xunit;

namespace Fld1.Engine.Db.Tests;

public sealed class FldDatabaseReferenceSnapshotTests
{
    [Fact]
    public void Reference_snapshot_tracks_unpublished_local_outbox_identity_until_acknowledged()
    {
        string root = Path.Combine(Path.GetTempPath(), "fld-reference-snapshot-local-" + Guid.NewGuid().ToString("N"));
        const uint namespaceId = 0x741;
        try
        {
            using var database = FldDatabase.Open(root, namespaceId, Id(0x31));
            byte[] recordId = FldDatabase.NewRecordId();
            database.Put(recordId, 7, "local-value"u8);
            Assert.NotEqual(0UL, database.Commit());

            var entry = Assert.Single(database.ReadOutboxBatch(0, 10));
            using (FldReferenceSnapshot references = database.OpenReferenceSnapshot())
            {
                Assert.True(references.UnpublishedLocalDraftHorizon.Count > 0);
                Assert.True(references.HasChangeIdReference(entry.ChangeId));
                Assert.True(references.HasRecordReference(entry.RecordId, entry.ChangeId));
            }

            // Sealing the WAL does not retire an unacknowledged Outbox identity.
            Assert.NotEqual(0UL, database.Checkpoint());
            using (FldReferenceSnapshot sealedReferences = database.OpenReferenceSnapshot())
            {
                Assert.True(sealedReferences.HasChangeIdReference(entry.ChangeId));
                Assert.True(sealedReferences.HasRecordReference(entry.RecordId, entry.ChangeId));
            }

            OutboxLease lease = database.AcquireOutboxLease(Id(0x32));
            database.AcknowledgeOutbox(entry.OutboxSeq, lease);
            using FldReferenceSnapshot acknowledged = database.OpenReferenceSnapshot();
            Assert.Equal(0UL, acknowledged.UnpublishedLocalDraftHorizon.Count);
            Assert.False(acknowledged.HasChangeIdReference(entry.ChangeId));
            Assert.False(acknowledged.HasRecordReference(entry.RecordId, entry.ChangeId));
        }
        finally
        {
            try { Directory.Delete(root, recursive: true); } catch { }
        }
    }

    [Fact]
    public void Reference_snapshot_tracks_apply_journal_change_record_and_transport_receipt()
    {
        string root = Path.Combine(Path.GetTempPath(), "fld-reference-snapshot-apply-" + Guid.NewGuid().ToString("N"));
        const uint namespaceId = 0x742;
        try
        {
            using var database = FldDatabase.Open(root, namespaceId, Id(0x41));
            byte[] recordId = FldDatabase.NewRecordId();
            byte[] changeId = Id(0x71);
            var write = new RemoteFieldWrite(
                recordId, 5, "remote-value"u8.ToArray(),
                100, 2, Id(0x61), changeId, keyEpoch: 4,
                originAuthorEpoch: 8, originSequence: 9);
            var batch = new RemoteBatchCommit(
                senderPeer: Id(0x51),
                senderEpoch: 6,
                namespaceId: checked((int)namespaceId),
                issuerNodeId: Id(0x53),
                issuerEpoch: 8,
                batchId: Id(0x52),
                cursorStart: 9,
                cursorEnd: 10,
                previousBatchDigest: Id(0x54),
                batchDigest: Id(0x55),
                recordsRoot: Id(0x56),
                writes: [write],
                relayWrites: [],
                protocolStateDelta: [0xA1]);

            RemoteBatchReceipt receipt = database.ApplyRemoteBatch(batch);
            Assert.False(receipt.IsDuplicate);
            Assert.NotEqual(0UL, database.Checkpoint());

            using FldReferenceSnapshot references = database.OpenReferenceSnapshot();
            Assert.Equal(0UL, references.UnpublishedLocalDraftHorizon.Count);
            Assert.True(references.HasChangeIdReference(changeId));
            Assert.True(references.HasRecordReference(recordId, changeId));
            Assert.True(references.HasReceiptReference(
                Id(0x51), transportPeerEpoch: 6, authorEpoch: 8,
                namespaceId: checked((ushort)namespaceId), batchId: Id(0x52)));
            Assert.False(references.HasReceiptReference(
                Id(0x51), transportPeerEpoch: 7, authorEpoch: 8,
                namespaceId: checked((ushort)namespaceId), batchId: Id(0x52)));
        }
        finally
        {
            try { Directory.Delete(root, recursive: true); } catch { }
        }
    }

    [Fact]
    public void Provider_snapshot_pin_blocks_change_and_record_reclamation_but_not_receipt_horizon()
    {
        string root = Path.Combine(Path.GetTempPath(), "fld-reference-snapshot-pin-" + Guid.NewGuid().ToString("N"));
        const uint namespaceId = 0x743;
        try
        {
            using var database = FldDatabase.Open(root, namespaceId, Id(0x31));
            byte[] recordId = FldDatabase.NewRecordId();
            database.Put(recordId, 9, "snapshot-value"u8);
            Assert.NotEqual(0UL, database.Commit());
            Assert.NotEqual(0UL, database.Checkpoint());

            using FldProviderSnapshot providerSnapshot = database.OpenProviderSnapshot();
            using FldReferenceSnapshot references = database.OpenReferenceSnapshot();
            Assert.True(references.HasSnapshotPin);
            Assert.True(references.HasChangeIdReference(Id(0x72)));
            Assert.True(references.HasRecordReference(recordId, Id(0x72)));
            Assert.False(references.HasReceiptReference(
                Id(0x51), transportPeerEpoch: 1, authorEpoch: 1,
                namespaceId: checked((ushort)namespaceId), batchId: Id(0x52)));
        }
        finally
        {
            try { Directory.Delete(root, recursive: true); } catch { }
        }
    }

    private static byte[] Id(byte value)
    {
        byte[] id = new byte[16];
        id[0] = value;
        return id;
    }
}
