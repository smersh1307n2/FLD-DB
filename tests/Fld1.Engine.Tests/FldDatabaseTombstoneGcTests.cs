using Fld1.Engine.Db;
using Fld1.Engine.Outbox;
using Xunit;

namespace Fld1.Engine.Db.Tests;

public sealed class FldDatabaseTombstoneGcTests
{
    [Fact]
    public void Field_tombstone_gc_requires_exact_approval_and_does_not_resurrect_old_value()
    {
        string root = Path.Combine(Path.GetTempPath(), "fld-tombstone-gc-field-" + Guid.NewGuid().ToString("N"));
        const uint namespaceId = 0x751;
        byte[] recordId = FldDatabase.NewRecordId();
        try
        {
            using (var database = FldDatabase.Open(root, namespaceId, Id(0x31)))
            {
                database.Put(recordId, 7, "before-delete"u8);
                Assert.NotEqual(0UL, database.Commit());
                Assert.NotEqual(0UL, database.Checkpoint());

                database.Delete(recordId, 7);
                Assert.NotEqual(0UL, database.Commit());
                Assert.NotEqual(0UL, database.Checkpoint());
                SealAndAcknowledgeOutbox(database);

                bool sawCandidate = false;
                int rejected = database.CompactTombstones(candidate =>
                {
                    sawCandidate = true;
                    Assert.Equal(recordId, candidate.RecordId.ToArray());
                    Assert.Equal((ushort)7, candidate.FieldId);
                    Assert.False(candidate.IsRecordTombstone);
                    Assert.True(candidate.IsFieldTombstone);
                    Assert.Equal(16, candidate.ChangeId.Length);
                    return false;
                });
                Assert.True(sawCandidate);
                Assert.Equal(0, rejected);
                Assert.True(database.TryResolve(recordId, 7, out FieldResolver.Resolved retained));
                Assert.True(retained.IsTombstone);

                int reclaimed = database.CompactTombstones(candidate =>
                {
                    Assert.True(candidate.IsFieldTombstone);
                    return true;
                });
                Assert.Equal(1, reclaimed);
                Assert.False(database.TryGetBytes(recordId, 7, out _));
            }

            using var reopened = FldDatabase.Open(root, namespaceId, Id(0x31));
            Assert.False(reopened.TryGetBytes(recordId, 7, out _));
            Assert.False(reopened.TryResolve(recordId, 7, out _));
        }
        finally
        {
            try { Directory.Delete(root, recursive: true); } catch { }
        }
    }

    [Fact]
    public void Record_tombstone_gc_removes_older_fields_but_preserves_newer_fields()
    {
        string root = Path.Combine(Path.GetTempPath(), "fld-tombstone-gc-record-" + Guid.NewGuid().ToString("N"));
        const uint namespaceId = 0x752;
        byte[] recordId = FldDatabase.NewRecordId();
        try
        {
            using (var database = FldDatabase.Open(root, namespaceId, Id(0x41)))
            {
                database.Put(recordId, 7, "before-record-delete"u8);
                Assert.NotEqual(0UL, database.Commit());
                Assert.NotEqual(0UL, database.Checkpoint());

                database.DeleteRecord(recordId);
                Assert.NotEqual(0UL, database.Commit());
                Assert.NotEqual(0UL, database.Checkpoint());

                database.Put(recordId, 9, "newer-field"u8);
                Assert.NotEqual(0UL, database.Commit());
                Assert.NotEqual(0UL, database.Checkpoint());
                SealAndAcknowledgeOutbox(database);

                int reclaimed = database.CompactTombstones(candidate =>
                {
                    Assert.True(candidate.IsRecordTombstone);
                    Assert.False(candidate.IsFieldTombstone);
                    Assert.Equal(FldLifecycle.FieldId, candidate.FieldId);
                    return true;
                });
                Assert.Equal(1, reclaimed);
                Assert.False(database.TryGetBytes(recordId, 7, out _));
                Assert.True(database.TryGetBytes(recordId, 9, out byte[]? newer));
                Assert.Equal("newer-field"u8.ToArray(), newer);
                Assert.False(database.IsRecordDead(recordId));
            }

            using var reopened = FldDatabase.Open(root, namespaceId, Id(0x41));
            Assert.False(reopened.TryGetBytes(recordId, 7, out _));
            Assert.True(reopened.TryGetBytes(recordId, 9, out byte[]? newer));
            Assert.Equal("newer-field"u8.ToArray(), newer);
            Assert.False(reopened.IsRecordDead(recordId));
        }
        finally
        {
            try { Directory.Delete(root, recursive: true); } catch { }
        }
    }

    [Fact]
    public void Field_tombstone_is_not_reclaimed_while_its_record_tombstone_is_unproven()
    {
        string root = Path.Combine(Path.GetTempPath(), "fld-tombstone-gc-record-fence-" + Guid.NewGuid().ToString("N"));
        const uint namespaceId = 0x753;
        byte[] recordId = FldDatabase.NewRecordId();
        try
        {
            using var database = FldDatabase.Open(root, namespaceId, Id(0x51));
            database.Put(recordId, 7, "old-value"u8);
            Assert.NotEqual(0UL, database.Commit());
            Assert.NotEqual(0UL, database.Checkpoint());
            database.Delete(recordId, 7);
            Assert.NotEqual(0UL, database.Commit());
            Assert.NotEqual(0UL, database.Checkpoint());
            database.DeleteRecord(recordId);
            Assert.NotEqual(0UL, database.Commit());
            Assert.NotEqual(0UL, database.Checkpoint());
            SealAndAcknowledgeOutbox(database);

            int fieldTombstoneCallbacks = 0;
            int reclaimed = database.CompactTombstones(candidate =>
            {
                Assert.True(candidate.IsRecordTombstone);
                Assert.False(candidate.IsFieldTombstone);
                return false;
            });
            Assert.Equal(0, reclaimed);
            Assert.Equal(0, fieldTombstoneCallbacks);
            Assert.True(database.TryResolve(recordId, 7, out FieldResolver.Resolved retained));
            Assert.True(retained.IsTombstone);

            database.UndeleteRecord(recordId);
            Assert.NotEqual(0UL, database.Commit());
            Assert.NotEqual(0UL, database.Checkpoint());
            SealAndAcknowledgeOutbox(database);
            Assert.False(database.TryGetBytes(recordId, 7, out _));
        }
        finally
        {
            try { Directory.Delete(root, recursive: true); } catch { }
        }
    }

    private static void SealAndAcknowledgeOutbox(FldDatabase database)
    {
        ulong highWatermark = database.GetOutboxHighWatermark();
        if (highWatermark == 0)
            return;
        OutboxLease lease = database.AcquireOutboxLease(Id(0x7E));
        database.AcknowledgeOutbox(highWatermark, lease);
    }

    private static byte[] Id(byte value)
    {
        byte[] id = new byte[16];
        id[0] = value;
        return id;
    }
}
