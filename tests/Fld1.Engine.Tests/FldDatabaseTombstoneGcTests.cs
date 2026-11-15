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
                Assert.True(database.TryResolve(recordId, 7, out var retained));
                Assert.True(retained.IsTombstone);

                int reclaimed = database.CompactTombstones(candidate =>
                {
                    Assert.True(candidate.IsFieldTombstone);
                    return true;
                }, limit: 4097);
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
            Assert.True(reopened.TryGetBytes(recordId, 9, out byte[]? reopenedNewer));
            Assert.Equal("newer-field"u8.ToArray(), reopenedNewer);
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

            int reclaimed = database.CompactTombstones(candidate =>
            {
                Assert.True(candidate.IsRecordTombstone);
                Assert.False(candidate.IsFieldTombstone);
                return false;
            });
            Assert.Equal(0, reclaimed);
            Assert.True(database.TryResolve(recordId, 7, out var retained));
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

    [Theory]
    [InlineData((int)FldCompactionFaultPoint.AfterReplacementRunDurable, false, false)]
    [InlineData((int)FldCompactionFaultPoint.AfterManifestRenameBeforeDirectoryFlush, true, false)]
    [InlineData((int)FldCompactionFaultPoint.AfterManifestAndOutboxPublished, true, false)]
    [InlineData((int)FldCompactionFaultPoint.AfterOldRunDeleted, true, false)]
    [InlineData((int)FldCompactionFaultPoint.AfterReplacementRunDurable, false, true)]
    [InlineData((int)FldCompactionFaultPoint.AfterManifestRenameBeforeDirectoryFlush, true, true)]
    [InlineData((int)FldCompactionFaultPoint.AfterManifestAndOutboxPublished, true, true)]
    [InlineData((int)FldCompactionFaultPoint.AfterOldRunDeleted, true, true)]
    public void Tombstone_gc_compaction_crash_cuts_recover_one_authoritative_generation(
        int faultPointValue, bool tombstoneMustBeGone, bool recordTombstone)
    {
        FldCompactionFaultPoint faultPoint = (FldCompactionFaultPoint)faultPointValue;
        string root = Path.Combine(Path.GetTempPath(), "fld-tombstone-gc-crash-" + faultPoint + "-" + recordTombstone + "-" + Guid.NewGuid().ToString("N"));
        const uint namespaceId = 0x754;
        byte[] recordId = FldDatabase.NewRecordId();
        bool injectFault = false;
        try
        {
            using (var database = FldDatabase.Open(new FldOpenOptions
            {
                Path = root,
                NamespaceId = namespaceId,
                LocalAuthor = Id(0x61),
                CompactionFaultInjector = point =>
                {
                    if (injectFault && point == faultPoint)
                        throw new IOException("Injected provider tombstone-compaction crash cut.");
                }
            }))
            {
                database.Put(recordId, 7, "before-delete"u8);
                Assert.NotEqual(0UL, database.Commit());
                Assert.NotEqual(0UL, database.Checkpoint());

                database.Put(recordId, 9, "before-update"u8);
                Assert.NotEqual(0UL, database.Commit());
                Assert.NotEqual(0UL, database.Checkpoint());

                if (recordTombstone)
                {
                    database.DeleteRecord(recordId);
                    Assert.NotEqual(0UL, database.Commit());
                    Assert.NotEqual(0UL, database.Checkpoint());
                }
                else
                {
                    database.Delete(recordId, 7);
                    Assert.NotEqual(0UL, database.Commit());
                    Assert.NotEqual(0UL, database.Checkpoint());
                }

                database.Put(recordId, 9, "newer-field"u8);
                Assert.NotEqual(0UL, database.Commit());
                Assert.NotEqual(0UL, database.Checkpoint());
                SealAndAcknowledgeOutbox(database);
                injectFault = true;

                Assert.Throws<IOException>(() => database.CompactTombstones(candidate =>
                    recordTombstone
                        ? candidate.IsRecordTombstone
                        : candidate.FieldId == 7 && candidate.IsFieldTombstone));
            }

            using (var recovered = FldDatabase.Open(root, namespaceId, Id(0x61)))
            {
                Assert.True(recovered.TryGetBytes(recordId, 9, out byte[]? preserved));
                Assert.Equal("newer-field"u8.ToArray(), preserved);

                if (tombstoneMustBeGone)
                {
                    if (recordTombstone)
                        Assert.False(recovered.IsRecordDead(recordId));
                    else
                    {
                        Assert.False(recovered.TryGetBytes(recordId, 7, out _));
                        Assert.False(recovered.TryResolve(recordId, 7, out _));
                    }
                }
                else
                {
                    if (recordTombstone)
                        Assert.True(recovered.IsRecordDead(recordId));
                    else
                    {
                        Assert.True(recovered.TryResolve(recordId, 7, out var retained));
                        Assert.True(retained.IsTombstone);
                    }

                    Assert.Equal(1, recovered.CompactTombstones(candidate =>
                        recordTombstone
                            ? candidate.IsRecordTombstone
                            : candidate.FieldId == 7 && candidate.IsFieldTombstone));
                    if (recordTombstone)
                        Assert.False(recovered.IsRecordDead(recordId));
                    else
                        Assert.False(recovered.TryGetBytes(recordId, 7, out _));
                }
            }

            using var reopened = FldDatabase.Open(root, namespaceId, Id(0x61));
            Assert.False(reopened.TryGetBytes(recordId, 7, out _));
            Assert.True(reopened.TryGetBytes(recordId, 9, out byte[]? finalValue));
            Assert.Equal("newer-field"u8.ToArray(), finalValue);
            Assert.False(reopened.IsRecordDead(recordId));
        }
        finally
        {
            try { Directory.Delete(root, recursive: true); } catch { }
        }
    }

    [Fact]
    public void Tombstone_gc_reclaims_only_one_bounded_page_and_repeats_until_empty()
    {
        string root = Path.Combine(Path.GetTempPath(), "fld-tombstone-gc-pages-" + Guid.NewGuid().ToString("N"));
        const uint namespaceId = 0x758;
        byte[][] recordIds = Enumerable.Range(0, 3)
            .Select(_ => FldDatabase.NewRecordId())
            .ToArray();

        try
        {
            using (var database = FldDatabase.Open(root, namespaceId, Id(0x71)))
            {
                foreach (byte[] recordId in recordIds)
                    database.Put(recordId, 7, "live-value"u8);
                Assert.NotEqual(0UL, database.Commit());
                Assert.NotEqual(0UL, database.Checkpoint());

                foreach (byte[] recordId in recordIds)
                    database.Delete(recordId, 7);
                Assert.NotEqual(0UL, database.Commit());
                Assert.NotEqual(0UL, database.Checkpoint());
                SealAndAcknowledgeOutbox(database);

                for (int page = 0; page < recordIds.Length; page++)
                {
                    int reclaimed = database.CompactTombstones(
                        static _ => true, limit: 1);
                    Assert.Equal(1, reclaimed);

                    int remaining = recordIds.Count(recordId =>
                        database.TryResolve(recordId, 7, out var current) && current.IsTombstone);
                    Assert.Equal(recordIds.Length - page - 1, remaining);
                }

                Assert.Equal(0, database.CompactTombstones(static _ => true, limit: 1));
                foreach (byte[] recordId in recordIds)
                    Assert.False(database.TryResolve(recordId, 7, out _));
            }

            using var reopened = FldDatabase.Open(root, namespaceId, Id(0x71));
            foreach (byte[] recordId in recordIds)
                Assert.False(reopened.TryResolve(recordId, 7, out _));
        }
        finally
        {
            try { Directory.Delete(root, recursive: true); } catch { }
        }
    }

    [Fact]
    public void Public_tombstone_gc_candidate_constructor_defensively_copies_identity_arrays()
    {
        byte[] recordId = Enumerable.Repeat((byte)0x11, 16).ToArray();
        byte[] changeId = Enumerable.Repeat((byte)0x22, 16).ToArray();
        byte[] authorNodeId = Enumerable.Repeat((byte)0x33, 16).ToArray();

        var candidate = new FldTombstoneGcCandidate(
            recordId, 7, changeId, authorNodeId,
            hlcWall: 123, hlcLogical: 4, originAuthorEpoch: 5,
            originSequence: 6, isRecordTombstone: false, isFieldTombstone: true);

        Array.Fill(recordId, (byte)0xA1);
        Array.Fill(changeId, (byte)0xA2);
        Array.Fill(authorNodeId, (byte)0xA3);

        Assert.All(candidate.RecordId.ToArray(), value => Assert.Equal(0x11, value));
        Assert.All(candidate.ChangeId.ToArray(), value => Assert.Equal(0x22, value));
        Assert.All(candidate.AuthorNodeId.ToArray(), value => Assert.Equal(0x33, value));
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
