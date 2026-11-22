using Fld1.Engine.Db;
using Xunit;

namespace Fld1.Engine.Db.Tests;

public sealed class FldProviderSnapshotStreamingTests
{
    [Fact]
    public void Provider_snapshot_merges_duplicate_record_ids_from_many_sorted_runs()
    {
        const uint namespaceId = 0x782;
        string root = Path.Combine(
            Path.GetTempPath(), "fld-provider-snapshot-stream-" + Guid.NewGuid().ToString("N"));
        byte[][] recordIds = [Id(0x10), Id(0x20), Id(0x30), Id(0x40), Id(0x50)];

        try
        {
            using (var database = FldDatabase.Open(root, namespaceId, Id(0x71)))
            {
                for (int generation = 0; generation < 4; generation++)
                {
                    for (int i = 0; i < recordIds.Length; i++)
                        database.Put(recordIds[i], 7, BitConverter.GetBytes(generation * 10 + i));

                    Assert.NotEqual(0UL, database.Commit());
                    Assert.NotEqual(0UL, database.Checkpoint());
                }

                using FldProviderSnapshot snapshot = database.OpenProviderSnapshot();
                Assert.Equal(recordIds.Length, snapshot.RecordCount);

                FldProviderSnapshotMetadataPage first = snapshot.ReadMetadataPage(0, 2);
                Assert.Equal(2, first.Entries.Count);
                Assert.True(first.HasMore);
                Assert.Equal(2, first.NextIndex);
                Assert.Equal(recordIds[0], first.Entries[0].RecordId);
                Assert.Equal(recordIds[1], first.Entries[1].RecordId);

                FldProviderSnapshotMetadataPage retry = snapshot.ReadMetadataPage(0, 2);
                Assert.Equal(first, retry);

                FldProviderSnapshotMetadataPage second = snapshot.ReadMetadataPage(first.NextIndex, 2);
                Assert.Equal(2, second.Entries.Count);
                Assert.True(second.HasMore);
                Assert.Equal(recordIds[2], second.Entries[0].RecordId);
                Assert.Equal(recordIds[3], second.Entries[1].RecordId);

                FldProviderSnapshotMetadataPage last = snapshot.ReadMetadataPage(second.NextIndex, 2);
                Assert.Single(last.Entries);
                Assert.False(last.HasMore);
                Assert.Equal(recordIds[4], last.Entries[0].RecordId);

                FldProviderSnapshotMetadataPage rewind = snapshot.ReadMetadataPage(0, 2);
                Assert.Equal(first.NextIndex, rewind.NextIndex);
                Assert.Equal(first.HasMore, rewind.HasMore);
                Assert.Equal(first.Entries[0].RecordId, rewind.Entries[0].RecordId);
                Assert.Equal(first.Entries[1].RecordId, rewind.Entries[1].RecordId);
            }
        }
        finally
        {
            try { Directory.Delete(root, recursive: true); } catch { }
        }
    }


    [Fact]
    public void Provider_snapshot_merges_sorted_unique_fields_from_runs_and_overlay()
    {
        const uint namespaceId = 0x783;
        string root = Path.Combine(
            Path.GetTempPath(), "fld-provider-snapshot-fields-" + Guid.NewGuid().ToString("N"));
        byte[] recordId = Id(0x31);

        try
        {
            using (var database = FldDatabase.Open(root, namespaceId, Id(0x72)))
            {
                database.Put(recordId, 7, [0x17]);
                database.Put(recordId, 3, [0x13]);
                Assert.NotEqual(0UL, database.Commit());
                Assert.NotEqual(0UL, database.Checkpoint());

                // Field 7 now exists in both the immutable Run and the current overlay;
                // field 5 exists only in the overlay. Snapshot enumeration must return
                // one sorted copy of each current field.
                database.Put(recordId, 7, [0x27]);
                database.Put(recordId, 5, [0x15]);
                Assert.NotEqual(0UL, database.Commit());

                using FldProviderSnapshot snapshot = database.OpenProviderSnapshot();
                FldProviderSnapshotMetadataPage page = snapshot.ReadMetadataPage(0, 8);
                Assert.False(page.HasMore);
                Assert.Equal(new ushort[] { 3, 5, 7 },
                    page.Entries.Select(entry => entry.FieldId).ToArray());
                Assert.Equal(3, page.Entries.Count);
            }
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

    [Fact]
    public void Metadata_page_payload_matches_object_page_without_materializing_entries()
    {
        const uint namespaceId = 0x785;
        string root = Path.Combine(
            Path.GetTempPath(), "fld-provider-snapshot-payload-" + Guid.NewGuid().ToString("N"));
        byte[][] recordIds = [Id(0x10), Id(0x20), Id(0x30)];

        try
        {
            using (var database = FldDatabase.Open(root, namespaceId, Id(0x73)))
            {
                for (int i = 0; i < recordIds.Length; i++)
                    database.Put(recordIds[i], 7, BitConverter.GetBytes(i + 1));
                Assert.NotEqual(0UL, database.Commit());

                using FldProviderSnapshot snapshot = database.OpenProviderSnapshot();
                byte[] raw = snapshot.ReadMetadataPagePayload(0, 2);
                Assert.Equal(raw, snapshot.ReadMetadataPagePayload(0, 2));

                var actual = FldDatabaseIpcProtocol.DecodeSnapshotPageResponse(raw);
                FldProviderSnapshotMetadataPage expected = snapshot.ReadMetadataPage(0, 2);
                Assert.Equal(expected.NextIndex, actual.NextIndex);
                Assert.Equal(expected.HasMore, actual.HasMore);
                Assert.Equal(expected.Entries.Count, actual.Entries.Count);
                for (int i = 0; i < expected.Entries.Count; i++)
                {
                    Assert.Equal(expected.Entries[i].RecordId, actual.Entries[i].RecordId);
                    Assert.Equal(expected.Entries[i].FieldId, actual.Entries[i].FieldId);
                    Assert.Equal(expected.Entries[i].ValueLength, actual.Entries[i].ValueLength);
                    Assert.Equal(expected.Entries[i].HlcWall, actual.Entries[i].HlcWall);
                    Assert.Equal(expected.Entries[i].HlcLogical, actual.Entries[i].HlcLogical);
                    Assert.Equal(expected.Entries[i].AuthorNodeId, actual.Entries[i].AuthorNodeId);
                    Assert.Equal(expected.Entries[i].ChangeId, actual.Entries[i].ChangeId);
                    Assert.Equal(expected.Entries[i].OriginAuthorEpoch, actual.Entries[i].OriginAuthorEpoch);
                    Assert.Equal(expected.Entries[i].OriginSequence, actual.Entries[i].OriginSequence);
                }
            }
        }
        finally
        {
            try { Directory.Delete(root, recursive: true); } catch { }
        }
    }

}
