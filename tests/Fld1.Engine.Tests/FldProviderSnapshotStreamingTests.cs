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

    private static byte[] Id(byte value)
    {
        byte[] id = new byte[16];
        id[0] = value;
        return id;
    }
}
