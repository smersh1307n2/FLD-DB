using Fld1.Engine.Db;
using Xunit;

namespace Fld1.Engine.Db.Tests;

public sealed class FldDatabasePublicationModeTests
{
    [Fact]
    public void Publication_cutover_is_durable_immutable_and_namespace_scoped()
    {
        string root = Path.Combine(Path.GetTempPath(), "fld-publication-mode-" + Guid.NewGuid().ToString("N"));
        const uint namespaceId = 0x731;
        try
        {
            using (var database = FldDatabase.Open(root, namespaceId, Id(0x31)))
            {
                Assert.False(database.TryGetMetadataStoreOwnedPublicationCutover(out ulong missing));
                Assert.Equal(0UL, missing);

                database.EnableMetadataStoreOwnedPublication(123);
                Assert.True(database.TryGetMetadataStoreOwnedPublicationCutover(out ulong watermark));
                Assert.Equal(123UL, watermark);

                database.EnableMetadataStoreOwnedPublication(123);
                Assert.Throws<InvalidOperationException>(() =>
                    database.EnableMetadataStoreOwnedPublication(124));
            }

            using (var reopened = FldDatabase.Open(root, namespaceId, Id(0x31)))
            {
                Assert.True(reopened.TryGetMetadataStoreOwnedPublicationCutover(out ulong watermark));
                Assert.Equal(123UL, watermark);
            }

            using (var otherNamespace = FldDatabase.Open(root, namespaceId + 1, Id(0x32)))
            {
                Assert.False(otherNamespace.TryGetMetadataStoreOwnedPublicationCutover(out ulong missing));
                Assert.Equal(0UL, missing);
            }
        }
        finally
        {
            try { Directory.Delete(root, recursive: true); } catch { }
        }
    }

    [Fact]
    public void Publication_cutover_fails_closed_while_local_drafts_or_outbox_entries_remain()
    {
        string root = Path.Combine(Path.GetTempPath(), "fld-publication-mode-pending-" + Guid.NewGuid().ToString("N"));
        try
        {
            using var database = FldDatabase.Open(root, 0x732, Id(0x33));
            byte[] recordId = FldDatabase.NewRecordId();
            database.Put(recordId, 7, "pending-local-draft"u8);
            Assert.Equal(1UL, database.Commit());
            Assert.True(database.ReadUnpublishedLocalDraftHorizon().Count != 0);

            Assert.Throws<InvalidOperationException>(() =>
                database.EnableMetadataStoreOwnedPublication(99));
            Assert.False(database.TryGetMetadataStoreOwnedPublicationCutover(out _));
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
