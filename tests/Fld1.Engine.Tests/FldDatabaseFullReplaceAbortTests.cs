using System.Security.Cryptography;
using Fld1.Engine.Db;
using Xunit;

namespace Fld1.Engine.Tests;

public sealed class FldDatabaseFullReplaceAbortTests
{
    [Fact]
    public void Failed_full_replace_is_never_published_and_abort_cursor_resumes_across_restarts()
    {
        string root = Path.Combine(Path.GetTempPath(), "fld-full-replace-abort-" + Guid.NewGuid().ToString("N"));
        const uint namespaceId = 0x755;
        byte[] originalRecordId = Id(0x01);
        Guid generation = Guid.Parse("aabbccdd-eeff-4011-8223-344556677889");
        byte[] expectedDigest = SHA256.HashData("expected-full-replace"u8);
        byte[] wrongDigest = SHA256.HashData("wrong-full-replace"u8);

        try
        {
            using (var database = FldDatabase.Open(root, namespaceId, Id(0x61)))
            {
                database.Put(originalRecordId, 5, "original-state"u8);
                Assert.NotEqual(0UL, database.Commit());
                Assert.NotEqual(0UL, database.Checkpoint());

                database.BeginProviderReplacement(
                    generation, sourceFeedWatermark: 0, expectedEntryCount: 3, expectedDigest);

                database.StageProviderReplacementPage(generation, 0, new[] { Entry(0x10, "replacement-1") });
                database.StageProviderReplacementPage(generation, 1, new[] { Entry(0x20, "replacement-2") });

                // An incomplete snapshot cannot publish a replacement generation.
                Assert.Throws<InvalidOperationException>(() =>
                    database.CommitProviderReplacement(generation, expectedDigest));

                database.StageProviderReplacementPage(generation, 2, new[] { Entry(0x30, "replacement-3") });

                // A caller-provided digest that does not match the staged manifest is rejected
                // before the active Manifest is switched.
                Assert.Throws<InvalidDataException>(() =>
                    database.CommitProviderReplacement(generation, wrongDigest));

                Assert.True(database.TryGetBytes(originalRecordId, 5, out byte[]? stillOriginal));
                Assert.Equal("original-state"u8.ToArray(), stillOriginal);

                // The abort cursor is persisted before deleting the first staged Run.
                Assert.False(database.AbortProviderReplacement(generation, limit: 1));
            }

            using (var recovered = FldDatabase.Open(root, namespaceId, Id(0x61)))
            {
                Assert.True(recovered.TryGetBytes(originalRecordId, 5, out byte[]? stillOriginal));
                Assert.Equal("original-state"u8.ToArray(), stillOriginal);

                // Resume from the durable abort cursor, then restart again between pages.
                Assert.False(recovered.AbortProviderReplacement(generation, limit: 1));
            }

            using (var recoveredAgain = FldDatabase.Open(root, namespaceId, Id(0x61)))
            {
                Assert.True(recoveredAgain.TryGetBytes(originalRecordId, 5, out byte[]? stillOriginal));
                Assert.Equal("original-state"u8.ToArray(), stillOriginal);
                Assert.True(recoveredAgain.AbortProviderReplacement(generation, limit: 1));
                string stagingDirectory = Path.Combine(root, "full-replace-staging");
                Assert.True(!Directory.Exists(stagingDirectory) ||
                    !Directory.EnumerateFiles(stagingDirectory).Any());
            }

            using var final = FldDatabase.Open(root, namespaceId, Id(0x61));
            Assert.True(final.TryGetBytes(originalRecordId, 5, out byte[]? finalOriginal));
            Assert.Equal("original-state"u8.ToArray(), finalOriginal);
            Assert.False(final.TryGetBytes(Id(0x10), 1, out _));
            Assert.False(final.TryGetBytes(Id(0x20), 1, out _));
            Assert.False(final.TryGetBytes(Id(0x30), 1, out _));
        }
        finally
        {
            try { Directory.Delete(root, recursive: true); } catch { }
        }
    }

    [Fact]
    public void Full_replace_manifest_rename_uncertain_completion_recovers_as_committed_not_abortable()
    {
        string root = Path.Combine(Path.GetTempPath(), "fld-full-replace-rename-" + Guid.NewGuid().ToString("N"));
        const uint namespaceId = 0x757;
        byte[] originalRecordId = Id(0x01);
        byte[] replacementRecordId = Id(0x10);
        Guid generation = Guid.Parse("bbccdde0-ff11-4223-8334-455667788990");
        byte[] digest = SHA256.HashData("installed-full-replace"u8);
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
                    if (injectFault && point == FldCompactionFaultPoint.AfterManifestRenameBeforeDirectoryFlush)
                        throw new IOException("Injected FULL_REPLACE Manifest rename/fsync uncertainty.");
                }
            }))
            {
                database.Put(originalRecordId, 5, "old-generation"u8);
                Assert.NotEqual(0UL, database.Commit());
                Assert.NotEqual(0UL, database.Checkpoint());

                ulong outboxHigh = database.GetOutboxHighWatermark();
                if (outboxHigh != 0)
                {
                    var lease = database.AcquireOutboxLease(Id(0x7E));
                    database.AcknowledgeOutbox(outboxHigh, lease);
                }

                database.BeginProviderReplacement(
                    generation, sourceFeedWatermark: outboxHigh, expectedEntryCount: 1, digest);
                database.StageProviderReplacementPage(
                    generation, 0, new[] { Entry(0x10, "new-generation") });

                injectFault = true;
                Assert.Throws<IOException>(() => database.CommitProviderReplacement(generation, digest));
                Assert.Throws<InvalidOperationException>(() =>
                    database.Put(originalRecordId, 6, "must-not-continue"u8));
            }

            using var recovered = FldDatabase.Open(root, namespaceId, Id(0x61));
            Assert.False(recovered.TryGetBytes(originalRecordId, 5, out _));
            Assert.True(recovered.TryGetBytes(replacementRecordId, 1, out byte[]? replacement));
            Assert.Equal("new-generation"u8.ToArray(), replacement);

            // The Manifest rename was visible at the process-restart boundary. The
            // staged marker is reconciled from the authoritative Manifest and cannot
            // be mistaken for an uncommitted generation eligible for abort.
            Assert.Throws<InvalidOperationException>(() =>
                recovered.AbortProviderReplacement(generation, limit: 1));
        }
        finally
        {
            try { Directory.Delete(root, recursive: true); } catch { }
        }
    }

    private static FldProviderSnapshotEntry Entry(byte recordId, string value)
        => new(
            Id(recordId),
            FieldId: 1,
            Value: System.Text.Encoding.UTF8.GetBytes(value),
            HlcWall: 10,
            HlcLogical: 0,
            AuthorNodeId: Id(0x61),
            KeyEpoch: 0,
            IsTombstone: false,
            ChangeId: Array.Empty<byte>(),
            OriginAuthorEpoch: 0,
            OriginSequence: 0,
            IsRecordTombstone: false);

    private static byte[] Id(byte value)
    {
        byte[] id = new byte[16];
        id[15] = value;
        return id;
    }
}
