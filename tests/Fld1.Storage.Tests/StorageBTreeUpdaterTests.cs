using System.Text;
using Fld1.Storage.Engine;
using Xunit;

namespace Fld1.Storage.Tests;

public sealed class StorageBTreeUpdaterTests
{
    [Fact]
    public void COW_batch_splits_leaf_publishes_new_root_and_preserves_old_root()
    {
        string directory = NewDirectory();
        const ulong partition = 101;
        const ulong generation = 4;
        try
        {
            StorageBTreeManifestV2 oldManifest = BuildInitial(directory, partition, generation, 700);
            var mutations = new[]
            {
                new StorageTreeMutation("key-00001a"u8.ToArray(), "inserted"u8.ToArray(), false),
                new StorageTreeMutation("key-00005"u8.ToArray(), ReadOnlyMemory<byte>.Empty, true),
                new StorageTreeMutation("key-00007x"u8.ToArray(), ReadOnlyMemory<byte>.Empty, true),
                new StorageTreeMutation("key-00321"u8.ToArray(), "updated-00321"u8.ToArray(), false)
            };

            StorageBTreeManifestV2 nextManifest;
            using (var files = new StorageBTreeGenerationFiles(directory, partition, generation))
            {
                nextManifest = StorageBTreeUpdater.ApplyBatch(oldManifest, files, mutations);
                Assert.Equal(oldManifest.CommitSeq + 1, nextManifest.CommitSeq);
                Assert.NotEqual(oldManifest.RootPageId, nextManifest.RootPageId);
                Assert.True(nextManifest.CommittedPageFileLength > oldManifest.CommittedPageFileLength);

                var oldReader = CreateReader(files, oldManifest);
                Assert.True(oldReader.TryGet("key-00321"u8, out StorageTreeValueReference oldValue));
                Assert.Equal("value-00321",
                    Encoding.UTF8.GetString(files.ReadData(oldValue.Offset, oldValue.Length, oldManifest.CommittedDataFileLength)));

                StorageBTreeRootPublisher.Publish(directory, nextManifest, files);

                // The old immutable root is still readable after the new root is published.
                Assert.True(oldReader.TryGet("key-00321"u8, out oldValue));
                Assert.Equal("value-00321",
                    Encoding.UTF8.GetString(files.ReadData(oldValue.Offset, oldValue.Length, oldManifest.CommittedDataFileLength)));

                var newReader = CreateReader(files, nextManifest);
                Assert.True(newReader.TryGet("key-00321"u8, out StorageTreeValueReference updatedValue));
                Assert.Equal("updated-00321",
                    Encoding.UTF8.GetString(files.ReadData(updatedValue.Offset, updatedValue.Length, nextManifest.CommittedDataFileLength)));
                Assert.True(newReader.TryGet("key-00001a"u8, out StorageTreeValueReference insertedValue));
                Assert.Equal("inserted",
                    Encoding.UTF8.GetString(files.ReadData(insertedValue.Offset, insertedValue.Length, nextManifest.CommittedDataFileLength)));
                Assert.False(newReader.TryGet("key-00005"u8, out _));
                Assert.False(newReader.TryGet("key-00007x"u8, out _));
                Assert.True(newReader.TryGet("key-00699"u8, out _));

                using var cursor = newReader.OpenPrefixCursor("key-"u8.ToArray());
                var keys = new List<string>();
                while (cursor.TryReadNext(out StorageTreeEntry entry))
                {
                    keys.Add(Encoding.UTF8.GetString(
                        files.ReadData(entry.Key.Offset, entry.Key.Length, nextManifest.CommittedDataFileLength)));
                }

                Assert.Equal(700, keys.Count);
                Assert.Contains("key-00001a", keys);
                Assert.DoesNotContain("key-00005", keys);
                Assert.DoesNotContain("key-00007x", keys);
                Assert.Equal(keys.OrderBy(x => x, StringComparer.Ordinal), keys);
            }

            Assert.Equal(nextManifest, StorageBTreeRootPublisher.TryLoad(directory, partition)!.Value);
            using var reopenedFiles = new StorageBTreeGenerationFiles(directory, partition, generation);
            var reopenedOldReader = CreateReader(reopenedFiles, oldManifest);
            var reopenedNewReader = CreateReader(reopenedFiles, nextManifest);
            Assert.True(reopenedOldReader.TryGet("key-00321"u8, out StorageTreeValueReference oldRef));
            Assert.True(reopenedNewReader.TryGet("key-00321"u8, out StorageTreeValueReference newRef));
            Assert.Equal("value-00321",
                Encoding.UTF8.GetString(reopenedFiles.ReadData(oldRef.Offset, oldRef.Length, oldManifest.CommittedDataFileLength)));
            Assert.Equal("updated-00321",
                Encoding.UTF8.GetString(reopenedFiles.ReadData(newRef.Offset, newRef.Length, nextManifest.CommittedDataFileLength)));
        }
        finally
        {
            TryDeleteDirectory(directory);
        }
    }

    [Fact]
    public void COW_batch_failure_before_root_replace_keeps_old_root_and_allows_tail_truncation_retry()
    {
        string directory = NewDirectory();
        const ulong partition = 102;
        const ulong generation = 1;
        try
        {
            StorageBTreeManifestV2 oldManifest = BuildInitial(directory, partition, generation, 100);
            var mutations = new[]
            {
                new StorageTreeMutation("key-00001a"u8.ToArray(), "inserted"u8.ToArray(), false)
            };

            using (var files = new StorageBTreeGenerationFiles(directory, partition, generation))
            {
                StorageBTreeManifestV2 next = StorageBTreeUpdater.ApplyBatch(oldManifest, files, mutations);
                Assert.Throws<InjectedFaultException>(() =>
                    StorageBTreeRootPublisher.Publish(directory, next, files, point =>
                    {
                        if (point == StorageFaultPoint.AfterBTreePageFlushBeforeManifest)
                            throw new InjectedFaultException();
                    }));
            }

            Assert.Equal(oldManifest, StorageBTreeRootPublisher.TryLoad(directory, partition)!.Value);

            StorageBTreeManifestV2 retried;
            using (var files = new StorageBTreeGenerationFiles(directory, partition, generation))
            {
                files.TruncateToCommittedLengths(
                    oldManifest.CommittedPageFileLength,
                    oldManifest.CommittedDataFileLength);
                retried = StorageBTreeUpdater.ApplyBatch(oldManifest, files, mutations);
                StorageBTreeRootPublisher.Publish(directory, retried, files);
            }

            Assert.Equal(retried, StorageBTreeRootPublisher.TryLoad(directory, partition)!.Value);
            Assert.Equal(oldManifest.CommitSeq + 1, retried.CommitSeq);
        }
        finally
        {
            TryDeleteDirectory(directory);
        }
    }

    [Fact]
    public void COW_batch_can_create_the_first_root_for_an_empty_generation()
    {
        string directory = NewDirectory();
        const ulong partition = 103;
        const ulong generation = 1;
        try
        {
            StorageBTreeManifestV2 empty;
            using (var files = new StorageBTreeGenerationFiles(directory, partition, generation))
            {
                empty = StorageBTreeGenerationBuilder.Build(
                    files, 0, Array.Empty<StorageTreeBuildInput>());
                StorageBTreeRootPublisher.Publish(directory, empty, files);
            }

            var mutations = new[]
            {
                new StorageTreeMutation("a"u8.ToArray(), "one"u8.ToArray(), false),
                new StorageTreeMutation("b"u8.ToArray(), "two"u8.ToArray(), false)
            };
            StorageBTreeManifestV2 next;
            using (var files = new StorageBTreeGenerationFiles(directory, partition, generation))
            {
                next = StorageBTreeUpdater.ApplyAndPublish(directory, empty, files, mutations);
                Assert.NotEqual(0UL, next.RootPageId);
            }

            using var reopened = new StorageBTreeGenerationFiles(directory, partition, generation);
            var reader = CreateReader(reopened, next);
            Assert.True(reader.TryGet("a"u8, out _));
            Assert.True(reader.TryGet("b"u8, out _));
        }
        finally
        {
            TryDeleteDirectory(directory);
        }
    }

    [Fact]
    public void COW_batch_rejects_unsorted_and_duplicate_mutations_before_writing()
    {
        string directory = NewDirectory();
        const ulong partition = 104;
        const ulong generation = 1;
        try
        {
            StorageBTreeManifestV2 current = BuildInitial(directory, partition, generation, 10);
            using var files = new StorageBTreeGenerationFiles(directory, partition, generation);
            ulong oldPages = files.PageFileLength;
            ulong oldData = files.DataFileLength;

            var unsorted = new[]
            {
                new StorageTreeMutation("z"u8.ToArray(), "1"u8.ToArray(), false),
                new StorageTreeMutation("a"u8.ToArray(), "2"u8.ToArray(), false)
            };
            Assert.Throws<InvalidDataException>(() => StorageBTreeUpdater.ApplyBatch(current, files, unsorted));

            var duplicate = new[]
            {
                new StorageTreeMutation("a"u8.ToArray(), "1"u8.ToArray(), false),
                new StorageTreeMutation("a"u8.ToArray(), "2"u8.ToArray(), false)
            };
            Assert.Throws<InvalidDataException>(() => StorageBTreeUpdater.ApplyBatch(current, files, duplicate));

            Assert.Equal(oldPages, files.PageFileLength);
            Assert.Equal(oldData, files.DataFileLength);
        }
        finally
        {
            TryDeleteDirectory(directory);
        }
    }

    private static StorageBTreeManifestV2 BuildInitial(string directory, ulong partition, ulong generation, int count)
    {
        using var files = new StorageBTreeGenerationFiles(directory, partition, generation);
        var entries = new StorageTreeBuildInput[count];
        for (int i = 0; i < count; i++)
        {
            byte[] key = Encoding.UTF8.GetBytes($"key-{i:D5}");
            bool tombstone = i == 5;
            byte[] value = tombstone ? Array.Empty<byte>() : Encoding.UTF8.GetBytes($"value-{i:D5}");
            entries[i] = new StorageTreeBuildInput(key, value, tombstone);
        }
        StorageBTreeManifestV2 manifest = StorageBTreeGenerationBuilder.Build(files, 10, entries);
        StorageBTreeRootPublisher.Publish(directory, manifest, files);
        return manifest;
    }

    private static StorageBTreeReader CreateReader(StorageBTreeGenerationFiles files, StorageBTreeManifestV2 manifest)
        => new(
            manifest.Generation,
            manifest.RootPageId,
            pageId => files.ReadPage(pageId, manifest.CommittedPageFileLength),
            key => files.ReadData(key.Offset, key.Length, manifest.CommittedDataFileLength));

    private static string NewDirectory()
        => Path.Combine(Path.GetTempPath(), "fld-btree-cow-" + Guid.NewGuid().ToString("N"));

    private static void TryDeleteDirectory(string directory)
    {
        try { if (Directory.Exists(directory)) Directory.Delete(directory, recursive: true); } catch { }
    }

    private sealed class InjectedFaultException : Exception { }
}
