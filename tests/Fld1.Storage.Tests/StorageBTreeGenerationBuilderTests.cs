using System.Text;
using Fld1.Storage.Engine;
using Xunit;

namespace Fld1.Storage.Tests;

public sealed class StorageBTreeGenerationBuilderTests
{
    [Fact]
    public void Bulk_generation_build_publish_reopen_and_read_multi_leaf_tree()
    {
        string directory = Path.Combine(Path.GetTempPath(), "fld-btree-roundtrip-" + Guid.NewGuid().ToString("N"));
        const ulong partition = 91;
        const ulong generation = 3;
        const int count = StorageBTreePageCodec.MaxLeafEntries + 47;
        try
        {
            StorageBTreeManifestV2 manifest;
            using (var files = new StorageBTreeGenerationFiles(directory, partition, generation))
            {
                var input = new StorageTreeBuildInput[count];
                for (int i = 0; i < count; i++)
                {
                    byte[] key = Encoding.UTF8.GetBytes($"key-{i:D5}");
                    bool tombstone = i == 5;
                    byte[] value = tombstone ? Array.Empty<byte>() : Encoding.UTF8.GetBytes($"value-{i:D5}");
                    input[i] = new StorageTreeBuildInput(key, value, tombstone);
                }

                manifest = StorageBTreeGenerationBuilder.Build(files, commitSeq: 27, input);
                Assert.NotEqual(0UL, manifest.RootPageId);
                Assert.True(manifest.CommittedPageFileLength >= 3UL * StoragePageCodec.PageSize);
                StorageBTreeRootPublisher.Publish(directory, manifest, files);
            }

            StorageBTreeManifestV2 reopenedManifest =
                StorageBTreeRootPublisher.TryLoad(directory, partition)!.Value;
            Assert.Equal(manifest, reopenedManifest);

            using var reopenedFiles = new StorageBTreeGenerationFiles(directory, partition, generation);
            var reader = new StorageBTreeReader(
                generation,
                reopenedManifest.RootPageId,
                pageId => reopenedFiles.ReadPage(pageId, reopenedManifest.CommittedPageFileLength),
                key => reopenedFiles.ReadData(key.Offset, key.Length, reopenedManifest.CommittedDataFileLength));

            Assert.True(reader.TryGet("key-00321"u8, out StorageTreeValueReference valueRef));
            Assert.Equal(Encoding.UTF8.GetBytes("value-00321"),
                reopenedFiles.ReadData(valueRef.Offset, valueRef.Length, reopenedManifest.CommittedDataFileLength));
            Assert.False(reader.TryGet("key-00005"u8, out _));

            using var cursor = reader.OpenPrefixCursor("key-000"u8.ToArray());
            var scanned = new List<string>();
            while (cursor.TryReadNext(out StorageTreeEntry entry))
            {
                string key = Encoding.UTF8.GetString(
                    reopenedFiles.ReadData(entry.Key.Offset, entry.Key.Length, reopenedManifest.CommittedDataFileLength));
                scanned.Add(key);
            }

            Assert.Equal(99, scanned.Count);
            Assert.DoesNotContain("key-00005", scanned);
            Assert.Equal("key-00000", scanned[0]);
            Assert.Equal("key-00099", scanned[^1]);
        }
        finally
        {
            try { if (Directory.Exists(directory)) Directory.Delete(directory, recursive: true); } catch { }
        }
    }

    [Fact]
    public void Bulk_generation_builds_and_publishes_an_empty_root()
    {
        string directory = Path.Combine(Path.GetTempPath(), "fld-btree-empty-" + Guid.NewGuid().ToString("N"));
        const ulong partition = 92;
        const ulong generation = 1;
        try
        {
            StorageBTreeManifestV2 manifest;
            using (var files = new StorageBTreeGenerationFiles(directory, partition, generation))
            {
                manifest = StorageBTreeGenerationBuilder.Build(
                    files, commitSeq: 0, Array.Empty<StorageTreeBuildInput>());
                Assert.Equal(0UL, manifest.RootPageId);
                Assert.Equal(0UL, manifest.CommittedPageFileLength);
                Assert.Equal(0UL, manifest.CommittedDataFileLength);
                StorageBTreeRootPublisher.Publish(directory, manifest, files);
            }

            Assert.Equal(manifest, StorageBTreeRootPublisher.TryLoad(directory, partition)!.Value);
            using var reopened = new StorageBTreeGenerationFiles(directory, partition, generation);
            var reader = new StorageBTreeReader(
                generation,
                manifest.RootPageId,
                pageId => reopened.ReadPage(pageId, manifest.CommittedPageFileLength),
                key => reopened.ReadData(key.Offset, key.Length, manifest.CommittedDataFileLength));
            Assert.False(reader.TryGet("absent"u8, out _));
        }
        finally
        {
            try { if (Directory.Exists(directory)) Directory.Delete(directory, recursive: true); } catch { }
        }
    }

    [Fact]
    public void Bulk_generation_requires_a_fresh_generation_and_strictly_sorted_keys()
    {
        string directory = Path.Combine(Path.GetTempPath(), "fld-btree-invalid-build-" + Guid.NewGuid().ToString("N"));
        try
        {
            using var files = new StorageBTreeGenerationFiles(directory, 93, 1);
            _ = files.AppendData("already-used"u8);
            Assert.Throws<InvalidOperationException>(() =>
                StorageBTreeGenerationBuilder.Build(files, 1, Array.Empty<StorageTreeBuildInput>()));
        }
        finally
        {
            try { if (Directory.Exists(directory)) Directory.Delete(directory, recursive: true); } catch { }
        }
    }
}
