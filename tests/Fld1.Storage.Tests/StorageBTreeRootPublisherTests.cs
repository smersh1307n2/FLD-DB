using Fld1.Storage.Engine;
using Xunit;

namespace Fld1.Storage.Tests;

public sealed class StorageBTreeRootPublisherTests
{
    [Fact]
    public void Publisher_keeps_old_root_when_failure_happens_before_manifest_replace()
    {
        string directory = NewDirectory();
        const ulong partition = 81;
        try
        {
            StorageBTreeManifestV2 first = CreateAndPublish(directory, partition, 1, 1, "v1"u8);
            var second = CreateGeneration(directory, partition, 2, 2, "v2"u8);
            using (second.Files)
            {
                Assert.Throws<InjectedFaultException>(() =>
                    StorageBTreeRootPublisher.Publish(directory, second.Manifest, second.Files, point =>
                    {
                        if (point == StorageFaultPoint.AfterBTreePageFlushBeforeManifest)
                            throw new InjectedFaultException();
                    }));
            }

            Assert.Equal(first, StorageBTreeRootPublisher.TryLoad(directory, partition)!.Value);

            using (var files = new StorageBTreeGenerationFiles(directory, partition, 2))
            {
                // The failed attempt left the generation files intact; publication can be retried.
                var manifest = new StorageBTreeManifestV2(
                    partition, 2, 2, 1,
                    files.PageFileLength, files.DataFileLength,
                    StoragePageCodec.PageSize, 0);
                StorageBTreeRootPublisher.Publish(directory, manifest, files);
            }

            Assert.Equal(second.Manifest, StorageBTreeRootPublisher.TryLoad(directory, partition)!.Value);
        }
        finally
        {
            TryDeleteDirectory(directory);
        }
    }

    [Fact]
    public void Failure_after_atomic_replace_is_uncertain_to_caller_but_reopen_sees_new_root()
    {
        string directory = NewDirectory();
        const ulong partition = 82;
        try
        {
            _ = CreateAndPublish(directory, partition, 1, 1, "old"u8);
            var next = CreateGeneration(directory, partition, 2, 2, "new"u8);
            using (next.Files)
            {
                Assert.Throws<InjectedFaultException>(() =>
                    StorageBTreeRootPublisher.Publish(directory, next.Manifest, next.Files, point =>
                    {
                        if (point == StorageFaultPoint.AfterBTreeManifestReplace)
                            throw new InjectedFaultException();
                    }));
            }

            Assert.Equal(next.Manifest, StorageBTreeRootPublisher.TryLoad(directory, partition)!.Value);
        }
        finally
        {
            TryDeleteDirectory(directory);
        }
    }

    [Fact]
    public void Try_load_fails_closed_when_manifest_generation_file_is_missing_or_root_page_is_corrupt()
    {
        string directory = NewDirectory();
        const ulong partition = 83;
        try
        {
            StorageBTreeManifestV2 manifest = CreateAndPublish(directory, partition, 1, 1, "value"u8);
            string prefix = Path.Combine(directory, "snapshots", $"p_{partition}.1");
            string dataPath = prefix + ".data";
            byte[] data = File.ReadAllBytes(dataPath);
            File.Delete(dataPath);
            Assert.Throws<InvalidDataException>(() => StorageBTreeRootPublisher.TryLoad(directory, partition));

            File.WriteAllBytes(dataPath, data);
            string pagePath = prefix + ".pages";
            byte[] pageFile = File.ReadAllBytes(pagePath);
            pageFile[32] ^= 0x20;
            File.WriteAllBytes(pagePath, pageFile);
            Assert.Throws<InvalidDataException>(() => StorageBTreeRootPublisher.TryLoad(directory, partition));

            Assert.Equal(1UL, manifest.Generation);
        }
        finally
        {
            TryDeleteDirectory(directory);
        }
    }

    private static StorageBTreeManifestV2 CreateAndPublish(
        string directory, ulong partition, ulong generation, ulong commitSeq, ReadOnlySpan<byte> value)
    {
        using var files = new StorageBTreeGenerationFiles(directory, partition, generation);
        _ = files.AppendData("k"u8);
        _ = files.AppendData(value);
        byte[] page = StoragePageCodec.Encode(generation, 1, StoragePageKind.Leaf, value);
        files.AppendPage(1, page);
        var manifest = new StorageBTreeManifestV2(
            partition, generation, commitSeq, 1,
            files.PageFileLength, files.DataFileLength,
            StoragePageCodec.PageSize, 0);
        StorageBTreeRootPublisher.Publish(directory, manifest, files);
        return manifest;
    }

    private static (StorageBTreeManifestV2 Manifest, StorageBTreeGenerationFiles Files) CreateGeneration(
        string directory, ulong partition, ulong generation, ulong commitSeq, ReadOnlySpan<byte> value)
    {
        var files = new StorageBTreeGenerationFiles(directory, partition, generation);
        _ = files.AppendData("k"u8);
        _ = files.AppendData(value);
        byte[] page = StoragePageCodec.Encode(generation, 1, StoragePageKind.Leaf, value);
        files.AppendPage(1, page);
        var manifest = new StorageBTreeManifestV2(
            partition, generation, commitSeq, 1,
            files.PageFileLength, files.DataFileLength,
            StoragePageCodec.PageSize, 0);
        return (manifest, files);
    }

    private static string NewDirectory()
        => Path.Combine(Path.GetTempPath(), "fld-btree-root-" + Guid.NewGuid().ToString("N"));

    private static void TryDeleteDirectory(string directory)
    {
        try { if (Directory.Exists(directory)) Directory.Delete(directory, recursive: true); } catch { }
    }

    private sealed class InjectedFaultException : Exception;
}
