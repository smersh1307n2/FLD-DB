using Fld1.Storage.Wal.Recovery;

namespace Fld1.Storage.Engine;

/// <summary>
/// Publishes and loads the standalone B+ tree v2 root. This is deliberately not
/// connected to StorageDatabase commits yet; callers must serialize writers and
/// must replay/validate WAL before truncating any generation-file tails.
/// </summary>
internal static class StorageBTreeRootPublisher
{
    private const string ManifestSuffix = ".root-v2.manifest";

    public static void Publish(
        string databaseDirectory,
        in StorageBTreeManifestV2 manifest,
        StorageBTreeGenerationFiles files,
        Action<StorageFaultPoint>? faultInjector = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(databaseDirectory);
        ArgumentNullException.ThrowIfNull(files);
        if (files.PartitionId != manifest.PartitionId || files.Generation != manifest.Generation)
            throw new InvalidDataException("B+ tree generation file identity does not match the manifest.");
        if (files.PageFileLength != manifest.CommittedPageFileLength ||
            files.DataFileLength != manifest.CommittedDataFileLength)
            throw new InvalidDataException("B+ tree manifest high-water marks must exactly match the generation files at publication.");

        byte[] encoded = StorageBTreeManifestCodec.Encode(manifest);
        files.FlushToDisk(
            beforeDataFlush: () => faultInjector?.Invoke(StorageFaultPoint.BeforeBTreeDataFlush),
            afterDataFlush: () => faultInjector?.Invoke(StorageFaultPoint.AfterBTreeDataFlush),
            beforePageFlush: () => faultInjector?.Invoke(StorageFaultPoint.BeforeBTreePageFlush),
            afterPageFlush: () => faultInjector?.Invoke(StorageFaultPoint.AfterBTreePageFlushBeforeManifest));

        // Validate the root page after both referenced files have been flushed and
        // before the manifest can make the new generation reachable.
        if (manifest.RootPageId != 0)
            _ = files.ReadPage(manifest.RootPageId, manifest.CommittedPageFileLength);

        string snapshots = Path.Combine(databaseDirectory, "snapshots");
        StorageDirectorySync.CreateDirectoryDurable(snapshots);
        string finalPath = ManifestPath(snapshots, manifest.PartitionId);
        string temporaryPath = finalPath + ".tmp";
        TryDelete(temporaryPath);
        WriteDurable(temporaryPath, encoded);
        faultInjector?.Invoke(StorageFaultPoint.AfterBTreeManifestFileFlushBeforeReplace);

        StorageDirectorySync.AtomicReplaceFile(
            temporaryPath,
            finalPath,
            flushParent: false,
            afterReplace: () => faultInjector?.Invoke(StorageFaultPoint.AfterBTreeManifestReplace));

        faultInjector?.Invoke(StorageFaultPoint.BeforeBTreeManifestDirectoryFsync);
        StorageDirectorySync.Flush(snapshots);
        faultInjector?.Invoke(StorageFaultPoint.AfterBTreeManifestDirectoryFsync);
    }

    /// <summary>
    /// Loads the v2 root, failing closed if its generation files or root page are
    /// missing, shorter than their committed ranges, or corrupt. File tails beyond
    /// committed lengths remain unreachable and are left for WAL-aware recovery.
    /// </summary>
    public static StorageBTreeManifestV2? TryLoad(string databaseDirectory, ulong partitionId)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(databaseDirectory);
        if (partitionId == 0) throw new ArgumentOutOfRangeException(nameof(partitionId));

        string snapshots = Path.Combine(databaseDirectory, "snapshots");
        string manifestPath = ManifestPath(snapshots, partitionId);
        if (!File.Exists(manifestPath))
            return null;

        StorageBTreeManifestV2 manifest = StorageBTreeManifestCodec.Decode(File.ReadAllBytes(manifestPath));
        if (manifest.PartitionId != partitionId)
            throw new InvalidDataException("B+ tree root manifest partition identity mismatch.");

        string prefix = $"p_{partitionId}.{manifest.Generation}";
        string pagesPath = Path.Combine(snapshots, prefix + ".pages");
        string dataPath = Path.Combine(snapshots, prefix + ".data");
        if (!File.Exists(pagesPath) || !File.Exists(dataPath))
            throw new InvalidDataException("B+ tree root manifest references a missing generation file.");

        using var files = new StorageBTreeGenerationFiles(databaseDirectory, partitionId, manifest.Generation);
        if (files.PageFileLength < manifest.CommittedPageFileLength ||
            files.DataFileLength < manifest.CommittedDataFileLength)
            throw new InvalidDataException("B+ tree root manifest references bytes beyond a generation file.");

        if (manifest.RootPageId != 0)
            _ = files.ReadPage(manifest.RootPageId, manifest.CommittedPageFileLength);

        return manifest;
    }

    private static string ManifestPath(string snapshotsDirectory, ulong partitionId)
        => Path.Combine(snapshotsDirectory, $"p_{partitionId}{ManifestSuffix}");

    private static void WriteDurable(string path, ReadOnlySpan<byte> bytes)
    {
        using var stream = new FileStream(
            path, FileMode.CreateNew, FileAccess.Write, FileShare.None,
            bufferSize: 4096, FileOptions.WriteThrough);
        stream.Write(bytes);
        stream.Flush(flushToDisk: true);
    }

    private static void TryDelete(string path)
    {
        if (File.Exists(path))
            File.Delete(path);
    }
}
