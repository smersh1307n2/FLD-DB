namespace Fld1.Storage.Engine;

/// <summary>Key memory must remain immutable until the bulk Build call returns.</summary>
internal readonly record struct StorageTreeBuildInput(
    ReadOnlyMemory<byte> Key,
    ReadOnlyMemory<byte> Value,
    bool IsTombstone);

/// <summary>
/// Builds a fresh immutable B+ tree generation into append-only generation files.
/// This is a bulk-generation builder, not an incremental COW updater.
/// </summary>
internal static class StorageBTreeGenerationBuilder
{
    public static StorageBTreeManifestV2 Build(
        StorageBTreeGenerationFiles files,
        ulong commitSeq,
        IEnumerable<StorageTreeBuildInput> sortedEntries)
    {
        ArgumentNullException.ThrowIfNull(files);
        ArgumentNullException.ThrowIfNull(sortedEntries);
        if (files.PageFileLength != 0 || files.DataFileLength != 0)
            throw new InvalidOperationException("A bulk generation build requires empty generation files.");

        ulong nextPageId = 1;
        ulong rootPageId = StorageBTreeBuilder.Build(
            files.Generation,
            CreateBuildEntries(),
            AllocatePageId,
            files.AppendPage);

        return new StorageBTreeManifestV2(
            files.PartitionId,
            files.Generation,
            commitSeq,
            rootPageId,
            files.PageFileLength,
            files.DataFileLength,
            StoragePageCodec.PageSize,
            FormatFlags: 0);

        IEnumerable<StorageTreeBuildEntry> CreateBuildEntries()
        {
            foreach (StorageTreeBuildInput entry in sortedEntries)
            {
                if (entry.Key.IsEmpty)
                    throw new InvalidDataException("B+ tree keys cannot be empty.");
                if (entry.IsTombstone && !entry.Value.IsEmpty)
                    throw new InvalidDataException("Tombstone build entries cannot carry value bytes.");

                ulong keyOffset = files.AppendData(entry.Key.Span);
                ulong valueOffset = entry.IsTombstone
                    ? files.DataFileLength
                    : files.AppendData(entry.Value.Span);

                yield return new StorageTreeBuildEntry(
                    entry.Key,
                    keyOffset,
                    valueOffset,
                    entry.IsTombstone ? 0 : entry.Value.Length,
                    entry.IsTombstone);
            }
        }

        ulong AllocatePageId()
        {
            ulong result = nextPageId;
            nextPageId = checked(nextPageId + 1);
            return result;
        }
    }
}
