using System.Buffers.Binary;
using System.Security.Cryptography;
using Fld1.Engine.Storage;
using Fld1.Run.Writer;
using Fld1.Run.Reader;
using Fld1.Storage.Engine;

namespace Fld1.Engine.Db;

public sealed partial class FldDatabase
{
    private const uint ProviderSnapshotMagic = 0x31505346; // "FSP1"
    private const ushort ProviderSnapshotVersion = 1;
    private const int ProviderSnapshotHeaderSize = 40;
    private const string ProviderSnapshotManifestName = "snapshot.manifest";
    private const string ProviderSnapshotOverlayName = "overlay.fld1";

    private sealed record ProviderSnapshotManifest(
        Guid Generation,
        uint NamespaceId,
        ulong SourceFeedWatermark,
        ulong[] RunIds,
        byte[]? OverlayHash);

    private FldProviderSnapshot OpenProviderSnapshotPersisted()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        lock (_commitLock)
        {
            EnsureHealthy();
            Guid generation = Guid.NewGuid();
            ulong sourceFeedWatermark = _outbox.HighWatermark;
            string snapshotDirectory = GetProviderSnapshotDirectory(generation);
            StorageDirectorySync.CreateDirectoryDurable(snapshotDirectory);

            try
            {
                ulong[] runIds = new ulong[_runs.Count];
                var readers = new List<RunReader>(_runs.Count + 1);
                for (int i = 0; i < _runs.Count; i++)
                {
                    runIds[i] = _runs[i].runId;
                    byte[] bytes = EnsureRunCached(i);
                    var reader = new RunReader(bytes);
                    if (!reader.TryValidate(out _) || reader.NamespaceId != _namespaceId)
                        throw new InvalidDataException("A source Run failed validation while opening a provider snapshot.");
                    readers.Add(reader);
                }

                byte[]? overlayHash = null;
                KeyValuePair<MemFieldKey, MemFieldValue>[] overlay = _overlay.Snapshot().ToArray();
                if (overlay.Length != 0)
                {
                    var builder = new RunBuilder(_namespaceId);
                    Span<byte> recordId = stackalloc byte[16];
                    foreach (var pair in overlay)
                    {
                        pair.Key.WriteRecordId(recordId);
                        builder.Add(
                            recordId,
                            pair.Key.FieldId,
                            pair.Value.IsTombstone ? ReadOnlySpan<byte>.Empty : pair.Value.Value,
                            pair.Value.Wall,
                            pair.Value.Logical,
                            pair.Value.AuthorNodeId,
                            pair.Value.KeyEpoch,
                            pair.Value.IsTombstone,
                            pair.Value.ChangeId,
                            pair.Value.OriginAuthorEpoch,
                            pair.Value.OriginSequence);
                    }

                    byte[] overlayBytes = builder.Build();
                    var overlayReader = new RunReader(overlayBytes);
                    if (!overlayReader.TryValidate(out _) || overlayReader.NamespaceId != _namespaceId)
                        throw new InvalidDataException("The provider snapshot overlay Run failed validation.");
                    WriteDurableSnapshotFile(
                        Path.Combine(snapshotDirectory, ProviderSnapshotOverlayName), overlayBytes);
                    overlayHash = SHA256.HashData(overlayBytes);
                    readers.Add(overlayReader);
                }

                RunReader[] runReaders = readers.ToArray();
                byte[][] recordIds = BuildProviderRecordIdIndex(runReaders);
                byte[] manifestBytes = EncodeProviderSnapshotManifest(
                    generation, _namespaceId, sourceFeedWatermark, runIds, overlayHash);
                WriteProviderSnapshotManifest(snapshotDirectory, manifestBytes);

                var snapshot = new FldProviderSnapshot(
                    generation,
                    sourceFeedWatermark,
                    runReaders,
                    Array.Empty<KeyValuePair<MemFieldKey, MemFieldValue>>(),
                    recordIds,
                    OnProviderSnapshotDisposed);
                _providerSnapshots.Add(generation, snapshot);
                return snapshot;
            }
            catch
            {
                try
                {
                    if (Directory.Exists(snapshotDirectory))
                    {
                        Directory.Delete(snapshotDirectory, recursive: true);
                        StorageDirectorySync.Flush(Path.GetDirectoryName(snapshotDirectory)!);
                    }
                }
                catch
                {
                    // A manifest is written last; an incomplete directory is not a valid generation.
                }
                throw;
            }
        }
    }

    /// <summary>
    /// Reopens an immutable FULL_REPLACE source generation after process restart.
    /// Its manifest pins the source Run IDs and a durable overlay Run.
    /// </summary>
    public FldProviderSnapshot RestoreProviderSnapshot(Guid generation)
    {
        if (generation == Guid.Empty) throw new ArgumentException("Snapshot generation is required.", nameof(generation));
        ObjectDisposedException.ThrowIf(_disposed, this);
        lock (_commitLock)
        {
            EnsureHealthy();
            if (_providerSnapshots.TryGetValue(generation, out FldProviderSnapshot? existing))
                return existing;

            string snapshotDirectory = GetProviderSnapshotDirectory(generation);
            ProviderSnapshotManifest manifest = ReadProviderSnapshotManifest(snapshotDirectory, generation);
            if (manifest.NamespaceId != _namespaceId)
                throw new InvalidDataException("Provider snapshot belongs to another FLD namespace.");

            var readers = new List<RunReader>(manifest.RunIds.Length + (manifest.OverlayHash is null ? 0 : 1));
            foreach (ulong runId in manifest.RunIds)
            {
                string runPath = Path.Combine(_dbDir, "runs", $"{runId}.fld1");
                byte[] bytes;
                if (_runBlobStore.TryOpenRead(RunBlobId(runId), out StorageBlobReader? blobReader) &&
                    blobReader is not null)
                {
                    using (blobReader)
                        bytes = blobReader.ReadAllBytes();
                }
                else if (_runBlobStore.ContainsBlob(RunBlobId(runId)))
                {
                    throw new InvalidDataException($"Pinned provider snapshot Run {runId} has an incomplete Storage blob.");
                }
                else
                {
                    bytes = File.ReadAllBytes(runPath);
                }
                var reader = new RunReader(bytes);
                if (!reader.TryValidate(out _) || reader.NamespaceId != _namespaceId)
                    throw new InvalidDataException($"Pinned provider snapshot Run {runId} failed validation.");
                readers.Add(reader);
            }

            if (manifest.OverlayHash is not null)
            {
                string overlayPath = Path.Combine(snapshotDirectory, ProviderSnapshotOverlayName);
                byte[] overlayBytes = File.ReadAllBytes(overlayPath);
                byte[] actualHash = SHA256.HashData(overlayBytes);
                if (!CryptographicOperations.FixedTimeEquals(actualHash, manifest.OverlayHash))
                    throw new InvalidDataException("Provider snapshot overlay checksum mismatch.");
                var overlayReader = new RunReader(overlayBytes);
                if (!overlayReader.TryValidate(out _) || overlayReader.NamespaceId != _namespaceId)
                    throw new InvalidDataException("Provider snapshot overlay Run failed validation.");
                readers.Add(overlayReader);
            }

            RunReader[] runReaders = readers.ToArray();
            byte[][] recordIds = BuildProviderRecordIdIndex(runReaders);
            var snapshot = new FldProviderSnapshot(
                generation,
                manifest.SourceFeedWatermark,
                runReaders,
                Array.Empty<KeyValuePair<MemFieldKey, MemFieldValue>>(),
                recordIds,
                OnProviderSnapshotDisposed);
            _providerSnapshots.Add(generation, snapshot);
            return snapshot;
        }
    }

    /// <summary>Checks a persisted source generation without loading its Run payloads.</summary>
    public bool HasProviderSnapshot(Guid generation)
    {
        if (generation == Guid.Empty) return false;
        ObjectDisposedException.ThrowIf(_disposed, this);
        lock (_commitLock)
        {
            if (_providerSnapshots.ContainsKey(generation))
                return true;
            string directory = GetProviderSnapshotDirectory(generation);
            if (!Directory.Exists(directory))
                return false;
            ProviderSnapshotManifest manifest;
            try
            {
                manifest = ReadProviderSnapshotManifest(directory, generation);
            }
            catch (FileNotFoundException)
            {
                return false;
            }
            foreach (ulong runId in manifest.RunIds)
            {
                if (!HasDurableProviderSnapshotRun(runId, verifyContent: false))
                    return false;
            }
            if (manifest.OverlayHash is not null)
            {
                string overlayPath = Path.Combine(directory, ProviderSnapshotOverlayName);
                if (!File.Exists(overlayPath))
                    return false;
                byte[] hash = SHA256.HashData(File.ReadAllBytes(overlayPath));
                if (!CryptographicOperations.FixedTimeEquals(hash, manifest.OverlayHash))
                    throw new InvalidDataException("Provider snapshot overlay checksum mismatch.");
            }
            return true;
        }
    }

    /// <summary>
    /// Returns the source-feed watermark bound by the durable snapshot manifest.
    /// This lets protocol adapters prove that sender metadata and the provider token
    /// describe the same snapshot barrier after process restart.
    /// </summary>
    public bool TryGetProviderSnapshotWatermark(Guid generation, out ulong sourceFeedWatermark)
    {
        if (generation == Guid.Empty)
        {
            sourceFeedWatermark = 0;
            return false;
        }
        ObjectDisposedException.ThrowIf(_disposed, this);
        lock (_commitLock)
        {
            EnsureHealthy();
            if (_providerSnapshots.TryGetValue(generation, out FldProviderSnapshot? existing))
            {
                sourceFeedWatermark = existing.SourceFeedWatermark;
                return true;
            }

            string directory = GetProviderSnapshotDirectory(generation);
            if (!Directory.Exists(directory))
            {
                sourceFeedWatermark = 0;
                return false;
            }
            ProviderSnapshotManifest manifest;
            try { manifest = ReadProviderSnapshotManifest(directory, generation); }
            catch (FileNotFoundException)
            {
                sourceFeedWatermark = 0;
                return false;
            }
            if (manifest.NamespaceId != _namespaceId)
                throw new InvalidDataException("Provider snapshot belongs to another FLD namespace.");
            foreach (ulong runId in manifest.RunIds)
            {
                if (!HasDurableProviderSnapshotRun(runId, verifyContent: true))
                {
                    sourceFeedWatermark = 0;
                    return false;
                }
            }
            if (manifest.OverlayHash is not null)
            {
                string overlayPath = Path.Combine(directory, ProviderSnapshotOverlayName);
                if (!File.Exists(overlayPath))
                {
                    sourceFeedWatermark = 0;
                    return false;
                }
                byte[] hash = SHA256.HashData(File.ReadAllBytes(overlayPath));
                if (!CryptographicOperations.FixedTimeEquals(hash, manifest.OverlayHash))
                    throw new InvalidDataException("Provider snapshot overlay checksum mismatch.");
            }
            sourceFeedWatermark = manifest.SourceFeedWatermark;
            return true;
        }
    }

    /// <summary>Releases a source generation and permits its formerly pinned Runs to be collected.</summary>
    public void ReleaseProviderSnapshot(Guid generation)
    {
        if (generation == Guid.Empty) return;
        ObjectDisposedException.ThrowIf(_disposed, this);
        lock (_commitLock)
        {
            if (_providerSnapshots.Remove(generation, out FldProviderSnapshot? snapshot))
                snapshot.Dispose();

            string snapshotDirectory = GetProviderSnapshotDirectory(generation);
            if (Directory.Exists(snapshotDirectory))
            {
                Directory.Delete(snapshotDirectory, recursive: true);
                StorageDirectorySync.Flush(Path.GetDirectoryName(snapshotDirectory)!);
            }
            GcOrphanRuns();
        }
    }

    /// <summary>
    /// Removes provider snapshot generations not referenced by durable sender progress.
    /// Call only after the host has recovered the active FULL_REPLACE token set.
    /// </summary>
    public void PruneProviderSnapshotsExcept(IReadOnlySet<Guid> activeGenerations)
    {
        ArgumentNullException.ThrowIfNull(activeGenerations);
        ObjectDisposedException.ThrowIf(_disposed, this);
        lock (_commitLock)
        {
            string root = Path.Combine(_dbDir, "snapshots");
            if (!Directory.Exists(root)) return;
            foreach (string directory in Directory.EnumerateDirectories(root))
            {
                string name = Path.GetFileName(directory);
                if (!Guid.TryParseExact(name, "N", out Guid generation) ||
                    activeGenerations.Contains(generation))
                    continue;

                if (_providerSnapshots.Remove(generation, out FldProviderSnapshot? snapshot))
                    snapshot.Dispose();
                Directory.Delete(directory, recursive: true);
                StorageDirectorySync.Flush(root);
            }
            GcOrphanRuns();
        }
    }

    private bool HasDurableProviderSnapshotRun(ulong runId, bool verifyContent)
    {
        if (_runBlobStore.TryOpenRead(RunBlobId(runId), out StorageBlobReader? blobReader) &&
            blobReader is not null)
        {
            using (blobReader)
            {
                if (verifyContent)
                    blobReader.CopyTo(Stream.Null); // Verify every page and the whole-blob digest without materializing the Run.
            }
            return true;
        }
        if (_runBlobStore.ContainsBlob(RunBlobId(runId)))
            return false; // A pending blob is not a valid snapshot Run; do not hide it behind a mirror.
        return File.Exists(Path.Combine(_dbDir, "runs", $"{runId}.fld1"));
    }

    private static byte[][] BuildProviderRecordIdIndex(RunReader[] runs)
    {
        var recordIds = new List<byte[]>();
        var seen = new HashSet<(ulong Low, ulong High)>();
        Span<byte> recordId = stackalloc byte[16];
        foreach (RunReader run in runs)
        {
            for (uint index = 0; index < run.NumClusters; index++)
            {
                if (!run.TryReadRecordIdAt(index, recordId))
                    throw new InvalidDataException("Provider snapshot Run cluster index is invalid.");
                AddRecordId(recordId, seen, recordIds);
            }
        }
        recordIds.Sort(static (left, right) => left.AsSpan().SequenceCompareTo(right));
        return recordIds.ToArray();
    }

    private string GetProviderSnapshotDirectory(Guid generation)
        => Path.Combine(_dbDir, "snapshots", generation.ToString("N"));

    private static byte[] EncodeProviderSnapshotManifest(
        Guid generation,
        uint namespaceId,
        ulong sourceFeedWatermark,
        ulong[] runIds,
        byte[]? overlayHash)
    {
        int length = checked(ProviderSnapshotHeaderSize + runIds.Length * sizeof(ulong) +
            (overlayHash is null ? 0 : 32) + 32);
        byte[] bytes = new byte[length];
        BinaryPrimitives.WriteUInt32LittleEndian(bytes, ProviderSnapshotMagic);
        BinaryPrimitives.WriteUInt16LittleEndian(bytes.AsSpan(4), ProviderSnapshotVersion);
        BinaryPrimitives.WriteUInt16LittleEndian(bytes.AsSpan(6), overlayHash is null ? (ushort)0 : (ushort)1);
        BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan(8), namespaceId);
        generation.TryWriteBytes(bytes.AsSpan(12, 16));
        BinaryPrimitives.WriteUInt64LittleEndian(bytes.AsSpan(28), sourceFeedWatermark);
        BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan(36), checked((uint)runIds.Length));
        int offset = ProviderSnapshotHeaderSize;
        foreach (ulong runId in runIds)
        {
            BinaryPrimitives.WriteUInt64LittleEndian(bytes.AsSpan(offset), runId);
            offset += sizeof(ulong);
        }
        if (overlayHash is not null)
        {
            overlayHash.CopyTo(bytes, offset);
            offset += 32;
        }
        SHA256.HashData(bytes.AsSpan(0, offset)).CopyTo(bytes, offset);
        return bytes;
    }

    private static ProviderSnapshotManifest DecodeProviderSnapshotManifest(byte[] bytes)
    {
        if (bytes.Length < ProviderSnapshotHeaderSize ||
            BinaryPrimitives.ReadUInt32LittleEndian(bytes) != ProviderSnapshotMagic ||
            BinaryPrimitives.ReadUInt16LittleEndian(bytes.AsSpan(4)) != ProviderSnapshotVersion)
            throw new InvalidDataException("Provider snapshot manifest has an unsupported header.");
        ushort flags = BinaryPrimitives.ReadUInt16LittleEndian(bytes.AsSpan(6));
        if ((flags & ~1) != 0)
            throw new InvalidDataException("Provider snapshot manifest contains unknown flags.");

        uint namespaceId = BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(8));
        Guid generation = new(bytes.AsSpan(12, 16));
        ulong watermark = BinaryPrimitives.ReadUInt64LittleEndian(bytes.AsSpan(28));
        uint runCount = BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(36));
        bool hasOverlay = (flags & 1) != 0;
        long expectedLength = ProviderSnapshotHeaderSize + (long)runCount * sizeof(ulong) + (hasOverlay ? 32 : 0) + 32;
        if (generation == Guid.Empty || expectedLength != bytes.Length || runCount > int.MaxValue)
            throw new InvalidDataException("Provider snapshot manifest length or generation is invalid.");

        var runIds = new ulong[(int)runCount];
        var unique = new HashSet<ulong>();
        int offset = ProviderSnapshotHeaderSize;
        for (int i = 0; i < runIds.Length; i++)
        {
            ulong runId = BinaryPrimitives.ReadUInt64LittleEndian(bytes.AsSpan(offset));
            if (runId == 0 || !unique.Add(runId))
                throw new InvalidDataException("Provider snapshot manifest contains a zero or duplicate Run ID.");
            runIds[i] = runId;
            offset += sizeof(ulong);
        }
        byte[]? overlayHash = null;
        if (hasOverlay)
        {
            overlayHash = bytes.AsSpan(offset, 32).ToArray();
            offset += 32;
        }
        byte[] actualHash = SHA256.HashData(bytes.AsSpan(0, offset));
        if (!CryptographicOperations.FixedTimeEquals(actualHash, bytes.AsSpan(offset, 32)))
            throw new InvalidDataException("Provider snapshot manifest checksum mismatch.");
        return new ProviderSnapshotManifest(generation, namespaceId, watermark, runIds, overlayHash);
    }

    private ProviderSnapshotManifest ReadProviderSnapshotManifest(string directory, Guid expectedGeneration)
    {
        byte[] bytes = File.ReadAllBytes(Path.Combine(directory, ProviderSnapshotManifestName));
        ProviderSnapshotManifest manifest = DecodeProviderSnapshotManifest(bytes);
        if (manifest.Generation != expectedGeneration)
            throw new InvalidDataException("Provider snapshot directory and manifest generation differ.");
        return manifest;
    }

    private static void WriteDurableSnapshotFile(string path, byte[] bytes)
    {
        using (var stream = new FileStream(
            path, FileMode.CreateNew, FileAccess.Write, FileShare.None,
            64 * 1024, FileOptions.WriteThrough))
        {
            stream.Write(bytes);
            stream.Flush(flushToDisk: true);
        }
        StorageDirectorySync.Flush(Path.GetDirectoryName(path)!);
    }

    private static void WriteProviderSnapshotManifest(string directory, byte[] bytes)
    {
        string manifestPath = Path.Combine(directory, ProviderSnapshotManifestName);
        string temporaryPath = manifestPath + ".tmp";
        if (File.Exists(temporaryPath)) File.Delete(temporaryPath);
        WriteDurableSnapshotFile(temporaryPath, bytes);
        File.Move(temporaryPath, manifestPath, overwrite: true);
        StorageDirectorySync.Flush(directory);
    }

    private HashSet<ulong>? GetPinnedProviderSnapshotRunIds()
    {
        var pinned = new HashSet<ulong>();
        string root = Path.Combine(_dbDir, "snapshots");
        if (!Directory.Exists(root)) return pinned;
        foreach (string directory in Directory.EnumerateDirectories(root))
        {
            string manifestPath = Path.Combine(directory, ProviderSnapshotManifestName);
            if (!File.Exists(manifestPath)) continue;
            try
            {
                ProviderSnapshotManifest manifest = DecodeProviderSnapshotManifest(File.ReadAllBytes(manifestPath));
                foreach (ulong runId in manifest.RunIds)
                    pinned.Add(runId);
            }
            catch
            {
                // Corrupt or unknown snapshot metadata must never cause a potentially
                // referenced immutable Run to be deleted.
                return null;
            }
        }
        return pinned;
    }
}
