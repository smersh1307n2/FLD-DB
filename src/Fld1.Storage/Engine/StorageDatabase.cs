using System.Buffers.Binary;
using Fld1.Storage.Wal.Format;
using Fld1.Storage.Wal.IO;

namespace Fld1.Storage.Engine;

/// <summary>
/// Embedded byte-key/value store built on the domain-neutral Storage WAL.
/// Each WriteBatch is one durable atomic commit. Values are opaque bytes.
/// </summary>
public sealed class StorageDatabase : IDisposable
{
    private const byte BatchCommitKind = 0x01;
    private const int MaxBatchOperations = 100_000;
    private const int MaxKeyLength = 1024 * 1024;
    private readonly object _gate = new();
    private readonly StoragePageCache _pageCache = new(16L * 1024 * 1024);
    private readonly StorageBTreeReaderCache _readerCache = new();
    private readonly StorageGenerationPins _generationPins = new();
    private readonly Dictionary<ulong, StorageBTreeGenerationFiles> _generationFiles = new();
    private readonly HashSet<ulong> _preserveGenerationFiles = new();
    private readonly WalManager _wal;
    private StorageBTreeGenerationFiles? _btreeFiles;
    private StorageBTreeManifestV2 _btreeManifest;
    private StorageBTreeReader? _btreeReader;
    private readonly string _directory;
    private readonly Action<StorageFaultPoint>? _faultInjector;
    private int _liveCount;
    private ulong _snapshotCommitSeq;
    private bool _disposed;
    private bool _faulted;
    private ulong _lastCommitSeq;

    public ulong PartitionId => _wal.PartitionId;
    /// <summary>Canonical directory containing this Storage partition.</summary>
    public string DirectoryPath => _directory;
    public ulong LastCommitSeq { get { lock (_gate) { EnsureUsable(); return _lastCommitSeq; } } }
    internal int ActiveWalSegment => _wal.ActiveSegment;
    public int Count { get { lock (_gate) { EnsureUsable(); if (_liveCount < 0) _liveCount = CountBTreeValues(); return _liveCount; } } }

    private StorageDatabase(string directory, WalManager wal, Action<StorageFaultPoint>? faultInjector)
    {
        _directory = directory;
        _wal = wal;
        _faultInjector = faultInjector;
    }

    public static StorageDatabase Open(string directory, ulong partitionId, long walRotateAfterBytes = 64L * 1024 * 1024)
        => OpenCore(directory, partitionId, walRotateAfterBytes, faultInjector: null);

    /// <summary>
    /// Acquire the same cross-process single-writer fence used by StorageDatabase.Open
    /// without opening or creating a generic Storage WAL. Domain databases with an
    /// existing on-disk format can use this lease while retaining their own WAL codec.
    /// </summary>
    public static StoragePartitionLease AcquirePartitionLease(string directory, ulong partitionId)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(directory);
        StorageDirectorySync.CreateDirectoryDurable(directory);
        string walDirectory = Path.Combine(directory, "wal");
        StorageDirectorySync.CreateDirectoryDurable(walDirectory);
        return new StoragePartitionLease(WalWriterFence.Acquire(walDirectory, partitionId));
    }

    internal static StorageDatabase OpenForTesting(
        string directory,
        ulong partitionId,
        Action<StorageFaultPoint> faultInjector,
        long walRotateAfterBytes = 64L * 1024 * 1024)
    {
        ArgumentNullException.ThrowIfNull(faultInjector);
        return OpenCore(directory, partitionId, walRotateAfterBytes, faultInjector);
    }

    private static StorageDatabase OpenCore(
        string directory,
        ulong partitionId,
        long walRotateAfterBytes,
        Action<StorageFaultPoint>? faultInjector)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(directory);
        if (walRotateAfterBytes < 0) throw new ArgumentOutOfRangeException(nameof(walRotateAfterBytes));
        directory = Path.GetFullPath(directory);
        StorageDirectorySync.CreateDirectoryDurable(directory);
        string walDirectory = Path.Combine(directory, "wal");
        // Acquire the partition's exclusive writer fence before reading the snapshot
        // or WAL. Otherwise a second process could race recovery against a checkpoint.
        var (wal, recovery) = faultInjector is null
            ? WalManager.OpenStreaming(walDirectory, partitionId, walRotateAfterBytes)
            : WalManager.OpenStreamingForTesting(walDirectory, partitionId, walRotateAfterBytes, faultInjector);
        var database = new StorageDatabase(directory, wal, faultInjector);
        try
        {
            // Always validate the legacy checkpoint manifest while it exists. It is
            // still used for upgrade compatibility, but the v2 root is authoritative
            // once published.
            StorageBTreeManifestV2? root = StorageBTreeRootPublisher.TryLoad(directory, partitionId);
            var snapshot = root.HasValue ? null : StorageSnapshotFile.Load(directory, partitionId);

            if (root.HasValue)
            {
                database._snapshotCommitSeq = root.Value.CommitSeq;
                database._btreeManifest = root.Value;
                database._lastCommitSeq = root.Value.CommitSeq;

                var files = new StorageBTreeGenerationFiles(directory, partitionId, root.Value.Generation);
                try
                {
                    // WAL has already been fully validated by OpenStreaming. Only now
                    // may bytes beyond the published root be discarded.
                    files.TruncateToCommittedLengths(
                        root.Value.CommittedPageFileLength,
                        root.Value.CommittedDataFileLength);
                    database._btreeFiles = files;
                    database._generationPins.Register(root.Value.Generation);
                    database._generationFiles.Add(root.Value.Generation, files);
                    database._btreeReader = database.CreateBTreeReader(root.Value, files);
                    database._liveCount = -1;
                }
                catch
                {
                    files.Dispose();
                    throw;
                }
            }
            else
            {
                if (snapshot is not null)
                {
                    database._liveCount = snapshot.Values.Count;
                    database._snapshotCommitSeq = snapshot.CommitSeq;
                    database._lastCommitSeq = snapshot.CommitSeq;
                }
                else if (!recovery.HasRecords && wal.NextCommitSeq != 1)
                {
                    throw new InvalidDataException("Storage WAL starts after sequence 1 but no snapshot or B+ tree root exists.");
                }

                // One-time migration from the legacy snapshot/WAL format. Publish the
                // root at the snapshot CommitSeq before replaying any later WAL records.
                ulong generation = snapshot?.Generation ?? 1;
                var files = new StorageBTreeGenerationFiles(directory, partitionId, generation);
                try
                {
                    files.TruncateToCommittedLengths(0, 0);
                    StorageBTreeManifestV2 initial = StorageBTreeGenerationBuilder.Build(
                        files, database._lastCommitSeq,
                        CreateBTreeBuildInputs(snapshot?.Values
                            ?? new SortedDictionary<StorageKey, byte[]>(StorageKeyComparer.Instance)));
                    StorageBTreeRootPublisher.Publish(directory, initial, files);
                    database._btreeFiles = files;
                    database._generationPins.Register(initial.Generation);
                    database._generationFiles.Add(initial.Generation, files);
                    database._btreeManifest = initial;
                    database._btreeReader = database.CreateBTreeReader(initial, files);
                }
                catch
                {
                    files.Dispose();
                    throw;
                }
            }

            recovery.Replay(record =>
            {
                if (record.CommitSeq <= database._lastCommitSeq)
                    return;
                if (record.Kind != BatchCommitKind)
                    throw new InvalidDataException($"Unknown Storage WAL record kind {record.Kind}.");
                database.ApplyRecovered(record.CommitSeq, record.Payload);
            });

            if (wal.NextCommitSeq != checked(database._lastCommitSeq + 1))
                throw new InvalidDataException("Storage root and WAL commit sequence do not form a contiguous recovery chain.");

            // Every WAL record is now validated and represented by the published root.
            // Keep covered WAL segments until an explicit checkpoint: their segment
            // identities also preserve the monotonic rotation sequence across reopen.
            CleanupUnreferencedGenerations(directory, partitionId, database._btreeManifest.Generation);
            return database;
        }
        catch
        {
            database._btreeFiles?.Dispose();
            wal.Dispose();
            throw;
        }
    }

    private static void CleanupUnreferencedGenerations(
        string directory,
        ulong partitionId,
        ulong currentGeneration)
    {
        string snapshots = Path.Combine(directory, "snapshots");
        if (!Directory.Exists(snapshots)) return;

        string prefix = $"p_{partitionId}.";
        bool changed = false;
        foreach (string extension in new[] { ".pages", ".data" })
        {
            foreach (string path in Directory.EnumerateFiles(snapshots, prefix + "*" + extension))
            {
                string name = Path.GetFileName(path);
                string generationText = name.Substring(prefix.Length, name.Length - prefix.Length - extension.Length);
                if (!ulong.TryParse(generationText, out ulong generation) ||
                    generation == currentGeneration)
                    continue;
                try
                {
                    File.Delete(path);
                    changed = true;
                }
                catch (IOException) { }
                catch (UnauthorizedAccessException) { }
            }
        }

        // Legacy snapshots are read only when migrating a database with no v2 root.
        // Once a valid root exists they are redundant and can be removed.
        foreach (string path in Directory.EnumerateFiles(snapshots, prefix + "*.snap"))
        {
            try { File.Delete(path); changed = true; }
            catch (IOException) { }
            catch (UnauthorizedAccessException) { }
        }
        string legacyManifest = Path.Combine(snapshots, $"p_{partitionId}.manifest");
        if (File.Exists(legacyManifest))
        {
            try { File.Delete(legacyManifest); changed = true; }
            catch (IOException) { }
            catch (UnauthorizedAccessException) { }
        }

        if (changed) StorageDirectorySync.Flush(snapshots);
    }

    private int CountBTreeValues()
    {
        StorageBTreeReader reader = _btreeReader
            ?? throw new InvalidOperationException("B+ tree reader is not initialized.");
        using StorageBTreePrefixCursor cursor = reader.OpenAllCursor();
        int count = 0;
        while (cursor.TryReadNext(out _))
            count = checked(count + 1);
        return count;
    }

    private StorageBTreeReader CreateBTreeReader(
        StorageBTreeManifestV2 manifest,
        StorageBTreeGenerationFiles files)
        => new(
            manifest.Generation,
            manifest.RootPageId,
            pageId => _pageCache.Acquire(
                new StoragePageKey(manifest.Generation, pageId),
                id => files.ReadPage(id, manifest.CommittedPageFileLength)),
            key => files.ReadData(
                key.Offset,
                key.Length,
                manifest.CommittedDataFileLength),
            _readerCache);

    private static StorageTreeBuildInput[] CreateBTreeBuildInputs(
        SortedDictionary<StorageKey, byte[]> values)
        => values.Select(pair => new StorageTreeBuildInput(
            pair.Key.Span.ToArray(),
            pair.Value,
            IsTombstone: false)).ToArray();

    private StorageTreeBuildInput[] CreateBTreeBuildInputsFromCurrentTree()
    {
        StorageBTreeGenerationFiles files = _btreeFiles
            ?? throw new InvalidOperationException("B+ tree generation files are not open.");
        StorageBTreeManifestV2 manifest = _btreeManifest;
        var reader = new StorageBTreeReader(
            manifest.Generation,
            manifest.RootPageId,
            pageId => files.ReadPage(pageId, manifest.CommittedPageFileLength),
            key => files.ReadData(key.Offset, key.Length, manifest.CommittedDataFileLength),
            _readerCache);

        var entries = new List<StorageTreeBuildInput>();
        using StorageBTreePrefixCursor cursor = reader.OpenAllCursor(includeTombstones: true);
        while (cursor.TryReadNext(out StorageTreeEntry entry))
        {
            byte[] key = files.ReadData(
                entry.Key.Offset, entry.Key.Length, manifest.CommittedDataFileLength);
            if (entry.Value.IsTombstone)
            {
                entries.Add(new StorageTreeBuildInput(
                    key, ReadOnlyMemory<byte>.Empty, IsTombstone: true));
            }
            else
            {
                byte[] value = files.ReadData(
                    entry.Value.Offset, entry.Value.Length, manifest.CommittedDataFileLength);
                entries.Add(new StorageTreeBuildInput(key, value, IsTombstone: false));
            }
        }

        return entries.ToArray();
    }

    public StorageWriteBatch BeginWriteBatch()
    {
        lock (_gate) { EnsureUsable(); return new StorageWriteBatch(this, _lastCommitSeq); }
    }

    public bool TryGet(ReadOnlySpan<byte> key, out byte[]? value)
    {
        if (key.IsEmpty) throw new ArgumentException("Storage key must not be empty.", nameof(key));
        lock (_gate)
        {
            EnsureUsable();
            StorageBTreeReader reader = _btreeReader
                ?? throw new InvalidOperationException("B+ tree reader is not initialized.");
            if (!reader.TryGet(key, out StorageTreeValueReference reference))
            {
                value = null;
                return false;
            }

            StorageBTreeGenerationFiles files = _btreeFiles
                ?? throw new InvalidOperationException("B+ tree generation files are not open.");
            value = files.ReadData(
                reference.Offset,
                reference.Length,
                _btreeManifest.CommittedDataFileLength);
            return true;
        }
    }

    /// <summary>
    /// Publish an immutable on-disk checkpoint, then reclaim only WAL segments
    /// fully covered by its independently durable manifest root.
    /// </summary>
    public void Checkpoint()
    {
        lock (_gate)
        {
            EnsureUsable();
            if (_lastCommitSeq == _snapshotCommitSeq) return;

            StorageBTreeGenerationFiles? compactedFiles = null;
            try
            {
                _wal.Flush();
                _wal.Rotate();

                // Compact the append-only COW generation into a fresh immutable
                // generation at the same CommitSeq. Root publication precedes WAL
                // truncation; old generation files are retained until explicit GC.
                ulong nextGeneration = checked(_btreeManifest.Generation + 1);
                compactedFiles = new StorageBTreeGenerationFiles(_directory, PartitionId, nextGeneration);
                compactedFiles.TruncateToCommittedLengths(0, 0);
                StorageBTreeManifestV2 compacted = StorageBTreeGenerationBuilder.Build(
                    compactedFiles, _lastCommitSeq, CreateBTreeBuildInputsFromCurrentTree());
                StorageBTreeRootPublisher.Publish(_directory, compacted, compactedFiles, _faultInjector);

                StorageBTreeGenerationFiles oldFiles = _btreeFiles
                    ?? throw new InvalidOperationException("B+ tree generation files are not open.");
                _generationPins.Register(compacted.Generation);
                _generationFiles.Add(compacted.Generation, compactedFiles);
                _btreeFiles = compactedFiles;
                _btreeManifest = compacted;
                _btreeReader = CreateBTreeReader(compacted, compactedFiles);
                compactedFiles = null;
                _ = _generationPins.Retire(oldFiles.Generation);
                ReclaimRetiredGenerations();

                // The B+ tree root is the checkpoint; no duplicate snapshot is written.
                _snapshotCommitSeq = _lastCommitSeq;
                _wal.TruncateSealedSegments(_btreeManifest.CommitSeq);
            }
            catch
            {
                compactedFiles?.Dispose();
                // A failed checkpoint may have published a new B+ tree root, an orphan
                // generation or a legacy snapshot. Reopen is the only authority for
                // reconciling the manifest/WAL pair.
                _faulted = true;
                throw;
            }
        }
    }

    public StorageSnapshot OpenSnapshot()
    {
        lock (_gate)
        {
            EnsureUsable();
            StorageBTreeReader reader = _btreeReader
                ?? throw new InvalidOperationException("B+ tree reader is not initialized.");
            StorageBTreeGenerationFiles files = _btreeFiles
                ?? throw new InvalidOperationException("B+ tree generation files are not open.");
            StorageGenerationPin pin = _generationPins.Acquire(_btreeManifest.Generation);
            return new StorageSnapshot(
                reader,
                files,
                _btreeManifest,
                pin,
                _lastCommitSeq,
                OnSnapshotPinReleased);
        }
    }

    private void OnSnapshotPinReleased(ulong generation)
    {
        lock (_gate)
            ReclaimRetiredGenerations();
    }

    private void ReclaimRetiredGenerations()
    {
        while (_generationPins.TryTakeReclaimable(out ulong generation))
        {
            if (!_generationFiles.Remove(generation, out StorageBTreeGenerationFiles? files))
                throw new InvalidOperationException("Retired B+ tree generation has no open file owner.");
            _pageCache.EvictGeneration(generation);
            _readerCache.EvictGeneration(generation);
            files.Dispose();
            if (_preserveGenerationFiles.Remove(generation))
                continue;

            string snapshots = Path.Combine(_directory, "snapshots");
            string prefix = $"p_{PartitionId}.{generation}";
            foreach (string extension in new[] { ".pages", ".data" })
            {
                string path = Path.Combine(snapshots, prefix + extension);
                if (File.Exists(path)) File.Delete(path);
            }
            StorageDirectorySync.Flush(snapshots);
        }
    }

    internal ulong Commit(IReadOnlyCollection<StorageMutation> mutations, ulong expectedBaseCommitSeq)
    {
        lock (_gate)
        {
            EnsureUsable();
            if (mutations.Count == 0) return _lastCommitSeq;
            if (_lastCommitSeq != expectedBaseCommitSeq)
                throw new StorageConflictException(expectedBaseCommitSeq, _lastCommitSeq);
            if (mutations.Count > MaxBatchOperations)
                throw new ArgumentOutOfRangeException(nameof(mutations), "Storage batch operation limit exceeded.");

            StorageBTreeReader currentReader = _btreeReader
                ?? throw new InvalidOperationException("B+ tree reader is not initialized.");
            int nextLiveCount = _liveCount < 0 ? CountBTreeValues() : _liveCount;
            foreach (StorageMutation mutation in mutations)
            {
                bool existed = currentReader.TryGet(mutation.Key, out _);
                if (mutation.IsDelete && existed) nextLiveCount--;
                else if (!mutation.IsDelete && !existed) nextLiveCount = checked(nextLiveCount + 1);
            }

            byte[] payload = EncodeBatch(mutations);
            try
            {
                StorageBTreeGenerationFiles files = _btreeFiles
                    ?? throw new InvalidOperationException("B+ tree generation files are not open.");
                StorageTreeMutation[] treeMutations = mutations
                    .OrderBy(x => new StorageKey(x.Key), StorageKeyComparer.Instance)
                    .Select(x => new StorageTreeMutation(x.Key, x.Value, x.IsDelete))
                    .ToArray();

                StorageBTreeManifestV2 next = StorageBTreeUpdater.ApplyAndPublish(
                    _directory,
                    _btreeManifest,
                    files,
                    treeMutations,
                    appendAndFlushWal: () =>
                    {
                        ulong sequence = _wal.Append(BatchCommitKind, payload);
                        _faultInjector?.Invoke(StorageFaultPoint.AfterWalAppendBeforeFlush);
                        _wal.Flush();
                        _faultInjector?.Invoke(StorageFaultPoint.AfterWalFlushBeforePublish);
                        return sequence;
                    },
                    faultInjector: _faultInjector);

                _btreeManifest = next;
                _btreeReader = CreateBTreeReader(next, files);
                _liveCount = nextLiveCount;
                _lastCommitSeq = next.CommitSeq;
                return next.CommitSeq;
            }
            catch
            {
                // WAL is durable before COW pages/root publication. Any failure makes
                // this instance unusable; reopen reconciles the last root with the WAL.
                _faulted = true;
                throw;
            }
        }
    }

    private void ApplyRecovered(ulong commitSeq, ReadOnlySpan<byte> payload)
    {
        if (commitSeq != _lastCommitSeq + 1)
            throw new InvalidDataException($"Storage WAL CommitSeq {commitSeq} does not follow {_lastCommitSeq}.");
        var mutations = DecodeBatch(payload);
        StorageBTreeReader currentReader = _btreeReader
            ?? throw new InvalidOperationException("B+ tree reader is not initialized.");
        int nextLiveCount = _liveCount;
        foreach (StorageMutation mutation in mutations)
        {
            bool existed = currentReader.TryGet(mutation.Key, out _);
            if (mutation.IsDelete && existed) nextLiveCount--;
            else if (!mutation.IsDelete && !existed) nextLiveCount = checked(nextLiveCount + 1);
        }

        StorageBTreeGenerationFiles files = _btreeFiles
            ?? throw new InvalidOperationException("B+ tree generation files are not open.");
        StorageTreeMutation[] treeMutations = mutations
            .OrderBy(x => new StorageKey(x.Key), StorageKeyComparer.Instance)
            .Select(x => new StorageTreeMutation(x.Key, x.Value, x.IsDelete))
            .ToArray();

        // The WAL record has already been validated and is durable. Replay therefore
        // applies COW pages/root directly without appending a duplicate WAL record.
        StorageBTreeManifestV2 next = StorageBTreeUpdater.ApplyBatch(
            _btreeManifest, files, treeMutations);
        if (next.CommitSeq != commitSeq)
            throw new InvalidDataException("Recovered WAL and B+ tree CommitSeq values diverged.");
        StorageBTreeRootPublisher.Publish(_directory, next, files, _faultInjector);

        _btreeManifest = next;
        _btreeReader = CreateBTreeReader(next, files);
        _liveCount = nextLiveCount;
        _lastCommitSeq = commitSeq;
    }

    private static byte[] EncodeBatch(IReadOnlyCollection<StorageMutation> mutations)
    {
        using var stream = new MemoryStream();
        Span<byte> header = stackalloc byte[4];
        BinaryPrimitives.WriteInt32LittleEndian(header, mutations.Count);
        stream.Write(header);

        Span<byte> lengths = stackalloc byte[8];
        foreach (var mutation in mutations.OrderBy(x => new StorageKey(x.Key), StorageKeyComparer.Instance))
        {
            if (mutation.Key.Length is 0 or > MaxKeyLength)
                throw new ArgumentOutOfRangeException(nameof(mutations), "Storage key length is invalid.");
            if (!mutation.IsDelete && mutation.Value.Length > WalConstants.MaxPayloadLen)
                throw new ArgumentOutOfRangeException(nameof(mutations), "Storage value is too large.");

            stream.WriteByte(mutation.IsDelete ? (byte)2 : (byte)1);
            BinaryPrimitives.WriteInt32LittleEndian(lengths, mutation.Key.Length);
            BinaryPrimitives.WriteInt32LittleEndian(lengths.Slice(4), mutation.IsDelete ? 0 : mutation.Value.Length);
            stream.Write(lengths);
            stream.Write(mutation.Key);
            if (!mutation.IsDelete) stream.Write(mutation.Value);
        }

        if (stream.Length > WalConstants.MaxPayloadLen)
            throw new ArgumentOutOfRangeException(nameof(mutations), "Encoded Storage batch exceeds the WAL payload limit.");
        return stream.ToArray();
    }

    private static List<StorageMutation> DecodeBatch(ReadOnlySpan<byte> payload)
    {
        if (payload.Length < 4) throw new InvalidDataException("Storage batch header is truncated.");
        int count = BinaryPrimitives.ReadInt32LittleEndian(payload);
        if (count is < 1 or > MaxBatchOperations)
            throw new InvalidDataException("Storage batch operation count is invalid.");

        int offset = 4;
        var result = new List<StorageMutation>(count);
        var seen = new HashSet<StorageKey>();
        for (int i = 0; i < count; i++)
        {
            if (payload.Length - offset < 9) throw new InvalidDataException("Storage batch operation header is truncated.");
            byte kind = payload[offset++];
            int keyLength = BinaryPrimitives.ReadInt32LittleEndian(payload.Slice(offset, 4)); offset += 4;
            int valueLength = BinaryPrimitives.ReadInt32LittleEndian(payload.Slice(offset, 4)); offset += 4;
            if (kind is not (1 or 2) || keyLength is <= 0 or > MaxKeyLength || valueLength < 0 ||
                (kind == 2 && valueLength != 0) || (kind == 1 && valueLength > WalConstants.MaxPayloadLen))
                throw new InvalidDataException("Storage batch operation fields are invalid.");
            if (payload.Length - offset < keyLength + valueLength)
                throw new InvalidDataException("Storage batch operation payload is truncated.");

            byte[] key = payload.Slice(offset, keyLength).ToArray(); offset += keyLength;
            byte[] value = payload.Slice(offset, valueLength).ToArray(); offset += valueLength;
            if (!seen.Add(new StorageKey(key)))
                throw new InvalidDataException("Storage batch contains duplicate keys.");
            result.Add(new StorageMutation(key, value, kind == 2));
        }

        if (offset != payload.Length) throw new InvalidDataException("Storage batch has trailing bytes.");
        return result;
    }

    private void EnsureUsable()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (_faulted) throw new InvalidOperationException("Storage requires reopen after a failed commit.");
    }

    public void Dispose()
    {
        lock (_gate)
        {
            if (_disposed) return;
            _disposed = true;
            try
            {
                if (_generationFiles.ContainsKey(_btreeManifest.Generation))
                    _preserveGenerationFiles.Add(_btreeManifest.Generation);
                foreach (ulong generation in _generationFiles.Keys.ToArray())
                    _ = _generationPins.Retire(generation);
                ReclaimRetiredGenerations();
                _btreeFiles = null;
                _btreeReader = null;
            }
            finally { _wal.Dispose(); }
        }
    }

    internal readonly record struct StorageMutation(byte[] Key, byte[] Value, bool IsDelete);

    internal readonly struct StorageKey : IEquatable<StorageKey>
    {
        private readonly byte[] _bytes;
        public StorageKey(ReadOnlySpan<byte> bytes) => _bytes = bytes.ToArray();
        public ReadOnlySpan<byte> Span => _bytes;
        public bool Equals(StorageKey other) => _bytes.AsSpan().SequenceEqual(other._bytes);
        public override bool Equals(object? obj) => obj is StorageKey other && Equals(other);
        public override int GetHashCode()
        {
            var hash = new HashCode();
            foreach (byte value in _bytes) hash.Add(value);
            return hash.ToHashCode();
        }
    }

    internal sealed class StorageKeyComparer : IComparer<StorageKey>
    {
        public static StorageKeyComparer Instance { get; } = new();
        public int Compare(StorageKey x, StorageKey y) => x.Span.SequenceCompareTo(y.Span);
    }
}

/// <summary>
/// Cross-process single-writer lease for a Storage partition. It may also fence a
/// domain-specific database that shares the physical directory but keeps its own WAL format.
/// </summary>
public sealed class StoragePartitionLease : IDisposable
{
    private WalWriterFence? _fence;

    internal StoragePartitionLease(WalWriterFence fence)
        => _fence = fence ?? throw new ArgumentNullException(nameof(fence));

    public void Dispose()
        => Interlocked.Exchange(ref _fence, null)?.Dispose();
}

/// <summary>One atomic set of opaque key/value mutations.</summary>
public sealed class StorageWriteBatch : IDisposable
{
    private readonly StorageDatabase _database;
    private readonly Dictionary<StorageDatabase.StorageKey, StorageDatabase.StorageMutation> _mutations = new();
    private readonly ulong _baseCommitSeq;
    private bool _completed;

    internal StorageWriteBatch(StorageDatabase database, ulong baseCommitSeq)
    {
        _database = database;
        _baseCommitSeq = baseCommitSeq;
    }

    /// <summary>Read-your-writes lookup. Commit fails if the database changed since this batch began.</summary>
    public bool TryGet(ReadOnlySpan<byte> key, out byte[]? value)
    {
        EnsureOpen();
        if (key.IsEmpty) throw new ArgumentException("Storage key must not be empty.", nameof(key));
        if (_mutations.TryGetValue(new StorageDatabase.StorageKey(key), out var mutation))
        {
            value = mutation.IsDelete ? null : mutation.Value.ToArray();
            return !mutation.IsDelete;
        }

        return _database.TryGet(key, out value);
    }

    public void Put(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
    {
        EnsureOpen();
        if (key.IsEmpty) throw new ArgumentException("Storage key must not be empty.", nameof(key));
        _mutations[new StorageDatabase.StorageKey(key)] =
            new StorageDatabase.StorageMutation(key.ToArray(), value.ToArray(), false);
    }

    public void Delete(ReadOnlySpan<byte> key)
    {
        EnsureOpen();
        if (key.IsEmpty) throw new ArgumentException("Storage key must not be empty.", nameof(key));
        _mutations[new StorageDatabase.StorageKey(key)] =
            new StorageDatabase.StorageMutation(key.ToArray(), Array.Empty<byte>(), true);
    }

    internal bool IsBoundTo(StorageDatabase database)
        => ReferenceEquals(_database, database);

    public ulong Commit()
    {
        EnsureOpen();
        _completed = true;
        return _database.Commit(_mutations.Values.ToArray(), _baseCommitSeq);
    }

    public void Dispose() => _completed = true;

    private void EnsureOpen()
    {
        ObjectDisposedException.ThrowIf(_completed, this);
    }
}

/// <summary>Immutable, stable point-in-time view of a Storage partition.</summary>
public sealed class StorageSnapshot : IDisposable
{
    private StorageBTreeReader? _reader;
    private StorageBTreeGenerationFiles? _files;
    private readonly StorageBTreeManifestV2 _manifest;
    private StorageGenerationPin? _pin;
    private Action<ulong>? _onPinReleased;
    private readonly object _cursorGate = new();
    private readonly HashSet<PrefixCursor> _cursors = new();
    public ulong CommitSeq { get; }

    internal StorageSnapshot(
        StorageBTreeReader reader,
        StorageBTreeGenerationFiles files,
        in StorageBTreeManifestV2 manifest,
        StorageGenerationPin pin,
        ulong commitSeq,
        Action<ulong> onPinReleased)
    {
        _reader = reader ?? throw new ArgumentNullException(nameof(reader));
        _files = files ?? throw new ArgumentNullException(nameof(files));
        _manifest = manifest;
        _pin = pin ?? throw new ArgumentNullException(nameof(pin));
        CommitSeq = commitSeq;
        _onPinReleased = onPinReleased ?? throw new ArgumentNullException(nameof(onPinReleased));
    }

    public bool TryGet(ReadOnlySpan<byte> key, out byte[]? value)
    {
        if (key.IsEmpty) throw new ArgumentException("Storage key must not be empty.", nameof(key));
        if (!GetReader().TryGet(key, out StorageTreeValueReference reference))
        {
            value = null;
            return false;
        }

        value = GetFiles().ReadData(
            reference.Offset,
            reference.Length,
            _manifest.CommittedDataFileLength);
        return true;
    }

    public IReadOnlyList<StorageEntry> ScanPrefix(ReadOnlySpan<byte> prefix, ReadOnlySpan<byte> afterKey, int limit)
    {
        using PrefixCursor cursor = OpenPrefixCursor(prefix, afterKey);
        return cursor.ReadPage(limit);
    }

    /// <summary>Creates a forward-only cursor over the immutable B+ tree generation pinned by this snapshot.</summary>
    public PrefixCursor OpenPrefixCursor(ReadOnlySpan<byte> prefix, ReadOnlySpan<byte> afterKey = default)
    {
        lock (_cursorGate)
        {
            StorageBTreeReader reader = GetReader();
            var cursor = new PrefixCursor(this, reader, prefix.ToArray(), afterKey.ToArray());
            _cursors.Add(cursor);
            return cursor;
        }
    }

    private void UnregisterCursor(PrefixCursor cursor)
    {
        lock (_cursorGate)
            _cursors.Remove(cursor);
    }

    private StorageBTreeReader GetReader()
        => _reader ?? throw new ObjectDisposedException(nameof(StorageSnapshot));

    private StorageBTreeGenerationFiles GetFiles()
        => _files ?? throw new ObjectDisposedException(nameof(StorageSnapshot));

    public sealed class PrefixCursor : IDisposable
    {
        private readonly StorageSnapshot _owner;
        private readonly byte[] _prefix;
        private readonly byte[] _afterKey;
        private StorageBTreePrefixCursor? _cursor;
        private byte[]? _currentKey;
        private byte[]? _currentValue;
        private bool _finished;
        private bool _disposed;

        internal PrefixCursor(StorageSnapshot owner, StorageBTreeReader reader, byte[] prefix, byte[] afterKey)
        {
            _owner = owner;
            _prefix = prefix;
            _afterKey = afterKey;
            _cursor = prefix.Length == 0
                ? reader.OpenAllCursor(afterKey: afterKey)
                : reader.OpenPrefixCursor(prefix, afterKey);
        }

        public IReadOnlyList<StorageEntry> ReadPage(int limit)
        {
            if (limit <= 0) throw new ArgumentOutOfRangeException(nameof(limit));
            var result = new List<StorageEntry>(Math.Min(limit, 128));
            VisitPage(limit, Add);
            return result;

            bool Add(in StorageEntryView entry)
            {
                result.Add(new StorageEntry(entry.Key.ToArray(), entry.Value.ToArray()));
                return true;
            }
        }

        /// <summary>
        /// Visit a bounded page using borrowed key/value spans. The view is valid only
        /// during the callback; do not retain it or perform asynchronous work in the visitor.
        /// Returning false stops after the current entry, which is considered visited.
        /// </summary>
        public int VisitPage(int limit, StorageEntryVisitor visitor)
        {
            if (limit <= 0) throw new ArgumentOutOfRangeException(nameof(limit));
            ArgumentNullException.ThrowIfNull(visitor);
            ObjectDisposedException.ThrowIf(_disposed, this);
            int visited = 0;
            while (visited < limit && TryReadNext(out StorageEntryView entry))
            {
                visited++;
                if (!visitor(in entry))
                    break;
            }
            return visited;
        }

        /// <summary>Read one borrowed entry without allocating a page or copying beyond the current key/value.</summary>
        public bool TryReadNext(out StorageEntryView entry)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            _ = _owner.GetReader(); // Fail if the owning snapshot has already been disposed.
            if (_finished)
            {
                entry = default;
                return false;
            }

            StorageBTreePrefixCursor cursor = _cursor
                ?? throw new ObjectDisposedException(nameof(PrefixCursor));
            StorageBTreeGenerationFiles files = _owner.GetFiles();
            StorageBTreeManifestV2 manifest = _owner._manifest;
            while (cursor.TryReadNext(out StorageTreeEntry treeEntry))
            {
                byte[] key = files.ReadData(
                    treeEntry.Key.Offset,
                    treeEntry.Key.Length,
                    manifest.CommittedDataFileLength);
                if (_afterKey.Length != 0 && key.AsSpan().SequenceCompareTo(_afterKey) <= 0)
                    continue;
                if (_prefix.Length != 0 && !key.AsSpan().StartsWith(_prefix))
                {
                    _finished = true;
                    break;
                }

                byte[] value = files.ReadData(
                    treeEntry.Value.Offset,
                    treeEntry.Value.Length,
                    manifest.CommittedDataFileLength);
                _currentKey = key;
                _currentValue = value;
                entry = new StorageEntryView(_currentKey, _currentValue);
                return true;
            }

            _finished = true;
            entry = default;
            return false;
        }

        public void Dispose()
        {
            if (_disposed) return;
            DisposeCore();
            _owner.UnregisterCursor(this);
        }

        internal void DisposeFromOwner() => DisposeCore();

        private void DisposeCore()
        {
            if (_disposed) return;
            _disposed = true;
            _finished = true;
            _cursor?.Dispose();
            _cursor = null;
            _currentKey = null;
            _currentValue = null;
        }
    }

    public void Dispose()
    {
        PrefixCursor[] cursors;
        lock (_cursorGate)
        {
            cursors = _cursors.ToArray();
            _cursors.Clear();
            _reader = null;
            _files = null;
        }
        foreach (PrefixCursor cursor in cursors)
            cursor.DisposeFromOwner();

        StorageGenerationPin? pin = Interlocked.Exchange(ref _pin, null);
        Action<ulong>? callback = Interlocked.Exchange(ref _onPinReleased, null);
        if (pin is null) return;
        ulong generation = pin.Generation;
        pin.Dispose();
        callback?.Invoke(generation);
    }
}

public sealed record StorageEntry(byte[] Key, byte[] Value);

/// <summary>Borrowed, read-only view of one key/value entry in a Storage snapshot.</summary>
public readonly ref struct StorageEntryView
{
    public ReadOnlySpan<byte> Key { get; }
    public ReadOnlySpan<byte> Value { get; }

    internal StorageEntryView(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
    {
        Key = key;
        Value = value;
    }
}

/// <summary>Visitor for bounded snapshot prefix scans; the view must not escape the callback.</summary>
public delegate bool StorageEntryVisitor(in StorageEntryView entry);

/// <summary>Optimistic transaction conflict: the database changed after the batch began.</summary>
public sealed class StorageConflictException : InvalidOperationException
{
    public ulong ExpectedCommitSeq { get; }
    public ulong ActualCommitSeq { get; }

    public StorageConflictException(ulong expectedCommitSeq, ulong actualCommitSeq)
        : base($"Storage write conflict: expected base CommitSeq {expectedCommitSeq}, actual {actualCommitSeq}.")
    {
        ExpectedCommitSeq = expectedCommitSeq;
        ActualCommitSeq = actualCommitSeq;
    }
}
