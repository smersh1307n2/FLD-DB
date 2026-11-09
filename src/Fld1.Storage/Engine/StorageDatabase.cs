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
    private readonly Dictionary<StorageKey, byte[]> _values = new();
    private SortedList<StorageKey, byte[]>? _snapshotIndex;
    private readonly WalManager _wal;
    private readonly string _directory;
    private readonly Action<StorageFaultPoint>? _faultInjector;
    private ulong _snapshotGeneration;
    private ulong _snapshotCommitSeq;
    private bool _disposed;
    private bool _faulted;
    private ulong _lastCommitSeq;

    public ulong PartitionId => _wal.PartitionId;
    public ulong LastCommitSeq { get { lock (_gate) { EnsureUsable(); return _lastCommitSeq; } } }
    public int Count { get { lock (_gate) { EnsureUsable(); return _values.Count; } } }

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
        StorageDirectorySync.CreateDirectoryDurable(directory);
        string walDirectory = Path.Combine(directory, "wal");
        // Acquire the partition's exclusive writer fence before reading the snapshot
        // or WAL. Otherwise a second process could race recovery against a checkpoint.
        var (wal, records) = faultInjector is null
            ? WalManager.Open(walDirectory, partitionId, walRotateAfterBytes)
            : WalManager.OpenForTesting(walDirectory, partitionId, walRotateAfterBytes, faultInjector);
        var database = new StorageDatabase(directory, wal, faultInjector);
        try
        {
            var snapshot = StorageSnapshotFile.Load(directory, partitionId);
            if (snapshot is not null)
            {
                foreach (var pair in snapshot.Values)
                    database._values.Add(pair.Key, pair.Value);
                database._snapshotGeneration = snapshot.Generation;
                database._snapshotCommitSeq = snapshot.CommitSeq;
                database._lastCommitSeq = snapshot.CommitSeq;
            }
            else if (records.Count == 0 && wal.NextCommitSeq != 1)
            {
                throw new InvalidDataException("Storage WAL starts after sequence 1 but no snapshot manifest exists.");
            }

            foreach (var record in records)
            {
                if (record.CommitSeq <= database._lastCommitSeq)
                    continue;
                if (record.Kind != BatchCommitKind)
                    throw new InvalidDataException($"Unknown Storage WAL record kind {record.Kind}.");
                database.ApplyRecovered(record.CommitSeq, record.Payload);
            }

            if (wal.NextCommitSeq != checked(database._lastCommitSeq + 1))
                throw new InvalidDataException("Storage snapshot and WAL commit sequence do not form a contiguous recovery chain.");

            return database;
        }
        catch
        {
            wal.Dispose();
            throw;
        }
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
            if (_values.TryGetValue(new StorageKey(key), out var stored))
            {
                value = stored.ToArray();
                return true;
            }

            value = null;
            return false;
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

            try
            {
                _wal.Flush();
                _wal.Rotate();
                ulong generation = checked(_snapshotGeneration + 1);
                StorageSnapshotFile.Store(_directory, PartitionId, generation, _lastCommitSeq,
                    new SortedDictionary<StorageKey, byte[]>(_values, StorageKeyComparer.Instance), _faultInjector);
                _snapshotGeneration = generation;
                _snapshotCommitSeq = _lastCommitSeq;
                _wal.TruncateSealedSegments(_snapshotCommitSeq);
            }
            catch
            {
                // A failed checkpoint may have published an orphan snapshot or a new
                // WAL segment. Reopen is the only authority for the manifest/WAL pair.
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
            // Cache the immutable sorted index for this commit epoch. Repeated
            // snapshots share it in O(1); a successful commit invalidates the cache.
            // Stored value arrays are immutable after publication, so old snapshots
            // safely retain their prior index while later commits replace live values.
            _snapshotIndex ??= new SortedList<StorageKey, byte[]>(
                _values, StorageKeyComparer.Instance);
            return new StorageSnapshot(_snapshotIndex, _lastCommitSeq);
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

            byte[] payload = EncodeBatch(mutations);
            try
            {
                ulong commitSeq = _wal.Append(BatchCommitKind, payload);
                _faultInjector?.Invoke(StorageFaultPoint.AfterWalAppendBeforeFlush);
                _wal.Flush();
                _faultInjector?.Invoke(StorageFaultPoint.AfterWalFlushBeforePublish);

                foreach (var mutation in mutations)
                {
                    var key = new StorageKey(mutation.Key);
                    if (mutation.IsDelete) _values.Remove(key);
                    else _values[key] = mutation.Value.ToArray();
                }

                _lastCommitSeq = commitSeq;
                _snapshotIndex = null;
                return commitSeq;
            }
            catch
            {
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
        foreach (var mutation in mutations)
        {
            var key = new StorageKey(mutation.Key);
            if (mutation.IsDelete) _values.Remove(key);
            else _values[key] = mutation.Value;
        }

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
            _wal.Dispose();
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
    private SortedList<StorageDatabase.StorageKey, byte[]>? _values;
    public ulong CommitSeq { get; }

    internal StorageSnapshot(SortedList<StorageDatabase.StorageKey, byte[]> values, ulong commitSeq)
    {
        _values = values;
        CommitSeq = commitSeq;
    }

    public bool TryGet(ReadOnlySpan<byte> key, out byte[]? value)
    {
        var values = GetValues();
        if (values.TryGetValue(new StorageDatabase.StorageKey(key), out var stored))
        {
            value = stored.ToArray();
            return true;
        }

        value = null;
        return false;
    }

    public IReadOnlyList<StorageEntry> ScanPrefix(ReadOnlySpan<byte> prefix, ReadOnlySpan<byte> afterKey, int limit)
    {
        using PrefixCursor cursor = OpenPrefixCursor(prefix, afterKey);
        return cursor.ReadPage(limit);
    }

    /// <summary>Creates a forward-only, bounded-memory cursor over one sorted key prefix.</summary>
    public PrefixCursor OpenPrefixCursor(ReadOnlySpan<byte> prefix, ReadOnlySpan<byte> afterKey = default)
    {
        return new PrefixCursor(this, prefix.ToArray(), afterKey.ToArray());
    }

    public sealed class PrefixCursor : IDisposable
    {
        private readonly StorageSnapshot _owner;
        private readonly byte[] _prefix;
        private readonly byte[] _afterKey;
        private int _index;
        private bool _finished;
        private bool _disposed;

        internal PrefixCursor(StorageSnapshot owner, byte[] prefix, byte[] afterKey)
        {
            _owner = owner;
            _prefix = prefix;
            _afterKey = afterKey;
            SortedList<StorageDatabase.StorageKey, byte[]> values = owner.GetValues();
            ReadOnlySpan<byte> startKey = afterKey.Length != 0 &&
                afterKey.AsSpan().SequenceCompareTo(prefix) > 0
                    ? afterKey
                    : prefix;
            _index = LowerBound(values, startKey);
        }

        private static int LowerBound(
            SortedList<StorageDatabase.StorageKey, byte[]> values, ReadOnlySpan<byte> key)
        {
            int low = 0;
            int high = values.Count;
            while (low < high)
            {
                int middle = low + ((high - low) >> 1);
                if (values.Keys[middle].Span.SequenceCompareTo(key) < 0)
                    low = middle + 1;
                else
                    high = middle;
            }
            return low;
        }

        public IReadOnlyList<StorageEntry> ReadPage(int limit)
        {
            if (limit <= 0) throw new ArgumentOutOfRangeException(nameof(limit));
            SortedList<StorageDatabase.StorageKey, byte[]> values = _owner.GetValues();
            if (_finished) return Array.Empty<StorageEntry>();

            var result = new List<StorageEntry>(Math.Min(limit, Math.Max(0, values.Count - _index)));
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
            SortedList<StorageDatabase.StorageKey, byte[]> values = _owner.GetValues();
            if (_finished) return 0;

            int visited = 0;
            while (visited < limit && TryReadNext(out StorageEntryView entry))
            {
                visited++;
                if (!visitor(in entry))
                    break;
            }
            return visited;
        }

        /// <summary>Read one borrowed entry without allocating a page or copying key/value bytes.</summary>
        public bool TryReadNext(out StorageEntryView entry)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            SortedList<StorageDatabase.StorageKey, byte[]> values = _owner.GetValues();
            if (_finished)
            {
                entry = default;
                return false;
            }

            while (_index < values.Count)
            {
                StorageDatabase.StorageKey key = values.Keys[_index];
                byte[] value = values.Values[_index];
                _index++;
                ReadOnlySpan<byte> keyBytes = key.Span;
                int prefixOrder = CompareToPrefix(keyBytes, _prefix);
                if (prefixOrder < 0) continue;
                if (prefixOrder > 0)
                {
                    _finished = true;
                    break;
                }
                if (_afterKey.Length != 0 && keyBytes.SequenceCompareTo(_afterKey) <= 0)
                    continue;

                entry = new StorageEntryView(keyBytes, value);
                return true;
            }

            _finished = true;
            entry = default;
            return false;
        }

        public void Dispose()
        {
            _disposed = true;
            _finished = true;
        }

        private static int CompareToPrefix(ReadOnlySpan<byte> key, ReadOnlySpan<byte> prefix)
        {
            int common = Math.Min(key.Length, prefix.Length);
            int order = key[..common].SequenceCompareTo(prefix[..common]);
            if (order != 0) return order;
            return key.Length < prefix.Length ? -1 : 0;
        }
    }

    private SortedList<StorageDatabase.StorageKey, byte[]> GetValues()
        => _values ?? throw new ObjectDisposedException(nameof(StorageSnapshot));

    public void Dispose() => _values = null;
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
