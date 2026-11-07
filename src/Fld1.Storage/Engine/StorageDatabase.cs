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
    private readonly WalManager _wal;
    private bool _disposed;
    private bool _faulted;
    private ulong _lastCommitSeq;

    public ulong PartitionId => _wal.PartitionId;
    public ulong LastCommitSeq { get { lock (_gate) { EnsureUsable(); return _lastCommitSeq; } } }
    public int Count { get { lock (_gate) { EnsureUsable(); return _values.Count; } } }

    private StorageDatabase(WalManager wal)
    {
        _wal = wal;
    }

    public static StorageDatabase Open(string directory, ulong partitionId, long walRotateAfterBytes = 64L * 1024 * 1024)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(directory);
        if (walRotateAfterBytes < 0) throw new ArgumentOutOfRangeException(nameof(walRotateAfterBytes));
        Directory.CreateDirectory(directory);
        string walDirectory = Path.Combine(directory, "wal");
        var (wal, records) = WalManager.Open(walDirectory, partitionId, walRotateAfterBytes);
        var database = new StorageDatabase(wal);
        try
        {
            foreach (var record in records)
            {
                if (record.Kind != BatchCommitKind)
                    throw new InvalidDataException($"Unknown Storage WAL record kind {record.Kind}.");
                database.ApplyRecovered(record.CommitSeq, record.Payload);
            }

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
        lock (_gate) { EnsureUsable(); return new StorageWriteBatch(this); }
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

    public StorageSnapshot OpenSnapshot()
    {
        lock (_gate)
        {
            EnsureUsable();
            var copy = new SortedDictionary<StorageKey, byte[]>(StorageKeyComparer.Instance);
            foreach (var pair in _values)
                copy.Add(pair.Key, pair.Value.ToArray());
            return new StorageSnapshot(copy, _lastCommitSeq);
        }
    }

    internal ulong Commit(IReadOnlyCollection<StorageMutation> mutations)
    {
        lock (_gate)
        {
            EnsureUsable();
            if (mutations.Count == 0) return _lastCommitSeq;
            if (mutations.Count > MaxBatchOperations)
                throw new ArgumentOutOfRangeException(nameof(mutations), "Storage batch operation limit exceeded.");

            byte[] payload = EncodeBatch(mutations);
            try
            {
                ulong commitSeq = _wal.Append(BatchCommitKind, payload);
                _wal.Flush();

                foreach (var mutation in mutations)
                {
                    var key = new StorageKey(mutation.Key);
                    if (mutation.IsDelete) _values.Remove(key);
                    else _values[key] = mutation.Value.ToArray();
                }

                _lastCommitSeq = commitSeq;
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

        foreach (var mutation in mutations.OrderBy(x => new StorageKey(x.Key), StorageKeyComparer.Instance))
        {
            if (mutation.Key.Length is 0 or > MaxKeyLength)
                throw new ArgumentOutOfRangeException(nameof(mutations), "Storage key length is invalid.");
            if (!mutation.IsDelete && mutation.Value.Length > WalConstants.MaxPayloadLen)
                throw new ArgumentOutOfRangeException(nameof(mutations), "Storage value is too large.");

            stream.WriteByte(mutation.IsDelete ? (byte)2 : (byte)1);
            Span<byte> lengths = stackalloc byte[8];
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

/// <summary>One atomic set of opaque key/value mutations.</summary>
public sealed class StorageWriteBatch
{
    private readonly StorageDatabase _database;
    private readonly Dictionary<StorageDatabase.StorageKey, StorageDatabase.StorageMutation> _mutations = new();
    private bool _completed;

    internal StorageWriteBatch(StorageDatabase database) => _database = database;

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
        return _database.Commit(_mutations.Values.ToArray());
    }

    private void EnsureOpen()
    {
        ObjectDisposedException.ThrowIf(_completed, this);
    }
}

/// <summary>Immutable, stable point-in-time view of a Storage partition.</summary>
public sealed class StorageSnapshot : IDisposable
{
    private SortedDictionary<StorageDatabase.StorageKey, byte[]>? _values;
    public ulong CommitSeq { get; }

    internal StorageSnapshot(SortedDictionary<StorageDatabase.StorageKey, byte[]> values, ulong commitSeq)
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
        if (limit <= 0) throw new ArgumentOutOfRangeException(nameof(limit));
        var values = GetValues();
        var result = new List<StorageEntry>(Math.Min(limit, values.Count));
        foreach (var pair in values)
        {
            if (!pair.Key.Span.StartsWith(prefix)) continue;
            if (!afterKey.IsEmpty && pair.Key.Span.SequenceCompareTo(afterKey) <= 0) continue;
            result.Add(new StorageEntry(pair.Key.Span.ToArray(), pair.Value.ToArray()));
            if (result.Count == limit) break;
        }

        return result;
    }

    private SortedDictionary<StorageDatabase.StorageKey, byte[]> GetValues()
        => _values ?? throw new ObjectDisposedException(nameof(StorageSnapshot));

    public void Dispose() => _values = null;
}

public sealed record StorageEntry(byte[] Key, byte[] Value);