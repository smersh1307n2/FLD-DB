using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using Fld1.Storage.Wal.Format;

namespace Fld1.Storage.Engine;

/// <summary>
/// Generic immutable blob store over a StorageDatabase. Blobs are opaque to Storage;
/// each blob is published only after all bounded pages are durable.
/// </summary>
public sealed class StorageBlobStore
{
    private const uint MetadataMagic = 0x31424C53; // "SLB1"
    private const ushort MetadataVersion = 1;
    private const int MetadataSize = 56;
    private const int BlobIdSize = 16;
    private const int MaxBlobPages = 65_535;
    private const int MaxPageSize = 4 * 1024 * 1024;
    private const int MaxPagesPerBatch = 8;
    private const int MaxConflictRetries = 16;

    private static readonly ConditionalWeakTable<StorageDatabase, ConcurrentDictionary<string, object>> Gates = new();

    private readonly StorageDatabase _database;
    private readonly byte[] _prefix;
    private readonly byte[] _metadataPrefix;
    private readonly byte[] _pagePrefix;
    private readonly object _gate;
    private readonly int _pageSize;
    private readonly int _pagesPerBatch;

    public int PageSize => _pageSize;

    public StorageBlobStore(
        StorageDatabase database,
        ReadOnlySpan<byte> keyPrefix,
        int pageSize = 1024 * 1024,
        int pagesPerBatch = 8)
    {
        _database = database ?? throw new ArgumentNullException(nameof(database));
        if (keyPrefix.IsEmpty || keyPrefix.Length > 64)
            throw new ArgumentOutOfRangeException(nameof(keyPrefix), "Blob key prefix must contain 1..64 bytes.");
        if (pageSize is < 4096 or > MaxPageSize)
            throw new ArgumentOutOfRangeException(nameof(pageSize), $"Blob page size must be 4 KiB..{MaxPageSize} bytes.");
        if (pagesPerBatch is < 1 or > MaxPagesPerBatch ||
            (long)pageSize * pagesPerBatch > 8L * 1024 * 1024)
            throw new ArgumentOutOfRangeException(nameof(pagesPerBatch), "Blob page batch must fit within the bounded 8 MiB write budget.");

        _prefix = keyPrefix.ToArray();
        _metadataPrefix = Append(_prefix, 0);
        _pagePrefix = Append(_prefix, 1);
        _pageSize = pageSize;
        _pagesPerBatch = pagesPerBatch;
        string gateKey = Convert.ToHexString(_prefix);
        var perDatabaseGates = Gates.GetValue(
            database, static _ => new ConcurrentDictionary<string, object>(StringComparer.Ordinal));
        _gate = perDatabaseGates.GetOrAdd(gateKey, static _ => new object());
    }

    /// <summary>
    /// Writes or resumes an immutable blob. Reusing an ID for different content is
    /// rejected. A pending metadata record makes interrupted page writes resumable
    /// and prevents orphan collection from deleting pages while Put is in progress.
    /// </summary>
    public void PutImmutableBlob(Guid blobId, ReadOnlyMemory<byte> content)
    {
        ValidateBlobId(blobId);
        int pageCount = GetPageCount(content.Length, _pageSize);
        byte[] digest = SHA256.HashData(content.Span);
        var metadata = new StorageBlobMetadata((ulong)content.Length, _pageSize, pageCount, digest, IsComplete: false);
        byte[] metadataKey = MetadataKey(blobId);

        lock (_gate)
        {
            bool alreadyComplete = false;
            ExecuteBatch(batch =>
            {
                if (batch.TryGet(metadataKey, out byte[]? existing))
                {
                    StorageBlobMetadata decoded = DecodeMetadata(existing!);
                    EnsureSameBlob(decoded, metadata);
                    alreadyComplete = decoded.IsComplete;
                }
                else
                {
                    batch.Put(metadataKey, EncodeMetadata(metadata));
                }
            });
            if (alreadyComplete) return;

            for (int firstPage = 0; firstPage < pageCount; firstPage += _pagesPerBatch)
            {
                int batchStart = firstPage;
                int batchEnd = Math.Min(pageCount, batchStart + _pagesPerBatch);
                ExecuteBatch(batch =>
                {
                    for (int pageIndex = batchStart; pageIndex < batchEnd; pageIndex++)
                    {
                        int offset = checked(pageIndex * _pageSize);
                        int length = Math.Min(_pageSize, content.Length - offset);
                        ReadOnlySpan<byte> page = content.Span.Slice(offset, length);
                        byte[] pageKey = PageKey(blobId, pageIndex);
                        if (batch.TryGet(pageKey, out byte[]? existing))
                        {
                            if (!existing!.AsSpan().SequenceEqual(page))
                                throw new InvalidDataException("Immutable Storage blob page conflicts with previously written bytes.");
                        }
                        else
                        {
                            batch.Put(pageKey, page);
                        }
                    }
                });
            }

            metadata = metadata with { IsComplete = true };
            ExecuteBatch(batch =>
            {
                if (!batch.TryGet(metadataKey, out byte[]? existing))
                    throw new InvalidDataException("Pending Storage blob metadata disappeared before publication.");
                StorageBlobMetadata decoded = DecodeMetadata(existing!);
                EnsureSameBlob(decoded, metadata);
                if (!decoded.IsComplete)
                    batch.Put(metadataKey, EncodeMetadata(metadata));
            });
        }
    }

    /// <summary>Opens a stable snapshot reader only for a fully published blob.</summary>
    public bool TryOpenRead(Guid blobId, out StorageBlobReader? reader)
    {
        ValidateBlobId(blobId);
        StorageSnapshot snapshot = _database.OpenSnapshot();
        try
        {
            if (!snapshot.TryGet(MetadataKey(blobId), out byte[]? bytes))
            {
                reader = null;
                snapshot.Dispose();
                return false;
            }
            StorageBlobMetadata metadata = DecodeMetadata(bytes!);
            EnsurePageSize(metadata);
            if (!metadata.IsComplete)
            {
                reader = null;
                snapshot.Dispose();
                return false;
            }
            reader = new StorageBlobReader(snapshot, _prefix, blobId, metadata);
            return true;
        }
        catch
        {
            snapshot.Dispose();
            throw;
        }
    }

    /// <summary>
    /// Atomically removes a blob's publication record and all its pages. Readers
    /// opened before this commit retain their immutable Storage snapshot.
    /// </summary>
    public bool DeleteBlob(Guid blobId)
    {
        ValidateBlobId(blobId);
        lock (_gate)
        {
            using StorageSnapshot snapshot = _database.OpenSnapshot();
            if (!snapshot.TryGet(MetadataKey(blobId), out byte[]? bytes))
                return false;
            StorageBlobMetadata metadata = DecodeMetadata(bytes!);
            EnsurePageSize(metadata);

            ExecuteBatch(batch =>
            {
                batch.Delete(MetadataKey(blobId));
                for (int pageIndex = 0; pageIndex < metadata.PageCount; pageIndex++)
                    batch.Delete(PageKey(blobId, pageIndex));
            });
            return true;
        }
    }

    /// <summary>
    /// Deletes at most maxPages page keys whose blob metadata is absent. Pending
    /// and complete blobs are protected. Call during exclusive startup/maintenance.
    /// </summary>
    public int PruneOrphanPages(int maxPages = 4096)
    {
        if (maxPages is < 1 or > 16_384) throw new ArgumentOutOfRangeException(nameof(maxPages));
        lock (_gate)
        {
            using StorageSnapshot snapshot = _database.OpenSnapshot();
            using StorageSnapshot.PrefixCursor cursor = snapshot.OpenPrefixCursor(_pagePrefix);
            var orphanKeys = new List<byte[]>(Math.Min(maxPages, 4096));
            while (orphanKeys.Count < maxPages && cursor.TryReadNext(out StorageEntryView entry))
            {
                if (entry.Key.Length != _pagePrefix.Length + BlobIdSize + sizeof(uint))
                    throw new InvalidDataException("Storage blob page key has an invalid length.");
                Guid blobId = new(entry.Key.Slice(_pagePrefix.Length, BlobIdSize));
                if (blobId == Guid.Empty)
                    throw new InvalidDataException("Storage blob page key contains an empty blob ID.");
                if (!snapshot.TryGet(MetadataKey(blobId), out _))
                    orphanKeys.Add(entry.Key.ToArray());
            }
            if (orphanKeys.Count == 0) return 0;
            ExecuteBatch(batch =>
            {
                foreach (byte[] key in orphanKeys)
                    batch.Delete(key);
            });
            return orphanKeys.Count;
        }
    }

    private void ExecuteBatch(Action<StorageWriteBatch> populate)
    {
        StorageConflictException? lastConflict = null;
        for (int attempt = 0; attempt < MaxConflictRetries; attempt++)
        {
            using StorageWriteBatch batch = _database.BeginWriteBatch();
            populate(batch);
            try
            {
                batch.Commit();
                return;
            }
            catch (StorageConflictException conflict)
            {
                lastConflict = conflict;
            }
        }
        throw lastConflict ?? new InvalidOperationException("Storage blob write retry limit exhausted.");
    }

    private byte[] MetadataKey(Guid blobId)
    {
        byte[] key = new byte[_metadataPrefix.Length + BlobIdSize];
        _metadataPrefix.CopyTo(key, 0);
        blobId.ToByteArray().CopyTo(key, _metadataPrefix.Length);
        return key;
    }

    private byte[] PageKey(Guid blobId, int pageIndex)
    {
        byte[] key = new byte[_pagePrefix.Length + BlobIdSize + sizeof(uint)];
        _pagePrefix.CopyTo(key, 0);
        blobId.ToByteArray().CopyTo(key, _pagePrefix.Length);
        BinaryPrimitives.WriteUInt32BigEndian(key.AsSpan(_pagePrefix.Length + BlobIdSize), checked((uint)pageIndex));
        return key;
    }

    private void EnsurePageSize(StorageBlobMetadata metadata)
    {
        if (metadata.PageSize != _pageSize)
            throw new InvalidDataException("Storage blob was opened with a different configured page size.");
    }

    private static void EnsureSameBlob(StorageBlobMetadata existing, StorageBlobMetadata requested)
    {
        if (existing.Length != requested.Length ||
            existing.PageSize != requested.PageSize ||
            existing.PageCount != requested.PageCount ||
            !CryptographicOperations.FixedTimeEquals(existing.Digest, requested.Digest))
            throw new InvalidOperationException("Immutable Storage blob ID was reused for different content or page geometry.");
    }

    private static int GetPageCount(int length, int pageSize)
    {
        long pages = (length + (long)pageSize - 1) / pageSize;
        if (pages > MaxBlobPages)
            throw new ArgumentOutOfRangeException(nameof(length), "Storage blob exceeds the maximum page count.");
        return (int)pages;
    }

    private static byte[] EncodeMetadata(StorageBlobMetadata metadata)
    {
        byte[] bytes = new byte[MetadataSize];
        BinaryPrimitives.WriteUInt32LittleEndian(bytes, MetadataMagic);
        BinaryPrimitives.WriteUInt16LittleEndian(bytes.AsSpan(4), MetadataVersion);
        BinaryPrimitives.WriteUInt16LittleEndian(bytes.AsSpan(6), metadata.IsComplete ? (ushort)1 : (ushort)0);
        BinaryPrimitives.WriteUInt64LittleEndian(bytes.AsSpan(8), metadata.Length);
        BinaryPrimitives.WriteInt32LittleEndian(bytes.AsSpan(16), metadata.PageSize);
        BinaryPrimitives.WriteInt32LittleEndian(bytes.AsSpan(20), metadata.PageCount);
        metadata.Digest.CopyTo(bytes, 24);
        return bytes;
    }

    private static StorageBlobMetadata DecodeMetadata(byte[] bytes)
    {
        if (bytes.Length != MetadataSize ||
            BinaryPrimitives.ReadUInt32LittleEndian(bytes) != MetadataMagic ||
            BinaryPrimitives.ReadUInt16LittleEndian(bytes.AsSpan(4)) != MetadataVersion)
            throw new InvalidDataException("Storage blob metadata magic/version/length is invalid.");
        ushort flags = BinaryPrimitives.ReadUInt16LittleEndian(bytes.AsSpan(6));
        ulong length = BinaryPrimitives.ReadUInt64LittleEndian(bytes.AsSpan(8));
        int pageSize = BinaryPrimitives.ReadInt32LittleEndian(bytes.AsSpan(16));
        int pageCount = BinaryPrimitives.ReadInt32LittleEndian(bytes.AsSpan(20));
        byte[] digest = bytes.AsSpan(24, 32).ToArray();
        if (flags > 1 || pageSize is < 4096 or > MaxPageSize ||
            pageCount is < 0 or > MaxBlobPages ||
            pageCount != (length == 0 ? 0 : checked((int)((length + (ulong)pageSize - 1) / (ulong)pageSize))))
            throw new InvalidDataException("Storage blob metadata fields are invalid.");
        return new StorageBlobMetadata(length, pageSize, pageCount, digest, flags == 1);
    }

    private static void ValidateBlobId(Guid blobId)
    {
        if (blobId == Guid.Empty) throw new ArgumentException("Blob ID must be non-zero.", nameof(blobId));
    }

    private static byte[] Append(byte[] prefix, byte kind)
    {
        byte[] key = new byte[prefix.Length + 1];
        prefix.CopyTo(key, 0);
        key[^1] = kind;
        return key;
    }

    internal sealed record StorageBlobMetadata(
        ulong Length, int PageSize, int PageCount, byte[] Digest, bool IsComplete);
}

/// <summary>Stable snapshot reader for an immutable Storage blob.</summary>
public sealed class StorageBlobReader : IDisposable
{
    private readonly byte[] _prefix;
    private readonly Guid _blobId;
    private readonly StorageBlobStore.StorageBlobMetadata _metadata;
    private StorageSnapshot? _snapshot;

    public Guid BlobId => _blobId;
    public ulong Length => _metadata.Length;
    public int PageSize => _metadata.PageSize;
    public int PageCount => _metadata.PageCount;
    public byte[] Digest => _metadata.Digest.ToArray();

    internal StorageBlobReader(
        StorageSnapshot snapshot,
        byte[] prefix,
        Guid blobId,
        StorageBlobStore.StorageBlobMetadata metadata)
    {
        _snapshot = snapshot;
        _prefix = prefix.ToArray();
        _blobId = blobId;
        _metadata = metadata;
    }

    public byte[] ReadPage(int pageIndex)
    {
        StorageSnapshot snapshot = GetSnapshot();
        if ((uint)pageIndex >= (uint)_metadata.PageCount)
            throw new ArgumentOutOfRangeException(nameof(pageIndex));
        byte[] key = PageKey(pageIndex);
        if (!snapshot.TryGet(key, out byte[]? page))
            throw new InvalidDataException($"Storage blob page {pageIndex} is missing.");
        int expectedLength = (int)Math.Min(
            (ulong)_metadata.PageSize,
            _metadata.Length - (ulong)pageIndex * (ulong)_metadata.PageSize);
        if (page!.Length != expectedLength)
            throw new InvalidDataException($"Storage blob page {pageIndex} has an invalid length.");
        return page;
    }

    public void CopyTo(Stream destination)
    {
        ArgumentNullException.ThrowIfNull(destination);
        using IncrementalHash hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        for (int pageIndex = 0; pageIndex < _metadata.PageCount; pageIndex++)
        {
            byte[] page = ReadPage(pageIndex);
            hash.AppendData(page);
            destination.Write(page);
        }
        byte[] actual = hash.GetHashAndReset();
        if (!CryptographicOperations.FixedTimeEquals(actual, _metadata.Digest))
            throw new InvalidDataException("Storage blob content digest mismatch.");
    }

    public byte[] ReadAllBytes()
    {
        if (_metadata.Length > int.MaxValue)
            throw new InvalidOperationException("Storage blob is too large to materialize as one byte array.");
        byte[] result = new byte[(int)_metadata.Length];
        using IncrementalHash hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        int offset = 0;
        for (int pageIndex = 0; pageIndex < _metadata.PageCount; pageIndex++)
        {
            byte[] page = ReadPage(pageIndex);
            page.CopyTo(result, offset);
            hash.AppendData(page);
            offset += page.Length;
        }
        if (offset != result.Length ||
            !CryptographicOperations.FixedTimeEquals(hash.GetHashAndReset(), _metadata.Digest))
            throw new InvalidDataException("Storage blob length or content digest mismatch.");
        return result;
    }

    private byte[] PageKey(int pageIndex)
    {
        byte[] key = new byte[_prefix.Length + 1 + 16 + sizeof(uint)];
        _prefix.CopyTo(key, 0);
        key[_prefix.Length] = 1;
        _blobId.ToByteArray().CopyTo(key, _prefix.Length + 1);
        BinaryPrimitives.WriteUInt32BigEndian(key.AsSpan(_prefix.Length + 17), checked((uint)pageIndex));
        return key;
    }

    private StorageSnapshot GetSnapshot()
        => _snapshot ?? throw new ObjectDisposedException(nameof(StorageBlobReader));

    public void Dispose()
        => Interlocked.Exchange(ref _snapshot, null)?.Dispose();
}

