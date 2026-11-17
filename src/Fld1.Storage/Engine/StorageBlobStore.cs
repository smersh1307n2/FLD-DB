using System.Collections.Concurrent;
using System.IO.MemoryMappedFiles;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using Fld1.Storage.Wal.Format;

namespace Fld1.Storage.Engine;

/// <summary>
/// Generic immutable page/blob store. StorageDatabase owns the small blob catalog
/// and its WAL; opaque page bytes are stored outside the in-memory KV index. Page
/// contents are flushed before atomic publication. Directory-entry fsync is applied
/// where StorageDirectorySync supports it; Windows directory-entry durability is not
/// claimed by this implementation.
/// </summary>
public sealed class StorageBlobStore
{
    private const uint MetadataMagic = 0x31424C53; // "SLB1"
    private const ushort MetadataVersion = 1;
    private const int MetadataSize = 56;
    private const int BlobIdSize = 16;
    private const int MaxBlobPages = 65_535;
    private const int MaxPageSize = 4 * 1024 * 1024;
    private const int MaxConflictRetries = 16;

    private static readonly ConditionalWeakTable<StorageDatabase, ConcurrentDictionary<string, SharedState>> SharedStates = new();

    private readonly StorageDatabase _database;
    private readonly byte[] _prefix;
    private readonly byte[] _metadataPrefix;
    private readonly string _blobDirectory;
    private readonly SharedState _shared;
    private readonly int _pageSize;

    /// <summary>Preferred page size for newly created blobs; existing blobs retain their persisted geometry.</summary>
    public int PageSize => _pageSize;

    public StorageBlobStore(
        StorageDatabase database,
        ReadOnlySpan<byte> keyPrefix,
        int pageSize = 1024 * 1024)
    {
        _database = database ?? throw new ArgumentNullException(nameof(database));
        if (keyPrefix.IsEmpty || keyPrefix.Length > 64)
            throw new ArgumentOutOfRangeException(nameof(keyPrefix), "Blob key prefix must contain 1..64 bytes.");
        if (pageSize is < 4096 or > MaxPageSize)
            throw new ArgumentOutOfRangeException(nameof(pageSize), $"Blob page size must be 4 KiB..{MaxPageSize} bytes.");

        _prefix = keyPrefix.ToArray();
        _metadataPrefix = Append(_prefix, 0);
        _pageSize = pageSize;
        _blobDirectory = Path.Combine(database.DirectoryPath, "blobs", Convert.ToHexString(_prefix));
        var perDatabase = SharedStates.GetValue(
            database, static _ => new ConcurrentDictionary<string, SharedState>(StringComparer.Ordinal));
        _shared = perDatabase.GetOrAdd(Convert.ToHexString(_prefix), static _ => new SharedState());
    }

    /// <summary>
    /// Writes or resumes an immutable blob. New blobs use one immutable contiguous
    /// content file so consumers can map them without a second Run mirror. A pending
    /// catalog row is committed first; the file is flushed and atomically published;
    /// the complete marker is committed last. Existing page-based blobs remain readable.
    /// </summary>
    public void PutImmutableBlob(Guid blobId, ReadOnlyMemory<byte> content)
    {
        ValidateBlobId(blobId);
        byte[] digest = SHA256.HashData(content.Span);
        byte[] metadataKey = MetadataKey(blobId);

        lock (_shared.Gate)
        {
            StorageBlobMetadata metadata = default!;
            bool alreadyComplete = false;
            ExecuteBatch(batch =>
            {
                alreadyComplete = false;
                if (batch.TryGet(metadataKey, out byte[]? existing))
                {
                    StorageBlobMetadata decoded = DecodeMetadata(existing!);
                    EnsureSameBlobContent(decoded, (ulong)content.Length, digest);
                    metadata = decoded; // Resume using the original durable page geometry.
                    alreadyComplete = decoded.IsComplete;
                }
                else
                {
                    // A catalog-less directory can survive an interrupted delete. Treat
                    // it as an occupied immutable ID until orphan maintenance removes it;
                    // otherwise a root-global Run allocator could reuse the ID and collide.
                    if (Directory.Exists(BlobDirectory(blobId)))
                        throw new InvalidDataException(
                            "Storage blob page directory exists without catalog metadata; orphan cleanup is required before ID reuse.");
                    int pageCount = GetPageCount(content.Length, _pageSize);
                    metadata = new StorageBlobMetadata(
                        (ulong)content.Length, _pageSize, pageCount, digest,
                        IsComplete: false, IsSingleFile: true);
                    batch.Put(metadataKey, EncodeMetadata(metadata));
                }
            });
            if (alreadyComplete)
            {
                if (metadata.IsSingleFile)
                    VerifyExistingSingleFile(blobId, content);
                else
                    VerifyExistingPages(blobId, content, metadata.PageSize);
                return;
            }

            string directory = BlobDirectory(blobId);
            StorageDirectorySync.CreateDirectoryDurable(directory);
            if (metadata.IsSingleFile)
            {
                WriteSingleFileAtomically(Path.Combine(directory, "content.blob"), content.Span);
            }
            else
            {
                int pageCount = metadata.PageCount;
                int pageSize = metadata.PageSize;
                for (int pageIndex = 0; pageIndex < pageCount; pageIndex++)
                {
                    int offset = checked(pageIndex * pageSize);
                    int length = Math.Min(pageSize, content.Length - offset);
                    WritePageAtomically(
                        Path.Combine(directory, PageFileName(pageIndex)),
                        content.Span.Slice(offset, length));
                }
            }
            StorageDirectorySync.Flush(directory);

            metadata = metadata with { IsComplete = true };
            ExecuteBatch(batch =>
            {
                if (!batch.TryGet(metadataKey, out byte[]? existing))
                    throw new InvalidDataException("Pending Storage blob metadata disappeared before publication.");
                StorageBlobMetadata decoded = DecodeMetadata(existing!);
                EnsureSameBlobContent(decoded, metadata.Length, metadata.Digest);
                if (!decoded.IsComplete)
                    batch.Put(metadataKey, EncodeMetadata(metadata));
            });
        }
    }

    /// <summary>
    /// Returns true when either catalog metadata or a page directory occupies this ID.
    /// Catalog-less directories are crash-left delete artifacts and still block ID reuse
    /// until orphan maintenance removes them.
    /// </summary>
    public bool ContainsBlob(Guid blobId)
    {
        ValidateBlobId(blobId);
        lock (_shared.Gate)
        {
            using StorageSnapshot snapshot = _database.OpenSnapshot();
            if (snapshot.TryGet(MetadataKey(blobId), out byte[]? bytes))
            {
                _ = DecodeMetadata(bytes!);
                return true;
            }
            return Directory.Exists(BlobDirectory(blobId));
        }
    }

    /// <summary>Opens a stable snapshot reader only for a fully published blob.</summary>
    public bool TryOpenRead(Guid blobId, out StorageBlobReader? reader)
    {
        ValidateBlobId(blobId);
        lock (_shared.Gate)
        {
            StorageSnapshot snapshot = _database.OpenSnapshot();
            bool pinAdded = false;
            try
            {
                if (!snapshot.TryGet(MetadataKey(blobId), out byte[]? bytes))
                {
                    reader = null;
                    snapshot.Dispose();
                    return false;
                }
                StorageBlobMetadata metadata = DecodeMetadata(bytes!);
                if (!metadata.IsComplete)
                {
                    reader = null;
                    snapshot.Dispose();
                    return false;
                }
                string directory = BlobDirectory(blobId);
                if (!Directory.Exists(directory))
                    throw new InvalidDataException("Published Storage blob directory is missing.");
                if (metadata.IsSingleFile)
                {
                    string contentPath = Path.Combine(directory, "content.blob");
                    if (!File.Exists(contentPath) || (ulong)new FileInfo(contentPath).Length != metadata.Length)
                        throw new InvalidDataException("Published Storage blob content file is missing or has an invalid length.");
                }
                _shared.Pins.TryGetValue(blobId, out int pins);
                _shared.Pins[blobId] = checked(pins + 1);
                pinAdded = true;
                reader = new StorageBlobReader(
                    snapshot, directory, blobId, metadata, ReleasePin);
                return true;
            }
            catch
            {
                snapshot.Dispose();
                if (pinAdded) ReleasePin(blobId);
                throw;
            }
        }
    }

    /// <summary>
    /// Removes the catalog row first, then its page directory. Existing readers pin
    /// their page files until disposal; if file deletion is interrupted, startup
    /// orphan collection removes the now-unreferenced directory.
    /// </summary>
    public bool DeleteBlob(Guid blobId)
    {
        ValidateBlobId(blobId);
        lock (_shared.Gate)
        {
            if (_shared.Pins.TryGetValue(blobId, out int pins) && pins != 0)
                return false;
            using StorageSnapshot snapshot = _database.OpenSnapshot();
            if (!snapshot.TryGet(MetadataKey(blobId), out byte[]? bytes))
                return false;
            _ = DecodeMetadata(bytes!);

            ExecuteBatch(batch => batch.Delete(MetadataKey(blobId)));
            try
            {
                string directory = BlobDirectory(blobId);
                if (Directory.Exists(directory))
                {
                    Directory.Delete(directory, recursive: true);
                    StorageDirectorySync.Flush(Path.GetDirectoryName(directory)!);
                }
            }
            catch
            {
                // The catalog row is already gone. Startup orphan collection retries.
            }
            return true;
        }
    }

    /// <summary>
    /// Deletes at most maxBlobs catalog generations not present in the caller's
    /// complete live set. Pending generations are included, allowing interrupted
    /// unreferenced puts to be reclaimed during exclusive startup maintenance.
    /// </summary>
    public int PruneUnreferencedBlobs(IReadOnlySet<Guid> liveBlobIds, int maxBlobs = 128)
    {
        ArgumentNullException.ThrowIfNull(liveBlobIds);
        if (maxBlobs is < 1 or > 4096) throw new ArgumentOutOfRangeException(nameof(maxBlobs));
        lock (_shared.Gate)
        {
            var candidates = new List<Guid>(Math.Min(maxBlobs, 128));
            using (StorageSnapshot snapshot = _database.OpenSnapshot())
            using (StorageSnapshot.PrefixCursor cursor = snapshot.OpenPrefixCursor(_metadataPrefix))
            {
                while (candidates.Count < maxBlobs && cursor.TryReadNext(out StorageEntryView entry))
                {
                    if (entry.Key.Length != _metadataPrefix.Length + BlobIdSize)
                        throw new InvalidDataException("Storage blob metadata key has an invalid length.");
                    Guid blobId = new(entry.Key.Slice(_metadataPrefix.Length, BlobIdSize));
                    if (blobId == Guid.Empty)
                        throw new InvalidDataException("Storage blob metadata key contains an empty blob ID.");
                    _ = DecodeMetadata(entry.Value.ToArray());
                    if (!liveBlobIds.Contains(blobId) &&
                        (!_shared.Pins.TryGetValue(blobId, out int pins) || pins == 0))
                        candidates.Add(blobId);
                }
            }
            int deleted = 0;
            foreach (Guid blobId in candidates)
                if (DeleteBlob(blobId)) deleted++;
            return deleted;
        }
    }

    /// <summary>
    /// Deletes at most maxBlobDirectories page directories that have no catalog row.
    /// Pending and complete blobs are protected. Call during exclusive startup or
    /// maintenance, after the authoritative liveness set has been reconstructed.
    /// </summary>
    public int PruneOrphanBlobs(int maxBlobDirectories = 4096)
    {
        if (maxBlobDirectories is < 1 or > 16_384)
            throw new ArgumentOutOfRangeException(nameof(maxBlobDirectories));
        lock (_shared.Gate)
        {
            if (!Directory.Exists(_blobDirectory)) return 0;
            using StorageSnapshot snapshot = _database.OpenSnapshot();
            int deleted = 0;
            foreach (string directory in Directory.EnumerateDirectories(_blobDirectory))
            {
                if (deleted == maxBlobDirectories) break;
                string name = Path.GetFileName(directory);
                if (!Guid.TryParseExact(name, "N", out Guid blobId) || blobId == Guid.Empty)
                    throw new InvalidDataException("Storage blob directory name is invalid.");
                if (snapshot.TryGet(MetadataKey(blobId), out _))
                    continue;
                if (_shared.Pins.TryGetValue(blobId, out int pins) && pins != 0)
                    continue;
                try
                {
                    Directory.Delete(directory, recursive: true);
                    deleted++;
                }
                catch
                {
                    // Retry on the next maintenance pass.
                }
            }
            if (deleted != 0) StorageDirectorySync.Flush(_blobDirectory);
            return deleted;
        }
    }

    private void VerifyExistingSingleFile(Guid blobId, ReadOnlyMemory<byte> content)
    {
        string path = Path.Combine(BlobDirectory(blobId), "content.blob");
        byte[] existing;
        try { existing = File.ReadAllBytes(path); }
        catch (FileNotFoundException ex)
        {
            throw new InvalidDataException("Published Storage blob content file is missing.", ex);
        }
        if (!existing.AsSpan().SequenceEqual(content.Span))
            throw new InvalidDataException("Published Storage blob content differs from its immutable bytes.");
    }

    /// <summary>
    /// Migrates an existing page-based immutable blob to one contiguous file. The
    /// catalog switches only after the new file is flushed and its whole-blob digest
    /// is verified. Old page files are removed after the catalog switch; a crash before
    /// that cleanup is harmless and a retry converges.
    /// </summary>
    public bool MigrateToSingleFile(Guid blobId)
    {
        ValidateBlobId(blobId);
        lock (_shared.Gate)
        {
            using StorageSnapshot snapshot = _database.OpenSnapshot();
            if (!snapshot.TryGet(MetadataKey(blobId), out byte[]? bytes))
                return false;
            StorageBlobMetadata metadata = DecodeMetadata(bytes!);
            if (!metadata.IsComplete)
                throw new InvalidDataException("Cannot migrate an incomplete Storage blob.");
            if (metadata.IsSingleFile)
                return true;
            if (_shared.Pins.TryGetValue(blobId, out int pins) && pins != 0)
                return false;

            string directory = BlobDirectory(blobId);
            string destination = Path.Combine(directory, "content.blob");
            string temporary = destination + ".tmp";
            using IncrementalHash hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
            byte[] buffer = System.Buffers.ArrayPool<byte>.Shared.Rent(64 * 1024);
            try
            {
                using (var output = new FileStream(
                    temporary, FileMode.Create, FileAccess.Write, FileShare.None,
                    bufferSize: 64 * 1024, FileOptions.WriteThrough))
                {
                    ulong copied = 0;
                    for (int pageIndex = 0; pageIndex < metadata.PageCount; pageIndex++)
                    {
                        string pagePath = Path.Combine(directory, PageFileName(pageIndex));
                        int expectedLength = (int)Math.Min(
                            (ulong)metadata.PageSize,
                            metadata.Length - (ulong)pageIndex * (ulong)metadata.PageSize);
                        using var page = new FileStream(
                            pagePath, FileMode.Open, FileAccess.Read, FileShare.Read,
                            bufferSize: 4096, FileOptions.SequentialScan);
                        int remaining = expectedLength;
                        while (remaining > 0)
                        {
                            int read = page.Read(buffer, 0, Math.Min(buffer.Length, remaining));
                            if (read == 0)
                                throw new InvalidDataException($"Storage blob page {pageIndex} is truncated.");
                            hash.AppendData(buffer.AsSpan(0, read));
                            output.Write(buffer, 0, read);
                            copied = checked(copied + (uint)read);
                            remaining -= read;
                        }
                        if (page.ReadByte() != -1)
                            throw new InvalidDataException($"Storage blob page {pageIndex} has trailing bytes.");
                    }
                    if (copied != metadata.Length ||
                        !CryptographicOperations.FixedTimeEquals(hash.GetHashAndReset(), metadata.Digest))
                        throw new InvalidDataException("Storage blob content digest mismatch during migration.");
                    output.Flush(flushToDisk: true);
                }

                StorageDirectorySync.AtomicReplaceFile(temporary, destination, flushParent: false);
                StorageDirectorySync.Flush(directory);
                StorageBlobMetadata migrated = metadata with { IsSingleFile = true };
                ExecuteBatch(batch =>
                {
                    if (!batch.TryGet(MetadataKey(blobId), out byte[]? current))
                        throw new InvalidDataException("Storage blob catalog row disappeared during migration.");
                    StorageBlobMetadata decoded = DecodeMetadata(current!);
                    EnsureSameBlobContent(decoded, metadata.Length, metadata.Digest);
                    if (!decoded.IsComplete)
                        throw new InvalidDataException("Storage blob became incomplete during migration.");
                    if (!decoded.IsSingleFile)
                        batch.Put(MetadataKey(blobId), EncodeMetadata(migrated));
                });

                for (int pageIndex = 0; pageIndex < metadata.PageCount; pageIndex++)
                {
                    try { File.Delete(Path.Combine(directory, PageFileName(pageIndex))); }
                    catch { /* the single-file catalog is authoritative; orphan pages are harmless */ }
                }
                StorageDirectorySync.Flush(directory);
                return true;
            }
            finally
            {
                System.Buffers.ArrayPool<byte>.Shared.Return(buffer);
                try { File.Delete(temporary); } catch { }
            }
        }
    }

    private void VerifyExistingPages(Guid blobId, ReadOnlyMemory<byte> content, int pageSize)
    {
        string directory = BlobDirectory(blobId);
        if (!Directory.Exists(directory))
            throw new InvalidDataException("Published Storage blob page directory is missing.");
        for (int pageIndex = 0; pageIndex < GetPageCount(content.Length, pageSize); pageIndex++)
        {
            int offset = checked(pageIndex * pageSize);
            int length = Math.Min(pageSize, content.Length - offset);
            byte[] existing;
            try { existing = File.ReadAllBytes(Path.Combine(directory, PageFileName(pageIndex))); }
            catch (FileNotFoundException ex)
            {
                throw new InvalidDataException("Published Storage blob page is missing.", ex);
            }
            if (!existing.AsSpan().SequenceEqual(content.Span.Slice(offset, length)))
                throw new InvalidDataException("Published Storage blob page differs from its immutable content.");
        }
    }

    private void ReleasePin(Guid blobId)
    {
        lock (_shared.Gate)
        {
            if (!_shared.Pins.TryGetValue(blobId, out int pins) || pins <= 0)
                throw new InvalidOperationException("Storage blob reader pin underflow.");
            if (pins == 1) _shared.Pins.Remove(blobId);
            else _shared.Pins[blobId] = pins - 1;
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
        throw lastConflict ?? new InvalidOperationException("Storage blob catalog retry limit exhausted.");
    }

    private string BlobDirectory(Guid blobId)
        => Path.Combine(_blobDirectory, blobId.ToString("N"));

    private static string PageFileName(int pageIndex)
        => pageIndex.ToString("D8", System.Globalization.CultureInfo.InvariantCulture) + ".page";

    private byte[] MetadataKey(Guid blobId)
    {
        byte[] key = new byte[_metadataPrefix.Length + BlobIdSize];
        _metadataPrefix.CopyTo(key, 0);
        blobId.ToByteArray().CopyTo(key, _metadataPrefix.Length);
        return key;
    }

    private static void EnsureSameBlobContent(
        StorageBlobMetadata existing, ulong requestedLength, ReadOnlySpan<byte> requestedDigest)
    {
        if (existing.Length != requestedLength ||
            !CryptographicOperations.FixedTimeEquals(existing.Digest, requestedDigest))
            throw new InvalidOperationException("Immutable Storage blob ID was reused for different content.");
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
        System.Buffers.Binary.BinaryPrimitives.WriteUInt32LittleEndian(bytes, MetadataMagic);
        System.Buffers.Binary.BinaryPrimitives.WriteUInt16LittleEndian(bytes.AsSpan(4), MetadataVersion);
        ushort flags = (ushort)((metadata.IsComplete ? 1 : 0) | (metadata.IsSingleFile ? 2 : 0));
        System.Buffers.Binary.BinaryPrimitives.WriteUInt16LittleEndian(bytes.AsSpan(6), flags);
        System.Buffers.Binary.BinaryPrimitives.WriteUInt64LittleEndian(bytes.AsSpan(8), metadata.Length);
        System.Buffers.Binary.BinaryPrimitives.WriteInt32LittleEndian(bytes.AsSpan(16), metadata.PageSize);
        System.Buffers.Binary.BinaryPrimitives.WriteInt32LittleEndian(bytes.AsSpan(20), metadata.PageCount);
        metadata.Digest.CopyTo(bytes, 24);
        return bytes;
    }

    private static StorageBlobMetadata DecodeMetadata(byte[] bytes)
    {
        if (bytes.Length != MetadataSize ||
            System.Buffers.Binary.BinaryPrimitives.ReadUInt32LittleEndian(bytes) != MetadataMagic ||
            System.Buffers.Binary.BinaryPrimitives.ReadUInt16LittleEndian(bytes.AsSpan(4)) != MetadataVersion)
            throw new InvalidDataException("Storage blob metadata magic/version/length is invalid.");
        ushort flags = System.Buffers.Binary.BinaryPrimitives.ReadUInt16LittleEndian(bytes.AsSpan(6));
        ulong length = System.Buffers.Binary.BinaryPrimitives.ReadUInt64LittleEndian(bytes.AsSpan(8));
        int pageSize = System.Buffers.Binary.BinaryPrimitives.ReadInt32LittleEndian(bytes.AsSpan(16));
        int pageCount = System.Buffers.Binary.BinaryPrimitives.ReadInt32LittleEndian(bytes.AsSpan(20));
        byte[] digest = bytes.AsSpan(24, 32).ToArray();
        if (flags > 3 || pageSize is < 4096 or > MaxPageSize ||
            pageCount is < 0 or > MaxBlobPages)
            throw new InvalidDataException("Storage blob metadata fields are invalid.");
        ulong pageSize64 = (ulong)pageSize;
        ulong expectedPageCount = length / pageSize64 +
            (length % pageSize64 == 0 ? 0UL : 1UL);
        if (expectedPageCount > MaxBlobPages || pageCount != (int)expectedPageCount)
            throw new InvalidDataException("Storage blob metadata length/page count are inconsistent.");
        return new StorageBlobMetadata(
            length, pageSize, pageCount, digest,
            IsComplete: (flags & 1) != 0,
            IsSingleFile: (flags & 2) != 0);
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

    private static void WriteSingleFileAtomically(string path, ReadOnlySpan<byte> content)
    {
        string temporary = path + ".tmp";
        using (var stream = new FileStream(
            temporary, FileMode.Create, FileAccess.Write, FileShare.None,
            bufferSize: 64 * 1024, FileOptions.WriteThrough))
        {
            stream.Write(content);
            stream.Flush(flushToDisk: true);
        }
        StorageDirectorySync.AtomicReplaceFile(temporary, path, flushParent: false);
    }

    private static void WritePageAtomically(string path, ReadOnlySpan<byte> page)
    {
        if (File.Exists(path))
        {
            if (!File.ReadAllBytes(path).AsSpan().SequenceEqual(page))
                throw new InvalidDataException("Immutable Storage blob page conflicts with previously written bytes.");
            return;
        }
        string temporary = path + ".tmp";
        using (var stream = new FileStream(
            temporary, FileMode.Create, FileAccess.Write, FileShare.None,
            bufferSize: 64 * 1024, FileOptions.WriteThrough))
        {
            stream.Write(page);
            stream.Flush(flushToDisk: true);
        }
        StorageDirectorySync.AtomicReplaceFile(temporary, path, flushParent: false);
    }

    private sealed class SharedState
    {
        public object Gate { get; } = new();
        public Dictionary<Guid, int> Pins { get; } = new();
    }

    internal sealed record StorageBlobMetadata(
        ulong Length, int PageSize, int PageCount, byte[] Digest,
        bool IsComplete, bool IsSingleFile = false);
}

/// <summary>Stable snapshot reader for an immutable Storage blob.</summary>
public sealed class StorageBlobReader : IDisposable
{
    private readonly string _directory;
    private readonly Guid _blobId;
    private readonly StorageBlobStore.StorageBlobMetadata _metadata;
    private readonly Action<Guid> _releasePin;
    private readonly object _gate = new();
    private StorageSnapshot? _snapshot;
    private bool _mappedViewTransferred;

    /// <summary>Gets the immutable blob identifier.</summary>
    public Guid BlobId => _blobId;
    /// <summary>Gets the exact byte length.</summary>
    public ulong Length => _metadata.Length;
    /// <summary>Gets the persisted page size for this blob.</summary>
    public int PageSize => _metadata.PageSize;
    /// <summary>Gets the persisted page count.</summary>
    public int PageCount => _metadata.PageCount;
    /// <summary>Gets a copy of the expected whole-blob SHA-256 digest.</summary>
    public byte[] Digest => _metadata.Digest.ToArray();
    /// <summary>True when the blob is stored in one contiguous file and can be memory-mapped.</summary>
    public bool IsSingleFile => _metadata.IsSingleFile;

    /// <summary>
    /// Opens a demand-paged read-only mapping and transfers this reader's snapshot pin
    /// to the returned view. Dispose the view to release both the mapping and blob pin.
    /// </summary>
    public StorageBlobMappedView OpenMappedView()
    {
        lock (_gate)
        {
            _ = GetSnapshot();
            if (!_metadata.IsSingleFile)
                throw new InvalidOperationException("Page-based Storage blobs must be migrated before memory mapping.");
            if (_mappedViewTransferred)
                throw new InvalidOperationException("Storage blob reader ownership was already transferred.");
            var view = new StorageBlobMappedView(
                this, Path.Combine(_directory, "content.blob"), _metadata.Length, _metadata.Digest);
            _mappedViewTransferred = true;
            return view;
        }
    }

    internal void ReleaseFromMappedView()
    {
        lock (_gate)
        {
            if (!_mappedViewTransferred)
                return;
            _mappedViewTransferred = false;
            DisposeCore();
        }
    }

    internal StorageBlobReader(
        StorageSnapshot snapshot,
        string directory,
        Guid blobId,
        StorageBlobStore.StorageBlobMetadata metadata,
        Action<Guid> releasePin)
    {
        _snapshot = snapshot;
        _directory = directory;
        _blobId = blobId;
        _metadata = metadata;
        _releasePin = releasePin;
    }

    /// <summary>Reads one page and validates its exact expected length; use CopyTo or ReadAllBytes for whole-blob digest verification.</summary>
    public byte[] ReadPage(int pageIndex)
    {
        lock (_gate)
        {
            _ = GetSnapshot();
            if ((uint)pageIndex >= (uint)_metadata.PageCount)
                throw new ArgumentOutOfRangeException(nameof(pageIndex));
            int expectedLength = (int)Math.Min(
                (ulong)_metadata.PageSize,
                _metadata.Length - (ulong)pageIndex * (ulong)_metadata.PageSize);
            if (_metadata.IsSingleFile)
            {
                string contentPath = Path.Combine(_directory, "content.blob");
                using var content = new FileStream(
                    contentPath, FileMode.Open, FileAccess.Read, FileShare.Read,
                    bufferSize: 4096, FileOptions.RandomAccess);
                if ((ulong)content.Length != _metadata.Length)
                    throw new InvalidDataException("Storage blob content file has an invalid length.");
                content.Position = checked((long)pageIndex * _metadata.PageSize);
                byte[] result = new byte[expectedLength];
                content.ReadExactly(result);
                return result;
            }

            string path = Path.Combine(
                _directory, pageIndex.ToString("D8", System.Globalization.CultureInfo.InvariantCulture) + ".page");
            byte[] page = File.ReadAllBytes(path);
            if (page.Length != expectedLength)
                throw new InvalidDataException($"Storage blob page {pageIndex} has an invalid length.");
            return page;
        }
    }

    /// <summary>
    /// Streams all pages through a fixed-size pooled buffer and verifies the total
    /// length and whole-blob SHA-256 before returning. The destination may contain
    /// partial bytes if validation or I/O fails.
    /// </summary>
    public void CopyTo(Stream destination)
    {
        ArgumentNullException.ThrowIfNull(destination);
        lock (_gate)
        {
            _ = GetSnapshot();
            using IncrementalHash hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
            byte[] buffer = System.Buffers.ArrayPool<byte>.Shared.Rent(64 * 1024);
            try
            {
                ulong copied = 0;
                if (_metadata.IsSingleFile)
                {
                    string contentPath = Path.Combine(_directory, "content.blob");
                    using var content = new FileStream(
                        contentPath, FileMode.Open, FileAccess.Read, FileShare.Read,
                        bufferSize: 64 * 1024, FileOptions.SequentialScan);
                    if ((ulong)content.Length != _metadata.Length)
                        throw new InvalidDataException("Storage blob content file has an invalid length.");
                    int read;
                    while ((read = content.Read(buffer, 0, buffer.Length)) > 0)
                    {
                        hash.AppendData(buffer.AsSpan(0, read));
                        destination.Write(buffer, 0, read);
                        copied = checked(copied + (uint)read);
                    }
                }
                else
                {
                    for (int pageIndex = 0; pageIndex < _metadata.PageCount; pageIndex++)
                    {
                        string path = Path.Combine(
                            _directory, pageIndex.ToString("D8", System.Globalization.CultureInfo.InvariantCulture) + ".page");
                        int expectedLength = (int)Math.Min(
                            (ulong)_metadata.PageSize,
                            _metadata.Length - (ulong)pageIndex * (ulong)_metadata.PageSize);
                        using var page = new FileStream(
                            path, FileMode.Open, FileAccess.Read, FileShare.Read,
                            bufferSize: 4096, FileOptions.SequentialScan);
                        int remaining = expectedLength;
                        while (remaining > 0)
                        {
                            int read = page.Read(buffer, 0, Math.Min(buffer.Length, remaining));
                            if (read == 0)
                                throw new InvalidDataException($"Storage blob page {pageIndex} is truncated.");
                            hash.AppendData(buffer.AsSpan(0, read));
                            destination.Write(buffer, 0, read);
                            copied = checked(copied + (uint)read);
                            remaining -= read;
                        }
                        if (page.ReadByte() != -1)
                            throw new InvalidDataException($"Storage blob page {pageIndex} has trailing bytes.");
                    }
                }

                if (copied != _metadata.Length ||
                    !CryptographicOperations.FixedTimeEquals(hash.GetHashAndReset(), _metadata.Digest))
                    throw new InvalidDataException("Storage blob content digest mismatch.");
            }
            finally
            {
                System.Buffers.ArrayPool<byte>.Shared.Return(buffer);
            }
        }
    }

    /// <summary>Materializes the blob and verifies its total length and whole-blob SHA-256.</summary>
    public byte[] ReadAllBytes()
    {
        lock (_gate)
        {
            _ = GetSnapshot();
            if (_metadata.Length > int.MaxValue)
                throw new InvalidOperationException("Storage blob is too large to materialize as one byte array.");
            byte[] result = new byte[(int)_metadata.Length];
            using var output = new MemoryStream(result, writable: true);
            CopyTo(output);
            if (output.Position != result.Length)
                throw new InvalidDataException("Storage blob length mismatch.");
            return result;
        }
    }

    private StorageSnapshot GetSnapshot()
        => _snapshot ?? throw new ObjectDisposedException(nameof(StorageBlobReader));

    public void Dispose()
    {
        lock (_gate)
        {
            if (_mappedViewTransferred)
                return; // The mapped view owns this reader and its durable blob pin.
            DisposeCore();
        }
    }

    private void DisposeCore()
    {
        StorageSnapshot? snapshot = _snapshot;
        if (snapshot is null) return;
        _snapshot = null;
        try { snapshot.Dispose(); }
        finally { _releasePin(_blobId); }
    }
}

public sealed unsafe class StorageBlobMappedView : MemoryManager<byte>
{
    private readonly StorageBlobReader _owner;
    private readonly MemoryMappedFile _mapping;
    private readonly MemoryMappedViewAccessor _view;
    private byte* _pointer;
    private readonly int _length;
    private bool _disposed;

    internal StorageBlobMappedView(
        StorageBlobReader owner, string path, ulong length, ReadOnlySpan<byte> expectedDigest)
    {
        _owner = owner;
        if (length == 0 || length > int.MaxValue)
            throw new InvalidDataException("Storage blob length is outside the supported mapped-memory range.");
        var stream = new FileStream(path, FileMode.Open, FileAccess.Read,
            FileShare.ReadWrite | FileShare.Delete, bufferSize: 1, FileOptions.RandomAccess);
        try
        {
            if ((ulong)stream.Length != length)
                throw new InvalidDataException("Storage blob content file length does not match its catalog.");
            _mapping = MemoryMappedFile.CreateFromFile(
                stream, mapName: null, capacity: 0, MemoryMappedFileAccess.Read,
                HandleInheritability.None, leaveOpen: false);
            try
            {
                _view = _mapping.CreateViewAccessor(0, stream.Length, MemoryMappedFileAccess.Read);
                try
                {
                    byte* pointer = null;
                    _view.SafeMemoryMappedViewHandle.AcquirePointer(ref pointer);
                    _pointer = pointer + _view.PointerOffset;
                    _length = checked((int)length);
                    byte[] actualDigest = SHA256.HashData(new ReadOnlySpan<byte>(_pointer, _length));
                    if (!CryptographicOperations.FixedTimeEquals(actualDigest, expectedDigest))
                        throw new InvalidDataException("Storage blob mapped content digest mismatch.");
                }
                catch
                {
                    if (_pointer != null)
                    {
                        _view.SafeMemoryMappedViewHandle.ReleasePointer();
                        _pointer = null;
                    }
                    _view.Dispose();
                    throw;
                }
            }
            catch
            {
                _mapping.Dispose();
                throw;
            }
        }
        catch
        {
            stream.Dispose();
            throw;
        }
    }

    public ReadOnlyMemory<byte> ReadOnlyMemory
    {
        get
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            return CreateMemory(_length);
        }
    }

    public override Span<byte> GetSpan()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        return new Span<byte>(_pointer, _length);
    }

    public override MemoryHandle Pin(int elementIndex = 0)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if ((uint)elementIndex > (uint)_length)
            throw new ArgumentOutOfRangeException(nameof(elementIndex));
        return new MemoryHandle(_pointer + elementIndex, default, this);
    }

    public override void Unpin() { }

    public void Close() => ((IDisposable)this).Dispose();

    protected override void Dispose(bool disposing)
    {
        if (_disposed) return;
        _disposed = true;
        if (_pointer != null)
        {
            _view.SafeMemoryMappedViewHandle.ReleasePointer();
            _pointer = null;
        }
        _view.Dispose();
        _mapping.Dispose();
        _owner.ReleaseFromMappedView();
    }
}
