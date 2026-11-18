namespace Fld1.Storage.Engine;

/// <summary>
/// Append-only page and data files for one immutable B+ tree generation. The manifest,
/// not physical file length, defines the readable committed prefix.
/// </summary>
internal sealed class StorageBTreeGenerationFiles : IDisposable
{
    private readonly object _gate = new();
    private readonly IStorageRandomAccessFile _pages;
    private readonly IStorageRandomAccessFile _data;
    private readonly ulong _partitionId;
    private readonly ulong _generation;
    private bool _disposed;

    public StorageBTreeGenerationFiles(string databaseDirectory, ulong partitionId, ulong generation)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(databaseDirectory);
        if (generation == 0) throw new ArgumentOutOfRangeException(nameof(generation));

        _partitionId = partitionId;
        _generation = generation;
        string snapshots = Path.Combine(databaseDirectory, "snapshots");
        StorageDirectorySync.CreateDirectoryDurable(snapshots);
        string prefix = $"p_{partitionId}.{generation}";
        string pagesPath = Path.Combine(snapshots, prefix + ".pages");
        string dataPath = Path.Combine(snapshots, prefix + ".data");
        EnsureDurableFileExists(pagesPath);
        _pages = new StorageRandomAccessFile(
            pagesPath,
            FileMode.Open,
            FileAccess.ReadWrite,
            FileShare.Read);
        IStorageRandomAccessFile? data = null;
        try
        {
            EnsureDurableFileExists(dataPath);
            data = new StorageRandomAccessFile(
                dataPath,
                FileMode.Open,
                FileAccess.ReadWrite,
                FileShare.Read);
            // Persist newly created directory entries where the platform supports
            // directory fsync. Windows namespace durability remains separately gated.
            StorageDirectorySync.Flush(snapshots);
        }
        catch
        {
            data?.Dispose();
            _pages.Dispose();
            throw;
        }
        _data = data ?? throw new InvalidOperationException("B+ tree data file was not opened.");

        // A crash may leave a partial trailing page. Keep the file open so recovery
        // can truncate it to the validated manifest high-water mark before appending.
    }

    public ulong PartitionId => _partitionId;
    public ulong Generation => _generation;

    public ulong PageFileLength
    {
        get { lock (_gate) { EnsureUsable(); return checked((ulong)_pages.Length); } }
    }

    public ulong DataFileLength
    {
        get { lock (_gate) { EnsureUsable(); return checked((ulong)_data.Length); } }
    }

    /// <summary>Appends opaque key/value bytes and returns their generation-local offset.</summary>
    public ulong AppendData(ReadOnlySpan<byte> bytes)
    {
        lock (_gate)
        {
            EnsureUsable();
            long offset = _data.Length;
            _data.WriteExactly(offset, bytes);
            return checked((ulong)offset);
        }
    }

    /// <summary>Appends the next sequential page ID. Existing page IDs are never overwritten.</summary>
    public void AppendPage(ulong pageId, byte[] encodedPage)
    {
        ArgumentNullException.ThrowIfNull(encodedPage);
        lock (_gate)
        {
            EnsureUsable();
            long length = _pages.Length;
            if (length % StoragePageCodec.PageSize != 0)
                throw new InvalidDataException("B+ tree page file length is not page aligned.");
            ulong expectedPageId = checked((ulong)(length / StoragePageCodec.PageSize) + 1);
            if (pageId != expectedPageId)
                throw new InvalidOperationException($"B+ tree page append expected ID {expectedPageId}, received {pageId}.");
            if (encodedPage.Length != StoragePageCodec.PageSize)
                throw new InvalidDataException("B+ tree page must have the exact fixed page size.");
            _ = StoragePageCodec.Decode(encodedPage, _generation, pageId);
            _pages.WriteExactly(length, encodedPage);
        }
    }

    /// <summary>Reads one page only from the manifest-committed page prefix.</summary>
    public byte[] ReadPage(ulong pageId, ulong committedPageFileLength)
    {
        lock (_gate)
        {
            EnsureUsable();
            ValidateCommittedPageLength(committedPageFileLength);
            if (pageId == 0)
                throw new InvalidDataException("B+ tree page ID zero is reserved.");
            ulong offset = checked((pageId - 1) * (ulong)StoragePageCodec.PageSize);
            if (checked(offset + (ulong)StoragePageCodec.PageSize) > committedPageFileLength)
                throw new InvalidDataException("B+ tree page ID is outside the committed manifest range.");
            byte[] page = new byte[StoragePageCodec.PageSize];
            _pages.ReadExactly(checked((long)offset), page);
            _ = StoragePageCodec.Decode(page, _generation, pageId);
            return page;
        }
    }

    /// <summary>Reads a key/value slice only when its complete range is manifest-committed.</summary>
    public byte[] ReadData(ulong offset, int length, ulong committedDataFileLength)
    {
        if (length < 0) throw new ArgumentOutOfRangeException(nameof(length));
        lock (_gate)
        {
            EnsureUsable();
            long physicalLength = _data.Length;
            if (committedDataFileLength > (ulong)physicalLength)
                throw new InvalidDataException("Manifest data-file length exceeds the physical file length.");
            ulong end = checked(offset + (ulong)length);
            if (end > committedDataFileLength || offset > long.MaxValue)
                throw new InvalidDataException("B+ tree data reference is outside the committed manifest range.");
            byte[] result = new byte[length];
            if (length != 0)
                _data.ReadExactly(checked((long)offset), result);
            return result;
        }
    }

    /// <summary>Reads a committed data slice into caller-owned scratch memory without allocating.</summary>
    public void ReadDataInto(
        ulong offset, int length, ulong committedDataFileLength, Span<byte> destination)
    {
        if (length < 0) throw new ArgumentOutOfRangeException(nameof(length));
        if (destination.Length < length)
            throw new ArgumentException("Destination buffer is smaller than the requested data slice.", nameof(destination));
        lock (_gate)
        {
            EnsureUsable();
            long physicalLength = _data.Length;
            if (committedDataFileLength > (ulong)physicalLength)
                throw new InvalidDataException("Manifest data-file length exceeds the physical file length.");
            ulong end = checked(offset + (ulong)length);
            if (end > committedDataFileLength || offset > long.MaxValue)
                throw new InvalidDataException("B+ tree data reference is outside the committed manifest range.");
            if (length != 0)
                _data.ReadExactly(checked((long)offset), destination[..length]);
        }
    }

    public void FlushToDisk(
        Action? beforeDataFlush = null,
        Action? afterDataFlush = null,
        Action? beforePageFlush = null,
        Action? afterPageFlush = null)
    {
        lock (_gate)
        {
            EnsureUsable();
            beforeDataFlush?.Invoke();
            _data.FlushToDisk();
            afterDataFlush?.Invoke();
            beforePageFlush?.Invoke();
            _pages.FlushToDisk();
            afterPageFlush?.Invoke();
        }
    }

    /// <summary>
    /// Drops only bytes beyond the validated manifest high-water marks. Call after WAL
    /// continuity/replay validation; never use physical file length as commit evidence.
    /// </summary>
    public void TruncateToCommittedLengths(ulong committedPageFileLength, ulong committedDataFileLength)
    {
        lock (_gate)
        {
            EnsureUsable();
            ValidateCommittedPageLength(committedPageFileLength);
            if (committedPageFileLength > (ulong)_pages.Length ||
                committedDataFileLength > (ulong)_data.Length ||
                committedPageFileLength > long.MaxValue ||
                committedDataFileLength > long.MaxValue)
                throw new InvalidDataException("Manifest committed file length exceeds the physical generation file.");

            _pages.SetLength(checked((long)committedPageFileLength));
            _data.SetLength(checked((long)committedDataFileLength));
            _data.FlushToDisk();
            _pages.FlushToDisk();
        }
    }

    private static void EnsureDurableFileExists(string path)
    {
        if (File.Exists(path))
            return;

        string temporaryPath = path + ".tmp";
        if (File.Exists(temporaryPath))
            File.Delete(temporaryPath);

        using (var stream = new FileStream(
            temporaryPath, FileMode.CreateNew, FileAccess.Write, FileShare.None,
            bufferSize: 4096, FileOptions.WriteThrough))
        {
            stream.Flush(flushToDisk: true);
        }

        // Unix fsyncs the parent directory; Windows uses the existing NTFS
        // write-through handle-rename primitive. A manifest can never reference
        // a generation file whose namespace entry has not passed this publication.
        StorageDirectorySync.AtomicCreateFile(temporaryPath, path);
    }

    private void ValidateCommittedPageLength(ulong committedPageFileLength)
    {
        if (committedPageFileLength % StoragePageCodec.PageSize != 0 ||
            committedPageFileLength > (ulong)_pages.Length)
            throw new InvalidDataException("Manifest committed page-file length is invalid.");
    }

    private void EnsureUsable()
        => ObjectDisposedException.ThrowIf(_disposed, this);

    public void Dispose()
    {
        lock (_gate)
        {
            if (_disposed) return;
            _disposed = true;
            _pages.Dispose();
            _data.Dispose();
        }
    }
}
