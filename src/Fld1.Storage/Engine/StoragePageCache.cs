namespace Fld1.Storage.Engine;

internal readonly record struct StoragePageKey(ulong Generation, ulong PageId);

/// <summary>
/// A byte-budgeted LRU for immutable, validated fixed-size pages. Pinned entries are
/// never evicted; a miss fails rather than exceeding the configured memory budget.
/// </summary>
internal sealed class StoragePageCache
{
    private readonly object _gate = new();
    private readonly Dictionary<StoragePageKey, Entry> _entries = new();
    private readonly LinkedList<Entry> _lru = new();
    private readonly long _capacityBytes;
    private long _residentBytes;
    private long _evictionCount;

    public StoragePageCache(long capacityBytes)
    {
        if (capacityBytes < StoragePageCodec.PageSize)
            throw new ArgumentOutOfRangeException(nameof(capacityBytes), "Cache must hold at least one Storage page.");
        _capacityBytes = capacityBytes;
    }

    public long CapacityBytes => _capacityBytes;
    public long ResidentBytes { get { lock (_gate) return _residentBytes; } }
    public int Count { get { lock (_gate) return _entries.Count; } }
    public long EvictionCount { get { lock (_gate) return _evictionCount; } }

    public StoragePageCacheLease Acquire(StoragePageKey key, Func<byte[]> loadPage)
    {
        ArgumentNullException.ThrowIfNull(loadPage);
        return Acquire(key, _ => loadPage());
    }

    public StoragePageCacheLease Acquire(StoragePageKey key, Func<ulong, byte[]> loadPage)
    {
        ArgumentNullException.ThrowIfNull(loadPage);
        if (key.Generation == 0) throw new ArgumentOutOfRangeException(nameof(key));
        if (key.PageId == 0) throw new ArgumentOutOfRangeException(nameof(key));

        lock (_gate)
        {
            if (_entries.TryGetValue(key, out Entry? cached))
            {
                cached.PinCount++;
                Touch(cached);
                return new StoragePageCacheLease(this, cached);
            }

            // Loading under the cache lock prevents duplicate concurrent misses and
            // guarantees that the budget check and insertion are one atomic operation.
            byte[] page = loadPage(key.PageId) ?? throw new InvalidDataException("Storage page loader returned null.");
            if (page.Length != StoragePageCodec.PageSize)
                throw new InvalidDataException($"Storage page loader must return exactly {StoragePageCodec.PageSize} bytes.");

            MakeRoom(StoragePageCodec.PageSize);
            Entry entry = new(key, page);
            entry.PinCount = 1;
            entry.Node = _lru.AddFirst(entry);
            _entries.Add(key, entry);
            _residentBytes += page.Length;
            return new StoragePageCacheLease(this, entry);
        }
    }

    public void EvictGeneration(ulong generation)
    {
        if (generation == 0) throw new ArgumentOutOfRangeException(nameof(generation));
        lock (_gate)
        {
            Entry? current = _lru.Last?.Value;
            while (current is not null)
            {
                Entry? previous = current.Node!.Previous?.Value;
                if (current.Key.Generation == generation && current.PinCount == 0)
                    Remove(current);
                current = previous;
            }
        }
    }

    internal StoragePageView Decode(Entry entry, ulong generation, ulong pageId)
    {
        lock (_gate)
        {
            if (entry.PinCount <= 0)
                throw new ObjectDisposedException(nameof(StoragePageCacheLease));
            if (entry.Key.Generation != generation || entry.Key.PageId != pageId)
                throw new InvalidDataException("Storage page cache lease identity mismatch.");
            if (entry.DecodedPage.HasValue)
                return entry.DecodedPage.Value;

            StoragePageView decoded = StoragePageCodec.Decode(entry.Bytes, generation, pageId);
            entry.DecodedPage = decoded;
            return decoded;
        }
    }

    internal void Release(Entry entry)
    {
        lock (_gate)
        {
            if (entry.PinCount <= 0)
                throw new InvalidOperationException("Storage page cache pin count underflow.");
            entry.PinCount--;
        }
    }

    private void MakeRoom(int requiredBytes)
    {
        while (_residentBytes > _capacityBytes - requiredBytes)
        {
            LinkedListNode<Entry>? node = _lru.Last;
            while (node is not null && node.Value.PinCount != 0)
                node = node.Previous;
            if (node is null)
                throw new InvalidOperationException("Storage page cache budget is exhausted by pinned pages.");
            Remove(node.Value);
            _evictionCount++;
        }
    }

    private void Touch(Entry entry)
    {
        if (entry.Node is null) throw new InvalidOperationException("Storage cache entry is detached.");
        _lru.Remove(entry.Node);
        _lru.AddFirst(entry.Node);
    }

    private void Remove(Entry entry)
    {
        if (entry.PinCount != 0)
            throw new InvalidOperationException("Cannot evict a pinned Storage page.");
        _entries.Remove(entry.Key);
        _lru.Remove(entry.Node!);
        entry.Node = null;
        _residentBytes -= entry.Bytes.Length;
    }

    internal sealed class Entry(StoragePageKey key, byte[] bytes)
    {
        public StoragePageKey Key { get; } = key;
        public byte[] Bytes { get; } = bytes;
        public int PinCount { get; set; }
        public StoragePageView? DecodedPage { get; set; }
        public LinkedListNode<Entry>? Node { get; set; }
    }
}

internal sealed class StoragePageCacheLease : IDisposable
{
    private StoragePageCache? _owner;
    private readonly StoragePageCache.Entry _entry;

    internal StoragePageCacheLease(StoragePageCache owner, StoragePageCache.Entry entry)
    {
        _owner = owner;
        _entry = entry;
    }

    /// <summary>The page memory is borrowed and must not outlive this lease.</summary>
    public ReadOnlyMemory<byte> Page
    {
        get
        {
            ObjectDisposedException.ThrowIf(_owner is null, this);
            return _entry.Bytes;
        }
    }

    /// <summary>Returns a CRC-validated page view cached for this immutable cache entry.</summary>
    public StoragePageView Decode(ulong generation, ulong pageId)
    {
        StoragePageCache owner = _owner ?? throw new ObjectDisposedException(nameof(StoragePageCacheLease));
        return owner.Decode(_entry, generation, pageId);
    }

    public void Dispose()
    {
        StoragePageCache? owner = Interlocked.Exchange(ref _owner, null);
        owner?.Release(_entry);
    }
}
