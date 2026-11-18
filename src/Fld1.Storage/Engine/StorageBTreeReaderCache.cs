namespace Fld1.Storage.Engine;

/// <summary>
/// Shared, bounded lookup and page-validation caches for every reader/snapshot owned
/// by one StorageDatabase. Hot lookup identity includes the immutable root so readers
/// at different commit sequences never observe one another's values.
/// </summary>
internal sealed class StorageBTreeReaderCache
{
    private const int HotLookupCapacity = 16_384;
    private const int ValidationCacheCapacity = 4_096;
    private const int TreeKeyCacheCapacity = 4_096;
    private const int MaxCachedKeyLength = 256;
    private readonly object _gate = new();
    private readonly Dictionary<HotLookupKey, HotLookupEntry> _hot = new();
    private readonly Queue<HotLookupKey> _hotOrder = new();
    private readonly HashSet<ValidatedPageKey> _validated = new();
    private readonly Queue<ValidatedPageKey> _validatedOrder = new();
    private readonly Dictionary<TreeKeyCacheKey, byte[]> _treeKeys = new();
    private readonly Queue<TreeKeyCacheKey> _treeKeyOrder = new();

    public int HotLookupCount { get { lock (_gate) return _hot.Count; } }
    public int ValidatedPageCount { get { lock (_gate) return _validated.Count; } }
    public int TreeKeyCount { get { lock (_gate) return _treeKeys.Count; } }

    public bool TryGetHot(
        ulong generation, ulong root, ReadOnlySpan<byte> key,
        out bool found, out StorageTreeValueReference value)
    {
        var cacheKey = new HotLookupKey(generation, root, HashKey(key));
        lock (_gate)
        {
            if (_hot.TryGetValue(cacheKey, out HotLookupEntry entry) &&
                entry.Key.AsSpan().SequenceEqual(key))
            {
                found = entry.Found;
                value = entry.Value;
                return true;
            }
        }

        found = false;
        value = default;
        return false;
    }

    public void RememberHot(
        ulong generation, ulong root, ReadOnlySpan<byte> key,
        bool found, StorageTreeValueReference value)
    {
        var cacheKey = new HotLookupKey(generation, root, HashKey(key));
        lock (_gate)
        {
            // Hash collisions only reduce cache coverage; they never affect correctness.
            if (_hot.ContainsKey(cacheKey))
                return;

            while (_hot.Count >= HotLookupCapacity)
            {
                HotLookupKey oldest = _hotOrder.Dequeue();
                _hot.Remove(oldest);
            }

            _hot.Add(cacheKey, new HotLookupEntry(key.ToArray(), found, value));
            _hotOrder.Enqueue(cacheKey);
        }
    }

    public int GetHotLookupCount(ulong generation, ulong root)
    {
        lock (_gate)
            return _hot.Keys.Count(key => key.Generation == generation && key.Root == root);
    }

    public bool TryGetTreeKey(ulong generation, StorageTreeKeyReference reference, out byte[] key)
    {
        if (reference.Length > MaxCachedKeyLength)
        {
            key = Array.Empty<byte>();
            return false;
        }

        lock (_gate)
        {
            if (_treeKeys.TryGetValue(
                    new TreeKeyCacheKey(generation, reference.Offset, reference.Length), out byte[]? cached))
            {
                key = cached;
                return true;
            }
        }

        key = Array.Empty<byte>();
        return false;
    }

    public void RememberTreeKey(ulong generation, StorageTreeKeyReference reference, byte[] key)
    {
        if (reference.Length > MaxCachedKeyLength || key.Length != reference.Length)
            return;

        var cacheKey = new TreeKeyCacheKey(generation, reference.Offset, reference.Length);
        lock (_gate)
        {
            if (_treeKeys.ContainsKey(cacheKey))
                return;
            while (_treeKeys.Count >= TreeKeyCacheCapacity)
            {
                TreeKeyCacheKey oldest = _treeKeyOrder.Dequeue();
                _treeKeys.Remove(oldest);
            }
            _treeKeys.Add(cacheKey, key);
            _treeKeyOrder.Enqueue(cacheKey);
        }
    }

    public bool IsValidated(ulong generation, ulong pageId)
    {
        lock (_gate)
            return _validated.Contains(new ValidatedPageKey(generation, pageId));
    }

    public void MarkValidated(ulong generation, ulong pageId)
    {
        var key = new ValidatedPageKey(generation, pageId);
        lock (_gate)
        {
            if (!_validated.Add(key))
                return;

            _validatedOrder.Enqueue(key);
            while (_validated.Count > ValidationCacheCapacity)
            {
                ValidatedPageKey oldest = _validatedOrder.Dequeue();
                _validated.Remove(oldest);
            }
        }
    }

    public void EvictGeneration(ulong generation)
    {
        lock (_gate)
        {
            foreach (HotLookupKey key in _hot.Keys.Where(key => key.Generation == generation).ToArray())
                _hot.Remove(key);
            foreach (ValidatedPageKey key in _validated.Where(key => key.Generation == generation).ToArray())
                _validated.Remove(key);
            foreach (TreeKeyCacheKey key in _treeKeys.Keys.Where(key => key.Generation == generation).ToArray())
                _treeKeys.Remove(key);

            // Remove stale FIFO nodes as well; otherwise repeated generation retirement
            // and reuse could grow the queues without bound or evict a newer duplicate key.
            RebuildQueue(_hotOrder, key => _hot.ContainsKey(key));
            RebuildQueue(_validatedOrder, key => _validated.Contains(key));
            RebuildQueue(_treeKeyOrder, key => _treeKeys.ContainsKey(key));
        }
    }

    private static void RebuildQueue<T>(Queue<T> queue, Func<T, bool> keep)
    {
        T[] retained = queue.Where(keep).ToArray();
        queue.Clear();
        foreach (T item in retained)
            queue.Enqueue(item);
    }

    private static ulong HashKey(ReadOnlySpan<byte> key)
    {
        const ulong offset = 14695981039346656037UL;
        const ulong prime = 1099511628211UL;
        ulong hash = offset;
        foreach (byte item in key)
        {
            hash ^= item;
            hash *= prime;
        }
        return hash ^ checked((ulong)key.Length);
    }

    private readonly record struct HotLookupKey(ulong Generation, ulong Root, ulong Hash);
    private readonly record struct ValidatedPageKey(ulong Generation, ulong PageId);
    private readonly record struct TreeKeyCacheKey(ulong Generation, ulong Offset, int Length);
    private readonly record struct HotLookupEntry(byte[] Key, bool Found, StorageTreeValueReference Value);
}
