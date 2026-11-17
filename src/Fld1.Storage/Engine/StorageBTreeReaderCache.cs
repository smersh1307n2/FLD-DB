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
    private readonly object _gate = new();
    private readonly Dictionary<HotLookupKey, HotLookupEntry> _hot = new();
    private readonly Queue<HotLookupKey> _hotOrder = new();
    private readonly HashSet<ValidatedPageKey> _validated = new();
    private readonly Queue<ValidatedPageKey> _validatedOrder = new();

    public int HotLookupCount { get { lock (_gate) return _hot.Count; } }
    public int ValidatedPageCount { get { lock (_gate) return _validated.Count; } }

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
        }
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
    private readonly record struct HotLookupEntry(byte[] Key, bool Found, StorageTreeValueReference Value);
}
