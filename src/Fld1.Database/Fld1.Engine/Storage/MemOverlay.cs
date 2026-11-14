using Fld1.Core.Util;

namespace Fld1.Engine.Storage;

/// <summary>
/// Session/engine L0-style overlay. LWW on put. Zero extra alloc on get miss path.
/// </summary>
public sealed class MemOverlay
{
    private readonly Dictionary<MemFieldKey, MemFieldValue> _map = new();
    private readonly object _lock = new();

    public int Count
    {
        get { lock (_lock) return _map.Count; }
    }

    public void Put(MemFieldKey key, MemFieldValue value)
    {
        lock (_lock)
        {
            if (_map.TryGetValue(key, out var cur) && Compare(value, cur) <= 0)
                return;
            _map[key] = value;
        }
    }

    public bool TryGet(MemFieldKey key, out MemFieldValue? value)
    {
        lock (_lock)
            return _map.TryGetValue(key, out value);
    }

    /// <summary>
    /// Replace the already-resolved LWW winner after a durable identity-stamp record.
    /// The caller must hold the database commit guard and provide the current winner;
    /// this intentionally bypasses HLC comparison because identity stamping does not
    /// represent a new field mutation.
    /// </summary>
    internal void SetResolved(MemFieldKey key, MemFieldValue value)
    {
        ArgumentNullException.ThrowIfNull(value);
        lock (_lock)
            _map[key] = value;
    }

    /// <summary>Snapshot for checkpoint (copy references; values immutable after put).</summary>
    public List<KeyValuePair<MemFieldKey, MemFieldValue>> Snapshot()
    {
        lock (_lock)
            return _map.ToList();
    }

    public void Clear()
    {
        lock (_lock) _map.Clear();
    }

    private static int Compare(MemFieldValue a, MemFieldValue b)
    {
        int c = a.Wall.CompareTo(b.Wall);
        if (c != 0) return c;
        c = a.Logical.CompareTo(b.Logical);
        if (c != 0) return c;
        return ByteUtil.CompareAuthorNodeId(a.AuthorNodeId, b.AuthorNodeId);
    }
}
