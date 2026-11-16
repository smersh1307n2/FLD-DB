using System.Buffers.Binary;
using BenchmarkDotNet.Attributes;
using Fld1.Storage.Engine;

namespace Fld1.Engine.Bench;

/// <summary>
/// Establishes the current in-memory StorageDatabase scaling baseline.
/// This intentionally measures the existing dictionary + sorted snapshot index;
/// it is not a target-engine benchmark and does not claim bounded memory.
/// </summary>
[MemoryDiagnoser]
public class StorageIndexBench
{
    private const int ValueLength = 128;
    private string _directory = null!;
    private string _freshDirectory = null!;
    private StorageDatabase _database = null!;
    private StorageSnapshot _storageSnapshot = null!;
    private byte[][] _keys = null!;
    private int _probeIndex;
    private StorageTreeBuildEntry[] _treeEntries = null!;
    private Dictionary<ulong, byte[]> _treeKeys = null!;
    private Dictionary<ulong, byte[]> _treePages = null!;
    private StorageBTreeReader _treeReader = null!;
    private ulong _treeRoot;
    private StorageBTreeReader _pressureReader = null!;
    private StoragePageCache _treePageCache = null!;
    private StorageBTreeReader _largeValueReader = null!;
    private int _randomProbe;


    [Params(1_000, 10_000, 100_000)]
    public int EntryCount { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _directory = Path.Combine(Path.GetTempPath(), "fld-storage-index-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_directory);
        _keys = new byte[EntryCount][];
        using (var seed = StorageDatabase.Open(_directory, 0, walRotateAfterBytes: 0))
        {
            using var batch = seed.BeginWriteBatch();
            for (int i = 0; i < EntryCount; i++)
            {
                byte[] key = new byte[8];
                BinaryPrimitives.WriteInt64BigEndian(key, i + 1L);
                byte[] value = new byte[ValueLength];
                BinaryPrimitives.WriteInt32LittleEndian(value, i);
                Random.Shared.NextBytes(value.AsSpan(4));
                _keys[i] = key;
                batch.Put(key, value);
            }
            batch.Commit();
            seed.Checkpoint();
        }

        // Keep fresh-open measurements on a separate physical directory so the
        // warm fixture can retain its own process-local WAL writer fence.
        _freshDirectory = Path.Combine(Path.GetTempPath(), "fld-storage-index-fresh-" + Guid.NewGuid().ToString("N"));
        CopyDirectory(_directory, _freshDirectory);

        _database = StorageDatabase.Open(_directory, 0, walRotateAfterBytes: 0);
        _storageSnapshot = _database.OpenSnapshot();
        _probeIndex = EntryCount / 2;
        if (!_database.TryGet(_keys[_probeIndex], out _))
            throw new InvalidDataException("Storage index benchmark fixture failed validation.");

        _treeKeys = new Dictionary<ulong, byte[]>(EntryCount);
        _treeEntries = new StorageTreeBuildEntry[EntryCount];
        for (int i = 0; i < EntryCount; i++)
        {
            ulong offset = checked((ulong)i + 1);
            _treeKeys.Add(offset, _keys[i]);
            _treeEntries[i] = new StorageTreeBuildEntry(_keys[i], offset, offset, ValueLength, false);
        }
        _treePages = new Dictionary<ulong, byte[]>();
        ulong nextPageId = 1;
        _treeRoot = StorageBTreeBuilder.Build(1, _treeEntries, () => nextPageId++,
            (pageId, page) => _treePages.Add(pageId, page));
        _treeReader = new StorageBTreeReader(1, _treeRoot, pageId => _treePages[pageId],
            key => new ReadOnlyMemory<byte>(_treeKeys[key.Offset]));
        if (!_treeReader.TryGet(_keys[_probeIndex], out _))
            throw new InvalidDataException("B+ tree benchmark fixture failed validation.");

        _treePageCache = new StoragePageCache(StoragePageCodec.PageSize * 4L);
        _pressureReader = new StorageBTreeReader(1, _treeRoot,
            pageId => _treePageCache.Acquire(new StoragePageKey(1, pageId), () => _treePages[pageId]),
            key => new ReadOnlyMemory<byte>(_treeKeys[key.Offset]));

        var largeValuePages = new Dictionary<ulong, byte[]>();
        ulong largePageId = 1;
        StorageTreeBuildEntry[] largeValueEntries = _treeEntries
            .Select(entry => new StorageTreeBuildEntry(entry.Key, entry.KeyOffset, entry.ValueOffset, 1024 * 1024, false))
            .ToArray();
        ulong largeRoot = StorageBTreeBuilder.Build(3, largeValueEntries, () => largePageId++,
            (pageId, page) => largeValuePages.Add(pageId, page));
        _largeValueReader = new StorageBTreeReader(3, largeRoot, pageId => largeValuePages[pageId],
            key => new ReadOnlyMemory<byte>(_treeKeys[key.Offset]));
    }

    private static void CopyDirectory(string source, string destination)
    {
        Directory.CreateDirectory(destination);
        foreach (string directory in Directory.EnumerateDirectories(source, "*", SearchOption.AllDirectories))
            Directory.CreateDirectory(Path.Combine(destination, Path.GetRelativePath(source, directory)));
        foreach (string file in Directory.EnumerateFiles(source, "*", SearchOption.AllDirectories))
            File.Copy(file, Path.Combine(destination, Path.GetRelativePath(source, file)));
    }


    [GlobalCleanup]
    public void Cleanup()
    {
        _storageSnapshot?.Dispose();
        _database?.Dispose();
        try { Directory.Delete(_directory, recursive: true); } catch { }
        try { Directory.Delete(_freshDirectory, recursive: true); } catch { }
    }

    [Benchmark]
    public int FreshOpenAndPointLookup()
    {
        using var database = StorageDatabase.Open(_freshDirectory, 0, walRotateAfterBytes: 0);
        if (!database.TryGet(_keys[_probeIndex], out byte[]? value))
            throw new InvalidDataException("Fresh Storage lookup did not find the fixture key.");
        return value!.Length;
    }

    [Benchmark]
    public int FreshOpenAndSnapshotReadOneEntry()
    {
        using var database = StorageDatabase.Open(_freshDirectory, 0, walRotateAfterBytes: 0);
        using var snapshot = database.OpenSnapshot();
        if (!snapshot.TryGet(_keys[_probeIndex], out byte[]? value))
            throw new InvalidDataException("Fresh Storage snapshot did not find the fixture key.");
        return value!.Length;
    }

    [Benchmark]
    public int CachedSnapshotAndReadOneEntry()
    {
        using var snapshot = _database.OpenSnapshot();
        if (!snapshot.TryGet(_keys[_probeIndex], out byte[]? value))
            throw new InvalidDataException("Cached Storage snapshot did not find the fixture key.");
        return value!.Length;
    }

    [Benchmark]
    public int WarmPointLookup()
    {
        if (!_database.TryGet(_keys[_probeIndex], out byte[]? value))
            throw new InvalidDataException("Warm Storage lookup did not find the fixture key.");
        return value!.Length;
    }

    [Benchmark]
    public int BTreeWarmPointLookup()
    {
        if (!_treeReader.TryGet(_keys[_probeIndex], out StorageTreeValueReference value))
            throw new InvalidDataException("B+ tree lookup did not find the fixture key.");
        return value.Length;
    }

    [Benchmark]
    public int StoragePrefixScan()
    {
        int count = 0;
        using var cursor = _storageSnapshot.OpenPrefixCursor(new byte[] { 0, 0, 0, 0 });
        while (cursor.TryReadNext(out _)) count++;
        if (count != EntryCount)
            throw new InvalidDataException($"Storage prefix scan returned {count} entries; expected {EntryCount}.");
        return count;
    }

    [Benchmark]
    public int BTreePrefixScan()
    {
        int count = 0;
        using var cursor = _treeReader.OpenPrefixCursor(new byte[] { 0, 0, 0, 0 });
        while (cursor.TryReadNext(out _)) count++;
        if (count != EntryCount)
            throw new InvalidDataException($"B+ tree prefix scan returned {count} entries; expected {EntryCount}.");
        return count;
    }

    [Benchmark]
    public int BTreeBulkBuild()
    {
        var pages = new Dictionary<ulong, byte[]>();
        ulong nextPageId = 1;
        ulong root = StorageBTreeBuilder.Build(2, _treeEntries, () => nextPageId++,
            (pageId, page) => pages.Add(pageId, page));
        return checked(pages.Count + (int)root);
    }

    [Benchmark]
    public int BTreeRandomPointLookup()
    {
        _randomProbe = (_randomProbe + 7919) % EntryCount;
        if (!_treeReader.TryGet(_keys[_randomProbe], out StorageTreeValueReference value))
            throw new InvalidDataException("B+ tree random lookup missed the fixture.");
        return value.Length;
    }

    [Benchmark]
    public int BTreePageCachePressureLookup()
    {
        _randomProbe = (_randomProbe + 3571) % EntryCount;
        if (!_pressureReader.TryGet(_keys[_randomProbe], out StorageTreeValueReference value))
            throw new InvalidDataException("B+ tree cache-pressure lookup missed the fixture.");
        return value.Length;
    }

    [Benchmark]
    public int BTreeLargeValueReferenceLookup()
    {
        if (!_largeValueReader.TryGet(_keys[_probeIndex], out StorageTreeValueReference value))
            throw new InvalidDataException("B+ tree large-value lookup missed the fixture.");
        return value.Length;
    }
}
