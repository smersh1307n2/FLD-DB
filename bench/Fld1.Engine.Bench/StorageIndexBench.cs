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
    private StorageDatabase _database = null!;
    private byte[][] _keys = null!;
    private int _probeIndex;

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

        _database = StorageDatabase.Open(_directory, 0, walRotateAfterBytes: 0);
        _probeIndex = EntryCount / 2;
        if (!_database.TryGet(_keys[_probeIndex], out _))
            throw new InvalidDataException("Storage index benchmark fixture failed validation.");
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _database?.Dispose();
        try { Directory.Delete(_directory, recursive: true); } catch { }
    }

    [Benchmark]
    public int FreshOpenAndPointLookup()
    {
        using var database = StorageDatabase.Open(_directory, 0, walRotateAfterBytes: 0);
        if (!database.TryGet(_keys[_probeIndex], out byte[]? value))
            throw new InvalidDataException("Fresh Storage lookup did not find the fixture key.");
        return value!.Length;
    }

    [Benchmark]
    public int FreshOpenAndSnapshotReadOneEntry()
    {
        using var database = StorageDatabase.Open(_directory, 0, walRotateAfterBytes: 0);
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
}
