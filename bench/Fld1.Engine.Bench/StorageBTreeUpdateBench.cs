using System.Buffers.Binary;
using BenchmarkDotNet.Attributes;
using Fld1.Storage.Engine;

namespace Fld1.Engine.Bench;

/// <summary>
/// Measures the durable Storage B+ tree copy-on-write update path at a fixed
/// database cardinality. Fixture creation is outside the measured operation.
/// One invocation per iteration keeps each sample on an identical fresh tree.
/// </summary>
[MemoryDiagnoser]
[InvocationCount(1)]
public class StorageBTreeUpdateBench
{
    private const ulong PartitionId = 0x53544F52414745;
    private const int ValueLength = 64;
    private string _directory = null!;
    private StorageDatabase _database = null!;
    private byte[][] _keys = null!;
    private byte[][] _values = null!;

    [Params(10_000, 50_000)]
    public int EntryCount { get; set; }

    [Params(64, 256, 1_024)]
    public int BatchSize { get; set; }

    [GlobalSetup]
    public void GlobalSetup()
    {
        _keys = new byte[EntryCount][];
        _values = new byte[EntryCount][];
        for (int i = 0; i < EntryCount; i++)
        {
            byte[] key = new byte[8];
            BinaryPrimitives.WriteUInt64BigEndian(key, checked((ulong)i + 1));
            _keys[i] = key;

            byte[] value = new byte[ValueLength];
            BinaryPrimitives.WriteInt32LittleEndian(value, i);
            Random.Shared.NextBytes(value.AsSpan(4));
            _values[i] = value;
        }
    }

    [IterationSetup]
    public void IterationSetup()
    {
        _directory = Path.Combine(Path.GetTempPath(),
            "fld-storage-update-" + Guid.NewGuid().ToString("N"));
        _database = StorageDatabase.Open(_directory, PartitionId, walRotateAfterBytes: 0);
        using var seed = _database.BeginWriteBatch();
        for (int i = 0; i < EntryCount; i++)
            seed.Put(_keys[i], _values[i]);
        seed.Commit();
    }

    [IterationCleanup]
    public void IterationCleanup()
    {
        _database.Dispose();
        try { Directory.Delete(_directory, recursive: true); } catch { }
    }

    [Benchmark(Description = "Durable sorted B+ tree update batch")]
    public ulong ApplySortedUpdateBatch()
    {
        using var batch = _database.BeginWriteBatch();
        for (int i = 0; i < BatchSize; i++)
        {
            int index = (int)((long)i * EntryCount / BatchSize);
            batch.Put(_keys[index], _values[index]);
        }
        return batch.Commit();
    }
}
