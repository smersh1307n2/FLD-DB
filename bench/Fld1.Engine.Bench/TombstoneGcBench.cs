using BenchmarkDotNet.Attributes;
using Fld1.Engine.Db;
using Fld1.Engine.Outbox;

namespace Fld1.Engine.Bench;

[InvocationCount(1)]
[MemoryDiagnoser]
public sealed class TombstoneGcBench
{
    private string _directory = string.Empty;
    private FldDatabase _database = null!;
    private byte[][] _recordIds = Array.Empty<byte[]>();
    private readonly byte[] _value = new byte[64];
    private long _lastGcAllocatedBytes;

    [Params(1_000, 10_000, 50_000)]
    public int TombstoneCount { get; set; }

    [Params(256, 4_096)]
    public int PageLimit { get; set; }

    [IterationSetup]
    public void Setup()
    {
        _directory = Path.Combine(
            Path.GetTempPath(), "fld-tombstone-gc-bench-" + Guid.NewGuid().ToString("N"));
        _database = FldDatabase.Open(_directory, 0);
        _recordIds = new byte[TombstoneCount][];

        // Build a compactable namespace with one current field tombstone per record.
        // Setup and durable writes are outside the measured invocation.
        for (int i = 0; i < TombstoneCount; i++)
        {
            byte[] recordId = FldDatabase.NewRecordId();
            _recordIds[i] = recordId;
            _database.Put(recordId, 1, _value);
        }

        if (_database.Commit() == 0 || _database.Checkpoint() == 0)
            throw new InvalidOperationException("Tombstone benchmark failed to persist its live-value generation.");

        foreach (byte[] recordId in _recordIds)
            _database.Delete(recordId, 1);

        if (_database.Commit() == 0 || _database.Checkpoint() == 0)
            throw new InvalidOperationException("Tombstone benchmark failed to persist its tombstone generation.");

        ulong highWatermark = _database.GetOutboxHighWatermark();
        if (highWatermark != 0)
        {
            OutboxLease lease = _database.AcquireOutboxLease(Id(0x7E));
            _database.AcknowledgeOutbox(highWatermark, lease);
        }

        if (_database.GetOutboxHighWatermark() != _database.GetOutboxAckedThrough())
            throw new InvalidOperationException("Tombstone benchmark Outbox prefix was not fully acknowledged.");
    }

    [IterationCleanup]
    public void Cleanup()
    {
        Console.WriteLine(
            $"TOMBSTONE_GC_ALLOCATED_BYTES Count={TombstoneCount} Limit={PageLimit} Bytes={_lastGcAllocatedBytes}");
        _database.Dispose();
        try { Directory.Delete(_directory, recursive: true); } catch { }
    }

    [Benchmark(Description = "Bounded FLD tombstone-GC page / namespace cardinality")]
    public int CompactBoundedTombstonePage()
    {
        long before = GC.GetAllocatedBytesForCurrentThread();
        int reclaimed = _database.CompactTombstones(static _ => true, PageLimit);
        _lastGcAllocatedBytes = GC.GetAllocatedBytesForCurrentThread() - before;

        int expected = Math.Min(TombstoneCount, PageLimit);
        if (reclaimed != expected)
            throw new InvalidOperationException(
                $"Tombstone GC reclaimed {reclaimed}; expected {expected} for count={TombstoneCount}, limit={PageLimit}.");
        return reclaimed;
    }

    private static byte[] Id(byte value)
    {
        byte[] id = new byte[16];
        id[0] = value;
        return id;
    }
}
