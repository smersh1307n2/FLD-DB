using BenchmarkDotNet.Attributes;
using Fld1.Engine.Db;
using Fld1.Engine.Outbox;

namespace Fld1.Engine.Bench;

/// <summary>
/// Measures the complete physical Tombstone-GC sweep when each maintenance call
/// is capped at the production page limit. This reveals repeated whole-namespace
/// rewrite cost that a single-page benchmark cannot show.
/// </summary>
[InvocationCount(1)]
[MemoryDiagnoser]
public class TombstoneGcSweepBench
{
    private const int PageLimit = 4_096;
    private string _directory = string.Empty;
    private FldDatabase _database = null!;
    private byte[][] _recordIds = Array.Empty<byte[]>();
    private readonly byte[] _value = new byte[64];
    private long _lastGcAllocatedBytes;

    [Params(1_000, 10_000, 50_000, 100_000, 250_000)]
    public int TombstoneCount { get; set; }

    [IterationSetup]
    public void Setup()
    {
        _directory = Path.Combine(
            Path.GetTempPath(), "fld-tombstone-gc-sweep-bench-" + Guid.NewGuid().ToString("N"));
        _database = FldDatabase.Open(_directory, 0);
        _recordIds = new byte[TombstoneCount][];

        for (int i = 0; i < TombstoneCount; i++)
        {
            byte[] recordId = FldDatabase.NewRecordId();
            _recordIds[i] = recordId;
            _database.Put(recordId, 1, _value);
        }

        if (_database.Commit() == 0 || _database.Checkpoint() == 0)
            throw new InvalidOperationException("Tombstone sweep benchmark failed to persist its live-value generation.");

        foreach (byte[] recordId in _recordIds)
            _database.Delete(recordId, 1);

        if (_database.Commit() == 0 || _database.Checkpoint() == 0)
            throw new InvalidOperationException("Tombstone sweep benchmark failed to persist its tombstone generation.");

        ulong highWatermark = _database.GetOutboxHighWatermark();
        if (highWatermark != 0)
        {
            OutboxLease lease = _database.AcquireOutboxLease(Id(0x7E));
            _database.AcknowledgeOutbox(highWatermark, lease);
        }

        if (_database.GetOutboxHighWatermark() != _database.GetOutboxWatermark())
            throw new InvalidOperationException("Tombstone sweep benchmark Outbox prefix was not fully acknowledged.");
    }

    [IterationCleanup]
    public void Cleanup()
    {
        Console.WriteLine(
            $"TOMBSTONE_GC_SWEEP_ALLOCATED_BYTES Count={TombstoneCount} PageLimit={PageLimit} Bytes={_lastGcAllocatedBytes}");
        _database.Dispose();
        try { Directory.Delete(_directory, recursive: true); } catch { }
    }

    [Benchmark(Description = "Complete FLD tombstone-GC sweep using bounded maintenance pages")]
    public int ReclaimAllTombstonesInBoundedPages()
    {
        long before = GC.GetAllocatedBytesForCurrentThread();
        int reclaimedTotal = 0;
        int pageCount = 0;

        while (reclaimedTotal < TombstoneCount)
        {
            int reclaimed = _database.CompactTombstones(static _ => true, PageLimit);
            if (reclaimed <= 0)
                throw new InvalidOperationException(
                    $"Tombstone sweep stopped after reclaiming {reclaimedTotal}/{TombstoneCount} entries.");
            reclaimedTotal += reclaimed;
            pageCount++;
        }

        _lastGcAllocatedBytes = GC.GetAllocatedBytesForCurrentThread() - before;
        int expectedPages = (TombstoneCount + PageLimit - 1) / PageLimit;
        if (reclaimedTotal != TombstoneCount || pageCount != expectedPages)
            throw new InvalidOperationException(
                $"Tombstone sweep reclaimed {reclaimedTotal} in {pageCount} pages; expected {TombstoneCount} in {expectedPages}.");

        return reclaimedTotal;
    }

    private static byte[] Id(byte value)
    {
        byte[] id = new byte[16];
        id[0] = value;
        return id;
    }
}
