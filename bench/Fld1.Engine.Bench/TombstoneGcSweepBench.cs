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
    // Keep each WAL GroupCommit payload below the 16 MiB wire/storage limit.
    // The final checkpoint still consolidates each phase into one Run.
    private const int SetupCommitBatchSize = 8_192;
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

        for (int offset = 0; offset < TombstoneCount; offset += SetupCommitBatchSize)
        {
            int end = Math.Min(offset + SetupCommitBatchSize, TombstoneCount);
            for (int i = offset; i < end; i++)
            {
                byte[] recordId = FldDatabase.NewRecordId();
                _recordIds[i] = recordId;
                _database.Put(recordId, 1, _value);
            }

            if (_database.Commit() == 0)
                throw new InvalidOperationException("Tombstone sweep benchmark failed to commit a live-value setup batch.");
        }

        // Consolidate all bounded WAL commits into one live-value Run before
        // creating tombstones, so setup batching does not inflate Run cardinality.
        if (_database.Checkpoint() == 0)
            throw new InvalidOperationException("Tombstone sweep benchmark failed to persist its live-value generation.");

        for (int offset = 0; offset < TombstoneCount; offset += SetupCommitBatchSize)
        {
            int end = Math.Min(offset + SetupCommitBatchSize, TombstoneCount);
            for (int i = offset; i < end; i++)
                _database.Delete(_recordIds[i], 1);

            if (_database.Commit() == 0)
                throw new InvalidOperationException("Tombstone sweep benchmark failed to commit a tombstone setup batch.");
        }

        // As above, leave one consolidated tombstone Run for the measured sweep.
        if (_database.Checkpoint() == 0)
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

    [Benchmark(Description = "Complete FLD tombstone-GC sweep in one namespace rewrite")]
    public int ReclaimAllTombstonesInSinglePass()
    {
        long before = GC.GetAllocatedBytesForCurrentThread();
        int reclaimed = _database.CompactTombstones(static _ => true, TombstoneCount);
        _lastGcAllocatedBytes = GC.GetAllocatedBytesForCurrentThread() - before;
        if (reclaimed != TombstoneCount)
            throw new InvalidOperationException(
                $"Single-pass tombstone sweep reclaimed {reclaimed}/{TombstoneCount} entries.");
        return reclaimed;
    }

    private static byte[] Id(byte value)
    {
        byte[] id = new byte[16];
        id[0] = value;
        return id;
    }
}
