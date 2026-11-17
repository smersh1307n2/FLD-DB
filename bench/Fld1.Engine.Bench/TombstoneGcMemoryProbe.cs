using System.Diagnostics;
using Fld1.Engine.Db;

namespace Fld1.Engine.Bench;

/// <summary>
/// Isolated memory probe. Separates OS process high-water mark, sampled heap occupancy,
/// compaction-thread allocations, sampler-thread allocations, and memory retained after a forced collection.
/// </summary>
internal static class TombstoneGcMemoryProbe
{
    public static int Run(string directory, int tombstoneCount)
    {
        using var database = FldDatabase.Open(directory, 0);

        // Remove unreachable setup garbage before establishing the measurement baseline.
        GC.Collect(GC.MaxGeneration, GCCollectionMode.Forced, blocking: true, compacting: true);
        GC.WaitForPendingFinalizers();
        GC.Collect(GC.MaxGeneration, GCCollectionMode.Forced, blocking: true, compacting: true);

        using var process = Process.GetCurrentProcess();
        process.Refresh();
        long baselineWorkingSet = process.WorkingSet64;
        long baselineProcessPeakWorkingSet = process.PeakWorkingSet64;
        long baselineManagedRetained = GC.GetTotalMemory(forceFullCollection: true);
        GCMemoryInfo baselineGcInfo = GC.GetGCMemoryInfo();
        int[] baselineCollections =
        [
            GC.CollectionCount(0),
            GC.CollectionCount(1),
            GC.CollectionCount(2)
        ];

        // Create the sampler before the process-allocation baseline. Its thread waits
        // until Start(), so thread startup and synchronization setup are excluded.
        using var sampler = new Sampler(process);
        long baselineProcessAllocated = GC.GetTotalAllocatedBytes(precise: true);
        sampler.Start();

        // CompactTombstones is synchronous. This per-thread counter isolates allocations
        // made by the thread executing compaction from the sampler's own allocations.
        long compactionThreadAllocatedBefore = GC.GetAllocatedBytesForCurrentThread();
        int reclaimed = database.CompactTombstones(static _ => true);
        long compactionThreadAllocated =
            GC.GetAllocatedBytesForCurrentThread() - compactionThreadAllocatedBefore;
        sampler.Stop();

        process.Refresh();
        long finalWorkingSet = process.WorkingSet64;
        long processPeakWorkingSet = process.PeakWorkingSet64;
        long processAllocatedDuringMeasurement =
            GC.GetTotalAllocatedBytes(precise: true) - baselineProcessAllocated;
        int[] collectionsDuringSweep =
        [
            GC.CollectionCount(0) - baselineCollections[0],
            GC.CollectionCount(1) - baselineCollections[1],
            GC.CollectionCount(2) - baselineCollections[2]
        ];

        // A full collection after the operation measures what remains reachable,
        // not the transient peak. Keep it explicitly separate from sampled occupancy.
        GC.Collect(GC.MaxGeneration, GCCollectionMode.Forced, blocking: true, compacting: true);
        GC.WaitForPendingFinalizers();
        GC.Collect(GC.MaxGeneration, GCCollectionMode.Forced, blocking: true, compacting: true);
        long postSweepManagedRetained = GC.GetTotalMemory(forceFullCollection: true);
        GCMemoryInfo postSweepGcInfo = GC.GetGCMemoryInfo();

        Console.WriteLine(
            $"TOMBSTONE_GC_MEMORY_DIAGNOSTIC Count={tombstoneCount} SampleIntervalMs=1 " +
            $"Reclaimed={reclaimed} " +
            $"BaselineWorkingSetBytes={baselineWorkingSet} FinalWorkingSetBytes={finalWorkingSet} " +
            $"SampledPeakWorkingSetBytes={sampler.PeakWorkingSetBytes} " +
            $"ProcessPeakWorkingSetBeforeBytes={baselineProcessPeakWorkingSet} " +
            $"ProcessPeakWorkingSetAfterBytes={processPeakWorkingSet} " +
            $"ProcessPeakWorkingSetDeltaBytes={Math.Max(0, processPeakWorkingSet - baselineProcessPeakWorkingSet)} " +
            $"SampledPeakManagedHeapOccupancyBytes={sampler.PeakManagedHeapOccupancyBytes} " +
            $"BaselineManagedRetainedBytes={baselineManagedRetained} " +
            $"PostSweepManagedRetainedBytes={postSweepManagedRetained} " +
            $"ManagedRetainedDeltaBytes={postSweepManagedRetained - baselineManagedRetained} " +
            $"CompactionThreadAllocatedBytes={compactionThreadAllocated} " +
            $"SamplerThreadAllocatedBytes={sampler.AllocatedBytes} " +
            $"ProcessAllocatedBytesDuringMeasurement={processAllocatedDuringMeasurement} " +
            $"Gen0Collections={collectionsDuringSweep[0]} Gen1Collections={collectionsDuringSweep[1]} Gen2Collections={collectionsDuringSweep[2]} " +
            $"BaselineHeapSizeBytes={baselineGcInfo.HeapSizeBytes} PostSweepHeapSizeBytes={postSweepGcInfo.HeapSizeBytes} " +
            $"BaselineFragmentedBytes={baselineGcInfo.FragmentedBytes} PostSweepFragmentedBytes={postSweepGcInfo.FragmentedBytes} " +
            $"BaselineCommittedBytes={baselineGcInfo.TotalCommittedBytes} PostSweepCommittedBytes={postSweepGcInfo.TotalCommittedBytes}");

        if (reclaimed != tombstoneCount)
            throw new InvalidOperationException(
                $"Isolated memory probe reclaimed {reclaimed}/{tombstoneCount} entries.");
        return 0;
    }

    private sealed class Sampler : IDisposable
    {
        private readonly Process _process;
        private readonly ManualResetEventSlim _start = new(false);
        private readonly ManualResetEventSlim _stop = new(false);
        private readonly ManualResetEventSlim _ready = new(false);
        private readonly Thread _thread;
        private bool _started;
        private bool _stopped;

        public long PeakWorkingSetBytes { get; private set; }
        public long PeakManagedHeapOccupancyBytes { get; private set; }
        public long AllocatedBytes { get; private set; }

        public Sampler(Process process)
        {
            _process = process;
            _thread = new Thread(SampleLoop)
            {
                IsBackground = true,
                Name = "FLD tombstone GC memory sampler"
            };
            _thread.Start();
            _ready.Wait();
        }

        public void Start()
        {
            if (_started)
                return;
            _process.Refresh();
            PeakWorkingSetBytes = _process.WorkingSet64;
            PeakManagedHeapOccupancyBytes = GC.GetTotalMemory(forceFullCollection: false);
            _started = true;
            _start.Set();
        }

        public void Stop()
        {
            if (_stopped)
                return;
            if (!_started)
                Start();
            _stop.Set();
            _thread.Join();
            _stopped = true;
        }

        private void SampleLoop()
        {
            _ready.Set();
            _start.Wait();
            long allocatedBefore = GC.GetAllocatedBytesForCurrentThread();
            while (!_stop.Wait(TimeSpan.FromMilliseconds(1)))
                Sample();
            AllocatedBytes = GC.GetAllocatedBytesForCurrentThread() - allocatedBefore;
        }

        private void Sample()
        {
            _process.Refresh();
            long workingSet = _process.WorkingSet64;
            long managedHeapOccupancy = GC.GetTotalMemory(forceFullCollection: false);
            if (workingSet > PeakWorkingSetBytes)
                PeakWorkingSetBytes = workingSet;
            if (managedHeapOccupancy > PeakManagedHeapOccupancyBytes)
                PeakManagedHeapOccupancyBytes = managedHeapOccupancy;
        }

        public void Dispose()
        {
            Stop();
            _start.Dispose();
            _stop.Dispose();
            _ready.Dispose();
        }
    }
}
