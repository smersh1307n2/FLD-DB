using BenchmarkDotNet.Attributes;
using Fld1.Engine.Db;
using Fld1.Engine.Outbox;

namespace Fld1.Engine.Bench;

/// <summary>
/// Measures one complete physical Tombstone-GC namespace sweep across increasing
/// cardinalities, including an isolated process peak-memory probe.
/// </summary>
[InvocationCount(1)]
[MemoryDiagnoser]
public class TombstoneGcSweepBench
{
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
            $"TOMBSTONE_GC_SWEEP_ALLOCATED_BYTES Count={TombstoneCount} Bytes={_lastGcAllocatedBytes}");
        if (_database is not null)
            _database.Dispose();
        try { Directory.Delete(_directory, recursive: true); } catch { }
    }

    [Benchmark(Description = "Complete FLD tombstone-GC sweep in one namespace rewrite")]
    public int ReclaimAllTombstonesInSinglePass()
    {
        long before = GC.GetAllocatedBytesForCurrentThread();
        int reclaimed = _database.CompactTombstones(static _ => true);
        _lastGcAllocatedBytes = GC.GetAllocatedBytesForCurrentThread() - before;
        if (reclaimed != TombstoneCount)
            throw new InvalidOperationException(
                $"Single-pass tombstone sweep reclaimed {reclaimed}/{TombstoneCount} entries.");
        return reclaimed;
    }

    [Benchmark(Description = "Diagnostic: isolated single-pass Tombstone-GC peak memory")]
    public int ReclaimAllTombstonesInSinglePassWithPeakSampling()
    {
        // Setup is performed by the BenchmarkDotNet parent process. Dispose its
        // database before launching a fresh process so setup RSS cannot contaminate
        // the measured process high-water mark.
        _database.Dispose();
        _database = null!;

        string assembly = typeof(TombstoneGcSweepBench).Assembly.Location;
        var startInfo = new System.Diagnostics.ProcessStartInfo("dotnet")
        {
            UseShellExecute = false,
            RedirectStandardOutput = true,
            RedirectStandardError = true
        };
        startInfo.ArgumentList.Add(assembly);
        startInfo.ArgumentList.Add("--tombstone-gc-probe-child");
        startInfo.ArgumentList.Add(_directory);
        startInfo.ArgumentList.Add(TombstoneCount.ToString(System.Globalization.CultureInfo.InvariantCulture));

        using var process = System.Diagnostics.Process.Start(startInfo)
            ?? throw new InvalidOperationException("Could not start isolated Tombstone-GC peak probe process.");
        string output = process.StandardOutput.ReadToEnd();
        string error = process.StandardError.ReadToEnd();
        process.WaitForExit();
        if (!string.IsNullOrWhiteSpace(output))
            Console.WriteLine(output.TrimEnd());
        if (process.ExitCode != 0)
            throw new InvalidOperationException(
                $"Isolated Tombstone-GC peak probe exited with {process.ExitCode}: {error}");
        return TombstoneCount;
    }

    public static int RunPeakProbeChild(string directory, int tombstoneCount)
    {
        using var database = FldDatabase.Open(directory, 0);
        using var sampler = new PeakMemorySampler();
        int reclaimed = database.CompactTombstones(static _ => true);
        sampler.Stop();
        Console.WriteLine(
            $"TOMBSTONE_GC_PEAK_SAMPLE Count={tombstoneCount} IntervalMs=2 " +
            $"BaselineWorkingSetBytes={sampler.BaselineWorkingSetBytes} " +
            $"PeakWorkingSetBytes={sampler.PeakWorkingSetBytes} " +
            $"WorkingSetDeltaBytes={sampler.PeakWorkingSetBytes - sampler.BaselineWorkingSetBytes} " +
            $"BaselineManagedHeapBytes={sampler.BaselineManagedHeapBytes} " +
            $"PeakManagedHeapBytes={sampler.PeakManagedHeapBytes} " +
            $"ManagedHeapDeltaBytes={sampler.PeakManagedHeapBytes - sampler.BaselineManagedHeapBytes}");
        if (reclaimed != tombstoneCount)
            throw new InvalidOperationException(
                $"Isolated peak-memory probe reclaimed {reclaimed}/{tombstoneCount} entries.");
        return 0;
    }

    /// <summary>
    /// Diagnostic-only sampler. Its background thread perturbs the measured operation;
    /// use its memory high-water marks, not its BenchmarkDotNet timing, as the result.
    /// The 2 ms sampling interval can miss shorter-lived peaks.
    /// </summary>
    private sealed class PeakMemorySampler : IDisposable
    {
        private readonly System.Diagnostics.Process _process;
        private readonly System.Threading.ManualResetEventSlim _stop = new(false);
        private readonly System.Threading.Thread _thread;
        private bool _stopped;

        public long BaselineWorkingSetBytes { get; }
        public long PeakWorkingSetBytes { get; private set; }
        public long BaselineManagedHeapBytes { get; }
        public long PeakManagedHeapBytes { get; private set; }

        public PeakMemorySampler()
        {
            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();
            _process = System.Diagnostics.Process.GetCurrentProcess();
            BaselineWorkingSetBytes = _process.WorkingSet64;
            PeakWorkingSetBytes = BaselineWorkingSetBytes;
            BaselineManagedHeapBytes = GC.GetTotalMemory(forceFullCollection: false);
            PeakManagedHeapBytes = BaselineManagedHeapBytes;
            _thread = new System.Threading.Thread(SampleLoop)
            {
                IsBackground = true,
                Name = "FLD Tombstone-GC peak-memory sampler"
            };
            _thread.Start();
        }

        public void Stop()
        {
            if (_stopped)
                return;
            _stop.Set();
            _thread.Join();
            Sample();
            _stopped = true;
        }

        private void SampleLoop()
        {
            while (!_stop.Wait(TimeSpan.FromMilliseconds(2)))
                Sample();
        }

        private void Sample()
        {
            long workingSet = _process.WorkingSet64;
            long managedHeap = GC.GetTotalMemory(forceFullCollection: false);
            if (workingSet > PeakWorkingSetBytes)
                PeakWorkingSetBytes = workingSet;
            if (managedHeap > PeakManagedHeapBytes)
                PeakManagedHeapBytes = managedHeap;
        }

        public void Dispose()
        {
            Stop();
            _stop.Dispose();
            _process.Dispose();
        }
    }
    private static byte[] Id(byte value)
    {
        byte[] id = new byte[16];
        id[0] = value;
        return id;
    }
}
