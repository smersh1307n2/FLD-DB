using System.Buffers.Binary;
using System.Diagnostics;
using System.Text.Json;
using Fld1.Storage.Engine;

namespace Fld1.Engine.Bench;

/// <summary>
/// One-shot 1M-entry B+ tree scale gate, separate from the normal PR microbenchmarks.
/// This measures the standalone immutable tree primitive, not StorageDatabase durability.
/// </summary>
internal static class StorageIndexMillionScale
{
    private const int EntryCount = 1_000_000;
    private const int KeyLength = 8;
    private const int ValueLength = 128;
    private const int LookupCount = 10_000;

    public static void Run()
    {
        string reportPath = Path.GetFullPath("storage-index-million-scale.json");
        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();

        using Process process = Process.GetCurrentProcess();
        long workingSetBefore = process.WorkingSet64;
        long allocatedBefore = GC.GetTotalAllocatedBytes(true);
        var keyBytes = new byte[checked(EntryCount * KeyLength)];
        var entries = new StorageTreeBuildEntry[EntryCount];

        for (int i = 0; i < EntryCount; i++)
        {
            int offset = checked(i * KeyLength);
            BinaryPrimitives.WriteUInt64BigEndian(keyBytes.AsSpan(offset, KeyLength), checked((ulong)i + 1));
            entries[i] = new StorageTreeBuildEntry(
                new ReadOnlyMemory<byte>(keyBytes, offset, KeyLength),
                checked((ulong)offset),
                checked((ulong)i * ValueLength),
                ValueLength,
                false);
        }

        var pages = new Dictionary<ulong, byte[]>();
        ulong nextPageId = 1;
        var buildWatch = Stopwatch.StartNew();
        ulong root = StorageBTreeBuilder.Build(1, entries, () => nextPageId++,
            (pageId, page) => pages.Add(pageId, page));
        buildWatch.Stop();
        long allocatedAfterBuild = GC.GetTotalAllocatedBytes(true);
        long workingSetAfterBuild = process.WorkingSet64;

        var pageCache = new StoragePageCache(StoragePageCodec.PageSize * 2048L);
        var reader = new StorageBTreeReader(1, root,
            pageId => pageCache.Acquire(new StoragePageKey(1, pageId), () => pages[pageId]),
            reference => new ReadOnlyMemory<byte>(keyBytes, checked((int)reference.Offset), reference.Length));
        byte[] lookupKey = new byte[KeyLength];
        var lookupWatch = Stopwatch.StartNew();
        for (int i = 0; i < LookupCount; i++)
        {
            int index = (int)(((long)i * 104_729 + 17) % EntryCount);
            BinaryPrimitives.WriteUInt64BigEndian(lookupKey, checked((ulong)index + 1));
            if (!reader.TryGet(lookupKey, out StorageTreeValueReference value) || value.Length != ValueLength)
                throw new InvalidDataException($"B+ tree point lookup failed at index {index}.");
        }
        lookupWatch.Stop();

        var scanWatch = Stopwatch.StartNew();
        int scanned = 0;
        using (var cursor = reader.OpenPrefixCursor(new byte[5]))
        {
            while (cursor.TryReadNext(out _))
                scanned++;
        }
        scanWatch.Stop();
        if (scanned != EntryCount)
            throw new InvalidDataException($"B+ tree prefix scan returned {scanned}; expected {EntryCount}.");

        process.Refresh();
        var report = new
        {
            Schema = "fld-storage-index-million-scale-v1",
            EntryCount,
            KeyLength,
            ValueLength,
            LookupCount,
            PrefixScanCount = scanned,
            PageSizeBytes = StoragePageCodec.PageSize,
            PageCount = pages.Count,
            RootPageId = root,
            BuildMilliseconds = buildWatch.Elapsed.TotalMilliseconds,
            PointLookupMilliseconds = lookupWatch.Elapsed.TotalMilliseconds,
            PrefixScanMilliseconds = scanWatch.Elapsed.TotalMilliseconds,
            BuildAllocatedBytes = allocatedAfterBuild - allocatedBefore,
            TotalAllocatedBytes = GC.GetTotalAllocatedBytes(true) - allocatedBefore,
            WorkingSetBeforeBytes = workingSetBefore,
            WorkingSetAfterBuildBytes = workingSetAfterBuild,
            WorkingSetAfterScanBytes = process.WorkingSet64,
            ManagedHeapAfterScanBytes = GC.GetTotalMemory(false),
            ProcessPeakWorkingSetBytes = process.PeakWorkingSet64
        };
        File.WriteAllText(reportPath, JsonSerializer.Serialize(report, new JsonSerializerOptions { WriteIndented = true }));
        Console.WriteLine($"1M B+ tree scale gate passed: pages={pages.Count}, build={buildWatch.Elapsed.TotalMilliseconds:F1} ms, lookups={lookupWatch.Elapsed.TotalMilliseconds:F1} ms, scan={scanWatch.Elapsed.TotalMilliseconds:F1} ms");
        Console.WriteLine($"Report: {reportPath}");
    }
}
