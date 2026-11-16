using System.Buffers.Binary;
using BenchmarkDotNet.Attributes;
using Fld1.Engine.Db;
using Fld1.Core.Uuid;

namespace Fld1.Engine.Bench;

/// <summary>
/// Measures fresh database open + first demand-paged Run mapping separately from
/// repeated reads after all Run mappings have been established. The filesystem page
/// cache is intentionally not described as cold: only the process mappings are fresh.
/// </summary>
[MemoryDiagnoser]
public sealed class RunMappingBench
{
    private const int RecordsPerRun = 128;
    private const int ValueLength = 64;

    private string _directory = null!;
    private string _freshDirectory = null!;
    private FldDatabase _warmDatabase = null!;
    private (byte[] RecordId, byte[] Value)[] _probes = null!;

    [Params(1, 16, 64)]
    public int RunCount { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        string fixtureId = Guid.NewGuid().ToString("N");
        _directory = Path.Combine(Path.GetTempPath(), "fld-run-map-warm-" + fixtureId);
        _freshDirectory = Path.Combine(Path.GetTempPath(), "fld-run-map-fresh-" + fixtureId);
        Directory.CreateDirectory(_directory);
        Directory.CreateDirectory(_freshDirectory);
        _probes = new (byte[] RecordId, byte[] Value)[RunCount];

        for (int run = 0; run < RunCount; run++)
            _probes[run] = (NewRecordId(), CreateValue(run, 0));

        SeedDatabase(_directory);
        SeedDatabase(_freshDirectory);

        _warmDatabase = FldDatabase.Open(_directory, namespaceId: 0);
        // Establish all process mappings before warm-path measurements.
        foreach (var probe in _probes)
        {
            if (!_warmDatabase.TryGet(probe.RecordId, 1, out ReadOnlySpan<byte> value) ||
                !value.SequenceEqual(probe.Value))
                throw new InvalidDataException("Run mapping benchmark fixture failed validation.");
        }
    }

    private void SeedDatabase(string directory)
    {
        using var seed = FldDatabase.Open(directory, namespaceId: 0);
        for (int run = 0; run < RunCount; run++)
        {
            for (int record = 0; record < RecordsPerRun; record++)
            {
                byte[] recordId = record == 0 ? _probes[run].RecordId : NewRecordId();
                byte[] value = record == 0 ? _probes[run].Value : CreateValue(run, record);
                seed.Put(recordId, 1, value);
            }
            seed.Commit();
            seed.Checkpoint();
        }
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _warmDatabase?.Dispose();
        try { Directory.Delete(_directory, recursive: true); } catch { }
        try { Directory.Delete(_freshDirectory, recursive: true); } catch { }
    }

    [Benchmark]
    public int FreshDatabaseOpenAndFirstMappedLookup()
    {
        using var database = FldDatabase.Open(_freshDirectory, namespaceId: 0);
        if (!database.TryGet(_probes[0].RecordId, 1, out ReadOnlySpan<byte> value))
            throw new InvalidDataException("Fresh mapped lookup did not find the fixture record.");
        return value.Length;
    }

    [Benchmark]
    public int WarmMappedLookups()
    {
        int checksum = 0;
        foreach (var probe in _probes)
        {
            if (!_warmDatabase.TryGet(probe.RecordId, 1, out ReadOnlySpan<byte> value))
                throw new InvalidDataException("Warm mapped lookup did not find the fixture record.");
            checksum += value.Length;
        }
        return checksum;
    }

    private static byte[] NewRecordId()
    {
        byte[] id = new byte[16];
        Uuid7.NewUuid7(id);
        return id;
    }

    private static byte[] CreateValue(int run, int record)
    {
        byte[] value = new byte[ValueLength];
        BinaryPrimitives.WriteInt32LittleEndian(value, run);
        BinaryPrimitives.WriteInt32LittleEndian(value.AsSpan(4), record);
        Random.Shared.NextBytes(value.AsSpan(8));
        return value;
    }
}
