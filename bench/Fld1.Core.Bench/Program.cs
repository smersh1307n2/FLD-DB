using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Running;
using Fld1.Core.Hash;
using Fld1.Core.Util;
using Fld1.Core.Uuid;

BenchmarkRunner.Run<CoreBench>();

[MemoryDiagnoser]
[SimpleJob(warmupCount: 1, iterationCount: 5)]
public class CoreBench
{
    private byte[] _uuidA = new byte[16];
    private byte[] _uuidB = new byte[16];
    private byte[] _payload = new byte[4096];

    [GlobalSetup]
    public void Setup()
    {
        Uuid7.NewUuid7(_uuidA);
        Uuid7.NewUuid7(_uuidB);
        Random.Shared.NextBytes(_payload);
    }

    [Benchmark]
    public int CompareUnsigned16() => ByteUtil.CompareUnsigned16(_uuidA, _uuidB);

    [Benchmark]
    public ulong ProjectK() => Uuid7.ProjectK(_uuidA);

    [Benchmark]
    public ulong XxHash64_4K() => Hashes.XxHash64(_payload);

    [Benchmark]
    public uint Crc32C_4K() => Hashes.Crc32C(_payload);

    [Benchmark]
    public byte[] Blake3_4K() => Hashes.Blake3_256(_payload);
}
