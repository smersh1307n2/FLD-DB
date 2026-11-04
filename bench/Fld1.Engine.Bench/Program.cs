using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Running;
using Fld1.Core.Uuid;
using Fld1.Engine.Db;

BenchmarkRunner.Run<EngineBench>();

[MemoryDiagnoser]
public class EngineBench
{
    private string _dir = null!;
    private FldDatabase _db = null!;
    private byte[] _rec = null!;
    private byte[] _val = null!;

    [GlobalSetup]
    public void Setup()
    {
        _dir = Path.Combine(Path.GetTempPath(), "fld-bench-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_dir);
        _db = FldDatabase.Open(_dir, 0);
        _rec = new byte[16];
        Uuid7.NewUuid7(_rec);
        _val = new byte[64];
        Random.Shared.NextBytes(_val);
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _db.Dispose();
        try { Directory.Delete(_dir, true); } catch { }
    }

    [Benchmark]
    public void Put()
    {
        _db.Put(_rec, 1, _val);
    }

    [Benchmark]
    public ulong PutCommit()
    {
        _db.Put(_rec, 1, _val);
        return _db.Commit();
    }

    [Benchmark]
    public bool GetOverlay()
    {
        _db.Put(_rec, 1, _val);
        return _db.TryGet(_rec, 1, out _);
    }
}
