using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Running;
using Fld1.Core.Uuid;
using Fld1.Engine.Db;

BenchmarkRunner.Run<EngineBench>();
BenchmarkRunner.Run<OutboxBench>();

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


[MemoryDiagnoser]
public class OutboxBench
{
    private const int EntryCount = 1024;
    private string _dir = null!;
    private FldDatabase _db = null!;
    private Fld1.Engine.Outbox.OutboxLease _lease = null!;
    private Fld1.Engine.Outbox.OutboxEntryVisitor _visitor = null!;
    private int _batchSize;
    private int _visited;

    [Params(64, 1024)]
    public int BatchSize
    {
        get => _batchSize;
        set => _batchSize = value;
    }

    [GlobalSetup]
    public void Setup()
    {
        _dir = Path.Combine(Path.GetTempPath(), "fld-outbox-bench-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_dir);
        _db = FldDatabase.Open(_dir, 0);

        for (int i = 0; i < EntryCount; i++)
        {
            var recordId = FldDatabase.NewRecordId();
            _db.Put(recordId, 1, "0123456789abcdef0123456789abcdef"u8);
        }
        _db.Commit();

        byte[] owner = new byte[16];
        Uuid7.NewUuid7(owner);
        _lease = _db.AcquireOutboxLease(owner);
        _visitor = Visit;
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _db.Dispose();
        try { Directory.Delete(_dir, true); } catch { }
    }

    [Benchmark(Baseline = true)]
    public int MaterializedBatch() => _db.ReadOutboxBatch(0, BatchSize).Count;

    [Benchmark]
    public int BorrowedBatch() => _db.VisitOutboxBatch(0, BatchSize, _lease, _visitor);

    private bool Visit(in Fld1.Engine.Outbox.OutboxEntryView entry)
    {
        _visited += entry.Value.Length;
        return true;
    }
}
