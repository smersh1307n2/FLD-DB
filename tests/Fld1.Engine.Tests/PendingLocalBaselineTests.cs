using Fld1.Engine.Db;
using Xunit;

namespace Fld1.Engine.Tests;

public sealed class PendingLocalBaselineTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "fld-pending-baseline-" + Guid.NewGuid().ToString("N"));

    public PendingLocalBaselineTests() => Directory.CreateDirectory(_dir);

    public void Dispose()
    {
        try { Directory.Delete(_dir, recursive: true); } catch { }
    }

    [Fact]
    public void Separate_commits_publish_separate_immutable_operations_for_same_field()
    {
        var recordId = FldDatabase.NewRecordId();
        using var db = FldDatabase.Open(_dir);

        for (byte value = 1; value <= 4; value++)
        {
            db.Put(recordId, 5, [value]);
            db.Commit();
        }

        var entries = db.ReadOutboxBatch(afterSeq: 0, limit: 10);
        Assert.Equal(4, entries.Count);
        Assert.Equal(new byte[] { 1 }, entries[0].Value);
        Assert.Equal(new byte[] { 2 }, entries[1].Value);
        Assert.Equal(new byte[] { 3 }, entries[2].Value);
        Assert.Equal(new byte[] { 4 }, entries[3].Value);
        Assert.Equal(4, entries.Select(entry => Convert.ToHexString(entry.ChangeId)).Distinct(StringComparer.Ordinal).Count());
        Assert.Equal(new ulong[] { 1, 2, 3, 4 }, entries.Select(entry => entry.OutboxSeq));
    }

    [Fact]
    public void Multiple_puts_inside_one_commit_are_already_coalesced_to_final_value()
    {
        var recordId = FldDatabase.NewRecordId();
        using var db = FldDatabase.Open(_dir);

        for (byte value = 1; value <= 4; value++)
            db.Put(recordId, 5, [value]);
        db.Commit();

        var entry = Assert.Single(db.ReadOutboxBatch(afterSeq: 0, limit: 10));
        Assert.Equal(new byte[] { 4 }, entry.Value);
        Assert.Equal(1UL, entry.OutboxSeq);
    }
}
