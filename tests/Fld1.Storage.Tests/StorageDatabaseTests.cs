using Fld1.Storage.Engine;
using Xunit;

namespace Fld1.Storage.Tests;

public sealed class StorageDatabaseTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "fld-storage-db-" + Guid.NewGuid().ToString("N"));
    public StorageDatabaseTests() => Directory.CreateDirectory(_dir);
    public void Dispose() { try { Directory.Delete(_dir, true); } catch { } }

    [Fact]
    public void Atomic_batch_is_durable_and_recovers_after_reopen()
    {
        using (var db = StorageDatabase.Open(_dir, 101))
        {
            using var batch = db.BeginWriteBatch();
            batch.Put("alpha"u8, "one"u8);
            batch.Put("beta"u8, "two"u8);
            Assert.Equal(1UL, batch.Commit());
            Assert.Equal(2, db.Count);
        }

        using var reopened = StorageDatabase.Open(_dir, 101);
        Assert.Equal(1UL, reopened.LastCommitSeq);
        Assert.True(reopened.TryGet("alpha"u8, out var alpha));
        Assert.Equal("one"u8.ToArray(), alpha);
        Assert.True(reopened.TryGet("beta"u8, out var beta));
        Assert.Equal("two"u8.ToArray(), beta);
    }

    [Fact]
    public void Batch_coalesces_repeated_key_updates_and_delete_is_durable()
    {
        using (var db = StorageDatabase.Open(_dir, 102))
        {
            using var first = db.BeginWriteBatch();
            first.Put("k"u8, "old"u8);
            first.Put("k"u8, "new"u8);
            first.Commit();

            using var second = db.BeginWriteBatch();
            second.Delete("k"u8);
            second.Commit();
        }

        using var reopened = StorageDatabase.Open(_dir, 102);
        Assert.False(reopened.TryGet("k"u8, out _));
        Assert.Equal(0, reopened.Count);
    }

    [Fact]
    public void Snapshot_is_stable_and_prefix_scan_is_ordered_and_bounded()
    {
        using var db = StorageDatabase.Open(_dir, 103);
        using (var batch = db.BeginWriteBatch())
        {
            batch.Put([0x10, 0x02], "b"u8);
            batch.Put([0x10, 0x01], "a"u8);
            batch.Put([0x20, 0x01], "other"u8);
            batch.Commit();
        }

        using var snapshot = db.OpenSnapshot();
        using (var update = db.BeginWriteBatch())
        {
            update.Put([0x10, 0x01], "changed"u8);
            update.Delete([0x10, 0x02]);
            update.Commit();
        }

        var page = snapshot.ScanPrefix([0x10], ReadOnlySpan<byte>.Empty, 1);
        var first = Assert.Single(page);
        Assert.Equal(new byte[] { 0x10, 0x01 }, first.Key);
        Assert.Equal("a"u8.ToArray(), first.Value);
        var next = snapshot.ScanPrefix([0x10], first.Key, 10);
        Assert.Equal(new byte[] { 0x10, 0x02 }, Assert.Single(next).Key);
        Assert.True(snapshot.TryGet([0x10, 0x02], out var stable));
        Assert.Equal("b"u8.ToArray(), stable);
    }

    [Fact]
    public void Partitions_are_isolated_even_when_keys_match()
    {
        using var one = StorageDatabase.Open(Path.Combine(_dir, "one"), 1);
        using var two = StorageDatabase.Open(Path.Combine(_dir, "two"), 2);
        using (var batch = one.BeginWriteBatch())
        {
            batch.Put("same"u8, "one"u8);
            batch.Commit();
        }

        Assert.True(one.TryGet("same"u8, out _));
        Assert.False(two.TryGet("same"u8, out _));
    }
}