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
    [Fact]
    public void Checkpoint_publishes_snapshot_before_truncating_wal_and_reopens()
    {
        using (var db = StorageDatabase.Open(_dir, 104, walRotateAfterBytes: 0))
        {
            using (var first = db.BeginWriteBatch())
            {
                first.Put("a"u8, "one"u8);
                first.Put("b"u8, "two"u8);
                first.Commit();
            }

            db.Checkpoint();
            Assert.Equal(1UL, db.LastCommitSeq);

            using (var second = db.BeginWriteBatch())
            {
                second.Put("a"u8, "updated"u8);
                second.Delete("b"u8);
                second.Put("c"u8, "three"u8);
                second.Commit();
            }
        }

        using var reopened = StorageDatabase.Open(_dir, 104, walRotateAfterBytes: 0);
        Assert.Equal(2UL, reopened.LastCommitSeq);
        Assert.True(reopened.TryGet("a"u8, out var a));
        Assert.Equal("updated"u8.ToArray(), a);
        Assert.False(reopened.TryGet("b"u8, out _));
        Assert.True(reopened.TryGet("c"u8, out var c));
        Assert.Equal("three"u8.ToArray(), c);
    }

    [Fact]
    public void Missing_snapshot_referenced_by_manifest_fails_closed()
    {
        using (var db = StorageDatabase.Open(_dir, 105))
        {
            using var batch = db.BeginWriteBatch();
            batch.Put("key"u8, "value"u8);
            batch.Commit();
            db.Checkpoint();
        }

        File.Delete(Path.Combine(_dir, "snapshots", "p_105.1.snap"));
        Assert.Throws<InvalidDataException>(() => StorageDatabase.Open(_dir, 105));
    }

    [Fact]
    public void Corrupt_snapshot_manifest_fails_closed()
    {
        using (var db = StorageDatabase.Open(_dir, 106))
        {
            using var batch = db.BeginWriteBatch();
            batch.Put("key"u8, "value"u8);
            batch.Commit();
            db.Checkpoint();
        }

        string manifest = Path.Combine(_dir, "snapshots", "p_106.manifest");
        byte[] bytes = File.ReadAllBytes(manifest);
        bytes[16] ^= 0x40;
        File.WriteAllBytes(manifest, bytes);
        Assert.Throws<InvalidDataException>(() => StorageDatabase.Open(_dir, 106));
    }

    [Fact]
    public void Repeated_checkpoints_keep_snapshot_and_wal_sequence_domains_contiguous()
    {
        using (var db = StorageDatabase.Open(_dir, 107, walRotateAfterBytes: 0))
        {
            for (byte i = 1; i <= 3; i++)
            {
                using var batch = db.BeginWriteBatch();
                batch.Put("value"u8, new[] { i });
                batch.Commit();
                db.Checkpoint();
            }
        }

        using var reopened = StorageDatabase.Open(_dir, 107, walRotateAfterBytes: 0);
        Assert.Equal(3UL, reopened.LastCommitSeq);
        Assert.True(reopened.TryGet("value"u8, out var value));
        Assert.Equal(new byte[] { 3 }, value);
    }

    [Fact]
    public void Write_batch_provides_read_your_writes_and_serializable_conflict_detection()
    {
        using var db = StorageDatabase.Open(_dir, 108);
        using var first = db.BeginWriteBatch();
        using var competing = db.BeginWriteBatch();

        Assert.False(first.TryGet("key"u8, out _));
        first.Put("key"u8, "staged"u8);
        Assert.True(first.TryGet("key"u8, out var staged));
        Assert.Equal("staged"u8.ToArray(), staged);

        using (var winner = db.BeginWriteBatch())
        {
            winner.Put("other"u8, "committed"u8);
            winner.Commit();
        }

        Assert.Throws<StorageConflictException>(() => first.Commit());
        competing.Put("another"u8, "value"u8);
        Assert.Throws<StorageConflictException>(() => competing.Commit());
        Assert.False(db.TryGet("key"u8, out _));
        Assert.True(db.TryGet("other"u8, out var other));
        Assert.Equal("committed"u8.ToArray(), other);
    }

}
