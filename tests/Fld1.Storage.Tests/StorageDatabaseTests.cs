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
    public void BTree_root_recovery_replays_durable_WAL_when_page_flush_precedes_manifest_publish()
    {
        const ulong partition = 114;
        string directory = Path.Combine(_dir, "btree-before-root");
        bool armed = false;

        using (var db = StorageDatabase.OpenForTesting(directory, partition, point =>
        {
            if (armed && point == StorageFaultPoint.AfterBTreePageFlushBeforeManifest)
                throw new IOException("Injected failure after B+ tree page flush.");
        }))
        {
            armed = true;
            using var batch = db.BeginWriteBatch();
            batch.Put("durable-key"u8, "durable-value"u8);
            Assert.Throws<IOException>(() => batch.Commit());
            Assert.Throws<InvalidOperationException>(() => db.TryGet("durable-key"u8, out _));
        }

        using (var recovered = StorageDatabase.Open(directory, partition))
        {
            Assert.Equal(1UL, recovered.LastCommitSeq);
            Assert.True(recovered.TryGet("durable-key"u8, out var value));
            Assert.Equal("durable-value"u8.ToArray(), value);
        }

        StorageBTreeManifestV2 root = StorageBTreeRootPublisher.TryLoad(directory, partition)!.Value;
        Assert.Equal(1UL, root.CommitSeq);
    }

    [Fact]
    public void BTree_root_recovery_uses_published_root_when_failure_occurs_after_manifest_replace()
    {
        const ulong partition = 115;
        string directory = Path.Combine(_dir, "btree-after-root");
        bool armed = false;

        using (var db = StorageDatabase.OpenForTesting(directory, partition, point =>
        {
            if (armed && point == StorageFaultPoint.AfterBTreeManifestReplace)
                throw new IOException("Injected failure after B+ tree root replacement.");
        }))
        {
            armed = true;
            using var batch = db.BeginWriteBatch();
            batch.Put("published-key"u8, "published-value"u8);
            Assert.Throws<IOException>(() => batch.Commit());
            Assert.Throws<InvalidOperationException>(() => db.TryGet("published-key"u8, out _));
        }

        using (var recovered = StorageDatabase.Open(directory, partition))
        {
            Assert.Equal(1UL, recovered.LastCommitSeq);
            Assert.True(recovered.TryGet("published-key"u8, out var value));
            Assert.Equal("published-value"u8.ToArray(), value);
        }

        Assert.Equal(1UL, StorageBTreeRootPublisher.TryLoad(directory, partition)!.Value.CommitSeq);
    }

    [Fact]
    public void Checkpoint_compaction_preserves_BTree_tombstones()
    {
        const ulong partition = 116;
        string directory = Path.Combine(_dir, "btree-tombstone-compaction");
        using (var db = StorageDatabase.Open(directory, partition))
        {
            using (var put = db.BeginWriteBatch())
            {
                put.Put("deleted-key"u8, "old-value"u8);
                put.Commit();
            }

            using (var delete = db.BeginWriteBatch())
            {
                delete.Delete("deleted-key"u8);
                delete.Commit();
            }

            db.Checkpoint();
            Assert.Equal(0, db.Count);
        }

        StorageBTreeManifestV2 root = StorageBTreeRootPublisher.TryLoad(directory, partition)!.Value;
        using (var files = new StorageBTreeGenerationFiles(directory, partition, root.Generation))
        {
            var reader = new StorageBTreeReader(
                root.Generation,
                root.RootPageId,
                pageId => files.ReadPage(pageId, root.CommittedPageFileLength),
                key => files.ReadData(key.Offset, key.Length, root.CommittedDataFileLength));
            using var cursor = reader.OpenAllCursor(includeTombstones: true);
            StorageTreeEntry tombstone = Assert.Single(ReadAll(cursor));
            Assert.True(tombstone.Value.IsTombstone);
            Assert.Equal("deleted-key",
                System.Text.Encoding.UTF8.GetString(files.ReadData(
                    tombstone.Key.Offset, tombstone.Key.Length, root.CommittedDataFileLength)));
        }

        using var reopened = StorageDatabase.Open(directory, partition);
        Assert.Equal(0, reopened.Count);
        Assert.False(reopened.TryGet("deleted-key"u8, out _));

        static List<StorageTreeEntry> ReadAll(StorageBTreePrefixCursor cursor)
        {
            var entries = new List<StorageTreeEntry>();
            while (cursor.TryReadNext(out StorageTreeEntry entry))
                entries.Add(entry);
            return entries;
        }
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
    public void Prefix_cursor_reuses_borrowed_buffers_and_read_pages_remain_owned()
    {
        using var db = StorageDatabase.Open(_dir, 130);
        using (var batch = db.BeginWriteBatch())
        {
            batch.Put([0x10, 0x01], new byte[96].Select((_, i) => (byte)i).ToArray());
            batch.Put([0x10, 0x02], new byte[96].Select((_, i) => (byte)(255 - i)).ToArray());
            batch.Put([0x10, 0x03], "third"u8);
            batch.Commit();
        }

        using var snapshot = db.OpenSnapshot();
        using var cursor = snapshot.OpenPrefixCursor([0x10]);

        IReadOnlyList<StorageEntry> firstPage = cursor.ReadPage(1);
        byte[] firstKey = firstPage[0].Key.ToArray();
        byte[] firstValue = firstPage[0].Value.ToArray();
        IReadOnlyList<StorageEntry> secondPage = cursor.ReadPage(1);
        Assert.Equal(new byte[] { 0x10, 0x01 }, firstPage[0].Key);
        Assert.Equal(firstValue, firstPage[0].Value);
        Assert.Equal(new byte[] { 0x10, 0x02 }, secondPage[0].Key);

        var visitedKeys = new List<byte[]>();
        int visited = cursor.VisitPage(8, (in StorageEntryView entry) =>
        {
            visitedKeys.Add(entry.Key.ToArray());
            return true;
        });
        Assert.Equal(1, visited);
        Assert.Equal(new byte[] { 0x10, 0x03 }, Assert.Single(visitedKeys));
        Assert.Equal(new byte[] { 0x10, 0x01 }, firstKey);
    }

    [Fact]
    public void Prefix_cursor_stops_after_visitor_rejects_current_entry()
    {
        using var db = StorageDatabase.Open(_dir, 131);
        using (var batch = db.BeginWriteBatch())
        {
            batch.Put([0x20, 0x01], "one"u8);
            batch.Put([0x20, 0x02], "two"u8);
            batch.Commit();
        }

        using var snapshot = db.OpenSnapshot();
        using var cursor = snapshot.OpenPrefixCursor([0x20]);
        int calls = 0;
        int visited = cursor.VisitPage(10, (in StorageEntryView _) =>
        {
            calls++;
            return false;
        });
        Assert.Equal(1, visited);
        Assert.Equal(1, calls);
        Assert.Equal(new byte[] { 0x20, 0x02 }, Assert.Single(cursor.ReadPage(1)).Key);
    }

    [Fact]
    public void Snapshot_pins_old_BTree_generation_until_disposed_after_checkpoint()
    {
        const ulong partition = 118;
        string directory = Path.Combine(_dir, "btree-snapshot-generation-pin");
        using (var seedDatabase = StorageDatabase.Open(directory, partition))
        {
            using var seed = seedDatabase.BeginWriteBatch();
            seed.Put("stable"u8, "before"u8);
            seed.Commit();
        }

        StorageBTreeManifestV2 oldRoot = StorageBTreeRootPublisher.TryLoad(directory, partition)!.Value;
        using var db = StorageDatabase.Open(directory, partition);
        var snapshot = db.OpenSnapshot();
        using (var update = db.BeginWriteBatch())
        {
            update.Put("stable"u8, "after"u8);
            update.Commit();
        }

        db.Checkpoint();
        StorageBTreeManifestV2 newRoot = StorageBTreeManifestCodec.Decode(
            File.ReadAllBytes(Path.Combine(directory, "snapshots", $"p_{partition}.root-v2.manifest")));
        Assert.NotEqual(oldRoot.Generation, newRoot.Generation);
        Assert.True(snapshot.TryGet("stable"u8, out var oldValue));
        Assert.Equal("before"u8.ToArray(), oldValue);
        string oldPages = Path.Combine(directory, "snapshots", $"p_{partition}.{oldRoot.Generation}.pages");
        Assert.True(File.Exists(oldPages));

        snapshot.Dispose();
        Assert.False(File.Exists(oldPages));
        Assert.True(db.TryGet("stable"u8, out var currentValue));
        Assert.Equal("after"u8.ToArray(), currentValue);
    }

    [Fact]
    public void Database_point_reads_follow_BTree_after_multileaf_COW_and_checkpoint()
    {
        const ulong partition = 117;
        string directory = Path.Combine(_dir, "btree-point-reads");
        using (var db = StorageDatabase.Open(directory, partition))
        {
            using (var seed = db.BeginWriteBatch())
            {
                for (int i = 0; i < 600; i++)
                    seed.Put(System.Text.Encoding.UTF8.GetBytes($"key-{i:D4}"),
                        System.Text.Encoding.UTF8.GetBytes($"value-{i:D4}"));
                seed.Commit();
            }

            foreach (int i in new[] { 0, 31, 127, 255, 511, 599 })
            {
                Assert.True(db.TryGet(System.Text.Encoding.UTF8.GetBytes($"key-{i:D4}"), out var value));
                Assert.Equal(System.Text.Encoding.UTF8.GetBytes($"value-{i:D4}"), value);
            }

            using (var update = db.BeginWriteBatch())
            {
                for (int i = 0; i < 600; i += 37)
                    update.Put(System.Text.Encoding.UTF8.GetBytes($"key-{i:D4}"),
                        System.Text.Encoding.UTF8.GetBytes($"updated-{i:D4}"));
                for (int i = 10; i < 600; i += 41)
                    update.Delete(System.Text.Encoding.UTF8.GetBytes($"key-{i:D4}"));
                update.Commit();
            }

            Assert.True(db.TryGet("key-0000"u8, out var changed));
            Assert.Equal("updated-0000"u8.ToArray(), changed);
            Assert.False(db.TryGet("key-0010"u8, out _));
            db.Checkpoint();
            Assert.True(db.TryGet("key-0592"u8, out var last));
            Assert.Equal("updated-0592"u8.ToArray(), last);
        }

        using var reopened = StorageDatabase.Open(directory, partition);
        Assert.True(reopened.TryGet("key-0000"u8, out var persisted));
        Assert.Equal("updated-0000"u8.ToArray(), persisted);
        Assert.False(reopened.TryGet("key-0010"u8, out _));
        Assert.True(reopened.TryGet("key-0592"u8, out var final));
        Assert.Equal("updated-0592"u8.ToArray(), final);
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
    public void Missing_BTree_generation_file_referenced_by_root_fails_closed()
    {
        const ulong partition = 105;
        using (var db = StorageDatabase.Open(_dir, partition))
        {
            using var batch = db.BeginWriteBatch();
            batch.Put("key"u8, "value"u8);
            batch.Commit();
            db.Checkpoint();
        }

        StorageBTreeManifestV2 root = StorageBTreeRootPublisher.TryLoad(_dir, partition)!.Value;
        File.Delete(Path.Combine(_dir, "snapshots", $"p_{partition}.{root.Generation}.pages"));
        Assert.Throws<InvalidDataException>(() => StorageDatabase.Open(_dir, partition));
    }

    [Fact]
    public void BTree_checkpoint_does_not_write_duplicate_legacy_snapshot()
    {
        using (var db = StorageDatabase.Open(_dir, 106))
        {
            using var batch = db.BeginWriteBatch();
            batch.Put("key"u8, "value"u8);
            batch.Commit();
            db.Checkpoint();
        }

        Assert.False(File.Exists(Path.Combine(_dir, "snapshots", "p_106.manifest")));
        Assert.Empty(Directory.GetFiles(Path.Combine(_dir, "snapshots"), "p_106.*.snap"));
        using var reopened = StorageDatabase.Open(_dir, 106);
        Assert.True(reopened.TryGet("key"u8, out var value));
        Assert.Equal("value"u8.ToArray(), value);
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

        Assert.Single(Directory.GetFiles(Path.Combine(_dir, "snapshots"), "p_107.*.pages"));
        Assert.Empty(Directory.GetFiles(Path.Combine(_dir, "snapshots"), "p_107.*.snap"));
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

    [Fact]
    public void Wal_writer_fence_rejects_same_partition_and_releases_from_another_thread()
    {
        var first = StorageDatabase.Open(_dir, 109);
        try
        {
            Assert.Throws<IOException>(() => StorageDatabase.Open(_dir, 109));

            Exception? disposeError = null;
            var disposer = new Thread(() =>
            {
                try { first.Dispose(); }
                catch (Exception ex) { disposeError = ex; }
            });
            disposer.Start();
            Assert.True(disposer.Join(TimeSpan.FromSeconds(5)));
            Assert.Null(disposeError);

            using var reopened = StorageDatabase.Open(_dir, 109);
            using var batch = reopened.BeginWriteBatch();
            batch.Put("fenced"u8, "reopened"u8);
            Assert.Equal(1UL, batch.Commit());
        }
        finally
        {
            first.Dispose();
        }
    }

    [Fact]
    public void Wal_writer_fence_recovers_abandoned_mutex_before_wal_recovery()
    {
        // This scenario abandons a named Mutex by terminating its owning managed thread.
        // Unix named-Mutex ownership is process-scoped, so thread exit does not abandon it there.
        // A separate child-process harness is required to exercise process-death recovery on Unix.
        if (!OperatingSystem.IsWindows())
            return;

        const ulong partitionId = 110;
        string walDirectory = Path.Combine(_dir, "wal");
        Directory.CreateDirectory(walDirectory);
        string fullPath = Path.GetFullPath(walDirectory);
        if (OperatingSystem.IsWindows() || OperatingSystem.IsMacOS())
            fullPath = fullPath.ToUpperInvariant();
        string identity = fullPath + "|" +
            partitionId.ToString(System.Globalization.CultureInfo.InvariantCulture);
        string digest = Convert.ToHexString(
            System.Security.Cryptography.SHA256.HashData(System.Text.Encoding.UTF8.GetBytes(identity)));
        string mutexName = OperatingSystem.IsWindows()
            ? "Global\\Fld1Storage_" + digest
            : "Fld1Storage_" + digest;

        using var abandoned = new Mutex(initiallyOwned: false, mutexName);
        var owner = new Thread(() => abandoned.WaitOne());
        owner.Start();
        Assert.True(owner.Join(TimeSpan.FromSeconds(5)));

        using var recovered = StorageDatabase.Open(_dir, partitionId);
        using var batch = recovered.BeginWriteBatch();
        batch.Put("after-abandon"u8, "recovered"u8);
        Assert.Equal(1UL, batch.Commit());
    }

    [Fact]
    public void Wal_writer_fence_rejects_symlink_alias_of_same_partition()
    {
        if (OperatingSystem.IsWindows())
            return; // Symlink creation may require Developer Mode or elevated privileges.

        string physical = Path.Combine(_dir, "physical-wal-directory");
        string alias = Path.Combine(_dir, "wal-directory-alias");
        Directory.CreateDirectory(physical);
        Directory.CreateSymbolicLink(alias, physical);
        Assert.NotNull(new DirectoryInfo(alias).LinkTarget);
        Assert.NotNull(new DirectoryInfo(alias).ResolveLinkTarget(returnFinalTarget: true));

        using var first = StorageDatabase.Open(physical, 111);
        Assert.Throws<IOException>(() => StorageDatabase.Open(alias, 111));
    }

    [Fact]
    public void Wal_writer_fence_rejects_case_alias_on_case_insensitive_filesystems()
    {
        string directory = Path.Combine(_dir, "case-alias-wal-directory");
        Directory.CreateDirectory(directory);
        string alias = directory.ToUpperInvariant();
        if (!Directory.Exists(alias) || string.Equals(directory, alias, StringComparison.Ordinal))
            return;

        using var first = StorageDatabase.Open(directory, 112);
        Assert.Throws<IOException>(() => StorageDatabase.Open(alias, 112));
    }

    [Fact]
    public void Checkpoint_failure_after_root_publish_requires_reopen_and_recovers()
    {
        const ulong partitionId = 113;
        string directory = Path.Combine(_dir, "checkpoint-publication-failure");
        bool armed = false;

        using (var database = StorageDatabase.OpenForTesting(directory, partitionId, point =>
        {
            if (armed && point == StorageFaultPoint.AfterBTreeManifestReplace)
                throw new IOException("Injected failure after checkpoint root publication.");
        }))
        {
            using (var batch = database.BeginWriteBatch())
            {
                batch.Put("durable"u8, "wal-value"u8);
                batch.Commit();
            }

            armed = true;
            Assert.Throws<IOException>(() => database.Checkpoint());
            Assert.Throws<InvalidOperationException>(() => database.TryGet("durable"u8, out _));
        }

        using var recovered = StorageDatabase.Open(directory, partitionId);
        Assert.Equal(1UL, recovered.LastCommitSeq);
        Assert.True(recovered.TryGet("durable"u8, out var value));
        Assert.Equal("wal-value"u8.ToArray(), value);
    }

    [Fact]
    public void Large_checkpoint_streams_entries_and_reopens_without_materialized_input_array()
    {
        const ulong partition = 118;
        const int count = 20_000;
        string directory = Path.Combine(_dir, "streamed-large-checkpoint");

        using (var db = StorageDatabase.Open(directory, partition))
        {
            for (int start = 0; start < count; start += 1_000)
            {
                using var batch = db.BeginWriteBatch();
                int end = Math.Min(count, start + 1_000);
                for (int i = start; i < end; i++)
                {
                    byte[] key = System.Text.Encoding.ASCII.GetBytes($"key-{i:D8}");
                    byte[] value = BitConverter.GetBytes(i);
                    batch.Put(key, value);
                }
                batch.Commit();
            }

            Assert.Equal(count, db.Count);
            db.Checkpoint();
            Assert.Equal(count, db.Count);
            Assert.True(db.TryGet("key-00000000"u8, out byte[]? first));
            Assert.Equal(BitConverter.GetBytes(0), first);
            Assert.True(db.TryGet("key-00019999"u8, out byte[]? last));
            Assert.Equal(BitConverter.GetBytes(count - 1), last);
        }

        using var reopened = StorageDatabase.Open(directory, partition);
        Assert.Equal(count, reopened.Count);
        Assert.True(reopened.TryGet("key-00010000"u8, out byte[]? middle));
        Assert.Equal(BitConverter.GetBytes(10_000), middle);
    }

    [Fact]
    public void Recovery_keeps_live_count_unknown_until_exact_recount()
    {
        const ulong partition = 122;
        string directory = Path.Combine(_dir, "recovery-lazy-live-count");
        bool armed = false;

        using (var database = StorageDatabase.OpenForTesting(directory, partition, point =>
        {
            if (armed && point == StorageFaultPoint.AfterWalFlushBeforePublish)
                throw new IOException("Injected failure after durable WAL flush.");
        }))
        {
            using var batch = database.BeginWriteBatch();
            for (int i = 0; i < 512; i++)
                batch.Put(System.Text.Encoding.ASCII.GetBytes($"recovered-{i:D4}"), BitConverter.GetBytes(i));

            armed = true;
            Assert.Throws<IOException>(() => batch.Commit());
        }

        using var recovered = StorageDatabase.Open(directory, partition);
        Assert.Equal(512, recovered.Count);
        Assert.True(recovered.TryGet("recovered-0000"u8, out byte[]? first));
        Assert.Equal(BitConverter.GetBytes(0), first);
        Assert.True(recovered.TryGet("recovered-0511"u8, out byte[]? last));
        Assert.Equal(BitConverter.GetBytes(511), last);
    }

    [Fact]
    public void Small_commit_keeps_unknown_live_count_lazy_until_count_is_requested()
    {
        const ulong partition = 121;
        string directory = Path.Combine(_dir, "lazy-live-count-after-large-batch");
        int recounts = 0;
        using (var database = StorageDatabase.Open(directory, partition))
        {
            database.AllocationObserver = (phase, _) =>
            {
                if (phase == "storage-live-count-recount")
                    recounts++;
            };

            using (var seed = database.BeginWriteBatch())
            {
                for (int i = 0; i < 512; i++)
                    seed.Put(System.Text.Encoding.ASCII.GetBytes($"key-{i:D4}"), BitConverter.GetBytes(i));
                seed.Commit();
            }

            using (var update = database.BeginWriteBatch())
            {
                update.Delete("key-0000"u8);
                update.Put("key-0512"u8, "new"u8);
                update.Commit();
            }

            Assert.Equal(0, recounts);
            Assert.Equal(512, database.Count);
            Assert.Equal(1, recounts);
            Assert.Equal(512, database.Count);
            Assert.Equal(1, recounts);
            database.AllocationObserver = null;
        }

        using var reopened = StorageDatabase.Open(directory, partition);
        Assert.Equal(512, reopened.Count);
        Assert.False(reopened.TryGet("key-0000"u8, out _));
        Assert.True(reopened.TryGet("key-0512"u8, out byte[]? value));
        Assert.Equal("new"u8.ToArray(), value);
    }

    [Fact]
    public void Page_cache_has_a_small_default_and_an_explicit_memory_budget()
    {
        using var defaultDatabase = StorageDatabase.Open(
            Path.Combine(_dir, "page-cache-default"), 119);
        Assert.Equal(2L * 1024 * 1024, defaultDatabase.PageCacheCapacityBytes);

        long customBudget = StoragePageCodec.PageSize * 2L;
        using var customDatabase = StorageDatabase.Open(
            Path.Combine(_dir, "page-cache-custom"), 120,
            pageCacheCapacityBytes: customBudget);
        Assert.Equal(customBudget, customDatabase.PageCacheCapacityBytes);
    }

}
