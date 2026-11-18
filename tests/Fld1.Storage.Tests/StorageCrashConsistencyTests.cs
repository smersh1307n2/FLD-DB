using Fld1.Storage.Engine;
using Fld1.Storage.Wal.Format;
using Xunit;

namespace Fld1.Storage.Tests;

/// <summary>
/// Exercises real on-disk crash states rather than only mocking storage failures.
/// The tests construct the durable file layout that can remain after interruption
/// at WAL append and snapshot/manifest publication boundaries.
/// </summary>
public sealed class StorageCrashConsistencyTests
{
    private readonly string _root = Path.Combine(
        Path.GetTempPath(), "binsync-storage-crash-" + Guid.NewGuid().ToString("N"));

    [Fact]
    public void Final_torn_wal_tail_is_truncated_and_database_can_append_again()
    {
        const ulong partitionId = 901;
        string directory = Path.Combine(_root, "final-torn-tail");
        Directory.CreateDirectory(directory);
        string walPath = Path.Combine(directory, "wal", $"p_{partitionId}.0.log");

        try
        {
            using (var database = StorageDatabase.Open(directory, partitionId))
                Put(database, 0x01, 0x11);

            long validLength = new FileInfo(walPath).Length;
            using (var stream = new FileStream(walPath, FileMode.Append, FileAccess.Write, FileShare.None))
            {
                stream.Write(new byte[] { 0xD1, 0xD2, 0xD3, 0xD4, 0xD5, 0xD6, 0xD7 });
                stream.Flush(flushToDisk: true);
            }

            using (var recovered = StorageDatabase.Open(directory, partitionId))
            {
                Assert.Equal(1UL, recovered.LastCommitSeq);
                Assert.True(recovered.TryGet(new byte[] { 0x01 }, out var first));
                Assert.Equal(new byte[] { 0x11 }, first);
                Assert.Equal(validLength, new FileInfo(walPath).Length);

                Put(recovered, 0x02, 0x22);
            }

            using var reopened = StorageDatabase.Open(directory, partitionId);
            Assert.Equal(2UL, reopened.LastCommitSeq);
            Assert.True(reopened.TryGet(new byte[] { 0x01 }, out var firstAfterReopen));
            Assert.Equal(new byte[] { 0x11 }, firstAfterReopen);
            Assert.True(reopened.TryGet(new byte[] { 0x02 }, out var secondAfterReopen));
            Assert.Equal(new byte[] { 0x22 }, secondAfterReopen);
        }
        finally
        {
            TryDeleteDirectory(directory);
        }
    }

    [Fact]
    public void Complete_wal_record_crc_corruption_fails_closed_without_truncation()
    {
        const ulong partitionId = 902;
        string directory = Path.Combine(_root, "corrupt-record");
        Directory.CreateDirectory(directory);
        string walPath = Path.Combine(directory, "wal", $"p_{partitionId}.0.log");

        try
        {
            using (var database = StorageDatabase.Open(directory, partitionId))
                Put(database, 0x03, 0x33);

            byte[] bytes = File.ReadAllBytes(walPath);
            Assert.True(bytes.Length > WalHeader.Size + WalEnvelope.Size + 4);
            bytes[WalHeader.Size + WalEnvelope.Size + 4] ^= 0x80;
            File.WriteAllBytes(walPath, bytes);

            long corruptLength = new FileInfo(walPath).Length;
            Assert.Throws<InvalidDataException>(() => StorageDatabase.Open(directory, partitionId));
            Assert.Equal(corruptLength, new FileInfo(walPath).Length);
        }
        finally
        {
            TryDeleteDirectory(directory);
        }
    }

    [Fact]
    public void Torn_tail_in_non_final_wal_segment_fails_closed()
    {
        const ulong partitionId = 903;
        string directory = Path.Combine(_root, "non-final-torn-tail");
        Directory.CreateDirectory(directory);
        string firstSegment = Path.Combine(directory, "wal", $"p_{partitionId}.0.log");

        try
        {
            using (var database = StorageDatabase.Open(directory, partitionId, walRotateAfterBytes: 1))
            {
                Put(database, 0x04, 0x44);
                Put(database, 0x05, 0x55);
            }

            using (var stream = new FileStream(firstSegment, FileMode.Append, FileAccess.Write, FileShare.None))
            {
                stream.Write(new byte[] { 1, 2, 3, 4, 5, 6, 7 });
                stream.Flush(flushToDisk: true);
            }

            Assert.Throws<InvalidDataException>(() => StorageDatabase.Open(directory, partitionId));
        }
        finally
        {
            TryDeleteDirectory(directory);
        }
    }


    [Fact]
    public void Orphaned_wal_rotation_temp_is_ignored_on_open_and_replaced_on_next_rotation()
    {
        const ulong partitionId = 906;
        string directory = Path.Combine(_root, "orphaned-wal-rotation-temp");
        string walDirectory = Path.Combine(directory, "wal");
        string orphanTemp = Path.Combine(walDirectory, $"p_{partitionId}.2.log.tmp");
        string nextSegment = Path.Combine(walDirectory, $"p_{partitionId}.2.log");

        try
        {
            using (var database = StorageDatabase.Open(directory, partitionId, walRotateAfterBytes: 1))
                Put(database, 0x09, 0x99);

            File.WriteAllBytes(orphanTemp, new byte[] { 0xFA, 0xFB, 0xFC });

            using (var recovered = StorageDatabase.Open(directory, partitionId, walRotateAfterBytes: 1))
            {
                Assert.Equal(1UL, recovered.LastCommitSeq);
                Put(recovered, 0x0A, 0xAA);
                Assert.Equal(2, recovered.ActiveWalSegment);
            }

            Assert.False(File.Exists(orphanTemp));
            Assert.True(File.Exists(nextSegment),
                $"Expected {nextSegment}; actual WAL files: {string.Join(", ", Directory.GetFiles(walDirectory).Select(Path.GetFileName))}");

            using var reopened = StorageDatabase.Open(directory, partitionId, walRotateAfterBytes: 1);
            Assert.Equal(2UL, reopened.LastCommitSeq);
            Assert.True(reopened.TryGet(new byte[] { 0x09 }, out _));
            Assert.True(reopened.TryGet(new byte[] { 0x0A }, out _));
        }
        finally
        {
            TryDeleteDirectory(directory);
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void BTree_checkpoint_recovers_across_root_publication_crash_window(bool newRootWasPublished)
    {
        const ulong partitionId = 904;
        string directory = Path.Combine(_root, newRootWasPublished ? "after-btree-root" : "before-btree-root");
        bool armed = false;
        StorageFaultPoint faultPoint = newRootWasPublished
            ? StorageFaultPoint.AfterBTreeManifestReplace
            : StorageFaultPoint.AfterBTreeManifestFileFlushBeforeReplace;

        try
        {
            using (var seedDatabase = StorageDatabase.Open(directory, partitionId, walRotateAfterBytes: 0))
            {
                Put(seedDatabase, 0x06, 0x66);
                Put(seedDatabase, 0x07, 0x77);
            }

            StorageBTreeManifestV2 oldRoot = StorageBTreeRootPublisher.TryLoad(directory, partitionId)!.Value;
            using (var database = StorageDatabase.OpenForTesting(directory, partitionId, point =>
            {
                if (armed && point == faultPoint)
                    throw new IOException("Injected B+ tree checkpoint root publication failure.");
            }, walRotateAfterBytes: 0))
            {
                Put(database, 0x08, 0x88);
                armed = true;
                Assert.Throws<IOException>(() => database.Checkpoint());
                Assert.Throws<InvalidOperationException>(() => database.TryGet(new byte[] { 0x06 }, out _));
            }

            StorageBTreeManifestV2 interruptedRoot = StorageBTreeRootPublisher.TryLoad(directory, partitionId)!.Value;
            Assert.Equal(newRootWasPublished, interruptedRoot.Generation != oldRoot.Generation);

            using (var recovered = StorageDatabase.Open(directory, partitionId, walRotateAfterBytes: 0))
            {
                Assert.Equal(3UL, recovered.LastCommitSeq);
                Assert.True(recovered.TryGet(new byte[] { 0x06 }, out var first));
                Assert.Equal(new byte[] { 0x66 }, first);
                Assert.True(recovered.TryGet(new byte[] { 0x07 }, out var second));
                Assert.Equal(new byte[] { 0x77 }, second);
                Assert.True(recovered.TryGet(new byte[] { 0x08 }, out var third));
                Assert.Equal(new byte[] { 0x88 }, third);
                Put(recovered, 0x09, 0x99);
                recovered.Checkpoint();
            }

            using var reopened = StorageDatabase.Open(directory, partitionId, walRotateAfterBytes: 0);
            Assert.Equal(4UL, reopened.LastCommitSeq);
            Assert.True(reopened.TryGet(new byte[] { 0x06 }, out _));
            Assert.True(reopened.TryGet(new byte[] { 0x07 }, out _));
            Assert.True(reopened.TryGet(new byte[] { 0x09 }, out _));
            Assert.Single(Directory.GetFiles(Path.Combine(directory, "snapshots"), $"p_{partitionId}.*.pages"));
        }
        finally
        {
            TryDeleteDirectory(directory);
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void BTree_root_manifest_or_referenced_generation_corruption_fails_closed(bool corruptManifest)
    {
        const ulong partitionId = 905;
        string directory = Path.Combine(_root, corruptManifest ? "corrupt-btree-root" : "missing-btree-generation");
        string snapshotsDirectory = Path.Combine(directory, "snapshots");
        string rootPath = Path.Combine(snapshotsDirectory, $"p_{partitionId}.root-v2.manifest");

        try
        {
            using (var database = StorageDatabase.Open(directory, partitionId))
                Put(database, 0x08, 0x88);

            StorageBTreeManifestV2 root = StorageBTreeRootPublisher.TryLoad(directory, partitionId)!.Value;
            if (corruptManifest)
            {
                byte[] manifest = File.ReadAllBytes(rootPath);
                manifest[^1] ^= 0x80;
                File.WriteAllBytes(rootPath, manifest);
            }
            else
            {
                File.Delete(Path.Combine(snapshotsDirectory, $"p_{partitionId}.{root.Generation}.pages"));
            }

            Assert.Throws<InvalidDataException>(() => StorageDatabase.Open(directory, partitionId));
        }
        finally
        {
            TryDeleteDirectory(directory);
        }
    }

    private static void Put(StorageDatabase database, byte key, byte value)
    {
        using var batch = database.BeginWriteBatch();
        batch.Put(new[] { key }, new[] { value });
        batch.Commit();
    }

    private static void CopyDirectory(string source, string destination)
    {
        Directory.CreateDirectory(destination);
        foreach (string file in Directory.EnumerateFiles(source))
            File.Copy(file, Path.Combine(destination, Path.GetFileName(file)), overwrite: true);
    }

    private static void TryDeleteDirectory(string path)
    {
        try
        {
            if (Directory.Exists(path))
                Directory.Delete(path, recursive: true);
        }
        catch
        {
            // Best-effort cleanup keeps the primary assertion failure visible.
        }
    }
    [Fact]
    public void Checkpoint_failure_after_BTree_root_publish_requires_reopen()
    {
        const ulong partitionId = 907;
        string directory = Path.Combine(_root, "checkpoint-publication-failure");
        bool armed = false;

        try
        {
            using (var database = StorageDatabase.OpenForTesting(directory, partitionId, point =>
            {
                if (armed && point == StorageFaultPoint.AfterBTreeManifestReplace)
                    throw new IOException("Injected failure after B+ tree root replacement.");
            }))
            {
                Put(database, 0x12, 0x34);
                armed = true;
                Assert.Throws<IOException>(() => database.Checkpoint());
                Assert.Throws<InvalidOperationException>(() => database.TryGet(new byte[] { 0x12 }, out _));
            }

            using var recovered = StorageDatabase.Open(directory, partitionId);
            Assert.Equal(1UL, recovered.LastCommitSeq);
            Assert.True(recovered.TryGet(new byte[] { 0x12 }, out var value));
            Assert.Equal(new byte[] { 0x34 }, value);
        }
        finally
        {
            TryDeleteDirectory(directory);
        }
    }

    [Fact]
    public void First_open_durably_creates_nested_database_directories()
    {
        const ulong partitionId = 925;
        string directory = Path.Combine(_root, "new-parent", "new-database");

        try
        {
            using (var database = StorageDatabase.Open(directory, partitionId))
                Put(database, 0x51, 0x15);

            using var reopened = StorageDatabase.Open(directory, partitionId);
            Assert.Equal(1UL, reopened.LastCommitSeq);
            Assert.True(reopened.TryGet(new byte[] { 0x51 }, out var value));
            Assert.Equal(new byte[] { 0x15 }, value);
        }
        finally
        {
            TryDeleteDirectory(_root);
        }
    }

    [Fact]
    public void Unix_directory_fsync_rejects_a_non_directory_path()
    {
        if (OperatingSystem.IsWindows())
            return; // Windows directory-entry durability is not claimed by this engine.

        string directory = Path.Combine(_root, "directory-fsync-error");
        Directory.CreateDirectory(directory);
        string regularFile = Path.Combine(directory, "not-a-directory");
        File.WriteAllBytes(regularFile, new byte[] { 0x01 });

        IOException error = Assert.Throws<IOException>(() => StorageDirectorySync.Flush(regularFile));
        Assert.Contains("directory", error.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Wal_directory_fsync_failure_after_segment_reclamation_requires_reopen()
    {
        const ulong partitionId = 924;
        string directory = Path.Combine(_root, "wal-directory-fsync-failure");
        int directorySyncCount = 0;

        try
        {
            using (var database = StorageDatabase.OpenForTesting(directory, partitionId, point =>
            {
                if (point == StorageFaultPoint.BeforeWalDirectoryFsync &&
                    Interlocked.Increment(ref directorySyncCount) == 2)
                    throw new IOException("Injected WAL directory fsync failure after reclamation.");
            }))
            {
                Put(database, 0x42, 0x24);
                Assert.Throws<IOException>(() => database.Checkpoint());
                Assert.Throws<InvalidOperationException>(() => database.TryGet(new byte[] { 0x42 }, out _));
            }

            using var recovered = StorageDatabase.Open(directory, partitionId);
            Assert.Equal(1UL, recovered.LastCommitSeq);
            Assert.True(recovered.TryGet(new byte[] { 0x42 }, out var value));
            Assert.Equal(new byte[] { 0x24 }, value);
        }
        finally
        {
            TryDeleteDirectory(directory);
        }
    }

    [Fact]
    public void Partial_wal_write_fails_closed_and_recovery_discards_the_torn_record()
    {
        const ulong partitionId = 922;
        string directory = Path.Combine(_root, "wal-partial-write");

        try
        {
            using (var database = StorageDatabase.OpenForTesting(directory, partitionId, point =>
            {
                if (point == StorageFaultPoint.AfterWalPartialWrite)
                    throw new IOException("Injected partial WAL write.");
            }, walRotateAfterBytes: long.MaxValue))
            {
                using var batch = database.BeginWriteBatch();
                batch.Put("partial-write-key"u8, "must-not-commit"u8);
                Assert.Throws<IOException>(() => batch.Commit());
                Assert.Throws<InvalidOperationException>(() => database.TryGet("partial-write-key"u8, out _));
            }

            using (var recovered = StorageDatabase.Open(directory, partitionId))
            {
                Assert.Equal(0UL, recovered.LastCommitSeq);
                Assert.False(recovered.TryGet("partial-write-key"u8, out _));
                using var batch = recovered.BeginWriteBatch();
                batch.Put("after-recovery"u8, "usable"u8);
                Assert.Equal(1UL, batch.Commit());
            }

            using var reopened = StorageDatabase.Open(directory, partitionId);
            Assert.Equal(1UL, reopened.LastCommitSeq);
            Assert.True(reopened.TryGet("after-recovery"u8, out var value));
            Assert.Equal("usable"u8.ToArray(), value);
        }
        finally
        {
            TryDeleteDirectory(directory);
        }
    }

    [Fact]
    public void Wal_fsync_failure_fails_closed_and_reopen_resolves_the_complete_record()
    {
        const ulong partitionId = 923;
        string directory = Path.Combine(_root, "wal-fsync-failure");

        try
        {
            using (var database = StorageDatabase.OpenForTesting(directory, partitionId, point =>
            {
                if (point == StorageFaultPoint.BeforeWalFsync)
                    throw new IOException("Injected WAL fsync failure.");
            }, walRotateAfterBytes: long.MaxValue))
            {
                using var batch = database.BeginWriteBatch();
                batch.Put("fsync-key"u8, "durable-boundary"u8);
                Assert.Throws<IOException>(() => batch.Commit());
                Assert.Throws<InvalidOperationException>(() => database.TryGet("fsync-key"u8, out _));
            }

            // The test simulates the fsync error while leaving the just-written bytes
            // visible to reopen. A real device/power-loss outcome is intentionally not
            // inferred from this process-level fault injection.
            using var recovered = StorageDatabase.Open(directory, partitionId);
            Assert.Equal(1UL, recovered.LastCommitSeq);
            Assert.True(recovered.TryGet("fsync-key"u8, out var value));
            Assert.Equal("durable-boundary"u8.ToArray(), value);
        }
        finally
        {
            TryDeleteDirectory(directory);
        }
    }

    [Theory]
    [InlineData((int)StorageFaultPoint.AfterWalAppendBeforeFlush)]
    [InlineData((int)StorageFaultPoint.AfterWalFlushBeforePublish)]
    public void Wal_commit_fault_points_require_reopen_and_recover_the_complete_commit(int faultPointValue)
    {
        StorageFaultPoint faultPoint = (StorageFaultPoint)faultPointValue;
        const ulong partitionId = 920;
        string directory = Path.Combine(_root, "wal-fault-" + faultPoint);

        try
        {
            using (var database = StorageDatabase.OpenForTesting(directory, partitionId, point =>
            {
                if (point == faultPoint)
                    throw new IOException("Injected WAL durability boundary failure.");
            }))
            {
                using var batch = database.BeginWriteBatch();
                batch.Put("fault-key"u8, "fault-value"u8);
                Assert.Throws<IOException>(() => batch.Commit());
                Assert.Throws<InvalidOperationException>(() => database.TryGet("fault-key"u8, out _));
            }

            using var recovered = StorageDatabase.Open(directory, partitionId);
            Assert.Equal(1UL, recovered.LastCommitSeq);
            Assert.True(recovered.TryGet("fault-key"u8, out var value));
            Assert.Equal("fault-value"u8.ToArray(), value);
        }
        finally
        {
            TryDeleteDirectory(directory);
        }
    }

    [Theory]
    [InlineData((int)StorageFaultPoint.AfterWalAppendBeforeFlush)]
    [InlineData((int)StorageFaultPoint.AfterWalFlushBeforePublish)]
    [InlineData((int)StorageFaultPoint.AfterBTreeDataFlush)]
    [InlineData((int)StorageFaultPoint.AfterBTreePageFlushBeforeManifest)]
    [InlineData((int)StorageFaultPoint.AfterBTreeManifestFileFlushBeforeReplace)]
    [InlineData((int)StorageFaultPoint.AfterBTreeManifestReplace)]
    [InlineData((int)StorageFaultPoint.BeforeBTreeManifestDirectoryFsync)]
    [InlineData((int)StorageFaultPoint.AfterBTreeManifestDirectoryFsync)]
    public void Multi_key_batch_is_all_or_none_across_wal_and_btree_publication_crash_points(
        int faultPointValue)
    {
        StorageFaultPoint faultPoint = (StorageFaultPoint)faultPointValue;
        const ulong partitionId = 926;
        string directory = Path.Combine(_root, "multi-key-commit-fault-" + faultPoint);

        try
        {
            using (var database = StorageDatabase.OpenForTesting(directory, partitionId, point =>
            {
                if (point == faultPoint)
                    throw new IOException("Injected multi-key StorageWriteBatch durability boundary failure.");
            }))
            {
                using var batch = database.BeginWriteBatch();
                batch.Put("operator-policy"u8, "generation=1;enabled=true"u8);
                batch.Put("operator-audit"u8, "sequence=1;generation=1"u8);
                Assert.Throws<IOException>(() => batch.Commit());
                Assert.Throws<InvalidOperationException>(() =>
                    database.TryGet("operator-policy"u8, out _));
            }

            using (var recovered = StorageDatabase.Open(directory, partitionId))
            {
                Assert.Equal(1UL, recovered.LastCommitSeq);
                Assert.True(recovered.TryGet("operator-policy"u8, out var policy));
                Assert.Equal("generation=1;enabled=true"u8.ToArray(), policy);
                Assert.True(recovered.TryGet("operator-audit"u8, out var audit));
                Assert.Equal("sequence=1;generation=1"u8.ToArray(), audit);
            }

            using var reopened = StorageDatabase.Open(directory, partitionId);
            Assert.Equal(1UL, reopened.LastCommitSeq);
            Assert.True(reopened.TryGet("operator-policy"u8, out _));
            Assert.True(reopened.TryGet("operator-audit"u8, out _));
        }
        finally
        {
            TryDeleteDirectory(directory);
        }
    }

    [Theory]
    [InlineData((int)StorageFaultPoint.AfterBTreeDataFlush)]
    [InlineData((int)StorageFaultPoint.AfterBTreePageFlushBeforeManifest)]
    [InlineData((int)StorageFaultPoint.AfterBTreeManifestFileFlushBeforeReplace)]
    [InlineData((int)StorageFaultPoint.AfterBTreeManifestReplace)]
    [InlineData((int)StorageFaultPoint.BeforeBTreeManifestDirectoryFsync)]
    [InlineData((int)StorageFaultPoint.AfterBTreeManifestDirectoryFsync)]
    public void BTree_checkpoint_fault_points_recover_and_allow_a_new_checkpoint(int faultPointValue)
    {
        StorageFaultPoint faultPoint = (StorageFaultPoint)faultPointValue;
        const ulong partitionId = 921;
        string directory = Path.Combine(_root, "checkpoint-fault-" + faultPoint);
        bool armed = false;

        try
        {
            using (var database = StorageDatabase.OpenForTesting(directory, partitionId, point =>
            {
                if (armed && point == faultPoint)
                    throw new IOException("Injected B+ tree checkpoint publication boundary failure.");
            }))
            {
                Put(database, 0x31, 0x41);
                armed = true;
                Assert.Throws<IOException>(() => database.Checkpoint());
                Assert.Throws<InvalidOperationException>(() => database.TryGet(new byte[] { 0x31 }, out _));
            }

            using (var recovered = StorageDatabase.Open(directory, partitionId))
            {
                Assert.Equal(1UL, recovered.LastCommitSeq);
                Assert.True(recovered.TryGet(new byte[] { 0x31 }, out var value));
                Assert.Equal(new byte[] { 0x41 }, value);
                Put(recovered, 0x32, 0x42);
                recovered.Checkpoint();
            }

            using var reopened = StorageDatabase.Open(directory, partitionId);
            Assert.Equal(2UL, reopened.LastCommitSeq);
            Assert.True(reopened.TryGet(new byte[] { 0x31 }, out var reopenedValue));
            Assert.Equal(new byte[] { 0x41 }, reopenedValue);
            Assert.True(reopened.TryGet(new byte[] { 0x32 }, out var secondValue));
            Assert.Equal(new byte[] { 0x42 }, secondValue);
        }
        finally
        {
            TryDeleteDirectory(directory);
        }
    }

}
