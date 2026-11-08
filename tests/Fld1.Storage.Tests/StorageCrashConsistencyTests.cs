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
            }

            Assert.False(File.Exists(orphanTemp));
            Assert.True(File.Exists(nextSegment));

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
    public void Snapshot_checkpoint_recovers_across_manifest_publication_crash_window(bool newManifestWasPublished)
    {
        const ulong partitionId = 904;
        string directory = Path.Combine(_root, newManifestWasPublished ? "after-manifest" : "before-manifest");
        string walDirectory = Path.Combine(directory, "wal");
        string snapshotsDirectory = Path.Combine(directory, "snapshots");
        string manifestPath = Path.Combine(snapshotsDirectory, $"p_{partitionId}.manifest");
        string firstSnapshotPath = Path.Combine(snapshotsDirectory, $"p_{partitionId}.1.snap");
        string secondSnapshotPath = Path.Combine(snapshotsDirectory, $"p_{partitionId}.2.snap");
        string walBeforeSecondCheckpoint = Path.Combine(_root, "wal-before-second-checkpoint-" + Guid.NewGuid().ToString("N"));
        string walAfterSecondCheckpoint = Path.Combine(_root, "wal-after-second-checkpoint-" + Guid.NewGuid().ToString("N"));

        Directory.CreateDirectory(directory);
        try
        {
            using (var database = StorageDatabase.Open(directory, partitionId))
            {
                Put(database, 0x06, 0x66);
                database.Checkpoint();
            }

            byte[] firstManifest = File.ReadAllBytes(manifestPath);
            byte[] firstSnapshot = File.ReadAllBytes(firstSnapshotPath);

            using (var database = StorageDatabase.Open(directory, partitionId))
            {
                Put(database, 0x07, 0x77);
                CopyDirectory(walDirectory, walBeforeSecondCheckpoint);
                database.Checkpoint();
                CopyDirectory(walDirectory, walAfterSecondCheckpoint);
            }

            // Reconstruct the WAL layout that exists after the new segment is
            // published, but before sealed-segment reclamation completes.
            Directory.Delete(walDirectory, recursive: true);
            CopyDirectory(walBeforeSecondCheckpoint, walDirectory);
            string newActiveSegment = Path.Combine(walAfterSecondCheckpoint, $"p_{partitionId}.2.log");
            Assert.True(File.Exists(newActiveSegment));
            File.Copy(newActiveSegment, Path.Combine(walDirectory, Path.GetFileName(newActiveSegment)));

            if (!newManifestWasPublished)
            {
                // Crash after the new immutable snapshot file was renamed into
                // place but before its manifest became the authoritative root.
                File.WriteAllBytes(manifestPath, firstManifest);
                File.WriteAllBytes(firstSnapshotPath, firstSnapshot);
                Assert.True(File.Exists(secondSnapshotPath)); // orphan generation is harmless
                File.WriteAllBytes(secondSnapshotPath + ".tmp", new byte[] { 0xFA, 0xFB, 0xFC });
                File.WriteAllBytes(manifestPath + ".tmp", new byte[] { 0xFD, 0xFE });
            }
            else
            {
                // Crash after manifest publication but before old WAL segments
                // were reclaimed. The snapshot is authoritative; old WAL commits
                // at or below its CommitSeq must be skipped during recovery.
                Assert.True(File.Exists(secondSnapshotPath));
            }

            using (var recovered = StorageDatabase.Open(directory, partitionId))
            {
                Assert.Equal(2UL, recovered.LastCommitSeq);
                Assert.True(recovered.TryGet(new byte[] { 0x06 }, out var first));
                Assert.Equal(new byte[] { 0x66 }, first);
                Assert.True(recovered.TryGet(new byte[] { 0x07 }, out var second));
                Assert.Equal(new byte[] { 0x77 }, second);

                if (!newManifestWasPublished)
                {
                    // The old manifest remains authoritative. Recovery removes the
                    // unreferenced generation so a subsequent checkpoint can reuse it.
                    Assert.False(File.Exists(secondSnapshotPath));
                    Assert.False(File.Exists(secondSnapshotPath + ".tmp"));
                    Assert.False(File.Exists(manifestPath + ".tmp"));
                    recovered.Checkpoint();
                }
            }

            using var reopened = StorageDatabase.Open(directory, partitionId);
            Assert.Equal(2UL, reopened.LastCommitSeq);
            Assert.True(reopened.TryGet(new byte[] { 0x06 }, out _));
            Assert.True(reopened.TryGet(new byte[] { 0x07 }, out _));
        }
        finally
        {
            TryDeleteDirectory(directory);
            TryDeleteDirectory(walBeforeSecondCheckpoint);
            TryDeleteDirectory(walAfterSecondCheckpoint);
        }
    }


    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void Snapshot_manifest_or_referenced_generation_corruption_fails_closed(bool corruptManifest)
    {
        const ulong partitionId = 905;
        string directory = Path.Combine(_root, corruptManifest ? "corrupt-manifest" : "missing-snapshot");
        string snapshotsDirectory = Path.Combine(directory, "snapshots");
        string manifestPath = Path.Combine(snapshotsDirectory, $"p_{partitionId}.manifest");
        string snapshotPath = Path.Combine(snapshotsDirectory, $"p_{partitionId}.1.snap");

        try
        {
            using (var database = StorageDatabase.Open(directory, partitionId))
            {
                Put(database, 0x08, 0x88);
                database.Checkpoint();
            }

            if (corruptManifest)
            {
                byte[] manifest = File.ReadAllBytes(manifestPath);
                manifest[^1] ^= 0x80;
                File.WriteAllBytes(manifestPath, manifest);
            }
            else
            {
                File.Delete(snapshotPath);
            }

            Assert.Throws<InvalidDataException>(() => StorageDatabase.Open(directory, partitionId));
            Assert.True(File.Exists(manifestPath));
            Assert.Equal(corruptManifest, File.Exists(snapshotPath));
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
    public void Checkpoint_failure_requires_reopen_and_recovers_from_wal()
    {
        const ulong partitionId = 907;
        string directory = Path.Combine(_root, "checkpoint-publication-failure");
        Directory.CreateDirectory(directory);
        string snapshots = Path.Combine(directory, "snapshots");

        try
        {
            using (var database = StorageDatabase.Open(directory, partitionId))
            {
                Put(database, 0x12, 0x34);
                Directory.CreateDirectory(snapshots);
                File.WriteAllBytes(Path.Combine(snapshots, $"p_{partitionId}.1.snap"),
                    new byte[] { 0xFA, 0xFB });
                Assert.Throws<IOException>(() => database.Checkpoint());
                Assert.Throws<InvalidOperationException>(() => database.TryGet(new byte[] { 0x12 }, out _));
            }

            using var recovered = StorageDatabase.Open(directory, partitionId);
            Assert.Equal(1UL, recovered.LastCommitSeq);
            Assert.True(recovered.TryGet(new byte[] { 0x12 }, out var value));
            Assert.Equal(new byte[] { 0x34 }, value);
            Assert.False(File.Exists(Path.Combine(snapshots, $"p_{partitionId}.1.snap")));
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
    [InlineData((int)StorageFaultPoint.AfterSnapshotFileFlushBeforeRename)]
    [InlineData((int)StorageFaultPoint.AfterSnapshotRenameBeforeManifest)]
    [InlineData((int)StorageFaultPoint.AfterManifestFileFlushBeforeRename)]
    [InlineData((int)StorageFaultPoint.AfterManifestRenameBeforeWalTruncate)]
    public void Checkpoint_fault_points_recover_and_allow_a_new_checkpoint(int faultPointValue)
    {
        StorageFaultPoint faultPoint = (StorageFaultPoint)faultPointValue;
        const ulong partitionId = 921;
        string directory = Path.Combine(_root, "checkpoint-fault-" + faultPoint);

        try
        {
            using (var database = StorageDatabase.OpenForTesting(directory, partitionId, point =>
            {
                if (point == faultPoint)
                    throw new IOException("Injected snapshot publication boundary failure.");
            }))
            {
                Put(database, 0x31, 0x41);
                Assert.Throws<IOException>(() => database.Checkpoint());
                Assert.Throws<InvalidOperationException>(() => database.TryGet(new byte[] { 0x31 }, out _));
            }

            using (var recovered = StorageDatabase.Open(directory, partitionId))
            {
                Assert.Equal(1UL, recovered.LastCommitSeq);
                Assert.True(recovered.TryGet(new byte[] { 0x31 }, out var value));
                Assert.Equal(new byte[] { 0x41 }, value);
                recovered.Checkpoint();
            }

            using var reopened = StorageDatabase.Open(directory, partitionId);
            Assert.Equal(1UL, reopened.LastCommitSeq);
            Assert.True(reopened.TryGet(new byte[] { 0x31 }, out var reopenedValue));
            Assert.Equal(new byte[] { 0x41 }, reopenedValue);
        }
        finally
        {
            TryDeleteDirectory(directory);
        }
    }

}
