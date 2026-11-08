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
        Path.GetTempPath(), "fld-storage-crash-" + Guid.NewGuid().ToString("N"));

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
}
