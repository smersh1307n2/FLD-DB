using Fld1.Engine.Db;
using Xunit;

namespace Fld1.Engine.Tests;

public sealed class FldDatabaseManifestDurabilityTests
{
    [Fact]
    public void Flush_manifest_uncertain_after_rename_faults_instance_and_reopen_recovers_wal()
    {
        string root = Path.Combine(Path.GetTempPath(), "fld-manifest-fault-" + Guid.NewGuid().ToString("N"));
        const uint namespaceId = 0x756;
        byte[] recordId = Id(0x11);
        bool injectFault = false;

        try
        {
            using (var database = FldDatabase.Open(new FldOpenOptions
            {
                Path = root,
                NamespaceId = namespaceId,
                LocalAuthor = Id(0x61),
                CompactionFaultInjector = point =>
                {
                    if (injectFault && point == FldCompactionFaultPoint.AfterManifestRenameBeforeDirectoryFlush)
                        throw new IOException("Injected uncertain Manifest rename/fsync completion.");
                }
            }))
            {
                database.Put(recordId, 5, "wal-recoverable"u8);
                Assert.NotEqual(0UL, database.Commit());
                injectFault = true;

                Assert.Throws<IOException>(() => database.FlushManifest());
                Assert.Throws<InvalidOperationException>(() => database.Put(recordId, 6, "must-not-continue"u8));
            }

            using var recovered = FldDatabase.Open(root, namespaceId, Id(0x61));
            Assert.True(recovered.TryGetBytes(recordId, 5, out byte[]? value));
            Assert.Equal("wal-recoverable"u8.ToArray(), value);
            Assert.False(recovered.TryGetBytes(recordId, 6, out _));
        }
        finally
        {
            try { Directory.Delete(root, recursive: true); } catch { }
        }
    }

    private static byte[] Id(byte value)
    {
        byte[] id = new byte[16];
        id[15] = value;
        return id;
    }
}
