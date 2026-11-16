using Fld1.Run.Builder;
using Fld1.Run.Reader;
using Xunit;

namespace Fld1.Engine.Tests;

public sealed class MappedRunFileTests
{
    [Fact]
    public void Mapped_run_reader_reads_valid_immutable_run_without_managed_file_copy()
    {
        string directory = Path.Combine(Path.GetTempPath(), "fld1-mapped-run-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(directory);
        string path = Path.Combine(directory, "1.fld1");
        byte[] recordId = new byte[16];
        recordId[0] = 0x42;
        recordId[6] = 0x70;
        recordId[8] = 0x80;
        byte[] author = new byte[16];
        author[15] = 0x17;
        byte[] expected = "mapped-value"u8.ToArray();
        byte[] run = new RunBuilder(7)
            .Add(recordId, 9, expected, wall: 1234, logical: 2, author)
            .Build();
        File.WriteAllBytes(path, run);

        try
        {
            using (MappedRunFile mapped = MappedRunFile.Open(path))
            {
                ReadOnlyMemory<byte> memory = mapped.ReadOnlyMemory;
                var reader = new RunReader(memory);
                Assert.True(reader.TryValidate(out var verify), verify.Detail);
                Assert.Equal(7U, reader.NamespaceId);
                Assert.True(reader.TryGet(recordId, 9, out ReadOnlySpan<byte> value));
                Assert.Equal(expected, value.ToArray());
            }

            // A released mapping must release the underlying view/file handle on all
            // supported platforms; this is especially important for Windows compaction.
            File.Delete(path);
            Assert.False(File.Exists(path));
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }

    [Fact]
    public void Mapped_run_memory_rejects_access_after_owner_disposal()
    {
        string path = Path.Combine(Path.GetTempPath(), "fld1-mapped-run-" + Guid.NewGuid().ToString("N"));
        byte[] run = new RunBuilder(1).Build();
        File.WriteAllBytes(path, run);
        try
        {
            MappedRunFile mapped = MappedRunFile.Open(path);
            mapped.Dispose();
            Assert.Throws<ObjectDisposedException>(() => _ = mapped.ReadOnlyMemory);
        }
        finally
        {
            File.Delete(path);
        }
    }
}
