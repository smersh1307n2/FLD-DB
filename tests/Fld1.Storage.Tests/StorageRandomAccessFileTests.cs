using Fld1.Storage.Engine;
using Xunit;

namespace Fld1.Storage.Tests;

public sealed class StorageRandomAccessFileTests
{
    [Fact]
    public void Positional_reads_and_writes_do_not_depend_on_shared_file_position()
    {
        string path = Path.Combine(Path.GetTempPath(), $"fld-pages-{Guid.NewGuid():N}.bin");
        try
        {
            using var file = new StorageRandomAccessFile(path, FileMode.CreateNew);
            byte[] first = Enumerable.Repeat((byte)0x11, 128).ToArray();
            byte[] second = Enumerable.Repeat((byte)0x22, 64).ToArray();
            file.WriteExactly(256, second);
            file.WriteExactly(0, first);
            file.FlushToDisk();

            byte[] actualFirst = new byte[first.Length];
            byte[] actualSecond = new byte[second.Length];
            file.ReadExactly(0, actualFirst);
            file.ReadExactly(256, actualSecond);

            Assert.Equal(first, actualFirst);
            Assert.Equal(second, actualSecond);
            Assert.Equal(320L, file.Length);
        }
        finally
        {
            if (File.Exists(path)) File.Delete(path);
        }
    }

    [Fact]
    public void Read_rejects_short_file_and_invalid_offsets()
    {
        string path = Path.Combine(Path.GetTempPath(), $"fld-pages-{Guid.NewGuid():N}.bin");
        try
        {
            using var file = new StorageRandomAccessFile(path, FileMode.CreateNew);
            file.WriteExactly(0, new byte[] { 1, 2, 3 });
            Assert.Throws<EndOfStreamException>(() => file.ReadExactly(0, new byte[4]));
            Assert.Throws<ArgumentOutOfRangeException>(() => file.ReadExactly(-1, new byte[1]));
            Assert.Throws<ArgumentOutOfRangeException>(() => file.WriteExactly(-1, new byte[1]));
        }
        finally
        {
            if (File.Exists(path)) File.Delete(path);
        }
    }
}
