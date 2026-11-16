using Fld1.Storage.Engine;
using Xunit;

namespace Fld1.Storage.Tests;

public sealed class StorageBlobStoreTests : IDisposable
{
    private readonly string _directory = Path.Combine(
        Path.GetTempPath(), "fld-storage-blobs-" + Guid.NewGuid().ToString("N"));

    public StorageBlobStoreTests() => Directory.CreateDirectory(_directory);

    public void Dispose()
    {
        try { Directory.Delete(_directory, recursive: true); }
        catch { }
    }

    [Fact]
    public void Copy_to_streams_multiple_pages_and_verifies_the_complete_blob()
    {
        byte[] expected = Enumerable.Range(0, 150_123)
            .Select(i => (byte)((i * 31 + 7) & 0xFF))
            .ToArray();
        Guid blobId = Guid.NewGuid();

        using var database = StorageDatabase.Open(_directory, 201);
        var blobs = new StorageBlobStore(database, "test"u8, pageSize: 4096);
        blobs.PutImmutableBlob(blobId, expected);

        Assert.True(blobs.TryOpenRead(blobId, out StorageBlobReader? reader));
        using (reader)
        using (var output = new MemoryStream())
        {
            Assert.Equal((ulong)expected.Length, reader!.Length);
            Assert.Equal(4096, reader.PageSize);
            Assert.Equal((expected.Length + 4095) / 4096, reader.PageCount);

            reader.CopyTo(output);

            Assert.Equal(expected, output.ToArray());
        }
    }

    [Fact]
    public void Copy_to_rejects_a_truncated_page_instead_of_returning_partial_success()
    {
        byte[] expected = Enumerable.Range(0, 9000)
            .Select(i => (byte)(i & 0xFF))
            .ToArray();
        Guid blobId = Guid.NewGuid();

        using var database = StorageDatabase.Open(_directory, 202);
        var blobs = new StorageBlobStore(database, "test"u8, pageSize: 4096);
        blobs.PutImmutableBlob(blobId, expected);
        Assert.True(blobs.TryOpenRead(blobId, out StorageBlobReader? reader));

        string pagePath = Path.Combine(
            _directory, "blobs", Convert.ToHexString("test"u8), blobId.ToString("N"), "00000001.page");
        File.WriteAllBytes(pagePath, new byte[] { 1, 2, 3 });

        using (reader)
        using (var output = new MemoryStream())
        {
            Assert.Throws<InvalidDataException>(() => reader!.CopyTo(output));
        }
    }

    [Fact]
    public void Copy_to_rejects_trailing_bytes_in_a_page()
    {
        byte[] expected = Enumerable.Range(0, 5000)
            .Select(i => (byte)(i & 0xFF))
            .ToArray();
        Guid blobId = Guid.NewGuid();

        using var database = StorageDatabase.Open(_directory, 203);
        var blobs = new StorageBlobStore(database, "test"u8, pageSize: 4096);
        blobs.PutImmutableBlob(blobId, expected);
        Assert.True(blobs.TryOpenRead(blobId, out StorageBlobReader? reader));

        string pagePath = Path.Combine(
            _directory, "blobs", Convert.ToHexString("test"u8), blobId.ToString("N"), "00000000.page");
        using (var append = new FileStream(pagePath, FileMode.Append, FileAccess.Write, FileShare.Read))
            append.WriteByte(0xFF);

        using (reader)
        using (var output = new MemoryStream())
        {
            Assert.Throws<InvalidDataException>(() => reader!.CopyTo(output));
        }
    }
}
