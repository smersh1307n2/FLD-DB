using System.Buffers.Binary;
using System.Security.Cryptography;
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
    public void New_blob_uses_one_contiguous_file_and_streams_with_digest_verification()
    {
        byte[] expected = Enumerable.Range(0, 150_123)
            .Select(i => (byte)((i * 31 + 7) & 0xFF))
            .ToArray();
        Guid blobId = Guid.NewGuid();

        using var database = StorageDatabase.Open(_directory, 201);
        var blobs = new StorageBlobStore(database, "test"u8, pageSize: 4096);
        blobs.PutImmutableBlob(blobId, expected);

        string blobDirectory = BlobDirectory(blobId);
        Assert.True(File.Exists(Path.Combine(blobDirectory, "content.blob")));
        Assert.Empty(Directory.GetFiles(blobDirectory, "*.page"));

        Assert.True(blobs.TryOpenRead(blobId, out StorageBlobReader? reader));
        using (reader)
        using (var output = new MemoryStream())
        {
            Assert.Equal((ulong)expected.Length, reader!.Length);
            Assert.Equal(4096, reader.PageSize);
            Assert.Equal((expected.Length + 4095) / 4096, reader.PageCount);
            Assert.True(reader.IsSingleFile);

            reader.CopyTo(output);
            Assert.Equal(expected, output.ToArray());
            Assert.Equal(expected, reader.ReadAllBytes());
            Assert.Equal(expected.AsSpan(4096, 4096).ToArray(), reader.ReadPage(1));
        }
    }

    [Fact]
    public void Mapped_blob_view_is_demand_paged_and_holds_blob_pin_until_disposed()
    {
        byte[] expected = Enumerable.Range(0, 32_000)
            .Select(i => (byte)((i * 13 + 5) & 0xFF))
            .ToArray();
        Guid blobId = Guid.NewGuid();

        using var database = StorageDatabase.Open(_directory, 202);
        var blobs = new StorageBlobStore(database, "test"u8, pageSize: 4096);
        blobs.PutImmutableBlob(blobId, expected);
        Assert.True(blobs.TryOpenRead(blobId, out StorageBlobReader? reader));

        StorageBlobMappedView view = reader!.OpenMappedView();
        reader.Dispose(); // ownership was transferred to the mapped view
        Assert.Equal(expected, view.ReadOnlyMemory.ToArray());
        Assert.False(blobs.DeleteBlob(blobId));

        view.Close();
        Assert.True(blobs.DeleteBlob(blobId));
    }

    [Fact]
    public void Legacy_page_blob_migrates_atomically_to_single_file()
    {
        byte[] expected = Enumerable.Range(0, 19_337)
            .Select(i => (byte)((i * 17 + 9) & 0xFF))
            .ToArray();
        Guid blobId = Guid.NewGuid();

        using var database = StorageDatabase.Open(_directory, 203);
        SeedLegacyPagedBlob(database, blobId, expected, pageSize: 4096);
        var blobs = new StorageBlobStore(database, "test"u8, pageSize: 4096);

        Assert.True(blobs.MigrateToSingleFile(blobId, out bool wasAlreadySingleFile));
        Assert.False(wasAlreadySingleFile);
        string blobDirectory = BlobDirectory(blobId);
        string contentPath = Path.Combine(blobDirectory, "content.blob");
        Assert.True(File.Exists(contentPath));
        Assert.Empty(Directory.GetFiles(blobDirectory, "*.page"));

        // Simulate a crash after the metadata switched to the contiguous file but
        // before old page cleanup completed. A retry verifies the file and converges.
        File.WriteAllBytes(Path.Combine(blobDirectory, "00000000.page"), new byte[] { 1, 2, 3 });
        Assert.True(blobs.MigrateToSingleFile(blobId, out wasAlreadySingleFile));
        Assert.True(wasAlreadySingleFile);
        Assert.Empty(Directory.GetFiles(blobDirectory, "*.page"));

        Assert.True(blobs.TryOpenRead(blobId, out StorageBlobReader? reader));
        using (reader)
        {
            Assert.True(reader!.IsSingleFile);
            Assert.Equal(expected, reader.ReadAllBytes());
        }
    }

    [Fact]
    public void Legacy_blob_migration_waits_until_existing_reader_releases_its_pin()
    {
        byte[] expected = Enumerable.Range(0, 9000).Select(i => (byte)i).ToArray();
        Guid blobId = Guid.NewGuid();

        using var database = StorageDatabase.Open(_directory, 204);
        SeedLegacyPagedBlob(database, blobId, expected, pageSize: 4096);
        var blobs = new StorageBlobStore(database, "test"u8, pageSize: 4096);

        Assert.True(blobs.TryOpenRead(blobId, out StorageBlobReader? reader));
        Assert.False(blobs.MigrateToSingleFile(blobId));
        reader!.Dispose();

        Assert.True(blobs.MigrateToSingleFile(blobId));
    }

    [Fact]
    public void Copy_to_rejects_a_truncated_single_file()
    {
        byte[] expected = Enumerable.Range(0, 9000).Select(i => (byte)(i & 0xFF)).ToArray();
        Guid blobId = Guid.NewGuid();

        using var database = StorageDatabase.Open(_directory, 205);
        var blobs = new StorageBlobStore(database, "test"u8, pageSize: 4096);
        blobs.PutImmutableBlob(blobId, expected);
        Assert.True(blobs.TryOpenRead(blobId, out StorageBlobReader? reader));

        using (reader)
        using (var output = new MemoryStream())
        {
            File.WriteAllBytes(Path.Combine(BlobDirectory(blobId), "content.blob"), new byte[] { 1, 2, 3 });
            Assert.Throws<InvalidDataException>(() => reader!.CopyTo(output));
        }
    }

    [Fact]
    public void Copy_to_rejects_trailing_bytes_in_single_file()
    {
        byte[] expected = Enumerable.Range(0, 5000).Select(i => (byte)(i & 0xFF)).ToArray();
        Guid blobId = Guid.NewGuid();

        using var database = StorageDatabase.Open(_directory, 206);
        var blobs = new StorageBlobStore(database, "test"u8, pageSize: 4096);
        blobs.PutImmutableBlob(blobId, expected);
        Assert.True(blobs.TryOpenRead(blobId, out StorageBlobReader? reader));

        using (reader)
        using (var output = new MemoryStream())
        {
            using (var append = new FileStream(
                Path.Combine(BlobDirectory(blobId), "content.blob"),
                FileMode.Append, FileAccess.Write, FileShare.Read))
                append.WriteByte(0xFF);
            Assert.Throws<InvalidDataException>(() => reader!.CopyTo(output));
        }
    }

    private string BlobDirectory(Guid blobId)
        => Path.Combine(_directory, "blobs", Convert.ToHexString("test"u8), blobId.ToString("N"));

    private void SeedLegacyPagedBlob(
        StorageDatabase database, Guid blobId, byte[] content, int pageSize)
    {
        string directory = BlobDirectory(blobId);
        Directory.CreateDirectory(directory);
        int pageCount = (content.Length + pageSize - 1) / pageSize;
        byte[] metadata = new byte[56];
        BinaryPrimitives.WriteUInt32LittleEndian(metadata, 0x31424C53); // SLB1
        BinaryPrimitives.WriteUInt16LittleEndian(metadata.AsSpan(4), 1);
        BinaryPrimitives.WriteUInt16LittleEndian(metadata.AsSpan(6), 1); // complete, legacy page layout
        BinaryPrimitives.WriteUInt64LittleEndian(metadata.AsSpan(8), (ulong)content.Length);
        BinaryPrimitives.WriteInt32LittleEndian(metadata.AsSpan(16), pageSize);
        BinaryPrimitives.WriteInt32LittleEndian(metadata.AsSpan(20), pageCount);
        SHA256.HashData(content).CopyTo(metadata, 24);

        byte[] key = new byte[5 + 16];
        "test"u8.CopyTo(key);
        key[4] = 0;
        blobId.ToByteArray().CopyTo(key, 5);
        using (StorageWriteBatch batch = database.BeginWriteBatch())
        {
            batch.Put(key, metadata);
            batch.Commit();
        }

        for (int pageIndex = 0; pageIndex < pageCount; pageIndex++)
        {
            int offset = pageIndex * pageSize;
            int length = Math.Min(pageSize, content.Length - offset);
            File.WriteAllBytes(
                Path.Combine(directory, pageIndex.ToString("D8") + ".page"),
                content.AsSpan(offset, length).ToArray());
        }
    }
}
