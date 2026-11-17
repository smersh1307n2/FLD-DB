using Fld1.Storage.Engine;
using Xunit;

namespace Fld1.Storage.Tests;

public sealed class StorageBTreeDurabilityPrimitiveTests
{
    [Fact]
    public void Manifest_v2_round_trips_and_detects_legacy_v1_without_decoding_it()
    {
        var expected = new StorageBTreeManifestV2(
            PartitionId: 17,
            Generation: 4,
            CommitSeq: 91,
            RootPageId: 3,
            CommittedPageFileLength: 3UL * StoragePageCodec.PageSize,
            CommittedDataFileLength: 4096,
            PageSize: StoragePageCodec.PageSize,
            FormatFlags: 0);

        byte[] encoded = StorageBTreeManifestCodec.Encode(expected);

        Assert.Equal(StorageBTreeManifestCodec.ManifestSize, encoded.Length);
        Assert.Equal(StorageManifestFormat.PagedIndexV2, StorageBTreeManifestCodec.DetectFormat(encoded));
        Assert.Equal(expected, StorageBTreeManifestCodec.Decode(encoded));
        Assert.Equal(StorageManifestFormat.LegacyV1,
            StorageBTreeManifestCodec.DetectFormat("FLDM"u8.ToArray()));
        Assert.Throws<InvalidDataException>(() => StorageBTreeManifestCodec.Decode("FLDM"u8.ToArray()));
    }

    [Fact]
    public void Manifest_v2_rejects_crc_reserved_bytes_and_unsupported_version()
    {
        byte[] encoded = StorageBTreeManifestCodec.Encode(ValidManifest());

        byte[] corruptCrc = encoded.ToArray();
        corruptCrc[24] ^= 0x40;
        Assert.Throws<InvalidDataException>(() => StorageBTreeManifestCodec.Decode(corruptCrc));

        byte[] reserved = encoded.ToArray();
        reserved[70] = 1;
        Assert.Throws<InvalidDataException>(() => StorageBTreeManifestCodec.Decode(reserved));

        byte[] version = encoded.ToArray();
        version[4] = 3;
        Assert.Throws<InvalidDataException>(() => StorageBTreeManifestCodec.Decode(version));
    }

    [Theory]
    [InlineData(0UL, 0UL)]
    [InlineData(2UL, 1UL)]
    [InlineData(4UL, 1UL)]
    public void Manifest_v2_rejects_root_outside_committed_page_range(ulong rootPageId, ulong pageCount)
    {
        var manifest = new StorageBTreeManifestV2(
            1, 1, 0, rootPageId,
            pageCount * (ulong)StoragePageCodec.PageSize,
            0, StoragePageCodec.PageSize, 0);

        Assert.Throws<InvalidDataException>(() => StorageBTreeManifestCodec.Encode(manifest));
    }

    [Fact]
    public void Generation_files_append_validate_read_and_truncate_uncommitted_tails()
    {
        string directory = Path.Combine(Path.GetTempPath(), "fld-btree-files-" + Guid.NewGuid().ToString("N"));
        const ulong partition = 71;
        const ulong generation = 9;
        byte[] page1 = StoragePageCodec.Encode(generation, 1, StoragePageKind.Leaf, "leaf-one"u8);
        byte[] page2 = StoragePageCodec.Encode(generation, 2, StoragePageKind.Leaf, "leaf-two"u8);

        try
        {
            ulong committedDataLength;
            ulong committedPageLength;
            using (var files = new StorageBTreeGenerationFiles(directory, partition, generation))
            {
                ulong keyOffset = files.AppendData("key"u8);
                ulong valueOffset = files.AppendData("value"u8);
                files.AppendPage(1, page1);
                files.FlushToDisk();

                committedDataLength = files.DataFileLength;
                committedPageLength = files.PageFileLength;

                _ = files.AppendData("orphan-tail"u8);
                files.AppendPage(2, page2);
                files.FlushToDisk();

                Assert.Equal("key"u8.ToArray(), files.ReadData(keyOffset, 3, committedDataLength));
                Assert.Equal("value"u8.ToArray(), files.ReadData(valueOffset, 5, committedDataLength));
                Assert.Equal(page1, files.ReadPage(1, committedPageLength));
                Assert.Throws<InvalidDataException>(() => files.ReadPage(2, committedPageLength));
                Assert.Throws<InvalidDataException>(() => files.ReadData(committedDataLength, 1, committedDataLength));

                files.TruncateToCommittedLengths(committedPageLength, committedDataLength);
                Assert.Equal(committedPageLength, files.PageFileLength);
                Assert.Equal(committedDataLength, files.DataFileLength);
            }

            using var reopened = new StorageBTreeGenerationFiles(directory, partition, generation);
            Assert.Equal(page1, reopened.ReadPage(1, committedPageLength));
            Assert.Equal("value"u8.ToArray(), reopened.ReadData(3, 5, committedDataLength));
            Assert.Throws<InvalidOperationException>(() => reopened.AppendPage(3, page2));
        }
        finally
        {
            try { if (Directory.Exists(directory)) Directory.Delete(directory, recursive: true); } catch { }
        }
    }

    [Fact]
    public void Generation_files_reject_page_id_gaps_and_corrupt_page_identity()
    {
        string directory = Path.Combine(Path.GetTempPath(), "fld-btree-invalid-" + Guid.NewGuid().ToString("N"));
        const ulong generation = 2;
        try
        {
            using var files = new StorageBTreeGenerationFiles(directory, 72, generation);
            byte[] page = StoragePageCodec.Encode(generation, 2, StoragePageKind.Leaf, "x"u8);
            Assert.Throws<InvalidOperationException>(() => files.AppendPage(2, page));

            byte[] wrongIdentity = StoragePageCodec.Encode(generation, 1, StoragePageKind.Leaf, "x"u8);
            wrongIdentity[16] ^= 1;
            Assert.Throws<InvalidDataException>(() => files.AppendPage(1, wrongIdentity));
        }
        finally
        {
            try { if (Directory.Exists(directory)) Directory.Delete(directory, recursive: true); } catch { }
        }
    }

    private static StorageBTreeManifestV2 ValidManifest()
        => new(
            1, 1, 1, 1,
            (ulong)StoragePageCodec.PageSize,
            32,
            StoragePageCodec.PageSize,
            0);
}
