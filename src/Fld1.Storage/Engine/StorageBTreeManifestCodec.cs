using System.Buffers.Binary;
using Fld1.Storage.Integrity;

namespace Fld1.Storage.Engine;

internal enum StorageManifestFormat : ushort
{
    LegacyV1 = 1,
    PagedIndexV2 = 2
}

/// <summary>
/// The durable root for one immutable B+ tree generation. File lengths are committed
/// high-water marks: bytes beyond them are unreachable crash leftovers.
/// </summary>
internal readonly record struct StorageBTreeManifestV2(
    ulong PartitionId,
    ulong Generation,
    ulong CommitSeq,
    ulong RootPageId,
    ulong CommittedPageFileLength,
    ulong CommittedDataFileLength,
    uint PageSize,
    uint FormatFlags);

/// <summary>
/// Fixed-width v2 root manifest. The legacy FLDM/v1 layout is detected but never
/// decoded as v2; the existing v1 snapshot reader remains the compatibility authority.
/// </summary>
internal static class StorageBTreeManifestCodec
{
    private static ReadOnlySpan<byte> LegacyMagic => "FLDM"u8;
    private static ReadOnlySpan<byte> Magic => "FLD2"u8;
    public const ushort Version = 2;
    public const int ManifestSize = 80;
    private const int CrcOffset = ManifestSize - sizeof(uint);
    private const int ReservedOffset = 64;
    private const int ReservedLength = CrcOffset - ReservedOffset;

    public static StorageManifestFormat DetectFormat(ReadOnlySpan<byte> bytes)
    {
        if (bytes.Length < 4)
            throw new InvalidDataException("Storage manifest is shorter than its magic.");

        if (bytes[..4].SequenceEqual(LegacyMagic))
            return StorageManifestFormat.LegacyV1;
        if (bytes[..4].SequenceEqual(Magic))
            return StorageManifestFormat.PagedIndexV2;

        throw new InvalidDataException("Storage manifest magic is unknown.");
    }

    public static byte[] Encode(in StorageBTreeManifestV2 manifest)
    {
        Validate(manifest);

        byte[] bytes = new byte[ManifestSize];
        Magic.CopyTo(bytes);
        BinaryPrimitives.WriteUInt16LittleEndian(bytes.AsSpan(4, 2), Version);
        BinaryPrimitives.WriteUInt16LittleEndian(bytes.AsSpan(6, 2), ManifestSize);
        BinaryPrimitives.WriteUInt64LittleEndian(bytes.AsSpan(8, 8), manifest.PartitionId);
        BinaryPrimitives.WriteUInt64LittleEndian(bytes.AsSpan(16, 8), manifest.Generation);
        BinaryPrimitives.WriteUInt64LittleEndian(bytes.AsSpan(24, 8), manifest.CommitSeq);
        BinaryPrimitives.WriteUInt64LittleEndian(bytes.AsSpan(32, 8), manifest.RootPageId);
        BinaryPrimitives.WriteUInt64LittleEndian(bytes.AsSpan(40, 8), manifest.CommittedPageFileLength);
        BinaryPrimitives.WriteUInt64LittleEndian(bytes.AsSpan(48, 8), manifest.CommittedDataFileLength);
        BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan(56, 4), manifest.PageSize);
        BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan(60, 4), manifest.FormatFlags);
        // Bytes 64..75 are reserved and remain zero.
        uint crc = StorageChecksums.Crc32C(bytes.AsSpan(0, CrcOffset));
        BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan(CrcOffset, 4), crc);
        return bytes;
    }

    public static StorageBTreeManifestV2 Decode(ReadOnlySpan<byte> bytes)
    {
        if (bytes.Length != ManifestSize)
            throw new InvalidDataException($"Paged Storage manifest length must be exactly {ManifestSize} bytes.");
        if (DetectFormat(bytes) != StorageManifestFormat.PagedIndexV2)
            throw new InvalidDataException("Storage manifest is not the paged-index v2 format.");
        if (BinaryPrimitives.ReadUInt16LittleEndian(bytes.Slice(4, 2)) != Version ||
            BinaryPrimitives.ReadUInt16LittleEndian(bytes.Slice(6, 2)) != ManifestSize)
            throw new InvalidDataException("Paged Storage manifest version or header size is unsupported.");
        if (bytes.Slice(ReservedOffset, ReservedLength).IndexOfAnyExcept((byte)0) >= 0)
            throw new InvalidDataException("Paged Storage manifest reserved bytes are non-zero.");

        uint expectedCrc = BinaryPrimitives.ReadUInt32LittleEndian(bytes.Slice(CrcOffset, 4));
        uint actualCrc = StorageChecksums.Crc32C(bytes[..CrcOffset]);
        if (actualCrc != expectedCrc)
            throw new InvalidDataException("Paged Storage manifest CRC32C mismatch.");

        var manifest = new StorageBTreeManifestV2(
            BinaryPrimitives.ReadUInt64LittleEndian(bytes.Slice(8, 8)),
            BinaryPrimitives.ReadUInt64LittleEndian(bytes.Slice(16, 8)),
            BinaryPrimitives.ReadUInt64LittleEndian(bytes.Slice(24, 8)),
            BinaryPrimitives.ReadUInt64LittleEndian(bytes.Slice(32, 8)),
            BinaryPrimitives.ReadUInt64LittleEndian(bytes.Slice(40, 8)),
            BinaryPrimitives.ReadUInt64LittleEndian(bytes.Slice(48, 8)),
            BinaryPrimitives.ReadUInt32LittleEndian(bytes.Slice(56, 4)),
            BinaryPrimitives.ReadUInt32LittleEndian(bytes.Slice(60, 4)));
        Validate(manifest);
        return manifest;
    }

    private static void Validate(in StorageBTreeManifestV2 manifest)
    {
        if (manifest.PartitionId == 0 || manifest.Generation == 0)
            throw new InvalidDataException("Paged Storage manifest identity must be non-zero.");
        if (manifest.PageSize != StoragePageCodec.PageSize)
            throw new InvalidDataException("Paged Storage manifest page size is unsupported.");
        if (manifest.FormatFlags != 0)
            throw new InvalidDataException("Paged Storage manifest format flags are unsupported.");
        if (manifest.CommittedPageFileLength % manifest.PageSize != 0)
            throw new InvalidDataException("Committed page-file length is not page aligned.");

        ulong pageCount = manifest.CommittedPageFileLength / manifest.PageSize;
        if ((manifest.RootPageId == 0) != (pageCount == 0) ||
            manifest.RootPageId > pageCount)
            throw new InvalidDataException("Paged Storage manifest root is outside its committed page range.");
    }
}
