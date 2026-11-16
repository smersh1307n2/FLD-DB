using System.Buffers.Binary;
using Fld1.Storage.Integrity;

namespace Fld1.Storage.Engine;

/// <summary>Immutable page kinds used by the Storage copy-on-write ordered index.</summary>
internal enum StoragePageKind : byte
{
    Leaf = 1,
    Internal = 2
}

/// <summary>A validated view over one fixed-size immutable Storage page.</summary>
internal readonly struct StoragePageView
{
    public ulong Generation { get; }
    public ulong PageId { get; }
    public StoragePageKind Kind { get; }
    public ReadOnlyMemory<byte> Payload { get; }

    public StoragePageView(ulong generation, ulong pageId, StoragePageKind kind, ReadOnlyMemory<byte> payload)
    {
        Generation = generation;
        PageId = pageId;
        Kind = kind;
        Payload = payload;
    }
}

/// <summary>
/// Fixed-size page envelope for the copy-on-write Storage index. The codec validates
/// page identity and CRC before exposing a payload; it has no database or FLD semantics.
/// </summary>
internal static class StoragePageCodec
{
    public const int PageSize = 16 * 1024;
    public const int HeaderSize = 32;
    private const uint Magic = 0x32475053; // "SPG2"
    private const ushort Version = 1;
    private const int CrcOffset = 28;

    public static byte[] Encode(
        ulong generation,
        ulong pageId,
        StoragePageKind kind,
        ReadOnlySpan<byte> payload)
    {
        if (generation == 0) throw new ArgumentOutOfRangeException(nameof(generation));
        if (pageId == 0) throw new ArgumentOutOfRangeException(nameof(pageId));
        ValidateKind(kind);
        if (payload.Length > PageSize - HeaderSize)
            throw new ArgumentOutOfRangeException(nameof(payload), "Storage page payload exceeds the fixed page capacity.");

        byte[] page = new byte[PageSize];
        Span<byte> header = page.AsSpan(0, HeaderSize);
        BinaryPrimitives.WriteUInt32LittleEndian(header, Magic);
        BinaryPrimitives.WriteUInt16LittleEndian(header.Slice(4), Version);
        header[6] = (byte)kind;
        header[7] = 0;
        BinaryPrimitives.WriteUInt64LittleEndian(header.Slice(8), generation);
        BinaryPrimitives.WriteUInt64LittleEndian(header.Slice(16), pageId);
        BinaryPrimitives.WriteUInt32LittleEndian(header.Slice(24), checked((uint)payload.Length));
        payload.CopyTo(page.AsSpan(HeaderSize));
        uint crc = StorageChecksums.Crc32C(
            page.AsSpan(0, CrcOffset),
            page.AsSpan(HeaderSize, payload.Length));
        BinaryPrimitives.WriteUInt32LittleEndian(header.Slice(CrcOffset), crc);
        return page;
    }

    public static StoragePageView Decode(ReadOnlyMemory<byte> page)
    {
        ReadOnlySpan<byte> bytes = page.Span;
        if (bytes.Length != PageSize)
            throw new InvalidDataException($"Storage page length must be exactly {PageSize} bytes.");
        if (BinaryPrimitives.ReadUInt32LittleEndian(bytes) != Magic)
            throw new InvalidDataException("Storage page magic is invalid.");
        if (BinaryPrimitives.ReadUInt16LittleEndian(bytes.Slice(4)) != Version)
            throw new InvalidDataException("Storage page version is unsupported.");
        if (bytes[7] != 0)
            throw new InvalidDataException("Storage page reserved flags are non-zero.");

        StoragePageKind kind = (StoragePageKind)bytes[6];
        ValidateKind(kind);
        ulong generation = BinaryPrimitives.ReadUInt64LittleEndian(bytes.Slice(8));
        ulong pageId = BinaryPrimitives.ReadUInt64LittleEndian(bytes.Slice(16));
        if (generation == 0 || pageId == 0)
            throw new InvalidDataException("Storage page identity must be non-zero.");

        uint rawLength = BinaryPrimitives.ReadUInt32LittleEndian(bytes.Slice(24));
        if (rawLength > PageSize - HeaderSize)
            throw new InvalidDataException("Storage page payload length exceeds page capacity.");
        int payloadLength = checked((int)rawLength);
        ReadOnlySpan<byte> payload = bytes.Slice(HeaderSize, payloadLength);
        foreach (byte padding in bytes.Slice(HeaderSize + payloadLength))
        {
            if (padding != 0)
                throw new InvalidDataException("Storage page padding is non-zero.");
        }

        uint expectedCrc = BinaryPrimitives.ReadUInt32LittleEndian(bytes.Slice(CrcOffset));
        uint actualCrc = StorageChecksums.Crc32C(bytes.Slice(0, CrcOffset), payload);
        if (actualCrc != expectedCrc)
            throw new InvalidDataException("Storage page CRC32C mismatch.");

        return new StoragePageView(generation, pageId, kind, page.Slice(HeaderSize, payloadLength));
    }

    public static StoragePageView Decode(
        ReadOnlyMemory<byte> page,
        ulong expectedGeneration,
        ulong expectedPageId)
    {
        StoragePageView view = Decode(page);
        if (view.Generation != expectedGeneration || view.PageId != expectedPageId)
            throw new InvalidDataException("Storage page identity does not match its manifest reference.");
        return view;
    }

    private static void ValidateKind(StoragePageKind kind)
    {
        if (kind is not StoragePageKind.Leaf and not StoragePageKind.Internal)
            throw new InvalidDataException($"Storage page kind {(byte)kind} is unsupported.");
    }
}
