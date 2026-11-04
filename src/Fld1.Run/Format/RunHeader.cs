using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using Fld1.Core.Util;

namespace Fld1.Run.Format;

/// <summary>
/// FLD1 run header — 256 bytes, 4 cache lines. Spec §1.1.
/// All multi-byte integers little-endian except magic and raw id fields.
/// </summary>
public static class RunHeader
{
    public const int Size = RunConstants.HeaderSize;

    // Offsets
    public const int OffMagic = 0x00;
    public const int OffBinaryVersion = 0x04;
    public const int OffFlags = 0x06;
    public const int OffNamespaceId = 0x08;
    public const int OffNumClusters = 0x0C;
    public const int OffNumFields = 0x10;
    public const int OffInternCount = 0x14;
    public const int OffPgmSegments = 0x18;
    public const int OffPgmEpsilon = 0x1C;
    public const int OffInternOff = 0x20;
    public const int OffPgmOff = 0x28;
    public const int OffSlotOff = 0x30;
    public const int OffDataOff = 0x38;
    public const int OffValuesOff = 0x40;
    public const int OffValuesLen = 0x48;
    public const int OffFooterOff = 0x50;
    public const int OffNumExternalValues = 0x58;
    public const int OffAliveFields = 0x5C;
    public const int OffContributorCount = 0x60;
    public const int OffGhostCount = 0x64;
    public const int OffSummaryXor = 0x68;
    public const int OffSummarySum = 0x78;
    public const int OffRootDigest = 0x88;
    public const int OffReservedPad = 0xA8;
    public const int ReservedPadLen = 88;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool CheckMagic(ReadOnlySpan<byte> header)
    {
        if (header.Length < 4) return false;
        // [A1-MAGIC] memcmp, never ReadUInt32LE == const
        return header[0] == (byte)'F'
            && header[1] == (byte)'L'
            && header[2] == (byte)'D'
            && header[3] == (byte)'1';
    }

    public static void WriteEmpty(Span<byte> header, uint namespaceId, ReadOnlySpan<byte> rootDigest32)
    {
        if (header.Length < Size) throw new ArgumentException("header buffer too small");
        if (rootDigest32.Length < 32) throw new ArgumentException("rootDigest must be 32 bytes");
        header.Clear();

        // magic
        RunConstants.Magic.CopyTo(header);
        BinaryPrimitives.WriteUInt16LittleEndian(header.Slice(OffBinaryVersion), RunConstants.BinaryVersion);
        // flags = 0 (no HAS_VALUES)
        BinaryPrimitives.WriteUInt16LittleEndian(header.Slice(OffFlags), 0);
        BinaryPrimitives.WriteUInt32LittleEndian(header.Slice(OffNamespaceId), namespaceId);
        // counts zero
        // internOff = 256
        BinaryPrimitives.WriteUInt64LittleEndian(header.Slice(OffInternOff), Size);
        // all section offs = 256 for empty (no sections)
        BinaryPrimitives.WriteUInt64LittleEndian(header.Slice(OffPgmOff), Size);
        BinaryPrimitives.WriteUInt64LittleEndian(header.Slice(OffSlotOff), Size);
        BinaryPrimitives.WriteUInt64LittleEndian(header.Slice(OffDataOff), Size);
        // valuesOff == footerOff when HAS_VALUES=0
        const ulong footerOff = Size; // 256
        BinaryPrimitives.WriteUInt64LittleEndian(header.Slice(OffValuesOff), footerOff);
        BinaryPrimitives.WriteUInt64LittleEndian(header.Slice(OffValuesLen), 0);
        BinaryPrimitives.WriteUInt64LittleEndian(header.Slice(OffFooterOff), footerOff);
        rootDigest32.Slice(0, 32).CopyTo(header.Slice(OffRootDigest, 32));
    }

    public static ushort ReadBinaryVersion(ReadOnlySpan<byte> h)
        => BinaryPrimitives.ReadUInt16LittleEndian(h.Slice(OffBinaryVersion));

    public static RunFlags ReadFlags(ReadOnlySpan<byte> h)
        => (RunFlags)BinaryPrimitives.ReadUInt16LittleEndian(h.Slice(OffFlags));

    public static uint ReadNamespaceId(ReadOnlySpan<byte> h)
        => BinaryPrimitives.ReadUInt32LittleEndian(h.Slice(OffNamespaceId));

    public static uint ReadNumClusters(ReadOnlySpan<byte> h)
        => BinaryPrimitives.ReadUInt32LittleEndian(h.Slice(OffNumClusters));

    public static uint ReadNumFields(ReadOnlySpan<byte> h)
        => BinaryPrimitives.ReadUInt32LittleEndian(h.Slice(OffNumFields));

    public static ulong ReadFooterOff(ReadOnlySpan<byte> h)
        => BinaryPrimitives.ReadUInt64LittleEndian(h.Slice(OffFooterOff));

    public static ulong ReadValuesOff(ReadOnlySpan<byte> h)
        => BinaryPrimitives.ReadUInt64LittleEndian(h.Slice(OffValuesOff));

    public static ulong ReadValuesLen(ReadOnlySpan<byte> h)
        => BinaryPrimitives.ReadUInt64LittleEndian(h.Slice(OffValuesLen));

    public static ReadOnlySpan<byte> RootDigest(ReadOnlySpan<byte> h)
        => h.Slice(OffRootDigest, 32);

    public static ReadOnlySpan<byte> ReservedPad(ReadOnlySpan<byte> h)
        => h.Slice(OffReservedPad, ReservedPadLen);
}
