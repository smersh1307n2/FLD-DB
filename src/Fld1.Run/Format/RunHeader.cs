using System.Buffers.Binary;
using System.Runtime.CompilerServices;

namespace Fld1.Run.Format;

/// <summary>
/// FLD1 run header — 256 bytes, 4 cache lines. Spec §1.1.
/// </summary>
public static class RunHeader
{
    public const int Size = RunConstants.HeaderSize;

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
        return header[0] == (byte)'F'
            && header[1] == (byte)'L'
            && header[2] == (byte)'D'
            && header[3] == (byte)'1';
    }

    public static void WriteEmpty(Span<byte> header, uint namespaceId, ReadOnlySpan<byte> rootDigest32)
    {
        WriteCore(
            header, namespaceId, flags: 0,
            numClusters: 0, numFields: 0, internCount: 0,
            pgmSegments: 0, pgmEpsilon: 0,
            internOff: Size, pgmOff: Size, slotOff: Size, dataOff: Size,
            valuesOff: Size, valuesLen: 0, footerOff: Size,
            numExternal: 0, aliveFields: 0, contributorCount: 0, ghostCount: 0,
            rootDigest32);
    }

    public static void WriteCore(
        Span<byte> header,
        uint namespaceId,
        ushort flags,
        uint numClusters,
        uint numFields,
        uint internCount,
        uint pgmSegments,
        uint pgmEpsilon,
        ulong internOff,
        ulong pgmOff,
        ulong slotOff,
        ulong dataOff,
        ulong valuesOff,
        ulong valuesLen,
        ulong footerOff,
        uint numExternal,
        uint aliveFields,
        uint contributorCount,
        uint ghostCount,
        ReadOnlySpan<byte> rootDigest32,
        ReadOnlySpan<byte> summaryXor = default,
        ReadOnlySpan<byte> summarySum = default)
    {
        if (header.Length < Size) throw new ArgumentException("header buffer too small");
        if (rootDigest32.Length < 32) throw new ArgumentException("rootDigest must be 32 bytes");
        header.Clear();

        RunConstants.Magic.CopyTo(header);
        BinaryPrimitives.WriteUInt16LittleEndian(header.Slice(OffBinaryVersion), RunConstants.BinaryVersion);
        BinaryPrimitives.WriteUInt16LittleEndian(header.Slice(OffFlags), flags);
        BinaryPrimitives.WriteUInt32LittleEndian(header.Slice(OffNamespaceId), namespaceId);
        BinaryPrimitives.WriteUInt32LittleEndian(header.Slice(OffNumClusters), numClusters);
        BinaryPrimitives.WriteUInt32LittleEndian(header.Slice(OffNumFields), numFields);
        BinaryPrimitives.WriteUInt32LittleEndian(header.Slice(OffInternCount), internCount);
        BinaryPrimitives.WriteUInt32LittleEndian(header.Slice(OffPgmSegments), pgmSegments);
        BinaryPrimitives.WriteUInt32LittleEndian(header.Slice(OffPgmEpsilon), pgmEpsilon);
        BinaryPrimitives.WriteUInt64LittleEndian(header.Slice(OffInternOff), internOff);
        BinaryPrimitives.WriteUInt64LittleEndian(header.Slice(OffPgmOff), pgmOff);
        BinaryPrimitives.WriteUInt64LittleEndian(header.Slice(OffSlotOff), slotOff);
        BinaryPrimitives.WriteUInt64LittleEndian(header.Slice(OffDataOff), dataOff);
        BinaryPrimitives.WriteUInt64LittleEndian(header.Slice(OffValuesOff), valuesOff);
        BinaryPrimitives.WriteUInt64LittleEndian(header.Slice(OffValuesLen), valuesLen);
        BinaryPrimitives.WriteUInt64LittleEndian(header.Slice(OffFooterOff), footerOff);
        BinaryPrimitives.WriteUInt32LittleEndian(header.Slice(OffNumExternalValues), numExternal);
        BinaryPrimitives.WriteUInt32LittleEndian(header.Slice(OffAliveFields), aliveFields);
        BinaryPrimitives.WriteUInt32LittleEndian(header.Slice(OffContributorCount), contributorCount);
        BinaryPrimitives.WriteUInt32LittleEndian(header.Slice(OffGhostCount), ghostCount);
        if (summaryXor.Length >= 16) summaryXor.Slice(0, 16).CopyTo(header.Slice(OffSummaryXor, 16));
        if (summarySum.Length >= 16) summarySum.Slice(0, 16).CopyTo(header.Slice(OffSummarySum, 16));
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
    public static uint ReadInternCount(ReadOnlySpan<byte> h)
        => BinaryPrimitives.ReadUInt32LittleEndian(h.Slice(OffInternCount));
    public static uint ReadPgmSegments(ReadOnlySpan<byte> h)
        => BinaryPrimitives.ReadUInt32LittleEndian(h.Slice(OffPgmSegments));
    public static uint ReadPgmEpsilon(ReadOnlySpan<byte> h)
        => BinaryPrimitives.ReadUInt32LittleEndian(h.Slice(OffPgmEpsilon));
    public static ulong ReadInternOff(ReadOnlySpan<byte> h)
        => BinaryPrimitives.ReadUInt64LittleEndian(h.Slice(OffInternOff));
    public static ulong ReadPgmOff(ReadOnlySpan<byte> h)
        => BinaryPrimitives.ReadUInt64LittleEndian(h.Slice(OffPgmOff));
    public static ulong ReadSlotOff(ReadOnlySpan<byte> h)
        => BinaryPrimitives.ReadUInt64LittleEndian(h.Slice(OffSlotOff));
    public static ulong ReadDataOff(ReadOnlySpan<byte> h)
        => BinaryPrimitives.ReadUInt64LittleEndian(h.Slice(OffDataOff));
    public static ulong ReadFooterOff(ReadOnlySpan<byte> h)
        => BinaryPrimitives.ReadUInt64LittleEndian(h.Slice(OffFooterOff));
    public static ulong ReadValuesOff(ReadOnlySpan<byte> h)
        => BinaryPrimitives.ReadUInt64LittleEndian(h.Slice(OffValuesOff));
    public static ulong ReadValuesLen(ReadOnlySpan<byte> h)
        => BinaryPrimitives.ReadUInt64LittleEndian(h.Slice(OffValuesLen));
    public static uint ReadAliveFields(ReadOnlySpan<byte> h)
        => BinaryPrimitives.ReadUInt32LittleEndian(h.Slice(OffAliveFields));
    public static ReadOnlySpan<byte> RootDigest(ReadOnlySpan<byte> h)
        => h.Slice(OffRootDigest, 32);
    public static ReadOnlySpan<byte> ReservedPad(ReadOnlySpan<byte> h)
        => h.Slice(OffReservedPad, ReservedPadLen);
}
