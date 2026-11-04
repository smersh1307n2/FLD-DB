using System.Buffers.Binary;
using Fld1.Core.Util;
using Fld1.Core.Uuid;
using Fld1.Run.Format;
using Fld1.Run.Verify;

namespace Fld1.Run.Reader;

/// <summary>
/// Zero-copy reader over an immutable FLD1 run buffer.
/// Lookup: PGM window → ShortKey filter → full RecordId match → FieldDesc binary search.
/// </summary>
public readonly struct RunReader
{
    private readonly ReadOnlyMemory<byte> _file;

    public RunReader(ReadOnlyMemory<byte> file) => _file = file;

    public ReadOnlySpan<byte> Span => _file.Span;
    public int Length => _file.Length;
    public uint NamespaceId => RunHeader.ReadNamespaceId(_file.Span);
    public RunFlags Flags => RunHeader.ReadFlags(_file.Span);
    public uint NumClusters => RunHeader.ReadNumClusters(_file.Span);
    public uint NumFields => RunHeader.ReadNumFields(_file.Span);
    public bool IsEmpty => NumClusters == 0 && NumFields == 0;
    public ReadOnlySpan<byte> RootDigest => RunHeader.RootDigest(_file.Span);

    public bool TryValidate(out RunVerifyResult result)
    {
        result = RunVerify.Verify(_file.Span);
        return result.IsOk;
    }

    /// <summary>
    /// Point get. Returns false on miss / tombstone / empty.
    /// </summary>
    public bool TryGet(ReadOnlySpan<byte> recordId, ushort fieldId, out ReadOnlySpan<byte> value)
    {
        value = default;
        if (recordId.Length != 16) return false;
        if (IsEmpty) return false;

        var file = _file.Span;
        int clusterIdx = FindCluster(file, recordId);
        if (clusterIdx < 0) return false;

        ulong slotOff = RunHeader.ReadSlotOff(file);
        int slotPos = (int)slotOff + clusterIdx * RunConstants.CompactSlotSize;
        ulong dataOffset = BinaryPrimitives.ReadUInt64LittleEndian(file.Slice(slotPos, 8));
        return TryGetFromCluster(file, (int)dataOffset, fieldId, out value);
    }

    private static int FindCluster(ReadOnlySpan<byte> file, ReadOnlySpan<byte> recordId)
    {
        uint n = RunHeader.ReadNumClusters(file);
        if (n == 0) return -1;

        ulong slotOff = RunHeader.ReadSlotOff(file);
        ulong dataOff = RunHeader.ReadDataOff(file);
        uint shortKey = Uuid7.ShortKey(recordId);

        // Linear scan for correctness (PGM window optimization later).
        // Spec: slot is prefilter; full RecordId in cluster header is source of truth.
        for (int i = 0; i < (int)n; i++)
        {
            int slotPos = (int)slotOff + i * RunConstants.CompactSlotSize;
            // ShortKey prefilter
            uint sk = BinaryPrimitives.ReadUInt32BigEndian(file.Slice(slotPos + 8, 4));
            if (sk != shortKey) continue;

            ulong off = BinaryPrimitives.ReadUInt64LittleEndian(file.Slice(slotPos, 8));
            var clusterRec = file.Slice((int)off, 16);
            if (ByteUtil.Equals16(clusterRec, recordId))
                return i;
        }
        return -1;
    }

    private static bool TryGetFromCluster(
        ReadOnlySpan<byte> file, int clusterOff, ushort fieldId, out ReadOnlySpan<byte> value)
    {
        value = default;
        var ch = file.Slice(clusterOff);
        ushort numFields = BinaryPrimitives.ReadUInt16LittleEndian(ch.Slice(16));
        // flags at 18
        int descBase = clusterOff + RunConstants.ClusterHeaderSize;

        // Binary search FieldDesc by FieldId
        int lo = 0, hi = numFields - 1;
        int found = -1;
        while (lo <= hi)
        {
            int mid = lo + ((hi - lo) >> 1);
            int d = descBase + mid * RunConstants.FieldDescSize;
            ushort fid = BinaryPrimitives.ReadUInt16LittleEndian(file.Slice(d, 2));
            int cmp = fid.CompareTo(fieldId);
            if (cmp == 0) { found = mid; break; }
            if (cmp < 0) lo = mid + 1;
            else hi = mid - 1;
        }
        if (found < 0) return false;

        int desc = descBase + found * RunConstants.FieldDescSize;
        ushort valLen = BinaryPrimitives.ReadUInt16LittleEndian(file.Slice(desc + 2, 2));
        byte flags = file[desc + 4];
        if ((flags & (byte)FieldDescFlags.Tombstone) != 0)
            return false;

        if (valLen == RunConstants.ExternalValLen)
            return false; // external not yet

        ushort payloadOffset = BinaryPrimitives.ReadUInt16LittleEndian(file.Slice(desc + 6, 2));

        // Count overrides to find payload base
        int overrideCount = 0;
        for (int i = 0; i < numFields; i++)
        {
            int d = descBase + i * RunConstants.FieldDescSize;
            if ((file[d + 4] & (byte)FieldDescFlags.HasHlcOverride) != 0)
                overrideCount++;
        }
        int payloadBase = descBase + numFields * RunConstants.FieldDescSize
                          + overrideCount * RunConstants.FieldHlcOverrideSize;

        value = file.Slice(payloadBase + payloadOffset, valLen);
        return true;
    }
}
