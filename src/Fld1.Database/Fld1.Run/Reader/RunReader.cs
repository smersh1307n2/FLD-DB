using System.Buffers.Binary;
using Fld1.Core.Hash;
using Fld1.Core.Util;
using Fld1.Core.Uuid;
using Fld1.Run.Format;
using Fld1.Run.Verify;

namespace Fld1.Run.Reader;

/// <summary>Resolved field from a run (for LWW merge across layers).</summary>
public readonly struct RunFieldState
{
    public readonly bool Found;
    public readonly bool IsTombstone;
    public readonly ushort FieldId;
    public readonly ushort KeyEpoch;
    public readonly ulong Wall;
    public readonly uint Logical;
    public readonly ReadOnlyMemory<byte> AuthorNodeId;
    public readonly ReadOnlyMemory<byte> Value;

    public RunFieldState(
        bool found, bool isTombstone, ushort fieldId, ushort keyEpoch,
        ulong wall, uint logical, ReadOnlyMemory<byte> author, ReadOnlyMemory<byte> value)
    {
        Found = found;
        IsTombstone = isTombstone;
        FieldId = fieldId;
        KeyEpoch = keyEpoch;
        Wall = wall;
        Logical = logical;
        AuthorNodeId = author;
        Value = value;
    }

    public static RunFieldState NotFound => default;
}

/// <summary>
/// Zero-copy reader over an immutable FLD1 run buffer.
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

    public bool TryGet(ReadOnlySpan<byte> recordId, ushort fieldId, out ReadOnlySpan<byte> value)
    {
        value = default;
        if (recordId.Length != 16 || IsEmpty) return false;

        var file = _file.Span;
        int clusterIdx = FindCluster(file, recordId);
        if (clusterIdx < 0) return false;

        ulong slotOff = RunHeader.ReadSlotOff(file);
        int slotPos = (int)slotOff + clusterIdx * RunConstants.CompactSlotSize;
        ulong dataOffset = BinaryPrimitives.ReadUInt64LittleEndian(file.Slice(slotPos, 8));
        return TryGetFromCluster(file, (int)dataOffset, fieldId, out value);
    }

    /// <summary>
    /// Get field including HLC/author/tombstone for cross-run LWW.
    /// </summary>
    public bool TryGetField(ReadOnlySpan<byte> recordId, ushort fieldId, out RunFieldState state)
    {
        state = RunFieldState.NotFound;
        if (recordId.Length != 16 || IsEmpty) return false;
        var file = _file.Span;
        int clusterIdx = FindCluster(file, recordId);
        if (clusterIdx < 0) return false;

        ulong slotOff = RunHeader.ReadSlotOff(file);
        int slotPos = (int)slotOff + clusterIdx * RunConstants.CompactSlotSize;
        ulong dataOffset = BinaryPrimitives.ReadUInt64LittleEndian(file.Slice(slotPos, 8));
        return TryGetFieldFromCluster(file, (int)dataOffset, fieldId, out state);
    }

    private static int FindCluster(ReadOnlySpan<byte> file, ReadOnlySpan<byte> recordId)
    {
        uint n = RunHeader.ReadNumClusters(file);
        if (n == 0) return -1;

        ulong slotOff = RunHeader.ReadSlotOff(file);
        uint shortKey = Uuid7.ShortKey(recordId);

        // PGM window when available, else full scan
        int lo = 0, hi = (int)n - 1;
        uint segs = RunHeader.ReadPgmSegments(file);
        if (segs > 0)
        {
            ulong k = Uuid7.ProjectK(recordId);
            var (wLo, wHi) = PgmWindow(file, k, (int)n);
            lo = wLo;
            hi = wHi;
        }

        for (int i = lo; i <= hi; i++)
        {
            int slotPos = (int)slotOff + i * RunConstants.CompactSlotSize;
            uint sk = BinaryPrimitives.ReadUInt32BigEndian(file.Slice(slotPos + 8, 4));
            if (sk != shortKey) continue;

            ulong off = BinaryPrimitives.ReadUInt64LittleEndian(file.Slice(slotPos, 8));
            if (ByteUtil.Equals16(file.Slice((int)off, 16), recordId))
                return i;
        }

        // Fallback full scan if PGM miss (spec [A9-PGM-TRUTH])
        if (segs > 0)
        {
            for (int i = 0; i < (int)n; i++)
            {
                if (i >= lo && i <= hi) continue;
                int slotPos = (int)slotOff + i * RunConstants.CompactSlotSize;
                uint sk = BinaryPrimitives.ReadUInt32BigEndian(file.Slice(slotPos + 8, 4));
                if (sk != shortKey) continue;
                ulong off = BinaryPrimitives.ReadUInt64LittleEndian(file.Slice(slotPos, 8));
                if (ByteUtil.Equals16(file.Slice((int)off, 16), recordId))
                    return i;
            }
        }
        return -1;
    }

    private static (int lo, int hi) PgmWindow(ReadOnlySpan<byte> file, ulong k, int n)
    {
        uint segs = RunHeader.ReadPgmSegments(file);
        uint defaultEps = RunHeader.ReadPgmEpsilon(file);
        ulong pgmOff = RunHeader.ReadPgmOff(file);

        // Find segment: last with StartKey <= k, or first
        int segIdx = 0;
        for (int s = 0; s < (int)segs; s++)
        {
            ulong startKey = BinaryPrimitives.ReadUInt64LittleEndian(
                file.Slice((int)pgmOff + s * RunConstants.PgmSegmentSize, 8));
            if (startKey <= k) segIdx = s;
            else break;
        }

        int spos = (int)pgmOff + segIdx * RunConstants.PgmSegmentSize;
        ulong segStartKey = BinaryPrimitives.ReadUInt64LittleEndian(file.Slice(spos, 8));
        uint startIdx = BinaryPrimitives.ReadUInt32LittleEndian(file.Slice(spos + 8, 4));
        ushort segEps = BinaryPrimitives.ReadUInt16LittleEndian(file.Slice(spos + 12, 2));
        ulong num = BinaryPrimitives.ReadUInt64LittleEndian(file.Slice(spos + 16, 8));
        ulong den = BinaryPrimitives.ReadUInt64LittleEndian(file.Slice(spos + 24, 8));
        if (den == 0) den = 1;

        ulong pred;
        if (k <= segStartKey)
            pred = startIdx;
        else
        {
            UInt128 p = (UInt128)(k - segStartKey) * num / den;
            pred = startIdx + (ulong)p;
        }

        uint eps = segEps != 0 ? segEps : defaultEps;
        long lo = (long)pred - (long)eps;
        long hi = (long)pred + (long)eps;
        if (lo < 0) lo = 0;
        if (hi >= n) hi = n - 1;
        return ((int)lo, (int)hi);
    }

    private static bool TryGetFromCluster(
        ReadOnlySpan<byte> file, int clusterOff, ushort fieldId, out ReadOnlySpan<byte> value)
    {
        value = default;
        var ch = file.Slice(clusterOff);
        ushort numFields = BinaryPrimitives.ReadUInt16LittleEndian(ch.Slice(16));
        int descBase = clusterOff + RunConstants.ClusterHeaderSize;

        int lo = 0, hi = numFields - 1, found = -1;
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

        ushort payloadOrRef = BinaryPrimitives.ReadUInt16LittleEndian(file.Slice(desc + 6, 2));

        if (valLen == RunConstants.ExternalValLen)
        {
            // Offset = ValueRef index
            ulong valuesOff = RunHeader.ReadValuesOff(file);
            int refPos = (int)valuesOff + payloadOrRef * RunConstants.ValueRefSize;
            ulong dataOff = BinaryPrimitives.ReadUInt64LittleEndian(file.Slice(refPos, 8));
            uint length = BinaryPrimitives.ReadUInt32LittleEndian(file.Slice(refPos + 8, 4));
            uint crc = BinaryPrimitives.ReadUInt32LittleEndian(file.Slice(refPos + 12, 4));
            if (length > RunConstants.MaxBlobLen) return false;
            value = file.Slice((int)dataOff, (int)length);
            // Optional CRC check on first read
            if (!Hashes.IsCrc32COrLegacy(value, crc))
                return false;
            return true;
        }

        // inline
        int overrideCount = 0;
        for (int i = 0; i < numFields; i++)
        {
            int d = descBase + i * RunConstants.FieldDescSize;
            if ((file[d + 4] & (byte)FieldDescFlags.HasHlcOverride) != 0)
                overrideCount++;
        }
        int payloadBase = descBase + numFields * RunConstants.FieldDescSize
                          + overrideCount * RunConstants.FieldHlcOverrideSize;
        value = file.Slice(payloadBase + payloadOrRef, valLen);
        return true;
    }

    private bool TryGetFieldFromCluster(
        ReadOnlySpan<byte> file, int clusterOff, ushort fieldId, out RunFieldState state)
    {
        state = RunFieldState.NotFound;
        ushort numFields = BinaryPrimitives.ReadUInt16LittleEndian(file.Slice(clusterOff + 16, 2));
        ulong modeWall = BinaryPrimitives.ReadUInt64LittleEndian(file.Slice(clusterOff + 20, 8));
        uint modeLogical = BinaryPrimitives.ReadUInt32LittleEndian(file.Slice(clusterOff + 28, 4));
        uint modeIntern = BinaryPrimitives.ReadUInt32LittleEndian(file.Slice(clusterOff + 32, 4));

        int descBase = clusterOff + RunConstants.ClusterHeaderSize;
        int lo = 0, hi = numFields - 1, found = -1;
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
        bool tomb = (flags & (byte)FieldDescFlags.Tombstone) != 0;
        ushort payloadOrRef = BinaryPrimitives.ReadUInt16LittleEndian(file.Slice(desc + 6, 2));

        ulong wall = modeWall;
        uint logical = modeLogical;
        uint internId = modeIntern;
        ushort keyEpoch = 0;

        int overrideCount = 0;
        for (int i = 0; i < numFields; i++)
        {
            int d = descBase + i * RunConstants.FieldDescSize;
            if ((file[d + 4] & (byte)FieldDescFlags.HasHlcOverride) != 0)
                overrideCount++;
        }
        int ovBase = descBase + numFields * RunConstants.FieldDescSize;
        if ((flags & (byte)FieldDescFlags.HasHlcOverride) != 0)
        {
            for (int oi = 0; oi < overrideCount; oi++)
            {
                int o = ovBase + oi * RunConstants.FieldHlcOverrideSize;
                ushort idx2 = BinaryPrimitives.ReadUInt16LittleEndian(file.Slice(o, 2));
                if (idx2 == found)
                {
                    keyEpoch = BinaryPrimitives.ReadUInt16LittleEndian(file.Slice(o + 2, 2));
                    wall = BinaryPrimitives.ReadUInt64LittleEndian(file.Slice(o + 4, 8));
                    logical = BinaryPrimitives.ReadUInt32LittleEndian(file.Slice(o + 12, 4));
                    internId = BinaryPrimitives.ReadUInt32LittleEndian(file.Slice(o + 16, 4));
                    break;
                }
            }
        }

        ReadOnlyMemory<byte> author = ReadOnlyMemory<byte>.Empty;
        if (internId > 0)
        {
            ulong internOff = RunHeader.ReadInternOff(file);
            uint internCount = RunHeader.ReadInternCount(file);
            if (internId <= internCount)
            {
                int aoff = (int)internOff + (int)(internId - 1) * RunConstants.InternEntrySize;
                author = _file.Slice(aoff, 16);
            }
        }

        ReadOnlyMemory<byte> valueMem = ReadOnlyMemory<byte>.Empty;
        if (!tomb)
        {
            if (valLen == RunConstants.ExternalValLen)
            {
                ulong valuesOff = RunHeader.ReadValuesOff(file);
                int refPos = (int)valuesOff + payloadOrRef * RunConstants.ValueRefSize;
                ulong dataOff = BinaryPrimitives.ReadUInt64LittleEndian(file.Slice(refPos, 8));
                uint length = BinaryPrimitives.ReadUInt32LittleEndian(file.Slice(refPos + 8, 4));
                uint crc = BinaryPrimitives.ReadUInt32LittleEndian(file.Slice(refPos + 12, 4));
                if (length <= RunConstants.MaxBlobLen)
                {
                    var slice = _file.Slice((int)dataOff, (int)length);
                    if (Hashes.IsCrc32COrLegacy(slice.Span, crc))
                        valueMem = slice;
                    else
                        return false;
                }
            }
            else
            {
                int payloadBase2 = ovBase + overrideCount * RunConstants.FieldHlcOverrideSize;
                valueMem = _file.Slice(payloadBase2 + payloadOrRef, valLen);
            }
        }

        state = new RunFieldState(true, tomb, fieldId, keyEpoch, wall, logical, author, valueMem);
        return true;
    }




    /// <summary>
    /// Zero-copy scan of all non-tombstone fields in a cluster (RecordId).
    /// Callback receives (fieldId, value span into file buffer).
    /// </summary>
    public void ScanRecord(ReadOnlySpan<byte> recordId, Action<ushort, ReadOnlySpan<byte>> onField)
    {
        if (recordId.Length != 16 || IsEmpty) return;
        var file = _file.Span;
        int clusterIdx = FindCluster(file, recordId);
        if (clusterIdx < 0) return;

        ulong slotOff = RunHeader.ReadSlotOff(file);
        int slotPos = (int)slotOff + clusterIdx * RunConstants.CompactSlotSize;
        ulong dataOffset = BinaryPrimitives.ReadUInt64LittleEndian(file.Slice(slotPos, 8));
        ScanClusterFields(file, (int)dataOffset, onField);
    }

    /// <summary>
    /// Enumerate every stored FieldId for a record, including tombstone descriptors.
    /// Used by compaction; user-visible scans continue to omit tombstones.
    /// </summary>
    public void ScanFieldIds(ReadOnlySpan<byte> recordId, Action<ushort> onFieldId)
    {
        ArgumentNullException.ThrowIfNull(onFieldId);
        if (recordId.Length != 16 || IsEmpty) return;

        var file = _file.Span;
        int clusterIdx = FindCluster(file, recordId);
        if (clusterIdx < 0) return;

        ulong slotOff = RunHeader.ReadSlotOff(file);
        int slotPos = (int)slotOff + clusterIdx * RunConstants.CompactSlotSize;
        ulong dataOffset = BinaryPrimitives.ReadUInt64LittleEndian(file.Slice(slotPos, 8));
        int clusterOff = checked((int)dataOffset);
        ushort numFields = BinaryPrimitives.ReadUInt16LittleEndian(file.Slice(clusterOff + 16, 2));
        int descBase = clusterOff + RunConstants.ClusterHeaderSize;

        for (int i = 0; i < numFields; i++)
        {
            int desc = descBase + i * RunConstants.FieldDescSize;
            ushort fieldId = BinaryPrimitives.ReadUInt16LittleEndian(file.Slice(desc, 2));
            onFieldId(fieldId);
        }
    }

    /// <summary>
    /// Enumerate every cluster RecordId (16B copied into buffer via callback).
    /// </summary>
    public void ScanRecordIds(Action<ReadOnlySpan<byte>> onRecordId)
    {
        if (IsEmpty) return;
        var file = _file.Span;
        uint n = RunHeader.ReadNumClusters(file);
        ulong slotOff = RunHeader.ReadSlotOff(file);
        for (uint i = 0; i < n; i++)
        {
            int slotPos = (int)slotOff + (int)i * RunConstants.CompactSlotSize;
            ulong off = BinaryPrimitives.ReadUInt64LittleEndian(file.Slice(slotPos, 8));
            onRecordId(file.Slice((int)off, 16));
        }
    }

    private static void ScanClusterFields(
        ReadOnlySpan<byte> file, int clusterOff, Action<ushort, ReadOnlySpan<byte>> onField)
    {
        var ch = file.Slice(clusterOff);
        ushort numFields = BinaryPrimitives.ReadUInt16LittleEndian(ch.Slice(16));
        int descBase = clusterOff + RunConstants.ClusterHeaderSize;

        int overrideCount = 0;
        for (int i = 0; i < numFields; i++)
        {
            int d = descBase + i * RunConstants.FieldDescSize;
            if ((file[d + 4] & (byte)FieldDescFlags.HasHlcOverride) != 0)
                overrideCount++;
        }
        int payloadBase = descBase + numFields * RunConstants.FieldDescSize
                          + overrideCount * RunConstants.FieldHlcOverrideSize;

        for (int i = 0; i < numFields; i++)
        {
            int d = descBase + i * RunConstants.FieldDescSize;
            ushort fieldId = BinaryPrimitives.ReadUInt16LittleEndian(file.Slice(d, 2));
            ushort valLen = BinaryPrimitives.ReadUInt16LittleEndian(file.Slice(d + 2, 2));
            byte flags = file[d + 4];
            if ((flags & (byte)FieldDescFlags.Tombstone) != 0)
                continue;
            ushort payloadOrRef = BinaryPrimitives.ReadUInt16LittleEndian(file.Slice(d + 6, 2));

            ReadOnlySpan<byte> value;
            if (valLen == RunConstants.ExternalValLen)
            {
                ulong valuesOff = RunHeader.ReadValuesOff(file);
                int refPos = (int)valuesOff + payloadOrRef * RunConstants.ValueRefSize;
                ulong dataOff = BinaryPrimitives.ReadUInt64LittleEndian(file.Slice(refPos, 8));
                uint length = BinaryPrimitives.ReadUInt32LittleEndian(file.Slice(refPos + 8, 4));
                uint crc = BinaryPrimitives.ReadUInt32LittleEndian(file.Slice(refPos + 12, 4));
                if (length > RunConstants.MaxBlobLen) continue;
                value = file.Slice((int)dataOff, (int)length);
                if (!Hashes.IsCrc32COrLegacy(value, crc)) continue;
            }
            else
            {
                value = file.Slice(payloadBase + payloadOrRef, valLen);
            }
            onField(fieldId, value);
        }
    }
}
