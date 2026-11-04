using System.Buffers.Binary;
using Fld1.Core.Canon;
using Fld1.Core.Hash;
using Fld1.Core.Util;
using Fld1.Core.Uuid;
using Fld1.Run.Format;

namespace Fld1.Run.Builder;

/// <summary>
/// Deterministic FLD1 STATE run builder. Spec PART I–II.
/// Inline values only in this iteration (len ≤ 256). External blobs next.
/// </summary>
public sealed class RunBuilder
{
    private readonly uint _namespaceId;
    private readonly List<FieldWrite> _writes = new();
    private readonly uint _inlinePolicy;

    public RunBuilder(uint namespaceId, uint inlinePolicy = RunConstants.MaxInlineValueLen)
    {
        if (namespaceId > 0xFFFF)
            throw new ArgumentOutOfRangeException(nameof(namespaceId));
        if (inlinePolicy > RunConstants.MaxInlineValueLen)
            throw new ArgumentOutOfRangeException(nameof(inlinePolicy));
        _namespaceId = namespaceId;
        _inlinePolicy = inlinePolicy;
    }

    public RunBuilder Add(FieldWrite write)
    {
        if (write.Value.Length > _inlinePolicy && write.Value.Length <= RunConstants.MaxBlobLen)
            throw new NotSupportedException("External values not yet implemented in this iteration");
        if (write.Value.Length > RunConstants.MaxBlobLen)
            throw new ArgumentException("value > 4 MB");
        _writes.Add(write);
        return this;
    }

    public RunBuilder Add(
        ReadOnlySpan<byte> recordId,
        ushort fieldId,
        ReadOnlySpan<byte> value,
        ulong wall,
        uint logical,
        ReadOnlySpan<byte> authorNodeId,
        ushort keyEpoch = 0,
        bool isTombstone = false)
    {
        return Add(new FieldWrite(recordId, fieldId, value, wall, logical, authorNodeId, keyEpoch, isTombstone));
    }

    public byte[] Build()
    {
        if (_writes.Count == 0)
            return EmptyRunBuilder.Create(_namespaceId);

        // --- LWW collapse: one winner per (RecordId, FieldId) ---
        // Winner = max (Wall, Logical, AuthorNodeId BE)
        var winners = new Dictionary<(IdKey, ushort), FieldWrite>();
        foreach (var w in _writes)
        {
            var key = (new IdKey(w.RecordId), w.FieldId);
            if (!winners.TryGetValue(key, out var cur) || CompareHlc(w, cur) > 0)
                winners[key] = w;
        }

        // Group by RecordId, sort RecordId ASC (BE)
        var byRecord = winners.Values
            .GroupBy(w => new IdKey(w.RecordId))
            .OrderBy(g => g.Key, IdKeyComparer.Instance)
            .ToList();

        // Intern authors: unique sorted ASC
        var authorSet = new SortedSet<IdKey>(IdKeyComparer.Instance);
        foreach (var w in winners.Values)
            authorSet.Add(new IdKey(w.AuthorNodeId));
        var authors = authorSet.ToList(); // index 0 → internId 1
        var authorToIntern = new Dictionary<IdKey, uint>(IdKeyComparer.Instance);
        for (int i = 0; i < authors.Count; i++)
            authorToIntern[authors[i]] = (uint)(i + 1);

        // Build cluster blobs in order
        var clusterBlobs = new List<byte[]>();
        var clusterRecordIds = new List<byte[]>();
        uint totalFields = 0;
        uint aliveFields = 0;
        bool anyHlcOverride = false;
        bool anyKeyEpoch = false;

        foreach (var group in byRecord)
        {
            var fields = group.OrderBy(f => f.FieldId).ToList();
            // Dup FieldId already collapsed by LWW dict
            totalFields += (uint)fields.Count;

            // Mode for BaseWall/Logical/InternId
            var mode = ComputeMode(fields, authorToIntern);

            // Overrides: field differs from mode OR keyEpoch != 0
            var overrides = new List<(int idx, FieldWrite f, uint internId)>();
            for (int i = 0; i < fields.Count; i++)
            {
                var f = fields[i];
                uint iid = authorToIntern[new IdKey(f.AuthorNodeId)];
                bool need = f.Wall != mode.Wall || f.Logical != mode.Logical || iid != mode.InternId || f.KeyEpoch != 0;
                if (need)
                    overrides.Add((i, f, iid));
            }
            if (overrides.Count > 0) anyHlcOverride = true;
            if (fields.Any(f => f.KeyEpoch != 0)) anyKeyEpoch = true;

            // Lifecycle DEAD?
            bool dead = false;
            foreach (var f in fields)
            {
                if (f.FieldId == 0 && f.Value.Length == 1 && f.Value[0] == 0x01)
                {
                    dead = true;
                    break;
                }
            }

            // Count alive (non-tombstone for contributor; simplified: non-tombstone)
            foreach (var f in fields)
            {
                if (!f.IsTombstone) aliveFields++;
            }

            // Payload area: concatenate values FieldId ASC, no gaps
            int payloadLen = 0;
            foreach (var f in fields)
            {
                if (f.IsTombstone)
                    continue; // tombstone has no value bytes in payload; ValLen=0
                payloadLen += f.Value.Length;
            }
            if (payloadLen > RunConstants.MaxPayloadArea)
                throw new InvalidOperationException("payloadArea > 65535");

            int descBytes = fields.Count * RunConstants.FieldDescSize;
            int ovBytes = overrides.Count * RunConstants.FieldHlcOverrideSize;
            int clusterSize = RunConstants.ClusterHeaderSize + descBytes + ovBytes + payloadLen;
            int pad = (8 - (clusterSize % 8)) % 8;
            clusterSize += pad;

            var blob = new byte[clusterSize];
            var span = blob.AsSpan();

            // Cluster header
            group.Key.CopyTo(span.Slice(0, 16));
            BinaryPrimitives.WriteUInt16LittleEndian(span.Slice(16), (ushort)fields.Count);
            ushort cflags = dead ? (ushort)ClusterFlags.Dead : (ushort)0;
            BinaryPrimitives.WriteUInt16LittleEndian(span.Slice(18), cflags);
            BinaryPrimitives.WriteUInt64LittleEndian(span.Slice(20), mode.Wall);
            BinaryPrimitives.WriteUInt32LittleEndian(span.Slice(28), mode.Logical);
            BinaryPrimitives.WriteUInt32LittleEndian(span.Slice(32), mode.InternId);
            // reserved 4 at 36 = 0

            // FieldDesc[]
            int payloadOff = 0;
            int descBase = RunConstants.ClusterHeaderSize;
            for (int i = 0; i < fields.Count; i++)
            {
                var f = fields[i];
                int d = descBase + i * RunConstants.FieldDescSize;
                BinaryPrimitives.WriteUInt16LittleEndian(span.Slice(d), f.FieldId);
                ushort valLen;
                ushort off;
                byte fdFlags = 0;
                if (f.IsTombstone)
                {
                    fdFlags |= (byte)FieldDescFlags.Tombstone;
                    valLen = 0;
                    off = 0;
                }
                else
                {
                    valLen = (ushort)f.Value.Length;
                    off = (ushort)payloadOff;
                    payloadOff += f.Value.Length;
                }
                if (overrides.Any(o => o.idx == i))
                    fdFlags |= (byte)FieldDescFlags.HasHlcOverride;

                BinaryPrimitives.WriteUInt16LittleEndian(span.Slice(d + 2), valLen);
                span[d + 4] = fdFlags;
                span[d + 5] = 0; // reserved
                BinaryPrimitives.WriteUInt16LittleEndian(span.Slice(d + 6), off);
            }

            // FieldHlcOverride[] FieldIdx ASC (already in order)
            int ovBase = descBase + descBytes;
            for (int oi = 0; oi < overrides.Count; oi++)
            {
                var (idx, f, iid) = overrides[oi];
                int o = ovBase + oi * RunConstants.FieldHlcOverrideSize;
                BinaryPrimitives.WriteUInt16LittleEndian(span.Slice(o), (ushort)idx);
                BinaryPrimitives.WriteUInt16LittleEndian(span.Slice(o + 2), f.KeyEpoch);
                BinaryPrimitives.WriteUInt64LittleEndian(span.Slice(o + 4), f.Wall);
                BinaryPrimitives.WriteUInt32LittleEndian(span.Slice(o + 12), f.Logical);
                BinaryPrimitives.WriteUInt32LittleEndian(span.Slice(o + 16), iid);
                // pad 4 at +20 = 0
            }

            // payload
            int payBase = ovBase + ovBytes;
            int cursor = 0;
            foreach (var f in fields)
            {
                if (f.IsTombstone) continue;
                f.Value.CopyTo(span.Slice(payBase + cursor, f.Value.Length));
                cursor += f.Value.Length;
            }

            clusterBlobs.Add(blob);
            clusterRecordIds.Add(group.Key.Bytes);
        }

        // --- Layout ---
        // intern @ 256
        ulong internOff = RunConstants.HeaderSize;
        int internBytes = authors.Count * RunConstants.InternEntrySize;

        // PGM after intern
        ulong pgmOff = internOff + (ulong)internBytes;
        // Simple single-segment PGM covering all clusters (valid, deterministic)
        int pgmSegments = clusterBlobs.Count == 0 ? 0 : 1;
        int pgmBytes = pgmSegments * RunConstants.PgmSegmentSize;
        uint pgmEpsilon = 32;

        // slots after pgm
        ulong slotOff = pgmOff + (ulong)pgmBytes;
        int slotBytes = clusterBlobs.Count * RunConstants.CompactSlotSize;

        // data after slots
        ulong dataOff = slotOff + (ulong)slotBytes;
        int dataBytes = 0;
        foreach (var b in clusterBlobs) dataBytes += b.Length;

        // no external values in this iteration
        ulong valuesOff = dataOff + (ulong)dataBytes;
        // pad data end to 8 for valuesOff alignment if needed
        // Spec: valuesOff % 8 == 0 when HAS_VALUES; for no external, valuesOff == footerOff
        // Align body end to 8
        ulong bodyEnd = valuesOff;
        ulong footerOff = (bodyEnd + 7) & ~7UL;
        ulong valuesLen = 0;
        // HAS_VALUES = 0 path: valuesOff == footerOff
        valuesOff = footerOff;

        int fileSize = (int)footerOff + RunConstants.FooterSize;
        var file = new byte[fileSize];
        var fs = file.AsSpan();

        // Intern table
        for (int i = 0; i < authors.Count; i++)
            authors[i].Bytes.CopyTo(fs.Slice((int)internOff + i * 16, 16));

        // PGM single segment
        if (pgmSegments == 1)
        {
            // StartKey = K(first), StartIdx=0, SegEps=0 (use default), Num/Den for constant or linear
            // For single segment over sorted keys: Predict uses linear from first to last
            ulong k0 = Uuid7.ProjectK(clusterRecordIds[0]);
            BinaryPrimitives.WriteUInt64LittleEndian(fs.Slice((int)pgmOff, 8), k0);
            BinaryPrimitives.WriteUInt32LittleEndian(fs.Slice((int)pgmOff + 8, 4), 0); // StartIdx
            BinaryPrimitives.WriteUInt16LittleEndian(fs.Slice((int)pgmOff + 12, 2), 0); // SegEps=0 → default
            // Reserved 2 = 0
            // Num/Den: if one cluster, Den=1 Num=0; else slope
            if (clusterBlobs.Count == 1)
            {
                BinaryPrimitives.WriteUInt64LittleEndian(fs.Slice((int)pgmOff + 16, 8), 0); // Num
                BinaryPrimitives.WriteUInt64LittleEndian(fs.Slice((int)pgmOff + 24, 8), 1); // Den
            }
            else
            {
                ulong kLast = Uuid7.ProjectK(clusterRecordIds[^1]);
                ulong spanK = kLast - k0;
                ulong spanI = (ulong)(clusterBlobs.Count - 1);
                // p = (k-k0)*Num/Den ≈ index; Num=spanI, Den=spanK (or 1 if 0)
                if (spanK == 0)
                {
                    BinaryPrimitives.WriteUInt64LittleEndian(fs.Slice((int)pgmOff + 16, 8), 0);
                    BinaryPrimitives.WriteUInt64LittleEndian(fs.Slice((int)pgmOff + 24, 8), 1);
                }
                else
                {
                    BinaryPrimitives.WriteUInt64LittleEndian(fs.Slice((int)pgmOff + 16, 8), spanI);
                    BinaryPrimitives.WriteUInt64LittleEndian(fs.Slice((int)pgmOff + 24, 8), spanK);
                }
            }
        }

        // Slots + data
        int dataCursor = (int)dataOff;
        for (int i = 0; i < clusterBlobs.Count; i++)
        {
            int slotPos = (int)slotOff + i * RunConstants.CompactSlotSize;
            BinaryPrimitives.WriteUInt64LittleEndian(fs.Slice(slotPos, 8), (ulong)dataCursor);
            uint shortKey = Uuid7.ShortKey(clusterRecordIds[i]);
            // ShortKey stored as 4 bytes — raw RecordId[12..15], which is BE interpretation
            // Spec: ShortKey = RecordId[12..15]. Write as raw bytes (not host endian concern for identity)
            clusterRecordIds[i].AsSpan(12, 4).CopyTo(fs.Slice(slotPos + 8, 4));
            // pad 4 = 0

            clusterBlobs[i].CopyTo(fs.Slice(dataCursor));
            dataCursor += clusterBlobs[i].Length;
        }

        // Flags
        ushort runFlags = 0;
        if (anyHlcOverride) runFlags |= (ushort)RunFlags.HasHlcOverrides;
        if (anyKeyEpoch) runFlags |= (ushort)RunFlags.HasKeyEpochs;
        // HAS_VALUES = 0 (no external section)

        // rootDigest: BLAKE3(tag ‖ ns ‖ body without header digests area?)
        // Spec: Run StateDigest = BLAKE3-256("FLD1-RUNSTATE-v1" ‖ ns ‖ body)
        // For empty: tag ‖ ns only. For non-empty we hash tag ‖ ns ‖ content after header.
        // Practical approach matching empty: tag ‖ ns LE ‖ file[256..footerOff)
        // But footer order says: compute digests from content, write into header, then xxhash.
        // Use: body = intern+pgm+slots+data (from internOff to footerOff)
        Span<byte> nsLe = stackalloc byte[4];
        BinaryPrimitives.WriteUInt32LittleEndian(nsLe, _namespaceId);

        // Temporary: write header with zero digest, then compute
        Span<byte> digest = stackalloc byte[32];
        // Hash tag ‖ ns ‖ bytes[internOff..footerOff)
        using (var _ = new DigestScope())
        {
            // Build hash input
            var bodySlice = fs.Slice((int)internOff, (int)(footerOff - internOff));
            // Blake3 tagged: tag + ns + body
            var hasherData = new byte[Tags.RunStateV1.Length + 4 + bodySlice.Length];
            Tags.RunStateV1.CopyTo(hasherData);
            nsLe.CopyTo(hasherData.AsSpan(Tags.RunStateV1.Length));
            bodySlice.CopyTo(hasherData.AsSpan(Tags.RunStateV1.Length + 4));
            Hashes.Blake3_256(hasherData, digest);
        }

        RunHeader.WriteCore(
            fs.Slice(0, RunHeader.Size),
            _namespaceId,
            runFlags,
            numClusters: (uint)clusterBlobs.Count,
            numFields: totalFields,
            internCount: (uint)authors.Count,
            pgmSegments: (uint)pgmSegments,
            pgmEpsilon: pgmEpsilon,
            internOff, pgmOff, slotOff, dataOff,
            valuesOff, valuesLen, footerOff,
            numExternal: 0,
            aliveFields: aliveFields,
            contributorCount: aliveFields, // no ghosts/retained tombs tracked yet
            ghostCount: 0,
            digest);

        // Footer xxHash64
        ulong hash = Hashes.XxHash64(fs.Slice(0, (int)footerOff));
        BinaryPrimitives.WriteUInt64LittleEndian(fs.Slice((int)footerOff, 8), hash);

        return file;
    }

    private static int CompareHlc(FieldWrite a, FieldWrite b)
    {
        int c = a.Wall.CompareTo(b.Wall);
        if (c != 0) return c;
        c = a.Logical.CompareTo(b.Logical);
        if (c != 0) return c;
        return ByteUtil.CompareUnsigned16(a.AuthorNodeId, b.AuthorNodeId);
    }

    private static (ulong Wall, uint Logical, uint InternId) ComputeMode(
        List<FieldWrite> fields,
        Dictionary<IdKey, uint> authorToIntern)
    {
        // Mode = most frequent (Wall, Logical, InternId) triple; tie → first in FieldId order
        var counts = new Dictionary<(ulong, uint, uint), int>();
        (ulong, uint, uint) first = default;
        bool hasFirst = false;
        foreach (var f in fields)
        {
            uint iid = authorToIntern[new IdKey(f.AuthorNodeId)];
            var t = (f.Wall, f.Logical, iid);
            if (!hasFirst) { first = t; hasFirst = true; }
            counts[t] = counts.GetValueOrDefault(t) + 1;
        }
        int best = -1;
        var mode = first;
        foreach (var kv in counts)
        {
            if (kv.Value > best)
            {
                best = kv.Value;
                mode = kv.Key;
            }
        }
        return mode;
    }

    private ref struct DigestScope { public void Dispose() { } }

    /// <summary>16-byte id key for dictionaries/sorting (unsigned BE compare).</summary>
    private readonly struct IdKey : IComparable<IdKey>
    {
        public readonly byte[] Bytes;
        public IdKey(byte[] bytes) => Bytes = bytes;
        public int CompareTo(IdKey other) => ByteUtil.CompareUnsigned16(Bytes, other.Bytes);
        public void CopyTo(Span<byte> dest) => Bytes.CopyTo(dest);
        public override bool Equals(object? obj) => obj is IdKey k && ByteUtil.Equals16(Bytes, k.Bytes);
        public override int GetHashCode()
        {
            ulong a = BinaryPrimitives.ReadUInt64LittleEndian(Bytes);
            ulong b = BinaryPrimitives.ReadUInt64LittleEndian(Bytes.AsSpan(8));
            return HashCode.Combine(a, b);
        }
    }

    private sealed class IdKeyComparer : IComparer<IdKey>, IEqualityComparer<IdKey>
    {
        public static readonly IdKeyComparer Instance = new();
        public int Compare(IdKey x, IdKey y) => x.CompareTo(y);
        public bool Equals(IdKey x, IdKey y) => ByteUtil.Equals16(x.Bytes, y.Bytes);
        public int GetHashCode(IdKey obj) => obj.GetHashCode();
    }
}
