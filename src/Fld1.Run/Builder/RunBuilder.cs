using System.Buffers.Binary;
using Fld1.Core.Canon;
using Fld1.Core.Hash;
using Fld1.Core.Util;
using Fld1.Core.Uuid;
using Fld1.Run.Format;

namespace Fld1.Run.Builder;

/// <summary>
/// Deterministic FLD1 STATE run builder. Spec PART I–II.
/// Supports inline (≤ policy, default 256) and external values (≤ 4 MB).
/// </summary>
public sealed class RunBuilder
{
    private readonly uint _namespaceId;
    private readonly List<FieldWrite> _writes = new();
    private readonly uint _inlinePolicy;
    private readonly uint _pgmEpsilon;

    public RunBuilder(
        uint namespaceId,
        uint inlinePolicy = RunConstants.MaxInlineValueLen,
        uint pgmEpsilon = RunConstants.MaxPgmEpsilon)
    {
        if (namespaceId > 0xFFFF)
            throw new ArgumentOutOfRangeException(nameof(namespaceId));
        if (inlinePolicy > RunConstants.MaxInlineValueLen)
            throw new ArgumentOutOfRangeException(nameof(inlinePolicy));
        if (pgmEpsilon > 65535)
            throw new ArgumentOutOfRangeException(nameof(pgmEpsilon));
        _namespaceId = namespaceId;
        _inlinePolicy = inlinePolicy;
        _pgmEpsilon = pgmEpsilon;
    }

    public RunBuilder Add(FieldWrite write)
    {
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
        => Add(new FieldWrite(recordId, fieldId, value, wall, logical, authorNodeId, keyEpoch, isTombstone));

    public byte[] Build()
    {
        if (_writes.Count == 0)
            return EmptyRunBuilder.Create(_namespaceId);

        // LWW: one winner per (RecordId, FieldId)
        var winners = new Dictionary<(IdKey, ushort), FieldWrite>();
        foreach (var w in _writes)
        {
            var key = (new IdKey(w.RecordId), w.FieldId);
            if (!winners.TryGetValue(key, out var cur) || CompareHlc(w, cur) > 0)
                winners[key] = w;
        }

        var byRecord = winners.Values
            .GroupBy(w => new IdKey(w.RecordId))
            .OrderBy(g => g.Key, IdKeyComparer.Instance)
            .ToList();

        // Intern authors ASC
        var authorSet = new SortedSet<IdKey>(AuthorIdComparer.Instance);
        foreach (var w in winners.Values)
            authorSet.Add(new IdKey(w.AuthorNodeId));
        var authors = authorSet.ToList();
        var authorToIntern = new Dictionary<IdKey, uint>(IdKeyComparer.Instance);
        for (int i = 0; i < authors.Count; i++)
            authorToIntern[authors[i]] = (uint)(i + 1);

        // External value refs collected in (RecordId, FieldId) order during cluster build
        var externalEntries = new List<ExternalEntry>();

        var clusterBlobs = new List<byte[]>();
        var clusterRecordIds = new List<byte[]>();
        uint totalFields = 0;
        uint aliveFields = 0;
        bool anyHlcOverride = false;
        bool anyKeyEpoch = false;
        bool anyExternal = false;

        foreach (var group in byRecord)
        {
            var fields = group.OrderBy(f => f.FieldId).ToList();
            totalFields += (uint)fields.Count;
            var mode = ComputeMode(fields, authorToIntern);

            var overrides = new List<(int idx, FieldWrite f, uint internId)>();
            for (int i = 0; i < fields.Count; i++)
            {
                var f = fields[i];
                uint iid = authorToIntern[new IdKey(f.AuthorNodeId)];
                bool need = f.Wall != mode.Wall || f.Logical != mode.Logical
                            || iid != mode.InternId || f.KeyEpoch != 0;
                if (need) overrides.Add((i, f, iid));
            }
            if (overrides.Count > 0) anyHlcOverride = true;
            if (fields.Any(f => f.KeyEpoch != 0)) anyKeyEpoch = true;

            bool dead = fields.Any(f =>
                f.FieldId == 0 && f.Value.Length == 1 && f.Value[0] == 0x01);

            bool clusterHasExternal = false;
            int payloadLen = 0;
            foreach (var f in fields)
            {
                if (!f.IsTombstone) aliveFields++;
                if (f.IsTombstone) continue;
                if ((uint)f.Value.Length > _inlinePolicy)
                {
                    clusterHasExternal = true;
                    anyExternal = true;
                }
                else
                {
                    payloadLen += f.Value.Length;
                }
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

            group.Key.CopyTo(span.Slice(0, 16));
            BinaryPrimitives.WriteUInt16LittleEndian(span.Slice(16), (ushort)fields.Count);
            ushort cflags = 0;
            if (dead) cflags |= (ushort)ClusterFlags.Dead;
            if (clusterHasExternal) cflags |= (ushort)ClusterFlags.HasExternal;
            BinaryPrimitives.WriteUInt16LittleEndian(span.Slice(18), cflags);
            BinaryPrimitives.WriteUInt64LittleEndian(span.Slice(20), mode.Wall);
            BinaryPrimitives.WriteUInt32LittleEndian(span.Slice(28), mode.Logical);
            BinaryPrimitives.WriteUInt32LittleEndian(span.Slice(32), mode.InternId);

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
                else if ((uint)f.Value.Length > _inlinePolicy)
                {
                    valLen = RunConstants.ExternalValLen;
                    // ValueRef index assigned after all clusters — store placeholder, fix up later
                    off = (ushort)externalEntries.Count;
                    externalEntries.Add(new ExternalEntry(group.Key.Bytes, f.FieldId, f.Value));
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
                span[d + 5] = 0;
                BinaryPrimitives.WriteUInt16LittleEndian(span.Slice(d + 6), off);
            }

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
            }

            int payBase = ovBase + ovBytes;
            int cursor = 0;
            foreach (var f in fields)
            {
                if (f.IsTombstone) continue;
                if ((uint)f.Value.Length > _inlinePolicy) continue;
                f.Value.CopyTo(span.Slice(payBase + cursor, f.Value.Length));
                cursor += f.Value.Length;
            }

            clusterBlobs.Add(blob);
            clusterRecordIds.Add(group.Key.Bytes);
        }

        // Sort external entries by (RecordId, FieldId) for ValueRef catalog — already insertion order
        // matches FieldId ASC within RecordId ASC because we iterate byRecord sorted + fields sorted.
        // Spec requires ValueRef sorted (RecordId, FieldId) ASC — our collection order is correct.

        // --- Section layout ---
        ulong internOff = RunConstants.HeaderSize;
        int internBytes = authors.Count * RunConstants.InternEntrySize;

        // PGM multi-segment
        var pgmSegs = BuildPgm(clusterRecordIds, _pgmEpsilon);
        ulong pgmOff = internOff + (ulong)internBytes;
        int pgmBytes = pgmSegs.Count * RunConstants.PgmSegmentSize;

        ulong slotOff = pgmOff + (ulong)pgmBytes;
        int slotBytes = clusterBlobs.Count * RunConstants.CompactSlotSize;

        ulong dataOff = slotOff + (ulong)slotBytes;
        int dataBytes = 0;
        foreach (var b in clusterBlobs) dataBytes += b.Length;

        // Values section
        ulong valuesOff;
        ulong valuesLen;
        int valuesSectionBytes = 0;
        if (externalEntries.Count > 0)
        {
            // Align values section start to 8
            ulong dataEnd = dataOff + (ulong)dataBytes;
            valuesOff = (dataEnd + 7) & ~7UL;
            int refBytes = externalEntries.Count * RunConstants.ValueRefSize;
            int blobBytes = 0;
            foreach (var e in externalEntries) blobBytes += e.Value.Length;
            // valuesLen includes refs + blobs + pad to 8
            int raw = refBytes + blobBytes;
            int padded = (raw + 7) & ~7;
            valuesLen = (ulong)padded;
            valuesSectionBytes = padded;
        }
        else
        {
            ulong dataEnd = dataOff + (ulong)dataBytes;
            valuesOff = (dataEnd + 7) & ~7UL; // will equal footerOff
            valuesLen = 0;
        }

        ulong footerOff = valuesOff + valuesLen;
        // When no external: valuesOff == footerOff already
        if (externalEntries.Count == 0)
        {
            footerOff = valuesOff;
            valuesOff = footerOff;
        }

        int fileSize = (int)footerOff + RunConstants.FooterSize;
        var file = new byte[fileSize];
        var fs = file.AsSpan();

        // Intern
        for (int i = 0; i < authors.Count; i++)
            authors[i].Bytes.CopyTo(fs.Slice((int)internOff + i * 16, 16));

        // PGM segments
        for (int s = 0; s < pgmSegs.Count; s++)
        {
            var seg = pgmSegs[s];
            int pos = (int)pgmOff + s * RunConstants.PgmSegmentSize;
            BinaryPrimitives.WriteUInt64LittleEndian(fs.Slice(pos, 8), seg.StartKey);
            BinaryPrimitives.WriteUInt32LittleEndian(fs.Slice(pos + 8, 4), seg.StartIdx);
            BinaryPrimitives.WriteUInt16LittleEndian(fs.Slice(pos + 12, 2), seg.SegEps);
            // reserved 2 = 0
            BinaryPrimitives.WriteUInt64LittleEndian(fs.Slice(pos + 16, 8), seg.Num);
            BinaryPrimitives.WriteUInt64LittleEndian(fs.Slice(pos + 24, 8), seg.Den);
        }

        // Slots + data
        int dataCursor = (int)dataOff;
        for (int i = 0; i < clusterBlobs.Count; i++)
        {
            int slotPos = (int)slotOff + i * RunConstants.CompactSlotSize;
            BinaryPrimitives.WriteUInt64LittleEndian(fs.Slice(slotPos, 8), (ulong)dataCursor);
            clusterRecordIds[i].AsSpan(12, 4).CopyTo(fs.Slice(slotPos + 8, 4));
            clusterBlobs[i].CopyTo(fs.Slice(dataCursor));
            dataCursor += clusterBlobs[i].Length;
        }

        // Values section: ValueRef catalog then blob pool
        if (externalEntries.Count > 0)
        {
            int refBase = (int)valuesOff;
            int blobBase = refBase + externalEntries.Count * RunConstants.ValueRefSize;
            int blobCursor = 0;
            for (int i = 0; i < externalEntries.Count; i++)
            {
                var e = externalEntries[i];
                int rp = refBase + i * RunConstants.ValueRefSize;
                // DataOffset is relative to start of blob pool (or absolute from file start?)
                // Spec: DataOffset[8] in ValueRef — absolute file offset is more useful for MMF;
                // Spec says DataOffset[i+1] = DataOffset[i] + Length[i] continuous.
                // Use absolute file offsets for simplicity and zero-copy.
                ulong absOff = (ulong)(blobBase + blobCursor);
                BinaryPrimitives.WriteUInt64LittleEndian(fs.Slice(rp, 8), absOff);
                BinaryPrimitives.WriteUInt32LittleEndian(fs.Slice(rp + 8, 4), (uint)e.Value.Length);
                uint crc = Hashes.Crc32C(e.Value);
                BinaryPrimitives.WriteUInt32LittleEndian(fs.Slice(rp + 12, 4), crc);
                e.Value.CopyTo(fs.Slice(blobBase + blobCursor, e.Value.Length));
                blobCursor += e.Value.Length;
            }
            // remaining pad already zero
        }

        ushort runFlags = 0;
        if (anyExternal) runFlags |= (ushort)RunFlags.HasValues;
        if (anyHlcOverride) runFlags |= (ushort)RunFlags.HasHlcOverrides;
        if (anyKeyEpoch) runFlags |= (ushort)RunFlags.HasKeyEpochs;

        // rootDigest = BLAKE3(tag ‖ ns ‖ body[internOff..footerOff))
        Span<byte> nsLe = stackalloc byte[4];
        BinaryPrimitives.WriteUInt32LittleEndian(nsLe, _namespaceId);
        var bodySlice = fs.Slice((int)internOff, (int)(footerOff - internOff));
        var hashInput = new byte[Tags.RunStateV1.Length + 4 + bodySlice.Length];
        Tags.RunStateV1.CopyTo(hashInput);
        nsLe.CopyTo(hashInput.AsSpan(Tags.RunStateV1.Length));
        bodySlice.CopyTo(hashInput.AsSpan(Tags.RunStateV1.Length + 4));
        Span<byte> digest = stackalloc byte[32];
        Hashes.Blake3_256(hashInput, digest);

        RunHeader.WriteCore(
            fs.Slice(0, RunHeader.Size),
            _namespaceId,
            runFlags,
            numClusters: (uint)clusterBlobs.Count,
            numFields: totalFields,
            internCount: (uint)authors.Count,
            pgmSegments: (uint)pgmSegs.Count,
            pgmEpsilon: _pgmEpsilon,
            internOff, pgmOff, slotOff, dataOff,
            valuesOff, valuesLen, footerOff,
            numExternal: (uint)externalEntries.Count,
            aliveFields: aliveFields,
            contributorCount: aliveFields,
            ghostCount: 0,
            digest);

        ulong xh = Hashes.XxHash64(fs.Slice(0, (int)footerOff));
        BinaryPrimitives.WriteUInt64LittleEndian(fs.Slice((int)footerOff, 8), xh);
        return file;
    }

    /// <summary>
    /// Greedy PGM segments with dup-group indivisibility. Spec §2.1 / [K-PGM-DUP-RANGE].
    /// </summary>
    private static List<PgmSeg> BuildPgm(List<byte[]> recordIds, uint defaultEps)
    {
        var segs = new List<PgmSeg>();
        if (recordIds.Count == 0) return segs;

        var keys = new ulong[recordIds.Count];
        for (int i = 0; i < recordIds.Count; i++)
            keys[i] = Uuid7.ProjectK(recordIds[i]);

        // Build dup-group boundaries: [start, end) ranges of equal K
        var groups = new List<(int start, int end)>();
        int gStart = 0;
        for (int i = 1; i <= keys.Length; i++)
        {
            if (i == keys.Length || keys[i] != keys[gStart])
            {
                groups.Add((gStart, i));
                gStart = i;
            }
        }

        int segStart = 0;
        int groupIdx = 0;
        while (groupIdx < groups.Count)
        {
            int start = groups[groupIdx].start;
            int end = groups[groupIdx].end; // exclusive, at least one group
            // Expand greedily by whole groups while max error ≤ defaultEps
            int nextGroup = groupIdx + 1;
            while (nextGroup < groups.Count)
            {
                int candidateEnd = groups[nextGroup].end;
                if (MaxError(keys, start, candidateEnd) <= defaultEps)
                {
                    end = candidateEnd;
                    nextGroup++;
                }
                else break;
            }

            // SegEps for dup groups: min(ceil((G-1)/2), 65535) when a single group is large
            ushort segEps = 0;
            int groupSize = end - start;
            // If this segment is exactly one dup-group with G>1, raise SegEps
            if (nextGroup == groupIdx + 1 && groupSize > 1)
            {
                int raised = (groupSize - 1 + 1) / 2; // ceil((G-1)/2)
                segEps = (ushort)Math.Min(raised, 65535);
            }

            ulong k0 = keys[start];
            uint startIdx = (uint)start;
            ulong num, den;
            if (end - start <= 1)
            {
                num = 0; den = 1;
            }
            else
            {
                ulong kLast = keys[end - 1];
                ulong spanK = kLast - k0;
                ulong spanI = (ulong)(end - start - 1);
                if (spanK == 0) { num = 0; den = 1; }
                else { num = spanI; den = spanK; }
            }

            segs.Add(new PgmSeg(k0, startIdx, segEps, num, den));
            groupIdx = nextGroup;
            segStart = end;
        }

        return segs;
    }

    /// <summary>Max |predict(i) - i| over [start, end).</summary>
    private static ulong MaxError(ulong[] keys, int start, int end)
    {
        if (end - start <= 1) return 0;
        ulong k0 = keys[start];
        ulong kLast = keys[end - 1];
        ulong spanK = kLast - k0;
        ulong spanI = (ulong)(end - start - 1);
        if (spanK == 0) return 0;
        ulong maxErr = 0;
        for (int i = start; i < end; i++)
        {
            // p = (k - k0) * spanI / spanK
            UInt128 num = (UInt128)(keys[i] - k0) * spanI;
            ulong pred = (ulong)(num / spanK);
            ulong actual = (ulong)(i - start);
            ulong err = pred > actual ? pred - actual : actual - pred;
            if (err > maxErr) maxErr = err;
        }
        return maxErr;
    }

    private static int CompareHlc(FieldWrite a, FieldWrite b)
    {
        int c = a.Wall.CompareTo(b.Wall);
        if (c != 0) return c;
        c = a.Logical.CompareTo(b.Logical);
        if (c != 0) return c;
        return ByteUtil.CompareAuthorNodeId(a.AuthorNodeId, b.AuthorNodeId);
    }

    private static (ulong Wall, uint Logical, uint InternId) ComputeMode(
        List<FieldWrite> fields,
        Dictionary<IdKey, uint> authorToIntern)
    {
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
            if (kv.Value > best) { best = kv.Value; mode = kv.Key; }
        }
        return mode;
    }

    private readonly struct PgmSeg(ulong startKey, uint startIdx, ushort segEps, ulong num, ulong den)
    {
        public readonly ulong StartKey = startKey;
        public readonly uint StartIdx = startIdx;
        public readonly ushort SegEps = segEps;
        public readonly ulong Num = num;
        public readonly ulong Den = den;
    }

    private sealed class ExternalEntry(byte[] recordId, ushort fieldId, byte[] value)
    {
        public readonly byte[] RecordId = recordId;
        public readonly ushort FieldId = fieldId;
        public readonly byte[] Value = value;
    }

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

    /// <summary>AuthorNodeId order = BinSync.NodeId (LE limbs).</summary>
    private sealed class AuthorIdComparer : IComparer<IdKey>, IEqualityComparer<IdKey>
    {
        public static readonly AuthorIdComparer Instance = new();
        public int Compare(IdKey x, IdKey y) => ByteUtil.CompareAuthorNodeId(x.Bytes, y.Bytes);
        public bool Equals(IdKey x, IdKey y) => ByteUtil.Equals16(x.Bytes, y.Bytes);
        public int GetHashCode(IdKey obj) => obj.GetHashCode();
    }
}
