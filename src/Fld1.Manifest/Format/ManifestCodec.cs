using System.Buffers.Binary;
using Fld1.Core.Hash;

namespace Fld1.Manifest.Format;

/// <summary>
/// Binary manifest.fldm codec.
/// Layout (all LE except raw 16B ids):
///   Generation u64 | Crc32C u32 | LastFlushedCommitSeq u64
///   CertifiedWM wall u64 | logical u32 | pad u32
///   NsCount u32 | NsId[NsCount] u32
///   RunCount u32 | RunEntry[RunCount]
///   RetainCount u32 | RetainEntry[RetainCount]
///   ResultClientCount u32 | ResultClient[...]
///   ProvenanceCount u32 | ProvenanceIndexEntry[...]
///   CoordinatorLeaseEpoch u64 | CoordinatorSessionId b16
/// CRC covers all bytes after the CRC field (from LastFlushedCommitSeq to end).
/// </summary>
public static class ManifestCodec
{
    public static byte[] Encode(ManifestDocument doc)
    {
        using var ms = new MemoryStream();
        // Leave space for Generation + Crc (12 bytes), write body first then patch
        WriteU64(ms, doc.Generation);
        WriteU32(ms, 0); // CRC placeholder
        long bodyStart = ms.Position;

        WriteU64(ms, doc.LastFlushedCommitSeq);
        WriteU64(ms, doc.CertifiedWatermark.Wall);
        WriteU32(ms, doc.CertifiedWatermark.Logical);
        WriteU32(ms, 0); // pad

        WriteU32(ms, (uint)doc.Namespaces.Count);
        foreach (var ns in doc.Namespaces)
            WriteU32(ms, ns);

        WriteU32(ms, (uint)doc.Runs.Count);
        foreach (var r in doc.Runs)
        {
            WriteU64(ms, r.RunId);
            WriteU16(ms, r.Level);
            WriteU16(ms, 0); // pad to align
            WriteU32(ms, r.Ns);
            if (r.MinRec.Length != 16 || r.MaxRec.Length != 16)
                throw new ArgumentException("RunEntry record bounds must be 16 bytes");
            ms.Write(r.MinRec);
            ms.Write(r.MaxRec);
            WriteU64(ms, r.SeqLo);
            WriteU64(ms, r.SeqHi);
        }

        WriteU32(ms, (uint)doc.Retains.Count);
        foreach (var t in doc.Retains)
        {
            WriteU64(ms, t.RunId);
            WriteU64(ms, t.RetainUntilEpoch);
        }

        WriteU32(ms, (uint)doc.ResultClients.Count);
        foreach (var c in doc.ResultClients)
        {
            if (c.ClientInstanceId.Length != 16) throw new ArgumentException("ClientInstanceId");
            ms.Write(c.ClientInstanceId);
            WriteU64(ms, c.SeqLo);
            WriteU64(ms, c.SeqHi);
            WriteU64(ms, c.FinalizedThroughSeq);
            WriteU64(ms, c.FloorSeq);
            WriteU32(ms, (uint)c.AcceptedBitmap.Length);
            ms.Write(c.AcceptedBitmap);
        }

        WriteU32(ms, (uint)doc.ProvenanceIndex.Count);
        foreach (var p in doc.ProvenanceIndex)
        {
            if (p.ClientInstanceId.Length != 16) throw new ArgumentException("ClientInstanceId");
            ms.Write(p.ClientInstanceId);
            WriteU64(ms, p.RequestSeq);
            ms.WriteByte(p.Outcome);
            ms.WriteByte(0); ms.WriteByte(0); ms.WriteByte(0); // pad
            WriteU64(ms, p.CommitSeq);
            WriteU16(ms, p.ErrorCode);
            WriteU16(ms, 0); // pad
        }

        WriteU64(ms, doc.CoordinatorLeaseEpoch);
        if (doc.CoordinatorSessionId.Length != 16)
            throw new ArgumentException("CoordinatorSessionId must be 16 bytes");
        ms.Write(doc.CoordinatorSessionId);

        var bytes = ms.ToArray();
        // CRC over body (after Generation+Crc fields)
        uint crc = Hashes.Crc32C(bytes.AsSpan((int)bodyStart));
        BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan(8, 4), crc);
        return bytes;
    }

    public static ManifestDocument Decode(ReadOnlySpan<byte> data)
    {
        if (data.Length < 12) throw new InvalidDataException("manifest too small");
        int pos = 0;
        ulong generation = ReadU64(data, ref pos);
        uint storedCrc = ReadU32(data, ref pos);
        int bodyStart = pos;
        uint computed = Hashes.Crc32C(data.Slice(bodyStart));
        if (computed != storedCrc)
            throw new InvalidDataException("manifest CRC mismatch");

        ulong lastFlushed = ReadU64(data, ref pos);
        ulong wmWall = ReadU64(data, ref pos);
        uint wmLog = ReadU32(data, ref pos);
        _ = ReadU32(data, ref pos); // pad

        uint nsCount = ReadU32(data, ref pos);
        var namespaces = new List<uint>((int)nsCount);
        for (uint i = 0; i < nsCount; i++)
            namespaces.Add(ReadU32(data, ref pos));

        uint runCount = ReadU32(data, ref pos);
        var runs = new List<RunEntry>((int)runCount);
        for (uint i = 0; i < runCount; i++)
        {
            ulong runId = ReadU64(data, ref pos);
            ushort level = ReadU16(data, ref pos);
            _ = ReadU16(data, ref pos);
            uint ns = ReadU32(data, ref pos);
            var minRec = ReadBytes(data, ref pos, 16);
            var maxRec = ReadBytes(data, ref pos, 16);
            ulong seqLo = ReadU64(data, ref pos);
            ulong seqHi = ReadU64(data, ref pos);
            runs.Add(new RunEntry
            {
                RunId = runId, Level = level, Ns = ns,
                MinRec = minRec, MaxRec = maxRec, SeqLo = seqLo, SeqHi = seqHi
            });
        }

        uint retainCount = ReadU32(data, ref pos);
        var retains = new List<RetainEntry>((int)retainCount);
        for (uint i = 0; i < retainCount; i++)
        {
            retains.Add(new RetainEntry
            {
                RunId = ReadU64(data, ref pos),
                RetainUntilEpoch = ReadU64(data, ref pos)
            });
        }

        uint rcCount = ReadU32(data, ref pos);
        var clients = new List<ResultClient>((int)rcCount);
        for (uint i = 0; i < rcCount; i++)
        {
            var cid = ReadBytes(data, ref pos, 16);
            ulong seqLo = ReadU64(data, ref pos);
            ulong seqHi = ReadU64(data, ref pos);
            ulong finalized = ReadU64(data, ref pos);
            ulong floor = ReadU64(data, ref pos);
            uint bmpLen = ReadU32(data, ref pos);
            var bmp = ReadBytes(data, ref pos, (int)bmpLen);
            clients.Add(new ResultClient
            {
                ClientInstanceId = cid, SeqLo = seqLo, SeqHi = seqHi,
                FinalizedThroughSeq = finalized, FloorSeq = floor, AcceptedBitmap = bmp
            });
        }

        uint provCount = ReadU32(data, ref pos);
        var prov = new List<ProvenanceIndexEntry>((int)provCount);
        for (uint i = 0; i < provCount; i++)
        {
            var cid = ReadBytes(data, ref pos, 16);
            ulong req = ReadU64(data, ref pos);
            byte outcome = data[pos++];
            pos += 3; // pad
            ulong commitSeq = ReadU64(data, ref pos);
            ushort err = ReadU16(data, ref pos);
            pos += 2; // pad
            prov.Add(new ProvenanceIndexEntry
            {
                ClientInstanceId = cid, RequestSeq = req, Outcome = outcome,
                CommitSeq = commitSeq, ErrorCode = err
            });
        }

        ulong leaseEpoch = ReadU64(data, ref pos);
        var sessionId = ReadBytes(data, ref pos, 16);

        if (pos != data.Length)
            throw new InvalidDataException($"trailing bytes pos={pos} len={data.Length}");

        return new ManifestDocument
        {
            Generation = generation,
            LastFlushedCommitSeq = lastFlushed,
            CertifiedWatermark = new CertifiedWatermark(wmWall, wmLog),
            Namespaces = namespaces,
            Runs = runs,
            Retains = retains,
            ResultClients = clients,
            ProvenanceIndex = prov,
            CoordinatorLeaseEpoch = leaseEpoch,
            CoordinatorSessionId = sessionId
        };
    }

    private static void WriteU16(Stream s, ushort v)
    {
        Span<byte> b = stackalloc byte[2];
        BinaryPrimitives.WriteUInt16LittleEndian(b, v);
        s.Write(b);
    }
    private static void WriteU32(Stream s, uint v)
    {
        Span<byte> b = stackalloc byte[4];
        BinaryPrimitives.WriteUInt32LittleEndian(b, v);
        s.Write(b);
    }
    private static void WriteU64(Stream s, ulong v)
    {
        Span<byte> b = stackalloc byte[8];
        BinaryPrimitives.WriteUInt64LittleEndian(b, v);
        s.Write(b);
    }
    private static ushort ReadU16(ReadOnlySpan<byte> p, ref int pos)
    {
        var v = BinaryPrimitives.ReadUInt16LittleEndian(p.Slice(pos)); pos += 2; return v;
    }
    private static uint ReadU32(ReadOnlySpan<byte> p, ref int pos)
    {
        var v = BinaryPrimitives.ReadUInt32LittleEndian(p.Slice(pos)); pos += 4; return v;
    }
    private static ulong ReadU64(ReadOnlySpan<byte> p, ref int pos)
    {
        var v = BinaryPrimitives.ReadUInt64LittleEndian(p.Slice(pos)); pos += 8; return v;
    }
    private static byte[] ReadBytes(ReadOnlySpan<byte> p, ref int pos, int n)
    {
        if (pos + n > p.Length) throw new InvalidDataException("truncated");
        var a = p.Slice(pos, n).ToArray(); pos += n; return a;
    }
}
