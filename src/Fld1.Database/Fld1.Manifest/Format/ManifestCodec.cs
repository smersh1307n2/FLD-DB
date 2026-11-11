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
        ArgumentNullException.ThrowIfNull(doc);
        ValidateDocument(doc);
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

        // Optional v1 extension. Older manifests end immediately after CoordinatorSessionId.
        ms.Write("FLOR"u8);
        WriteU32(ms, (uint)doc.LocalOperationReceipts.Count);
        foreach (var receipt in doc.LocalOperationReceipts)
        {
            WriteU32(ms, receipt.NamespaceId);
            ms.Write(receipt.ClientId);
            ms.Write(receipt.OperationId);
            ms.Write(receipt.Fingerprint);
            WriteU64(ms, receipt.CommitSeq);
            WriteU32(ms, (uint)receipt.Result.Length);
            ms.Write(receipt.Result);
        }

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
        if (!Hashes.IsCrc32COrLegacy(data.Slice(bodyStart), storedCrc))
            throw new InvalidDataException("manifest CRC mismatch");

        ulong lastFlushed = ReadU64(data, ref pos);
        ulong wmWall = ReadU64(data, ref pos);
        uint wmLog = ReadU32(data, ref pos);
        if (ReadU32(data, ref pos) != 0)
            throw new InvalidDataException("manifest watermark reserved bytes are non-zero");

        int nsCount = ReadCount(data, ref pos, minimumEntryBytes: 4, maxCount: 65_536, "namespace");
        var namespaces = new List<uint>(nsCount);
        for (int i = 0; i < nsCount; i++)
            namespaces.Add(ReadU32(data, ref pos));

        int runCount = ReadCount(data, ref pos, minimumEntryBytes: 64, maxCount: 1_000_000, "run");
        var runs = new List<RunEntry>(runCount);
        for (int i = 0; i < runCount; i++)
        {
            ulong runId = ReadU64(data, ref pos);
            ushort level = ReadU16(data, ref pos);
            if (ReadU16(data, ref pos) != 0)
                throw new InvalidDataException("run entry reserved bytes are non-zero");
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

        int retainCount = ReadCount(data, ref pos, minimumEntryBytes: 16, maxCount: 1_000_000, "retain");
        var retains = new List<RetainEntry>(retainCount);
        for (int i = 0; i < retainCount; i++)
        {
            retains.Add(new RetainEntry
            {
                RunId = ReadU64(data, ref pos),
                RetainUntilEpoch = ReadU64(data, ref pos)
            });
        }

        int rcCount = ReadCount(data, ref pos, minimumEntryBytes: 52, maxCount: 100_000, "result client");
        var clients = new List<ResultClient>(rcCount);
        for (int i = 0; i < rcCount; i++)
        {
            var cid = ReadBytes(data, ref pos, 16);
            ulong seqLo = ReadU64(data, ref pos);
            ulong seqHi = ReadU64(data, ref pos);
            ulong finalized = ReadU64(data, ref pos);
            ulong floor = ReadU64(data, ref pos);
            uint bmpLen = ReadU32(data, ref pos);
            if (bmpLen > 1_048_576)
                throw new InvalidDataException($"result client bitmap too large: {bmpLen}");
            var bmp = ReadBytes(data, ref pos, checked((int)bmpLen));
            clients.Add(new ResultClient
            {
                ClientInstanceId = cid, SeqLo = seqLo, SeqHi = seqHi,
                FinalizedThroughSeq = finalized, FloorSeq = floor, AcceptedBitmap = bmp
            });
        }

        int provCount = ReadCount(data, ref pos, minimumEntryBytes: 40, maxCount: 1_000_000, "provenance");
        var prov = new List<ProvenanceIndexEntry>(provCount);
        for (int i = 0; i < provCount; i++)
        {
            var cid = ReadBytes(data, ref pos, 16);
            ulong req = ReadU64(data, ref pos);
            byte outcome = ReadU8(data, ref pos);
            if (ReadU8(data, ref pos) != 0 || ReadU8(data, ref pos) != 0 || ReadU8(data, ref pos) != 0)
                throw new InvalidDataException("provenance reserved bytes are non-zero");
            ulong commitSeq = ReadU64(data, ref pos);
            ushort err = ReadU16(data, ref pos);
            if (ReadU16(data, ref pos) != 0)
                throw new InvalidDataException("provenance reserved word is non-zero");
            prov.Add(new ProvenanceIndexEntry
            {
                ClientInstanceId = cid, RequestSeq = req, Outcome = outcome,
                CommitSeq = commitSeq, ErrorCode = err
            });
        }

        ulong leaseEpoch = ReadU64(data, ref pos);
        var sessionId = ReadBytes(data, ref pos, 16);
        var localReceipts = new List<LocalOperationReceiptEntry>();
        if (pos < data.Length)
        {
            if (data.Length - pos < 8 || !data.Slice(pos, 4).SequenceEqual("FLOR"u8))
                throw new InvalidDataException("unknown manifest extension");
            pos += 4;
            int receiptCount = ReadCount(
                data, ref pos, minimumEntryBytes: 80, maxCount: 1_000_000, "local operation receipt");
            localReceipts = new List<LocalOperationReceiptEntry>(receiptCount);
            for (int i = 0; i < receiptCount; i++)
            {
                uint namespaceId = ReadU32(data, ref pos);
                byte[] clientId = ReadBytes(data, ref pos, 16);
                byte[] operationId = ReadBytes(data, ref pos, 16);
                byte[] fingerprint = ReadBytes(data, ref pos, 32);
                ulong commitSeq = ReadU64(data, ref pos);
                uint resultLength = ReadU32(data, ref pos);
                if (resultLength > 4 * 1024 * 1024)
                    throw new InvalidDataException("local operation receipt result exceeds 4 MiB");
                byte[] result = ReadBytes(data, ref pos, checked((int)resultLength));
                localReceipts.Add(new LocalOperationReceiptEntry
                {
                    NamespaceId = namespaceId,
                    ClientId = clientId,
                    OperationId = operationId,
                    Fingerprint = fingerprint,
                    CommitSeq = commitSeq,
                    Result = result
                });
            }
        }

        if (pos != data.Length)
            throw new InvalidDataException($"trailing bytes pos={pos} len={data.Length}");

        var document = new ManifestDocument
        {
            Generation = generation,
            LastFlushedCommitSeq = lastFlushed,
            CertifiedWatermark = new CertifiedWatermark(wmWall, wmLog),
            Namespaces = namespaces,
            Runs = runs,
            Retains = retains,
            ResultClients = clients,
            ProvenanceIndex = prov,
            LocalOperationReceipts = localReceipts,
            CoordinatorLeaseEpoch = leaseEpoch,
            CoordinatorSessionId = sessionId
        };
        ValidateDocument(document);
        return document;
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
    private static int ReadCount(
        ReadOnlySpan<byte> data, ref int pos, int minimumEntryBytes, int maxCount, string label)
    {
        uint raw = ReadU32(data, ref pos);
        int remaining = data.Length - pos;
        if (raw > maxCount || raw > (uint)(remaining / minimumEntryBytes))
            throw new InvalidDataException($"manifest {label} count out of bounds: {raw}");
        return (int)raw;
    }

    private static byte ReadU8(ReadOnlySpan<byte> data, ref int pos)
    {
        RequireBytes(data, pos, 1);
        return data[pos++];
    }

    private static ushort ReadU16(ReadOnlySpan<byte> data, ref int pos)
    {
        RequireBytes(data, pos, 2);
        ushort value = BinaryPrimitives.ReadUInt16LittleEndian(data.Slice(pos, 2));
        pos += 2;
        return value;
    }

    private static uint ReadU32(ReadOnlySpan<byte> data, ref int pos)
    {
        RequireBytes(data, pos, 4);
        uint value = BinaryPrimitives.ReadUInt32LittleEndian(data.Slice(pos, 4));
        pos += 4;
        return value;
    }

    private static ulong ReadU64(ReadOnlySpan<byte> data, ref int pos)
    {
        RequireBytes(data, pos, 8);
        ulong value = BinaryPrimitives.ReadUInt64LittleEndian(data.Slice(pos, 8));
        pos += 8;
        return value;
    }

    private static byte[] ReadBytes(ReadOnlySpan<byte> data, ref int pos, int length)
    {
        RequireBytes(data, pos, length);
        byte[] value = data.Slice(pos, length).ToArray();
        pos += length;
        return value;
    }

    private static void RequireBytes(ReadOnlySpan<byte> data, int pos, int length)
    {
        if (pos < 0 || length < 0 || pos > data.Length - length)
            throw new InvalidDataException("manifest is truncated");
    }

    private static bool IsZero(ReadOnlySpan<byte> value)
    {
        byte aggregate = 0;
        foreach (byte part in value) aggregate |= part;
        return aggregate == 0;
    }

    private static void ValidateDocument(ManifestDocument document)
    {
        var namespaces = new HashSet<uint>();
        foreach (uint ns in document.Namespaces)
        {
            if (ns > 0xFFFF || !namespaces.Add(ns))
                throw new InvalidDataException($"invalid or duplicate namespace id {ns}");
        }

        // RunId is also the filename, so it must be unique across all namespaces.
        var runIds = new HashSet<ulong>();
        foreach (var run in document.Runs)
        {
            if (run.RunId == 0 || run.Ns > 0xFFFF || !namespaces.Contains(run.Ns)
                || run.MinRec.Length != 16 || run.MaxRec.Length != 16
                || run.SeqLo > run.SeqHi
                || run.MinRec.AsSpan().SequenceCompareTo(run.MaxRec) > 0
                || !runIds.Add(run.RunId))
                throw new InvalidDataException($"invalid or duplicate manifest RunEntry {run.RunId}");
        }

        var retainIds = new HashSet<ulong>();
        foreach (var retain in document.Retains)
        {
            if (retain.RunId == 0 || !retainIds.Add(retain.RunId))
                throw new InvalidDataException($"invalid or duplicate RetainEntry {retain.RunId}");
        }

        var clientIds = new HashSet<string>(StringComparer.Ordinal);
        foreach (var client in document.ResultClients)
        {
            if (client.ClientInstanceId.Length != 16 || client.AcceptedBitmap.Length > 1_048_576
                || client.SeqLo > client.SeqHi || client.FinalizedThroughSeq > client.SeqHi
                || !clientIds.Add(Convert.ToHexString(client.ClientInstanceId)))
                throw new InvalidDataException("invalid or duplicate ResultClient");
        }

        foreach (var item in document.ProvenanceIndex)
        {
            if (item.ClientInstanceId.Length != 16 || item.Outcome > ManifestOutcome.Skipped)
                throw new InvalidDataException("invalid ProvenanceIndexEntry");
        }

        var operationReceiptKeys = new HashSet<string>(StringComparer.Ordinal);
        foreach (var receipt in document.LocalOperationReceipts)
        {
            if (receipt.NamespaceId > 0xFFFF || receipt.ClientId.Length != 16 || IsZero(receipt.ClientId)
                || receipt.OperationId.Length != 16 || IsZero(receipt.OperationId)
                || receipt.Fingerprint.Length != 32 || receipt.CommitSeq == 0
                || receipt.Result.Length > 4 * 1024 * 1024)
                throw new InvalidDataException("invalid LocalOperationReceiptEntry");
            string key = Convert.ToHexString(receipt.ClientId) + Convert.ToHexString(receipt.OperationId);
            if (!operationReceiptKeys.Add(key))
                throw new InvalidDataException("duplicate LocalOperationReceiptEntry");
        }

        if (document.CoordinatorSessionId.Length != 16)
            throw new InvalidDataException("CoordinatorSessionId must be 16 bytes");
    }
}
