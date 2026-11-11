using System.Buffers.Binary;
using Fld1.Wal.Format;

namespace Fld1.Wal.Codec;

/// <summary>One field mutation inside GROUP_COMMIT (durable HLC + author for replay).</summary>
public sealed class WalFieldMutation
{
    public required byte[] RecordId { get; init; } // 16
    public required ushort FieldId { get; init; }
    public required byte OpKind { get; init; }
    public required ushort KeyEpoch { get; init; }
    public required ulong Wall { get; init; }
    public required uint Logical { get; init; }
    public required byte[] AuthorNodeId { get; init; } // 16
    /// <summary>Stable BIN-SYNC ChangeId; empty only when decoding legacy WAL body v1.</summary>
    public byte[] ChangeId { get; init; } = Array.Empty<byte>();
    /// <summary>Immutable origin author epoch; zero only for legacy/unpublished field state.</summary>
    public uint OriginAuthorEpoch { get; init; }
    /// <summary>Immutable origin sequence; zero only before publication or for legacy state.</summary>
    public ulong OriginSequence { get; init; }
    public required byte[] Value { get; init; }
}

public sealed class WalMutation
{
    public required byte[] ClientInstanceId { get; init; } // 16
    public required ulong RequestSeq { get; init; }
    public required ushort Ns { get; init; }
    public required List<WalFieldMutation> Fields { get; init; }
}

public sealed class WalResult
{
    public required byte[] ClientInstanceId { get; init; }
    public required ulong RequestSeq { get; init; }
    public required byte Status { get; init; }
    public required ushort ErrorCode { get; init; }
    public required ulong CommitSeq { get; init; }
    public required uint ProvenanceIdx { get; init; } // 0xFFFFFFFF = none
}

public sealed class WalProvenance
{
    public required byte[] ChangeId { get; init; } // 16
    public required byte[] SenderPeer { get; init; } // 16
    public required ushort Ns { get; init; }
    public required byte IsFirst { get; init; }
}

/// <summary>Stable idempotency receipt for one local Database IPC operation.</summary>
public sealed class WalLocalOperationReceipt
{
    public required ushort NamespaceId { get; init; }
    public required ulong ClientSequence { get; init; }
    public required byte[] ClientId { get; init; } // 16, stable across reconnects
    public required byte[] OperationId { get; init; } // 16, independent of transport RequestId
    public required byte[] Fingerprint { get; init; } // 32, canonical request digest
    public required byte[] Result { get; init; } // bounded opaque response
}

public sealed class GroupCommitPayload
{
    public required List<WalMutation> Mutations { get; init; }
    public required List<WalResult> Results { get; init; }
    public required List<WalProvenance> Provenance { get; init; }
    public List<WalLocalOperationReceipt> LocalOperationReceipts { get; init; } = [];
}

/// <summary>
/// Encode/decode GROUP_COMMIT payload.
/// Field frame: RecordId16 | FieldId | OpKind | Flags | KeyEpoch | Wall | Logical | Author16 | ValLen | Value
/// </summary>
public static class GroupCommitCodec
{
    public static byte[] Encode(GroupCommitPayload p)
    {
        if (p.Mutations.Count > WalConstants.MaxMutations)
            throw new ArgumentException("MutationsCount limit");
        if (p.Results.Count > WalConstants.MaxResults)
            throw new ArgumentException("ResultsCount limit");
        if (p.Provenance.Count > WalConstants.MaxProvenance)
            throw new ArgumentException("ProvenanceCount limit");
        if (p.LocalOperationReceipts.Count > WalConstants.MaxResults)
            throw new ArgumentException("LocalOperationReceiptCount limit");

        using var ms = new MemoryStream();
        WriteU32(ms, (uint)p.Mutations.Count);
        WriteU32(ms, (uint)p.Results.Count);
        WriteU32(ms, (uint)p.Provenance.Count);
        WriteU32(ms, 4); // v4 adds stable local-operation receipts

        Span<byte> zeroChangeId = stackalloc byte[16];
        zeroChangeId.Clear();

        foreach (var m in p.Mutations)
        {
            if (m.ClientInstanceId.Length != 16) throw new ArgumentException("ClientInstanceId");
            ms.Write(m.ClientInstanceId);
            WriteU64(ms, m.RequestSeq);
            WriteU16(ms, m.Ns);
            WriteU32(ms, (uint)m.Fields.Count);
            foreach (var f in m.Fields)
            {
                if (f.RecordId.Length != 16) throw new ArgumentException("RecordId");
                if (f.AuthorNodeId.Length != 16) throw new ArgumentException("AuthorNodeId");
                if (f.Value.Length > WalConstants.MaxValLen) throw new ArgumentException("ValLen");
                ms.Write(f.RecordId);
                WriteU16(ms, f.FieldId);
                ms.WriteByte(f.OpKind);
                ms.WriteByte(0); // Flags
                WriteU16(ms, f.KeyEpoch);
                WriteU64(ms, f.Wall);
                WriteU32(ms, f.Logical);
                ms.Write(f.AuthorNodeId);
                if (f.ChangeId.Length is not (0 or 16))
                    throw new ArgumentException("ChangeId must be empty or 16 bytes");
                // A local unpublished draft has an assigned author epoch but no
                // immutable origin sequence yet. Sequence without an epoch is invalid.
                if (f.OriginAuthorEpoch == 0 && f.OriginSequence != 0)
                    throw new ArgumentException("OriginSequence requires a nonzero OriginAuthorEpoch.");
                if (f.ChangeId.Length == 16)
                    ms.Write(f.ChangeId);
                else
                {
                    // Recovery deterministically derives the stable ID for a zero marker.
                    ms.Write(zeroChangeId);
                }
                WriteU32(ms, f.OriginAuthorEpoch);
                WriteU64(ms, f.OriginSequence);
                WriteU32(ms, (uint)f.Value.Length);
                ms.Write(f.Value);
            }
        }

        foreach (var r in p.Results)
        {
            if (r.ClientInstanceId.Length != 16) throw new ArgumentException("ClientInstanceId");
            ms.Write(r.ClientInstanceId);
            WriteU64(ms, r.RequestSeq);
            ms.WriteByte(r.Status);
            WriteU16(ms, r.ErrorCode);
            WriteU64(ms, r.CommitSeq);
            WriteU32(ms, r.ProvenanceIdx);
        }

        foreach (var pr in p.Provenance)
        {
            if (pr.ChangeId.Length != 16 || pr.SenderPeer.Length != 16)
                throw new ArgumentException("provenance ids");
            ms.Write(pr.ChangeId);
            ms.Write(pr.SenderPeer);
            WriteU16(ms, pr.Ns);
            ms.WriteByte(pr.IsFirst);
            ms.WriteByte(0);
        }

        WriteU32(ms, (uint)p.LocalOperationReceipts.Count);
        var receiptKeys = new HashSet<string>(StringComparer.Ordinal);
        foreach (var receipt in p.LocalOperationReceipts)
        {
            ValidateReceipt(receipt, nameof(p));
            string key = receipt.NamespaceId.ToString(System.Globalization.CultureInfo.InvariantCulture)
                + ":" + Convert.ToHexString(receipt.ClientId) + Convert.ToHexString(receipt.OperationId);
            if (!receiptKeys.Add(key))
                throw new ArgumentException("Duplicate local operation receipt identity.", nameof(p));
            WriteU16(ms, receipt.NamespaceId);
            WriteU64(ms, receipt.ClientSequence);
            ms.Write(receipt.ClientId);
            ms.Write(receipt.OperationId);
            ms.Write(receipt.Fingerprint);
            WriteU32(ms, (uint)receipt.Result.Length);
            ms.Write(receipt.Result);
        }

        var bytes = ms.ToArray();
        if (bytes.Length > WalConstants.MaxPayloadLen)
            throw new ArgumentException("payload > 16 MB");
        return bytes;
    }

    public static GroupCommitPayload Decode(ReadOnlySpan<byte> payload)
    {
        int pos = 0;
        uint mutCount = ReadU32(payload, ref pos);
        uint resCount = ReadU32(payload, ref pos);
        uint provCount = ReadU32(payload, ref pos);
        uint bodyVer = ReadU32(payload, ref pos);
        if (bodyVer is not (1 or 2 or 3 or 4))
            throw new InvalidDataException($"unsupported GROUP_COMMIT body ver={bodyVer}");
        if (mutCount > WalConstants.MaxMutations || resCount > WalConstants.MaxResults
            || provCount > WalConstants.MaxProvenance)
            throw new InvalidDataException("count limit");

        var mutations = new List<WalMutation>((int)mutCount);
        for (uint i = 0; i < mutCount; i++)
        {
            var clientId = ReadBytes(payload, ref pos, 16);
            ulong reqSeq = ReadU64(payload, ref pos);
            ushort ns = ReadU16(payload, ref pos);
            uint fieldCount = ReadU32(payload, ref pos);
            var fields = new List<WalFieldMutation>((int)fieldCount);
            for (uint f = 0; f < fieldCount; f++)
            {
                var rid = ReadBytes(payload, ref pos, 16);
                ushort fid = ReadU16(payload, ref pos);
                byte op = payload[pos++];
                byte flags = payload[pos++];
                if (flags != 0) throw new InvalidDataException("field flags nonzero");
                ushort keyEpoch = ReadU16(payload, ref pos);
                ulong wall = ReadU64(payload, ref pos);
                uint logical = ReadU32(payload, ref pos);
                var author = ReadBytes(payload, ref pos, 16);
                var changeId = bodyVer >= 2
                    ? ReadBytes(payload, ref pos, 16)
                    : Array.Empty<byte>();
                uint originAuthorEpoch = bodyVer >= 3 ? ReadU32(payload, ref pos) : 0;
                ulong originSequence = bodyVer >= 3 ? ReadU64(payload, ref pos) : 0;
                // (epoch != 0, sequence == 0) is the durable local-draft state.
                // A published/remote origin always has both values nonzero.
                if (originAuthorEpoch == 0 && originSequence != 0)
                    throw new InvalidDataException("GROUP_COMMIT origin sequence requires a nonzero epoch.");
                uint valLen = ReadU32(payload, ref pos);
                if (valLen > WalConstants.MaxValLen) throw new InvalidDataException("ValLen");
                var val = ReadBytes(payload, ref pos, (int)valLen);
                fields.Add(new WalFieldMutation
                {
                    RecordId = rid, FieldId = fid, OpKind = op,
                    KeyEpoch = keyEpoch, Wall = wall, Logical = logical,
                    AuthorNodeId = author, ChangeId = changeId,
                    OriginAuthorEpoch = originAuthorEpoch, OriginSequence = originSequence,
                    Value = val
                });
            }
            mutations.Add(new WalMutation
            {
                ClientInstanceId = clientId, RequestSeq = reqSeq, Ns = ns, Fields = fields
            });
        }

        var results = new List<WalResult>((int)resCount);
        for (uint i = 0; i < resCount; i++)
        {
            var clientId = ReadBytes(payload, ref pos, 16);
            ulong reqSeq = ReadU64(payload, ref pos);
            byte status = payload[pos++];
            ushort err = ReadU16(payload, ref pos);
            ulong commitSeq = ReadU64(payload, ref pos);
            uint provIdx = ReadU32(payload, ref pos);
            results.Add(new WalResult
            {
                ClientInstanceId = clientId, RequestSeq = reqSeq, Status = status,
                ErrorCode = err, CommitSeq = commitSeq, ProvenanceIdx = provIdx
            });
        }

        var provenance = new List<WalProvenance>((int)provCount);
        for (uint i = 0; i < provCount; i++)
        {
            var changeId = ReadBytes(payload, ref pos, 16);
            var sender = ReadBytes(payload, ref pos, 16);
            ushort ns = ReadU16(payload, ref pos);
            byte isFirst = payload[pos++];
            byte rsv = payload[pos++];
            if (rsv != 0) throw new InvalidDataException("prov reserved");
            provenance.Add(new WalProvenance
            {
                ChangeId = changeId, SenderPeer = sender, Ns = ns, IsFirst = isFirst
            });
        }

        var localReceipts = new List<WalLocalOperationReceipt>();
        if (bodyVer >= 4)
        {
            uint receiptCount = ReadU32(payload, ref pos);
            if (receiptCount > WalConstants.MaxResults)
                throw new InvalidDataException("local operation receipt count limit");
            localReceipts = new List<WalLocalOperationReceipt>((int)receiptCount);
            var decodedReceiptKeys = new HashSet<string>(StringComparer.Ordinal);
            for (uint i = 0; i < receiptCount; i++)
            {
                ushort namespaceId = ReadU16(payload, ref pos);
                ulong clientSequence = ReadU64(payload, ref pos);
                byte[] clientId = ReadBytes(payload, ref pos, 16);
                byte[] operationId = ReadBytes(payload, ref pos, 16);
                byte[] fingerprint = ReadBytes(payload, ref pos, 32);
                uint resultLength = ReadU32(payload, ref pos);
                if (resultLength > WalConstants.MaxValLen)
                    throw new InvalidDataException("local operation receipt result length limit");
                var receipt = new WalLocalOperationReceipt
                {
                    NamespaceId = namespaceId,
                    ClientSequence = clientSequence,
                    ClientId = clientId,
                    OperationId = operationId,
                    Fingerprint = fingerprint,
                    Result = ReadBytes(payload, ref pos, (int)resultLength)
                };
                try { ValidateReceipt(receipt, "payload"); }
                catch (ArgumentException exception)
                {
                    throw new InvalidDataException("invalid local operation receipt", exception);
                }
                string key = namespaceId.ToString(System.Globalization.CultureInfo.InvariantCulture)
                    + ":" + Convert.ToHexString(clientId) + Convert.ToHexString(operationId);
                if (!decodedReceiptKeys.Add(key))
                    throw new InvalidDataException("duplicate local operation receipt identity");
                localReceipts.Add(receipt);
            }
        }

        if (pos != payload.Length)
            throw new InvalidDataException($"trailing bytes pos={pos} len={payload.Length}");

        return new GroupCommitPayload
        {
            Mutations = mutations,
            Results = results,
            Provenance = provenance,
            LocalOperationReceipts = localReceipts
        };
    }

    private static void ValidateReceipt(WalLocalOperationReceipt receipt, string parameterName)
    {
        ArgumentNullException.ThrowIfNull(receipt);
        ArgumentNullException.ThrowIfNull(receipt.ClientId);
        ArgumentNullException.ThrowIfNull(receipt.OperationId);
        ArgumentNullException.ThrowIfNull(receipt.Fingerprint);
        ArgumentNullException.ThrowIfNull(receipt.Result);
        if (receipt.ClientId.Length != 16 || IsZero(receipt.ClientId))
            throw new ArgumentException("Receipt ClientId must be a nonzero 16-byte identifier.", parameterName);
        if (receipt.OperationId.Length != 16 || IsZero(receipt.OperationId))
            throw new ArgumentException("Receipt OperationId must be a nonzero 16-byte identifier.", parameterName);
        if (receipt.ClientSequence == 0)
            throw new ArgumentException("Receipt ClientSequence must be nonzero.", parameterName);
        if (receipt.Fingerprint.Length != 32)
            throw new ArgumentException("Receipt Fingerprint must be exactly 32 bytes.", parameterName);
        if (receipt.Result.Length > WalConstants.MaxValLen)
            throw new ArgumentException("Receipt result exceeds the WAL value limit.", parameterName);
    }

    private static bool IsZero(ReadOnlySpan<byte> value)
    {
        byte aggregate = 0;
        foreach (byte part in value) aggregate |= part;
        return aggregate == 0;
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
