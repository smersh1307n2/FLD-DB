using System.Buffers.Binary;
using Fld1.Wal.Format;

namespace Fld1.Wal.Codec;

/// <summary>
/// Durable APPLY transaction. BIN-SYNC prepares and interprets all protocol metadata;
/// FLD stores it verbatim together with the decoded field mutations.
/// </summary>
public sealed class WalApplyCommit
{
    public required byte[] SenderPeer { get; init; }
    public required uint SenderEpoch { get; init; }
    public required ushort NamespaceId { get; init; }
    public required byte[] IssuerNodeId { get; init; }
    public required uint IssuerEpoch { get; init; }
    public required byte[] BatchId { get; init; }
    public required ulong CursorStart { get; init; }
    public required ulong CursorEnd { get; init; }
    public required byte[] PreviousBatchDigest { get; init; }
    public required byte[] BatchDigest { get; init; }
    public required byte[] RecordsRoot { get; init; }
    public required byte[] ProtocolStateDelta { get; init; }
    public required uint AppliedCount { get; init; }
    public required List<WalFieldMutation> Fields { get; init; }
}

public static class ApplyCommitCodec
{
    private const uint BodyVersion = 1;
    private const int MaxProtocolStateDelta = 4 * 1024 * 1024;

    public static byte[] Encode(WalApplyCommit commit)
    {
        ArgumentNullException.ThrowIfNull(commit);
        ValidateId(commit.SenderPeer, nameof(commit.SenderPeer), nonzero: true);
        ValidateId(commit.IssuerNodeId, nameof(commit.IssuerNodeId), nonzero: true);
        ValidateId(commit.BatchId, nameof(commit.BatchId), nonzero: true);
        ValidateId(commit.PreviousBatchDigest, nameof(commit.PreviousBatchDigest));
        ValidateId(commit.BatchDigest, nameof(commit.BatchDigest));
        ValidateId(commit.RecordsRoot, nameof(commit.RecordsRoot));
        ArgumentNullException.ThrowIfNull(commit.ProtocolStateDelta);
        ArgumentNullException.ThrowIfNull(commit.Fields);
        if (commit.CursorStart == 0 || commit.CursorEnd < commit.CursorStart)
            throw new ArgumentOutOfRangeException(nameof(commit.CursorStart), "Invalid incoming cursor range.");
        if (commit.ProtocolStateDelta.Length > MaxProtocolStateDelta)
            throw new ArgumentOutOfRangeException(nameof(commit.ProtocolStateDelta));
        if (commit.Fields.Count > WalConstants.MaxMutations)
            throw new ArgumentOutOfRangeException(nameof(commit.Fields));

        using var ms = new MemoryStream();
        WriteU32(ms, BodyVersion);
        ms.Write(commit.SenderPeer);
        WriteU32(ms, commit.SenderEpoch);
        WriteU16(ms, commit.NamespaceId);
        ms.Write(commit.IssuerNodeId);
        WriteU32(ms, commit.IssuerEpoch);
        ms.Write(commit.BatchId);
        WriteU64(ms, commit.CursorStart);
        WriteU64(ms, commit.CursorEnd);
        ms.Write(commit.PreviousBatchDigest);
        ms.Write(commit.BatchDigest);
        ms.Write(commit.RecordsRoot);
        WriteU32(ms, (uint)commit.ProtocolStateDelta.Length);
        ms.Write(commit.ProtocolStateDelta);
        WriteU32(ms, commit.AppliedCount);
        WriteU32(ms, (uint)commit.Fields.Count);

        foreach (var field in commit.Fields)
        {
            ValidateId(field.RecordId, nameof(field.RecordId));
            ValidateId(field.AuthorNodeId, nameof(field.AuthorNodeId), nonzero: true);
            ValidateId(field.ChangeId, nameof(field.ChangeId), nonzero: true);
            if (field.OpKind is not (1 or 2))
                throw new ArgumentOutOfRangeException(nameof(field.OpKind));
            if (field.Value.Length > WalConstants.MaxValLen)
                throw new ArgumentOutOfRangeException(nameof(field.Value));
            if (field.OpKind == 2 && field.Value.Length != 0)
                throw new ArgumentException("Tombstone payload must be empty.", nameof(field));

            ms.Write(field.RecordId);
            WriteU16(ms, field.FieldId);
            ms.WriteByte(field.OpKind);
            WriteU16(ms, field.KeyEpoch);
            WriteU64(ms, field.Wall);
            WriteU32(ms, field.Logical);
            ms.Write(field.AuthorNodeId);
            ms.Write(field.ChangeId);
            WriteU32(ms, (uint)field.Value.Length);
            ms.Write(field.Value);
            if (ms.Length > WalConstants.MaxPayloadLen)
                throw new ArgumentException("APPLY_COMMIT payload exceeds WAL limit.", nameof(commit));
        }

        return ms.ToArray();
    }

    public static WalApplyCommit Decode(ReadOnlySpan<byte> payload)
    {
        int pos = 0;
        if (ReadU32(payload, ref pos) != BodyVersion)
            throw new InvalidDataException("Unsupported APPLY_COMMIT body version.");

        var senderPeer = ReadBytes(payload, ref pos, 16);
        uint senderEpoch = ReadU32(payload, ref pos);
        ushort ns = ReadU16(payload, ref pos);
        var issuer = ReadBytes(payload, ref pos, 16);
        uint issuerEpoch = ReadU32(payload, ref pos);
        var batchId = ReadBytes(payload, ref pos, 16);
        ulong cursorStart = ReadU64(payload, ref pos);
        ulong cursorEnd = ReadU64(payload, ref pos);
        var previousDigest = ReadBytes(payload, ref pos, 16);
        var batchDigest = ReadBytes(payload, ref pos, 16);
        var recordsRoot = ReadBytes(payload, ref pos, 16);
        uint deltaLength = ReadU32(payload, ref pos);
        if (deltaLength > MaxProtocolStateDelta)
            throw new InvalidDataException("ProtocolStateDelta exceeds limit.");
        var delta = ReadBytes(payload, ref pos, checked((int)deltaLength));
        uint appliedCount = ReadU32(payload, ref pos);
        uint fieldCount = ReadU32(payload, ref pos);
        if (fieldCount > WalConstants.MaxMutations)
            throw new InvalidDataException("APPLY_COMMIT field count exceeds limit.");

        var fields = new List<WalFieldMutation>((int)fieldCount);
        for (uint i = 0; i < fieldCount; i++)
        {
            var recordId = ReadBytes(payload, ref pos, 16);
            ushort fieldId = ReadU16(payload, ref pos);
            byte opKind = ReadByte(payload, ref pos);
            ushort keyEpoch = ReadU16(payload, ref pos);
            ulong wall = ReadU64(payload, ref pos);
            uint logical = ReadU32(payload, ref pos);
            var author = ReadBytes(payload, ref pos, 16);
            var changeId = ReadBytes(payload, ref pos, 16);
            uint valueLength = ReadU32(payload, ref pos);
            if (valueLength > WalConstants.MaxValLen)
                throw new InvalidDataException("APPLY_COMMIT field value exceeds limit.");
            var value = ReadBytes(payload, ref pos, checked((int)valueLength));
            fields.Add(new WalFieldMutation
            {
                RecordId = recordId,
                FieldId = fieldId,
                OpKind = opKind,
                KeyEpoch = keyEpoch,
                Wall = wall,
                Logical = logical,
                AuthorNodeId = author,
                ChangeId = changeId,
                Value = value
            });
        }

        if (pos != payload.Length)
            throw new InvalidDataException("Trailing bytes in APPLY_COMMIT payload.");

        var result = new WalApplyCommit
        {
            SenderPeer = senderPeer,
            SenderEpoch = senderEpoch,
            NamespaceId = ns,
            IssuerNodeId = issuer,
            IssuerEpoch = issuerEpoch,
            BatchId = batchId,
            CursorStart = cursorStart,
            CursorEnd = cursorEnd,
            PreviousBatchDigest = previousDigest,
            BatchDigest = batchDigest,
            RecordsRoot = recordsRoot,
            ProtocolStateDelta = delta,
            AppliedCount = appliedCount,
            Fields = fields
        };

        ValidateId(result.SenderPeer, nameof(result.SenderPeer), nonzero: true);
        ValidateId(result.IssuerNodeId, nameof(result.IssuerNodeId), nonzero: true);
        ValidateId(result.BatchId, nameof(result.BatchId), nonzero: true);
        ValidateId(result.PreviousBatchDigest, nameof(result.PreviousBatchDigest));
        ValidateId(result.BatchDigest, nameof(result.BatchDigest));
        ValidateId(result.RecordsRoot, nameof(result.RecordsRoot));
        if (cursorStart == 0 || cursorEnd < cursorStart)
            throw new InvalidDataException("Invalid APPLY_COMMIT cursor range.");
        foreach (var field in fields)
        {
            ValidateId(field.RecordId, nameof(field.RecordId));
            ValidateId(field.AuthorNodeId, nameof(field.AuthorNodeId), nonzero: true);
            ValidateId(field.ChangeId, nameof(field.ChangeId), nonzero: true);
            if (field.OpKind is not (1 or 2) || (field.OpKind == 2 && field.Value.Length != 0))
                throw new InvalidDataException("Invalid APPLY_COMMIT operation.");
        }
        return result;
    }

    private static void ValidateId(byte[]? value, string name, bool nonzero = false)
    {
        if (value is null || value.Length != 16)
            throw new ArgumentException($"{name} must be exactly 16 bytes.", name);
        if (!nonzero) return;
        byte aggregate = 0;
        foreach (byte b in value) aggregate |= b;
        if (aggregate == 0)
            throw new ArgumentException($"{name} must be nonzero.", name);
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
        Ensure(p, pos, 2);
        ushort value = BinaryPrimitives.ReadUInt16LittleEndian(p.Slice(pos, 2));
        pos += 2;
        return value;
    }
    private static uint ReadU32(ReadOnlySpan<byte> p, ref int pos)
    {
        Ensure(p, pos, 4);
        uint value = BinaryPrimitives.ReadUInt32LittleEndian(p.Slice(pos, 4));
        pos += 4;
        return value;
    }
    private static ulong ReadU64(ReadOnlySpan<byte> p, ref int pos)
    {
        Ensure(p, pos, 8);
        ulong value = BinaryPrimitives.ReadUInt64LittleEndian(p.Slice(pos, 8));
        pos += 8;
        return value;
    }
    private static byte ReadByte(ReadOnlySpan<byte> p, ref int pos)
    {
        Ensure(p, pos, 1);
        return p[pos++];
    }
    private static byte[] ReadBytes(ReadOnlySpan<byte> p, ref int pos, int length)
    {
        Ensure(p, pos, length);
        var value = p.Slice(pos, length).ToArray();
        pos += length;
        return value;
    }
    private static void Ensure(ReadOnlySpan<byte> p, int pos, int length)
    {
        if (length < 0 || pos < 0 || pos > p.Length - length)
            throw new InvalidDataException("Truncated APPLY_COMMIT payload.");
    }
}
