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
    public required List<WalFieldMutation> RelayFields { get; init; }
    /// <summary>Journal-only form: Fields contains only relay payloads and is also the relay subset.</summary>
    public bool RelayOnly { get; init; }
}

public static class ApplyCommitCodec
{
    private const uint BodyVersion = 4;
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
        ArgumentNullException.ThrowIfNull(commit.RelayFields);
        if (commit.CursorStart == 0 || commit.CursorEnd < commit.CursorStart)
            throw new ArgumentOutOfRangeException(nameof(commit.CursorStart), "Invalid incoming cursor range.");
        if (commit.ProtocolStateDelta.Length > MaxProtocolStateDelta)
            throw new ArgumentOutOfRangeException(nameof(commit.ProtocolStateDelta));
        if (commit.Fields.Count > WalConstants.MaxMutations || commit.RelayFields.Count > WalConstants.MaxMutations)
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
        WriteByte(ms, commit.RelayOnly ? (byte)1 : (byte)0);
        WriteFields(ms, commit.Fields);
        if (commit.RelayOnly)
        {
            EnsureUniqueChangeIds(commit.Fields);
            if (!SameFieldLists(commit.Fields, commit.RelayFields))
                throw new ArgumentException("Relay-only APPLY_COMMIT must store exactly its relay fields.", nameof(commit));
        }
        else
        {
            var relayIndices = BuildRelayIndices(commit.Fields, commit.RelayFields);
            WriteU32(ms, (uint)relayIndices.Count);
            foreach (uint index in relayIndices)
                WriteVarUInt32(ms, index);
        }

        return ms.ToArray();
    }

    public static WalApplyCommit Decode(ReadOnlySpan<byte> payload)
    {
        int pos = 0;
        uint bodyVersion = ReadU32(payload, ref pos);
        if (bodyVersion is not (2 or BodyVersion))
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
        bool relayOnly = false;
        List<WalFieldMutation> fields;
        List<WalFieldMutation> relayFields;
        if (bodyVersion == 2)
        {
            fields = ReadFields(payload, ref pos, includeOrigin: false);
            relayFields = ReadFields(payload, ref pos, includeOrigin: false);
            _ = BuildRelayIndices(fields, relayFields);
        }
        else
        {
            byte flags = ReadByte(payload, ref pos);
            if ((flags & ~1) != 0)
                throw new InvalidDataException("Unknown APPLY_COMMIT flags.");
            relayOnly = (flags & 1) != 0;
            fields = ReadFields(payload, ref pos, includeOrigin: bodyVersion >= 4);
            EnsureUniqueChangeIds(fields);
            if (relayOnly)
            {
                relayFields = fields;
            }
            else
            {
                uint relayCount = ReadU32(payload, ref pos);
                if (relayCount > fields.Count)
                    throw new InvalidDataException("APPLY_COMMIT relay index count exceeds field count.");
                relayFields = new List<WalFieldMutation>((int)relayCount);
                var seen = new HashSet<uint>();
                for (uint i = 0; i < relayCount; i++)
                {
                    uint index = ReadVarUInt32(payload, ref pos);
                    if (index >= fields.Count || !seen.Add(index))
                        throw new InvalidDataException("Invalid or duplicate APPLY_COMMIT relay index.");
                    relayFields.Add(fields[(int)index]);
                }
            }
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
            Fields = fields,
            RelayFields = relayFields,
            RelayOnly = relayOnly
        };

        ValidateId(result.SenderPeer, nameof(result.SenderPeer), nonzero: true);
        ValidateId(result.IssuerNodeId, nameof(result.IssuerNodeId), nonzero: true);
        ValidateId(result.BatchId, nameof(result.BatchId), nonzero: true);
        ValidateId(result.PreviousBatchDigest, nameof(result.PreviousBatchDigest));
        ValidateId(result.BatchDigest, nameof(result.BatchDigest));
        ValidateId(result.RecordsRoot, nameof(result.RecordsRoot));
        if (cursorStart == 0 || cursorEnd < cursorStart)
            throw new InvalidDataException("Invalid APPLY_COMMIT cursor range.");
        foreach (var field in fields.Concat(relayFields))
        {
            ValidateId(field.RecordId, nameof(field.RecordId));
            ValidateId(field.AuthorNodeId, nameof(field.AuthorNodeId), nonzero: true);
            ValidateId(field.ChangeId, nameof(field.ChangeId), nonzero: true);
            if ((field.OriginAuthorEpoch == 0) != (field.OriginSequence == 0))
                throw new InvalidDataException("APPLY_COMMIT origin epoch/sequence must be both zero or both nonzero.");
            if (field.OpKind is not (1 or 2) || (field.OpKind == 2 && field.Value.Length != 0))
                throw new InvalidDataException("Invalid APPLY_COMMIT operation.");
        }
        return result;
    }

    private readonly record struct ChangeKey(ulong Low, ulong High)
    {
        internal static ChangeKey From(byte[] id)
            => new(BinaryPrimitives.ReadUInt64LittleEndian(id),
                BinaryPrimitives.ReadUInt64LittleEndian(id.AsSpan(8)));
    }

    private static List<uint> BuildRelayIndices(
        List<WalFieldMutation> fields, List<WalFieldMutation> relayFields)
    {
        var byChangeId = new Dictionary<ChangeKey, int>(fields.Count);
        for (int i = 0; i < fields.Count; i++)
        {
            var field = fields[i];
            ValidateId(field.ChangeId, nameof(field.ChangeId), nonzero: true);
            if (!byChangeId.TryAdd(ChangeKey.From(field.ChangeId), i))
                throw new InvalidDataException("Duplicate ChangeId in APPLY_COMMIT fields.");
        }

        var indices = new List<uint>(relayFields.Count);
        var seen = new HashSet<ChangeKey>();
        foreach (var relay in relayFields)
        {
            ValidateRelayField(relay);
            var key = ChangeKey.From(relay.ChangeId);
            if (!seen.Add(key) || !byChangeId.TryGetValue(key, out int index) ||
                !SameField(fields[index], relay))
                throw new InvalidDataException("APPLY_COMMIT relay fields are not an exact subset of fields.");
            indices.Add((uint)index);
        }
        return indices;
    }

    private static void ValidateRelayField(WalFieldMutation field)
    {
        if (field.RecordId is null || field.RecordId.Length != 16 ||
            field.AuthorNodeId is null || field.AuthorNodeId.Length != 16 ||
            field.ChangeId is null || field.ChangeId.Length != 16 ||
            field.Value is null)
            throw new InvalidDataException("Malformed APPLY_COMMIT relay field identity or value.");

        byte authorBits = 0;
        foreach (byte value in field.AuthorNodeId) authorBits |= value;
        byte changeBits = 0;
        foreach (byte value in field.ChangeId) changeBits |= value;
        if (authorBits == 0 || changeBits == 0 ||
            field.OpKind is not (1 or 2) ||
            (field.OpKind == 2 && field.Value.Length != 0))
            throw new InvalidDataException("Invalid APPLY_COMMIT relay field.");
    }

    private static void EnsureUniqueChangeIds(List<WalFieldMutation> fields)
    {
        var seen = new HashSet<ChangeKey>();
        foreach (var field in fields)
            if (!seen.Add(ChangeKey.From(field.ChangeId)))
                throw new InvalidDataException("Duplicate ChangeId in APPLY_COMMIT fields.");
    }

    private static bool SameFieldLists(
        List<WalFieldMutation> left, List<WalFieldMutation> right)
    {
        if (left.Count != right.Count) return false;
        for (int i = 0; i < left.Count; i++)
            if (!SameField(left[i], right[i])) return false;
        return true;
    }

    private static bool SameField(WalFieldMutation left, WalFieldMutation right)
        => left.FieldId == right.FieldId && left.OpKind == right.OpKind &&
           left.KeyEpoch == right.KeyEpoch && left.Wall == right.Wall &&
           left.Logical == right.Logical &&
           left.RecordId.AsSpan().SequenceEqual(right.RecordId) &&
           left.AuthorNodeId.AsSpan().SequenceEqual(right.AuthorNodeId) &&
           left.ChangeId.AsSpan().SequenceEqual(right.ChangeId) &&
           left.OriginAuthorEpoch == right.OriginAuthorEpoch &&
           left.OriginSequence == right.OriginSequence &&
           left.Value.AsSpan().SequenceEqual(right.Value);

    private static void WriteVarUInt32(Stream stream, uint value)
    {
        while (value >= 0x80)
        {
            stream.WriteByte((byte)(value | 0x80));
            value >>= 7;
        }
        stream.WriteByte((byte)value);
    }

    private static uint ReadVarUInt32(ReadOnlySpan<byte> payload, ref int pos)
    {
        uint value = 0;
        for (int i = 0; i < 5; i++)
        {
            byte current = ReadByte(payload, ref pos);
            if (i == 4 && (current & 0xF0) != 0)
                throw new InvalidDataException("APPLY_COMMIT relay index varint overflows UInt32.");
            value |= (uint)(current & 0x7F) << (i * 7);
            if ((current & 0x80) == 0)
            {
                if (i > 0 && current == 0)
                    throw new InvalidDataException("Non-canonical APPLY_COMMIT relay index varint.");
                return value;
            }
        }
        throw new InvalidDataException("Unterminated APPLY_COMMIT relay index varint.");
    }

    private static void WriteFields(Stream stream, List<WalFieldMutation> fields)
    {
        WriteU32(stream, (uint)fields.Count);
        foreach (var field in fields)
        {
            ValidateId(field.RecordId, nameof(field.RecordId));
            ValidateId(field.AuthorNodeId, nameof(field.AuthorNodeId), nonzero: true);
            ValidateId(field.ChangeId, nameof(field.ChangeId), nonzero: true);
            if ((field.OriginAuthorEpoch == 0) != (field.OriginSequence == 0))
                throw new ArgumentException("OriginAuthorEpoch and OriginSequence must be both zero or both nonzero.", nameof(field));
            if (field.OpKind is not (1 or 2))
                throw new ArgumentOutOfRangeException(nameof(field.OpKind));
            if (field.Value.Length > WalConstants.MaxValLen)
                throw new ArgumentOutOfRangeException(nameof(field.Value));
            if (field.OpKind == 2 && field.Value.Length != 0)
                throw new ArgumentException("Tombstone payload must be empty.", nameof(field));
            stream.Write(field.RecordId);
            WriteU16(stream, field.FieldId);
            stream.WriteByte(field.OpKind);
            WriteU16(stream, field.KeyEpoch);
            WriteU64(stream, field.Wall);
            WriteU32(stream, field.Logical);
            stream.Write(field.AuthorNodeId);
            stream.Write(field.ChangeId);
            WriteU32(stream, field.OriginAuthorEpoch);
            WriteU64(stream, field.OriginSequence);
            WriteU32(stream, (uint)field.Value.Length);
            stream.Write(field.Value);
            if (stream.Length > WalConstants.MaxPayloadLen)
                throw new ArgumentException("APPLY_COMMIT payload exceeds WAL limit.");
        }
    }

    private static List<WalFieldMutation> ReadFields(ReadOnlySpan<byte> payload, ref int pos, bool includeOrigin)
    {
        uint count = ReadU32(payload, ref pos);
        if (count > WalConstants.MaxMutations)
            throw new InvalidDataException("APPLY_COMMIT field count exceeds limit.");
        var fields = new List<WalFieldMutation>((int)count);
        for (uint i = 0; i < count; i++)
        {
            var recordId = ReadBytes(payload, ref pos, 16);
            ushort fieldId = ReadU16(payload, ref pos);
            byte opKind = ReadByte(payload, ref pos);
            ushort keyEpoch = ReadU16(payload, ref pos);
            ulong wall = ReadU64(payload, ref pos);
            uint logical = ReadU32(payload, ref pos);
            var author = ReadBytes(payload, ref pos, 16);
            var changeId = ReadBytes(payload, ref pos, 16);
            uint originAuthorEpoch = includeOrigin ? ReadU32(payload, ref pos) : 0;
            ulong originSequence = includeOrigin ? ReadU64(payload, ref pos) : 0;
            if ((originAuthorEpoch == 0) != (originSequence == 0))
                throw new InvalidDataException("APPLY_COMMIT origin epoch/sequence must be both zero or both nonzero.");
            uint valueLength = ReadU32(payload, ref pos);
            if (valueLength > WalConstants.MaxValLen)
                throw new InvalidDataException("APPLY_COMMIT field value exceeds limit.");
            var value = ReadBytes(payload, ref pos, checked((int)valueLength));
            fields.Add(new WalFieldMutation
            {
                RecordId = recordId, FieldId = fieldId, OpKind = opKind,
                KeyEpoch = keyEpoch, Wall = wall, Logical = logical,
                AuthorNodeId = author, ChangeId = changeId,
                OriginAuthorEpoch = originAuthorEpoch, OriginSequence = originSequence,
                Value = value
            });
        }
        return fields;
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

    private static void WriteByte(Stream stream, byte value) => stream.WriteByte(value);

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
