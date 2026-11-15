namespace Fld1.Run.Builder;

/// <summary>
/// One logical field version to place into a STATE run.
/// Value is copied; max inline 256, larger → external (later).
/// </summary>
public sealed class FieldWrite
{
    public byte[] RecordId { get; }
    public ushort FieldId { get; }
    public byte[] Value { get; }
    public ulong Wall { get; }
    public uint Logical { get; }
    public byte[] AuthorNodeId { get; }
    public ushort KeyEpoch { get; }
    public bool IsTombstone { get; }
    public byte[] ChangeId { get; }
    public uint OriginAuthorEpoch { get; }
    public ulong OriginSequence { get; }

    internal ReadOnlyMemory<byte> RecordIdMemory { get; }
    internal ReadOnlyMemory<byte> ValueMemory { get; }
    internal ReadOnlyMemory<byte> AuthorNodeIdMemory { get; }
    internal ReadOnlyMemory<byte> ChangeIdMemory { get; }

    public FieldWrite(
        ReadOnlySpan<byte> recordId,
        ushort fieldId,
        ReadOnlySpan<byte> value,
        ulong wall,
        uint logical,
        ReadOnlySpan<byte> authorNodeId,
        ushort keyEpoch = 0,
        bool isTombstone = false,
        ReadOnlySpan<byte> changeId = default,
        uint originAuthorEpoch = 0,
        ulong originSequence = 0)
    {
        if (recordId.Length != 16) throw new ArgumentException("RecordId must be 16 bytes");
        if (authorNodeId.Length != 16) throw new ArgumentException("AuthorNodeId must be 16 bytes");
        if (value.Length > Format.RunConstants.MaxBlobLen)
            throw new ArgumentException("value > 4 MB");
        if (wall == 0) throw new ArgumentException("HLC wall must be > 0", nameof(wall));
        if (changeId.Length is not (0 or 16))
            throw new ArgumentException("ChangeId must be empty or 16 bytes.", nameof(changeId));
        if (changeId.Length == 16)
        {
            byte aggregate = 0;
            foreach (byte part in changeId) aggregate |= part;
            if (aggregate == 0)
                throw new ArgumentException("ChangeId must be nonzero.", nameof(changeId));
        }
        // A local identity draft is durable before BIN-SYNC publication assigns
        // its author sequence. Epoch may therefore be present while sequence is zero;
        // the inverse (sequence without an epoch) is never a valid origin.
        if (originAuthorEpoch == 0 && originSequence != 0)
            throw new ArgumentException("Origin sequence requires an author epoch.");
        if (originAuthorEpoch != 0 && changeId.IsEmpty)
            throw new ArgumentException("OperationOrigin requires a stable ChangeId.");

        RecordId = recordId.ToArray();
        FieldId = fieldId;
        Value = value.ToArray();
        Wall = wall;
        Logical = logical;
        AuthorNodeId = authorNodeId.ToArray();
        KeyEpoch = keyEpoch;
        IsTombstone = isTombstone;
        ChangeId = changeId.ToArray();
        OriginAuthorEpoch = originAuthorEpoch;
        OriginSequence = originSequence;

        RecordIdMemory = RecordId;
        ValueMemory = Value;
        AuthorNodeIdMemory = AuthorNodeId;
        ChangeIdMemory = ChangeId;
    }

    // Internal compaction path: borrow immutable Run-backed slices instead of
    // copying RecordId/value/author/provenance into four new arrays per field.
    internal FieldWrite(
        ReadOnlyMemory<byte> recordId,
        ushort fieldId,
        ReadOnlyMemory<byte> value,
        ulong wall,
        uint logical,
        ReadOnlyMemory<byte> authorNodeId,
        ushort keyEpoch,
        bool isTombstone,
        ReadOnlyMemory<byte> changeId,
        uint originAuthorEpoch,
        ulong originSequence)
    {
        ReadOnlySpan<byte> recordIdSpan = recordId.Span;
        ReadOnlySpan<byte> valueSpan = value.Span;
        ReadOnlySpan<byte> authorNodeIdSpan = authorNodeId.Span;
        ReadOnlySpan<byte> changeIdSpan = changeId.Span;
        if (recordIdSpan.Length != 16) throw new ArgumentException("RecordId must be 16 bytes");
        if (authorNodeIdSpan.Length != 16) throw new ArgumentException("AuthorNodeId must be 16 bytes");
        if (valueSpan.Length > Format.RunConstants.MaxBlobLen)
            throw new ArgumentException("value > 4 MB");
        if (wall == 0) throw new ArgumentException("HLC wall must be > 0", nameof(wall));
        if (changeIdSpan.Length is not (0 or 16))
            throw new ArgumentException("ChangeId must be empty or 16 bytes.", nameof(changeId));
        if (changeIdSpan.Length == 16)
        {
            byte aggregate = 0;
            foreach (byte part in changeIdSpan) aggregate |= part;
            if (aggregate == 0)
                throw new ArgumentException("ChangeId must be nonzero.", nameof(changeId));
        }
        if (originAuthorEpoch == 0 && originSequence != 0)
            throw new ArgumentException("Origin sequence requires an author epoch.");
        if (originAuthorEpoch != 0 && changeIdSpan.IsEmpty)
            throw new ArgumentException("OperationOrigin requires a stable ChangeId.");

        RecordId = Array.Empty<byte>();
        Value = Array.Empty<byte>();
        AuthorNodeId = Array.Empty<byte>();
        ChangeId = Array.Empty<byte>();
        FieldId = fieldId;
        Wall = wall;
        Logical = logical;
        KeyEpoch = keyEpoch;
        IsTombstone = isTombstone;
        OriginAuthorEpoch = originAuthorEpoch;
        OriginSequence = originSequence;

        RecordIdMemory = recordId;
        ValueMemory = value;
        AuthorNodeIdMemory = authorNodeId;
        ChangeIdMemory = changeId;
    }
}
