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

    public FieldWrite(
        ReadOnlySpan<byte> recordId,
        ushort fieldId,
        ReadOnlySpan<byte> value,
        ulong wall,
        uint logical,
        ReadOnlySpan<byte> authorNodeId,
        ushort keyEpoch = 0,
        bool isTombstone = false)
    {
        if (recordId.Length != 16) throw new ArgumentException("RecordId must be 16 bytes");
        if (authorNodeId.Length != 16) throw new ArgumentException("AuthorNodeId must be 16 bytes");
        if (value.Length > Format.RunConstants.MaxBlobLen)
            throw new ArgumentException("value > 4 MB");
        if (wall == 0) throw new ArgumentException("HLC wall must be > 0", nameof(wall));

        RecordId = recordId.ToArray();
        FieldId = fieldId;
        Value = value.ToArray();
        Wall = wall;
        Logical = logical;
        AuthorNodeId = authorNodeId.ToArray();
        KeyEpoch = keyEpoch;
        IsTombstone = isTombstone;
    }
}
