namespace Fld1.Engine.Sync;

/// <summary>
/// Incoming remote field mutation (from any transport after BIN-SYNC decode).
/// HLC/author are remote-authoritative; local clock is not advanced by these values.
/// </summary>
public readonly struct RemoteFieldWrite
{
    public readonly byte[] RecordId;
    public readonly ushort FieldId;
    public readonly byte[] Value;
    public readonly ulong Wall;
    public readonly uint Logical;
    public readonly byte[] AuthorNodeId;
    public readonly ushort KeyEpoch;
    public readonly bool IsTombstone;
    public readonly byte[]? ChangeId;

    public RemoteFieldWrite(
        byte[] recordId, ushort fieldId, byte[] value,
        ulong wall, uint logical, byte[] authorNodeId,
        ushort keyEpoch = 0, bool isTombstone = false, byte[]? changeId = null)
    {
        if (recordId.Length != 16) throw new ArgumentException("RecordId");
        if (authorNodeId.Length != 16) throw new ArgumentException("AuthorNodeId");
        RecordId = recordId;
        FieldId = fieldId;
        Value = value ?? Array.Empty<byte>();
        Wall = wall;
        Logical = logical;
        AuthorNodeId = authorNodeId;
        KeyEpoch = keyEpoch;
        IsTombstone = isTombstone;
        ChangeId = changeId;
    }
}
