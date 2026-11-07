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
        byte[] changeId, ushort keyEpoch = 0, bool isTombstone = false)
    {
        ArgumentNullException.ThrowIfNull(recordId);
        ArgumentNullException.ThrowIfNull(authorNodeId);
        if (recordId.Length != 16) throw new ArgumentException("RecordId");
        if (authorNodeId.Length != 16) throw new ArgumentException("AuthorNodeId");
        ArgumentNullException.ThrowIfNull(changeId);
        if (changeId.Length != 16)
            throw new ArgumentException("ChangeId must be 16 bytes.", nameof(changeId));
        if (IsZero(changeId))
            throw new ArgumentException("ChangeId must be nonzero.", nameof(changeId));
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

    private static bool IsZero(ReadOnlySpan<byte> value)
    {
        byte aggregate = 0;
        foreach (byte b in value) aggregate |= b;
        return aggregate == 0;
    }
}
