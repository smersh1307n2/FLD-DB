namespace Fld1.Engine.Db;

/// <summary>
/// One provider-owned local draft with caller-stable ChangeId, HLC and author epoch.
/// The local author identity is supplied by the owning FldDatabase, never by the caller.
/// OriginSequence remains zero until the synchronization publisher freezes the draft.
/// </summary>
public sealed record FldLocalProviderMutation(
    byte[] RecordId,
    ushort FieldId,
    byte[] Value,
    byte[] ChangeId,
    uint OriginAuthorEpoch,
    ulong Wall,
    uint Logical,
    ushort KeyEpoch = 0,
    bool IsTombstone = false)
{
    public void Validate()
    {
        ArgumentNullException.ThrowIfNull(RecordId);
        ArgumentNullException.ThrowIfNull(Value);
        ArgumentNullException.ThrowIfNull(ChangeId);
        if (RecordId.Length != 16 || IsZero(RecordId))
            throw new ArgumentException("Provider RecordId must be a nonzero 16-byte identity.", nameof(RecordId));
        if (ChangeId.Length != 16 || IsZero(ChangeId))
            throw new ArgumentException("Provider ChangeId must be a nonzero 16-byte identity.", nameof(ChangeId));
        if (OriginAuthorEpoch == 0)
            throw new ArgumentOutOfRangeException(nameof(OriginAuthorEpoch));
        if (Wall == 0)
            throw new ArgumentOutOfRangeException(nameof(Wall));
        if (Value.Length > Fld1.Wal.Format.WalConstants.MaxValLen)
            throw new ArgumentOutOfRangeException(nameof(Value), "Provider value exceeds the WAL value limit.");

        if (FieldId == FldLifecycle.FieldId)
        {
            if (IsTombstone || KeyEpoch != 0 ||
                !Value.AsSpan().SequenceEqual(FldLifecycle.DeadValue))
                throw new ArgumentException("The lifecycle field is reserved for the canonical record-dead value.");
        }
        else if (IsTombstone && Value.Length != 0)
        {
            throw new ArgumentException("Provider field tombstones must not carry a value.", nameof(Value));
        }
    }

    private static bool IsZero(ReadOnlySpan<byte> bytes)
    {
        byte aggregate = 0;
        foreach (byte value in bytes) aggregate |= value;
        return aggregate == 0;
    }
}
