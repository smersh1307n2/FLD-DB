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
    bool IsTombstone = false);
