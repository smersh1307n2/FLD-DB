namespace Fld1.Engine.Db;

/// <summary>
/// Exact current FLD tombstone identity offered to a caller-owned retention proof.
/// The callback must return true only when every replica has installed the required
/// replacement generation and no pending source/relay reference can resurrect it.
/// </summary>
public readonly record struct FldTombstoneGcCandidate(
    ReadOnlyMemory<byte> RecordId,
    ushort FieldId,
    ReadOnlyMemory<byte> ChangeId,
    ReadOnlyMemory<byte> AuthorNodeId,
    ulong HlcWall,
    uint HlcLogical,
    uint OriginAuthorEpoch,
    ulong OriginSequence,
    bool IsRecordTombstone,
    bool IsFieldTombstone);
