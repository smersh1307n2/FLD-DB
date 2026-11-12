namespace Fld1.Engine.Db;

/// <summary>
/// Exact current FLD tombstone identity offered to a caller-owned retention proof.
/// The callback must return true only when every replica has installed the required
/// replacement generation and no pending source/relay reference can resurrect it.
/// </summary>
public readonly struct FldTombstoneGcCandidate : IEquatable<FldTombstoneGcCandidate>
{
    private readonly byte[] _recordId;
    private readonly byte[] _changeId;
    private readonly byte[] _authorNodeId;

    public ReadOnlyMemory<byte> RecordId => _recordId;
    public ushort FieldId { get; }
    public ReadOnlyMemory<byte> ChangeId => _changeId;
    public ReadOnlyMemory<byte> AuthorNodeId => _authorNodeId;
    public ulong HlcWall { get; }
    public uint HlcLogical { get; }
    public uint OriginAuthorEpoch { get; }
    public ulong OriginSequence { get; }
    public bool IsRecordTombstone { get; }
    public bool IsFieldTombstone { get; }

    public FldTombstoneGcCandidate(
        ReadOnlyMemory<byte> recordId,
        ushort fieldId,
        ReadOnlyMemory<byte> changeId,
        ReadOnlyMemory<byte> authorNodeId,
        ulong hlcWall,
        uint hlcLogical,
        uint originAuthorEpoch,
        ulong originSequence,
        bool isRecordTombstone,
        bool isFieldTombstone)
    {
        if (recordId.Length != 16 || changeId.Length is not (0 or 16) ||
            authorNodeId.Length is not (0 or 16) ||
            isRecordTombstone == isFieldTombstone)
            throw new ArgumentException("FLD tombstone candidate identity or kind is invalid.");

        _recordId = recordId.ToArray();
        _changeId = changeId.ToArray();
        _authorNodeId = authorNodeId.ToArray();
        FieldId = fieldId;
        HlcWall = hlcWall;
        HlcLogical = hlcLogical;
        OriginAuthorEpoch = originAuthorEpoch;
        OriginSequence = originSequence;
        IsRecordTombstone = isRecordTombstone;
        IsFieldTombstone = isFieldTombstone;
    }

    public bool Equals(FldTombstoneGcCandidate other)
        => FieldId == other.FieldId &&
           HlcWall == other.HlcWall &&
           HlcLogical == other.HlcLogical &&
           OriginAuthorEpoch == other.OriginAuthorEpoch &&
           OriginSequence == other.OriginSequence &&
           IsRecordTombstone == other.IsRecordTombstone &&
           IsFieldTombstone == other.IsFieldTombstone &&
           RecordId.Span.SequenceEqual(other.RecordId.Span) &&
           ChangeId.Span.SequenceEqual(other.ChangeId.Span) &&
           AuthorNodeId.Span.SequenceEqual(other.AuthorNodeId.Span);

    public override bool Equals(object? obj)
        => obj is FldTombstoneGcCandidate other && Equals(other);

    public override int GetHashCode()
    {
        var hash = new HashCode();
        foreach (byte value in RecordId.Span) hash.Add(value);
        hash.Add(FieldId);
        foreach (byte value in ChangeId.Span) hash.Add(value);
        foreach (byte value in AuthorNodeId.Span) hash.Add(value);
        hash.Add(HlcWall);
        hash.Add(HlcLogical);
        hash.Add(OriginAuthorEpoch);
        hash.Add(OriginSequence);
        hash.Add(IsRecordTombstone);
        hash.Add(IsFieldTombstone);
        return hash.ToHashCode();
    }

    public static bool operator ==(FldTombstoneGcCandidate left, FldTombstoneGcCandidate right)
        => left.Equals(right);

    public static bool operator !=(FldTombstoneGcCandidate left, FldTombstoneGcCandidate right)
        => !left.Equals(right);
}
