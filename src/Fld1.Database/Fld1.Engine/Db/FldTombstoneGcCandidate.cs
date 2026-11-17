namespace Fld1.Engine.Db;

/// <summary>
/// Exact current FLD tombstone identity offered to a caller-owned retention proof.
/// The callback must return true only when every replica has installed the required
/// replacement generation and no pending source/relay reference can resurrect it.
/// Stores the three binary identities in one owned buffer to avoid three separate
/// short-lived arrays for every tombstone examined by a full namespace sweep.
/// </summary>
public readonly struct FldTombstoneGcCandidate : IEquatable<FldTombstoneGcCandidate>
{
    private readonly byte[] _identity;
    private readonly int _changeIdLength;
    private readonly int _authorNodeIdLength;

    public ReadOnlyMemory<byte> RecordId => _identity.AsMemory(0, 16);
    public ushort FieldId { get; }
    public ReadOnlyMemory<byte> ChangeId => _identity.AsMemory(16, _changeIdLength);
    public ReadOnlyMemory<byte> AuthorNodeId => _identity.AsMemory(16 + _changeIdLength, _authorNodeIdLength);
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
        : this(
            CopyAndValidate(recordId.Span, fieldId, changeId.Span, authorNodeId.Span,
                isRecordTombstone, isFieldTombstone),
            fieldId, hlcWall, hlcLogical, originAuthorEpoch, originSequence,
            isRecordTombstone, isFieldTombstone)
    {
    }

    private FldTombstoneGcCandidate(
        (byte[] Identity, int ChangeIdLength, int AuthorNodeIdLength) identity,
        ushort fieldId,
        ulong hlcWall,
        uint hlcLogical,
        uint originAuthorEpoch,
        ulong originSequence,
        bool isRecordTombstone,
        bool isFieldTombstone)
    {
        _identity = identity.Identity;
        _changeIdLength = identity.ChangeIdLength;
        _authorNodeIdLength = identity.AuthorNodeIdLength;
        FieldId = fieldId;
        HlcWall = hlcWall;
        HlcLogical = hlcLogical;
        OriginAuthorEpoch = originAuthorEpoch;
        OriginSequence = originSequence;
        IsRecordTombstone = isRecordTombstone;
        IsFieldTombstone = isFieldTombstone;
    }

    internal static FldTombstoneGcCandidate CreateOwned(
        byte[] identity,
        int changeIdLength,
        int authorNodeIdLength,
        ushort fieldId,
        ulong hlcWall,
        uint hlcLogical,
        uint originAuthorEpoch,
        ulong originSequence,
        bool isRecordTombstone,
        bool isFieldTombstone)
    {
        if (identity is null)
            throw new ArgumentNullException(nameof(identity));
        ValidateIdentity(16, fieldId, changeIdLength, authorNodeIdLength,
            isRecordTombstone, isFieldTombstone);
        if (identity.Length != 16 + changeIdLength + authorNodeIdLength)
            throw new ArgumentException("FLD tombstone candidate identity buffer has an invalid length.", nameof(identity));
        return new FldTombstoneGcCandidate(
            (identity, changeIdLength, authorNodeIdLength), fieldId, hlcWall, hlcLogical,
            originAuthorEpoch, originSequence, isRecordTombstone, isFieldTombstone);
    }

    private static (byte[] Identity, int ChangeIdLength, int AuthorNodeIdLength) CopyAndValidate(
        ReadOnlySpan<byte> recordId,
        ushort fieldId,
        ReadOnlySpan<byte> changeId,
        ReadOnlySpan<byte> authorNodeId,
        bool isRecordTombstone,
        bool isFieldTombstone)
    {
        ValidateIdentity(recordId.Length, fieldId, changeId.Length, authorNodeId.Length,
            isRecordTombstone, isFieldTombstone);
        byte[] identity = new byte[recordId.Length + changeId.Length + authorNodeId.Length];
        recordId.CopyTo(identity);
        changeId.CopyTo(identity.AsSpan(recordId.Length));
        authorNodeId.CopyTo(identity.AsSpan(recordId.Length + changeId.Length));
        return (identity, changeId.Length, authorNodeId.Length);
    }

    private static void ValidateIdentity(
        int recordIdLength,
        ushort fieldId,
        int changeIdLength,
        int authorNodeIdLength,
        bool isRecordTombstone,
        bool isFieldTombstone)
    {
        if (recordIdLength != 16 || changeIdLength is not (0 or 16) ||
            authorNodeIdLength is not (0 or 16) ||
            isRecordTombstone == isFieldTombstone ||
            (isRecordTombstone && fieldId != FldLifecycle.FieldId) ||
            (isFieldTombstone && fieldId == FldLifecycle.FieldId))
            throw new ArgumentException("FLD tombstone candidate identity or kind is invalid.");
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
