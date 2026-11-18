using System.Buffers.Binary;

namespace Fld1.Engine.Db;

internal readonly record struct FldId128(ulong Low, ulong High)
{
    public static FldId128 From(ReadOnlySpan<byte> bytes)
    {
        if (bytes.Length != 16)
            throw new InvalidDataException("A retained FLD identity must be exactly 16 bytes.");
        return new FldId128(
            BinaryPrimitives.ReadUInt64LittleEndian(bytes),
            BinaryPrimitives.ReadUInt64LittleEndian(bytes.Slice(8)));
    }
}

/// <summary>
/// Immutable, partition-local reference index built while the caller holds the FLD
/// commit gate. It lets the BIN-SYNC adapter answer many candidate queries without
/// rescanning Outbox and ApplyJournal for every ChangeId/RecordId.
/// </summary>
public sealed class FldReferenceSnapshot : IDisposable
{
    private readonly HashSet<FldId128> _changeIds;
    private readonly HashSet<FldId128> _recordIds;
    private readonly bool _unsealedWal;
    private readonly bool _snapshotPin;

    internal FldReferenceSnapshot(
        FldDraftHorizon drafts,
        HashSet<FldId128> changeIds,
        HashSet<FldId128> recordIds,
        bool unsealedWal,
        bool snapshotPin)
    {
        UnpublishedLocalDraftHorizon = drafts;
        _changeIds = changeIds;
        _recordIds = recordIds;
        _unsealedWal = unsealedWal;
        _snapshotPin = snapshotPin;
    }

    /// <summary>Gets the conservative unpublished local-operation horizon captured with this reference index.</summary>
    public FldDraftHorizon UnpublishedLocalDraftHorizon { get; }

    /// <summary>
    /// Gets whether any in-memory or persisted provider-snapshot generation is pinned.
    /// While true, ChangeId and RecordId queries conservatively report every candidate
    /// as referenced; receipt identity remains an independent horizon.
    /// </summary>
    public bool HasSnapshotPin => _snapshotPin;

    /// <summary>
    /// Returns whether the exact 16-byte ChangeId is retained by unsealed WAL,
    /// a provider-snapshot pin, or the captured dirty/Outbox/ApplyJournal index.
    /// </summary>
    /// <param name="changeId">Canonical 16-byte ChangeId.</param>
    public bool HasChangeIdReference(ReadOnlySpan<byte> changeId)
        => _unsealedWal || _snapshotPin || _changeIds.Contains(FldId128.From(changeId));

    /// <summary>
    /// Returns whether the exact 16-byte RecordId or tombstone ChangeId is retained
    /// by unsealed WAL, a provider-snapshot pin, or the captured reference index.
    /// </summary>
    /// <param name="recordId">Canonical 16-byte RecordId.</param>
    /// <param name="tombstoneChangeId">Canonical 16-byte ChangeId for the tombstone.</param>
    public bool HasRecordReference(
        ReadOnlySpan<byte> recordId, ReadOnlySpan<byte> tombstoneChangeId)
        => _unsealedWal || _snapshotPin ||
           _recordIds.Contains(FldId128.From(recordId)) ||
           _changeIds.Contains(FldId128.From(tombstoneChangeId));

    /// <summary>
    /// An unsealed WAL is a cross-store uncertainty barrier for every receipt. A
    /// committed ApplyJournal receipt is FLD's independent replay/recovery evidence,
    /// not a reference to the separate BIN-SYNC receipt row, and therefore does not
    /// veto BIN-SYNC receipt compaction. Provider-snapshot pins are also independent.
    /// </summary>
    public bool HasReceiptReference(
        ReadOnlySpan<byte> senderPeer, uint authorEpoch,
        uint transportPeerEpoch, ushort namespaceId,
        ReadOnlySpan<byte> batchId)
    {
        _ = FldId128.From(senderPeer);
        _ = FldId128.From(batchId);
        if (authorEpoch == 0 || transportPeerEpoch == 0 || namespaceId == 0)
            throw new ArgumentOutOfRangeException(nameof(authorEpoch),
                "Receipt reference identity epochs and namespace must be nonzero.");
        return _unsealedWal;
    }

    /// <summary>Ends the caller's logical use of this index; it owns no database lock or file pin.</summary>
    public void Dispose()
    {
        // The snapshot owns only in-memory indexes; no file or lock remains pinned.
    }
}
