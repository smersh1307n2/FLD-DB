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

internal readonly record struct FldReceiptReferenceKey(
    FldId128 SenderPeer,
    uint TransportEpoch,
    uint AuthorEpoch,
    ushort NamespaceId,
    FldId128 BatchId);

/// <summary>
/// Immutable, partition-local reference index built while the caller holds the FLD
/// commit gate. It lets the BIN-SYNC adapter answer many candidate queries without
/// rescanning Outbox and ApplyJournal for every ChangeId/RecordId.
/// </summary>
public sealed class FldReferenceSnapshot : IDisposable
{
    private readonly HashSet<FldId128> _changeIds;
    private readonly HashSet<FldId128> _recordIds;
    private readonly HashSet<FldReceiptReferenceKey> _receipts;
    private readonly bool _unsealedWal;
    private readonly bool _snapshotPin;

    internal FldReferenceSnapshot(
        FldDraftHorizon drafts,
        HashSet<FldId128> changeIds,
        HashSet<FldId128> recordIds,
        HashSet<FldReceiptReferenceKey> receipts,
        bool unsealedWal,
        bool snapshotPin)
    {
        UnpublishedLocalDraftHorizon = drafts;
        _changeIds = changeIds;
        _recordIds = recordIds;
        _receipts = receipts;
        _unsealedWal = unsealedWal;
        _snapshotPin = snapshotPin;
    }

    public FldDraftHorizon UnpublishedLocalDraftHorizon { get; }

    /// <summary>True while any in-memory or persisted provider-snapshot generation is pinned.</summary>
    public bool HasSnapshotPin => _snapshotPin;

    public bool HasChangeIdReference(ReadOnlySpan<byte> changeId)
        => _unsealedWal || _snapshotPin || _changeIds.Contains(FldId128.From(changeId));

    public bool HasRecordReference(
        ReadOnlySpan<byte> recordId, ReadOnlySpan<byte> tombstoneChangeId)
        => _unsealedWal || _snapshotPin ||
           _recordIds.Contains(FldId128.From(recordId)) ||
           _changeIds.Contains(FldId128.From(tombstoneChangeId));

    public bool HasReceiptReference(
        ReadOnlySpan<byte> senderPeer, uint authorEpoch,
        uint transportPeerEpoch, ushort namespaceId,
        ReadOnlySpan<byte> batchId)
        => _unsealedWal || _receipts.Contains(new FldReceiptReferenceKey(
            FldId128.From(senderPeer), transportPeerEpoch, authorEpoch,
            namespaceId, FldId128.From(batchId)));

    public void Dispose()
    {
        // The snapshot owns only in-memory indexes; no file or lock remains pinned.
    }
}
