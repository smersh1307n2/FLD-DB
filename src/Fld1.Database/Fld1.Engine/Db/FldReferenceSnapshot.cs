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
    /// Returns whether the receipt key is retained by unsealed WAL or the captured
    /// ApplyJournal receipt index. Provider-snapshot pins do not extend receipt lifetime.
    /// </summary>
    /// <param name="senderPeer">Canonical 16-byte transport sender identity.</param>
    /// <param name="authorEpoch">Wire author epoch.</param>
    /// <param name="transportPeerEpoch">Sender transport-peer epoch.</param>
    /// <param name="namespaceId">FLD namespace of the receipt.</param>
    /// <param name="batchId">Canonical 16-byte batch identity.</param>
    public bool HasReceiptReference(
        ReadOnlySpan<byte> senderPeer, uint authorEpoch,
        uint transportPeerEpoch, ushort namespaceId,
        ReadOnlySpan<byte> batchId)
        => _unsealedWal || _receipts.Contains(new FldReceiptReferenceKey(
            FldId128.From(senderPeer), transportPeerEpoch, authorEpoch,
            namespaceId, FldId128.From(batchId)));

    /// <summary>Ends the caller's logical use of this index; it owns no database lock or file pin.</summary>
    public void Dispose()
    {
        // The snapshot owns only in-memory indexes; no file or lock remains pinned.
    }
}
