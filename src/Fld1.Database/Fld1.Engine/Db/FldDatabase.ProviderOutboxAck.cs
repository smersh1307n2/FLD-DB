using Fld1.Manifest.Format;
using Fld1.Engine.Outbox;

namespace Fld1.Engine.Db;

public sealed partial class FldDatabase
{
    /// <summary>
    /// Acknowledges a BIN-SYNC safe Outbox prefix and retires the matching contiguous
    /// local-operation receipts. A receipt is eligible only when every Outbox entry
    /// produced by its WAL commit is inside the acknowledged prefix. Receipt-only reads
    /// have no Outbox commit and therefore remain independently acknowledged.
    /// </summary>
    public void AcknowledgeProviderOutbox(ulong throughSequence, OutboxLease lease)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentNullException.ThrowIfNull(lease);

        lock (_commitLock)
        {
            EnsureHealthy();
            AcknowledgeProviderReceiptsForOutboxPrefix(throughSequence, lease);
            _outbox.Acknowledge(throughSequence, lease);
        }
    }

    private void AcknowledgeProviderReceiptsForOutboxPrefix(
        ulong throughSequence,
        OutboxLease lease)
    {
        ulong acknowledgedOutbox = _outbox.AckedThrough;
        ulong highWatermark = _outbox.HighWatermark;
        if (throughSequence > highWatermark)
            throw new ArgumentOutOfRangeException(nameof(throughSequence));
        if (throughSequence <= acknowledgedOutbox)
            return;

        var prefix = new List<(ulong Sequence, ulong CommitSequence)>();
        ulong cursor = acknowledgedOutbox;
        while (cursor < throughSequence)
        {
            ulong previousCursor = cursor;
            int visited = _outbox.VisitBatch(cursor, 512, lease, (in OutboxEntryView entry) =>
            {
                if (entry.OutboxSeq > throughSequence)
                    return false;
                prefix.Add((entry.OutboxSeq, entry.CommitSeq));
                cursor = entry.OutboxSeq;
                return true;
            });
            if (visited == 0 || cursor == previousCursor)
                break;
        }

        if (cursor < throughSequence)
            throw new InvalidDataException("The requested provider Outbox ACK prefix is not fully present.");
        if (prefix.Count == 0)
            return;

        // A single WAL commit can append several Outbox entries. Exclude the final
        // commit group if the safe prefix ends before that group's last entry.
        ulong lastCommitSequence = prefix[^1].CommitSequence;
        bool commitContinuesAfterPrefix = false;
        _outbox.VisitBatch(throughSequence, 1, lease, (in OutboxEntryView next) =>
        {
            commitContinuesAfterPrefix = next.CommitSeq == lastCommitSequence;
            return false;
        });

        var completeCommitSequences = prefix
            .Where(entry => !commitContinuesAfterPrefix || entry.CommitSequence != lastCommitSequence)
            .Select(entry => entry.CommitSequence)
            .ToHashSet();
        if (completeCommitSequences.Count == 0)
            return;

        if (!_localOperationClients.TryGetValue(
            LocalOperationClientKey(lease.OwnerId), out LocalOperationClientState? state))
            return;

        ulong safeReceiptSequence = state.AcknowledgedThroughSequence;
        LocalOperationReceiptEntry[] receipts = _localOperationReceipts.Values
            .Where(receipt =>
                receipt.NamespaceId == state.NamespaceId &&
                receipt.ClientId.AsSpan().SequenceEqual(state.ClientId) &&
                receipt.ClientSequence > state.AcknowledgedThroughSequence)
            .OrderBy(receipt => receipt.ClientSequence)
            .ToArray();

        foreach (LocalOperationReceiptEntry receipt in receipts)
        {
            // Stop at the first receipt whose commit is not fully covered. This keeps
            // the receipt ACK a contiguous prefix even when read-only receipts are mixed in.
            if (!completeCommitSequences.Contains(receipt.CommitSeq))
                break;
            safeReceiptSequence = receipt.ClientSequence;
        }

        if (safeReceiptSequence > state.AcknowledgedThroughSequence)
            AcknowledgeLocalOperationReceipts(state.ClientId, safeReceiptSequence);
    }
}

