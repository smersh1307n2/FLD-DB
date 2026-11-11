using Fld1.Engine.Outbox;

namespace Fld1.Engine.Db;

public sealed partial class FldDatabase
{
    /// <summary>
    /// Acknowledges a BIN-SYNC safe Outbox prefix and retires the matching contiguous
    /// auto-sequenced provider-operation receipts. Provider OperationId is the big-endian
    /// Guid form of the mutation ChangeId. Receipt retirement happens before Outbox ACK:
    /// if the process stops between the two durable writes, replaying this call is safe.
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

        var prefix = new List<(ulong Sequence, ulong CommitSequence, Guid ChangeId)>();
        ulong cursor = acknowledgedOutbox;
        while (cursor < throughSequence)
        {
            int visited = _outbox.VisitBatch(cursor, 512, lease, (in OutboxEntryView entry) =>
            {
                if (entry.OutboxSeq > throughSequence)
                    return false;
                prefix.Add((
                    entry.OutboxSeq,
                    entry.CommitSeq,
                    new Guid(entry.ChangeId, bigEndian: true)));
                cursor = entry.OutboxSeq;
                return true;
            });
            if (visited == 0)
                break;
        }

        if (cursor < throughSequence)
            throw new InvalidDataException("The requested provider Outbox ACK prefix is not fully present.");

        if (prefix.Count == 0)
            return;

        // A provider batch may create multiple Outbox entries under one WAL commit.
        // Do not retire its receipt if the safe prefix ends in the middle of that commit.
        ulong lastCommitSequence = prefix[^1].CommitSequence;
        bool commitContinuesAfterPrefix = false;
        _outbox.VisitBatch(throughSequence, 1, lease, (in OutboxEntryView next) =>
        {
            commitContinuesAfterPrefix = next.CommitSeq == lastCommitSequence;
            return false;
        });
        if (commitContinuesAfterPrefix)
            prefix.RemoveAll(entry => entry.CommitSequence == lastCommitSequence);
        if (prefix.Count == 0)
            return;

        string clientKey = LocalOperationClientKey(lease.OwnerId);
        if (!_localOperationClients.TryGetValue(clientKey, out LocalOperationClientState? state))
            return;

        var coveredChangeIds = prefix.Select(entry => entry.ChangeId).ToHashSet();
        LocalOperationReceiptEntry[] receipts = _localOperationReceipts.Values
            .Where(receipt =>
                receipt.NamespaceId == state.NamespaceId &&
                receipt.ClientId.AsSpan().SequenceEqual(state.ClientId) &&
                receipt.ClientSequence > state.AcknowledgedThroughSequence)
            .OrderBy(receipt => receipt.ClientSequence)
            .ToArray();

        ulong safeReceiptSequence = state.AcknowledgedThroughSequence;
        foreach (LocalOperationReceiptEntry receipt in receipts)
        {
            // The async provider derives OperationId from ChangeId. Stop at the first
            // unrelated or not-yet-published receipt to preserve the contiguous prefix.
            Guid operationId = new(receipt.OperationId, bigEndian: true);
            if (!coveredChangeIds.Contains(operationId))
                break;
            safeReceiptSequence = receipt.ClientSequence;
        }

        if (safeReceiptSequence > state.AcknowledgedThroughSequence)
            AcknowledgeLocalOperationReceipts(state.ClientId, safeReceiptSequence);
    }
}
