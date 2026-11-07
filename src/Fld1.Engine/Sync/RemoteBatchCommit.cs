namespace Fld1.Engine.Sync;

/// <summary>
/// Validated, transport-neutral input for one atomic remote batch apply.
/// BIN-SYNC owns the meaning of all protocol fields and ProtocolStateDelta.
/// </summary>
public sealed class RemoteBatchCommit
{
    public byte[] SenderPeer { get; }
    public uint SenderEpoch { get; }
    public ushort NamespaceId { get; }
    public byte[] IssuerNodeId { get; }
    public uint IssuerEpoch { get; }
    public byte[] BatchId { get; }
    public ulong CursorStart { get; }
    public ulong CursorEnd { get; }
    public byte[] PreviousBatchDigest { get; }
    public byte[] BatchDigest { get; }
    public byte[] RecordsRoot { get; }
    public byte[] ProtocolStateDelta { get; }
    public IReadOnlyList<RemoteFieldWrite> Writes { get; }

    public RemoteBatchCommit(
        byte[] senderPeer,
        uint senderEpoch,
        ushort namespaceId,
        byte[] issuerNodeId,
        uint issuerEpoch,
        byte[] batchId,
        ulong cursorStart,
        ulong cursorEnd,
        byte[] previousBatchDigest,
        byte[] batchDigest,
        byte[] recordsRoot,
        IReadOnlyList<RemoteFieldWrite> writes,
        byte[] protocolStateDelta)
    {
        SenderPeer = CopyId(senderPeer, nameof(senderPeer), nonzero: true);
        IssuerNodeId = CopyId(issuerNodeId, nameof(issuerNodeId), nonzero: true);
        BatchId = CopyId(batchId, nameof(batchId), nonzero: true);
        PreviousBatchDigest = CopyId(previousBatchDigest, nameof(previousBatchDigest));
        BatchDigest = CopyId(batchDigest, nameof(batchDigest));
        RecordsRoot = CopyId(recordsRoot, nameof(recordsRoot));
        if (cursorStart == 0 || cursorEnd < cursorStart)
            throw new ArgumentOutOfRangeException(nameof(cursorStart), "Invalid incoming cursor range.");
        CursorStart = cursorStart;
        CursorEnd = cursorEnd;
        SenderEpoch = senderEpoch;
        IssuerEpoch = issuerEpoch;
        NamespaceId = namespaceId;

        ArgumentNullException.ThrowIfNull(writes);
        if (writes.Count == 0)
            throw new ArgumentException("A remote batch must contain at least one mutation.", nameof(writes));
        var copy = new RemoteFieldWrite[writes.Count];
        for (int i = 0; i < writes.Count; i++)
        {
            var write = writes[i];
            copy[i] = new RemoteFieldWrite(
                CopyId(write.RecordId, nameof(write.RecordId)),
                write.FieldId,
                write.Value.ToArray(),
                write.Wall,
                write.Logical,
                CopyId(write.AuthorNodeId, nameof(write.AuthorNodeId)),
                CopyId(write.ChangeId, nameof(write.ChangeId), nonzero: true),
                write.KeyEpoch,
                write.IsTombstone);
        }
        Writes = copy;

        ArgumentNullException.ThrowIfNull(protocolStateDelta);
        if (protocolStateDelta.Length > 4 * 1024 * 1024)
            throw new ArgumentOutOfRangeException(nameof(protocolStateDelta));
        ProtocolStateDelta = protocolStateDelta.ToArray();
    }

    private static byte[] CopyId(byte[]? value, string name, bool nonzero = false)
    {
        ArgumentNullException.ThrowIfNull(value, name);
        if (value.Length != 16)
            throw new ArgumentException($"{name} must be exactly 16 bytes.", name);
        if (nonzero)
        {
            byte aggregate = 0;
            foreach (byte b in value) aggregate |= b;
            if (aggregate == 0)
                throw new ArgumentException($"{name} must be nonzero.", name);
        }
        return value.ToArray();
    }
}

/// <summary>Durable result of ApplyRemoteBatch; safe to use as the basis for protocol ACK.</summary>
public sealed class RemoteBatchReceipt
{
    public required ulong CommitSeq { get; init; }
    public required ulong CursorStart { get; init; }
    public required ulong CursorEnd { get; init; }
    public required byte[] BatchId { get; init; }
    public required byte[] ProtocolStateDelta { get; init; }
    public required int AppliedCount { get; init; }
    public required bool IsDuplicate { get; init; }
}

/// <summary>One durable journal batch; JournalSeq is a local paging token, not a sync cursor.</summary>
public sealed class RelayJournalBatch
{
    public required ulong JournalSeq { get; init; }
    public required ulong CommitSeq { get; init; }
    public required RemoteBatchCommit Batch { get; init; }
}
