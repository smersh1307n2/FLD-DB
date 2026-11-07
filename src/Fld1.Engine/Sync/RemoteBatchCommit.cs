using System.Buffers.Binary;

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
    /// <summary>BIN-SYNC-selected relay subset; includes LWW losers, excludes seen/self-echo operations.</summary>
    public IReadOnlyList<RemoteFieldWrite> RelayWrites { get; }

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
        IReadOnlyList<RemoteFieldWrite> relayWrites,
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
        Writes = CopyWrites(writes, nameof(writes));
        RelayWrites = CopyWrites(relayWrites, nameof(relayWrites));
        ValidateRelaySubset(Writes, RelayWrites);

        ArgumentNullException.ThrowIfNull(protocolStateDelta);
        if (protocolStateDelta.Length > 4 * 1024 * 1024)
            throw new ArgumentOutOfRangeException(nameof(protocolStateDelta));
        ProtocolStateDelta = protocolStateDelta.ToArray();
    }

    private readonly record struct ChangeKey(ulong Low, ulong High)
    {
        public static ChangeKey From(ReadOnlySpan<byte> id)
            => new(BinaryPrimitives.ReadUInt64LittleEndian(id),
                BinaryPrimitives.ReadUInt64LittleEndian(id.Slice(8)));
    }

    private static void ValidateRelaySubset(
        IReadOnlyList<RemoteFieldWrite> writes,
        IReadOnlyList<RemoteFieldWrite> relayWrites)
    {
        var byChangeId = new Dictionary<ChangeKey, RemoteFieldWrite>(writes.Count);
        foreach (var write in writes)
        {
            if (!byChangeId.TryAdd(ChangeKey.From(write.ChangeId), write))
                throw new ArgumentException("ChangeId values must be unique within a remote batch.", nameof(writes));
        }

        var seenRelayIds = new HashSet<ChangeKey>();
        foreach (var relay in relayWrites)
        {
            var key = ChangeKey.From(relay.ChangeId);
            if (!seenRelayIds.Add(key))
                throw new ArgumentException("Relay ChangeId values must be unique.", nameof(relayWrites));
            if (!byChangeId.TryGetValue(key, out var source) || !SameOperation(source, relay))
                throw new ArgumentException("RelayWrites must be an exact subset of Writes.", nameof(relayWrites));
        }
    }

    private static bool SameOperation(in RemoteFieldWrite left, in RemoteFieldWrite right)
        => left.FieldId == right.FieldId
            && left.Wall == right.Wall
            && left.Logical == right.Logical
            && left.KeyEpoch == right.KeyEpoch
            && left.IsTombstone == right.IsTombstone
            && left.RecordId.AsSpan().SequenceEqual(right.RecordId)
            && left.AuthorNodeId.AsSpan().SequenceEqual(right.AuthorNodeId)
            && left.Value.AsSpan().SequenceEqual(right.Value);

    private static RemoteFieldWrite[] CopyWrites(
        IReadOnlyList<RemoteFieldWrite>? writes, string name)
    {
        ArgumentNullException.ThrowIfNull(writes, name);
        var copy = new RemoteFieldWrite[writes.Count];
        for (int i = 0; i < writes.Count; i++)
        {
            var write = writes[i];
            var value = write.Value.ToArray();
            if (write.IsTombstone && value.Length != 0)
                throw new ArgumentException("Tombstone payload must be empty.", name);
            copy[i] = new RemoteFieldWrite(
                CopyId(write.RecordId, nameof(write.RecordId)),
                write.FieldId,
                value,
                write.Wall,
                write.Logical,
                CopyId(write.AuthorNodeId, nameof(write.AuthorNodeId), nonzero: true),
                CopyId(write.ChangeId, nameof(write.ChangeId), nonzero: true),
                write.KeyEpoch,
                write.IsTombstone);
        }
        return copy;
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
