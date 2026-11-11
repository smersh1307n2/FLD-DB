using Fld1.Storage.Ipc;
using Fld1.Engine.Sync;

namespace Fld1.Engine.Db;

/// <summary>
/// Remote FLD Database facade. The caller persists ClientSequence and OperationId
/// before sending, and reuses both unchanged on retry across reconnects.
/// </summary>
public sealed class FldDatabaseIpcClient : IAsyncDisposable
{
    private readonly StorageIpcClientConnection _connection;

    private FldDatabaseIpcClient(StorageIpcClientConnection connection)
        => _connection = connection;

    public Guid ClientId => _connection.ClientId;

    public static async Task<FldDatabaseIpcClient> ConnectToDatabaseAsync(
        string rootDirectory,
        Guid clientId,
        TimeSpan timeout,
        CancellationToken cancellationToken = default)
    {
        StorageIpcClientConnection connection =
            await StorageIpcClientConnection.ConnectToDatabaseAsync(
                rootDirectory, 0, clientId, timeout, cancellationToken).ConfigureAwait(false);
        return new FldDatabaseIpcClient(connection);
    }

    public async Task<FldDatabaseIpcWriteResult> WriteBatchAsync(
        Guid operationId,
        ulong clientSequence,
        IReadOnlyList<FldDatabaseIpcMutation> mutations,
        CancellationToken cancellationToken = default)
    {
        byte[] payload = FldDatabaseIpcProtocol.EncodeWriteBatchPayload(clientSequence, mutations);
        ReadOnlyMemory<byte> response = await SendAsync(
            operationId, FldDatabaseIpcOperationCodes.WriteBatch, payload, cancellationToken).ConfigureAwait(false);
        (ulong commitSeq, bool duplicate, byte[] result) = FldDatabaseIpcProtocol.DecodeCommitResult(response);
        return FldDatabaseIpcProtocol.DecodeWriteResult(commitSeq, duplicate, result);
    }

    public async Task<FldDatabaseIpcReadResult> ReadFieldAsync(
        Guid operationId,
        ulong clientSequence,
        ReadOnlyMemory<byte> recordId,
        ushort fieldId,
        CancellationToken cancellationToken = default)
    {
        byte[] payload = FldDatabaseIpcProtocol.EncodeReadFieldPayload(
            clientSequence, recordId.Span, fieldId);
        ReadOnlyMemory<byte> response = await SendAsync(
            operationId, FldDatabaseIpcOperationCodes.ReadField, payload, cancellationToken).ConfigureAwait(false);
        (ulong commitSeq, bool duplicate, byte[] result) = FldDatabaseIpcProtocol.DecodeCommitResult(response);
        return FldDatabaseIpcProtocol.DecodeReadResult(commitSeq, duplicate, result);
    }

    public async Task<bool> AcknowledgeThroughAsync(
        ulong acknowledgedThroughSequence,
        CancellationToken cancellationToken = default)
    {
        byte[] payload = FldDatabaseIpcProtocol.EncodeAcknowledgementPayload(acknowledgedThroughSequence);
        ReadOnlyMemory<byte> response = await SendAsync(
            Guid.NewGuid(), FldDatabaseIpcOperationCodes.AcknowledgeReceipts, payload, cancellationToken)
            .ConfigureAwait(false);
        if (response.Length != 1 || response.Span[0] > 1)
            throw new InvalidDataException("FLD IPC acknowledgement response is invalid.");
        return response.Span[0] == 1;
    }

    public async Task<FldDatabaseIpcOutboxLease> AcquireOutboxLeaseAsync(
        CancellationToken cancellationToken = default)
    {
        ReadOnlyMemory<byte> response = await SendAsync(
            Guid.NewGuid(), FldDatabaseIpcOperationCodes.AcquireOutboxLease,
            ReadOnlyMemory<byte>.Empty, cancellationToken).ConfigureAwait(false);
        return FldDatabaseIpcProtocol.DecodeOutboxLeaseResponse(response);
    }

    /// <summary>
    /// Reads the next Outbox entry after afterSequence, or continues the exact entry
    /// identified by entrySequence. Large values are returned in bounded chunks.
    /// </summary>
    public async Task<FldDatabaseIpcOutboxEntryChunk?> ReadOutboxEntryChunkAsync(
        FldDatabaseIpcOutboxLease lease,
        ulong afterSequence,
        ulong entrySequence = 0,
        uint valueOffset = 0,
        uint maxChunkBytes = (uint)FldDatabaseIpcProtocol.MaxOutboxChunkBytes,
        CancellationToken cancellationToken = default)
    {
        ulong requestCursor = entrySequence == 0 ? afterSequence : entrySequence - 1;
        byte[] payload = FldDatabaseIpcProtocol.EncodeReadOutboxEntryChunkPayload(
            lease, requestCursor, entrySequence, valueOffset, maxChunkBytes);
        ReadOnlyMemory<byte> response = await SendAsync(
            Guid.NewGuid(), FldDatabaseIpcOperationCodes.ReadOutboxEntryChunk,
            payload, cancellationToken).ConfigureAwait(false);
        return FldDatabaseIpcProtocol.DecodeOutboxEntryChunkResponse(response);
    }

    public async Task<bool> AcknowledgeOutboxThroughAsync(
        FldDatabaseIpcOutboxLease lease,
        ulong throughSequence,
        CancellationToken cancellationToken = default)
    {
        byte[] payload = FldDatabaseIpcProtocol.EncodeOutboxAcknowledgementPayload(
            lease, throughSequence);
        ReadOnlyMemory<byte> response = await SendAsync(
            Guid.NewGuid(), FldDatabaseIpcOperationCodes.AcknowledgeOutbox,
            payload, cancellationToken).ConfigureAwait(false);
        return FldDatabaseIpcProtocol.DecodeOutboxBooleanResponse(response);
    }

    public async Task<bool> ReleaseOutboxLeaseAsync(
        FldDatabaseIpcOutboxLease lease,
        CancellationToken cancellationToken = default)
    {
        byte[] payload = FldDatabaseIpcProtocol.EncodeOutboxLeasePayload(lease);
        ReadOnlyMemory<byte> response = await SendAsync(
            Guid.NewGuid(), FldDatabaseIpcOperationCodes.ReleaseOutboxLease,
            payload, cancellationToken).ConfigureAwait(false);
        return FldDatabaseIpcProtocol.DecodeOutboxBooleanResponse(response);
    }

    /// <summary>
    /// Opens an immutable provider snapshot. Persist and reuse operationId when retrying
    /// an uncertain open request on the same Database host generation.
    /// </summary>
    public async Task<FldDatabaseIpcSnapshotHandle> OpenSnapshotAsync(
        Guid operationId,
        CancellationToken cancellationToken = default)
    {
        if (operationId == Guid.Empty)
            throw new ArgumentException("Snapshot-open operation id must be nonzero.", nameof(operationId));
        ReadOnlyMemory<byte> response = await SendAsync(
            operationId, FldDatabaseIpcOperationCodes.OpenSnapshot,
            ReadOnlyMemory<byte>.Empty, cancellationToken).ConfigureAwait(false);
        return FldDatabaseIpcProtocol.DecodeSnapshotOpenResponse(response);
    }

    /// <summary>Reads a bounded metadata page from the immutable snapshot generation.</summary>
    public async Task<FldProviderSnapshotMetadataPage> ReadSnapshotPageAsync(
        FldDatabaseIpcSnapshotHandle handle,
        int afterIndex,
        int limit = FldDatabaseIpcProtocol.MaxSnapshotPageEntries,
        CancellationToken cancellationToken = default)
    {
        byte[] payload = FldDatabaseIpcProtocol.EncodeSnapshotPagePayload(handle, afterIndex, limit);
        ReadOnlyMemory<byte> response = await SendAsync(
            Guid.NewGuid(), FldDatabaseIpcOperationCodes.ReadSnapshotPage,
            payload, cancellationToken).ConfigureAwait(false);
        return FldDatabaseIpcProtocol.DecodeSnapshotPageResponse(response);
    }

    /// <summary>Reads one bounded value slice by the RecordId/FieldId returned in a snapshot page.</summary>
    public async Task<FldProviderSnapshotValueChunk?> ReadSnapshotValueChunkAsync(
        FldDatabaseIpcSnapshotHandle handle,
        ReadOnlyMemory<byte> recordId,
        ushort fieldId,
        uint valueOffset = 0,
        uint maxChunkBytes = (uint)FldDatabaseIpcProtocol.MaxSnapshotValueChunkBytes,
        CancellationToken cancellationToken = default)
    {
        byte[] payload = FldDatabaseIpcProtocol.EncodeSnapshotValueChunkPayload(
            handle, recordId.ToArray(), fieldId, valueOffset, maxChunkBytes);
        ReadOnlyMemory<byte> response = await SendAsync(
            Guid.NewGuid(), FldDatabaseIpcOperationCodes.ReadSnapshotValueChunk,
            payload, cancellationToken).ConfigureAwait(false);
        return FldDatabaseIpcProtocol.DecodeSnapshotValueChunkResponse(response);
    }

    /// <summary>Releases a snapshot handle. Repeated release requests are idempotent.</summary>
    public async Task<bool> ReleaseSnapshotAsync(
        FldDatabaseIpcSnapshotHandle handle,
        CancellationToken cancellationToken = default)
    {
        byte[] payload = FldDatabaseIpcProtocol.EncodeSnapshotReleasePayload(handle);
        ReadOnlyMemory<byte> response = await SendAsync(
            Guid.NewGuid(), FldDatabaseIpcOperationCodes.ReleaseSnapshot,
            payload, cancellationToken).ConfigureAwait(false);
        return FldDatabaseIpcProtocol.DecodeSnapshotReleaseResponse(response);
    }

    /// <summary>
    /// Appends provider-owned local drafts with stable ChangeId, HLC and author epoch.
    /// Retry with the same OperationId, ClientSequence and exact mutation payload.
    /// </summary>
    public async Task<FldDatabaseIpcWriteResult> AppendLocalProviderBatchAsync(
        Guid operationId,
        ulong clientSequence,
        IReadOnlyList<FldLocalProviderMutation> mutations,
        CancellationToken cancellationToken = default)
    {
        if (operationId == Guid.Empty)
            throw new ArgumentException("Local provider IPC operation id must be nonzero.", nameof(operationId));
        byte[] payload = FldDatabaseIpcLocalProviderProtocol.EncodeRequest(clientSequence, mutations);
        ReadOnlyMemory<byte> response = await SendAsync(
            operationId, FldDatabaseIpcOperationCodes.AppendLocalProviderBatch, payload, cancellationToken)
            .ConfigureAwait(false);
        (ulong commitSeq, bool duplicate, byte[] result) = FldDatabaseIpcProtocol.DecodeCommitResult(response);
        return FldDatabaseIpcProtocol.DecodeWriteResult(commitSeq, duplicate, result);
    }

    /// <summary>
    /// Atomically applies one contextual remote batch. Retry with the same batch identity;
    /// ApplyJournal, rather than the IPC operation id, is the durable deduplication authority.
    /// </summary>
    public async Task<RemoteBatchReceipt> ApplyRemoteBatchAsync(
        Guid operationId,
        RemoteBatchCommit batch,
        CancellationToken cancellationToken = default)
    {
        if (operationId == Guid.Empty)
            throw new ArgumentException("Remote APPLY IPC operation id must be nonzero.", nameof(operationId));
        byte[] payload = FldDatabaseIpcRemoteApplyProtocol.EncodeRequest(batch);
        ReadOnlyMemory<byte> response = await SendAsync(
            operationId, FldDatabaseIpcOperationCodes.ApplyRemoteBatch, payload, cancellationToken)
            .ConfigureAwait(false);
        return FldDatabaseIpcRemoteApplyProtocol.DecodeReceipt(response);
    }

    private async Task<ReadOnlyMemory<byte>> SendAsync(
        Guid operationId,
        ushort operationCode,
        ReadOnlyMemory<byte> payload,
        CancellationToken cancellationToken)
    {
        byte[] request = StorageIpcOperationProtocol.Encode(
            new StorageIpcOperationEnvelope(operationId, operationCode, payload));
        return await _connection.RequestAsync(request, cancellationToken).ConfigureAwait(false);
    }

    public ValueTask DisposeAsync() => _connection.DisposeAsync();
}
