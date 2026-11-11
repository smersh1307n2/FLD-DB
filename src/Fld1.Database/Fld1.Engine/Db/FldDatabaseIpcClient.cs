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

    /// <summary>Atomically stamps current fields with immutable publication origins.</summary>
    public async Task<int> CommitPublishedOriginsAsync(
        IReadOnlyList<FldPublishedOrigin> publishedOrigins,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(publishedOrigins);
        if (publishedOrigins.Count == 0) return 0;
        byte[] payload = FldDatabaseIpcPublishedOriginProtocol.EncodeRequest(publishedOrigins);
        ReadOnlyMemory<byte> response = await SendAsync(
            Guid.NewGuid(), FldDatabaseIpcOperationCodes.CommitPublishedOrigins,
            payload, cancellationToken).ConfigureAwait(false);
        return FldDatabaseIpcPublishedOriginProtocol.DecodeResult(response);
    }

    /// <summary>Reads the authoritative LocalAuthor identity configured by the Database Host.</summary>
    public async Task<byte[]> GetLocalAuthorAsync(CancellationToken cancellationToken = default)
    {
        ReadOnlyMemory<byte> response = await SendAsync(
            Guid.NewGuid(), FldDatabaseIpcOperationCodes.GetLocalAuthor,
            ReadOnlyMemory<byte>.Empty, cancellationToken).ConfigureAwait(false);
        return FldDatabaseIpcProtocol.DecodeLocalAuthorResponse(response);
    }

    public async Task<FldDatabaseIpcCurrentMetadata?> ReadCurrentMetadataAsync(ReadOnlyMemory<byte> recordId,ushort fieldId,CancellationToken cancellationToken=default)
    {
        byte[] p=FldDatabaseIpcProtocol.EncodeReadCurrentMetadataPayload(recordId.Span,fieldId);
        var r=await SendAsync(Guid.NewGuid(),FldDatabaseIpcOperationCodes.ReadCurrentMetadata,p,cancellationToken).ConfigureAwait(false);
        return FldDatabaseIpcProtocol.DecodeCurrentMetadataResponse(r);
    }
    public async Task<FldDatabaseIpcCurrentValueChunk> ReadCurrentValueChunkAsync(ReadOnlyMemory<byte> recordId,ushort fieldId,ReadOnlyMemory<byte> expectedChangeId,uint valueOffset=0,uint maxChunkBytes=(uint)FldDatabaseIpcProtocol.MaxCurrentValueChunkBytes,CancellationToken cancellationToken=default)
    {
        byte[] p=FldDatabaseIpcProtocol.EncodeReadCurrentValueChunkPayload(recordId.Span,fieldId,expectedChangeId.Span,valueOffset,maxChunkBytes);
        var r=await SendAsync(Guid.NewGuid(),FldDatabaseIpcOperationCodes.ReadCurrentValueChunk,p,cancellationToken).ConfigureAwait(false);
        return FldDatabaseIpcProtocol.DecodeCurrentValueChunkResponse(r);
    }

    /// <summary>Reads the current durable Outbox high watermark without acquiring a consumer lease.</summary>
    public async Task<ulong> GetOutboxHighWatermarkAsync(
        CancellationToken cancellationToken = default)
    {
        ReadOnlyMemory<byte> response = await SendAsync(
            Guid.NewGuid(), FldDatabaseIpcOperationCodes.GetOutboxHighWatermark,
            ReadOnlyMemory<byte>.Empty, cancellationToken).ConfigureAwait(false);
        return FldDatabaseIpcProtocol.DecodeOutboxHighWatermarkResponse(response);
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

    /// <summary>
    /// Acknowledges the safe Outbox prefix and retires the matching contiguous
    /// auto-sequenced provider append receipts.
    /// </summary>
    public async Task<bool> AcknowledgeProviderOutboxThroughAsync(
        FldDatabaseIpcOutboxLease lease,
        ulong throughSequence,
        CancellationToken cancellationToken = default)
    {
        byte[] payload = FldDatabaseIpcProtocol.EncodeOutboxAcknowledgementPayload(
            lease, throughSequence);
        ReadOnlyMemory<byte> response = await SendAsync(
            Guid.NewGuid(), FldDatabaseIpcOperationCodes.AcknowledgeProviderOutbox,
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
    /// Appends an identity-preserving provider batch with server-assigned contiguous receipt sequence.
    /// The caller supplies a stable OperationId (normally derived from the mutation ChangeId);
    /// the same id and exact payload are safe to retry across client and Host restarts.
    /// </summary>
    public async Task<FldDatabaseIpcWriteResult> AppendLocalProviderBatchAutoSequenceAsync(
        Guid operationId,
        IReadOnlyList<FldLocalProviderMutation> mutations,
        CancellationToken cancellationToken = default)
    {
        if (operationId == Guid.Empty)
            throw new ArgumentException("Local provider IPC operation id must be nonzero.", nameof(operationId));
        byte[] payload = FldDatabaseIpcLocalProviderProtocol.EncodeAutoSequenceRequest(mutations);
        ReadOnlyMemory<byte> response = await SendAsync(
            operationId, FldDatabaseIpcOperationCodes.AppendLocalProviderBatchAutoSequence,
            payload, cancellationToken).ConfigureAwait(false);
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
