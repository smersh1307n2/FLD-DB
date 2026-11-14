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

    /// <summary>Read the immutable Metadata-Store-owned publication cutover for this database namespace.</summary>
    public async Task<(bool Exists, ulong NodeOutboxHighWatermark)> TryGetMetadataStoreOwnedPublicationCutoverAsync(
        CancellationToken cancellationToken = default)
    {
        ReadOnlyMemory<byte> response = await SendAsync(
            Guid.NewGuid(), FldDatabaseIpcOperationCodes.TryGetMetadataStoreOwnedPublicationCutover,
            ReadOnlyMemory<byte>.Empty, cancellationToken).ConfigureAwait(false);
        return FldDatabaseIpcPublicationModeProtocol.DecodeTryGetResult(response);
    }

    /// <summary>Enable the immutable Metadata-Store-owned publication cutover.</summary>
    public async Task EnableMetadataStoreOwnedPublicationAsync(
        ulong nodeOutboxHighWatermark,
        CancellationToken cancellationToken = default)
    {
        byte[] payload = FldDatabaseIpcPublicationModeProtocol.EncodeEnableRequest(nodeOutboxHighWatermark);
        ReadOnlyMemory<byte> response = await SendAsync(
            Guid.NewGuid(), FldDatabaseIpcOperationCodes.EnableMetadataStoreOwnedPublication,
            payload, cancellationToken).ConfigureAwait(false);
        if (!response.IsEmpty)
            throw new InvalidDataException("FLD publication cutover acknowledgement must be empty.");
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
    /// Reclaims only the exact tombstone identities approved by the caller. The Host
    /// reopens its own reference snapshot and revalidates each candidate under the
    /// database commit monitor before publishing the replacement Run.
    /// </summary>
    public async Task<int> CompactTombstonesAsync(
        IReadOnlyList<FldTombstoneGcCandidate> approvedCandidates,
        int limit = FldDatabaseIpcProtocol.MaxTombstoneGcCandidates,
        CancellationToken cancellationToken = default)
    {
        byte[] payload = FldDatabaseIpcProtocol.EncodeTombstoneGcRequest(approvedCandidates, limit);
        ReadOnlyMemory<byte> response = await SendAsync(
            Guid.NewGuid(), FldDatabaseIpcOperationCodes.CompactTombstones,
            payload, cancellationToken).ConfigureAwait(false);
        return FldDatabaseIpcProtocol.DecodeTombstoneGcResponse(response, limit);
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
    /// The response includes the assigned ClientSequence; the caller must persist it before the
    /// separate receipt ACK. The caller supplies a stable OperationId (normally derived from the
    /// mutation ChangeId); the same id and exact payload are safe to retry across client and Host restarts.
    /// </summary>
    public async Task<FldDatabaseIpcAutoSequenceWriteResult> AppendLocalProviderBatchAutoSequenceAsync(
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
        return FldDatabaseIpcProtocol.DecodeAutoSequenceWriteResult(response);
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

    /// <summary>
    /// Acquires the exclusive Host-wide boundary used to compose Metadata GC with
    /// the FLD reference snapshot. The lease is scoped to this authenticated IPC session;
    /// closing the connection releases it even if the caller crashes.
    /// </summary>
    public async Task<Guid> AcquireGcLeaseAsync(CancellationToken cancellationToken = default)
    {
        ReadOnlyMemory<byte> response = await SendAsync(
            Guid.NewGuid(), FldDatabaseIpcOperationCodes.AcquireGcLease,
            ReadOnlyMemory<byte>.Empty, cancellationToken).ConfigureAwait(false);
        return FldDatabaseIpcProtocol.DecodeGcLeaseToken(response);
    }

    /// <summary>Renews the idle deadline of the exclusive Host-wide GC boundary.</summary>
    public async Task RenewGcLeaseAsync(Guid token, CancellationToken cancellationToken = default)
    {
        byte[] payload = FldDatabaseIpcProtocol.EncodeGcLeaseToken(token);
        ReadOnlyMemory<byte> response = await SendAsync(
            Guid.NewGuid(), FldDatabaseIpcOperationCodes.RenewGcLease,
            payload, cancellationToken).ConfigureAwait(false);
        if (!response.IsEmpty)
            throw new InvalidDataException("FLD GC lease renewal response must be empty.");
    }

    /// <summary>Releases the exclusive Host-wide GC boundary.</summary>
    public async Task ReleaseGcLeaseAsync(Guid token, CancellationToken cancellationToken = default)
    {
        byte[] payload = FldDatabaseIpcProtocol.EncodeGcLeaseToken(token);
        ReadOnlyMemory<byte> response = await SendAsync(
            Guid.NewGuid(), FldDatabaseIpcOperationCodes.ReleaseGcLease,
            payload, cancellationToken).ConfigureAwait(false);
        if (!response.IsEmpty)
            throw new InvalidDataException("FLD GC lease release response must be empty.");
    }

    /// <summary>Opens one immutable Host reference index while the caller owns the GC lease.</summary>
    public async Task<FldDatabaseIpcGcReferenceScan> OpenGcReferenceScanAsync(
        CancellationToken cancellationToken = default)
    {
        ReadOnlyMemory<byte> response = await SendAsync(
            Guid.NewGuid(), FldDatabaseIpcOperationCodes.OpenGcReferenceScan,
            ReadOnlyMemory<byte>.Empty, cancellationToken).ConfigureAwait(false);
        return FldDatabaseIpcGcProtocol.DecodeScanResponse(response);
    }

    public async Task<bool> HasGcChangeIdReferenceAsync(
        Guid token, ReadOnlyMemory<byte> changeId,
        CancellationToken cancellationToken = default)
    {
        byte[] payload = FldDatabaseIpcGcProtocol.EncodeChangeIdQuery(token, changeId.Span);
        ReadOnlyMemory<byte> response = await SendAsync(
            Guid.NewGuid(), FldDatabaseIpcOperationCodes.QueryGcReferenceScan,
            payload, cancellationToken).ConfigureAwait(false);
        return FldDatabaseIpcGcProtocol.DecodeBooleanResponse(response);
    }

    public async Task<bool> HasGcRecordReferenceAsync(
        Guid token, ReadOnlyMemory<byte> recordId, ReadOnlyMemory<byte> tombstoneChangeId,
        CancellationToken cancellationToken = default)
    {
        byte[] payload = FldDatabaseIpcGcProtocol.EncodeRecordQuery(
            token, recordId.Span, tombstoneChangeId.Span);
        ReadOnlyMemory<byte> response = await SendAsync(
            Guid.NewGuid(), FldDatabaseIpcOperationCodes.QueryGcReferenceScan,
            payload, cancellationToken).ConfigureAwait(false);
        return FldDatabaseIpcGcProtocol.DecodeBooleanResponse(response);
    }

    public async Task<bool> HasGcReceiptReferenceAsync(
        Guid token, ReadOnlyMemory<byte> senderPeer, uint authorEpoch,
        uint transportPeerEpoch, ReadOnlyMemory<byte> batchId,
        CancellationToken cancellationToken = default)
    {
        byte[] payload = FldDatabaseIpcGcProtocol.EncodeReceiptQuery(
            token, senderPeer.Span, authorEpoch, transportPeerEpoch, batchId.Span);
        ReadOnlyMemory<byte> response = await SendAsync(
            Guid.NewGuid(), FldDatabaseIpcOperationCodes.QueryGcReferenceScan,
            payload, cancellationToken).ConfigureAwait(false);
        return FldDatabaseIpcGcProtocol.DecodeBooleanResponse(response);
    }

    public async Task ReleaseGcReferenceScanAsync(
        Guid token, CancellationToken cancellationToken = default)
    {
        byte[] payload = FldDatabaseIpcGcProtocol.EncodeToken(token);
        ReadOnlyMemory<byte> response = await SendAsync(
            Guid.NewGuid(), FldDatabaseIpcOperationCodes.ReleaseGcReferenceScan,
            payload, cancellationToken).ConfigureAwait(false);
        if (!response.IsEmpty)
            throw new InvalidDataException("FLD GC reference-scan release response must be empty.");
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
