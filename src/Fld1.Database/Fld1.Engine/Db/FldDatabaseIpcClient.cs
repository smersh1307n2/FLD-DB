using Fld1.Storage.Ipc;

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
        uint maxChunkBytes = 256 * 1024,
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
