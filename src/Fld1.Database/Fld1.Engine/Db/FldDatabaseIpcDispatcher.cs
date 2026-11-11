using Fld1.Storage.Ipc;

namespace Fld1.Engine.Db;

/// <summary>Dispatches local Database IPC operations to one owned FldDatabase instance.</summary>
public sealed class FldDatabaseIpcDispatcher
{
    private readonly FldDatabase _database;

    public FldDatabaseIpcDispatcher(FldDatabase database)
        => _database = database ?? throw new ArgumentNullException(nameof(database));

    public ValueTask<ReadOnlyMemory<byte>> HandleAsync(
        StorageIpcRequestContext context,
        ReadOnlyMemory<byte> payload,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (context.ClientId == Guid.Empty)
            throw new InvalidDataException("FLD IPC ClientId must be nonzero.");

        StorageIpcOperationEnvelope operation = StorageIpcOperationProtocol.Decode(payload);
        Span<byte> clientId = stackalloc byte[16];
        Span<byte> operationId = stackalloc byte[16];
        context.ClientId.TryWriteBytes(clientId, bigEndian: true, out _);
        operation.OperationId.TryWriteBytes(operationId, bigEndian: true, out _);

        byte[] response = operation.OperationCode switch
        {
            FldDatabaseIpcOperationCodes.WriteBatch =>
                HandleWriteBatch(clientId, operationId, operation),
            FldDatabaseIpcOperationCodes.ReadField =>
                HandleReadField(clientId, operationId, operation),
            FldDatabaseIpcOperationCodes.AcknowledgeReceipts =>
                HandleAcknowledgement(clientId, operation.Payload),
            _ => throw new NotSupportedException($"Unsupported FLD Database IPC operation code {operation.OperationCode}.")
        };
        return ValueTask.FromResult<ReadOnlyMemory<byte>>(response);
    }

    private byte[] HandleWriteBatch(
        ReadOnlySpan<byte> clientId,
        ReadOnlySpan<byte> operationId,
        StorageIpcOperationEnvelope operation)
    {
        (ulong sequence, IReadOnlyList<FldDatabaseIpcMutation> mutations) =
            FldDatabaseIpcProtocol.DecodeWriteBatchPayload(operation.Payload);
        byte[] fingerprint = FldDatabaseIpcProtocol.ComputeFingerprint(
            operation.OperationCode, operation.Payload.Span);
        var batch = new FldWriteBatch();
        foreach (FldDatabaseIpcMutation mutation in mutations)
        {
            if (mutation.IsDelete)
                batch.Delete(mutation.RecordId, mutation.FieldId, mutation.KeyEpoch);
            else
                batch.Put(mutation.RecordId, mutation.FieldId, mutation.Value, mutation.KeyEpoch);
        }

        var receipt = new FldLocalOperationReceipt
        {
            ClientId = clientId.ToArray(),
            OperationId = operationId.ToArray(),
            ClientSequence = sequence,
            Fingerprint = fingerprint,
            Result = FldDatabaseIpcProtocol.EncodeWriteResult(mutations.Count)
        };
        FldLocalOperationCommitResult committed = _database.ApplyLocalOperation(batch, receipt);
        return FldDatabaseIpcProtocol.EncodeCommitResult(committed);
    }

    private byte[] HandleReadField(
        ReadOnlySpan<byte> clientId,
        ReadOnlySpan<byte> operationId,
        StorageIpcOperationEnvelope operation)
    {
        (ulong sequence, byte[] recordId, ushort fieldId) =
            FldDatabaseIpcProtocol.DecodeReadFieldPayload(operation.Payload);
        byte[] fingerprint = FldDatabaseIpcProtocol.ComputeFingerprint(
            operation.OperationCode, operation.Payload.Span);
        FldLocalOperationCommitResult result = _database.ReadFieldWithReceipt(
            recordId, fieldId, clientId, operationId, sequence, fingerprint);
        return FldDatabaseIpcProtocol.EncodeCommitResult(result);
    }

    private byte[] HandleAcknowledgement(
        ReadOnlySpan<byte> clientId,
        ReadOnlyMemory<byte> payload)
    {
        ulong throughSequence = FldDatabaseIpcProtocol.DecodeAcknowledgementPayload(payload);
        bool acknowledged = _database.AcknowledgeLocalOperationReceipts(clientId, throughSequence);
        return [acknowledged ? (byte)1 : (byte)0];
    }
}
