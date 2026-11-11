using Fld1.Engine.Outbox;
using Fld1.Storage.Ipc;
using Fld1.Engine.Sync;

namespace Fld1.Engine.Db;

/// <summary>Dispatches local Database IPC operations to one owned FldDatabase instance.</summary>
public sealed class FldDatabaseIpcDispatcher : IDisposable
{
    private const int MaxActiveSnapshotHandles = 64;
    private const int MaxActiveSnapshotHandlesPerClient = 4;

    private readonly FldDatabase _database;
    private readonly object _snapshotGate = new();
    private readonly Dictionary<Guid, IpcSnapshotHandle> _snapshotHandles = new();
    private readonly Dictionary<(Guid ClientId, Guid OperationId), Guid> _snapshotOpenOperations = new();
    private readonly Guid _hostGeneration = Guid.NewGuid();
    private volatile bool _disposed;

    private sealed class IpcSnapshotHandle(Guid clientId, Guid openOperationId, FldProviderSnapshot snapshot)
    {
        public Guid ClientId { get; } = clientId;
        public Guid OpenOperationId { get; } = openOperationId;
        public FldProviderSnapshot Snapshot { get; } = snapshot;
        public object Gate { get; } = new();
        public bool Released { get; set; }
    }

    public FldDatabaseIpcDispatcher(FldDatabase database)
        => _database = database ?? throw new ArgumentNullException(nameof(database));

    /// <summary>Stable for this dispatcher lifetime; changes when the Database host restarts.</summary>
    public Guid HostGeneration => _hostGeneration;

    public ValueTask<ReadOnlyMemory<byte>> HandleAsync(
        StorageIpcRequestContext context,
        ReadOnlyMemory<byte> payload,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        ThrowIfDisposed();
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
            FldDatabaseIpcOperationCodes.AcquireOutboxLease =>
                HandleAcquireOutboxLease(clientId, operation.Payload),
            FldDatabaseIpcOperationCodes.ReadOutboxEntryChunk =>
                HandleReadOutboxEntryChunk(clientId, operation.Payload),
            FldDatabaseIpcOperationCodes.AcknowledgeOutbox =>
                HandleOutboxAcknowledgement(clientId, operation.Payload),
            FldDatabaseIpcOperationCodes.ReleaseOutboxLease =>
                HandleReleaseOutboxLease(clientId, operation.Payload),
            FldDatabaseIpcOperationCodes.OpenSnapshot =>
                HandleOpenSnapshot(context.ClientId, operation.OperationId, operation.Payload),
            FldDatabaseIpcOperationCodes.ReadSnapshotPage =>
                HandleReadSnapshotPage(context.ClientId, operation.Payload),
            FldDatabaseIpcOperationCodes.ReadSnapshotValueChunk =>
                HandleReadSnapshotValueChunk(context.ClientId, operation.Payload),
            FldDatabaseIpcOperationCodes.ReleaseSnapshot =>
                HandleReleaseSnapshot(context.ClientId, operation.Payload),
            FldDatabaseIpcOperationCodes.ApplyRemoteBatch =>
                HandleApplyRemoteBatch(operation.Payload),
            FldDatabaseIpcOperationCodes.AppendLocalProviderBatch =>
                HandleAppendLocalProviderBatch(clientId, operationId, operation),
            FldDatabaseIpcOperationCodes.GetOutboxHighWatermark =>
                HandleGetOutboxHighWatermark(operation.Payload),
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
    private byte[] HandleGetOutboxHighWatermark(ReadOnlyMemory<byte> payload)
    {
        if (!payload.IsEmpty)
            throw new InvalidDataException("FLD Outbox high-watermark request must be empty.");
        return FldDatabaseIpcProtocol.EncodeOutboxHighWatermarkResponse(
            _database.GetOutboxHighWatermark());
    }

    private byte[] HandleAcquireOutboxLease(
        ReadOnlySpan<byte> clientId,
        ReadOnlyMemory<byte> payload)
    {
        if (!payload.IsEmpty)
            throw new InvalidDataException("FLD Outbox lease acquisition payload must be empty.");
        OutboxLease lease = _database.AcquireOutboxLease(clientId);
        return FldDatabaseIpcProtocol.EncodeOutboxLeaseResponse(lease.Epoch);
    }

    private byte[] HandleReadOutboxEntryChunk(
        ReadOnlySpan<byte> clientId,
        ReadOnlyMemory<byte> payload)
    {
        var request = FldDatabaseIpcProtocol.DecodeReadOutboxEntryChunkPayload(payload);
        var lease = new OutboxLease(request.LeaseEpoch, clientId.ToArray());
        ulong afterSequence = request.EntrySequence == 0
            ? request.AfterSequence
            : request.EntrySequence - 1;
        byte[]? response = null;
        _database.VisitOutboxBatch(afterSequence, 1, lease, (in OutboxEntryView entry) =>
        {
            if (request.EntrySequence != 0 && entry.OutboxSeq != request.EntrySequence)
                return false;
            response = FldDatabaseIpcProtocol.EncodeOutboxEntryChunkResponse(
                in entry, request.ValueOffset, request.MaxChunkBytes);
            return false;
        });
        return response ?? [0];
    }

    private byte[] HandleOutboxAcknowledgement(
        ReadOnlySpan<byte> clientId,
        ReadOnlyMemory<byte> payload)
    {
        (ulong leaseEpoch, ulong throughSequence) =
            FldDatabaseIpcProtocol.DecodeOutboxAcknowledgementPayload(payload);
        _database.AcknowledgeOutbox(
            throughSequence, new OutboxLease(leaseEpoch, clientId.ToArray()));
        return FldDatabaseIpcProtocol.EncodeOutboxBooleanResponse(true);
    }

    private byte[] HandleReleaseOutboxLease(
        ReadOnlySpan<byte> clientId,
        ReadOnlyMemory<byte> payload)
    {
        ulong leaseEpoch = FldDatabaseIpcProtocol.DecodeOutboxLeasePayload(payload);
        _database.ReleaseOutboxLease(new OutboxLease(leaseEpoch, clientId.ToArray()));
        return FldDatabaseIpcProtocol.EncodeOutboxBooleanResponse(true);
    }

    private byte[] HandleAppendLocalProviderBatch(
        ReadOnlySpan<byte> clientId,
        ReadOnlySpan<byte> operationId,
        StorageIpcOperationEnvelope operation)
    {
        (ulong sequence, IReadOnlyList<FldLocalProviderMutation> mutations) =
            FldDatabaseIpcLocalProviderProtocol.DecodeRequest(operation.Payload);
        byte[] fingerprint = FldDatabaseIpcProtocol.ComputeFingerprint(
            operation.OperationCode, operation.Payload.Span);
        var receipt = new FldLocalOperationReceipt
        {
            ClientId = clientId.ToArray(),
            OperationId = operationId.ToArray(),
            ClientSequence = sequence,
            Fingerprint = fingerprint,
            Result = FldDatabaseIpcProtocol.EncodeWriteResult(mutations.Count)
        };
        FldLocalOperationCommitResult committed =
            _database.ApplyLocalProviderOperation(mutations, receipt);
        return FldDatabaseIpcProtocol.EncodeCommitResult(committed);
    }

    private byte[] HandleApplyRemoteBatch(ReadOnlyMemory<byte> payload)
    {
        RemoteBatchCommit batch = FldDatabaseIpcRemoteApplyProtocol.DecodeRequest(payload);
        RemoteBatchReceipt receipt = _database.ApplyRemoteBatch(batch);
        return FldDatabaseIpcRemoteApplyProtocol.EncodeReceipt(receipt);
    }

    private byte[] HandleOpenSnapshot(Guid clientId, Guid operationId, ReadOnlyMemory<byte> payload)
    {
        if (!payload.IsEmpty)
            throw new InvalidDataException("FLD snapshot-open request payload must be empty.");
        if (operationId == Guid.Empty)
            throw new InvalidDataException("FLD snapshot-open operation id must be nonzero.");

        lock (_snapshotGate)
        {
            ThrowIfDisposed();
            var operationKey = (clientId, operationId);
            if (_snapshotOpenOperations.TryGetValue(operationKey, out Guid existingToken))
            {
                if (!_snapshotHandles.TryGetValue(existingToken, out IpcSnapshotHandle? existing) ||
                    existing.Released)
                    throw new InvalidOperationException(
                        "The snapshot associated with this open operation was already released; use a new operation id.");
                return FldDatabaseIpcProtocol.EncodeSnapshotOpenResponse(
                    _hostGeneration,
                    existing.Snapshot.Generation,
                    existing.Snapshot.SourceFeedWatermark,
                    existing.Snapshot.RecordCount);
            }

            if (_snapshotHandles.Count >= MaxActiveSnapshotHandles ||
                _snapshotHandles.Values.Count(handle => handle.ClientId == clientId) >=
                    MaxActiveSnapshotHandlesPerClient)
                throw new InvalidOperationException("The active FLD snapshot handle limit has been reached.");

            FldProviderSnapshot snapshot = _database.OpenProviderSnapshotForIpc(clientId, _hostGeneration);
            var handle = new IpcSnapshotHandle(clientId, operationId, snapshot);
            _snapshotHandles.Add(snapshot.Generation, handle);
            _snapshotOpenOperations.Add(operationKey, snapshot.Generation);
            return FldDatabaseIpcProtocol.EncodeSnapshotOpenResponse(
                _hostGeneration, snapshot.Generation, snapshot.SourceFeedWatermark, snapshot.RecordCount);
        }
    }

    private byte[] HandleReadSnapshotPage(Guid clientId, ReadOnlyMemory<byte> payload)
    {
        var request = FldDatabaseIpcProtocol.DecodeSnapshotPagePayload(payload);
        IpcSnapshotHandle handle = GetSnapshotHandle(clientId, request.HostGeneration, request.SnapshotToken);
        lock (handle.Gate)
        {
            if (handle.Released)
                throw new InvalidOperationException("FLD snapshot handle has been released.");
            FldProviderSnapshotMetadataPage page =
                handle.Snapshot.ReadMetadataPage(request.AfterIndex, request.Limit);
            return FldDatabaseIpcProtocol.EncodeSnapshotPageResponse(page);
        }
    }

    private byte[] HandleReadSnapshotValueChunk(Guid clientId, ReadOnlyMemory<byte> payload)
    {
        var request = FldDatabaseIpcProtocol.DecodeSnapshotValueChunkPayload(payload);
        IpcSnapshotHandle handle = GetSnapshotHandle(clientId, request.HostGeneration, request.SnapshotToken);
        lock (handle.Gate)
        {
            if (handle.Released)
                throw new InvalidOperationException("FLD snapshot handle has been released.");
            FldProviderSnapshotValueChunk? chunk = handle.Snapshot.ReadValueChunk(
                request.RecordId, request.FieldId, request.ValueOffset, request.MaxChunkBytes);
            return FldDatabaseIpcProtocol.EncodeSnapshotValueChunkResponse(chunk);
        }
    }

    private byte[] HandleReleaseSnapshot(Guid clientId, ReadOnlyMemory<byte> payload)
    {
        var request = FldDatabaseIpcProtocol.DecodeSnapshotReleasePayload(payload);
        if (request.HostGeneration != _hostGeneration)
            throw new InvalidDataException("FLD snapshot handle belongs to a different Database host generation.");

        IpcSnapshotHandle? handle;
        lock (_snapshotGate)
        {
            ThrowIfDisposed();
            if (!_snapshotHandles.TryGetValue(request.SnapshotToken, out handle))
                return FldDatabaseIpcProtocol.EncodeSnapshotReleaseResponse(true);
            if (handle.ClientId != clientId)
                throw new InvalidDataException("FLD snapshot handle is bound to another IPC client.");
            _snapshotHandles.Remove(request.SnapshotToken);
            _snapshotOpenOperations.Remove((clientId, handle.OpenOperationId));
        }

        lock (handle.Gate)
        {
            if (!handle.Released)
            {
                handle.Released = true;
                _database.ReleaseProviderSnapshot(handle.Snapshot.Generation);
            }
        }
        return FldDatabaseIpcProtocol.EncodeSnapshotReleaseResponse(true);
    }

    private IpcSnapshotHandle GetSnapshotHandle(Guid clientId, Guid hostGeneration, Guid snapshotToken)
    {
        if (hostGeneration != _hostGeneration)
            throw new InvalidDataException("FLD snapshot handle belongs to a different Database host generation.");
        lock (_snapshotGate)
        {
            ThrowIfDisposed();
            if (!_snapshotHandles.TryGetValue(snapshotToken, out IpcSnapshotHandle? handle))
                throw new InvalidDataException("FLD snapshot handle is unknown or has been released.");
            if (handle.ClientId != clientId)
                throw new InvalidDataException("FLD snapshot handle is bound to another IPC client.");
            return handle;
        }
    }

    private void ThrowIfDisposed()
    {
        if (_disposed)
            throw new ObjectDisposedException(nameof(FldDatabaseIpcDispatcher));
    }

    public void Dispose()
    {
        IpcSnapshotHandle[] handles;
        lock (_snapshotGate)
        {
            if (_disposed)
                return;
            _disposed = true;
            handles = _snapshotHandles.Values.ToArray();
            _snapshotHandles.Clear();
            _snapshotOpenOperations.Clear();
        }

        foreach (IpcSnapshotHandle handle in handles)
        {
            lock (handle.Gate)
            {
                if (handle.Released)
                    continue;
                handle.Released = true;
                _database.ReleaseProviderSnapshot(handle.Snapshot.Generation);
            }
        }
    }

}
