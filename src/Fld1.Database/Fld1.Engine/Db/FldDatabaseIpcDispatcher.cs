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
    private readonly bool _enableTombstoneGc;
    private readonly object _snapshotGate = new();
    private readonly Dictionary<Guid, IpcSnapshotHandle> _snapshotHandles = new();
    private readonly Dictionary<(Guid ClientId, Guid OperationId), Guid> _snapshotOpenOperations = new();
    private readonly Guid _hostGeneration = Guid.NewGuid();
    private volatile bool _disposed;
    private readonly AsyncIpcGcBoundary _gcBoundary = new();
    private IDisposable? _gcBoundaryLease;
    private readonly object _gcLeaseGate = new();
    private readonly TimeSpan _gcLeaseIdleTimeout;
    private readonly Timer _gcLeaseTimer;
    private long _gcLeaseExpiresAtUtcTicks;
    private Guid _gcLeaseToken;
    private Guid _gcLeaseClientId;
    private ulong _gcLeaseSessionGeneration;
    private readonly Dictionary<Guid, GcReferenceScanHandle> _gcReferenceScans = new();
    private const int MaxActiveGcApprovalSpools = 4;
    private readonly Dictionary<Guid, GcApprovalSpoolHandle> _gcApprovalSpools = new();

    private sealed class GcApprovalSpoolHandle(
        Guid clientId, ulong sessionGeneration, FldTombstoneGcApprovalSpool spool)
    {
        public Guid ClientId { get; } = clientId;
        public ulong SessionGeneration { get; } = sessionGeneration;
        public FldTombstoneGcApprovalSpool Spool { get; } = spool;
        public int AppendCalls;
        public int AppendedCandidates;
        public long AppendElapsedTicks;
        public long AppendAllocatedBytes;
        public long SealElapsedTicks;
        public long SealAllocatedBytes;
    }

    private sealed record GcReferenceScanHandle(
        Guid ClientId, ulong SessionGeneration, FldReferenceSnapshot Snapshot,
        bool CommittedWalSealSucceeded);

    private sealed class IpcSnapshotHandle(
        Guid clientId, Guid openOperationId, FldProviderSnapshot snapshot, bool isTransient)
    {
        public Guid ClientId { get; } = clientId;
        public Guid OpenOperationId { get; } = openOperationId;
        public FldProviderSnapshot Snapshot { get; } = snapshot;
        public bool IsTransient { get; } = isTransient;
        public object Gate { get; } = new();
        public bool Released { get; set; }
        public int ReadPageCalls;
        public int ReadPageEntries;
        public long ReadPageElapsedTicks;
        public long ReadPageAllocatedBytes;
    }

    public FldDatabaseIpcDispatcher(
        FldDatabase database,
        bool enableTombstoneGc = false,
        TimeSpan? gcLeaseIdleTimeout = null)
    {
        _database = database ?? throw new ArgumentNullException(nameof(database));
        _enableTombstoneGc = enableTombstoneGc;
        string scratchDirectory = _database.GetTombstoneGcScratchDirectory();
        // The database root is process-owned. Any spool left by a previous Host
        // generation is disposable scratch, never a recovery input.
        foreach (string staleSpool in Directory.EnumerateFiles(
                     scratchDirectory, "*.tombstone-gc-approval.tmp"))
        {
            try { File.Delete(staleSpool); } catch { /* Retry on the next Host start. */ }
        }
        _gcLeaseIdleTimeout = gcLeaseIdleTimeout ?? TimeSpan.FromMinutes(5);
        if (_gcLeaseIdleTimeout <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(gcLeaseIdleTimeout));
        _gcLeaseTimer = new Timer(
            static state => ((FldDatabaseIpcDispatcher)state!).ExpireGcLeaseTimer(),
            this, Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);
    }

    /// <summary>Stable for this dispatcher lifetime; changes when the Database host restarts.</summary>
    public Guid HostGeneration => _hostGeneration;

    public async ValueTask<ReadOnlyMemory<byte>> HandleAsync(
        StorageIpcRequestContext context,
        ReadOnlyMemory<byte> payload,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        ThrowIfDisposed();
        if (context.ClientId == Guid.Empty || context.SessionGeneration == 0)
            throw new InvalidDataException("FLD IPC client/session identity must be nonzero.");

        StorageIpcOperationEnvelope operation = StorageIpcOperationProtocol.Decode(payload);
        if (operation.OperationCode == FldDatabaseIpcOperationCodes.AcquireGcLease)
        {
            if (!operation.Payload.IsEmpty)
                throw new InvalidDataException("FLD GC lease acquisition payload must be empty.");
            lock (_gcLeaseGate)
            {
                if (_gcLeaseToken != Guid.Empty && _gcLeaseClientId == context.ClientId &&
                    _gcLeaseSessionGeneration == context.SessionGeneration)
                    throw new InvalidOperationException("This IPC session already owns the FLD GC lease.");
            }
            IDisposable boundaryLease = await _gcBoundary.EnterWriteAsync(cancellationToken).ConfigureAwait(false);
            lock (_gcLeaseGate)
            {
                if (_gcLeaseToken != Guid.Empty)
                {
                    boundaryLease.Dispose();
                    throw new InvalidOperationException("A FLD GC lease is already active.");
                }
                _gcLeaseToken = Guid.NewGuid();
                _gcLeaseClientId = context.ClientId;
                _gcLeaseSessionGeneration = context.SessionGeneration;
                _gcBoundaryLease = boundaryLease;
                RenewGcLeaseLocked();
                return FldDatabaseIpcProtocol.EncodeGcLeaseToken(_gcLeaseToken);
            }
        }

        if (operation.OperationCode == FldDatabaseIpcOperationCodes.RenewGcLease)
        {
            Guid token = FldDatabaseIpcProtocol.DecodeGcLeaseToken(operation.Payload);
            lock (_gcLeaseGate)
            {
                EnsureGcLeaseOwner(context);
                if (_gcLeaseToken != token)
                    throw new InvalidDataException("FLD GC lease renewal token is unknown.");
                RenewGcLeaseLocked();
            }
            return ReadOnlyMemory<byte>.Empty;
        }

        if (operation.OperationCode == FldDatabaseIpcOperationCodes.ReleaseGcLease)
        {
            Guid token = FldDatabaseIpcProtocol.DecodeGcLeaseToken(operation.Payload);
            IDisposable? boundaryLease;
            lock (_gcLeaseGate)
            {
                // Releasing an already-released token is idempotent. This closes the
                // uncertain-response window without allowing a caller to release a
                // different live session's lease.
                if (_gcLeaseToken == Guid.Empty)
                    return ReadOnlyMemory<byte>.Empty;
                if (_gcLeaseToken != token || _gcLeaseClientId != context.ClientId ||
                    _gcLeaseSessionGeneration != context.SessionGeneration)
                    throw new InvalidDataException("FLD GC lease token is unknown or belongs to another IPC session.");
                foreach (Guid scanToken in _gcReferenceScans
                    .Where(pair => pair.Value.ClientId == context.ClientId &&
                        pair.Value.SessionGeneration == context.SessionGeneration)
                    .Select(pair => pair.Key).ToArray())
                {
                    _gcReferenceScans[scanToken].Snapshot.Dispose();
                    _gcReferenceScans.Remove(scanToken);
                }
                AbortApprovalSpoolsForSession(context.ClientId, context.SessionGeneration);
                _gcLeaseToken = Guid.Empty;
                _gcLeaseClientId = Guid.Empty;
                _gcLeaseSessionGeneration = 0;
                _gcLeaseExpiresAtUtcTicks = 0;
                _gcLeaseTimer.Change(Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);
                boundaryLease = _gcBoundaryLease;
                _gcBoundaryLease = null;
            }
            boundaryLease?.Dispose();
            return ReadOnlyMemory<byte>.Empty;
        }

        bool ownsGcLease;
        lock (_gcLeaseGate)
            ownsGcLease = _gcLeaseToken != Guid.Empty && _gcLeaseClientId == context.ClientId &&
                _gcLeaseSessionGeneration == context.SessionGeneration;
        if (ownsGcLease && operation.OperationCode is not (
            FldDatabaseIpcOperationCodes.OpenGcReferenceScan or
            FldDatabaseIpcOperationCodes.QueryGcReferenceScan or
            FldDatabaseIpcOperationCodes.QueryGcChangeIdBatch or
            FldDatabaseIpcOperationCodes.ReleaseGcReferenceScan or
            FldDatabaseIpcOperationCodes.RenewGcLease or
            FldDatabaseIpcOperationCodes.CommitTombstoneGcApprovalSpool or
            FldDatabaseIpcOperationCodes.AbortTombstoneGcApprovalSpool))
            throw new InvalidOperationException(
                "The FLD GC lease owner may only access its reference scan until the Host-wide lease is released.");
        IDisposable? dispatchLease = ownsGcLease
            ? null
            : await _gcBoundary.EnterReadAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            Span<byte> clientId = stackalloc byte[16];
            Span<byte> operationId = stackalloc byte[16];
            context.ClientId.TryWriteBytes(clientId, bigEndian: true, out _);
            operation.OperationId.TryWriteBytes(operationId, bigEndian: true, out _);
            byte[] response = operation.OperationCode switch
            {
                FldDatabaseIpcOperationCodes.WriteBatch => HandleWriteBatch(clientId, operationId, operation),
                FldDatabaseIpcOperationCodes.ReadField => HandleReadField(clientId, operationId, operation),
                FldDatabaseIpcOperationCodes.AcknowledgeReceipts => HandleAcknowledgement(clientId, operation.Payload),
                FldDatabaseIpcOperationCodes.AcquireOutboxLease => HandleAcquireOutboxLease(clientId, operation.Payload),
                FldDatabaseIpcOperationCodes.ReadOutboxEntryChunk => HandleReadOutboxEntryChunk(clientId, operation.Payload),
                FldDatabaseIpcOperationCodes.AcknowledgeOutbox => HandleOutboxAcknowledgement(clientId, operation.Payload),
                FldDatabaseIpcOperationCodes.ReleaseOutboxLease => HandleReleaseOutboxLease(clientId, operation.Payload),
                FldDatabaseIpcOperationCodes.OpenSnapshot => HandleOpenSnapshot(context.ClientId, operation.OperationId, operation.Payload),
                FldDatabaseIpcOperationCodes.ReadSnapshotPage => HandleReadSnapshotPage(context.ClientId, operation.Payload),
                FldDatabaseIpcOperationCodes.ReadSnapshotValueChunk => HandleReadSnapshotValueChunk(context.ClientId, operation.Payload),
                FldDatabaseIpcOperationCodes.ReleaseSnapshot => HandleReleaseSnapshot(context.ClientId, operation.Payload),
                FldDatabaseIpcOperationCodes.ApplyRemoteBatch => HandleApplyRemoteBatch(operation.Payload),
                FldDatabaseIpcOperationCodes.AppendLocalProviderBatch => HandleAppendLocalProviderBatch(clientId, operationId, operation),
                FldDatabaseIpcOperationCodes.AppendLocalProviderBatchAutoSequence => HandleAppendLocalProviderBatchAutoSequence(clientId, operationId, operation),
                FldDatabaseIpcOperationCodes.GetLocalAuthor => HandleGetLocalAuthor(operation.Payload),
                FldDatabaseIpcOperationCodes.CommitPublishedOrigins => HandleCommitPublishedOrigins(operation.Payload),
                FldDatabaseIpcOperationCodes.TryGetMetadataStoreOwnedPublicationCutover => HandleTryGetMetadataStoreOwnedPublicationCutover(operation.Payload),
                FldDatabaseIpcOperationCodes.EnableMetadataStoreOwnedPublication => HandleEnableMetadataStoreOwnedPublication(operation.Payload),
                FldDatabaseIpcOperationCodes.BeginTombstoneGcApprovalSpool => HandleBeginTombstoneGcApprovalSpool(context, operation.Payload),
                FldDatabaseIpcOperationCodes.AppendTombstoneGcApprovalSpool => HandleAppendTombstoneGcApprovalSpool(context, operation.Payload),
                FldDatabaseIpcOperationCodes.SealTombstoneGcApprovalSpool => HandleSealTombstoneGcApprovalSpool(context, operation.Payload),
                FldDatabaseIpcOperationCodes.CommitTombstoneGcApprovalSpool => HandleCommitTombstoneGcApprovalSpool(context, operation.Payload),
                FldDatabaseIpcOperationCodes.AbortTombstoneGcApprovalSpool => HandleAbortTombstoneGcApprovalSpool(context, operation.Payload),
                FldDatabaseIpcOperationCodes.OpenGcReferenceScan => HandleOpenGcReferenceScan(context, operation.Payload),
                FldDatabaseIpcOperationCodes.QueryGcReferenceScan => HandleQueryGcReferenceScan(context, operation.Payload),
                FldDatabaseIpcOperationCodes.QueryGcChangeIdBatch => HandleQueryGcChangeIdBatch(context, operation.Payload),
                FldDatabaseIpcOperationCodes.ReleaseGcReferenceScan => HandleReleaseGcReferenceScan(context, operation.Payload),
                FldDatabaseIpcOperationCodes.GetOutboxHighWatermark => HandleGetOutboxHighWatermark(operation.Payload),
                FldDatabaseIpcOperationCodes.ReadCurrentMetadata => HandleReadCurrentMetadata(operation.Payload),
                FldDatabaseIpcOperationCodes.ReadCurrentValueChunk => HandleReadCurrentValueChunk(operation.Payload),
                _ => throw new NotSupportedException("Unsupported FLD Database IPC operation code " + operation.OperationCode + ".")
            };
            return response;
        }
        finally
        {
            dispatchLease?.Dispose();
        }
    }

    public void OnClientDisconnected(Guid clientId, ulong sessionGeneration)
    {
        IDisposable? boundaryLease = null;
        lock (_gcLeaseGate)
        {
            foreach (Guid scanToken in _gcReferenceScans
                .Where(pair => pair.Value.ClientId == clientId &&
                    pair.Value.SessionGeneration == sessionGeneration)
                .Select(pair => pair.Key).ToArray())
            {
                _gcReferenceScans[scanToken].Snapshot.Dispose();
                _gcReferenceScans.Remove(scanToken);
            }
            AbortApprovalSpoolsForSession(clientId, sessionGeneration);
            if (_gcLeaseToken != Guid.Empty && _gcLeaseClientId == clientId &&
                _gcLeaseSessionGeneration == sessionGeneration)
            {
                _gcLeaseToken = Guid.Empty;
                _gcLeaseClientId = Guid.Empty;
                _gcLeaseSessionGeneration = 0;
                _gcLeaseExpiresAtUtcTicks = 0;
                _gcLeaseTimer.Change(Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);
                boundaryLease = _gcBoundaryLease;
                _gcBoundaryLease = null;
            }
        }
        boundaryLease?.Dispose();
    }

    private byte[] HandleOpenGcReferenceScan(
        StorageIpcRequestContext context, ReadOnlyMemory<byte> payload)
    {
        byte sealMode = payload.IsEmpty ? (byte)0 : payload.Length == 1 ? payload.Span[0] : byte.MaxValue;
        if (sealMode is not (0 or 1 or 2))
            throw new InvalidDataException("FLD GC reference-scan open payload is invalid.");
        bool sealCommittedWal = sealMode != 0;
        string sealDomain = sealMode switch { 1 => "tombstone", 2 => "receipt", _ => "none" };
        lock (_gcLeaseGate)
        {
            EnsureGcLeaseOwner(context);
            if (_gcReferenceScans.Count != 0)
                throw new InvalidOperationException("The Host GC lease already owns an active reference scan.");
            bool committedWalSealSucceeded = false;
            if (sealCommittedWal)
            {
                // Receipt and tombstone GC both require a sealed committed WAL prefix.
                // The caller selects the GC domain; this operation only seals the
                // committed prefix and opens a stable reference snapshot. It never
                // performs physical tombstone reclamation.
                using IDisposable commitLease = _database.AcquireCommitGuard();
                long walSealAllocatedBefore = GC.GetTotalAllocatedBytes(precise: true);
                long walSealStarted = System.Diagnostics.Stopwatch.GetTimestamp();
                WriteSnapshotGcDiagnostic($"{sealDomain}-wal-seal-before", Guid.Empty);
                committedWalSealSucceeded = _database.TrySealCommittedWalForGc();
                WriteSnapshotGcDiagnostic($"{sealDomain}-wal-seal-after", Guid.Empty,
                    $"ElapsedMs={System.Diagnostics.Stopwatch.GetElapsedTime(walSealStarted).TotalMilliseconds:F3} " +
                    $"AllocatedBytes={GC.GetTotalAllocatedBytes(precise: true) - walSealAllocatedBefore} " +
                    $"Succeeded={committedWalSealSucceeded}");
            }
            Guid token = Guid.NewGuid();
            long referenceAllocatedBefore = GC.GetTotalAllocatedBytes(precise: true);
            long referenceStarted = System.Diagnostics.Stopwatch.GetTimestamp();
            WriteSnapshotGcDiagnostic("reference-snapshot-open-before", token);
            FldReferenceSnapshot snapshot = _database.OpenReferenceSnapshot();
            long referenceAllocatedBytes = GC.GetTotalAllocatedBytes(precise: true) - referenceAllocatedBefore;
            WriteSnapshotGcDiagnostic("reference-snapshot-open-after", token,
                $"ChangeIds={snapshot.ChangeIdCount} RecordIds={snapshot.RecordIdCount} " +
                $"ElapsedMs={System.Diagnostics.Stopwatch.GetElapsedTime(referenceStarted).TotalMilliseconds:F3} " +
                $"AllocatedBytes={referenceAllocatedBytes}");
            _gcReferenceScans.Add(token,
                new GcReferenceScanHandle(context.ClientId, context.SessionGeneration,
                    snapshot, committedWalSealSucceeded));
            return FldDatabaseIpcGcProtocol.EncodeScanResponse(
                new FldDatabaseIpcGcReferenceScan(
                    token, snapshot.UnpublishedLocalDraftHorizon,
                    snapshot.HasSnapshotPin || (sealCommittedWal && !committedWalSealSucceeded),
                    checked((ushort)_database.NamespaceId)));
        }
    }

    private byte[] HandleQueryGcReferenceScan(
        StorageIpcRequestContext context, ReadOnlyMemory<byte> payload)
    {
        FldDatabaseIpcGcReferenceQuery query = FldDatabaseIpcGcProtocol.DecodeQuery(payload);
        lock (_gcLeaseGate)
        {
            EnsureGcLeaseOwner(context);
            if (!_gcReferenceScans.TryGetValue(query.Token, out GcReferenceScanHandle? handle) ||
                handle.ClientId != context.ClientId ||
                handle.SessionGeneration != context.SessionGeneration)
                throw new InvalidDataException("FLD GC reference-scan token is unknown or belongs to another IPC session.");

            bool referenced = query.Kind switch
            {
                FldDatabaseIpcGcReferenceKind.ChangeId =>
                    handle.Snapshot.HasChangeIdReference(query.FirstId),
                FldDatabaseIpcGcReferenceKind.Record =>
                    handle.Snapshot.HasRecordReference(query.FirstId, query.SecondId!),
                FldDatabaseIpcGcReferenceKind.Receipt =>
                    handle.Snapshot.HasReceiptReference(
                        query.FirstId, query.AuthorEpoch, query.TransportPeerEpoch,
                        checked((ushort)_database.NamespaceId), query.SecondId!),
                _ => throw new InvalidDataException("FLD GC reference query kind is invalid.")
            };
            return FldDatabaseIpcGcProtocol.EncodeBooleanResponse(referenced);
        }
    }


    private byte[] HandleQueryGcChangeIdBatch(
        StorageIpcRequestContext context, ReadOnlyMemory<byte> payload)
    {
        (Guid token, ReadOnlyMemory<byte> packedChangeIds, int count) =
            FldDatabaseIpcGcProtocol.DecodeChangeIdBatchQueryView(payload);
        var referenced = new bool[count];
        lock (_gcLeaseGate)
        {
            EnsureGcLeaseOwner(context);
            if (!_gcReferenceScans.TryGetValue(token, out GcReferenceScanHandle? handle) ||
                handle.ClientId != context.ClientId ||
                handle.SessionGeneration != context.SessionGeneration)
                throw new InvalidDataException("FLD GC reference-scan token is unknown or belongs to another IPC session.");
            ReadOnlySpan<byte> ids = packedChangeIds.Span;
            for (int i = 0; i < count; i++)
                referenced[i] = handle.Snapshot.HasChangeIdReference(ids.Slice(i * 16, 16));
        }
        return FldDatabaseIpcGcProtocol.EncodeChangeIdBatchResponse(referenced);
    }

    private byte[] HandleReleaseGcReferenceScan(
        StorageIpcRequestContext context, ReadOnlyMemory<byte> payload)
    {
        Guid token = FldDatabaseIpcGcProtocol.DecodeToken(payload);
        lock (_gcLeaseGate)
        {
            EnsureGcLeaseOwner(context);
            if (!_gcReferenceScans.TryGetValue(token, out GcReferenceScanHandle? handle))
                return Array.Empty<byte>(); // Idempotent retry after an uncertain release response.
            if (handle.ClientId != context.ClientId ||
                handle.SessionGeneration != context.SessionGeneration)
                throw new InvalidDataException("FLD GC reference-scan token belongs to another IPC session.");
            _gcReferenceScans.Remove(token);
            handle.Snapshot.Dispose();
        }
        return Array.Empty<byte>();
    }

    private void EnsureGcLeaseOwner(StorageIpcRequestContext context)
    {
        if (_gcLeaseToken == Guid.Empty || _gcLeaseClientId != context.ClientId ||
            _gcLeaseSessionGeneration != context.SessionGeneration)
            throw new InvalidOperationException("FLD GC reference scans require the active Host-wide GC lease.");
        if (_gcLeaseExpiresAtUtcTicks <= DateTime.UtcNow.Ticks)
        {
            ClearGcLeaseLocked();
            throw new InvalidOperationException("The FLD Host GC lease expired while idle.");
        }
        RenewGcLeaseLocked();
    }

    private void RenewGcLeaseLocked()
    {
        _gcLeaseExpiresAtUtcTicks = DateTime.UtcNow.Add(_gcLeaseIdleTimeout).Ticks;
        _gcLeaseTimer.Change(_gcLeaseIdleTimeout, Timeout.InfiniteTimeSpan);
    }

    private void ExpireGcLeaseTimer()
    {
        lock (_gcLeaseGate)
        {
            if (_gcLeaseToken == Guid.Empty)
                return;
            long remainingTicks = _gcLeaseExpiresAtUtcTicks - DateTime.UtcNow.Ticks;
            if (remainingTicks > 0)
            {
                _gcLeaseTimer.Change(TimeSpan.FromTicks(remainingTicks), Timeout.InfiniteTimeSpan);
                return;
            }
            ClearGcLeaseLocked();
        }
    }

    // Caller holds _gcLeaseGate. Expiry is also the final safety net if an idle
    // client keeps its pipe open without issuing another scan operation.
    private void ClearGcLeaseLocked()
    {
        AbortApprovalSpoolsForSession(_gcLeaseClientId, _gcLeaseSessionGeneration);
        foreach (GcReferenceScanHandle scan in _gcReferenceScans.Values)
            scan.Snapshot.Dispose();
        _gcReferenceScans.Clear();
        _gcLeaseToken = Guid.Empty;
        _gcLeaseClientId = Guid.Empty;
        _gcLeaseSessionGeneration = 0;
        _gcLeaseExpiresAtUtcTicks = 0;
        _gcLeaseTimer.Change(Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);
        IDisposable? boundaryLease = _gcBoundaryLease;
        _gcBoundaryLease = null;
        boundaryLease?.Dispose();
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
    private byte[] HandleTryGetMetadataStoreOwnedPublicationCutover(ReadOnlyMemory<byte> payload)
    {
        if (!payload.IsEmpty)
            throw new InvalidDataException("FLD publication cutover query must have an empty payload.");
        bool exists = _database.TryGetMetadataStoreOwnedPublicationCutover(out ulong watermark);
        return FldDatabaseIpcPublicationModeProtocol.EncodeTryGetResult(exists, watermark);
    }

    private byte[] HandleEnableMetadataStoreOwnedPublication(ReadOnlyMemory<byte> payload)
    {
        ulong watermark = FldDatabaseIpcPublicationModeProtocol.DecodeEnableRequest(payload);
        _database.EnableMetadataStoreOwnedPublication(watermark);
        return Array.Empty<byte>();
    }

    private byte[] HandleCommitPublishedOrigins(ReadOnlyMemory<byte> payload)
    {
        IReadOnlyList<FldPublishedOrigin> origins =
            FldDatabaseIpcPublishedOriginProtocol.DecodeRequest(payload);
        int committed = _database.CommitPublishedOrigins(origins);
        return FldDatabaseIpcPublishedOriginProtocol.EncodeResult(committed);
    }

    private byte[] HandleBeginTombstoneGcApprovalSpool(
        StorageIpcRequestContext context, ReadOnlyMemory<byte> payload)
    {
        if (!_enableTombstoneGc)
            throw new NotSupportedException("Physical tombstone GC is disabled by FLD Database Host configuration.");
        if (!payload.IsEmpty)
            throw new InvalidDataException("Tombstone-GC approval-spool begin payload must be empty.");

        lock (_gcLeaseGate)
        {
            if (_gcApprovalSpools.Count >= MaxActiveGcApprovalSpools)
                throw new InvalidOperationException("The Host has reached its active tombstone-GC approval-spool limit.");
            if (_gcApprovalSpools.Values.Any(item =>
                item.ClientId == context.ClientId && item.SessionGeneration == context.SessionGeneration))
                throw new InvalidOperationException("This IPC session already owns a tombstone-GC approval spool.");
            Guid token = Guid.NewGuid();
            var spool = new FldTombstoneGcApprovalSpool(
                _database.GetTombstoneGcScratchDirectory(), token);
            _gcApprovalSpools.Add(token,
                new GcApprovalSpoolHandle(context.ClientId, context.SessionGeneration, spool));
            return FldDatabaseIpcProtocol.EncodeGcLeaseToken(token);
        }
    }

    private byte[] HandleAppendTombstoneGcApprovalSpool(
        StorageIpcRequestContext context, ReadOnlyMemory<byte> payload)
    {
        long allocatedBefore = GC.GetTotalAllocatedBytes(precise: true);
        long started = System.Diagnostics.Stopwatch.GetTimestamp();
        (Guid token, ReadOnlyMemory<byte> packedCandidates, int count) =
            FldDatabaseIpcProtocol.DecodeTombstoneGcApprovalSpoolAppendView(payload);
        lock (_gcLeaseGate)
        {
            GcApprovalSpoolHandle handle = GetApprovalSpool(context, token);
            handle.Spool.AppendPacked(packedCandidates.Span, count);
            handle.AppendCalls++;
            handle.AppendedCandidates = checked(handle.AppendedCandidates + count);
            handle.AppendElapsedTicks += System.Diagnostics.Stopwatch.GetTimestamp() - started;
            handle.AppendAllocatedBytes += GC.GetTotalAllocatedBytes(precise: true) - allocatedBefore;
        }
        return Array.Empty<byte>();
    }

    private byte[] HandleSealTombstoneGcApprovalSpool(
        StorageIpcRequestContext context, ReadOnlyMemory<byte> payload)
    {
        Guid token = FldDatabaseIpcProtocol.DecodeGcLeaseToken(payload);
        lock (_gcLeaseGate)
        {
            GcApprovalSpoolHandle handle = GetApprovalSpool(context, token);
            long allocatedBefore = GC.GetTotalAllocatedBytes(precise: true);
            long started = System.Diagnostics.Stopwatch.GetTimestamp();
            byte[] digest = handle.Spool.Seal();
            byte[] response = FldDatabaseIpcProtocol.EncodeTombstoneGcApprovalSpoolSealResponse(
                handle.Spool.Count, digest);
            handle.SealElapsedTicks += System.Diagnostics.Stopwatch.GetTimestamp() - started;
            handle.SealAllocatedBytes += GC.GetTotalAllocatedBytes(precise: true) - allocatedBefore;
            WriteSnapshotGcDiagnostic("spool-summary", token,
                $"Count={handle.Spool.Count} AppendCalls={handle.AppendCalls} " +
                $"AppendedCandidates={handle.AppendedCandidates} " +
                $"AppendElapsedMs={System.Diagnostics.Stopwatch.GetElapsedTime(0, handle.AppendElapsedTicks).TotalMilliseconds:F3} " +
                $"AppendAllocatedBytes={handle.AppendAllocatedBytes} " +
                $"SealElapsedMs={System.Diagnostics.Stopwatch.GetElapsedTime(0, handle.SealElapsedTicks).TotalMilliseconds:F3} " +
                $"SealAllocatedBytes={handle.SealAllocatedBytes}");
            return response;
        }
    }

    private byte[] HandleCommitTombstoneGcApprovalSpool(
        StorageIpcRequestContext context, ReadOnlyMemory<byte> payload)
    {
        Guid token = FldDatabaseIpcProtocol.DecodeGcLeaseToken(payload);
        lock (_gcLeaseGate)
        {
            EnsureGcLeaseOwner(context);
            GcApprovalSpoolHandle handle = GetApprovalSpool(context, token);
            try
            {
                if (!handle.Spool.IsSealed)
                    throw new InvalidOperationException("Tombstone-GC approval spool must be sealed before commit.");
                GcReferenceScanHandle? referenceScan = _gcReferenceScans.Values.FirstOrDefault(scan =>
                    scan.ClientId == context.ClientId &&
                    scan.SessionGeneration == context.SessionGeneration);
                if (referenceScan is null)
                    throw new InvalidOperationException("Tombstone-GC spool commit requires an active Host reference scan.");
                if (!_enableTombstoneGc)
                    throw new NotSupportedException("Physical tombstone GC is disabled by FLD Database Host configuration.");

                // Reuse the fresh reference snapshot opened after the Host-wide lease
                // was acquired. Opening a second reference snapshot here would make
                // the first scan look like an external pin and veto every commit.
                FldReferenceSnapshot references = referenceScan.Snapshot;
                if (!referenceScan.CommittedWalSealSucceeded)
                    return FldDatabaseIpcProtocol.EncodeTombstoneGcApprovalSpoolCommitResponse(0);
                WriteSnapshotGcDiagnostic("tombstone-commit-before-guard", token);
                using IDisposable commitLease = _database.AcquireCommitGuard();
                WriteSnapshotGcDiagnostic("tombstone-commit-after-guard", token);
                FldDraftHorizon draftHorizon = references.UnpublishedLocalDraftHorizon;
                if (references.HasSnapshotPin || draftHorizon.Count != 0 ||
                    draftHorizon.MaxHlcWall != 0 || draftHorizon.MaxHlcLogical != 0)
                    return FldDatabaseIpcProtocol.EncodeTombstoneGcApprovalSpoolCommitResponse(0);

                WriteSnapshotGcDiagnostic("tombstone-commit-before-spool-reader", token);
                using FldTombstoneGcApprovalSpool.Reader approved = handle.Spool.OpenReader();
                WriteSnapshotGcDiagnostic("tombstone-commit-after-spool-reader", token);
                long approvalTicks = 0;
                long changeIdTicks = 0;
                long recordIdTicks = 0;
                int predicateCalls = 0;
                bool CanReclaim(FldTombstoneGcCandidate candidate)
                {
                    predicateCalls++;
                    long started = System.Diagnostics.Stopwatch.GetTimestamp();
                    bool isApproved = approved.IsApproved(candidate);
                    approvalTicks += System.Diagnostics.Stopwatch.GetTimestamp() - started;
                    if (!isApproved)
                        return false;

                    started = System.Diagnostics.Stopwatch.GetTimestamp();
                    bool hasChangeReference = references.HasChangeIdReference(candidate.ChangeId.Span);
                    changeIdTicks += System.Diagnostics.Stopwatch.GetTimestamp() - started;
                    if (hasChangeReference)
                        return false;

                    started = System.Diagnostics.Stopwatch.GetTimestamp();
                    bool hasRecordReference = references.HasRecordReference(
                        candidate.RecordId.Span, candidate.ChangeId.Span);
                    recordIdTicks += System.Diagnostics.Stopwatch.GetTimestamp() - started;
                    return !hasRecordReference;
                }

                WriteSnapshotGcDiagnostic("tombstone-commit-before-compact", token);
                long allocatedBefore = GC.GetTotalAllocatedBytes(precise: true);
                long commitStarted = System.Diagnostics.Stopwatch.GetTimestamp();
                int reclaimed = _database.CompactTombstones(CanReclaim);
                long commitTicks = System.Diagnostics.Stopwatch.GetTimestamp() - commitStarted;
                WriteSnapshotGcDiagnostic("tombstone-commit-after-compact", token);
                long commitAllocatedBytes = GC.GetTotalAllocatedBytes(precise: true) - allocatedBefore;
                WriteSnapshotGcDiagnostic("tombstone-commit-summary", token,
                    $"Candidates={handle.Spool.Count} PredicateCalls={predicateCalls} Reclaimed={reclaimed} " +
                    $"CommitElapsedMs={System.Diagnostics.Stopwatch.GetElapsedTime(0, commitTicks).TotalMilliseconds:F3} " +
                    $"CommitAllocatedBytes={commitAllocatedBytes} " +
                    $"ApprovalElapsedMs={System.Diagnostics.Stopwatch.GetElapsedTime(0, approvalTicks).TotalMilliseconds:F3} " +
                    $"ChangeIdCheckElapsedMs={System.Diagnostics.Stopwatch.GetElapsedTime(0, changeIdTicks).TotalMilliseconds:F3} " +
                    $"RecordIdCheckElapsedMs={System.Diagnostics.Stopwatch.GetElapsedTime(0, recordIdTicks).TotalMilliseconds:F3}");
                return FldDatabaseIpcProtocol.EncodeTombstoneGcApprovalSpoolCommitResponse(reclaimed);
            }
            finally
            {
                if (_gcApprovalSpools.Remove(token, out GcApprovalSpoolHandle? current))
                    current.Spool.Dispose();
            }
        }
    }

    private byte[] HandleAbortTombstoneGcApprovalSpool(
        StorageIpcRequestContext context, ReadOnlyMemory<byte> payload)
    {
        Guid token = FldDatabaseIpcProtocol.DecodeGcLeaseToken(payload);
        lock (_gcLeaseGate)
        {
            if (!_gcApprovalSpools.TryGetValue(token, out GcApprovalSpoolHandle? handle))
                return Array.Empty<byte>();
            if (handle.ClientId != context.ClientId ||
                handle.SessionGeneration != context.SessionGeneration)
                throw new InvalidDataException("Tombstone-GC approval spool belongs to another IPC session.");
            _gcApprovalSpools.Remove(token);
            handle.Spool.Dispose();
        }
        return Array.Empty<byte>();
    }

    private GcApprovalSpoolHandle GetApprovalSpool(StorageIpcRequestContext context, Guid token)
    {
        if (!_gcApprovalSpools.TryGetValue(token, out GcApprovalSpoolHandle? handle) ||
            handle.ClientId != context.ClientId ||
            handle.SessionGeneration != context.SessionGeneration)
            throw new InvalidDataException("Tombstone-GC approval spool token is unknown or belongs to another IPC session.");
        return handle;
    }

    // Caller holds _gcLeaseGate.
    private void AbortApprovalSpoolsForSession(Guid clientId, ulong sessionGeneration)
    {
        if (clientId == Guid.Empty || sessionGeneration == 0)
            return;
        foreach (Guid token in _gcApprovalSpools
            .Where(pair => pair.Value.ClientId == clientId &&
                pair.Value.SessionGeneration == sessionGeneration)
            .Select(pair => pair.Key).ToArray())
        {
            GcApprovalSpoolHandle handle = _gcApprovalSpools[token];
            _gcApprovalSpools.Remove(token);
            handle.Spool.Dispose();
        }
    }

    private byte[] HandleGetLocalAuthor(ReadOnlyMemory<byte> payload)
    {
        if (!payload.IsEmpty)
            throw new InvalidDataException("FLD LocalAuthor request payload must be empty.");
        return FldDatabaseIpcProtocol.EncodeLocalAuthorResponse(_database.LocalAuthor);
    }

    private byte[] HandleReadCurrentMetadata(ReadOnlyMemory<byte> payload)
    {
        var key=FldDatabaseIpcProtocol.DecodeReadCurrentMetadataPayload(payload);
        FldDatabaseIpcCurrentMetadata? result=null;
        if(_database.TryReadProviderCurrentMetadata(key.RecordId,key.FieldId,out FldProviderCurrentMetadata? m)&&m is not null)
            result=new(m.ValueLength,m.HlcWall,m.HlcLogical,m.AuthorNodeId,m.KeyEpoch,m.IsTombstone,m.ChangeId,m.OriginAuthorEpoch,m.OriginSequence);
        return FldDatabaseIpcProtocol.EncodeCurrentMetadataResponse(result);
    }
    private byte[] HandleReadCurrentValueChunk(ReadOnlyMemory<byte> payload)
    {
        var q=FldDatabaseIpcProtocol.DecodeReadCurrentValueChunkPayload(payload);
        var c=_database.ReadProviderCurrentValueChunk(q.RecordId,q.FieldId,q.ExpectedChangeId,q.ValueOffset,q.MaxChunkBytes);
        return FldDatabaseIpcProtocol.EncodeCurrentValueChunkResponse(new((FldDatabaseIpcCurrentReadStatus)c.Status,c.ValueLength,c.ValueOffset,c.ValueChunk));
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

    private byte[] HandleAppendLocalProviderBatchAutoSequence(
        ReadOnlySpan<byte> clientId,
        ReadOnlySpan<byte> operationId,
        StorageIpcOperationEnvelope operation)
    {
        IReadOnlyList<FldLocalProviderMutation> mutations =
            FldDatabaseIpcLocalProviderProtocol.DecodeAutoSequenceRequest(operation.Payload);
        byte[] fingerprint = FldDatabaseIpcProtocol.ComputeFingerprint(
            operation.OperationCode, operation.Payload.Span);
        FldLocalOperationCommitResult committed =
            _database.ApplyLocalProviderOperationWithAutoSequence(
                mutations, clientId, operationId, fingerprint);
        return FldDatabaseIpcProtocol.EncodeAutoSequenceCommitResult(committed);
    }

    private byte[] HandleApplyRemoteBatch(ReadOnlyMemory<byte> payload)
    {
        RemoteBatchCommit batch = FldDatabaseIpcRemoteApplyProtocol.DecodeRequest(payload);
        RemoteBatchReceipt receipt = _database.ApplyRemoteBatch(batch);
        return FldDatabaseIpcRemoteApplyProtocol.EncodeReceipt(receipt);
    }

    private byte[] HandleOpenSnapshot(Guid clientId, Guid operationId, ReadOnlyMemory<byte> payload)
    {
        bool transient = FldDatabaseIpcProtocol.DecodeSnapshotOpenPayload(payload);
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
                if (existing.IsTransient != transient)
                    throw new InvalidOperationException(
                        "A snapshot-open operation id cannot be reused with a different durability mode.");
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

            WriteSnapshotGcDiagnostic("open-before", operationId);
            FldProviderSnapshot snapshot = transient
                ? _database.OpenTransientProviderSnapshotForIpc(clientId, _hostGeneration)
                : _database.OpenProviderSnapshotForIpc(clientId, _hostGeneration);
            var handle = new IpcSnapshotHandle(clientId, operationId, snapshot, transient);
            _snapshotHandles.Add(snapshot.Generation, handle);
            _snapshotOpenOperations.Add(operationKey, snapshot.Generation);
            int recordCount = snapshot.RecordCount;
            WriteSnapshotGcDiagnostic("open-after", operationId);
            return FldDatabaseIpcProtocol.EncodeSnapshotOpenResponse(
                _hostGeneration, snapshot.Generation, snapshot.SourceFeedWatermark, recordCount);
        }
    }

    private static void WriteSnapshotGcDiagnostic(
        string stage, Guid operationId, string? detail = null)
    {
        string? path = Environment.GetEnvironmentVariable("FLD_TEST_GC_DIAGNOSTICS_FILE");
        if (string.IsNullOrWhiteSpace(path))
            return;

        try
        {
            string line = $"FLD_HOST_GC_DIAG Stage={stage} Operation={operationId:N} " +
                $"AllocatedBytes={GC.GetTotalAllocatedBytes(precise: true)} " +
                $"ManagedHeapBytes={GC.GetTotalMemory(forceFullCollection: false)}" +
                (string.IsNullOrWhiteSpace(detail) ? string.Empty : " " + detail);
            File.AppendAllText(path, line + Environment.NewLine);
        }
        catch (IOException) { }
        catch (UnauthorizedAccessException) { }
    }

    private byte[] HandleReadSnapshotPage(Guid clientId, ReadOnlyMemory<byte> payload)
    {
        var request = FldDatabaseIpcProtocol.DecodeSnapshotPagePayload(payload);
        IpcSnapshotHandle handle = GetSnapshotHandle(clientId, request.HostGeneration, request.SnapshotToken);
        lock (handle.Gate)
        {
            if (handle.Released)
                throw new InvalidOperationException("FLD snapshot handle has been released.");
            long allocatedBefore = GC.GetTotalAllocatedBytes(precise: true);
            long started = System.Diagnostics.Stopwatch.GetTimestamp();
            byte[] response =
                handle.Snapshot.ReadMetadataPagePayload(request.AfterIndex, request.Limit);
            int count = response[5] | (response[6] << 8);
            handle.ReadPageCalls++;
            handle.ReadPageEntries = checked(handle.ReadPageEntries + count);
            handle.ReadPageElapsedTicks += System.Diagnostics.Stopwatch.GetTimestamp() - started;
            handle.ReadPageAllocatedBytes += GC.GetTotalAllocatedBytes(precise: true) - allocatedBefore;
            return response;
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
                WriteSnapshotGcDiagnostic("snapshot-page-summary", handle.OpenOperationId,
                    $"Calls={handle.ReadPageCalls} Entries={handle.ReadPageEntries} " +
                    $"ElapsedMs={System.Diagnostics.Stopwatch.GetElapsedTime(0, handle.ReadPageElapsedTicks).TotalMilliseconds:F3} " +
                    $"AllocatedBytes={handle.ReadPageAllocatedBytes}");
                WriteSnapshotGcDiagnostic("release-before", handle.OpenOperationId);
                handle.Released = true;
                _database.ReleaseProviderSnapshot(handle.Snapshot.Generation);
                WriteSnapshotGcDiagnostic("release-after", handle.OpenOperationId);
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

    private sealed class AsyncIpcGcBoundary
    {
        private readonly SemaphoreSlim _writerQueue = new(1, 1);
        private readonly object _gate = new();
        private int _readers;
        private bool _writerActive;
        private bool _writerPending;
        private TaskCompletionSource _changed = NewSignal();

        public async ValueTask<IDisposable> EnterReadAsync(CancellationToken cancellationToken)
        {
            while (true)
            {
                Task wait;
                lock (_gate)
                {
                    if (!_writerActive && !_writerPending)
                    {
                        _readers++;
                        return new BoundaryLease(this, writer: false);
                    }
                    wait = _changed.Task;
                }
                await wait.WaitAsync(cancellationToken).ConfigureAwait(false);
            }
        }

        public async ValueTask<IDisposable> EnterWriteAsync(CancellationToken cancellationToken)
        {
            await _writerQueue.WaitAsync(cancellationToken).ConfigureAwait(false);
            lock (_gate)
            {
                _writerPending = true;
                Pulse();
            }
            try
            {
                while (true)
                {
                    Task wait;
                    lock (_gate)
                    {
                        if (!_writerActive && _readers == 0)
                        {
                            _writerPending = false;
                            _writerActive = true;
                            Pulse();
                            return new BoundaryLease(this, writer: true);
                        }
                        wait = _changed.Task;
                    }
                    await wait.WaitAsync(cancellationToken).ConfigureAwait(false);
                }
            }
            catch
            {
                lock (_gate)
                {
                    _writerPending = false;
                    Pulse();
                }
                _writerQueue.Release();
                throw;
            }
        }

        private void Release(bool writer)
        {
            lock (_gate)
            {
                if (writer)
                {
                    if (!_writerActive)
                        throw new SynchronizationLockException("FLD IPC GC write boundary is not held.");
                    _writerActive = false;
                }
                else
                {
                    if (_readers <= 0)
                        throw new SynchronizationLockException("FLD IPC GC read boundary is not held.");
                    _readers--;
                }
                Pulse();
            }
            if (writer)
                _writerQueue.Release();
        }

        private void Pulse()
        {
            TaskCompletionSource previous = _changed;
            _changed = NewSignal();
            previous.TrySetResult();
        }

        private static TaskCompletionSource NewSignal()
            => new(TaskCreationOptions.RunContinuationsAsynchronously);

        private sealed class BoundaryLease : IDisposable
        {
            private AsyncIpcGcBoundary? _owner;
            private readonly bool _writer;

            public BoundaryLease(AsyncIpcGcBoundary owner, bool writer)
            {
                _owner = owner;
                _writer = writer;
            }

            public void Dispose()
                => Interlocked.Exchange(ref _owner, null)?.Release(_writer);
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

        IDisposable? boundaryLease;
        lock (_gcLeaseGate)
        {
            foreach (GcApprovalSpoolHandle spool in _gcApprovalSpools.Values)
                spool.Spool.Dispose();
            _gcApprovalSpools.Clear();
            foreach (GcReferenceScanHandle scan in _gcReferenceScans.Values)
                scan.Snapshot.Dispose();
            _gcReferenceScans.Clear();
            _gcLeaseToken = Guid.Empty;
            _gcLeaseClientId = Guid.Empty;
            _gcLeaseSessionGeneration = 0;
            _gcLeaseExpiresAtUtcTicks = 0;
            _gcLeaseTimer.Change(Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);
            boundaryLease = _gcBoundaryLease;
            _gcBoundaryLease = null;
        }
        boundaryLease?.Dispose();
        _gcLeaseTimer.Dispose();
    }

}
