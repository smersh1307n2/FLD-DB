namespace Fld1.Engine.Db;

public sealed partial class FldDatabase
{
    /// <summary>
    /// Atomically appends provider drafts while assigning the next per-client receipt
    /// sequence under the same database commit lock. OperationId plus request fingerprint
    /// is the stable retry identity; no client-side sequence journal is required.
    /// </summary>
    public FldLocalOperationCommitResult ApplyLocalProviderOperationWithAutoSequence(
        IReadOnlyList<FldLocalProviderMutation> mutations,
        ReadOnlySpan<byte> clientId,
        ReadOnlySpan<byte> operationId,
        ReadOnlySpan<byte> fingerprint)
    {
        ArgumentNullException.ThrowIfNull(mutations);
        ValidateOperationIdentity(clientId, operationId, fingerprint);
        lock (_commitLock)
        {
            EnsureHealthy();
            string key = LocalOperationKey(clientId, operationId);
            if (_localOperationReceipts.TryGetValue(key, out var stored))
            {
                if (!stored.Fingerprint.AsSpan().SequenceEqual(fingerprint))
                    throw new InvalidDataException("ClientId + OperationId was reused with a different request fingerprint.");
                return new FldLocalOperationCommitResult
                {
                    CommitSeq = stored.CommitSeq,
                    ClientSequence = stored.ClientSequence,
                    Result = stored.Result.ToArray(),
                    IsDuplicate = true
                };
            }

            ulong sequence = 1;
            if (_localOperationClients.TryGetValue(LocalOperationClientKey(clientId), out LocalOperationClientState? state))
                sequence = checked(state.LastCommittedSequence + 1);

            var receipt = new FldLocalOperationReceipt
            {
                ClientId = clientId.ToArray(),
                OperationId = operationId.ToArray(),
                ClientSequence = sequence,
                Fingerprint = fingerprint.ToArray(),
                Result = FldDatabaseIpcProtocol.EncodeWriteResult(mutations.Count)
            };
            return ApplyLocalProviderOperation(mutations, receipt);
        }
    }
}
