namespace Fld1.Engine.Db;

/// <summary>
/// Stable identity and opaque result for one local Database IPC operation.
/// ClientId and OperationId are independent of any transport session/request ID.
/// </summary>
public sealed class FldLocalOperationReceipt
{
    public required byte[] ClientId { get; init; }
    public required byte[] OperationId { get; init; }
    /// <summary>Strictly contiguous, durable per-client sequence, reused unchanged on retry.</summary>
    public required ulong ClientSequence { get; init; }
    public required byte[] Fingerprint { get; init; }
    public required byte[] Result { get; init; }
}

/// <summary>Outcome of committing or replaying a local idempotent Database operation.</summary>
public sealed class FldLocalOperationCommitResult
{
    public required ulong CommitSeq { get; init; }
    /// <summary>Durable per-client receipt sequence; zero when the operation has no receipt.</summary>
    public ulong ClientSequence { get; init; }
    public required byte[] Result { get; init; }
    public required bool IsDuplicate { get; init; }
}
