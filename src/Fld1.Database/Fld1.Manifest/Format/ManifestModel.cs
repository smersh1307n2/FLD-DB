namespace Fld1.Manifest.Format;

/// <summary>Certified watermark as HLC wall+logical (no author in manifest scale).</summary>
public readonly struct CertifiedWatermark
{
    public readonly ulong Wall;
    public readonly uint Logical;
    public CertifiedWatermark(ulong wall, uint logical) { Wall = wall; Logical = logical; }
}

public sealed class RunEntry
{
    public required ulong RunId { get; init; }
    public required ushort Level { get; init; }
    public required uint Ns { get; init; }
    public required byte[] MinRec { get; init; } // 16
    public required byte[] MaxRec { get; init; } // 16
    public required ulong SeqLo { get; init; }
    public required ulong SeqHi { get; init; }
}

public sealed class RetainEntry
{
    public required ulong RunId { get; init; }
    public required ulong RetainUntilEpoch { get; init; }
}

/// <summary>
/// Per-client result tracking. Spec PART VI.
/// FinalizedThroughSeq = N means all RequestSeq ≤ N are finally classified.
/// </summary>
public sealed class ResultClient
{
    public required byte[] ClientInstanceId { get; init; } // 16
    public required ulong SeqLo { get; init; }
    public required ulong SeqHi { get; init; }
    public required ulong FinalizedThroughSeq { get; init; }
    public required ulong FloorSeq { get; init; }
    /// <summary>Accepted RequestSeq bits relative to FloorSeq (bit i = FloorSeq+i).</summary>
    public required byte[] AcceptedBitmap { get; init; }
}

public sealed class ProvenanceIndexEntry
{
    public required byte[] ClientInstanceId { get; init; }
    public required ulong RequestSeq { get; init; }
    public required byte Outcome { get; init; } // COMMITTED/REJECTED/IDEMPOTENT/SKIPPED
    public required ulong CommitSeq { get; init; }
    public required ushort ErrorCode { get; init; }
}

/// <summary>Checkpointed local IPC idempotency receipt; keyed by ClientId + OperationId.</summary>
public sealed class LocalOperationReceiptEntry
{
    public required uint NamespaceId { get; init; }
    public required byte[] ClientId { get; init; } // 16
    public required byte[] OperationId { get; init; } // 16
    public required byte[] Fingerprint { get; init; } // 32
    public required ulong CommitSeq { get; init; }
    public required byte[] Result { get; init; }
}

public sealed class ManifestDocument
{
    public required ulong Generation { get; init; }
    public required ulong LastFlushedCommitSeq { get; init; }
    public required CertifiedWatermark CertifiedWatermark { get; init; }
    public required List<uint> Namespaces { get; init; }
    public required List<RunEntry> Runs { get; init; }
    public required List<RetainEntry> Retains { get; init; }
    public required List<ResultClient> ResultClients { get; init; }
    public required List<ProvenanceIndexEntry> ProvenanceIndex { get; init; }
    public List<LocalOperationReceiptEntry> LocalOperationReceipts { get; init; } = [];
    public required ulong CoordinatorLeaseEpoch { get; init; }
    public required byte[] CoordinatorSessionId { get; init; } // 16, zeros if none

    public static ManifestDocument Empty(ulong generation = 0) => new()
    {
        Generation = generation,
        LastFlushedCommitSeq = 0,
        CertifiedWatermark = new CertifiedWatermark(0, 0),
        Namespaces = [],
        Runs = [],
        Retains = [],
        ResultClients = [],
        ProvenanceIndex = [],
        LocalOperationReceipts = [],
        CoordinatorLeaseEpoch = 0,
        CoordinatorSessionId = new byte[16]
    };
}

public static class ManifestOutcome
{
    public const byte Committed = 0;
    public const byte Rejected = 1;
    public const byte Idempotent = 2;
    public const byte Skipped = 3;
}
