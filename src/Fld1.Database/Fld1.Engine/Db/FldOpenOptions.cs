namespace Fld1.Engine.Db;

/// <summary>Options for <see cref="FldDatabase.Open(FldOpenOptions)"/>.</summary>
public sealed class FldOpenOptions
{
    public required string Path { get; init; }
    public uint NamespaceId { get; init; }
    /// <summary>
    /// Durable database author identity. If omitted on a new database, FLD generates
    /// and persists one. In BIN-SYNC external-storage mode this must equal NodeId.
    /// </summary>
    public byte[]? LocalAuthor { get; init; }

    /// <summary>Auto Checkpoint when overlay field count ≥ N (0 = off).</summary>
    public int AutoCheckpointOverlayFields { get; init; }

    /// <summary>Rotate WAL segment after this many bytes (default 64 MiB). 0 = never auto-rotate.</summary>
    public long WalRotateAfterBytes { get; init; } = 64L * 1024 * 1024;

    /// <summary>After Checkpoint/Compact, delete WAL segments fully covered by sealed seq.</summary>
    public bool TruncateSealedWal { get; init; } = true;

    /// <summary>Auto Compact when run count for ns ≥ N (0 = off).</summary>
    public int AutoCompactRunCount { get; init; }
}
