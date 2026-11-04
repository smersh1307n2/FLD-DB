namespace Fld1.Engine.Db;

/// <summary>Options for <see cref="FldDatabase.Open(FldOpenOptions)"/>.</summary>
public sealed class FldOpenOptions
{
    /// <summary>Database directory (created if missing).</summary>
    public required string Path { get; init; }

    /// <summary>Namespace id 0..65535 (BIN-SYNC u16 domain).</summary>
    public uint NamespaceId { get; init; }

    /// <summary>16-byte durable author node id; generated if null.</summary>
    public byte[]? LocalAuthor { get; init; }

    /// <summary>
    /// If &gt; 0, after Commit when overlay field count ≥ this value, auto Checkpoint.
    /// 0 = manual checkpoint only (default).
    /// </summary>
    public int AutoCheckpointOverlayFields { get; init; }
}
