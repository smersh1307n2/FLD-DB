namespace Fld1.Wal.Format;

/// <summary>Micro-WAL v2 constants. Spec PART V.</summary>
public static class WalConstants
{
    /// <summary>Raw magic bytes — always memcmp, never UInt32LE compare. [WAL-MAGIC-BYTES]</summary>
    public static ReadOnlySpan<byte> Magic => "FLDW"u8; // 0x46 0x4C 0x44 0x57

    public const ushort WalVersion = 2;
    public const int HeaderSize = 64;
    public const int EnvelopeSize = 20;
    public const int RecordCrcSize = 4;
    public const int MinRecordSize = 24; // envelope without crc fields fully + empty payload + record crc... RawRecordSize = 24 + Length

    public const byte KindGroupCommit = 0x01;
    public const byte KindNsCreate = 0x02;
    public const byte KindSnapshotInstall = 0x03;
    public const byte KindApplyCommit = 0x04;
    public const byte KindIdentityStamp = 0x05;

    public const int MaxMutations = 10_000;
    public const int MaxResults = 10_000;
    public const int MaxProvenance = 10_000;
    public const int MaxValLen = 4 * 1024 * 1024;
    public const int MaxPayloadLen = 16 * 1024 * 1024;

    public const byte StatusCommitted = 0;
    public const byte StatusRejected = 1;
    public const byte StatusIdempotentDuplicate = 3;
}

public enum WalRecordKind : byte
{
    GroupCommit = WalConstants.KindGroupCommit,
    NsCreate = WalConstants.KindNsCreate,
    SnapshotInstall = WalConstants.KindSnapshotInstall,
    ApplyCommit = WalConstants.KindApplyCommit,
    IdentityStamp = WalConstants.KindIdentityStamp,
}
