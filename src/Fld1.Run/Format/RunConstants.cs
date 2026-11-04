namespace Fld1.Run.Format;

/// <summary>FLD1 physical format constants (spec PART I).</summary>
public static class RunConstants
{
    public static ReadOnlySpan<byte> Magic => "FLD1"u8; // 0x46 0x4C 0x44 0x31
    public const ushort BinaryVersion = 1;
    public const int HeaderSize = 256;
    public const int FooterSize = 8;
    public const int EmptyRunSize = 264; // header + footer, HAS_VALUES=0
    public const int ClusterHeaderSize = 40;
    public const int FieldDescSize = 8;
    public const int FieldHlcOverrideSize = 24;
    public const int CompactSlotSize = 16;
    public const int PgmSegmentSize = 32;
    public const int ValueRefSize = 16;
    public const int InternEntrySize = 16;
    public const uint MaxInlineValueLen = 256;
    public const ushort ExternalValLen = 0xFFFF;
    public const int MaxPayloadArea = 65535;
    public const int MaxBlobLen = 4 * 1024 * 1024;
    public const uint MaxPgmEpsilon = 32;
}

[Flags]
public enum RunFlags : ushort
{
    None = 0,
    HasValues = 0x01,
    IsCompacted = 0x02,
    HasHlcOverrides = 0x04,
    HasKeyEpochs = 0x08,
}

[Flags]
public enum ClusterFlags : ushort
{
    None = 0,
    Dead = 0x01,
    HasExternal = 0x02,
}

[Flags]
public enum FieldDescFlags : byte
{
    None = 0,
    Tombstone = 0x01,
    HasHlcOverride = 0x02,
}
