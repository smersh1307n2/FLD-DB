namespace Fld1.Storage.Wal.Format;

/// <summary>Generic storage WAL framing constants. Record kinds are caller-owned opaque tags.</summary>
public static class WalConstants
{
    public static ReadOnlySpan<byte> Magic => "FLDW"u8;
    public const ushort WalVersion = 2;
    public const int HeaderSize = 64;
    public const int EnvelopeSize = 20;
    public const int RecordCrcSize = 4;
    public const int MinRecordSize = 24;
    public const int MaxPayloadLen = 16 * 1024 * 1024;
}