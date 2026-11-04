namespace Fld1.Engine.Db;

/// <summary>BIN-SYNC / FLD lifecycle field 0x0000.</summary>
public static class FldLifecycle
{
    public const ushort FieldId = 0x0000;
    public const byte Alive = 0x00;
    public const byte Dead = 0x01;

    public static ReadOnlySpan<byte> AliveValue => new byte[] { Alive };
    public static ReadOnlySpan<byte> DeadValue => new byte[] { Dead };
}
