using System.Text;

namespace Fld1.Core.Canon;

/// <summary>
/// Canonical ASCII tags without NUL/pad. Spec [TAG-ASCII].
/// </summary>
public static class Tags
{
    // "FLD1-RUNSTATE-v1" = 16 bytes
    public static ReadOnlySpan<byte> RunStateV1 => "FLD1-RUNSTATE-v1"u8;

    // "FLD1-DBSTATE-v1" = 15 bytes
    public static ReadOnlySpan<byte> DbStateV1 => "FLD1-DBSTATE-v1"u8;

    // "FLD1-COMP-v1" = 12 bytes
    public static ReadOnlySpan<byte> CompV1 => "FLD1-COMP-v1"u8;

    public static readonly string RunStateV1String = "FLD1-RUNSTATE-v1";
    public static readonly string DbStateV1String = "FLD1-DBSTATE-v1";
    public static readonly string CompV1String = "FLD1-COMP-v1";
}
