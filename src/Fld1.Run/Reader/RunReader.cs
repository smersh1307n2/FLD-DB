using Fld1.Run.Format;
using Fld1.Run.Verify;

namespace Fld1.Run.Reader;

/// <summary>
/// Zero-copy reader over an immutable FLD1 run buffer (MMF or byte[]).
/// </summary>
public readonly struct RunReader
{
    private readonly ReadOnlyMemory<byte> _file;

    public RunReader(ReadOnlyMemory<byte> file)
    {
        _file = file;
    }

    public ReadOnlySpan<byte> Span => _file.Span;
    public int Length => _file.Length;

    public bool TryValidate(out RunVerifyResult result)
    {
        result = RunVerify.Verify(_file.Span);
        return result.IsOk;
    }

    public uint NamespaceId => RunHeader.ReadNamespaceId(_file.Span);
    public RunFlags Flags => RunHeader.ReadFlags(_file.Span);
    public uint NumClusters => RunHeader.ReadNumClusters(_file.Span);
    public uint NumFields => RunHeader.ReadNumFields(_file.Span);
    public bool IsEmpty => NumClusters == 0 && NumFields == 0;
    public ReadOnlySpan<byte> RootDigest => RunHeader.RootDigest(_file.Span);

    /// <summary>
    /// Logical Get on empty run → miss (not CORRUPT). Spec [A8-EMPTY].
    /// Full cluster lookup comes in later iterations.
    /// </summary>
    public bool TryGet(ReadOnlySpan<byte> recordId, ushort fieldId, out ReadOnlySpan<byte> value)
    {
        value = default;
        if (IsEmpty) return false;
        // Non-empty path: not yet implemented in this iteration
        return false;
    }
}
