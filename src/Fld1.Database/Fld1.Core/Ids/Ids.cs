using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Fld1.Core.Util;

namespace Fld1.Core.Ids;

/// <summary>
/// Fixed-size 16-byte identifier (RecordId or AuthorNodeId).
/// Stored as raw bytes; comparison is unsigned BE memcmp.
/// </summary>
[StructLayout(LayoutKind.Sequential, Size = 16)]
public unsafe struct Id16 : IEquatable<Id16>, IComparable<Id16>
{
    public fixed byte Bytes[16];

    public Id16(ReadOnlySpan<byte> src)
    {
        if (src.Length != 16) throw new ArgumentException("must be 16 bytes");
        fixed (byte* p = Bytes)
            src.CopyTo(new Span<byte>(p, 16));
    }

    public Span<byte> AsSpan()
    {
        fixed (byte* p = Bytes)
            return new Span<byte>(p, 16);
    }

    public ReadOnlySpan<byte> AsReadOnlySpan()
    {
        fixed (byte* p = Bytes)
            return new ReadOnlySpan<byte>(p, 16);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int CompareTo(Id16 other)
        => ByteUtil.CompareUnsigned16(AsReadOnlySpan(), other.AsReadOnlySpan());

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool Equals(Id16 other)
        => ByteUtil.Equals16(AsReadOnlySpan(), other.AsReadOnlySpan());

    public override bool Equals(object? obj) => obj is Id16 id && Equals(id);

    public override int GetHashCode()
    {
        var s = AsReadOnlySpan();
        ulong a = System.Buffers.Binary.BinaryPrimitives.ReadUInt64LittleEndian(s);
        ulong b = System.Buffers.Binary.BinaryPrimitives.ReadUInt64LittleEndian(s.Slice(8));
        return HashCode.Combine(a, b);
    }

    public static bool operator ==(Id16 a, Id16 b) => a.Equals(b);
    public static bool operator !=(Id16 a, Id16 b) => !a.Equals(b);
    public static bool operator <(Id16 a, Id16 b) => a.CompareTo(b) < 0;
    public static bool operator >(Id16 a, Id16 b) => a.CompareTo(b) > 0;

    public override string ToString()
        => Convert.ToHexString(AsReadOnlySpan()).ToLowerInvariant();
}

/// <summary>
/// NamespaceId: formatVersion 1 → ≤ 0xFFFF (zero-extend from BIN-SYNC u16).
/// </summary>
public readonly struct NamespaceId : IEquatable<NamespaceId>
{
    public readonly uint Value;

    public NamespaceId(uint value)
    {
        if (value > 0xFFFF)
            throw new ArgumentOutOfRangeException(nameof(value), "formatVersion 1: namespaceId ≤ 0xFFFF");
        Value = value;
    }

    public NamespaceId(ushort value) => Value = value;

    public bool Equals(NamespaceId other) => Value == other.Value;
    public override bool Equals(object? obj) => obj is NamespaceId n && Equals(n);
    public override int GetHashCode() => (int)Value;
    public static bool operator ==(NamespaceId a, NamespaceId b) => a.Value == b.Value;
    public static bool operator !=(NamespaceId a, NamespaceId b) => a.Value != b.Value;
    public override string ToString() => Value.ToString();
}
