using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Fld1.Core.Util;

/// <summary>
/// Canonical unsigned byte-wise comparisons and helpers.
/// Spec: [A-CMP-ONE] single helper ByteUtil.CompareUnsigned.
/// </summary>
public static class ByteUtil
{
    /// <summary>
    /// Unsigned lexicographic comparison of two spans (memcmp-style, BE order for multi-byte keys).
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int CompareUnsigned(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b)
    {
        int len = Math.Min(a.Length, b.Length);
        for (int i = 0; i < len; i++)
        {
            int d = a[i] - b[i];
            if (d != 0) return d;
        }
        return a.Length - b.Length;
    }

    /// <summary>
    /// Unsigned comparison of exactly 16-byte spans (RecordId / AuthorNodeId).
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int CompareUnsigned16(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b)
    {
        if (a.Length != 16 || b.Length != 16)
            throw new ArgumentException("Expected exactly 16 bytes");
        // Prefer ulong compares for speed on 64-bit
        ref byte ra = ref MemoryMarshal.GetReference(a);
        ref byte rb = ref MemoryMarshal.GetReference(b);
        ulong a0 = Unsafe.ReadUnaligned<ulong>(ref ra);
        ulong b0 = Unsafe.ReadUnaligned<ulong>(ref rb);
        // Big-endian logical order ⇒ reverse bytes for numerical compare, or compare byte-wise
        // Spec: unsigned BE memcmp → first differing byte decides (higher address later).
        // So we must compare as raw BE bytes, not host endian.
        // Fast path: compare as two big-endian u64.
        ulong a0be = System.Buffers.Binary.BinaryPrimitives.ReverseEndianness(a0);
        ulong b0be = System.Buffers.Binary.BinaryPrimitives.ReverseEndianness(b0);
        int c = a0be.CompareTo(b0be);
        if (c != 0) return c;
        ulong a1 = Unsafe.ReadUnaligned<ulong>(ref Unsafe.Add(ref ra, 8));
        ulong b1 = Unsafe.ReadUnaligned<ulong>(ref Unsafe.Add(ref rb, 8));
        ulong a1be = System.Buffers.Binary.BinaryPrimitives.ReverseEndianness(a1);
        ulong b1be = System.Buffers.Binary.BinaryPrimitives.ReverseEndianness(b1);
        return a1be.CompareTo(b1be);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool Equals16(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b)
    {
        if (a.Length != 16 || b.Length != 16) return false;
        ref byte ra = ref MemoryMarshal.GetReference(a);
        ref byte rb = ref MemoryMarshal.GetReference(b);
        return Unsafe.ReadUnaligned<ulong>(ref ra) == Unsafe.ReadUnaligned<ulong>(ref rb)
            && Unsafe.ReadUnaligned<ulong>(ref Unsafe.Add(ref ra, 8)) == Unsafe.ReadUnaligned<ulong>(ref Unsafe.Add(ref rb, 8));
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void WriteUInt16LE(Span<byte> dest, ushort value)
    {
        System.Buffers.Binary.BinaryPrimitives.WriteUInt16LittleEndian(dest, value);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void WriteUInt32LE(Span<byte> dest, uint value)
    {
        System.Buffers.Binary.BinaryPrimitives.WriteUInt32LittleEndian(dest, value);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void WriteUInt64LE(Span<byte> dest, ulong value)
    {
        System.Buffers.Binary.BinaryPrimitives.WriteUInt64LittleEndian(dest, value);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ushort ReadUInt16LE(ReadOnlySpan<byte> src)
        => System.Buffers.Binary.BinaryPrimitives.ReadUInt16LittleEndian(src);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static uint ReadUInt32LE(ReadOnlySpan<byte> src)
        => System.Buffers.Binary.BinaryPrimitives.ReadUInt32LittleEndian(src);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ulong ReadUInt64LE(ReadOnlySpan<byte> src)
        => System.Buffers.Binary.BinaryPrimitives.ReadUInt64LittleEndian(src);
}
