using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Fld1.Core.Util;

/// <summary>
/// Canonical unsigned byte-wise comparisons and helpers.
/// Spec: [A-CMP-ONE] single helper family.
/// AuthorNodeId order matches BinSync.NodeId (LE u64 limbs) for LWW convergence.
/// RecordId order remains unsigned BE memcmp (UUIDv7 / PGM).
/// </summary>
public static class ByteUtil
{
    /// <summary>
    /// Unsigned lexicographic comparison (byte 0 first) — RecordId / general keys.
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
    /// RecordId / 16B BE memcmp (unsigned lexicographic).
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int CompareUnsigned16(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b)
    {
        if (a.Length != 16 || b.Length != 16)
            throw new ArgumentException("Expected exactly 16 bytes");
        ref byte ra = ref MemoryMarshal.GetReference(a);
        ref byte rb = ref MemoryMarshal.GetReference(b);
        ulong a0 = Unsafe.ReadUnaligned<ulong>(ref ra);
        ulong b0 = Unsafe.ReadUnaligned<ulong>(ref rb);
        ulong a0be = BinaryPrimitives.ReverseEndianness(a0);
        ulong b0be = BinaryPrimitives.ReverseEndianness(b0);
        int c = a0be.CompareTo(b0be);
        if (c != 0) return c;
        ulong a1 = Unsafe.ReadUnaligned<ulong>(ref Unsafe.Add(ref ra, 8));
        ulong b1 = Unsafe.ReadUnaligned<ulong>(ref Unsafe.Add(ref rb, 8));
        ulong a1be = BinaryPrimitives.ReverseEndianness(a1);
        ulong b1be = BinaryPrimitives.ReverseEndianness(b1);
        return a1be.CompareTo(b1be);
    }

    /// <summary>
    /// AuthorNodeId total order matching BinSync.NodeId (Lo then Hi as LE u64).
    /// Required for LWW / digest convergence with BinSync.Core ≥ 2.2.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int CompareAuthorNodeId(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b)
    {
        if (a.Length != 16 || b.Length != 16)
            throw new ArgumentException("Expected exactly 16 bytes");
        ulong aLo = BinaryPrimitives.ReadUInt64LittleEndian(a);
        ulong bLo = BinaryPrimitives.ReadUInt64LittleEndian(b);
        if (aLo != bLo) return aLo < bLo ? -1 : 1;
        ulong aHi = BinaryPrimitives.ReadUInt64LittleEndian(a.Slice(8));
        ulong bHi = BinaryPrimitives.ReadUInt64LittleEndian(b.Slice(8));
        if (aHi != bHi) return aHi < bHi ? -1 : 1;
        return 0;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool Equals16(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b)
    {
        if (a.Length != 16 || b.Length != 16) return false;
        ref byte ra = ref MemoryMarshal.GetReference(a);
        ref byte rb = ref MemoryMarshal.GetReference(b);
        return Unsafe.ReadUnaligned<ulong>(ref ra) == Unsafe.ReadUnaligned<ulong>(ref rb)
            && Unsafe.ReadUnaligned<ulong>(ref Unsafe.Add(ref ra, 8))
                == Unsafe.ReadUnaligned<ulong>(ref Unsafe.Add(ref rb, 8));
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void WriteUInt16LE(Span<byte> dest, ushort value)
        => BinaryPrimitives.WriteUInt16LittleEndian(dest, value);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void WriteUInt32LE(Span<byte> dest, uint value)
        => BinaryPrimitives.WriteUInt32LittleEndian(dest, value);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void WriteUInt64LE(Span<byte> dest, ulong value)
        => BinaryPrimitives.WriteUInt64LittleEndian(dest, value);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ushort ReadUInt16LE(ReadOnlySpan<byte> src)
        => BinaryPrimitives.ReadUInt16LittleEndian(src);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static uint ReadUInt32LE(ReadOnlySpan<byte> src)
        => BinaryPrimitives.ReadUInt32LittleEndian(src);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ulong ReadUInt64LE(ReadOnlySpan<byte> src)
        => BinaryPrimitives.ReadUInt64LittleEndian(src);
}
