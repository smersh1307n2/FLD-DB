using System.IO.Hashing;
using System.Numerics;
using System.Runtime.CompilerServices;
using Blake3.Managed;

namespace Fld1.Core.Hash;

/// <summary>
/// Canonical hashes used by FLD1:
/// - xxHash64 (seed=0) for run footer
/// - CRC32C for blobs / envelopes
/// - BLAKE3-256 for StateDigest / RootDigest
/// </summary>
public static class Hashes
{
    /// <summary>
    /// xxHash64 over data with seed 0. Spec [F-FOOTER].
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ulong XxHash64(ReadOnlySpan<byte> data)
        => System.IO.Hashing.XxHash64.HashToUInt64(data, seed: 0);

    /// <summary>
    /// CRC-32C/Castagnoli. New FLD1 files use the reflected Castagnoli polynomial.
    /// BitOperations dispatches to hardware CRC instructions when available and uses
    /// an optimized software fallback on other runtimes/architectures.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static uint Crc32C(ReadOnlySpan<byte> data)
        => ~AppendCrc32C(uint.MaxValue, data);

    /// <summary>CRC-32C over two logically concatenated spans without copying.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static uint Crc32C(ReadOnlySpan<byte> first, ReadOnlySpan<byte> second)
    {
        uint crc = AppendCrc32C(uint.MaxValue, first);
        return ~AppendCrc32C(crc, second);
    }

    /// <summary>
    /// Legacy CRC-32/IEEE used by early FLD1 builds before the CRC-32C correction.
    /// Only use this when validating old persisted files; never for new writes.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static uint Crc32Ieee(ReadOnlySpan<byte> data)
        => Crc32.HashToUInt32(data);

    /// <summary>Legacy CRC-32/IEEE over two concatenated spans without copying.</summary>
    public static uint Crc32Ieee(ReadOnlySpan<byte> first, ReadOnlySpan<byte> second)
    {
        using var hasher = new Crc32();
        hasher.Append(first);
        hasher.Append(second);
        return hasher.GetCurrentHashAsUInt32();
    }

    /// <summary>
    /// Accept the normative Castagnoli checksum or the legacy IEEE checksum emitted
    /// by earlier builds. Writers must continue to emit only CRC-32C.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool IsCrc32COrLegacy(ReadOnlySpan<byte> data, uint stored)
        => stored == Crc32C(data) || stored == Crc32Ieee(data);

    /// <summary>Two-span variant of <see cref="IsCrc32COrLegacy(ReadOnlySpan{byte}, uint)"/>.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool IsCrc32COrLegacy(
        ReadOnlySpan<byte> first, ReadOnlySpan<byte> second, uint stored)
        => stored == Crc32C(first, second) || stored == Crc32Ieee(first, second);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static uint AppendCrc32C(uint crc, ReadOnlySpan<byte> data)
    {
        int offset = 0;
        while (data.Length - offset >= 8)
        {
            ulong value = System.Buffers.Binary.BinaryPrimitives.ReadUInt64LittleEndian(
                data.Slice(offset, 8));
            crc = BitOperations.Crc32C(crc, value);
            offset += 8;
        }

        if (data.Length - offset >= 4)
        {
            uint value = System.Buffers.Binary.BinaryPrimitives.ReadUInt32LittleEndian(
                data.Slice(offset, 4));
            crc = BitOperations.Crc32C(crc, value);
            offset += 4;
        }

        if (data.Length - offset >= 2)
        {
            ushort value = System.Buffers.Binary.BinaryPrimitives.ReadUInt16LittleEndian(
                data.Slice(offset, 2));
            crc = BitOperations.Crc32C(crc, value);
            offset += 2;
        }

        if (offset < data.Length)
            crc = BitOperations.Crc32C(crc, data[offset]);

        return crc;
    }

    /// <summary>
    /// BLAKE3-256 into caller-provided 32-byte buffer.
    /// </summary>
    public static void Blake3_256(ReadOnlySpan<byte> data, Span<byte> digest32)
    {
        if (digest32.Length < 32) throw new ArgumentException("digest must be ≥ 32 bytes");
        using var hasher = Hasher.New();
        hasher.Update(data);
        var hash = hasher.Finalize();
        hash.AsSpan().CopyTo(digest32.Slice(0, 32));
    }

    /// <summary>
    /// BLAKE3-256 of (tag ‖ body). Convenience for canonical digests.
    /// </summary>
    public static void Blake3_256Tagged(ReadOnlySpan<byte> tag, ReadOnlySpan<byte> body, Span<byte> digest32)
    {
        if (digest32.Length < 32) throw new ArgumentException("digest must be ≥ 32 bytes");
        using var hasher = Hasher.New();
        hasher.Update(tag);
        hasher.Update(body);
        var hash = hasher.Finalize();
        hash.AsSpan().CopyTo(digest32.Slice(0, 32));
    }

    /// <summary>
    /// Allocate and return 32-byte BLAKE3 digest.
    /// </summary>
    public static byte[] Blake3_256(ReadOnlySpan<byte> data)
    {
        var d = new byte[32];
        Blake3_256(data, d);
        return d;
    }
}
