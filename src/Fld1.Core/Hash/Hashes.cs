using System.IO.Hashing;
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
    /// CRC32C (Castagnoli). Spec [K-BLOB-CRC], WAL envelopes.
    /// System.IO.Hashing.Crc32 uses the Castagnoli polynomial.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static uint Crc32C(ReadOnlySpan<byte> data)
        => Crc32.HashToUInt32(data);

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
