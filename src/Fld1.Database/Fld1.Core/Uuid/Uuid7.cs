using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using Fld1.Core.Util;

namespace Fld1.Core.Uuid;

/// <summary>
/// UUIDv7 helpers. Layout per RFC 9562 / FLD spec §0.6.
/// K(u) = (ExtractTimestampMs(u) &lt;&lt; 12) | ExtractRandA(u)  // 60 bit
/// </summary>
public static class Uuid7
{
    public const byte Version = 0x7;
    public const byte Variant = 0b10; // top 2 bits of byte 8

    /// <summary>
    /// Extract 48-bit Unix timestamp (ms) from bytes 0..5 (big-endian).
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ulong ExtractTimestampMs(ReadOnlySpan<byte> uuid)
    {
        if (uuid.Length < 6) throw new ArgumentException("UUID too short");
        // bytes 0..5 BE → 48-bit
        ulong hi = uuid[0];
        hi = (hi << 8) | uuid[1];
        hi = (hi << 8) | uuid[2];
        hi = (hi << 8) | uuid[3];
        hi = (hi << 8) | uuid[4];
        hi = (hi << 8) | uuid[5];
        return hi;
    }

    /// <summary>
    /// Extract 12-bit rand_a from bytes 6..7 (after version nibble).
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ushort ExtractRandA(ReadOnlySpan<byte> uuid)
    {
        if (uuid.Length < 8) throw new ArgumentException("UUID too short");
        // byte6: hi nibble = version, lo nibble = rand_a[11..8]
        // byte7: rand_a[7..0]
        int loNibble = uuid[6] & 0x0F;
        return (ushort)((loNibble << 8) | uuid[7]);
    }

    /// <summary>
    /// 60-bit monotonic projection key for PGM: (ms &lt;&lt; 12) | rand_a.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ulong ProjectK(ReadOnlySpan<byte> uuid)
    {
        ulong ms = ExtractTimestampMs(uuid);
        ushort ra = ExtractRandA(uuid);
        return (ms << 12) | ra;
    }

    /// <summary>
    /// Validate version=7 and variant=10xx. Returns false if invalid.
    /// Spec [K-UUIDV7]: ingest/build rejects invalid → INVALID_RECORD_ID.
    /// Readers do not reject non-v7 (fallback search).
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool IsValidV7(ReadOnlySpan<byte> uuid)
    {
        if (uuid.Length != 16) return false;
        byte ver = (byte)((uuid[6] >> 4) & 0x0F);
        if (ver != Version) return false;
        byte varBits = (byte)((uuid[8] >> 6) & 0b11);
        return varBits == Variant;
    }

    /// <summary>
    /// Generate a new UUIDv7 into the provided 16-byte span.
    /// Wall time in milliseconds UTC; random bits from CSPRNG.
    /// </summary>
    public static void NewUuid7(Span<byte> dest)
    {
        if (dest.Length != 16) throw new ArgumentException("dest must be 16 bytes");
        long ms = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        if (ms < 0) ms = 0;
        // timestamp 48 bit BE into 0..5
        dest[0] = (byte)(ms >> 40);
        dest[1] = (byte)(ms >> 32);
        dest[2] = (byte)(ms >> 24);
        dest[3] = (byte)(ms >> 16);
        dest[4] = (byte)(ms >> 8);
        dest[5] = (byte)ms;

        Span<byte> rnd = stackalloc byte[10];
        RandomNumberGenerator.Fill(rnd);

        // byte6: version 7 + rand_a hi
        dest[6] = (byte)(0x70 | (rnd[0] & 0x0F));
        dest[7] = rnd[1];
        // byte8: variant 10 + rand_b
        dest[8] = (byte)(0x80 | (rnd[2] & 0x3F));
        rnd.Slice(3, 7).CopyTo(dest.Slice(9, 7));
    }

    /// <summary>
    /// ShortKey = RecordId[12..15] (last 4 bytes) for SIMD prefilter.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static uint ShortKey(ReadOnlySpan<byte> uuid)
    {
        if (uuid.Length < 16) throw new ArgumentException("UUID too short");
        return BinaryPrimitives.ReadUInt32BigEndian(uuid.Slice(12, 4));
    }
}
