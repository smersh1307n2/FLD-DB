using System.Buffers.Binary;
using Fld1.Core.Canon;
using Fld1.Core.Hash;
using Fld1.Run.Format;

namespace Fld1.Run.Builder;

/// <summary>
/// Builds the canonical empty FLD1 run (264 bytes). Spec [A8-EMPTY].
/// rootDigest = BLAKE3-256("FLD1-RUNSTATE-v1" ‖ ns u32 LE)
/// </summary>
public static class EmptyRunBuilder
{
    /// <summary>
    /// Write empty run into a buffer of length ≥ 264. Returns bytes written (264).
    /// </summary>
    public static int Write(Span<byte> dest, uint namespaceId)
    {
        if (namespaceId > 0xFFFF)
            throw new ArgumentOutOfRangeException(nameof(namespaceId), "formatVersion 1: ns ≤ 0xFFFF");
        if (dest.Length < RunConstants.EmptyRunSize)
            throw new ArgumentException($"dest must be ≥ {RunConstants.EmptyRunSize} bytes");

        dest.Clear();

        // Digest body = tag ‖ ns u32 LE
        Span<byte> nsLe = stackalloc byte[4];
        BinaryPrimitives.WriteUInt32LittleEndian(nsLe, namespaceId);
        Span<byte> digest = stackalloc byte[32];
        Hashes.Blake3_256Tagged(Tags.RunStateV1, nsLe, digest);

        RunHeader.WriteEmpty(dest.Slice(0, RunHeader.Size), namespaceId, digest);

        // Footer: xxHash64(bytes[0..footerOff), seed=0)
        ulong footerOff = RunHeader.ReadFooterOff(dest);
        ulong hash = Hashes.XxHash64(dest.Slice(0, (int)footerOff));
        BinaryPrimitives.WriteUInt64LittleEndian(dest.Slice((int)footerOff, 8), hash);

        return RunConstants.EmptyRunSize;
    }

    public static byte[] Create(uint namespaceId)
    {
        var buf = new byte[RunConstants.EmptyRunSize];
        Write(buf, namespaceId);
        return buf;
    }
}
