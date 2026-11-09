using System.Buffers.Binary;
using Fld1.Core.Hash;
using Fld1.Core.Uuid;

namespace Fld1.Engine.Storage;

/// <summary>
/// Persists the database's durable author identity. The identity must remain stable
/// across reopen and must match BIN-SYNC NodeId when the database is externally hosted.
/// </summary>
internal static class LocalAuthorStore
{
    private static ReadOnlySpan<byte> Magic => "FLDA"u8;
    private const ushort Version = 1;
    private const int FileSize = 28;

    public static byte[] LoadOrCreate(string dbDir, byte[]? requested, bool hasExistingData)
    {
        if (requested is not null)
            ValidateAuthor(requested);

        string path = Path.Combine(dbDir, "local-author.flda");
        if (File.Exists(path))
            return Load(path, requested);

        if (hasExistingData && requested is null)
            throw new InvalidOperationException(
                "This database has durable data but no persisted LocalAuthor. Reopen it with the original BIN-SYNC NodeId to establish the author identity.");

        byte[] author = requested?.ToArray() ?? new byte[16];
        if (requested is null)
            Uuid7.NewUuid7(author);

        Span<byte> bytes = stackalloc byte[FileSize];
        bytes.Clear();
        Magic.CopyTo(bytes);
        BinaryPrimitives.WriteUInt16LittleEndian(bytes.Slice(4, 2), Version);
        author.CopyTo(bytes.Slice(8, 16));
        BinaryPrimitives.WriteUInt32LittleEndian(bytes.Slice(24, 4), Hashes.Crc32C(bytes.Slice(0, 24)));

        string tempPath = path + ".tmp";
        try
        {
            using (var fs = new FileStream(
                tempPath, FileMode.Create, FileAccess.Write, FileShare.None,
                bufferSize: FileSize, FileOptions.WriteThrough))
            {
                fs.Write(bytes);
                fs.Flush(flushToDisk: true);
            }

            File.Move(tempPath, path);
            TryFsyncDirectory(dbDir);
            return author;
        }
        catch (IOException) when (File.Exists(path))
        {
            try { File.Delete(tempPath); } catch { }
            return Load(path, requested);
        }
        catch
        {
            try { File.Delete(tempPath); } catch { }
            throw;
        }
    }

    private static byte[] Load(string path, byte[]? requested)
    {
        byte[] bytes = File.ReadAllBytes(path);
        if (bytes.Length != FileSize
            || !bytes.AsSpan(0, 4).SequenceEqual(Magic)
            || BinaryPrimitives.ReadUInt16LittleEndian(bytes.AsSpan(4, 2)) != Version
            || bytes[6] != 0 || bytes[7] != 0)
            throw new InvalidDataException("LocalAuthor file has invalid magic, version, reserved bytes, or length.");

        uint storedCrc = BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(24, 4));
        if (!Hashes.IsCrc32COrLegacy(bytes.AsSpan(0, 24), storedCrc))
            throw new InvalidDataException("LocalAuthor file CRC mismatch.");

        byte[] author = bytes.AsSpan(8, 16).ToArray();
        ValidateAuthor(author);
        if (requested is not null && !requested.AsSpan().SequenceEqual(author))
            throw new InvalidOperationException(
                "Configured LocalAuthor does not match the database's persisted author identity / BIN-SYNC NodeId.");

        return author;
    }

    private static void ValidateAuthor(ReadOnlySpan<byte> author)
    {
        if (author.Length != 16)
            throw new ArgumentException("LocalAuthor must be exactly 16 bytes.", nameof(author));
        bool allZero = true;
        foreach (byte value in author)
            allZero &= value == 0;
        if (allZero)
            throw new ArgumentException("LocalAuthor must be non-zero.", nameof(author));
    }

    private static void TryFsyncDirectory(string directory)
    {
        try
        {
            if (OperatingSystem.IsLinux() || OperatingSystem.IsMacOS())
            {
                using var fs = new FileStream(directory, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
                fs.Flush(flushToDisk: true);
            }
        }
        catch
        {
            // Directory handles cannot be flushed on every supported filesystem.
        }
    }
}
