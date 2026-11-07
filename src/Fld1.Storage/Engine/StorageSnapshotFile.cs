using System.Buffers.Binary;
using System.Text;
using Fld1.Storage.Integrity;

namespace Fld1.Storage.Engine;

/// <summary>
/// Immutable Storage checkpoint files rooted by an independently atomic manifest.
/// The manifest is published only after the complete snapshot is flushed.
/// </summary>
internal static class StorageSnapshotFile
{
    private static ReadOnlySpan<byte> SnapshotMagic => "FLDS"u8;
    private static ReadOnlySpan<byte> ManifestMagic => "FLDM"u8;
    private const ushort Version = 1;
    private const int SnapshotHeaderSize = 36;
    private const int ManifestSize = 48;

    internal sealed record State(
        ulong Generation,
        ulong CommitSeq,
        SortedDictionary<StorageDatabase.StorageKey, byte[]> Values);

    public static State? Load(string directory, ulong partitionId)
    {
        string snapshots = Path.Combine(directory, "snapshots");
        string manifestPath = ManifestPath(snapshots, partitionId);
        if (!File.Exists(manifestPath)) return null;

        byte[] manifest = File.ReadAllBytes(manifestPath);
        if (manifest.Length != ManifestSize || !manifest.AsSpan(0, 4).SequenceEqual(ManifestMagic))
            throw new InvalidDataException("Storage snapshot manifest has an invalid header or length.");
        if (BinaryPrimitives.ReadUInt16LittleEndian(manifest.AsSpan(4, 2)) != Version ||
            BinaryPrimitives.ReadUInt16LittleEndian(manifest.AsSpan(6, 2)) != 0)
            throw new InvalidDataException("Storage snapshot manifest version or reserved flags are invalid.");
        if (!StorageChecksums.IsCrc32COrLegacy(manifest.AsSpan(0, ManifestSize - 4),
                BinaryPrimitives.ReadUInt32LittleEndian(manifest.AsSpan(ManifestSize - 4))))
            throw new InvalidDataException("Storage snapshot manifest checksum mismatch.");

        ulong storedPartition = BinaryPrimitives.ReadUInt64LittleEndian(manifest.AsSpan(8, 8));
        ulong generation = BinaryPrimitives.ReadUInt64LittleEndian(manifest.AsSpan(16, 8));
        ulong commitSeq = BinaryPrimitives.ReadUInt64LittleEndian(manifest.AsSpan(24, 8));
        ulong snapshotLength = BinaryPrimitives.ReadUInt64LittleEndian(manifest.AsSpan(32, 8));
        uint snapshotCrc = BinaryPrimitives.ReadUInt32LittleEndian(manifest.AsSpan(40, 4));
        if (storedPartition != partitionId || generation == 0 || snapshotLength > int.MaxValue)
            throw new InvalidDataException("Storage snapshot manifest identity or length is invalid.");

        string snapshotPath = SnapshotPath(snapshots, partitionId, generation);
        if (!File.Exists(snapshotPath))
            throw new InvalidDataException($"Storage snapshot manifest references missing generation {generation}.");
        byte[] bytes = File.ReadAllBytes(snapshotPath);
        if ((ulong)bytes.Length != snapshotLength || bytes.Length < SnapshotHeaderSize + 4 ||
            BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(bytes.Length - 4)) != snapshotCrc ||
            !StorageChecksums.IsCrc32COrLegacy(bytes.AsSpan(0, bytes.Length - 4), snapshotCrc))
            throw new InvalidDataException("Storage snapshot file length or checksum does not match its manifest.");

        return DecodeSnapshot(bytes, partitionId, generation, commitSeq);
    }

    public static void Store(
        string directory,
        ulong partitionId,
        ulong generation,
        ulong commitSeq,
        SortedDictionary<StorageDatabase.StorageKey, byte[]> values)
    {
        if (generation == 0) throw new ArgumentOutOfRangeException(nameof(generation));
        string snapshots = Path.Combine(directory, "snapshots");
        Directory.CreateDirectory(snapshots);

        byte[] snapshot = EncodeSnapshot(partitionId, generation, commitSeq, values);
        uint snapshotCrc = BinaryPrimitives.ReadUInt32LittleEndian(snapshot.AsSpan(snapshot.Length - 4));
        string finalSnapshot = SnapshotPath(snapshots, partitionId, generation);
        string tempSnapshot = finalSnapshot + ".tmp";
        if (File.Exists(finalSnapshot))
            throw new IOException($"Storage snapshot generation already exists: {generation}.");
        TryDelete(tempSnapshot);
        WriteDurable(tempSnapshot, snapshot);
        File.Move(tempSnapshot, finalSnapshot);
        TryFsyncDirectory(snapshots);

        byte[] manifest = EncodeManifest(partitionId, generation, commitSeq, (ulong)snapshot.Length, snapshotCrc);
        string finalManifest = ManifestPath(snapshots, partitionId);
        string tempManifest = finalManifest + ".tmp";
        TryDelete(tempManifest);
        WriteDurable(tempManifest, manifest);
        File.Move(tempManifest, finalManifest, overwrite: true);
        TryFsyncDirectory(snapshots);

        // The manifest is now the durable root for the new generation. StorageSnapshot
        // instances are in-memory copies, so no reader pins the previous disk generation.
        string prefix = $"p_{partitionId}.";
        foreach (string stale in Directory.EnumerateFiles(snapshots, prefix + "*.snap"))
        {
            if (string.Equals(stale, finalSnapshot, StringComparison.Ordinal)) continue;
            try { File.Delete(stale); }
            catch { /* orphan cleanup is best-effort; the manifest remains authoritative */ }
        }
        TryFsyncDirectory(snapshots);
    }

    private static byte[] EncodeSnapshot(
        ulong partitionId,
        ulong generation,
        ulong commitSeq,
        SortedDictionary<StorageDatabase.StorageKey, byte[]> values)
    {
        using var stream = new MemoryStream();
        stream.Write(SnapshotMagic);
        Span<byte> header = stackalloc byte[SnapshotHeaderSize - 4];
        BinaryPrimitives.WriteUInt16LittleEndian(header, Version);
        BinaryPrimitives.WriteUInt16LittleEndian(header.Slice(2), 0);
        BinaryPrimitives.WriteUInt64LittleEndian(header.Slice(4), partitionId);
        BinaryPrimitives.WriteUInt64LittleEndian(header.Slice(12), generation);
        BinaryPrimitives.WriteUInt64LittleEndian(header.Slice(20), commitSeq);
        BinaryPrimitives.WriteUInt32LittleEndian(header.Slice(28), checked((uint)values.Count));
        stream.Write(header);

        Span<byte> lengths = stackalloc byte[8];
        foreach (var pair in values)
        {
            BinaryPrimitives.WriteUInt32LittleEndian(lengths, checked((uint)pair.Key.Span.Length));
            BinaryPrimitives.WriteUInt32LittleEndian(lengths.Slice(4), checked((uint)pair.Value.Length));
            stream.Write(lengths);
            stream.Write(pair.Key.Span);
            stream.Write(pair.Value);
        }

        byte[] body = stream.ToArray();
        uint crc = StorageChecksums.Crc32C(body);
        byte[] result = new byte[body.Length + 4];
        body.CopyTo(result, 0);
        BinaryPrimitives.WriteUInt32LittleEndian(result.AsSpan(body.Length), crc);
        return result;
    }

    private static State DecodeSnapshot(byte[] bytes, ulong partitionId, ulong generation, ulong commitSeq)
    {
        ReadOnlySpan<byte> data = bytes;
        if (!data.Slice(0, 4).SequenceEqual(SnapshotMagic) ||
            BinaryPrimitives.ReadUInt16LittleEndian(data.Slice(4, 2)) != Version ||
            BinaryPrimitives.ReadUInt16LittleEndian(data.Slice(6, 2)) != 0 ||
            BinaryPrimitives.ReadUInt64LittleEndian(data.Slice(8, 8)) != partitionId ||
            BinaryPrimitives.ReadUInt64LittleEndian(data.Slice(16, 8)) != generation ||
            BinaryPrimitives.ReadUInt64LittleEndian(data.Slice(24, 8)) != commitSeq)
            throw new InvalidDataException("Storage snapshot identity or format is invalid.");

        uint count = BinaryPrimitives.ReadUInt32LittleEndian(data.Slice(32, 4));
        int offset = SnapshotHeaderSize;
        int end = bytes.Length - 4;
        var values = new SortedDictionary<StorageDatabase.StorageKey, byte[]>(StorageDatabase.StorageKeyComparer.Instance);
        StorageDatabase.StorageKey? previous = null;
        for (uint i = 0; i < count; i++)
        {
            if (end - offset < 8) throw new InvalidDataException("Storage snapshot entry header is truncated.");
            uint keyLength = BinaryPrimitives.ReadUInt32LittleEndian(data.Slice(offset, 4));
            uint valueLength = BinaryPrimitives.ReadUInt32LittleEndian(data.Slice(offset + 4, 4));
            offset += 8;
            if (keyLength is 0 or > 1024 * 1024 || valueLength > int.MaxValue ||
                (ulong)(end - offset) < (ulong)keyLength + valueLength)
                throw new InvalidDataException("Storage snapshot entry length is invalid.");

            var key = new StorageDatabase.StorageKey(data.Slice(offset, (int)keyLength));
            offset += (int)keyLength;
            byte[] value = data.Slice(offset, (int)valueLength).ToArray();
            offset += (int)valueLength;
            if (previous.HasValue && StorageDatabase.StorageKeyComparer.Instance.Compare(previous.Value, key) >= 0)
                throw new InvalidDataException("Storage snapshot keys are not strictly ordered.");
            values.Add(key, value);
            previous = key;
        }

        if (offset != end) throw new InvalidDataException("Storage snapshot has trailing or missing bytes.");
        return new State(generation, commitSeq, values);
    }

    private static byte[] EncodeManifest(ulong partitionId, ulong generation, ulong commitSeq, ulong length, uint snapshotCrc)
    {
        byte[] bytes = new byte[ManifestSize];
        ManifestMagic.CopyTo(bytes);
        BinaryPrimitives.WriteUInt16LittleEndian(bytes.AsSpan(4, 2), Version);
        BinaryPrimitives.WriteUInt64LittleEndian(bytes.AsSpan(8, 8), partitionId);
        BinaryPrimitives.WriteUInt64LittleEndian(bytes.AsSpan(16, 8), generation);
        BinaryPrimitives.WriteUInt64LittleEndian(bytes.AsSpan(24, 8), commitSeq);
        BinaryPrimitives.WriteUInt64LittleEndian(bytes.AsSpan(32, 8), length);
        BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan(40, 4), snapshotCrc);
        uint crc = StorageChecksums.Crc32C(bytes.AsSpan(0, ManifestSize - 4));
        BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan(ManifestSize - 4), crc);
        return bytes;
    }

    private static string ManifestPath(string directory, ulong partitionId)
        => Path.Combine(directory, $"p_{partitionId}.manifest");

    private static string SnapshotPath(string directory, ulong partitionId, ulong generation)
        => Path.Combine(directory, $"p_{partitionId}.{generation}.snap");

    private static void WriteDurable(string path, ReadOnlySpan<byte> bytes)
    {
        using var stream = new FileStream(path, FileMode.CreateNew, FileAccess.Write, FileShare.None,
            bufferSize: 64 * 1024, FileOptions.WriteThrough);
        stream.Write(bytes);
        stream.Flush(flushToDisk: true);
    }

    private static void TryDelete(string path)
    {
        if (File.Exists(path)) File.Delete(path);
    }

    private static void TryFsyncDirectory(string path)
    {
        try
        {
            if (OperatingSystem.IsLinux() || OperatingSystem.IsMacOS())
            {
                using var stream = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
                stream.Flush(flushToDisk: true);
            }
        }
        catch
        {
            // Directory handles are not flushable on every supported filesystem.
        }
    }
}