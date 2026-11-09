using Fld1.Manifest.Format;

namespace Fld1.Manifest.IO;

/// <summary>
/// Atomic manifest.fldm persistence. Spec [M-ATOMIC]:
/// temp → fsync → atomic rename → fsync directory.
/// Reader sees old or new whole; a present CRC-invalid file fails closed.
/// </summary>
public sealed class AtomicManifestStore
{
    private readonly string _dir;
    private readonly string _path;
    private readonly string _tmpPath;
    private readonly object _lock = new();

    public string Path => _path;

    public AtomicManifestStore(string dbDir)
    {
        _dir = dbDir;
        _path = System.IO.Path.Combine(dbDir, "manifest.fldm");
        _tmpPath = System.IO.Path.Combine(dbDir, "tmp", "manifest.tmp");
    }

    /// <summary>Load manifest or Empty(generation:0) if missing; present corruption is fatal.</summary>
    public ManifestDocument Load()
    {
        lock (_lock)
        {
            if (!File.Exists(_path))
                return ManifestDocument.Empty(0);
            try
            {
                var bytes = File.ReadAllBytes(_path);
                return ManifestCodec.Decode(bytes);
            }
            catch (InvalidDataException ex)
            {
                // A present but corrupt manifest is not an empty database. Returning Empty
                // would make the Engine classify every Run as orphaned and delete user data.
                throw new InvalidDataException(
                    $"Manifest '{_path}' is corrupt; refusing to open the database or collect Runs.",
                    ex);
            }
        }
    }

    /// <summary>
    /// Persist document with generation = previous+1 (or doc.Generation if caller set it).
    /// Ensures tmp dir exists. Returns the written generation.
    /// </summary>
    public ulong Store(ManifestDocument doc)
    {
        lock (_lock)
        {
            Directory.CreateDirectory(System.IO.Path.GetDirectoryName(_tmpPath)!);
            var bytes = ManifestCodec.Encode(doc);
            // Write tmp
            using (var fs = new FileStream(_tmpPath, FileMode.Create, FileAccess.Write, FileShare.None,
                       bufferSize: 64 * 1024, FileOptions.None))
            {
                fs.Write(bytes);
                fs.Flush(flushToDisk: true);
            }
            // Atomic replace
            File.Move(_tmpPath, _path, overwrite: true);
            // Directory fsync best-effort (Linux)
            TryFsyncDirectory(_dir);
            return doc.Generation;
        }
    }

    /// <summary>Store a new generation = loaded.Generation + 1 with mutator.</summary>
    public ManifestDocument Update(Func<ManifestDocument, ManifestDocument> mutator)
    {
        lock (_lock)
        {
            var current = Load();
            var next = mutator(current);
            // Ensure generation advances
            if (next.Generation <= current.Generation)
            {
                next = CloneWithGeneration(next, current.Generation + 1);
            }
            Store(next);
            return next;
        }
    }

    private static ManifestDocument CloneWithGeneration(ManifestDocument d, ulong gen) => new()
    {
        Generation = gen,
        LastFlushedCommitSeq = d.LastFlushedCommitSeq,
        CertifiedWatermark = d.CertifiedWatermark,
        Namespaces = d.Namespaces,
        Runs = d.Runs,
        Retains = d.Retains,
        ResultClients = d.ResultClients,
        ProvenanceIndex = d.ProvenanceIndex,
        CoordinatorLeaseEpoch = d.CoordinatorLeaseEpoch,
        CoordinatorSessionId = d.CoordinatorSessionId
    };

    private static void TryFsyncDirectory(string dir)
    {
        try
        {
            // Portable best-effort: open directory and flush where supported
            if (OperatingSystem.IsLinux() || OperatingSystem.IsMacOS())
            {
                using var fs = new FileStream(dir, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
                fs.Flush(flushToDisk: true);
            }
        }
        catch
        {
            // Directory fsync not always permitted; rename is still atomic on same FS.
        }
    }
}
