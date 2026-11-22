using Fld1.Manifest.Format;
using Fld1.Storage.Engine;

namespace Fld1.Manifest.IO;

/// <summary>
/// Atomic manifest.fldm persistence. Spec [M-ATOMIC]:
/// temp → fsync → atomic rename → directory fsync on Linux/macOS (Windows directory durability is not claimed).
/// Reader sees old or new whole; a present CRC-invalid file fails closed.
/// </summary>
public sealed class AtomicManifestStore
{
    private readonly string _dir;
    private readonly string _path;
    private readonly string _tmpPath;
    private readonly object _lock = new();
    private readonly Action? _afterManifestRenameBeforeDirectoryFlush;

    public string Path => _path;

    public AtomicManifestStore(string dbDir) : this(dbDir, null) { }

    internal AtomicManifestStore(string dbDir, Action? afterManifestRenameBeforeDirectoryFlush)
    {
        _dir = dbDir;
        _path = System.IO.Path.Combine(dbDir, "manifest.fldm");
        _tmpPath = System.IO.Path.Combine(dbDir, "tmp", "manifest.tmp");
        _afterManifestRenameBeforeDirectoryFlush = afterManifestRenameBeforeDirectoryFlush;
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
            StorageDirectorySync.CreateDirectoryDurable(System.IO.Path.GetDirectoryName(_tmpPath)!);
            var bytes = ManifestCodec.Encode(doc);
            // Write tmp
            using (var fs = new FileStream(_tmpPath, FileMode.Create, FileAccess.Write, FileShare.None,
                       bufferSize: 64 * 1024, FileOptions.None))
            {
                fs.Write(bytes);
                fs.Flush(flushToDisk: true);
            }
            // Atomic replace. A failure after rename is an uncertain completion:
            // callers must fault their in-memory transaction owner and recover by reopening.
            StorageDirectorySync.AtomicReplaceFile(
                _tmpPath, _path, afterReplace: _afterManifestRenameBeforeDirectoryFlush);
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
        LocalOperationReceipts = d.LocalOperationReceipts,
        LocalOperationClients = d.LocalOperationClients,
        CoordinatorLeaseEpoch = d.CoordinatorLeaseEpoch,
        CoordinatorSessionId = d.CoordinatorSessionId
    };

}
