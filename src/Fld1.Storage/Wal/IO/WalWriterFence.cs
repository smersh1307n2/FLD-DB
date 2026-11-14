using System.Collections.Concurrent;
using System.Security.Cryptography;
using System.Text;

namespace Fld1.Storage.Wal.IO;

/// <summary>
/// Holds a named, OS-managed single-writer mutex for one WAL partition. A single
/// process-wide owner thread acquires and releases every named Mutex so callers may
/// dispose StorageDatabase from any thread without violating Mutex thread affinity.
/// The OS releases all held mutexes if the process terminates.
/// </summary>
internal sealed class WalWriterFence : IDisposable
{
    private static readonly Lazy<Manager> SharedManager = new(static () => new Manager());
    private readonly Guid _token;
    private int _disposed;

    private WalWriterFence(Guid token) => _token = token;

    public static WalWriterFence Acquire(string walDirectory, ulong partitionId)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(walDirectory);
        string fullPath = ResolvePhysicalDirectory(walDirectory);
        string identity = fullPath + "|" + partitionId.ToString(System.Globalization.CultureInfo.InvariantCulture);
        string digest = Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(identity)));
        string name = OperatingSystem.IsWindows()
            ? "Global\\Fld1Storage_" + digest
            : "Fld1Storage_" + digest;

        Guid token = SharedManager.Value.Acquire(name);
        return new WalWriterFence(token);
    }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
            return;
        SharedManager.Value.Release(_token);
    }

    private static string ResolvePhysicalDirectory(string path)
    {
        string fullPath = Path.GetFullPath(path);
        for (int pass = 0; pass < 40; pass++)
        {
            string root = Path.GetPathRoot(fullPath)
                ?? throw new IOException("WAL directory path has no filesystem root.");
            if (OperatingSystem.IsWindows())
                root = root.ToUpperInvariant();

            string current = root;
            string[] segments = fullPath[root.Length..].Split(
                new[] { Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar },
                StringSplitOptions.RemoveEmptyEntries);
            bool restart = false;

            for (int i = 0; i < segments.Length; i++)
            {
                string segment = segments[i];
                string actualSegment = segment;
                if (Directory.Exists(current))
                {
                    string?[] matches = Directory.EnumerateFileSystemEntries(current)
                        .Select(entry => Path.GetFileName(entry))
                        .Where(name => name is not null &&
                            string.Equals(name, segment, StringComparison.OrdinalIgnoreCase))
                        .Take(2)
                        .ToArray();
                    if (matches.Length == 1)
                        actualSegment = matches[0]!;
                    else if (matches.Length > 1 &&
                             matches.Any(name => string.Equals(name, segment, StringComparison.Ordinal)))
                        actualSegment = segment;
                }

                current = Path.Combine(current, actualSegment);
                FileSystemInfo? target = new DirectoryInfo(current)
                    .ResolveLinkTarget(returnFinalTarget: true);
                if (target is null)
                    continue;

                string resolvedTarget = Path.GetFullPath(target.FullName);
                string remaining = i + 1 < segments.Length
                    ? Path.Combine(segments[(i + 1)..])
                    : string.Empty;
                fullPath = remaining.Length == 0
                    ? resolvedTarget
                    : Path.Combine(resolvedTarget, remaining);
                restart = true;
                break;
            }

            if (!restart)
                return Path.TrimEndingDirectorySeparator(current);
        }

        throw new IOException("WAL directory path contains too many symbolic-link indirections.");
    }

    private sealed class Manager
    {
        private readonly BlockingCollection<Request> _requests = new();
        private readonly Dictionary<Guid, (Mutex Mutex, string Name)> _held = new();
        private readonly HashSet<string> _activeNames = new(StringComparer.Ordinal);

        public Manager()
        {
            var owner = new Thread(ProcessRequests)
            {
                IsBackground = true,
                Name = "Fld1.Storage.WalFenceManager"
            };
            owner.Start();
        }

        public Guid Acquire(string name)
        {
            var request = new Request(name, Guid.NewGuid(), release: false);
            _requests.Add(request);
            request.Completed.Wait();
            try
            {
                if (request.Error is not null)
                    throw new IOException("WAL partition is already open for writing.", request.Error);
                return request.Token;
            }
            finally
            {
                request.Completed.Dispose();
            }
        }

        public void Release(Guid token)
        {
            var request = new Request(null, token, release: true);
            _requests.Add(request);
            request.Completed.Wait();
            try
            {
                if (request.Error is not null)
                    throw new IOException("Failed to release the WAL partition writer fence.", request.Error);
            }
            finally
            {
                request.Completed.Dispose();
            }
        }

        private void ProcessRequests()
        {
            foreach (Request request in _requests.GetConsumingEnumerable())
            {
                try
                {
                    if (request.Release)
                    {
                        if (_held.Remove(request.Token, out var held))
                        {
                            _activeNames.Remove(held.Name);
                            try { held.Mutex.ReleaseMutex(); }
                            finally { held.Mutex.Dispose(); }
                        }
                    }
                    else
                    {
                        if (_activeNames.Contains(request.Name!))
                            throw new IOException("WAL partition is already open for writing in this process.");

                        var mutex = new Mutex(initiallyOwned: false, request.Name!);
                        bool acquired;
                        try
                        {
                            acquired = mutex.WaitOne(0);
                        }
                        catch (AbandonedMutexException)
                        {
                            // The previous process terminated while owning the mutex.
                            // This thread now owns it; WAL recovery proceeds under fence.
                            acquired = true;
                        }

                        if (!acquired)
                        {
                            mutex.Dispose();
                            throw new IOException("Another process owns the WAL partition writer fence.");
                        }

                        _held.Add(request.Token, (mutex, request.Name!));
                        _activeNames.Add(request.Name!);
                    }
                }
                catch (Exception ex)
                {
                    request.Error = ex;
                }
                finally
                {
                    request.Completed.Set();
                }
            }
        }
    }

    private sealed class Request
    {
        public string? Name { get; }
        public Guid Token { get; }
        public bool Release { get; }
        public ManualResetEventSlim Completed { get; } = new(false);
        public Exception? Error { get; set; }

        public Request(string? name, Guid token, bool release)
        {
            Name = name;
            Token = token;
            Release = release;
        }
    }
}
