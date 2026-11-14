using System.ComponentModel;
using System.Runtime.InteropServices;

namespace Fld1.Storage.Engine;

/// <summary>
/// Flushes directory-entry updates on Unix. FileStream does not expose a portable
/// directory handle, so use the native open/fsync/close sequence explicitly.
/// </summary>
public static class StorageDirectorySync
{
    private static readonly object DirectoryGate = new();
    // Retain entries whose parent fsync failed so a later retry in this process
    // cannot mistake an existing-but-not-yet-durable directory for a durable one.
    private static readonly HashSet<string> PendingDirectoryEntries = new(StringComparer.Ordinal);
    private const int LinuxODirectory = 0x10000;
    private const int MacOsODirectory = 0x00100000;

    [DllImport("libc", EntryPoint = "open", SetLastError = true, CharSet = CharSet.Ansi)]
    private static extern int LinuxOpen(string path, int flags);

    [DllImport("libc", EntryPoint = "fsync", SetLastError = true)]
    private static extern int LinuxFsync(int fileDescriptor);

    [DllImport("libc", EntryPoint = "close", SetLastError = true)]
    private static extern int LinuxClose(int fileDescriptor);

    [DllImport("libSystem.B.dylib", EntryPoint = "open", SetLastError = true, CharSet = CharSet.Ansi)]
    private static extern int MacOsOpen(string path, int flags);

    [DllImport("libSystem.B.dylib", EntryPoint = "fsync", SetLastError = true)]
    private static extern int MacOsFsync(int fileDescriptor);

    [DllImport("libSystem.B.dylib", EntryPoint = "close", SetLastError = true)]
    private static extern int MacOsClose(int fileDescriptor);

    /// <summary>Creates a directory and durably flushes newly created parent entries on Unix.</summary>
    public static void CreateDirectoryDurable(string path)
    {
        string fullPath = Path.GetFullPath(path);
        lock (DirectoryGate)
        {
            var missing = new Stack<string>();
            string? current = fullPath;
            while (!string.IsNullOrEmpty(current) && !Directory.Exists(current))
            {
                missing.Push(current);
                current = Path.GetDirectoryName(current);
            }

            foreach (string created in missing)
                PendingDirectoryEntries.Add(created);
            Directory.CreateDirectory(fullPath);

            // Flush parent entries shallowest-first. If any fsync fails, keep
            // that entry pending so the next call retries it even though the
            // directory itself now exists in this process.
            foreach (string created in PendingDirectoryEntries.OrderBy(value => value.Length).ToArray())
            {
                string? parent = Path.GetDirectoryName(created);
                if (string.IsNullOrEmpty(parent) || !Directory.Exists(parent))
                    continue; // An unrelated pending path may not have been created yet.
                Flush(parent);
                PendingDirectoryEntries.Remove(created);
            }
        }
    }

    /// <summary>Flushes directory-entry updates on Linux/macOS; Windows directory durability is not claimed.</summary>
    public static void Flush(string directory)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(directory);
        if (OperatingSystem.IsWindows())
            return; // Windows directory-entry durability is not claimed by this engine.

        bool macOs = OperatingSystem.IsMacOS();
        if (!macOs && !OperatingSystem.IsLinux())
            return;

        int descriptor = macOs
            ? MacOsOpen(directory, MacOsODirectory)
            : LinuxOpen(directory, LinuxODirectory);
        if (descriptor < 0)
            throw CreateIoException("open directory for fsync", directory);

        try
        {
            int result = macOs ? MacOsFsync(descriptor) : LinuxFsync(descriptor);
            if (result != 0)
                throw CreateIoException("fsync directory", directory);
        }
        finally
        {
            if (macOs) _ = MacOsClose(descriptor);
            else _ = LinuxClose(descriptor);
        }
    }

    private static IOException CreateIoException(string operation, string directory)
        => new($"Unable to {operation} '{directory}'.", new Win32Exception(Marshal.GetLastPInvokeError()));
}
