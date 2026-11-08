using System.ComponentModel;
using System.Runtime.InteropServices;

namespace Fld1.Storage.Engine;

/// <summary>
/// Flushes directory-entry updates on Unix. FileStream does not expose a portable
/// directory handle, so use the native open/fsync/close sequence explicitly.
/// </summary>
internal static class StorageDirectorySync
{
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

    internal static void CreateDirectoryDurable(string path)
    {
        string fullPath = Path.GetFullPath(path);
        var missing = new Stack<string>();
        string? current = fullPath;
        while (!string.IsNullOrEmpty(current) && !Directory.Exists(current))
        {
            missing.Push(current);
            current = Path.GetDirectoryName(current);
        }

        Directory.CreateDirectory(fullPath);

        // A mkdir changes the parent directory entry. Flush each parent from
        // the shallowest newly-created directory to the deepest one.
        while (missing.Count != 0)
        {
            string created = missing.Pop();
            string? parent = Path.GetDirectoryName(created);
            if (!string.IsNullOrEmpty(parent) && Directory.Exists(parent))
                Flush(parent);
        }
    }

    internal static void Flush(string directory)
    {
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
