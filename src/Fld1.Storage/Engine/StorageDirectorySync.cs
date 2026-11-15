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
    private const uint WindowsMoveFileReplaceExisting = 0x00000001;
    private const uint WindowsMoveFileWriteThrough = 0x00000008;

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

    [DllImport("kernel32.dll", EntryPoint = "MoveFileExW", SetLastError = true, CharSet = CharSet.Unicode, ExactSpelling = true)]
    [return: MarshalAs(UnmanagedType.Bool)]
    private static extern bool WindowsMoveFileEx(string existingFileName, string newFileName, uint flags);

    /// <summary>Creates a directory and durably flushes newly created parent entries on Unix.</summary>
    public static void CreateDirectoryDurable(string path)
    {
        string fullPath = Path.GetFullPath(path);
        if (OperatingSystem.IsWindows())
        {
            CreateDirectoryDurableWindows(fullPath);
            return;
        }
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

    /// <summary>
    /// Flushes directory-entry updates on Linux/macOS. Windows publication uses
    /// MOVEFILE_WRITE_THROUGH through AtomicReplaceFile and CreateDirectoryDurable.
    /// </summary>
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

    /// <summary>
    /// Atomically replaces a file after its caller has flushed the temporary file.
    /// Unix additionally fsyncs the containing directory; Windows uses
    /// MOVEFILE_WRITE_THROUGH so the rename is durable before returning.
    /// </summary>
    public static void AtomicReplaceFile(
        string temporaryPath,
        string destinationPath,
        bool flushParent = true,
        Action? afterReplace = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(temporaryPath);
        ArgumentException.ThrowIfNullOrWhiteSpace(destinationPath);
        if (OperatingSystem.IsWindows())
        {
            if (!WindowsMoveFileEx(temporaryPath, destinationPath,
                    WindowsMoveFileReplaceExisting | WindowsMoveFileWriteThrough))
                throw new IOException(
                    $"Unable to durably replace '{destinationPath}'.",
                    new Win32Exception(Marshal.GetLastPInvokeError()));
            afterReplace?.Invoke();
            return;
        }

        File.Move(temporaryPath, destinationPath, overwrite: true);
        afterReplace?.Invoke();
        if (flushParent)
            Flush(Path.GetDirectoryName(destinationPath)!);
    }

    private static void CreateDirectoryDurableWindows(string fullPath)
    {
        var missing = new Stack<string>();
        string? current = fullPath;
        while (!string.IsNullOrEmpty(current) && !Directory.Exists(current))
        {
            missing.Push(current);
            current = Path.GetDirectoryName(current);
        }

        foreach (string created in missing)
        {
            string parent = Path.GetDirectoryName(created)
                ?? throw new IOException($"Unable to resolve parent directory for '{created}'.");
            string temporary = Path.Combine(
                parent, "." + Path.GetFileName(created) + ".create-" + Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(temporary);
            if (WindowsMoveFileEx(temporary, created, WindowsMoveFileWriteThrough))
                continue;

            int error = Marshal.GetLastPInvokeError();
            try { Directory.Delete(temporary); } catch { }
            if (Directory.Exists(created))
                continue; // Another opener won the create race.
            throw new IOException(
                $"Unable to durably create directory '{created}' with MOVEFILE_WRITE_THROUGH.",
                new Win32Exception(error));
        }
    }

    private static IOException CreateIoException(string operation, string directory)
        => new($"Unable to {operation} '{directory}'.", new Win32Exception(Marshal.GetLastPInvokeError()));
}
