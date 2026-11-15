using System.ComponentModel;
using System.Runtime.InteropServices;
using System.Collections.Concurrent;
using System.Text;
using Microsoft.Win32.SafeHandles;

namespace Fld1.Storage.Engine;

/// <summary>
/// Platform-specific durable publication primitives: directory fsync on Unix and
/// write-through atomic moves on Windows.
/// </summary>
public static class StorageDirectorySync
{
    private static readonly object DirectoryGate = new();
    // Retain entries whose parent fsync failed so a later retry in this process
    // cannot mistake an existing-but-not-yet-durable directory for a durable one.
    private static readonly HashSet<string> PendingDirectoryEntries = new(StringComparer.Ordinal);
    private const int LinuxODirectory = 0x10000;
    private const int MacOsODirectory = 0x00100000;
    private const uint WindowsGenericRead = 0x80000000;
    private const uint WindowsGenericWrite = 0x40000000;
    private const uint WindowsDeleteAccess = 0x00010000;
    private const uint WindowsOpenExisting = 3;
    private const uint WindowsFileFlagWriteThrough = 0x80000000;
    private const uint WindowsFileFlagBackupSemantics = 0x02000000;
    private const int WindowsFileRenameInfo = 3;
    private const uint WindowsDriveFixed = 3;
    private static readonly ConcurrentDictionary<string, bool> SupportedWindowsVolumes = new(StringComparer.OrdinalIgnoreCase);

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

    [DllImport("kernel32.dll", EntryPoint = "CreateFileW", SetLastError = true, CharSet = CharSet.Unicode, ExactSpelling = true)]
    private static extern SafeFileHandle WindowsCreateFile(
        string fileName, uint desiredAccess, uint shareMode, IntPtr securityAttributes,
        uint creationDisposition, uint flagsAndAttributes, IntPtr templateFile);

    [DllImport("kernel32.dll", EntryPoint = "FlushFileBuffers", SetLastError = true, ExactSpelling = true)]
    [return: MarshalAs(UnmanagedType.Bool)]
    private static extern bool WindowsFlushFileBuffers(SafeFileHandle fileHandle);

    [DllImport("kernel32.dll", EntryPoint = "SetFileInformationByHandle", SetLastError = true, ExactSpelling = true)]
    [return: MarshalAs(UnmanagedType.Bool)]
    private static extern bool WindowsSetFileInformationByHandle(
        SafeFileHandle fileHandle, int fileInformationClass, IntPtr fileInformation, uint bufferSize);

    [DllImport("kernel32.dll", EntryPoint = "GetVolumePathNameW", SetLastError = true, CharSet = CharSet.Unicode, ExactSpelling = true)]
    [return: MarshalAs(UnmanagedType.Bool)]
    private static extern bool WindowsGetVolumePathName(string fileName, StringBuilder volumePathName, uint bufferLength);

    [DllImport("kernel32.dll", EntryPoint = "GetDriveTypeW", SetLastError = true, CharSet = CharSet.Unicode, ExactSpelling = true)]
    private static extern uint WindowsGetDriveType(string rootPathName);

    [DllImport("kernel32.dll", EntryPoint = "GetVolumeInformationW", SetLastError = true, CharSet = CharSet.Unicode, ExactSpelling = true)]
    [return: MarshalAs(UnmanagedType.Bool)]
    private static extern bool WindowsGetVolumeInformation(
        string rootPathName, StringBuilder? volumeNameBuffer, uint volumeNameSize,
        out uint volumeSerialNumber, out uint maximumComponentLength, out uint fileSystemFlags,
        StringBuilder fileSystemNameBuffer, uint fileSystemNameSize);

    [StructLayout(LayoutKind.Sequential)]
    private struct WindowsFileRenameInfoHeader
    {
        [MarshalAs(UnmanagedType.U1)]
        public byte ReplaceIfExists;
        public IntPtr RootDirectory;
        public uint FileNameLength;
        public char FileName;
    }

    /// <summary>Creates directories using platform-specific namespace publication; Windows remains subject to the power-loss proof gate.</summary>
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
    /// NTFS FILE_FLAG_WRITE_THROUGH handle-scoped renames through AtomicReplaceFile and CreateDirectoryDurable.
    /// </summary>
    public static void Flush(string directory)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(directory);
        if (OperatingSystem.IsWindows())
            return; // Direct directory fsync is unavailable; use the write-through publication helpers.

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
    /// Unix additionally fsyncs the containing directory; Windows uses an NTFS
    /// FILE_FLAG_WRITE_THROUGH handle-scoped rename. Other Windows filesystems are rejected.
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
            WindowsRenameByHandle(temporaryPath, destinationPath, replaceExisting: true, directory: false);
            afterReplace?.Invoke();
            return;
        }

        File.Move(temporaryPath, destinationPath, overwrite: true);
        afterReplace?.Invoke();
        if (flushParent)
            Flush(Path.GetDirectoryName(destinationPath)!);
    }

    /// <summary>
    /// Publishes a new file without replacing an existing destination. The source
    /// file must already be flushed by its writer.
    /// </summary>
    public static void AtomicCreateFile(
        string temporaryPath,
        string destinationPath,
        bool flushParent = true,
        Action? afterMove = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(temporaryPath);
        ArgumentException.ThrowIfNullOrWhiteSpace(destinationPath);
        if (OperatingSystem.IsWindows())
        {
            WindowsRenameByHandle(temporaryPath, destinationPath, replaceExisting: false, directory: false);
            afterMove?.Invoke();
            return;
        }

        File.Move(temporaryPath, destinationPath, overwrite: false);
        afterMove?.Invoke();
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
            try
            {
                WindowsRenameByHandle(temporary, created, replaceExisting: false, directory: true);
            }
            catch (IOException)
            {
                try { Directory.Delete(temporary); } catch { }
                if (Directory.Exists(created))
                    continue; // Another opener won the create race.
                throw;
            }
            catch
            {
                try { Directory.Delete(temporary); } catch { }
                throw;
            }
        }
    }

    private static void WindowsRenameByHandle(
        string sourcePath,
        string destinationPath,
        bool replaceExisting,
        bool directory)
    {
        EnsureSupportedWindowsNtfsVolume(sourcePath);
        string source = ToWindowsExtendedPath(sourcePath);
        string destination = ToWindowsExtendedPath(destinationPath);
        uint access = WindowsDeleteAccess;
        uint flags = WindowsFileFlagWriteThrough;
        if (directory)
            flags |= WindowsFileFlagBackupSemantics;
        else
            access |= WindowsGenericRead | WindowsGenericWrite;

        using SafeFileHandle handle = WindowsCreateFile(
            source, access, 0, IntPtr.Zero, WindowsOpenExisting, flags, IntPtr.Zero);
        if (handle.IsInvalid)
            throw new IOException(
                $"Unable to open '{sourcePath}' for a write-through rename.",
                new Win32Exception(Marshal.GetLastPInvokeError()));

        if (!directory && !WindowsFlushFileBuffers(handle))
            throw new IOException(
                $"Unable to flush temporary file '{sourcePath}' before rename.",
                new Win32Exception(Marshal.GetLastPInvokeError()));

        byte[] destinationBytes = Encoding.Unicode.GetBytes(destination);
        int nameOffset = Marshal.OffsetOf<WindowsFileRenameInfoHeader>(
            nameof(WindowsFileRenameInfoHeader.FileName)).ToInt32();
        int rootOffset = Marshal.OffsetOf<WindowsFileRenameInfoHeader>(
            nameof(WindowsFileRenameInfoHeader.RootDirectory)).ToInt32();
        int lengthOffset = Marshal.OffsetOf<WindowsFileRenameInfoHeader>(
            nameof(WindowsFileRenameInfoHeader.FileNameLength)).ToInt32();
        int bufferLength = checked(nameOffset + destinationBytes.Length);
        IntPtr info = Marshal.AllocHGlobal(bufferLength);
        try
        {
            Marshal.WriteByte(info, 0, replaceExisting ? (byte)1 : (byte)0);
            Marshal.WriteIntPtr(info, rootOffset, IntPtr.Zero);
            Marshal.WriteInt32(info, lengthOffset, destinationBytes.Length);
            Marshal.Copy(destinationBytes, 0, IntPtr.Add(info, nameOffset), destinationBytes.Length);
            if (!WindowsSetFileInformationByHandle(
                    handle, WindowsFileRenameInfo, info, checked((uint)bufferLength)))
                throw new IOException(
                    $"Unable to publish '{destinationPath}' with a write-through handle rename.",
                    new Win32Exception(Marshal.GetLastPInvokeError()));
        }
        finally
        {
            Marshal.FreeHGlobal(info);
        }
    }

    private static void EnsureSupportedWindowsNtfsVolume(string path)
    {
        string fullPath = Path.GetFullPath(path);
        var volumeBuffer = new StringBuilder(32768);
        if (!WindowsGetVolumePathName(fullPath, volumeBuffer, (uint)volumeBuffer.Capacity))
            throw new IOException(
                $"Unable to resolve the volume for '{path}'.",
                new Win32Exception(Marshal.GetLastPInvokeError()));

        string volumePath = volumeBuffer.ToString();
        if (!SupportedWindowsVolumes.TryGetValue(volumePath, out bool supported))
        {
            supported = WindowsGetDriveType(volumePath) == WindowsDriveFixed;
            if (supported)
            {
                var fileSystemName = new StringBuilder(64);
                if (!WindowsGetVolumeInformation(
                        volumePath, null, 0, out _, out _, out _, fileSystemName, (uint)fileSystemName.Capacity))
                    throw new IOException(
                        $"Unable to query the filesystem for '{path}'.",
                        new Win32Exception(Marshal.GetLastPInvokeError()));
                supported = string.Equals(fileSystemName.ToString(), "NTFS", StringComparison.OrdinalIgnoreCase);
            }
            SupportedWindowsVolumes.TryAdd(volumePath, supported);
        }

        if (!supported)
            throw new PlatformNotSupportedException(
                $"Durable Windows namespace publication is currently restricted to local fixed NTFS volumes; '{path}' is not on a supported volume.");
    }

    private static string ToWindowsExtendedPath(string path)
    {
        string fullPath = Path.GetFullPath(path);
        if (fullPath.StartsWith("\\\\?\\", StringComparison.Ordinal))
            return fullPath;
        if (fullPath.StartsWith("\\\\", StringComparison.Ordinal))
            return "\\\\?\\UNC\\" + fullPath[2..];
        return "\\\\?\\" + fullPath;
    }

    private static IOException CreateIoException(string operation, string directory)
        => new($"Unable to {operation} '{directory}'.", new Win32Exception(Marshal.GetLastPInvokeError()));
}
