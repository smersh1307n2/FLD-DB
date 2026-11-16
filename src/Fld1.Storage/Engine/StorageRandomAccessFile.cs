using Microsoft.Win32.SafeHandles;
using System.Buffers;

namespace Fld1.Storage.Engine;

internal interface IStorageRandomAccessFile : IDisposable
{
    long Length { get; }
    void ReadExactly(long offset, Span<byte> destination);
    void WriteExactly(long offset, ReadOnlySpan<byte> source);
    void FlushToDisk();
}

/// <summary>
/// Synchronous positional file access for immutable page generations. Operations do not
/// mutate a shared stream position and therefore can be used by concurrent readers.
/// </summary>
internal sealed class StorageRandomAccessFile : IStorageRandomAccessFile
{
    private readonly SafeFileHandle _handle;
    private bool _disposed;

    public StorageRandomAccessFile(
        string path,
        FileMode mode,
        FileAccess access = FileAccess.ReadWrite,
        FileShare share = FileShare.Read,
        FileOptions options = FileOptions.RandomAccess)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(path);
        _handle = File.OpenHandle(path, mode, access, share, options | FileOptions.RandomAccess);
    }

    public long Length
    {
        get
        {
            ThrowIfDisposed();
            return RandomAccess.GetLength(_handle);
        }
    }

    public void ReadExactly(long offset, Span<byte> destination)
    {
        ThrowIfDisposed();
        ValidateRange(offset, destination.Length);
        int totalRead = 0;
        while (totalRead < destination.Length)
        {
            int read = RandomAccess.Read(_handle, destination[totalRead..], checked(offset + totalRead));
            if (read == 0)
                throw new EndOfStreamException("Unexpected end of Storage page file.");
            totalRead += read;
        }
    }

    public void WriteExactly(long offset, ReadOnlySpan<byte> source)
    {
        ThrowIfDisposed();
        ValidateRange(offset, source.Length);
        RandomAccess.Write(_handle, source, offset);
    }

    public void FlushToDisk()
    {
        ThrowIfDisposed();
        RandomAccess.FlushToDisk(_handle);
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        _handle.Dispose();
    }

    private void ThrowIfDisposed()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
    }

    private static void ValidateRange(long offset, int length)
    {
        if (offset < 0) throw new ArgumentOutOfRangeException(nameof(offset));
        _ = checked(offset + length);
    }
}
