using System.Buffers;
using System.IO.MemoryMappedFiles;
using System.Runtime.InteropServices;

namespace Fld1.Run.Reader;

/// <summary>
/// Read-only, demand-paged view of an immutable Run file. The owner must keep this
/// object alive while any RunReader/ReadOnlyMemory slice obtained from it is in use.
/// FldDatabase therefore retires replaced mappings and releases them only on Dispose.
/// </summary>
internal sealed unsafe class MappedRunFile : MemoryManager<byte>, IRunMemoryMapping
{
    private readonly MemoryMappedFile _mapping;
    private readonly MemoryMappedViewAccessor _view;
    private byte* _pointer;
    private readonly int _length;
    private bool _disposed;

    private MappedRunFile(MemoryMappedFile mapping, MemoryMappedViewAccessor view, int length)
    {
        _mapping = mapping;
        _view = view;
        _length = length;
        byte* pointer = null;
        view.SafeMemoryMappedViewHandle.AcquirePointer(ref pointer);
        _pointer = pointer + view.PointerOffset;
    }

    public static MappedRunFile Open(string path)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(path);
        var stream = new FileStream(path, FileMode.Open, FileAccess.Read,
            FileShare.ReadWrite | FileShare.Delete, bufferSize: 1, FileOptions.RandomAccess);
        try
        {
            if (stream.Length <= 0 || stream.Length > int.MaxValue)
                throw new InvalidDataException("Run file length is outside the supported mapped-memory range.");
            MemoryMappedFile mapping = MemoryMappedFile.CreateFromFile(
                stream, mapName: null, capacity: 0, MemoryMappedFileAccess.Read,
                HandleInheritability.None, leaveOpen: false);
            try
            {
                MemoryMappedViewAccessor view = mapping.CreateViewAccessor(
                    0, stream.Length, MemoryMappedFileAccess.Read);
                try
                {
                    return new MappedRunFile(mapping, view, checked((int)stream.Length));
                }
                catch
                {
                    view.Dispose();
                    throw;
                }
            }
            catch
            {
                mapping.Dispose();
                throw;
            }
        }
        catch
        {
            stream.Dispose();
            throw;
        }
    }

    public ReadOnlyMemory<byte> ReadOnlyMemory
    {
        get
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            return CreateMemory(_length);
        }
    }

    public override Span<byte> GetSpan()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        return new Span<byte>(_pointer, _length);
    }

    public override MemoryHandle Pin(int elementIndex = 0)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if ((uint)elementIndex > (uint)_length)
            throw new ArgumentOutOfRangeException(nameof(elementIndex));
        return new MemoryHandle(_pointer + elementIndex, default, this);
    }

    public override void Unpin() { }

    public void Close() => ((IDisposable)this).Dispose();

    protected override void Dispose(bool disposing)
    {
        if (_disposed)
            return;
        _disposed = true;
        if (_pointer != null)
        {
            _view.SafeMemoryMappedViewHandle.ReleasePointer();
            _pointer = null;
        }
        _view.Dispose();
        _mapping.Dispose();
    }
}
