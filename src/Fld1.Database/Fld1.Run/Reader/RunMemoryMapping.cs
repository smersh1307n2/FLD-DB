using Fld1.Storage.Engine;

namespace Fld1.Run.Reader;

internal interface IRunMemoryMapping
{
    ReadOnlyMemory<byte> ReadOnlyMemory { get; }
    void Close();
}

/// <summary>Adapts a Storage-owned mapped immutable blob to the FLD Run reader lifetime.</summary>
internal sealed class StorageBlobRunMapping : IRunMemoryMapping
{
    private StorageBlobMappedView? _view;

    public StorageBlobRunMapping(StorageBlobMappedView view)
        => _view = view ?? throw new ArgumentNullException(nameof(view));

    public ReadOnlyMemory<byte> ReadOnlyMemory
        => (_view ?? throw new ObjectDisposedException(nameof(StorageBlobRunMapping))).ReadOnlyMemory;

    public void Close()
    {
        StorageBlobMappedView? view = Interlocked.Exchange(ref _view, null);
        view?.Close();
    }
}
