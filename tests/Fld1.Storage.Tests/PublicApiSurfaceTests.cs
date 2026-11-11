using Fld1.Storage.Engine;
using Xunit;

namespace Fld1.Storage.Tests;

public sealed class PublicApiSurfaceTests
{
    [Fact]
    public void Storage_assembly_exports_only_the_approved_byte_storage_surface()
    {
        string[] actual = typeof(StorageDatabase).Assembly.GetExportedTypes()
            .Select(type => type.FullName ?? throw new InvalidOperationException("Exported type has no full name."))
            .OrderBy(name => name, StringComparer.Ordinal)
            .ToArray();

        string[] expected =
        {
            "Fld1.Storage.Engine.StorageBlobReader",
            "Fld1.Storage.Engine.StorageBlobStore",
            "Fld1.Storage.Engine.StorageConflictException",
            "Fld1.Storage.Engine.StorageDatabase",
            "Fld1.Storage.Engine.StorageDirectorySync",
            "Fld1.Storage.Engine.StorageEntry",
            "Fld1.Storage.Engine.StorageEntryView",
            "Fld1.Storage.Engine.StorageEntryVisitor",
            "Fld1.Storage.Engine.StoragePartitionLease",
            "Fld1.Storage.Engine.StorageSnapshot",
            "Fld1.Storage.Engine.StorageSnapshot+PrefixCursor",
            "Fld1.Storage.Engine.StorageWriteBatch",
            "Fld1.Storage.Ipc.StorageIpcFrame",
            "Fld1.Storage.Ipc.StorageIpcFrameType",
            "Fld1.Storage.Ipc.StorageIpcProtocol",
            "Fld1.Storage.Ipc.StorageIpcClientConnection",
            "Fld1.Storage.Ipc.StorageIpcRemoteException",
            "Fld1.Storage.Ipc.StorageIpcRequestContext",
            "Fld1.Storage.Ipc.StorageIpcRequestHandler",
            "Fld1.Storage.Ipc.StorageIpcServer"
        };

        Assert.Equal(expected.OrderBy(name => name, StringComparer.Ordinal), actual);
        Assert.All(typeof(StorageDatabase).Assembly.GetExportedTypes(), type =>
            Assert.Contains(type.Namespace, new[] { "Fld1.Storage.Engine", "Fld1.Storage.Ipc" }));
        Assert.DoesNotContain(actual, name =>
            name.Contains(".Wal.", StringComparison.Ordinal) ||
            name.Contains(".Integrity.", StringComparison.Ordinal) ||
            name.Contains("StorageFaultPoint", StringComparison.Ordinal) ||
            name.Contains("StorageMutation", StringComparison.Ordinal) ||
            name.Contains("StorageKey", StringComparison.Ordinal));
    }
}
