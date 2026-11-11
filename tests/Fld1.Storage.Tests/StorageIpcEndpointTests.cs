using Fld1.Storage.Engine;
using Fld1.Storage.Ipc;
using Xunit;

namespace Fld1.Storage.Tests;

public sealed class StorageIpcEndpointTests
{
    [Fact]
    public void Pipe_name_is_canonical_and_partition_scoped()
    {
        string root = Path.Combine(Path.GetTempPath(), "fld ipc root " + Guid.NewGuid().ToString("N"));
        string first = StorageIpcEndpoint.ComputePipeName(root, 1);
        string same = StorageIpcEndpoint.ComputePipeName(
            Path.Combine(root, "."), 1);
        string otherPartition = StorageIpcEndpoint.ComputePipeName(root, 2);

        Assert.Equal(first, same);
        Assert.NotEqual(first, otherPartition);
        Assert.True(first.StartsWith("fld1-", StringComparison.Ordinal));
    }

    [Fact]
    public void Endpoint_record_is_atomic_readable_and_deleted_only_by_its_owner()
    {
        string root = Path.Combine(Path.GetTempPath(), "fld ipc endpoint " + Guid.NewGuid().ToString("N"));
        Guid firstOwner = Guid.NewGuid();
        Guid secondOwner = Guid.NewGuid();

        try
        {
            StorageIpcEndpointInfo first = StorageIpcEndpoint.Create(
                root, 0x101, firstOwner, Environment.ProcessId);
            StorageIpcEndpoint.Publish(root, first);
            Assert.Equal(first, StorageIpcEndpoint.Read(root, 0x101));

            StorageIpcEndpointInfo second = StorageIpcEndpoint.Create(
                root, 0x101, secondOwner, Environment.ProcessId);
            StorageIpcEndpoint.Publish(root, second);

            Assert.False(StorageIpcEndpoint.DeleteIfOwned(root, firstOwner));
            Assert.Equal(second, StorageIpcEndpoint.Read(root, 0x101));
            Assert.True(StorageIpcEndpoint.DeleteIfOwned(root, secondOwner));
            Assert.Throws<FileNotFoundException>(() => StorageIpcEndpoint.Read(root, 0x101));
        }
        finally
        {
            try { Directory.Delete(root, recursive: true); } catch { }
        }
    }

    [Fact]
    public async Task Client_connects_from_published_endpoint_after_listener_is_ready()
    {
        string root = Path.Combine(Path.GetTempPath(), "fld ipc endpoint connect " + Guid.NewGuid().ToString("N"));
        const ulong partition = 0x203;
        string pipeName = StorageIpcEndpoint.ComputePipeName(root, partition);
        Guid ownerId = Guid.NewGuid();

        try
        {
            using var database = StorageDatabase.Open(root, partition);
            var dispatcher = new StorageIpcOperationDispatcher(database);
            using var serverCancellation = new CancellationTokenSource();
            var server = new StorageIpcServer(pipeName, dispatcher.HandleAsync);
            Task serverTask = server.RunAsync(serverCancellation.Token);

            try
            {
                await server.Ready.WaitAsync(TimeSpan.FromSeconds(10));
                StorageIpcEndpoint.Publish(root, StorageIpcEndpoint.Create(
                    root, partition, ownerId, Environment.ProcessId));

                await using var client = await StorageIpcClientConnection.ConnectToDatabaseAsync(
                    root, partition, Guid.NewGuid(), TimeSpan.FromSeconds(10));
                byte[] request = StorageIpcOperationProtocol.Encode(
                    new StorageIpcOperationEnvelope(
                        Guid.NewGuid(), StorageIpcOperationCodes.Get, "missing-key"u8.ToArray()));
                ReadOnlyMemory<byte> response = await client.RequestAsync(request);
                Assert.Equal(new byte[] { 0, 0, 0, 0, 0 }, response.ToArray());
            }
            finally
            {
                serverCancellation.Cancel();
                await serverTask.WaitAsync(TimeSpan.FromSeconds(10));
            }
        }
        finally
        {
            StorageIpcEndpoint.DeleteIfOwned(root, ownerId);
            try { Directory.Delete(root, recursive: true); } catch { }
        }
    }

    [Fact]
    public void Endpoint_reader_rejects_a_different_partition()
    {
        string root = Path.Combine(Path.GetTempPath(), "fld ipc endpoint partition " + Guid.NewGuid().ToString("N"));
        try
        {
            StorageIpcEndpoint.Publish(root, StorageIpcEndpoint.Create(
                root, 0x201, Guid.NewGuid(), Environment.ProcessId));
            Assert.Throws<InvalidDataException>(() => StorageIpcEndpoint.Read(root, 0x202));
        }
        finally
        {
            try { Directory.Delete(root, recursive: true); } catch { }
        }
    }
}
