using Fld1.Storage.Ipc;
using Xunit;

namespace Fld1.Storage.Tests;

public sealed class StorageIpcConnectionTests
{
    [Fact]
    public async Task Local_pipe_client_server_round_trip_keeps_session_usable_after_remote_error()
    {
        string pipeName = "fld1-ipc-" + Guid.NewGuid().ToString("N");
        Guid clientId = Guid.NewGuid();
        using var serverCancellation = new CancellationTokenSource();
        var server = new StorageIpcServer(pipeName, EchoClientAndPayload);
        Task serverTask = server.RunAsync(serverCancellation.Token);

        await using var client = await StorageIpcClientConnection.ConnectAsync(
            pipeName, clientId, TimeSpan.FromSeconds(10));
        Assert.NotEqual(0UL, client.SessionGeneration);

        ReadOnlyMemory<byte> first = await client.RequestAsync("first"u8.ToArray());
        Assert.Equal(clientId, new Guid(first.Span[..16]));
        Assert.Equal("first"u8.ToArray(), first.Span[16..].ToArray());

        StorageIpcRemoteException remote = await Assert.ThrowsAsync<StorageIpcRemoteException>(
            async () => await client.RequestAsync("fail"u8.ToArray()));
        Assert.Equal(2UL, remote.RequestId);
        Assert.Contains("intentional handler failure", remote.Message, StringComparison.Ordinal);

        ReadOnlyMemory<byte> third = await client.RequestAsync("third"u8.ToArray());
        Assert.Equal(clientId, new Guid(third.Span[..16]));
        Assert.Equal("third"u8.ToArray(), third.Span[16..].ToArray());

        await client.DisposeAsync();
        serverCancellation.Cancel();
        await serverTask.WaitAsync(TimeSpan.FromSeconds(10));
    }

    [Fact]
    public async Task Local_pipe_server_isolates_multiple_client_sessions()
    {
        string pipeName = "fld1-ipc-" + Guid.NewGuid().ToString("N");
        Guid clientAId = Guid.NewGuid();
        Guid clientBId = Guid.NewGuid();
        using var serverCancellation = new CancellationTokenSource();
        var server = new StorageIpcServer(pipeName, EchoClientAndPayload);
        Task serverTask = server.RunAsync(serverCancellation.Token);

        await using var clientA = await StorageIpcClientConnection.ConnectAsync(
            pipeName, clientAId, TimeSpan.FromSeconds(10));
        await using var clientB = await StorageIpcClientConnection.ConnectAsync(
            pipeName, clientBId, TimeSpan.FromSeconds(10));
        Assert.NotEqual(clientA.SessionGeneration, clientB.SessionGeneration);

        Task<ReadOnlyMemory<byte>> requestA = clientA.RequestAsync("alpha"u8.ToArray()).AsTask();
        Task<ReadOnlyMemory<byte>> requestB = clientB.RequestAsync("beta"u8.ToArray()).AsTask();
        await Task.WhenAll(requestA, requestB).WaitAsync(TimeSpan.FromSeconds(10));
        ReadOnlyMemory<byte> resultA = await requestA;
        ReadOnlyMemory<byte> resultB = await requestB;

        Assert.Equal(clientAId, new Guid(resultA.Span[..16]));
        Assert.Equal("alpha"u8.ToArray(), resultA.Span[16..].ToArray());
        Assert.Equal(clientBId, new Guid(resultB.Span[..16]));
        Assert.Equal("beta"u8.ToArray(), resultB.Span[16..].ToArray());

        await clientA.DisposeAsync();
        await clientB.DisposeAsync();
        serverCancellation.Cancel();
        await serverTask.WaitAsync(TimeSpan.FromSeconds(10));
    }

    [Fact]
    public async Task Local_pipe_server_rejects_clients_above_configured_admission_limit()
    {
        string pipeName = "fld1-ipc-" + Guid.NewGuid().ToString("N");
        using var serverCancellation = new CancellationTokenSource();
        var server = new StorageIpcServer(pipeName, EchoClientAndPayload, maxConcurrentClients: 1);
        Task serverTask = server.RunAsync(serverCancellation.Token);

        await using var admittedClient = await StorageIpcClientConnection.ConnectAsync(
            pipeName, Guid.NewGuid(), TimeSpan.FromSeconds(10));

        Exception? rejection = await Record.ExceptionAsync(async () =>
            await StorageIpcClientConnection.ConnectAsync(
                pipeName, Guid.NewGuid(), TimeSpan.FromSeconds(10)));
        Assert.NotNull(rejection);
        Assert.True(rejection is IOException or InvalidDataException,
            $"Expected a transport-level rejection, received {rejection.GetType().Name}.");

        ReadOnlyMemory<byte> response = await admittedClient.RequestAsync("still-usable"u8.ToArray());
        Assert.Equal("still-usable"u8.ToArray(), response.Span[16..].ToArray());

        await admittedClient.DisposeAsync();
        StorageIpcClientConnection? replacementClient = null;
        for (int attempt = 0; attempt < 50 && replacementClient is null; attempt++)
        {
            try
            {
                replacementClient = await StorageIpcClientConnection.ConnectAsync(
                    pipeName, Guid.NewGuid(), TimeSpan.FromSeconds(2));
            }
            catch (Exception exception) when (exception is IOException or InvalidDataException)
            {
                await Task.Delay(20);
            }
        }

        Assert.NotNull(replacementClient);
        await using (replacementClient)
        {
            ReadOnlyMemory<byte> replacementResponse =
                await replacementClient.RequestAsync("slot-reused"u8.ToArray());
            Assert.Equal("slot-reused"u8.ToArray(), replacementResponse.Span[16..].ToArray());
        }

        serverCancellation.Cancel();
        await serverTask.WaitAsync(TimeSpan.FromSeconds(10));
    }

    private static ValueTask<ReadOnlyMemory<byte>> EchoClientAndPayload(
        StorageIpcRequestContext context,
        ReadOnlyMemory<byte> payload,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (payload.Span.SequenceEqual("fail"u8))
            throw new InvalidOperationException("intentional handler failure");

        byte[] response = new byte[16 + payload.Length];
        context.ClientId.TryWriteBytes(response);
        payload.Span.CopyTo(response.AsSpan(16));
        return ValueTask.FromResult<ReadOnlyMemory<byte>>(response);
    }
}
