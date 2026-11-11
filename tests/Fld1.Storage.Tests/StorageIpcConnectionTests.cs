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

        byte[] first = await client.RequestAsync("first"u8.ToArray());
        Assert.Equal(clientId, new Guid(first.AsSpan(0, 16)));
        Assert.Equal("first"u8.ToArray(), first.AsSpan(16).ToArray());

        StorageIpcRemoteException remote = await Assert.ThrowsAsync<StorageIpcRemoteException>(
            async () => await client.RequestAsync("fail"u8.ToArray()));
        Assert.Equal(2UL, remote.RequestId);
        Assert.Contains("intentional handler failure", remote.Message, StringComparison.Ordinal);

        byte[] third = await client.RequestAsync("third"u8.ToArray());
        Assert.Equal(clientId, new Guid(third.AsSpan(0, 16)));
        Assert.Equal("third"u8.ToArray(), third.AsSpan(16).ToArray());

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

        Assert.Equal(clientAId, new Guid(requestA.Result.Span[..16]));
        Assert.Equal("alpha"u8.ToArray(), requestA.Result.Span[16..].ToArray());
        Assert.Equal(clientBId, new Guid(requestB.Result.Span[..16]));
        Assert.Equal("beta"u8.ToArray(), requestB.Result.Span[16..].ToArray());

        await clientA.DisposeAsync();
        await clientB.DisposeAsync();
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
