using Fld1.Storage.Ipc;
using Xunit;

namespace Fld1.Storage.Tests;

public sealed class StorageIpcProtocolTests
{
    [Fact]
    public async Task Request_frame_round_trips_with_request_and_session_identity()
    {
        byte[] payload = "read-prefix:alpha"u8.ToArray();
        var expected = new StorageIpcFrame(
            StorageIpcFrameType.Request, 42, 7, payload);
        using var stream = new MemoryStream();

        await StorageIpcProtocol.WriteAsync(stream, expected);
        stream.Position = 0;
        StorageIpcFrame? actual = await StorageIpcProtocol.ReadAsync(stream);

        Assert.NotNull(actual);
        Assert.Equal(expected.Type, actual.Value.Type);
        Assert.Equal(expected.RequestId, actual.Value.RequestId);
        Assert.Equal(expected.SessionGeneration, actual.Value.SessionGeneration);
        Assert.Equal(payload, actual.Value.Payload.ToArray());
    }

    [Fact]
    public async Task Hello_and_goodbye_enforce_session_generation_boundaries()
    {
        using var stream = new MemoryStream();
        await StorageIpcProtocol.WriteAsync(stream,
            new StorageIpcFrame(StorageIpcFrameType.Hello, 0, 0, Array.Empty<byte>()));
        await StorageIpcProtocol.WriteAsync(stream,
            new StorageIpcFrame(StorageIpcFrameType.HelloAck, 0, 91, "v1"u8.ToArray()));
        await StorageIpcProtocol.WriteAsync(stream,
            new StorageIpcFrame(StorageIpcFrameType.Goodbye, 0, 91, Array.Empty<byte>()));

        stream.Position = 0;
        StorageIpcFrame? hello = await StorageIpcProtocol.ReadAsync(stream);
        StorageIpcFrame? ack = await StorageIpcProtocol.ReadAsync(stream);
        StorageIpcFrame? goodbye = await StorageIpcProtocol.ReadAsync(stream);

        Assert.Equal(StorageIpcFrameType.Hello, hello?.Type);
        Assert.Equal(0UL, hello?.SessionGeneration);
        Assert.Equal(StorageIpcFrameType.HelloAck, ack?.Type);
        Assert.Equal(91UL, ack?.SessionGeneration);
        Assert.Equal(StorageIpcFrameType.Goodbye, goodbye?.Type);
        Assert.Null(await StorageIpcProtocol.ReadAsync(stream));
    }

    [Fact]
    public async Task Payload_corruption_is_rejected_by_crc32c()
    {
        using var stream = new MemoryStream();
        await StorageIpcProtocol.WriteAsync(stream,
            new StorageIpcFrame(StorageIpcFrameType.Response, 3, 5, "durable-result"u8.ToArray()));
        byte[] bytes = stream.ToArray();
        bytes[^1] ^= 0x40;
        using var corrupted = new MemoryStream(bytes);

        await Assert.ThrowsAsync<InvalidDataException>(
            async () => await StorageIpcProtocol.ReadAsync(corrupted));
    }

    [Fact]
    public async Task Truncated_payload_is_rejected_instead_of_returning_a_partial_frame()
    {
        using var stream = new MemoryStream();
        await StorageIpcProtocol.WriteAsync(stream,
            new StorageIpcFrame(StorageIpcFrameType.Request, 9, 11, "payload"u8.ToArray()));
        byte[] bytes = stream.ToArray();
        using var truncated = new MemoryStream(bytes[..^1]);

        await Assert.ThrowsAsync<EndOfStreamException>(
            async () => await StorageIpcProtocol.ReadAsync(truncated));
    }

    [Fact]
    public async Task Invalid_session_identity_and_oversized_payload_are_rejected_before_write()
    {
        using var stream = new MemoryStream();
        await Assert.ThrowsAsync<InvalidDataException>(
            async () => await StorageIpcProtocol.WriteAsync(stream,
                new StorageIpcFrame(StorageIpcFrameType.Request, 1, 0, Array.Empty<byte>())));

        byte[] oversized = new byte[StorageIpcProtocol.MaxPayloadBytes + 1];
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(
            async () => await StorageIpcProtocol.WriteAsync(stream,
                new StorageIpcFrame(StorageIpcFrameType.Request, 1, 1, oversized)));
    }

    [Fact]
    public void Crc32c_matches_the_castagnoli_check_vector()
    {
        Assert.Equal(0xE3069283u, StorageIpcProtocol.ComputeCrc32C(
            ReadOnlySpan<byte>.Empty, "123456789"u8));
    }
}
