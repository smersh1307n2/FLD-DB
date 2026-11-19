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
    public void Stable_operation_envelope_round_trips_with_canonical_guid_byte_order()
    {
        Guid operationId = Guid.Parse("00112233-4455-6677-8899-aabbccddeeff");
        byte[] payload = "write-field:record-1"u8.ToArray();
        byte[] encoded = StorageIpcOperationProtocol.Encode(
            new StorageIpcOperationEnvelope(operationId, 7, payload));

        Assert.Equal(StorageIpcOperationProtocol.HeaderBytes + payload.Length, encoded.Length);
        Assert.Equal(
            new byte[] { 0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77,
                0x88, 0x99, 0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF },
            encoded.AsSpan(8, 16).ToArray());

        StorageIpcOperationEnvelope decoded = StorageIpcOperationProtocol.Decode(encoded);
        Assert.Equal(operationId, decoded.OperationId);
        Assert.Equal(7, decoded.OperationCode);
        Assert.Equal(payload, decoded.Payload.ToArray());
    }

    [Fact]
    public void Stable_operation_envelope_rejects_empty_identity_invalid_code_and_bad_lengths()
    {
        byte[] payload = "payload"u8.ToArray();
        Assert.Throws<ArgumentException>(() =>
            StorageIpcOperationProtocol.Encode(
                new StorageIpcOperationEnvelope(Guid.Empty, 1, payload)));
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            StorageIpcOperationProtocol.Encode(
                new StorageIpcOperationEnvelope(Guid.NewGuid(), 0, payload)));
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            StorageIpcOperationProtocol.Encode(
                new StorageIpcOperationEnvelope(
                    Guid.NewGuid(), 1, new byte[StorageIpcOperationProtocol.MaxPayloadBytes + 1])));

        Assert.Throws<InvalidDataException>(() =>
            StorageIpcOperationProtocol.Decode(new byte[StorageIpcOperationProtocol.HeaderBytes - 1]));

        byte[] encoded = StorageIpcOperationProtocol.Encode(
            new StorageIpcOperationEnvelope(Guid.NewGuid(), 1, payload));
        encoded[0] ^= 0xFF;
        Assert.Throws<InvalidDataException>(() => StorageIpcOperationProtocol.Decode(encoded));
    }

    [Fact]
    public void Stable_operation_envelope_rejects_unknown_version_zero_code_and_trailing_bytes()
    {
        byte[] encoded = StorageIpcOperationProtocol.Encode(
            new StorageIpcOperationEnvelope(Guid.NewGuid(), 1, "x"u8.ToArray()));

        byte[] badVersion = encoded.ToArray();
        badVersion[4] = 2;
        Assert.Throws<InvalidDataException>(() => StorageIpcOperationProtocol.Decode(badVersion));

        byte[] zeroCode = encoded.ToArray();
        zeroCode[6] = 0;
        zeroCode[7] = 0;
        Assert.Throws<InvalidDataException>(() => StorageIpcOperationProtocol.Decode(zeroCode));

        byte[] trailing = encoded.Concat(new byte[] { 0 }).ToArray();
        Assert.Throws<InvalidDataException>(() => StorageIpcOperationProtocol.Decode(trailing));
    }

    [Fact]
    public void Crc32c_matches_the_castagnoli_check_vector()
    {
        Assert.Equal(0xE3069283u, StorageIpcProtocol.ComputeCrc32C(
            ReadOnlySpan<byte>.Empty, "123456789"u8));
    }
}
