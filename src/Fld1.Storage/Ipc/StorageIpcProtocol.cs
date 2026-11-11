using System.Buffers.Binary;

namespace Fld1.Storage.Ipc;

/// <summary>
/// Versioned, bounded frame codec for the local Shared Engine IPC control/data plane.
/// The frame header is fixed-width and little-endian; payloads are opaque to Storage.
/// </summary>
public static class StorageIpcProtocol
{
    public const ushort CurrentVersion = 1;
    public const int HeaderBytes = 32;
    public const int MaxPayloadBytes = 4 * 1024 * 1024;

    private const uint Magic = 0x31534C46; // "FLS1"
    private const uint Crc32CPolynomial = 0x82F63B78;

    public static async ValueTask WriteAsync(
        Stream stream,
        StorageIpcFrame frame,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(stream);
        ValidateFrame(frame);

        byte[] header = new byte[HeaderBytes];
        BinaryPrimitives.WriteUInt32LittleEndian(header, Magic);
        BinaryPrimitives.WriteUInt16LittleEndian(header.AsSpan(4), CurrentVersion);
        BinaryPrimitives.WriteUInt16LittleEndian(header.AsSpan(6), (ushort)frame.Type);
        BinaryPrimitives.WriteUInt64LittleEndian(header.AsSpan(8), frame.RequestId);
        BinaryPrimitives.WriteUInt64LittleEndian(header.AsSpan(16), frame.SessionGeneration);
        BinaryPrimitives.WriteInt32LittleEndian(header.AsSpan(24), frame.Payload.Length);
        uint crc = ComputeCrc32C(header.AsSpan(0, 28), frame.Payload.Span);
        BinaryPrimitives.WriteUInt32LittleEndian(header.AsSpan(28), crc);

        await stream.WriteAsync(header, cancellationToken).ConfigureAwait(false);
        if (!frame.Payload.IsEmpty)
            await stream.WriteAsync(frame.Payload, cancellationToken).ConfigureAwait(false);
        await stream.FlushAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Reads one complete frame. Clean EOF before the first header byte returns null;
    /// partial headers or payloads are protocol truncation errors.
    /// </summary>
    public static async ValueTask<StorageIpcFrame?> ReadAsync(
        Stream stream,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(stream);
        byte[] header = new byte[HeaderBytes];
        int first = await stream.ReadAsync(header.AsMemory(0, 1), cancellationToken).ConfigureAwait(false);
        if (first == 0)
            return null;

        await ReadExactlyAsync(stream, header.AsMemory(1), cancellationToken).ConfigureAwait(false);

        if (BinaryPrimitives.ReadUInt32LittleEndian(header) != Magic)
            throw new InvalidDataException("Storage IPC frame magic is invalid.");
        ushort version = BinaryPrimitives.ReadUInt16LittleEndian(header.AsSpan(4));
        if (version != CurrentVersion)
            throw new InvalidDataException($"Unsupported Storage IPC protocol version {version}.");

        ushort rawType = BinaryPrimitives.ReadUInt16LittleEndian(header.AsSpan(6));
        if (!Enum.IsDefined(typeof(StorageIpcFrameType), rawType))
            throw new InvalidDataException($"Unknown Storage IPC frame type {rawType}.");

        ulong requestId = BinaryPrimitives.ReadUInt64LittleEndian(header.AsSpan(8));
        ulong generation = BinaryPrimitives.ReadUInt64LittleEndian(header.AsSpan(16));
        int payloadLength = BinaryPrimitives.ReadInt32LittleEndian(header.AsSpan(24));
        uint expectedCrc = BinaryPrimitives.ReadUInt32LittleEndian(header.AsSpan(28));
        if (payloadLength < 0 || payloadLength > MaxPayloadBytes)
            throw new InvalidDataException("Storage IPC payload length is outside the protocol limit.");

        byte[] payload = payloadLength == 0 ? Array.Empty<byte>() : new byte[payloadLength];
        if (payloadLength != 0)
            await ReadExactlyAsync(stream, payload, cancellationToken).ConfigureAwait(false);

        uint actualCrc = ComputeCrc32C(header.AsSpan(0, 28), payload);
        if (actualCrc != expectedCrc)
            throw new InvalidDataException("Storage IPC frame CRC32C mismatch.");

        var frame = new StorageIpcFrame(
            (StorageIpcFrameType)rawType, requestId, generation, payload);
        ValidateFrame(frame);
        return frame;
    }

    public static uint ComputeCrc32C(ReadOnlySpan<byte> headerPrefix, ReadOnlySpan<byte> payload)
    {
        uint crc = uint.MaxValue;
        Update(ref crc, headerPrefix);
        Update(ref crc, payload);
        return ~crc;
    }

    private static void Update(ref uint crc, ReadOnlySpan<byte> data)
    {
        foreach (byte value in data)
        {
            crc ^= value;
            for (int bit = 0; bit < 8; bit++)
                crc = (crc >> 1) ^ ((crc & 1) != 0 ? Crc32CPolynomial : 0);
        }
    }

    private static async ValueTask ReadExactlyAsync(
        Stream stream,
        Memory<byte> destination,
        CancellationToken cancellationToken)
    {
        int offset = 0;
        while (offset < destination.Length)
        {
            int read = await stream.ReadAsync(destination[offset..], cancellationToken).ConfigureAwait(false);
            if (read == 0)
                throw new EndOfStreamException("Storage IPC frame ended before its declared length.");
            offset += read;
        }
    }

    private static void ValidateFrame(in StorageIpcFrame frame)
    {
        if (!Enum.IsDefined(frame.Type))
            throw new ArgumentOutOfRangeException(nameof(frame), "Unknown Storage IPC frame type.");
        if (frame.Payload.Length > MaxPayloadBytes)
            throw new ArgumentOutOfRangeException(nameof(frame), "Storage IPC payload exceeds the protocol limit.");

        switch (frame.Type)
        {
            case StorageIpcFrameType.Hello:
                if (frame.RequestId != 0 || frame.SessionGeneration != 0)
                    throw new InvalidDataException("Hello must use request id and session generation zero.");
                break;
            case StorageIpcFrameType.HelloAck:
                if (frame.RequestId != 0 || frame.SessionGeneration == 0)
                    throw new InvalidDataException("HelloAck must assign a nonzero session generation.");
                break;
            case StorageIpcFrameType.Goodbye:
                if (frame.RequestId != 0 || frame.SessionGeneration == 0)
                    throw new InvalidDataException("Goodbye must use request id zero and a live session generation.");
                break;
            default:
                if (frame.RequestId == 0 || frame.SessionGeneration == 0)
                    throw new InvalidDataException("Request/response frames require nonzero request id and session generation.");
                break;
        }
    }
}

/// <summary>Transport-level frame types. Domain opcodes live inside Request payloads.</summary>
public enum StorageIpcFrameType : ushort
{
    Hello = 1,
    HelloAck = 2,
    Request = 3,
    Response = 4,
    Error = 5,
    Goodbye = 6
}

/// <summary>One validated Storage IPC frame; payload ownership belongs to the caller.</summary>
public readonly record struct StorageIpcFrame(
    StorageIpcFrameType Type,
    ulong RequestId,
    ulong SessionGeneration,
    ReadOnlyMemory<byte> Payload);
