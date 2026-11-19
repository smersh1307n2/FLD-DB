using System.Buffers.Binary;

namespace Fld1.Storage.Ipc;

/// <summary>
/// Stable application-operation envelope carried inside an IPC Request frame.
/// OperationId is generated and durably retained by the client before its first
/// send, then reused unchanged for retries across connection/session generations.
/// Storage does not interpret OperationCode or Payload.
/// </summary>
public readonly record struct StorageIpcOperationEnvelope(
    Guid OperationId,
    ushort OperationCode,
    ReadOnlyMemory<byte> Payload);

/// <summary>
/// Canonical codec for the stable operation identity envelope. The outer IPC frame
/// supplies transport CRC32C; the host computes a canonical request fingerprint
/// over OperationCode and Payload for its durable idempotency ledger.
/// </summary>
public static class StorageIpcOperationProtocol
{
    public const ushort CurrentVersion = 1;
    public const int HeaderBytes = 28;
    public const int MaxPayloadBytes = StorageIpcProtocol.MaxPayloadBytes - HeaderBytes;

    private const uint Magic = 0x31504F53; // "SOP1"

    public static byte[] Encode(in StorageIpcOperationEnvelope envelope)
    {
        Validate(envelope);

        byte[] encoded = new byte[checked(HeaderBytes + envelope.Payload.Length)];
        Span<byte> header = encoded.AsSpan(0, HeaderBytes);
        BinaryPrimitives.WriteUInt32LittleEndian(header, Magic);
        BinaryPrimitives.WriteUInt16LittleEndian(header[4..], CurrentVersion);
        BinaryPrimitives.WriteUInt16LittleEndian(header[6..], envelope.OperationCode);
        if (!envelope.OperationId.TryWriteBytes(header.Slice(8, 16), bigEndian: true, out int written) ||
            written != 16)
            throw new InvalidOperationException("Could not encode the stable IPC OperationId.");

        BinaryPrimitives.WriteInt32LittleEndian(header[24..], envelope.Payload.Length);
        envelope.Payload.Span.CopyTo(encoded.AsSpan(HeaderBytes));
        return encoded;
    }

    /// <summary>
    /// Decodes an envelope without copying its payload. The returned payload memory
    /// references the supplied input buffer, which must remain alive and unchanged.
    /// </summary>
    public static StorageIpcOperationEnvelope Decode(ReadOnlyMemory<byte> encoded)
    {
        if (encoded.Length < HeaderBytes)
            throw new InvalidDataException("Storage IPC operation envelope is truncated.");

        ReadOnlySpan<byte> header = encoded.Span[..HeaderBytes];
        if (BinaryPrimitives.ReadUInt32LittleEndian(header) != Magic)
            throw new InvalidDataException("Storage IPC operation envelope magic is invalid.");

        ushort version = BinaryPrimitives.ReadUInt16LittleEndian(header[4..]);
        if (version != CurrentVersion)
            throw new InvalidDataException($"Unsupported Storage IPC operation version {version}.");

        ushort operationCode = BinaryPrimitives.ReadUInt16LittleEndian(header[6..]);
        Guid operationId = new(header.Slice(8, 16), bigEndian: true);
        int payloadLength = BinaryPrimitives.ReadInt32LittleEndian(header[24..]);
        if (operationCode == 0 || operationId == Guid.Empty ||
            payloadLength < 0 || payloadLength > MaxPayloadBytes ||
            encoded.Length != HeaderBytes + payloadLength)
            throw new InvalidDataException("Storage IPC operation identity or payload length is invalid.");

        return new StorageIpcOperationEnvelope(
            operationId,
            operationCode,
            encoded[HeaderBytes..]);
    }

    private static void Validate(in StorageIpcOperationEnvelope envelope)
    {
        if (envelope.OperationId == Guid.Empty)
            throw new ArgumentException("Stable IPC OperationId must be non-empty.", nameof(envelope));
        if (envelope.OperationCode == 0)
            throw new ArgumentOutOfRangeException(nameof(envelope), "Operation code zero is reserved.");
        if (envelope.Payload.Length > MaxPayloadBytes)
            throw new ArgumentOutOfRangeException(nameof(envelope), "Operation payload exceeds the IPC frame limit.");
    }
}
