using System.Buffers.Binary;

namespace Fld1.Wal.Codec;

/// <summary>
/// Durable acknowledgement that a client has persisted all operation results through
/// a contiguous client sequence. It retires only local IPC receipts, never field data.
/// </summary>
public sealed class WalLocalOperationAck
{
    public required ushort NamespaceId { get; init; }
    public required byte[] ClientId { get; init; }
    public required ulong AcknowledgedThroughSequence { get; init; }
}

public static class WalLocalOperationAckCodec
{
    private const uint Version = 1;
    private const int EncodedLength = sizeof(uint) + sizeof(ushort) + 16 + sizeof(ulong);

    public static byte[] Encode(WalLocalOperationAck ack)
    {
        ArgumentNullException.ThrowIfNull(ack);
        Validate(ack);
        byte[] encoded = new byte[EncodedLength];
        BinaryPrimitives.WriteUInt32LittleEndian(encoded, Version);
        BinaryPrimitives.WriteUInt16LittleEndian(encoded.AsSpan(4), ack.NamespaceId);
        ack.ClientId.CopyTo(encoded, 6);
        BinaryPrimitives.WriteUInt64LittleEndian(encoded.AsSpan(22), ack.AcknowledgedThroughSequence);
        return encoded;
    }

    public static WalLocalOperationAck Decode(ReadOnlySpan<byte> payload)
    {
        if (payload.Length != EncodedLength)
            throw new InvalidDataException("Local-operation acknowledgement payload length is invalid.");
        uint version = BinaryPrimitives.ReadUInt32LittleEndian(payload);
        if (version != Version)
            throw new InvalidDataException($"Unsupported local-operation acknowledgement version {version}.");
        var ack = new WalLocalOperationAck
        {
            NamespaceId = BinaryPrimitives.ReadUInt16LittleEndian(payload[4..]),
            ClientId = payload.Slice(6, 16).ToArray(),
            AcknowledgedThroughSequence = BinaryPrimitives.ReadUInt64LittleEndian(payload[22..])
        };
        try { Validate(ack); }
        catch (ArgumentException exception)
        {
            throw new InvalidDataException("Local-operation acknowledgement identity is invalid.", exception);
        }
        return ack;
    }

    private static void Validate(WalLocalOperationAck ack)
    {
        ArgumentNullException.ThrowIfNull(ack.ClientId);
        if (ack.ClientId.Length != 16 || IsZero(ack.ClientId))
            throw new ArgumentException("Acknowledgement ClientId must be nonzero and 16 bytes.", nameof(ack));
        if (ack.AcknowledgedThroughSequence == 0)
            throw new ArgumentOutOfRangeException(nameof(ack), "Acknowledgement sequence must be nonzero.");
    }

    private static bool IsZero(ReadOnlySpan<byte> value)
    {
        byte aggregate = 0;
        foreach (byte part in value) aggregate |= part;
        return aggregate == 0;
    }
}
