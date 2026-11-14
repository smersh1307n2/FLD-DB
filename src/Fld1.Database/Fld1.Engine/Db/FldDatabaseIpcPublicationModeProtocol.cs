using System.Buffers.Binary;

namespace Fld1.Engine.Db;

public static class FldDatabaseIpcPublicationModeProtocol
{
    public static byte[] EncodeTryGetResult(bool exists, ulong nodeOutboxHighWatermark)
    {
        byte[] payload = new byte[9];
        payload[0] = exists ? (byte)1 : (byte)0;
        BinaryPrimitives.WriteUInt64LittleEndian(payload.AsSpan(1), exists ? nodeOutboxHighWatermark : 0);
        return payload;
    }

    public static (bool Exists, ulong NodeOutboxHighWatermark) DecodeTryGetResult(ReadOnlyMemory<byte> payload)
    {
        if (payload.Length != 9 || payload.Span[0] > 1)
            throw new InvalidDataException("FLD publication cutover response is invalid.");
        ulong watermark = BinaryPrimitives.ReadUInt64LittleEndian(payload.Span.Slice(1, 8));
        bool exists = payload.Span[0] == 1;
        if (!exists && watermark != 0)
            throw new InvalidDataException("Missing FLD publication cutover must have a zero watermark.");
        return (exists, watermark);
    }

    public static byte[] EncodeEnableRequest(ulong nodeOutboxHighWatermark)
    {
        byte[] payload = new byte[8];
        BinaryPrimitives.WriteUInt64LittleEndian(payload, nodeOutboxHighWatermark);
        return payload;
    }

    public static ulong DecodeEnableRequest(ReadOnlyMemory<byte> payload)
    {
        if (payload.Length != 8)
            throw new InvalidDataException("FLD publication cutover request must be exactly eight bytes.");
        return BinaryPrimitives.ReadUInt64LittleEndian(payload.Span);
    }
}
