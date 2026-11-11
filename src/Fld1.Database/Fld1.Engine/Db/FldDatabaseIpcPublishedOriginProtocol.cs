using System.Buffers.Binary;

namespace Fld1.Engine.Db;

/// <summary>Versioned bounded codec for immutable provider publication-origin stamps.</summary>
public static class FldDatabaseIpcPublishedOriginProtocol
{
    private const byte Version = 1;
    private const int HeaderBytes = 5;
    private const int EntryBytes = 46;
    private const int MaximumEntries = 4096;

    public static byte[] EncodeRequest(IReadOnlyList<FldPublishedOrigin> origins)
    {
        ArgumentNullException.ThrowIfNull(origins);
        if (origins.Count > MaximumEntries)
            throw new ArgumentOutOfRangeException(nameof(origins));
        int length = checked(HeaderBytes + origins.Count * EntryBytes);
        if (length > Fld1.Storage.Ipc.StorageIpcProtocol.MaxPayloadBytes)
            throw new ArgumentOutOfRangeException(nameof(origins));
        byte[] payload = new byte[length];
        payload[0] = Version;
        BinaryPrimitives.WriteUInt32LittleEndian(payload.AsSpan(1), (uint)origins.Count);
        int offset = HeaderBytes;
        foreach (FldPublishedOrigin origin in origins)
        {
            ArgumentNullException.ThrowIfNull(origin);
            if (origin.RecordId is not { Length: 16 } ||
                origin.ChangeId is not { Length: 16 } ||
                IsZero(origin.ChangeId) ||
                origin.OriginAuthorEpoch == 0 || origin.OriginSequence == 0)
                throw new ArgumentException("Published-origin entry is not canonical.", nameof(origins));
            origin.RecordId.CopyTo(payload, offset);
            BinaryPrimitives.WriteUInt16LittleEndian(payload.AsSpan(offset + 16), origin.FieldId);
            origin.ChangeId.CopyTo(payload, offset + 18);
            BinaryPrimitives.WriteUInt32LittleEndian(payload.AsSpan(offset + 34), origin.OriginAuthorEpoch);
            BinaryPrimitives.WriteUInt64LittleEndian(payload.AsSpan(offset + 38), origin.OriginSequence);
            offset += EntryBytes;
        }
        return payload;
    }

    public static IReadOnlyList<FldPublishedOrigin> DecodeRequest(ReadOnlyMemory<byte> payload)
    {
        if (payload.Length < HeaderBytes || payload.Span[0] != Version)
            throw new InvalidDataException("Published-origin request version/header is invalid.");
        uint count = BinaryPrimitives.ReadUInt32LittleEndian(payload.Span[1..]);
        if (count > MaximumEntries ||
            payload.Length != checked(HeaderBytes + checked((int)count) * EntryBytes))
            throw new InvalidDataException("Published-origin request count/length is invalid.");
        var result = new FldPublishedOrigin[checked((int)count)];
        int offset = HeaderBytes;
        for (int i = 0; i < result.Length; i++)
        {
            byte[] recordId = payload.Slice(offset, 16).ToArray();
            ushort fieldId = BinaryPrimitives.ReadUInt16LittleEndian(payload.Span[(offset + 16)..]);
            byte[] changeId = payload.Slice(offset + 18, 16).ToArray();
            uint epoch = BinaryPrimitives.ReadUInt32LittleEndian(payload.Span[(offset + 34)..]);
            ulong sequence = BinaryPrimitives.ReadUInt64LittleEndian(payload.Span[(offset + 38)..]);
            if (IsZero(changeId) || epoch == 0 || sequence == 0)
                throw new InvalidDataException("Published-origin request contains a zero identity/coordinate.");
            result[i] = new FldPublishedOrigin
            {
                RecordId = recordId,
                FieldId = fieldId,
                ChangeId = changeId,
                OriginAuthorEpoch = epoch,
                OriginSequence = sequence
            };
            offset += EntryBytes;
        }
        return result;
    }

    public static byte[] EncodeResult(int committedCount)
    {
        if (committedCount < 0 || committedCount > MaximumEntries)
            throw new ArgumentOutOfRangeException(nameof(committedCount));
        byte[] response = new byte[4];
        BinaryPrimitives.WriteUInt32LittleEndian(response, (uint)committedCount);
        return response;
    }

    public static int DecodeResult(ReadOnlyMemory<byte> response)
    {
        if (response.Length != 4)
            throw new InvalidDataException("Published-origin response length is invalid.");
        uint count = BinaryPrimitives.ReadUInt32LittleEndian(response.Span);
        if (count > MaximumEntries)
            throw new InvalidDataException("Published-origin response count is invalid.");
        return checked((int)count);
    }

    private static bool IsZero(ReadOnlySpan<byte> value)
    {
        byte aggregate = 0;
        foreach (byte part in value) aggregate |= part;
        return aggregate == 0;
    }
}
