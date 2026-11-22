using System.Buffers.Binary;

namespace Fld1.Engine.Db;

public enum FldDatabaseIpcGcReferenceKind : byte
{
    ChangeId = 1,
    Record = 2,
    Receipt = 3
}

public sealed record FldDatabaseIpcGcReferenceScan(
    Guid Token,
    FldDraftHorizon UnpublishedLocalDraftHorizon,
    bool HasSnapshotPin,
    ushort NamespaceId);

public sealed record FldDatabaseIpcGcReferenceQuery(
    FldDatabaseIpcGcReferenceKind Kind,
    Guid Token,
    byte[] FirstId,
    byte[]? SecondId = null,
    uint AuthorEpoch = 0,
    uint TransportPeerEpoch = 0);

/// <summary>Bounded codecs for the session-owned Host GC reference snapshot.</summary>
public static class FldDatabaseIpcGcProtocol
{
    private const int ScanResponseBytes = 39;
    private const int QueryHeaderBytes = 17;

    public static byte[] EncodeScanResponse(FldDatabaseIpcGcReferenceScan scan)
    {
        ArgumentNullException.ThrowIfNull(scan);
        if (scan.Token == Guid.Empty)
            throw new ArgumentException("GC reference-scan token must be nonzero.", nameof(scan));
        FldDraftHorizon horizon = scan.UnpublishedLocalDraftHorizon;
        if (horizon.Count == 0 && (horizon.MaxHlcWall != 0 || horizon.MaxHlcLogical != 0))
            throw new ArgumentException("An empty draft horizon cannot carry an HLC maximum.", nameof(scan));

        byte[] payload = new byte[ScanResponseBytes];
        scan.Token.TryWriteBytes(payload.AsSpan(0, 16), bigEndian: true, out _);
        BinaryPrimitives.WriteUInt64LittleEndian(payload.AsSpan(16), horizon.Count);
        BinaryPrimitives.WriteUInt64LittleEndian(payload.AsSpan(24), horizon.MaxHlcWall);
        BinaryPrimitives.WriteUInt32LittleEndian(payload.AsSpan(32), horizon.MaxHlcLogical);
        payload[36] = scan.HasSnapshotPin ? (byte)1 : (byte)0;
        if (scan.NamespaceId == 0)
            throw new ArgumentOutOfRangeException(nameof(scan), "GC reference-scan namespace must be nonzero.");
        BinaryPrimitives.WriteUInt16LittleEndian(payload.AsSpan(37), scan.NamespaceId);
        return payload;
    }

    public static FldDatabaseIpcGcReferenceScan DecodeScanResponse(ReadOnlyMemory<byte> payload)
    {
        if (payload.Length != ScanResponseBytes || payload.Span[36] > 1 ||
            BinaryPrimitives.ReadUInt16LittleEndian(payload.Span[37..]) == 0)
            throw new InvalidDataException("FLD GC reference-scan response is invalid.");
        Guid token = new(payload.Span[..16], bigEndian: true);
        if (token == Guid.Empty)
            throw new InvalidDataException("FLD GC reference-scan token must be nonzero.");
        var horizon = new FldDraftHorizon(
            BinaryPrimitives.ReadUInt64LittleEndian(payload.Span[16..]),
            BinaryPrimitives.ReadUInt64LittleEndian(payload.Span[24..]),
            BinaryPrimitives.ReadUInt32LittleEndian(payload.Span[32..]));
        if (horizon.Count == 0 && (horizon.MaxHlcWall != 0 || horizon.MaxHlcLogical != 0))
            throw new InvalidDataException("FLD GC reference-scan draft horizon is inconsistent.");
        return new FldDatabaseIpcGcReferenceScan(
            token, horizon, payload.Span[36] != 0,
            BinaryPrimitives.ReadUInt16LittleEndian(payload.Span[37..]));
    }

    public static byte[] EncodeChangeIdQuery(Guid token, ReadOnlySpan<byte> changeId)
        => EncodeQuery(FldDatabaseIpcGcReferenceKind.ChangeId, token, changeId, ReadOnlySpan<byte>.Empty, 0, 0);

    public static byte[] EncodeRecordQuery(
        Guid token, ReadOnlySpan<byte> recordId, ReadOnlySpan<byte> tombstoneChangeId)
        => EncodeQuery(FldDatabaseIpcGcReferenceKind.Record, token, recordId, tombstoneChangeId, 0, 0);

    public static byte[] EncodeReceiptQuery(
        Guid token, ReadOnlySpan<byte> senderPeer, uint authorEpoch,
        uint transportPeerEpoch, ReadOnlySpan<byte> batchId)
        => EncodeQuery(FldDatabaseIpcGcReferenceKind.Receipt, token, senderPeer, batchId,
            authorEpoch, transportPeerEpoch);

    private static byte[] EncodeQuery(
        FldDatabaseIpcGcReferenceKind kind, Guid token,
        ReadOnlySpan<byte> first, ReadOnlySpan<byte> second,
        uint authorEpoch, uint transportPeerEpoch)
    {
        if (token == Guid.Empty || first.Length != 16 || first.IndexOfAnyExcept((byte)0) < 0)
            throw new ArgumentException("GC reference query identities must be nonzero 16-byte values.");
        int extra = kind == FldDatabaseIpcGcReferenceKind.ChangeId ? 0 : 16;
        if ((kind == FldDatabaseIpcGcReferenceKind.ChangeId && !second.IsEmpty) ||
            (kind != FldDatabaseIpcGcReferenceKind.ChangeId &&
             (second.Length != 16 || second.IndexOfAnyExcept((byte)0) < 0)))
            throw new ArgumentException("GC reference query has an invalid second identity.");
        if (kind == FldDatabaseIpcGcReferenceKind.Receipt && authorEpoch == 0)
            throw new ArgumentOutOfRangeException(nameof(authorEpoch));

        byte[] payload = new byte[QueryHeaderBytes + 16 + extra +
            (kind == FldDatabaseIpcGcReferenceKind.Receipt ? 8 : 0)];
        payload[0] = (byte)kind;
        token.TryWriteBytes(payload.AsSpan(1, 16), bigEndian: true, out _);
        first.CopyTo(payload.AsSpan(QueryHeaderBytes, 16));
        if (extra != 0)
            second.CopyTo(payload.AsSpan(QueryHeaderBytes + 16, 16));
        if (kind == FldDatabaseIpcGcReferenceKind.Receipt)
        {
            BinaryPrimitives.WriteUInt32LittleEndian(payload.AsSpan(QueryHeaderBytes + 32), authorEpoch);
            BinaryPrimitives.WriteUInt32LittleEndian(payload.AsSpan(QueryHeaderBytes + 36), transportPeerEpoch);
        }
        return payload;
    }

    public static FldDatabaseIpcGcReferenceQuery DecodeQuery(ReadOnlyMemory<byte> payload)
    {
        if (payload.Length < QueryHeaderBytes)
            throw new InvalidDataException("FLD GC reference query is truncated.");
        var kind = (FldDatabaseIpcGcReferenceKind)payload.Span[0];
        Guid token = new(payload.Span.Slice(1, 16), bigEndian: true);
        if (token == Guid.Empty)
            throw new InvalidDataException("FLD GC reference query token must be nonzero.");
        int expected = kind switch
        {
            FldDatabaseIpcGcReferenceKind.ChangeId => 33,
            FldDatabaseIpcGcReferenceKind.Record => 49,
            FldDatabaseIpcGcReferenceKind.Receipt => 57,
            _ => throw new InvalidDataException("FLD GC reference query kind is invalid.")
        };
        if (payload.Length != expected)
            throw new InvalidDataException("FLD GC reference query length is invalid.");
        byte[] first = payload.Slice(QueryHeaderBytes, 16).ToArray();
        if (first.All(value => value == 0))
            throw new InvalidDataException("FLD GC reference query identity must be nonzero.");
        byte[]? second = kind == FldDatabaseIpcGcReferenceKind.ChangeId
            ? null
            : payload.Slice(QueryHeaderBytes + 16, 16).ToArray();
        if (second is not null && second.All(value => value == 0))
            throw new InvalidDataException("FLD GC reference query second identity must be nonzero.");
        uint authorEpoch = kind == FldDatabaseIpcGcReferenceKind.Receipt
            ? BinaryPrimitives.ReadUInt32LittleEndian(payload.Span[(QueryHeaderBytes + 32)..])
            : 0;
        uint transportPeerEpoch = kind == FldDatabaseIpcGcReferenceKind.Receipt
            ? BinaryPrimitives.ReadUInt32LittleEndian(payload.Span[(QueryHeaderBytes + 36)..])
            : 0;
        if (kind == FldDatabaseIpcGcReferenceKind.Receipt && authorEpoch == 0)
            throw new InvalidDataException("FLD receipt reference author epoch must be nonzero.");
        return new FldDatabaseIpcGcReferenceQuery(
            kind, token, first, second, authorEpoch, transportPeerEpoch);
    }


    public const int MaxChangeIdBatchCount = 256;

    /// <summary>Encodes a packed sequence of 16-byte ChangeIds without per-id arrays.</summary>
    public static byte[] EncodeChangeIdBatchQuery(
        Guid token, ReadOnlyMemory<byte> packedChangeIds, int count)
    {
        if (token == Guid.Empty || count is < 1 or > MaxChangeIdBatchCount ||
            packedChangeIds.Length != checked(count * 16))
            throw new ArgumentOutOfRangeException(nameof(count));

        byte[] payload = new byte[checked(18 + packedChangeIds.Length)];
        token.TryWriteBytes(payload.AsSpan(0, 16), bigEndian: true, out _);
        BinaryPrimitives.WriteUInt16LittleEndian(payload.AsSpan(16), checked((ushort)count));
        ReadOnlySpan<byte> source = packedChangeIds.Span;
        for (int offset = 0; offset < source.Length; offset += 16)
        {
            ReadOnlySpan<byte> id = source.Slice(offset, 16);
            if (id.IndexOfAnyExcept((byte)0) < 0)
                throw new ArgumentException("GC ChangeId batch identities must be nonzero 16-byte values.", nameof(packedChangeIds));
        }
        source.CopyTo(payload.AsSpan(18));
        return payload;
    }

    /// <summary>Validates a batch and returns a zero-copy view over its packed identities.</summary>
    public static (Guid Token, ReadOnlyMemory<byte> PackedChangeIds, int Count)
        DecodeChangeIdBatchQueryView(ReadOnlyMemory<byte> payload)
    {
        if (payload.Length < 34)
            throw new InvalidDataException("FLD GC ChangeId batch query is truncated.");
        Guid token = new(payload.Span[..16], bigEndian: true);
        ushort count = BinaryPrimitives.ReadUInt16LittleEndian(payload.Span[16..]);
        if (token == Guid.Empty || count is < 1 or > MaxChangeIdBatchCount ||
            payload.Length != 18 + count * 16)
            throw new InvalidDataException("FLD GC ChangeId batch query header or length is invalid.");
        ReadOnlyMemory<byte> packed = payload.Slice(18);
        for (int offset = 0; offset < packed.Length; offset += 16)
            if (packed.Span.Slice(offset, 16).IndexOfAnyExcept((byte)0) < 0)
                throw new InvalidDataException("FLD GC ChangeId batch contains a zero identity.");
        return (token, packed, count);
    }

    public static byte[] EncodeChangeIdBatchQuery(Guid token, IReadOnlyList<byte[]> changeIds)
    {
        ArgumentNullException.ThrowIfNull(changeIds);
        if (token == Guid.Empty || changeIds.Count is < 1 or > MaxChangeIdBatchCount)
            throw new ArgumentOutOfRangeException(nameof(changeIds));
        byte[] payload = new byte[checked(18 + changeIds.Count * 16)];
        token.TryWriteBytes(payload.AsSpan(0, 16), bigEndian: true, out _);
        BinaryPrimitives.WriteUInt16LittleEndian(payload.AsSpan(16), checked((ushort)changeIds.Count));
        int offset = 18;
        foreach (byte[] id in changeIds)
        {
            if (id is null || id.Length != 16 || id.AsSpan().IndexOfAnyExcept((byte)0) < 0)
                throw new ArgumentException("GC ChangeId batch identities must be nonzero 16-byte values.", nameof(changeIds));
            id.CopyTo(payload, offset);
            offset += 16;
        }
        return payload;
    }

    public static (Guid Token, byte[][] ChangeIds) DecodeChangeIdBatchQuery(ReadOnlyMemory<byte> payload)
    {
        if (payload.Length < 34)
            throw new InvalidDataException("FLD GC ChangeId batch query is truncated.");
        Guid token = new(payload.Span[..16], bigEndian: true);
        ushort count = BinaryPrimitives.ReadUInt16LittleEndian(payload.Span[16..]);
        if (token == Guid.Empty || count is < 1 or > MaxChangeIdBatchCount ||
            payload.Length != 18 + count * 16)
            throw new InvalidDataException("FLD GC ChangeId batch query header or length is invalid.");
        var ids = new byte[count][];
        int offset = 18;
        for (int i = 0; i < count; i++)
        {
            ReadOnlySpan<byte> id = payload.Span.Slice(offset, 16);
            if (id.IndexOfAnyExcept((byte)0) < 0)
                throw new InvalidDataException("FLD GC ChangeId batch contains a zero identity.");
            ids[i] = id.ToArray();
            offset += 16;
        }
        return (token, ids);
    }

    public static byte[] EncodeChangeIdBatchResponse(IReadOnlyList<bool> referenced)
    {
        ArgumentNullException.ThrowIfNull(referenced);
        if (referenced.Count is < 1 or > MaxChangeIdBatchCount)
            throw new ArgumentOutOfRangeException(nameof(referenced));
        int bitmapBytes = (referenced.Count + 7) / 8;
        byte[] payload = new byte[2 + bitmapBytes];
        BinaryPrimitives.WriteUInt16LittleEndian(payload, checked((ushort)referenced.Count));
        for (int i = 0; i < referenced.Count; i++)
            if (referenced[i]) payload[2 + i / 8] |= (byte)(1 << (i % 8));
        return payload;
    }

    public static bool[] DecodeChangeIdBatchResponse(ReadOnlyMemory<byte> payload, int expectedCount)
    {
        if (expectedCount is < 1 or > MaxChangeIdBatchCount)
            throw new ArgumentOutOfRangeException(nameof(expectedCount));
        int bitmapBytes = (expectedCount + 7) / 8;
        if (payload.Length != 2 + bitmapBytes ||
            BinaryPrimitives.ReadUInt16LittleEndian(payload.Span) != expectedCount)
            throw new InvalidDataException("FLD GC ChangeId batch response length or count is invalid.");
        if (expectedCount % 8 != 0)
        {
            byte unusedMask = (byte)~((1 << (expectedCount % 8)) - 1);
            if ((payload.Span[^1] & unusedMask) != 0)
                throw new InvalidDataException("FLD GC ChangeId batch response has nonzero padding bits.");
        }
        var result = new bool[expectedCount];
        for (int i = 0; i < expectedCount; i++)
            result[i] = (payload.Span[2 + i / 8] & (1 << (i % 8))) != 0;
        return result;
    }

    public static byte[] EncodeBooleanResponse(bool value) => [value ? (byte)1 : (byte)0];

    public static bool DecodeBooleanResponse(ReadOnlyMemory<byte> payload)
    {
        if (payload.Length != 1 || payload.Span[0] > 1)
            throw new InvalidDataException("FLD GC boolean response is invalid.");
        return payload.Span[0] != 0;
    }

    public static Guid DecodeToken(ReadOnlyMemory<byte> payload)
    {
        if (payload.Length != 16)
            throw new InvalidDataException("FLD GC reference-scan token length is invalid.");
        Guid token = new(payload.Span, bigEndian: true);
        if (token == Guid.Empty)
            throw new InvalidDataException("FLD GC reference-scan token must be nonzero.");
        return token;
    }

    public static byte[] EncodeToken(Guid token)
    {
        if (token == Guid.Empty)
            throw new ArgumentException("FLD GC reference-scan token must be nonzero.", nameof(token));
        byte[] payload = new byte[16];
        token.TryWriteBytes(payload, bigEndian: true, out _);
        return payload;
    }
}
