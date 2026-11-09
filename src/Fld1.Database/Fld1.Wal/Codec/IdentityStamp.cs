using System.Buffers.Binary;
using Fld1.Wal.Format;

namespace Fld1.Wal.Codec;

/// <summary>
/// Durable assignment of an immutable BIN-SYNC OperationOrigin to an existing FLD
/// field mutation. This record never writes field payload or creates an Outbox entry.
/// </summary>
public sealed class WalIdentityStamp
{
    public required byte[] RecordId { get; init; }
    public required ushort FieldId { get; init; }
    public required byte[] ChangeId { get; init; }
    public required uint OriginAuthorEpoch { get; init; }
    public required ulong OriginSequence { get; init; }
}

/// <summary>Versioned, deterministic codec for WAL identity-stamp records.</summary>
public static class IdentityStampCodec
{
    private const uint BodyVersion = 1;
    private const int HeaderSize = 8;
    private const int EntrySize = 48;

    private readonly record struct FieldKey(ulong RecordLo, ulong RecordHi, ushort FieldId)
    {
        public static FieldKey From(ReadOnlySpan<byte> recordId, ushort fieldId)
            => new(
                BinaryPrimitives.ReadUInt64LittleEndian(recordId),
                BinaryPrimitives.ReadUInt64LittleEndian(recordId.Slice(8)),
                fieldId);
    }

    public static byte[] Encode(IReadOnlyList<WalIdentityStamp> stamps)
    {
        ArgumentNullException.ThrowIfNull(stamps);
        if (stamps.Count is 0 or > WalConstants.MaxMutations)
            throw new ArgumentOutOfRangeException(nameof(stamps));

        byte[] payload = new byte[checked(HeaderSize + stamps.Count * EntrySize)];
        BinaryPrimitives.WriteUInt32LittleEndian(payload.AsSpan(0, 4), BodyVersion);
        BinaryPrimitives.WriteUInt32LittleEndian(payload.AsSpan(4, 4), checked((uint)stamps.Count));

        var seen = new HashSet<FieldKey>();
        int offset = HeaderSize;
        foreach (WalIdentityStamp stamp in stamps)
        {
            Validate(stamp);
            if (!seen.Add(FieldKey.From(stamp.RecordId, stamp.FieldId)))
                throw new ArgumentException("Identity-stamp batch contains a duplicate field key.", nameof(stamps));

            stamp.RecordId.CopyTo(payload.AsSpan(offset, 16));
            BinaryPrimitives.WriteUInt16LittleEndian(payload.AsSpan(offset + 16, 2), stamp.FieldId);
            // bytes offset+18..19 are reserved and stay zero.
            stamp.ChangeId.CopyTo(payload.AsSpan(offset + 20, 16));
            BinaryPrimitives.WriteUInt32LittleEndian(payload.AsSpan(offset + 36, 4), stamp.OriginAuthorEpoch);
            BinaryPrimitives.WriteUInt64LittleEndian(payload.AsSpan(offset + 40, 8), stamp.OriginSequence);
            offset += EntrySize;
        }

        if (payload.Length > WalConstants.MaxPayloadLen)
            throw new ArgumentOutOfRangeException(nameof(stamps), "Identity-stamp payload exceeds WAL limit.");
        return payload;
    }

    public static IReadOnlyList<WalIdentityStamp> Decode(ReadOnlySpan<byte> payload)
    {
        if (payload.Length < HeaderSize)
            throw new InvalidDataException("Identity-stamp payload is truncated.");
        if (BinaryPrimitives.ReadUInt32LittleEndian(payload.Slice(0, 4)) != BodyVersion)
            throw new InvalidDataException("Unsupported identity-stamp body version.");

        uint count = BinaryPrimitives.ReadUInt32LittleEndian(payload.Slice(4, 4));
        if (count is 0 or > WalConstants.MaxMutations ||
            payload.Length != checked(HeaderSize + (int)count * EntrySize))
            throw new InvalidDataException("Identity-stamp count or payload length is invalid.");

        var stamps = new WalIdentityStamp[(int)count];
        var seen = new HashSet<FieldKey>();
        int offset = HeaderSize;
        for (int i = 0; i < stamps.Length; i++)
        {
            if (payload[offset + 18] != 0 || payload[offset + 19] != 0)
                throw new InvalidDataException("Identity-stamp reserved bytes are nonzero.");

            var stamp = new WalIdentityStamp
            {
                RecordId = payload.Slice(offset, 16).ToArray(),
                FieldId = BinaryPrimitives.ReadUInt16LittleEndian(payload.Slice(offset + 16, 2)),
                ChangeId = payload.Slice(offset + 20, 16).ToArray(),
                OriginAuthorEpoch = BinaryPrimitives.ReadUInt32LittleEndian(payload.Slice(offset + 36, 4)),
                OriginSequence = BinaryPrimitives.ReadUInt64LittleEndian(payload.Slice(offset + 40, 8))
            };
            ValidateDecoded(stamp);
            if (!seen.Add(FieldKey.From(stamp.RecordId, stamp.FieldId)))
                throw new InvalidDataException("Identity-stamp payload contains a duplicate field key.");
            stamps[i] = stamp;
            offset += EntrySize;
        }

        return stamps;
    }

    private static void Validate(WalIdentityStamp? stamp)
    {
        ArgumentNullException.ThrowIfNull(stamp);
        if (stamp.RecordId is null || stamp.RecordId.Length != 16)
            throw new ArgumentException("Identity-stamp RecordId must be 16 bytes.");
        if (stamp.ChangeId is null || stamp.ChangeId.Length != 16 || IsZero(stamp.ChangeId))
            throw new ArgumentException("Identity-stamp ChangeId must be a nonzero 16-byte identity.");
        if (stamp.OriginAuthorEpoch == 0 || stamp.OriginSequence == 0)
            throw new ArgumentException("Identity-stamp origin epoch and sequence must be nonzero.");
    }

    private static void ValidateDecoded(WalIdentityStamp stamp)
    {
        if (stamp.RecordId.Length != 16 || stamp.ChangeId.Length != 16 ||
            IsZero(stamp.ChangeId) || stamp.OriginAuthorEpoch == 0 || stamp.OriginSequence == 0)
            throw new InvalidDataException("Identity-stamp entry contains an invalid identity or origin.");
    }

    private static bool IsZero(ReadOnlySpan<byte> value)
    {
        byte aggregate = 0;
        foreach (byte part in value) aggregate |= part;
        return aggregate == 0;
    }
}
