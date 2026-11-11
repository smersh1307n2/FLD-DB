using System.Buffers.Binary;
using System.Security.Cryptography;
using Fld1.Storage.Ipc;

namespace Fld1.Engine.Db;

public static class FldDatabaseIpcOperationCodes
{
    public const ushort ReadField = 0x2101;
    public const ushort WriteBatch = 0x2102;
    public const ushort AcknowledgeReceipts = 0x2103;
}

public sealed record FldDatabaseIpcMutation(
    byte[] RecordId,
    ushort FieldId,
    ushort KeyEpoch,
    bool IsDelete,
    byte[] Value);

public sealed record FldDatabaseIpcWriteResult(ulong CommitSeq, bool IsDuplicate, int MutationCount);

public sealed record FldDatabaseIpcReadResult(
    ulong CommitSeq,
    bool IsDuplicate,
    bool Found,
    byte[]? Value);

public static class FldDatabaseIpcProtocol
{
    private const int WriteHeaderBytes = 12;
    private const int MutationHeaderBytes = 25;
    private const int ReadPayloadBytes = 26;
    private const int ResultHeaderBytes = 13;
    private const int MaxMutationCount = 4096;

    public static byte[] EncodeWriteBatchPayload(
        ulong clientSequence,
        IReadOnlyList<FldDatabaseIpcMutation> mutations)
    {
        ArgumentNullException.ThrowIfNull(mutations);
        if (clientSequence == 0)
            throw new ArgumentOutOfRangeException(nameof(clientSequence));
        if (mutations.Count is < 1 or > MaxMutationCount)
            throw new ArgumentOutOfRangeException(nameof(mutations));

        int length = WriteHeaderBytes;
        foreach (FldDatabaseIpcMutation mutation in mutations)
        {
            ValidateMutation(mutation);
            length = checked(length + MutationHeaderBytes + mutation.Value.Length);
        }
        if (length > StorageIpcOperationProtocol.MaxPayloadBytes)
            throw new ArgumentOutOfRangeException(nameof(mutations), "Write batch exceeds the IPC frame limit.");

        byte[] payload = new byte[length];
        BinaryPrimitives.WriteUInt64LittleEndian(payload, clientSequence);
        BinaryPrimitives.WriteUInt32LittleEndian(payload.AsSpan(8), (uint)mutations.Count);
        int position = WriteHeaderBytes;
        foreach (FldDatabaseIpcMutation mutation in mutations)
        {
            mutation.RecordId.CopyTo(payload, position);
            position += 16;
            BinaryPrimitives.WriteUInt16LittleEndian(payload.AsSpan(position), mutation.FieldId);
            position += 2;
            BinaryPrimitives.WriteUInt16LittleEndian(payload.AsSpan(position), mutation.KeyEpoch);
            position += 2;
            payload[position++] = mutation.IsDelete ? (byte)1 : (byte)0;
            BinaryPrimitives.WriteUInt32LittleEndian(payload.AsSpan(position), (uint)mutation.Value.Length);
            position += 4;
            mutation.Value.CopyTo(payload, position);
            position += mutation.Value.Length;
        }
        return payload;
    }

    public static (ulong ClientSequence, IReadOnlyList<FldDatabaseIpcMutation> Mutations)
        DecodeWriteBatchPayload(ReadOnlyMemory<byte> payload)
    {
        ReadOnlySpan<byte> span = payload.Span;
        if (span.Length < WriteHeaderBytes)
            throw new InvalidDataException("FLD write batch payload is truncated.");
        ulong sequence = BinaryPrimitives.ReadUInt64LittleEndian(span);
        uint rawCount = BinaryPrimitives.ReadUInt32LittleEndian(span[8..]);
        if (sequence == 0 || rawCount is 0 or > MaxMutationCount)
            throw new InvalidDataException("FLD write batch sequence or mutation count is invalid.");

        var mutations = new List<FldDatabaseIpcMutation>((int)rawCount);
        int position = WriteHeaderBytes;
        for (int i = 0; i < rawCount; i++)
        {
            if (span.Length - position < MutationHeaderBytes)
                throw new InvalidDataException("FLD write batch mutation header is truncated.");
            byte[] recordId = span.Slice(position, 16).ToArray();
            position += 16;
            ushort fieldId = BinaryPrimitives.ReadUInt16LittleEndian(span[position..]);
            position += 2;
            ushort keyEpoch = BinaryPrimitives.ReadUInt16LittleEndian(span[position..]);
            position += 2;
            byte kind = span[position++];
            uint rawLength = BinaryPrimitives.ReadUInt32LittleEndian(span[position..]);
            position += 4;
            if (kind > 1 || rawLength > WalConstants.MaxValLen || rawLength > (uint)(span.Length - position))
                throw new InvalidDataException("FLD write batch mutation kind or value length is invalid.");
            byte[] value = span.Slice(position, (int)rawLength).ToArray();
            position += (int)rawLength;
            if (kind == 1 && value.Length != 0)
                throw new InvalidDataException("FLD delete mutation must not carry a value.");
            var mutation = new FldDatabaseIpcMutation(recordId, fieldId, keyEpoch, kind == 1, value);
            ValidateMutation(mutation);
            mutations.Add(mutation);
        }
        if (position != span.Length)
            throw new InvalidDataException("FLD write batch payload has trailing bytes.");
        return (sequence, mutations);
    }

    public static byte[] EncodeReadFieldPayload(ulong clientSequence, ReadOnlySpan<byte> recordId, ushort fieldId)
    {
        if (clientSequence == 0)
            throw new ArgumentOutOfRangeException(nameof(clientSequence));
        if (recordId.Length != 16)
            throw new ArgumentException("RecordId must be 16 bytes.", nameof(recordId));
        byte[] payload = new byte[ReadPayloadBytes];
        BinaryPrimitives.WriteUInt64LittleEndian(payload, clientSequence);
        recordId.CopyTo(payload.AsSpan(8, 16));
        BinaryPrimitives.WriteUInt16LittleEndian(payload.AsSpan(24), fieldId);
        return payload;
    }

    public static (ulong ClientSequence, byte[] RecordId, ushort FieldId)
        DecodeReadFieldPayload(ReadOnlyMemory<byte> payload)
    {
        ReadOnlySpan<byte> span = payload.Span;
        if (span.Length != ReadPayloadBytes)
            throw new InvalidDataException("FLD read field payload length is invalid.");
        ulong sequence = BinaryPrimitives.ReadUInt64LittleEndian(span);
        if (sequence == 0)
            throw new InvalidDataException("FLD read field ClientSequence must be nonzero.");
        return (sequence, span.Slice(8, 16).ToArray(), BinaryPrimitives.ReadUInt16LittleEndian(span[24..]));
    }

    public static byte[] EncodeAcknowledgementPayload(ulong acknowledgedThroughSequence)
    {
        if (acknowledgedThroughSequence == 0)
            throw new ArgumentOutOfRangeException(nameof(acknowledgedThroughSequence));
        byte[] payload = new byte[8];
        BinaryPrimitives.WriteUInt64LittleEndian(payload, acknowledgedThroughSequence);
        return payload;
    }

    public static ulong DecodeAcknowledgementPayload(ReadOnlyMemory<byte> payload)
    {
        if (payload.Length != 8)
            throw new InvalidDataException("FLD receipt acknowledgement payload length is invalid.");
        ulong sequence = BinaryPrimitives.ReadUInt64LittleEndian(payload.Span);
        if (sequence == 0)
            throw new InvalidDataException("FLD receipt acknowledgement sequence must be nonzero.");
        return sequence;
    }

    public static byte[] ComputeFingerprint(ushort operationCode, ReadOnlySpan<byte> payload)
    {
        byte[] canonical = new byte[checked(sizeof(ushort) + payload.Length)];
        BinaryPrimitives.WriteUInt16LittleEndian(canonical, operationCode);
        payload.CopyTo(canonical.AsSpan(sizeof(ushort)));
        return SHA256.HashData(canonical);
    }

    public static byte[] EncodeCommitResult(FldLocalOperationCommitResult result)
    {
        ArgumentNullException.ThrowIfNull(result);
        byte[] encoded = new byte[checked(ResultHeaderBytes + result.Result.Length)];
        BinaryPrimitives.WriteUInt64LittleEndian(encoded, result.CommitSeq);
        encoded[8] = result.IsDuplicate ? (byte)1 : (byte)0;
        BinaryPrimitives.WriteUInt32LittleEndian(encoded.AsSpan(9), (uint)result.Result.Length);
        result.Result.CopyTo(encoded, ResultHeaderBytes);
        return encoded;
    }

    public static (ulong CommitSeq, bool IsDuplicate, byte[] Result) DecodeCommitResult(ReadOnlyMemory<byte> encoded)
    {
        ReadOnlySpan<byte> span = encoded.Span;
        if (span.Length < ResultHeaderBytes)
            throw new InvalidDataException("FLD IPC commit result is truncated.");
        byte duplicate = span[8];
        uint rawLength = BinaryPrimitives.ReadUInt32LittleEndian(span[9..]);
        if (duplicate > 1 || rawLength != span.Length - ResultHeaderBytes)
            throw new InvalidDataException("FLD IPC commit result flags or length are invalid.");
        return (BinaryPrimitives.ReadUInt64LittleEndian(span), duplicate == 1,
            span.Slice(ResultHeaderBytes, (int)rawLength).ToArray());
    }

    public static byte[] EncodeWriteResult(int mutationCount)
    {
        if (mutationCount is < 1 or > MaxMutationCount)
            throw new ArgumentOutOfRangeException(nameof(mutationCount));
        byte[] result = new byte[5];
        result[0] = 1;
        BinaryPrimitives.WriteUInt32LittleEndian(result.AsSpan(1), (uint)mutationCount);
        return result;
    }

    public static FldDatabaseIpcWriteResult DecodeWriteResult(
        ulong commitSeq, bool isDuplicate, ReadOnlySpan<byte> result)
    {
        if (result.Length != 5 || result[0] != 1)
            throw new InvalidDataException("FLD IPC write result is invalid.");
        uint count = BinaryPrimitives.ReadUInt32LittleEndian(result[1..]);
        if (count is 0 or > MaxMutationCount)
            throw new InvalidDataException("FLD IPC write result mutation count is invalid.");
        return new FldDatabaseIpcWriteResult(commitSeq, isDuplicate, (int)count);
    }

    public static FldDatabaseIpcReadResult DecodeReadResult(
        ulong commitSeq, bool isDuplicate, ReadOnlySpan<byte> result)
    {
        if (result.Length < 5 || result[0] > 1)
            throw new InvalidDataException("FLD IPC read result is invalid.");
        uint rawLength = BinaryPrimitives.ReadUInt32LittleEndian(result[1..]);
        if (rawLength != result.Length - 5 || (result[0] == 0 && rawLength != 0))
            throw new InvalidDataException("FLD IPC read result value length is invalid.");
        return new FldDatabaseIpcReadResult(
            commitSeq, isDuplicate, result[0] == 1,
            result[0] == 1 ? result.Slice(5, (int)rawLength).ToArray() : null);
    }

    private static void ValidateMutation(FldDatabaseIpcMutation mutation)
    {
        ArgumentNullException.ThrowIfNull(mutation);
        ArgumentNullException.ThrowIfNull(mutation.RecordId);
        ArgumentNullException.ThrowIfNull(mutation.Value);
        if (mutation.RecordId.Length != 16 || mutation.RecordId.All(value => value == 0))
            throw new ArgumentException("Mutation RecordId must be a nonzero 16-byte identifier.", nameof(mutation));
        if (mutation.Value.Length > WalConstants.MaxValLen)
            throw new ArgumentOutOfRangeException(nameof(mutation), "Mutation value exceeds the WAL value limit.");
        if (mutation.IsDelete && mutation.Value.Length != 0)
            throw new ArgumentException("Delete mutation must not carry a value.", nameof(mutation));
    }
}
