using System.Buffers.Binary;
using System.Security.Cryptography;
using Fld1.Engine.Outbox;
using Fld1.Storage.Ipc;
using Fld1.Wal.Format;

namespace Fld1.Engine.Db;

public static class FldDatabaseIpcOperationCodes
{
    public const ushort ReadField = 0x2101;
    public const ushort WriteBatch = 0x2102;
    public const ushort AcknowledgeReceipts = 0x2103;
    public const ushort AcquireOutboxLease = 0x2104;
    public const ushort ReadOutboxEntryChunk = 0x2105;
    public const ushort AcknowledgeOutbox = 0x2106;
    public const ushort ReleaseOutboxLease = 0x2107;
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

public readonly record struct FldDatabaseIpcOutboxLease(ulong Epoch);

public sealed record FldDatabaseIpcOutboxEntryChunk(
    ulong OutboxSeq,
    ulong CommitSeq,
    byte[] ChangeId,
    byte[] RecordId,
    ushort FieldId,
    byte OpKind,
    byte[] AuthorNodeId,
    ulong Wall,
    uint Logical,
    ushort KeyEpoch,
    uint ValueLength,
    uint ValueOffset,
    byte[] ValueChunk)
{
    public bool IsComplete => (ulong)ValueOffset + (uint)ValueChunk.Length == ValueLength;
}

public static class FldDatabaseIpcProtocol
{
    private const int WriteHeaderBytes = 12;
    private const int MutationHeaderBytes = 25;
    private const int ReadPayloadBytes = 26;
    private const int ResultHeaderBytes = 13;
    private const int MaxMutationCount = 4096;
    private const int OutboxChunkRequestBytes = 32;
    private const int OutboxChunkResponseHeaderBytes = 94;
    public const int MaxOutboxChunkBytes = StorageIpcProtocol.MaxPayloadBytes - OutboxChunkResponseHeaderBytes;

    public static byte[] EncodeOutboxLeaseResponse(ulong leaseEpoch)
    {
        if (leaseEpoch == 0)
            throw new ArgumentOutOfRangeException(nameof(leaseEpoch));
        byte[] payload = new byte[8];
        BinaryPrimitives.WriteUInt64LittleEndian(payload, leaseEpoch);
        return payload;
    }

    public static FldDatabaseIpcOutboxLease DecodeOutboxLeaseResponse(ReadOnlyMemory<byte> payload)
    {
        if (payload.Length != 8)
            throw new InvalidDataException("FLD Outbox lease response length is invalid.");
        ulong epoch = BinaryPrimitives.ReadUInt64LittleEndian(payload.Span);
        if (epoch == 0)
            throw new InvalidDataException("FLD Outbox lease epoch must be nonzero.");
        return new FldDatabaseIpcOutboxLease(epoch);
    }

    public static byte[] EncodeOutboxLeasePayload(FldDatabaseIpcOutboxLease lease)
    {
        if (lease.Epoch == 0)
            throw new ArgumentOutOfRangeException(nameof(lease));
        byte[] payload = new byte[8];
        BinaryPrimitives.WriteUInt64LittleEndian(payload, lease.Epoch);
        return payload;
    }

    public static ulong DecodeOutboxLeasePayload(ReadOnlyMemory<byte> payload)
    {
        if (payload.Length != 8)
            throw new InvalidDataException("FLD Outbox lease payload length is invalid.");
        ulong epoch = BinaryPrimitives.ReadUInt64LittleEndian(payload.Span);
        if (epoch == 0)
            throw new InvalidDataException("FLD Outbox lease epoch must be nonzero.");
        return epoch;
    }

    public static byte[] EncodeOutboxAcknowledgementPayload(
        FldDatabaseIpcOutboxLease lease,
        ulong throughSequence)
    {
        if (lease.Epoch == 0)
            throw new ArgumentOutOfRangeException(nameof(lease));
        if (throughSequence == 0)
            throw new ArgumentOutOfRangeException(nameof(throughSequence));
        byte[] payload = new byte[16];
        BinaryPrimitives.WriteUInt64LittleEndian(payload, lease.Epoch);
        BinaryPrimitives.WriteUInt64LittleEndian(payload.AsSpan(8), throughSequence);
        return payload;
    }

    public static (ulong LeaseEpoch, ulong ThroughSequence) DecodeOutboxAcknowledgementPayload(
        ReadOnlyMemory<byte> payload)
    {
        if (payload.Length != 16)
            throw new InvalidDataException("FLD Outbox acknowledgement payload length is invalid.");
        ulong epoch = BinaryPrimitives.ReadUInt64LittleEndian(payload.Span);
        ulong through = BinaryPrimitives.ReadUInt64LittleEndian(payload.Span[8..]);
        if (epoch == 0 || through == 0)
            throw new InvalidDataException("FLD Outbox acknowledgement identity must be nonzero.");
        return (epoch, through);
    }

    public static byte[] EncodeReadOutboxEntryChunkPayload(
        FldDatabaseIpcOutboxLease lease,
        ulong afterSequence,
        ulong entrySequence,
        uint valueOffset,
        uint maxChunkBytes)
    {
        if (lease.Epoch == 0)
            throw new ArgumentOutOfRangeException(nameof(lease));
        if (entrySequence == 0 && valueOffset != 0)
            throw new ArgumentException("A new Outbox entry read must start at value offset zero.", nameof(valueOffset));
        if (entrySequence != 0 && (entrySequence <= afterSequence || entrySequence - afterSequence != 1))
            throw new ArgumentException("An Outbox chunk continuation must target the next sequence after its cursor.", nameof(entrySequence));
        if (maxChunkBytes == 0 || maxChunkBytes > MaxOutboxChunkBytes)
            throw new ArgumentOutOfRangeException(nameof(maxChunkBytes));

        byte[] payload = new byte[OutboxChunkRequestBytes];
        BinaryPrimitives.WriteUInt64LittleEndian(payload, lease.Epoch);
        BinaryPrimitives.WriteUInt64LittleEndian(payload.AsSpan(8), afterSequence);
        BinaryPrimitives.WriteUInt64LittleEndian(payload.AsSpan(16), entrySequence);
        BinaryPrimitives.WriteUInt32LittleEndian(payload.AsSpan(24), valueOffset);
        BinaryPrimitives.WriteUInt32LittleEndian(payload.AsSpan(28), maxChunkBytes);
        return payload;
    }

    public static (ulong LeaseEpoch, ulong AfterSequence, ulong EntrySequence, uint ValueOffset, uint MaxChunkBytes)
        DecodeReadOutboxEntryChunkPayload(ReadOnlyMemory<byte> payload)
    {
        if (payload.Length != OutboxChunkRequestBytes)
            throw new InvalidDataException("FLD Outbox chunk request length is invalid.");
        ReadOnlySpan<byte> span = payload.Span;
        ulong epoch = BinaryPrimitives.ReadUInt64LittleEndian(span);
        ulong after = BinaryPrimitives.ReadUInt64LittleEndian(span[8..]);
        ulong entry = BinaryPrimitives.ReadUInt64LittleEndian(span[16..]);
        uint offset = BinaryPrimitives.ReadUInt32LittleEndian(span[24..]);
        uint maxChunk = BinaryPrimitives.ReadUInt32LittleEndian(span[28..]);
        if (epoch == 0 || maxChunk == 0 || maxChunk > MaxOutboxChunkBytes
            || (entry == 0 && offset != 0)
            || (entry != 0 && (entry <= after || entry - after != 1)))
            throw new InvalidDataException("FLD Outbox chunk request fields are invalid.");
        return (epoch, after, entry, offset, maxChunk);
    }

    /// <summary>
    /// Copies one bounded value slice from the borrowed Outbox view. This must run
    /// synchronously inside the OutboxEntryVisitor callback.
    /// </summary>
    public static byte[] EncodeOutboxEntryChunkResponse(
        in OutboxEntryView entry,
        uint valueOffset,
        uint maxChunkBytes)
    {
        if (valueOffset > (uint)entry.Value.Length)
            throw new ArgumentOutOfRangeException(nameof(valueOffset));
        if (maxChunkBytes == 0 || maxChunkBytes > MaxOutboxChunkBytes)
            throw new ArgumentOutOfRangeException(nameof(maxChunkBytes));

        uint totalLength = (uint)entry.Value.Length;
        int remaining = checked((int)(totalLength - valueOffset));
        int chunkLength = Math.Min(remaining, checked((int)maxChunkBytes));
        byte[] response = new byte[checked(OutboxChunkResponseHeaderBytes + chunkLength)];
        response[0] = 1;
        BinaryPrimitives.WriteUInt64LittleEndian(response.AsSpan(1), entry.OutboxSeq);
        BinaryPrimitives.WriteUInt64LittleEndian(response.AsSpan(9), entry.CommitSeq);
        entry.ChangeId.CopyTo(response.AsSpan(17, 16));
        entry.RecordId.CopyTo(response.AsSpan(33, 16));
        BinaryPrimitives.WriteUInt16LittleEndian(response.AsSpan(49), entry.FieldId);
        response[51] = entry.OpKind;
        entry.AuthorNodeId.CopyTo(response.AsSpan(52, 16));
        BinaryPrimitives.WriteUInt64LittleEndian(response.AsSpan(68), entry.Wall);
        BinaryPrimitives.WriteUInt32LittleEndian(response.AsSpan(76), entry.Logical);
        BinaryPrimitives.WriteUInt16LittleEndian(response.AsSpan(80), entry.KeyEpoch);
        BinaryPrimitives.WriteUInt32LittleEndian(response.AsSpan(82), totalLength);
        BinaryPrimitives.WriteUInt32LittleEndian(response.AsSpan(86), valueOffset);
        BinaryPrimitives.WriteUInt32LittleEndian(response.AsSpan(90), (uint)chunkLength);
        entry.Value.Slice(checked((int)valueOffset), chunkLength).CopyTo(response.AsSpan(OutboxChunkResponseHeaderBytes));
        return response;
    }

    public static FldDatabaseIpcOutboxEntryChunk? DecodeOutboxEntryChunkResponse(
        ReadOnlyMemory<byte> response)
    {
        ReadOnlySpan<byte> span = response.Span;
        if (span.Length == 1 && span[0] == 0)
            return null;
        if (span.Length < OutboxChunkResponseHeaderBytes || span[0] != 1)
            throw new InvalidDataException("FLD Outbox chunk response is truncated or has an invalid status.");

        uint totalLength = BinaryPrimitives.ReadUInt32LittleEndian(span[82..]);
        uint valueOffset = BinaryPrimitives.ReadUInt32LittleEndian(span[86..]);
        uint chunkLength = BinaryPrimitives.ReadUInt32LittleEndian(span[90..]);
        if (valueOffset > totalLength
            || chunkLength > totalLength - valueOffset
            || chunkLength != (uint)(span.Length - OutboxChunkResponseHeaderBytes))
            throw new InvalidDataException("FLD Outbox chunk response lengths are invalid.");

        return new FldDatabaseIpcOutboxEntryChunk(
            BinaryPrimitives.ReadUInt64LittleEndian(span[1..]),
            BinaryPrimitives.ReadUInt64LittleEndian(span[9..]),
            span.Slice(17, 16).ToArray(),
            span.Slice(33, 16).ToArray(),
            BinaryPrimitives.ReadUInt16LittleEndian(span[49..]),
            span[51],
            span.Slice(52, 16).ToArray(),
            BinaryPrimitives.ReadUInt64LittleEndian(span[68..]),
            BinaryPrimitives.ReadUInt32LittleEndian(span[76..]),
            BinaryPrimitives.ReadUInt16LittleEndian(span[80..]),
            totalLength,
            valueOffset,
            span.Slice(OutboxChunkResponseHeaderBytes, (int)chunkLength).ToArray());
    }

    public static byte[] EncodeOutboxBooleanResponse(bool value) => [value ? (byte)1 : (byte)0];

    public static bool DecodeOutboxBooleanResponse(ReadOnlyMemory<byte> response)
    {
        if (response.Length != 1 || response.Span[0] > 1)
            throw new InvalidDataException("FLD Outbox boolean response is invalid.");
        return response.Span[0] == 1;
    }

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
        if (duplicate > 1 || rawLength != (uint)(span.Length - ResultHeaderBytes))
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
        if (rawLength != (uint)(result.Length - 5) || (result[0] == 0 && rawLength != 0))
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
