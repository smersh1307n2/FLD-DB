using System.Buffers.Binary;
using Fld1.Engine.Sync;
using Fld1.Storage.Ipc;

namespace Fld1.Engine.Db;

/// <summary>
/// Bounded wire codec for the contextual remote APPLY IPC operation.
/// The durable idempotency key remains the ApplyJournal issuer/batch identity,
/// not the local IPC operation id.
/// </summary>
public static class FldDatabaseIpcRemoteApplyProtocol
{
    public const byte Version = 1;
    public const int MaximumPayloadBytes = StorageIpcProtocol.MaxPayloadBytes;
    private const int MaximumOperations = RemoteBatchCommit.MaximumOperationsPerBatch;
    private const int FixedHeaderBytes = 183;
    private const int FieldHeaderBytes = 81;
    private const int ReceiptHeaderBytes = 49;

    public static byte[] EncodeRequest(RemoteBatchCommit batch)
    {
        ArgumentNullException.ThrowIfNull(batch);
        using var stream = new MemoryStream();
        using var writer = new BinaryWriter(stream);
        writer.Write(Version);
        writer.Write(batch.SenderPeer);
        writer.Write(batch.SenderEpoch);
        writer.Write(batch.NamespaceId);
        writer.Write(batch.IssuerNodeId);
        writer.Write(batch.IssuerEpoch);
        writer.Write(batch.BatchId);
        writer.Write(batch.CursorStart);
        writer.Write(batch.CursorEnd);
        writer.Write(batch.PreviousBatchDigest);
        writer.Write(batch.BatchDigest);
        writer.Write(batch.RecordsRoot);
        writer.Write(checked((uint)batch.ProtocolStateDelta.Length));
        writer.Write(checked((uint)batch.Writes.Count));
        writer.Write(checked((uint)batch.RelayWrites.Count));
        writer.Write(batch.ProtocolStateDelta);

        var indices = new Dictionary<string, int>(batch.Writes.Count, StringComparer.Ordinal);
        for (int i = 0; i < batch.Writes.Count; i++)
            indices.Add(Convert.ToHexString(batch.Writes[i].ChangeId), i);

        foreach (RemoteFieldWrite write in batch.Writes)
            WriteField(writer, write);

        foreach (RemoteFieldWrite relay in batch.RelayWrites)
        {
            if (!indices.TryGetValue(Convert.ToHexString(relay.ChangeId), out int index))
                throw new InvalidDataException("RelayWrites contains an operation outside Writes.");
            writer.Write(checked((uint)index));
        }

        writer.Flush();
        if (stream.Length > MaximumPayloadBytes)
            throw new ArgumentOutOfRangeException(nameof(batch),
                $"Remote APPLY request exceeds the {MaximumPayloadBytes:N0}-byte IPC payload limit.");
        return stream.ToArray();
    }

    public static RemoteBatchCommit DecodeRequest(ReadOnlyMemory<byte> payload)
    {
        if (payload.Length < FixedHeaderBytes || payload.Length > MaximumPayloadBytes)
            throw new InvalidDataException("Remote APPLY request length is outside the supported bounds.");

        using var stream = new MemoryStream(payload.ToArray(), writable: false);
        using var reader = new BinaryReader(stream);
        if (reader.ReadByte() != Version)
            throw new InvalidDataException("Unsupported remote APPLY IPC version.");

        byte[] senderPeer = ReadExact(reader, 16);
        uint senderEpoch = reader.ReadUInt32();
        ushort namespaceId = reader.ReadUInt16();
        byte[] issuerNodeId = ReadExact(reader, 16);
        uint issuerEpoch = reader.ReadUInt32();
        byte[] batchId = ReadExact(reader, 16);
        ulong cursorStart = reader.ReadUInt64();
        ulong cursorEnd = reader.ReadUInt64();
        byte[] previousDigest = ReadExact(reader, 32);
        byte[] digest = ReadExact(reader, 32);
        byte[] recordsRoot = ReadExact(reader, 32);
        uint deltaLength = reader.ReadUInt32();
        uint writeCount = reader.ReadUInt32();
        uint relayCount = reader.ReadUInt32();

        if (deltaLength > MaximumPayloadBytes || writeCount > MaximumOperations ||
            relayCount > writeCount)
            throw new InvalidDataException("Remote APPLY request declares an oversized delta or operation count.");

        long required = (long)deltaLength + (long)writeCount * FieldHeaderBytes + (long)relayCount * sizeof(uint);
        if (required != stream.Length - stream.Position)
            throw new InvalidDataException("Remote APPLY request lengths do not match the encoded payload.");

        byte[] delta = ReadExact(reader, checked((int)deltaLength));
        var writes = new RemoteFieldWrite[checked((int)writeCount)];
        for (int i = 0; i < writes.Length; i++)
            writes[i] = ReadField(reader);

        var relayWrites = new RemoteFieldWrite[checked((int)relayCount)];
        var seen = new HashSet<uint>();
        for (int i = 0; i < relayWrites.Length; i++)
        {
            uint index = reader.ReadUInt32();
            if (index >= writeCount || !seen.Add(index))
                throw new InvalidDataException("Remote APPLY relay index is duplicate or out of range.");
            relayWrites[i] = writes[index];
        }

        if (stream.Position != stream.Length)
            throw new InvalidDataException("Remote APPLY request contains trailing bytes.");

        try
        {
            return new RemoteBatchCommit(senderPeer, senderEpoch, namespaceId,
                issuerNodeId, issuerEpoch, batchId, cursorStart, cursorEnd,
                previousDigest, digest, recordsRoot, writes, relayWrites, delta);
        }
        catch (ArgumentException exception)
        {
            throw new InvalidDataException("Remote APPLY request violates the batch contract.", exception);
        }
    }

    public static byte[] EncodeReceipt(RemoteBatchReceipt receipt)
    {
        ArgumentNullException.ThrowIfNull(receipt);
        ArgumentNullException.ThrowIfNull(receipt.BatchId);
        ArgumentNullException.ThrowIfNull(receipt.ProtocolStateDelta);
        if (receipt.BatchId.Length != 16 ||
            receipt.ProtocolStateDelta.Length > MaximumPayloadBytes - ReceiptHeaderBytes)
            throw new InvalidDataException("Remote APPLY receipt fields exceed protocol bounds.");

        byte[] response = new byte[checked(ReceiptHeaderBytes + receipt.ProtocolStateDelta.Length)];
        BinaryPrimitives.WriteUInt64LittleEndian(response.AsSpan(0, 8), receipt.CommitSeq);
        BinaryPrimitives.WriteUInt64LittleEndian(response.AsSpan(8, 8), receipt.CursorStart);
        BinaryPrimitives.WriteUInt64LittleEndian(response.AsSpan(16, 8), receipt.CursorEnd);
        receipt.BatchId.CopyTo(response, 24);
        BinaryPrimitives.WriteUInt32LittleEndian(response.AsSpan(40, 4), checked((uint)receipt.AppliedCount));
        response[44] = receipt.IsDuplicate ? (byte)1 : (byte)0;
        BinaryPrimitives.WriteUInt32LittleEndian(response.AsSpan(45, 4),
            checked((uint)receipt.ProtocolStateDelta.Length));
        receipt.ProtocolStateDelta.CopyTo(response, ReceiptHeaderBytes);
        return response;
    }

    public static RemoteBatchReceipt DecodeReceipt(ReadOnlyMemory<byte> response)
    {
        ReadOnlySpan<byte> span = response.Span;
        if (span.Length < ReceiptHeaderBytes || span.Length > MaximumPayloadBytes ||
            span[44] > 1)
            throw new InvalidDataException("Remote APPLY receipt header is invalid.");
        uint deltaLength = BinaryPrimitives.ReadUInt32LittleEndian(span.Slice(45, 4));
        if (deltaLength != (uint)(span.Length - ReceiptHeaderBytes))
            throw new InvalidDataException("Remote APPLY receipt delta length is invalid.");
        uint appliedCount = BinaryPrimitives.ReadUInt32LittleEndian(span.Slice(40, 4));
        if (appliedCount > MaximumOperations)
            throw new InvalidDataException("Remote APPLY receipt applied count is invalid.");

        return new RemoteBatchReceipt
        {
            CommitSeq = BinaryPrimitives.ReadUInt64LittleEndian(span.Slice(0, 8)),
            CursorStart = BinaryPrimitives.ReadUInt64LittleEndian(span.Slice(8, 8)),
            CursorEnd = BinaryPrimitives.ReadUInt64LittleEndian(span.Slice(16, 8)),
            BatchId = span.Slice(24, 16).ToArray(),
            AppliedCount = checked((int)appliedCount),
            IsDuplicate = span[44] == 1,
            ProtocolStateDelta = span.Slice(ReceiptHeaderBytes).ToArray()
        };
    }

    private static void WriteField(BinaryWriter writer, RemoteFieldWrite write)
    {
        if (write.Value.Length > MaximumPayloadBytes)
            throw new ArgumentOutOfRangeException(nameof(write), "Remote field value exceeds the IPC frame limit.");
        writer.Write(write.RecordId);
        writer.Write(write.FieldId);
        writer.Write(write.Wall);
        writer.Write(write.Logical);
        writer.Write(write.AuthorNodeId);
        writer.Write(write.ChangeId);
        writer.Write(write.KeyEpoch);
        writer.Write(write.IsTombstone ? (byte)1 : (byte)0);
        writer.Write(write.OriginAuthorEpoch);
        writer.Write(write.OriginSequence);
        writer.Write(checked((uint)write.Value.Length));
        writer.Write(write.Value);
    }

    private static RemoteFieldWrite ReadField(BinaryReader reader)
    {
        byte[] recordId = ReadExact(reader, 16);
        ushort fieldId = reader.ReadUInt16();
        ulong wall = reader.ReadUInt64();
        uint logical = reader.ReadUInt32();
        byte[] author = ReadExact(reader, 16);
        byte[] changeId = ReadExact(reader, 16);
        ushort keyEpoch = reader.ReadUInt16();
        byte tombstone = reader.ReadByte();
        uint originEpoch = reader.ReadUInt32();
        ulong originSequence = reader.ReadUInt64();
        uint valueLength = reader.ReadUInt32();
        if (tombstone > 1 || valueLength > MaximumPayloadBytes ||
            valueLength > reader.BaseStream.Length - reader.BaseStream.Position)
            throw new InvalidDataException("Remote APPLY field flags or value length are invalid.");
        byte[] value = ReadExact(reader, checked((int)valueLength));
        if (tombstone == 1 && value.Length != 0)
            throw new InvalidDataException("Remote APPLY tombstones must have empty values.");
        try
        {
            return new RemoteFieldWrite(recordId, fieldId, value, wall, logical,
                author, changeId, keyEpoch, tombstone == 1, originEpoch, originSequence);
        }
        catch (ArgumentException exception)
        {
            throw new InvalidDataException("Remote APPLY field metadata is invalid.", exception);
        }
    }

    private static byte[] ReadExact(BinaryReader reader, int length)
    {
        byte[] bytes = reader.ReadBytes(length);
        if (bytes.Length != length)
            throw new InvalidDataException("Remote APPLY payload is truncated.");
        return bytes;
    }
}
