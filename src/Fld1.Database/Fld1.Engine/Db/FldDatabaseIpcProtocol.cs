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
    public const ushort OpenSnapshot = 0x2108;
    public const ushort ReadSnapshotPage = 0x2109;
    public const ushort ReadSnapshotValueChunk = 0x210A;
    public const ushort ReleaseSnapshot = 0x210B;
    public const ushort ApplyRemoteBatch = 0x210C;
    public const ushort AppendLocalProviderBatch = 0x210D;
    public const ushort GetOutboxHighWatermark = 0x210E;
    public const ushort ReadCurrentMetadata = 0x210F;
    public const ushort ReadCurrentValueChunk = 0x2110;
    public const ushort AppendLocalProviderBatchAutoSequence = 0x2111;
    public const ushort AcknowledgeProviderOutbox = 0x2112;
    public const ushort GetLocalAuthor = 0x2113;
}

public sealed record FldDatabaseIpcMutation(
    byte[] RecordId,
    ushort FieldId,
    ushort KeyEpoch,
    bool IsDelete,
    byte[] Value);

public sealed record FldDatabaseIpcWriteResult(ulong CommitSeq, bool IsDuplicate, int MutationCount);
public sealed record FldDatabaseIpcCurrentMetadata(uint ValueLength, ulong HlcWall, uint HlcLogical, byte[] AuthorNodeId, ushort KeyEpoch, bool IsTombstone, byte[] ChangeId, uint OriginAuthorEpoch, ulong OriginSequence);
public enum FldDatabaseIpcCurrentReadStatus : byte { Missing = 0, Changed = 1, Data = 2 }
public sealed record FldDatabaseIpcCurrentValueChunk(FldDatabaseIpcCurrentReadStatus Status, uint ValueLength, uint ValueOffset, byte[] ValueChunk)
{
    public bool IsComplete => Status == FldDatabaseIpcCurrentReadStatus.Data && (ulong)ValueOffset + (uint)ValueChunk.Length == ValueLength;
}

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

public sealed record FldDatabaseIpcSnapshotHandle(
    Guid HostGeneration,
    Guid SnapshotToken,
    ulong SourceFeedWatermark,
    int RecordCount);

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
    private const int SnapshotOpenResponseBytes = 44;
    private const int SnapshotPageRequestBytes = 38;
    private const int SnapshotPageResponseHeaderBytes = 7;
    private const int SnapshotPageEntryBytes = 81;
    private const int SnapshotValueChunkRequestBytes = 58;
    private const int SnapshotValueChunkResponseHeaderBytes = 13;
    private const int SnapshotReleasePayloadBytes = 32;
    public const int MaxSnapshotPageEntries = 256;
    public const int MaxSnapshotValueChunkBytes = StorageIpcProtocol.MaxPayloadBytes - SnapshotValueChunkResponseHeaderBytes;

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

    public static byte[] EncodeLocalAuthorResponse(ReadOnlySpan<byte> localAuthor)
    {
        if (localAuthor.Length != 16 || IsZeroIdentifier(localAuthor))
            throw new ArgumentException("FLD LocalAuthor must be a nonzero 16-byte identifier.", nameof(localAuthor));
        return localAuthor.ToArray();
    }

    public static byte[] DecodeLocalAuthorResponse(ReadOnlyMemory<byte> response)
    {
        if (response.Length != 16 || IsZeroIdentifier(response.Span))
            throw new InvalidDataException("FLD LocalAuthor response must be a nonzero 16-byte identifier.");
        return response.ToArray();
    }

    private static bool IsZeroIdentifier(ReadOnlySpan<byte> value)
    {
        byte aggregate = 0;
        foreach (byte part in value) aggregate |= part;
        return aggregate == 0;
    }

    public const int MaxCurrentValueChunkBytes = StorageIpcProtocol.MaxPayloadBytes - 13;

    public static byte[] EncodeReadCurrentMetadataPayload(ReadOnlySpan<byte> recordId, ushort fieldId)
    {
        if (recordId.Length != 16) throw new ArgumentException("RecordId must be 16 bytes.", nameof(recordId));
        byte[] payload = new byte[18]; recordId.CopyTo(payload);
        BinaryPrimitives.WriteUInt16LittleEndian(payload.AsSpan(16), fieldId); return payload;
    }
    public static (byte[] RecordId, ushort FieldId) DecodeReadCurrentMetadataPayload(ReadOnlyMemory<byte> payload)
    {
        if (payload.Length != 18) throw new InvalidDataException("Current metadata request length is invalid.");
        return (payload[..16].ToArray(), BinaryPrimitives.ReadUInt16LittleEndian(payload.Span[16..]));
    }
    public static byte[] EncodeCurrentMetadataResponse(FldDatabaseIpcCurrentMetadata? m)
    {
        byte[] b = new byte[64]; if (m is null) return b;
        if (m.AuthorNodeId is not { Length: 16 } || m.ChangeId is not { Length: 16 } || (m.IsTombstone && m.ValueLength != 0))
            throw new ArgumentException("Current metadata is not canonical.", nameof(m));
        b[0]=1; BinaryPrimitives.WriteUInt32LittleEndian(b.AsSpan(1),m.ValueLength);
        BinaryPrimitives.WriteUInt64LittleEndian(b.AsSpan(5),m.HlcWall); BinaryPrimitives.WriteUInt32LittleEndian(b.AsSpan(13),m.HlcLogical);
        m.AuthorNodeId.CopyTo(b,17); BinaryPrimitives.WriteUInt16LittleEndian(b.AsSpan(33),m.KeyEpoch); b[35]=m.IsTombstone?(byte)1:(byte)0;
        m.ChangeId.CopyTo(b,36); BinaryPrimitives.WriteUInt32LittleEndian(b.AsSpan(52),m.OriginAuthorEpoch); BinaryPrimitives.WriteUInt64LittleEndian(b.AsSpan(56),m.OriginSequence); return b;
    }
    public static FldDatabaseIpcCurrentMetadata? DecodeCurrentMetadataResponse(ReadOnlyMemory<byte> p)
    {
        if(p.Length!=64 || p.Span[0]>1) throw new InvalidDataException("Current metadata response is invalid.");
        if(p.Span[0]==0){if(p.Span[1..].IndexOfAnyExcept((byte)0)>=0)throw new InvalidDataException("Missing metadata response contains data.");return null;}
        ReadOnlySpan<byte>s=p.Span; bool tomb=s[35] switch{0=>false,1=>true,_=>throw new InvalidDataException("Invalid tombstone flag.")};
        uint len=BinaryPrimitives.ReadUInt32LittleEndian(s[1..]); if(tomb&&len!=0)throw new InvalidDataException("Tombstone has a value.");
        return new(len,BinaryPrimitives.ReadUInt64LittleEndian(s[5..]),BinaryPrimitives.ReadUInt32LittleEndian(s[13..]),s.Slice(17,16).ToArray(),BinaryPrimitives.ReadUInt16LittleEndian(s[33..]),tomb,s.Slice(36,16).ToArray(),BinaryPrimitives.ReadUInt32LittleEndian(s[52..]),BinaryPrimitives.ReadUInt64LittleEndian(s[56..]));
    }
    public static byte[] EncodeReadCurrentValueChunkPayload(ReadOnlySpan<byte> recordId,ushort fieldId,ReadOnlySpan<byte> changeId,uint offset,uint max)
    {
        if(recordId.Length!=16)throw new ArgumentException("RecordId must be 16 bytes.",nameof(recordId));
        if(changeId.Length!=16)throw new ArgumentException("ChangeId must be 16 bytes.",nameof(changeId));
        if(max is 0 or > MaxCurrentValueChunkBytes)throw new ArgumentOutOfRangeException(nameof(max));
        byte[] b=new byte[42];recordId.CopyTo(b);BinaryPrimitives.WriteUInt16LittleEndian(b.AsSpan(16),fieldId);changeId.CopyTo(b.AsSpan(18));
        BinaryPrimitives.WriteUInt32LittleEndian(b.AsSpan(34),offset);BinaryPrimitives.WriteUInt32LittleEndian(b.AsSpan(38),max);return b;
    }
    public static (byte[] RecordId,ushort FieldId,byte[] ExpectedChangeId,uint ValueOffset,uint MaxChunkBytes) DecodeReadCurrentValueChunkPayload(ReadOnlyMemory<byte> p)
    {
        if(p.Length!=42)throw new InvalidDataException("Current value request length is invalid.");
        uint max=BinaryPrimitives.ReadUInt32LittleEndian(p.Span[38..]);if(max is 0 or > MaxCurrentValueChunkBytes)throw new InvalidDataException("Current value chunk size is invalid.");
        return(p[..16].ToArray(),BinaryPrimitives.ReadUInt16LittleEndian(p.Span[16..]),p.Slice(18,16).ToArray(),BinaryPrimitives.ReadUInt32LittleEndian(p.Span[34..]),max);
    }
    public static byte[] EncodeCurrentValueChunkResponse(FldDatabaseIpcCurrentValueChunk c)
    {
        ArgumentNullException.ThrowIfNull(c);
        if(c.Status!=FldDatabaseIpcCurrentReadStatus.Data&&(c.ValueLength!=0||c.ValueOffset!=0||c.ValueChunk.Length!=0))throw new ArgumentException("Non-data chunk must be empty.",nameof(c));
        if(c.ValueChunk.Length>MaxCurrentValueChunkBytes||(ulong)c.ValueOffset+(uint)c.ValueChunk.Length>c.ValueLength)throw new ArgumentException("Current value chunk bounds invalid.",nameof(c));
        byte[] b=new byte[13+c.ValueChunk.Length];b[0]=(byte)c.Status;BinaryPrimitives.WriteUInt32LittleEndian(b.AsSpan(1),c.ValueLength);
        BinaryPrimitives.WriteUInt32LittleEndian(b.AsSpan(5),c.ValueOffset);BinaryPrimitives.WriteUInt32LittleEndian(b.AsSpan(9),(uint)c.ValueChunk.Length);c.ValueChunk.CopyTo(b,13);return b;
    }
    public static FldDatabaseIpcCurrentValueChunk DecodeCurrentValueChunkResponse(ReadOnlyMemory<byte> p)
    {
        if(p.Length<13)throw new InvalidDataException("Current value response truncated.");
        var status=p.Span[0] switch{0=>FldDatabaseIpcCurrentReadStatus.Missing,1=>FldDatabaseIpcCurrentReadStatus.Changed,2=>FldDatabaseIpcCurrentReadStatus.Data,_=>throw new InvalidDataException("Invalid current value status.")};
        uint len=BinaryPrimitives.ReadUInt32LittleEndian(p.Span[1..]),off=BinaryPrimitives.ReadUInt32LittleEndian(p.Span[5..]),n=BinaryPrimitives.ReadUInt32LittleEndian(p.Span[9..]);
        if(n!=p.Length-13||(status!=FldDatabaseIpcCurrentReadStatus.Data&&(len!=0||off!=0||n!=0))||(ulong)off+n>len)throw new InvalidDataException("Current value response bounds invalid.");
        return new(status,len,off,p.Slice(13,checked((int)n)).ToArray());
    }

    public static byte[] EncodeOutboxHighWatermarkResponse(ulong highWatermark)
    {
        byte[] response = new byte[8];
        BinaryPrimitives.WriteUInt64LittleEndian(response, highWatermark);
        return response;
    }

    public static ulong DecodeOutboxHighWatermarkResponse(ReadOnlyMemory<byte> response)
    {
        if (response.Length != 8)
            throw new InvalidDataException("FLD Outbox high-watermark response length is invalid.");
        return BinaryPrimitives.ReadUInt64LittleEndian(response.Span);
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

    public static byte[] EncodeSnapshotOpenResponse(
        Guid hostGeneration,
        Guid snapshotToken,
        ulong sourceFeedWatermark,
        int recordCount)
    {
        if (hostGeneration == Guid.Empty || snapshotToken == Guid.Empty)
            throw new ArgumentException("Snapshot host generation and token must be non-empty.");
        if (recordCount < 0)
            throw new ArgumentOutOfRangeException(nameof(recordCount));
        byte[] response = new byte[SnapshotOpenResponseBytes];
        WriteGuid(hostGeneration, response);
        WriteGuid(snapshotToken, response.AsSpan(16));
        BinaryPrimitives.WriteUInt64LittleEndian(response.AsSpan(32), sourceFeedWatermark);
        BinaryPrimitives.WriteUInt32LittleEndian(response.AsSpan(40), checked((uint)recordCount));
        return response;
    }

    public static FldDatabaseIpcSnapshotHandle DecodeSnapshotOpenResponse(ReadOnlyMemory<byte> response)
    {
        if (response.Length != SnapshotOpenResponseBytes)
            throw new InvalidDataException("FLD snapshot-open response length is invalid.");
        Guid hostGeneration = ReadGuid(response.Span);
        Guid snapshotToken = ReadGuid(response.Span[16..]);
        ulong watermark = BinaryPrimitives.ReadUInt64LittleEndian(response.Span[32..]);
        uint recordCount = BinaryPrimitives.ReadUInt32LittleEndian(response.Span[40..]);
        if (hostGeneration == Guid.Empty || snapshotToken == Guid.Empty || recordCount > int.MaxValue)
            throw new InvalidDataException("FLD snapshot-open response fields are invalid.");
        return new FldDatabaseIpcSnapshotHandle(hostGeneration, snapshotToken, watermark, (int)recordCount);
    }

    public static byte[] EncodeSnapshotPagePayload(
        FldDatabaseIpcSnapshotHandle handle,
        int afterIndex,
        int limit)
    {
        ValidateSnapshotHandle(handle);
        if (afterIndex < 0)
            throw new ArgumentOutOfRangeException(nameof(afterIndex));
        if (limit is < 1 or > MaxSnapshotPageEntries)
            throw new ArgumentOutOfRangeException(nameof(limit));
        byte[] payload = new byte[SnapshotPageRequestBytes];
        WriteGuid(handle.HostGeneration, payload);
        WriteGuid(handle.SnapshotToken, payload.AsSpan(16));
        BinaryPrimitives.WriteInt32LittleEndian(payload.AsSpan(32), afterIndex);
        BinaryPrimitives.WriteUInt16LittleEndian(payload.AsSpan(36), checked((ushort)limit));
        return payload;
    }

    public static (Guid HostGeneration, Guid SnapshotToken, int AfterIndex, int Limit)
        DecodeSnapshotPagePayload(ReadOnlyMemory<byte> payload)
    {
        if (payload.Length != SnapshotPageRequestBytes)
            throw new InvalidDataException("FLD snapshot-page request length is invalid.");
        Guid hostGeneration = ReadGuid(payload.Span);
        Guid snapshotToken = ReadGuid(payload.Span[16..]);
        int afterIndex = BinaryPrimitives.ReadInt32LittleEndian(payload.Span[32..]);
        ushort limit = BinaryPrimitives.ReadUInt16LittleEndian(payload.Span[36..]);
        if (hostGeneration == Guid.Empty || snapshotToken == Guid.Empty ||
            afterIndex < 0 || limit is < 1 or > MaxSnapshotPageEntries)
            throw new InvalidDataException("FLD snapshot-page request fields are invalid.");
        return (hostGeneration, snapshotToken, afterIndex, limit);
    }

    public static byte[] EncodeSnapshotPageResponse(FldProviderSnapshotMetadataPage page)
    {
        ArgumentNullException.ThrowIfNull(page);
        if (page.NextIndex < 0 || page.Entries.Count > MaxSnapshotPageEntries)
            throw new ArgumentOutOfRangeException(nameof(page));
        byte[] response = new byte[checked(SnapshotPageResponseHeaderBytes + page.Entries.Count * SnapshotPageEntryBytes)];
        BinaryPrimitives.WriteInt32LittleEndian(response, page.NextIndex);
        response[4] = page.HasMore ? (byte)1 : (byte)0;
        BinaryPrimitives.WriteUInt16LittleEndian(response.AsSpan(5), checked((ushort)page.Entries.Count));
        int offset = SnapshotPageResponseHeaderBytes;
        foreach (FldProviderSnapshotEntryMetadata entry in page.Entries)
        {
            ArgumentNullException.ThrowIfNull(entry);
            if (entry.RecordId is null || entry.RecordId.Length != 16 ||
                entry.RecordId.AsSpan().IndexOfAnyExcept((byte)0) < 0 ||
                entry.AuthorNodeId is null || entry.ChangeId is null ||
                (entry.AuthorNodeId.Length is not (0 or 16)) ||
                (entry.ChangeId.Length is not (0 or 16)) ||
                ((entry.IsTombstone || entry.IsRecordTombstone) && entry.ValueLength != 0) ||
                (entry.IsRecordTombstone && (entry.IsTombstone || entry.KeyEpoch != 0)))
                throw new InvalidDataException("FLD snapshot page entry identity or tombstone fields are invalid.");
            entry.RecordId.CopyTo(response, offset);
            offset += 16;
            BinaryPrimitives.WriteUInt16LittleEndian(response.AsSpan(offset), entry.FieldId);
            offset += 2;
            BinaryPrimitives.WriteUInt32LittleEndian(response.AsSpan(offset), entry.ValueLength);
            offset += 4;
            BinaryPrimitives.WriteUInt64LittleEndian(response.AsSpan(offset), entry.HlcWall);
            offset += 8;
            BinaryPrimitives.WriteUInt32LittleEndian(response.AsSpan(offset), entry.HlcLogical);
            offset += 4;
            byte flags = 0;
            if (entry.IsTombstone) flags |= 1;
            if (entry.IsRecordTombstone) flags |= 2;
            if (entry.AuthorNodeId.Length == 16) flags |= 4;
            if (entry.ChangeId.Length == 16) flags |= 8;
            response[offset++] = flags;
            if (entry.AuthorNodeId.Length == 16)
                entry.AuthorNodeId.CopyTo(response, offset);
            offset += 16;
            BinaryPrimitives.WriteUInt16LittleEndian(response.AsSpan(offset), entry.KeyEpoch);
            offset += 2;
            if (entry.ChangeId.Length == 16)
                entry.ChangeId.CopyTo(response, offset);
            offset += 16;
            BinaryPrimitives.WriteUInt32LittleEndian(response.AsSpan(offset), entry.OriginAuthorEpoch);
            offset += 4;
            BinaryPrimitives.WriteUInt64LittleEndian(response.AsSpan(offset), entry.OriginSequence);
            offset += 8;
        }
        return response;
    }

    public static FldProviderSnapshotMetadataPage DecodeSnapshotPageResponse(ReadOnlyMemory<byte> response)
    {
        ReadOnlySpan<byte> span = response.Span;
        if (span.Length < SnapshotPageResponseHeaderBytes)
            throw new InvalidDataException("FLD snapshot-page response is truncated.");
        int nextIndex = BinaryPrimitives.ReadInt32LittleEndian(span);
        byte hasMore = span[4];
        ushort count = BinaryPrimitives.ReadUInt16LittleEndian(span[5..]);
        long expectedLength = SnapshotPageResponseHeaderBytes + (long)count * SnapshotPageEntryBytes;
        if (nextIndex < 0 || hasMore > 1 || count > MaxSnapshotPageEntries || expectedLength != span.Length)
            throw new InvalidDataException("FLD snapshot-page response header or length is invalid.");
        var entries = new FldProviderSnapshotEntryMetadata[count];
        int offset = SnapshotPageResponseHeaderBytes;
        for (int i = 0; i < entries.Length; i++)
        {
            byte[] recordId = span.Slice(offset, 16).ToArray();
            offset += 16;
            ushort fieldId = BinaryPrimitives.ReadUInt16LittleEndian(span[offset..]);
            offset += 2;
            uint valueLength = BinaryPrimitives.ReadUInt32LittleEndian(span[offset..]);
            offset += 4;
            ulong wall = BinaryPrimitives.ReadUInt64LittleEndian(span[offset..]);
            offset += 8;
            uint logical = BinaryPrimitives.ReadUInt32LittleEndian(span[offset..]);
            offset += 4;
            byte flags = span[offset++];
            if ((flags & 0xF0) != 0)
                throw new InvalidDataException("FLD snapshot-page entry contains unknown flags.");
            byte[] author = span.Slice(offset, 16).ToArray();
            offset += 16;
            ushort keyEpoch = BinaryPrimitives.ReadUInt16LittleEndian(span[offset..]);
            offset += 2;
            byte[] changeId = span.Slice(offset, 16).ToArray();
            offset += 16;
            uint originEpoch = BinaryPrimitives.ReadUInt32LittleEndian(span[offset..]);
            offset += 4;
            ulong originSequence = BinaryPrimitives.ReadUInt64LittleEndian(span[offset..]);
            offset += 8;
            bool isTombstone = (flags & 1) != 0;
            bool isRecordTombstone = (flags & 2) != 0;
            if (recordId.AsSpan().IndexOfAnyExcept((byte)0) < 0 ||
                ((isTombstone || isRecordTombstone) && valueLength != 0) ||
                (isRecordTombstone && (isTombstone || keyEpoch != 0)))
                throw new InvalidDataException("FLD snapshot tombstone entry fields are invalid.");
            entries[i] = new FldProviderSnapshotEntryMetadata(
                recordId,
                fieldId,
                valueLength,
                wall,
                logical,
                (flags & 4) != 0 ? author : Array.Empty<byte>(),
                keyEpoch,
                isTombstone,
                (flags & 8) != 0 ? changeId : Array.Empty<byte>(),
                originEpoch,
                originSequence,
                isRecordTombstone);
        }
        return new FldProviderSnapshotMetadataPage(nextIndex, hasMore == 1, entries);
    }

    public static byte[] EncodeSnapshotValueChunkPayload(
        FldDatabaseIpcSnapshotHandle handle,
        ReadOnlySpan<byte> recordId,
        ushort fieldId,
        uint valueOffset,
        uint maxChunkBytes)
    {
        ValidateSnapshotHandle(handle);
        if (recordId.Length != 16 || recordId.IndexOfAnyExcept((byte)0) < 0)
            throw new ArgumentException("Snapshot value RecordId must be a nonzero 16-byte identifier.", nameof(recordId));
        if (maxChunkBytes == 0 || maxChunkBytes > MaxSnapshotValueChunkBytes)
            throw new ArgumentOutOfRangeException(nameof(maxChunkBytes));
        byte[] payload = new byte[SnapshotValueChunkRequestBytes];
        WriteGuid(handle.HostGeneration, payload);
        WriteGuid(handle.SnapshotToken, payload.AsSpan(16));
        recordId.CopyTo(payload.AsSpan(32, 16));
        BinaryPrimitives.WriteUInt16LittleEndian(payload.AsSpan(48), fieldId);
        BinaryPrimitives.WriteUInt32LittleEndian(payload.AsSpan(50), valueOffset);
        BinaryPrimitives.WriteUInt32LittleEndian(payload.AsSpan(54), maxChunkBytes);
        return payload;
    }

    public static (Guid HostGeneration, Guid SnapshotToken, byte[] RecordId, ushort FieldId, uint ValueOffset, uint MaxChunkBytes)
        DecodeSnapshotValueChunkPayload(ReadOnlyMemory<byte> payload)
    {
        if (payload.Length != SnapshotValueChunkRequestBytes)
            throw new InvalidDataException("FLD snapshot value-chunk request length is invalid.");
        Guid hostGeneration = ReadGuid(payload.Span);
        Guid snapshotToken = ReadGuid(payload.Span[16..]);
        byte[] recordId = payload.Span.Slice(32, 16).ToArray();
        ushort fieldId = BinaryPrimitives.ReadUInt16LittleEndian(payload.Span[48..]);
        uint valueOffset = BinaryPrimitives.ReadUInt32LittleEndian(payload.Span[50..]);
        uint maxChunkBytes = BinaryPrimitives.ReadUInt32LittleEndian(payload.Span[54..]);
        if (hostGeneration == Guid.Empty || snapshotToken == Guid.Empty ||
            recordId.All(value => value == 0) || maxChunkBytes == 0 ||
            maxChunkBytes > MaxSnapshotValueChunkBytes)
            throw new InvalidDataException("FLD snapshot value-chunk request fields are invalid.");
        return (hostGeneration, snapshotToken, recordId, fieldId, valueOffset, maxChunkBytes);
    }

    public static byte[] EncodeSnapshotValueChunkResponse(FldProviderSnapshotValueChunk? chunk)
    {
        if (chunk is null)
            return new byte[SnapshotValueChunkResponseHeaderBytes];
        if ((ulong)chunk.ValueOffset + (uint)chunk.ValueChunk.Length > chunk.ValueLength ||
            chunk.ValueChunk.Length > MaxSnapshotValueChunkBytes)
            throw new InvalidDataException("FLD snapshot value-chunk response fields are invalid.");
        byte[] response = new byte[checked(SnapshotValueChunkResponseHeaderBytes + chunk.ValueChunk.Length)];
        response[0] = 1;
        BinaryPrimitives.WriteUInt32LittleEndian(response.AsSpan(1), chunk.ValueLength);
        BinaryPrimitives.WriteUInt32LittleEndian(response.AsSpan(5), chunk.ValueOffset);
        BinaryPrimitives.WriteUInt32LittleEndian(response.AsSpan(9), checked((uint)chunk.ValueChunk.Length));
        chunk.ValueChunk.CopyTo(response, SnapshotValueChunkResponseHeaderBytes);
        return response;
    }

    public static FldProviderSnapshotValueChunk? DecodeSnapshotValueChunkResponse(ReadOnlyMemory<byte> response)
    {
        ReadOnlySpan<byte> span = response.Span;
        if (span.Length < SnapshotValueChunkResponseHeaderBytes || span[0] > 1)
            throw new InvalidDataException("FLD snapshot value-chunk response is truncated or invalid.");
        uint valueLength = BinaryPrimitives.ReadUInt32LittleEndian(span[1..]);
        uint valueOffset = BinaryPrimitives.ReadUInt32LittleEndian(span[5..]);
        uint chunkLength = BinaryPrimitives.ReadUInt32LittleEndian(span[9..]);
        if (chunkLength != (uint)(span.Length - SnapshotValueChunkResponseHeaderBytes) ||
            chunkLength > MaxSnapshotValueChunkBytes ||
            (ulong)valueOffset + chunkLength > valueLength)
            throw new InvalidDataException("FLD snapshot value-chunk response length is invalid.");
        if (span[0] == 0)
        {
            if (valueLength != 0 || valueOffset != 0 || chunkLength != 0)
                throw new InvalidDataException("Missing FLD snapshot value must have an empty response.");
            return null;
        }
        return new FldProviderSnapshotValueChunk(
            valueLength, valueOffset, span.Slice(SnapshotValueChunkResponseHeaderBytes, (int)chunkLength).ToArray());
    }

    public static byte[] EncodeSnapshotReleasePayload(FldDatabaseIpcSnapshotHandle handle)
    {
        ValidateSnapshotHandle(handle);
        byte[] payload = new byte[SnapshotReleasePayloadBytes];
        WriteGuid(handle.HostGeneration, payload);
        WriteGuid(handle.SnapshotToken, payload.AsSpan(16));
        return payload;
    }

    public static (Guid HostGeneration, Guid SnapshotToken) DecodeSnapshotReleasePayload(ReadOnlyMemory<byte> payload)
    {
        if (payload.Length != SnapshotReleasePayloadBytes)
            throw new InvalidDataException("FLD snapshot release request length is invalid.");
        Guid hostGeneration = ReadGuid(payload.Span);
        Guid snapshotToken = ReadGuid(payload.Span[16..]);
        if (hostGeneration == Guid.Empty || snapshotToken == Guid.Empty)
            throw new InvalidDataException("FLD snapshot release request fields are invalid.");
        return (hostGeneration, snapshotToken);
    }

    public static byte[] EncodeSnapshotReleaseResponse(bool released) => [released ? (byte)1 : (byte)0];

    public static bool DecodeSnapshotReleaseResponse(ReadOnlyMemory<byte> response)
    {
        if (response.Length != 1 || response.Span[0] > 1)
            throw new InvalidDataException("FLD snapshot release response is invalid.");
        return response.Span[0] == 1;
    }

    private static void ValidateSnapshotHandle(FldDatabaseIpcSnapshotHandle handle)
    {
        ArgumentNullException.ThrowIfNull(handle);
        if (handle.HostGeneration == Guid.Empty || handle.SnapshotToken == Guid.Empty)
            throw new ArgumentException("Snapshot handle identity must be non-empty.", nameof(handle));
    }

    private static void WriteGuid(Guid value, Span<byte> destination)
    {
        if (destination.Length < 16) throw new ArgumentException("GUID destination is too small.", nameof(destination));
        value.TryWriteBytes(destination[..16], bigEndian: true, out _);
    }

    private static Guid ReadGuid(ReadOnlySpan<byte> source)
    {
        if (source.Length < 16) throw new ArgumentException("GUID source is too small.", nameof(source));
        return new Guid(source[..16], bigEndian: true);
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
