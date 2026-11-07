namespace Fld1.Engine.Outbox;

/// <summary>One durable outbox item for BIN-SYNC push (at-least-once).</summary>
public sealed class OutboxEntry
{
    public required ulong OutboxSeq { get; init; }
    public required ulong CommitSeq { get; init; }
    public required byte[] ChangeId { get; init; } // 16
    public required byte[] RecordId { get; init; } // 16
    public required ushort FieldId { get; init; }
    public required byte OpKind { get; init; } // 1=put, 2=tombstone
    public required byte[] AuthorNodeId { get; init; } // 16
    public required ulong Wall { get; init; }
    public required uint Logical { get; init; }
    public required ushort KeyEpoch { get; init; }
    public required byte[] Value { get; init; }
}


/// <summary>
/// Borrowed Outbox record view. All spans are valid only for the duration of the
/// OutboxEntryVisitor callback; consumers must not retain them.
/// </summary>
public readonly ref struct OutboxEntryView
{
    public ulong OutboxSeq { get; }
    public ulong CommitSeq { get; }
    public ReadOnlySpan<byte> ChangeId { get; }
    public ReadOnlySpan<byte> RecordId { get; }
    public ushort FieldId { get; }
    public byte OpKind { get; }
    public ReadOnlySpan<byte> AuthorNodeId { get; }
    public ulong Wall { get; }
    public uint Logical { get; }
    public ushort KeyEpoch { get; }
    public ReadOnlySpan<byte> Value { get; }

    internal OutboxEntryView(ReadOnlySpan<byte> record, uint valueLength)
    {
        OutboxSeq = System.Buffers.Binary.BinaryPrimitives.ReadUInt64LittleEndian(record.Slice(0, 8));
        CommitSeq = System.Buffers.Binary.BinaryPrimitives.ReadUInt64LittleEndian(record.Slice(8, 8));
        ChangeId = record.Slice(16, 16);
        RecordId = record.Slice(32, 16);
        FieldId = System.Buffers.Binary.BinaryPrimitives.ReadUInt16LittleEndian(record.Slice(48, 2));
        OpKind = record[50];
        AuthorNodeId = record.Slice(52, 16);
        Wall = System.Buffers.Binary.BinaryPrimitives.ReadUInt64LittleEndian(record.Slice(68, 8));
        Logical = System.Buffers.Binary.BinaryPrimitives.ReadUInt32LittleEndian(record.Slice(76, 4));
        KeyEpoch = System.Buffers.Binary.BinaryPrimitives.ReadUInt16LittleEndian(record.Slice(80, 2));
        Value = record.Slice(88, checked((int)valueLength));
    }
}

/// <summary>Return false to stop enumeration. The view is borrowed and must not escape.</summary>
public delegate bool OutboxEntryVisitor(in OutboxEntryView entry);
