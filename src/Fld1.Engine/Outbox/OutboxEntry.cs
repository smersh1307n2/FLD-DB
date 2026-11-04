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
