using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using Fld1.Core.Util;

namespace Fld1.Engine.Storage;

/// <summary>Dictionary key (RecordId 16B + FieldId) without heap string keys.</summary>
public readonly struct MemFieldKey : IEquatable<MemFieldKey>
{
    public readonly ulong RecLo, RecHi;
    public readonly ushort FieldId;

    public MemFieldKey(ReadOnlySpan<byte> recordId, ushort fieldId)
    {
        if (recordId.Length != 16) throw new ArgumentException("RecordId");
        RecLo = BinaryPrimitives.ReadUInt64LittleEndian(recordId);
        RecHi = BinaryPrimitives.ReadUInt64LittleEndian(recordId.Slice(8));
        FieldId = fieldId;
    }

    public void WriteRecordId(Span<byte> dest)
    {
        BinaryPrimitives.WriteUInt64LittleEndian(dest, RecLo);
        BinaryPrimitives.WriteUInt64LittleEndian(dest.Slice(8), RecHi);
    }

    public bool Equals(MemFieldKey other)
        => RecLo == other.RecLo && RecHi == other.RecHi && FieldId == other.FieldId;

    public override bool Equals(object? obj) => obj is MemFieldKey k && Equals(k);
    public override int GetHashCode() => HashCode.Combine(RecLo, RecHi, FieldId);
}

public sealed class MemFieldValue
{
    public required byte[] Value { get; init; }
    public required ulong Wall { get; init; }
    public required uint Logical { get; init; }
    public required byte[] AuthorNodeId { get; init; }
    public required ushort KeyEpoch { get; init; }
    public required bool IsTombstone { get; init; }
    /// <summary>Stable BIN-SYNC ChangeId preserved across WAL recovery.</summary>
    public byte[] ChangeId { get; init; } = Array.Empty<byte>();
}
