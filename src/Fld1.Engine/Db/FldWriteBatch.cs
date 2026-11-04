namespace Fld1.Engine.Db;

/// <summary>
/// Simple write batch for the imperative API (not LINQ).
/// Apply with <see cref="FldDatabase.Apply(FldWriteBatch)"/> then <see cref="FldDatabase.Commit"/>.
/// </summary>
public sealed class FldWriteBatch
{
    internal readonly List<Entry> Entries = new();

    public int Count => Entries.Count;

    public FldWriteBatch Put(ReadOnlySpan<byte> recordId, ushort fieldId, ReadOnlySpan<byte> value)
    {
        if (recordId.Length != 16) throw new ArgumentException("RecordId must be 16 bytes");
        Entries.Add(new Entry(recordId.ToArray(), fieldId, value.ToArray(), Tombstone: false));
        return this;
    }

    public FldWriteBatch Put(byte[] recordId, ushort fieldId, byte[] value)
    {
        if (recordId.Length != 16) throw new ArgumentException("RecordId must be 16 bytes");
        Entries.Add(new Entry(recordId, fieldId, value, Tombstone: false));
        return this;
    }

    /// <summary>Lifecycle-style delete: field tombstone (value empty).</summary>
    public FldWriteBatch Delete(ReadOnlySpan<byte> recordId, ushort fieldId)
    {
        if (recordId.Length != 16) throw new ArgumentException("RecordId must be 16 bytes");
        Entries.Add(new Entry(recordId.ToArray(), fieldId, Array.Empty<byte>(), Tombstone: true));
        return this;
    }

    public void Clear() => Entries.Clear();

    internal readonly record struct Entry(byte[] RecordId, ushort FieldId, byte[] Value, bool Tombstone);
}
