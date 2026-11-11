using System.Buffers.Binary;
using Fld1.Storage.Ipc;

namespace Fld1.Engine.Db;

/// <summary>Bounded codec for identity-preserving local provider draft batches.</summary>
public static class FldDatabaseIpcLocalProviderProtocol
{
    public const int MaximumPayloadBytes = StorageIpcProtocol.MaxPayloadBytes;
    private const int HeaderBytes = 12;
    private const int MutationHeaderBytes = 57;
    private const int MaximumMutationCount = 4096;

    public static byte[] EncodeRequest(
        ulong clientSequence,
        IReadOnlyList<FldLocalProviderMutation> mutations)
    {
        ArgumentNullException.ThrowIfNull(mutations);
        if (clientSequence == 0)
            throw new ArgumentOutOfRangeException(nameof(clientSequence));
        if (mutations.Count is < 1 or > MaximumMutationCount)
            throw new ArgumentOutOfRangeException(nameof(mutations));

        long length = HeaderBytes;
        foreach (FldLocalProviderMutation mutation in mutations)
        {
            ArgumentNullException.ThrowIfNull(mutation);
            mutation.Validate();
            length += MutationHeaderBytes + (long)mutation.Value.Length;
        }
        if (length > MaximumPayloadBytes)
            throw new ArgumentOutOfRangeException(nameof(mutations),
                $"Local provider batch exceeds the {MaximumPayloadBytes:N0}-byte IPC payload limit.");

        using var stream = new MemoryStream(checked((int)length));
        using var writer = new BinaryWriter(stream);
        writer.Write(clientSequence);
        writer.Write(checked((uint)mutations.Count));
        foreach (FldLocalProviderMutation mutation in mutations)
        {
            writer.Write(mutation.RecordId);
            writer.Write(mutation.FieldId);
            writer.Write(mutation.KeyEpoch);
            writer.Write(mutation.IsTombstone ? (byte)1 : (byte)0);
            writer.Write(mutation.OriginAuthorEpoch);
            writer.Write(mutation.Wall);
            writer.Write(mutation.Logical);
            writer.Write(mutation.ChangeId);
            writer.Write(checked((uint)mutation.Value.Length));
            writer.Write(mutation.Value);
        }
        writer.Flush();
        return stream.ToArray();
    }

    public static (ulong ClientSequence, IReadOnlyList<FldLocalProviderMutation> Mutations)
        DecodeRequest(ReadOnlyMemory<byte> payload)
    {
        if (payload.Length < HeaderBytes || payload.Length > MaximumPayloadBytes)
            throw new InvalidDataException("Local provider IPC request length is invalid.");

        using var stream = new MemoryStream(payload.ToArray(), writable: false);
        using var reader = new BinaryReader(stream);
        ulong sequence = reader.ReadUInt64();
        uint count = reader.ReadUInt32();
        if (sequence == 0 || count is < 1 or > MaximumMutationCount ||
            (long)count * MutationHeaderBytes > stream.Length - stream.Position)
            throw new InvalidDataException("Local provider IPC request header is invalid.");

        var mutations = new FldLocalProviderMutation[checked((int)count)];
        for (int i = 0; i < mutations.Length; i++)
        {
            byte[] recordId = ReadExact(reader, 16);
            ushort fieldId = reader.ReadUInt16();
            ushort keyEpoch = reader.ReadUInt16();
            byte tombstone = reader.ReadByte();
            uint authorEpoch = reader.ReadUInt32();
            ulong wall = reader.ReadUInt64();
            uint logical = reader.ReadUInt32();
            byte[] changeId = ReadExact(reader, 16);
            uint valueLength = reader.ReadUInt32();
            if (tombstone > 1 || valueLength > MaximumPayloadBytes ||
                valueLength > stream.Length - stream.Position)
                throw new InvalidDataException("Local provider IPC mutation flags or value length are invalid.");
            byte[] value = ReadExact(reader, checked((int)valueLength));
            if (tombstone == 1 && value.Length != 0)
                throw new InvalidDataException("Local provider tombstone value must be empty.");
            var mutation = new FldLocalProviderMutation(
                recordId, fieldId, value, changeId, authorEpoch,
                wall, logical, keyEpoch, tombstone == 1);
            try { mutation.Validate(); }
            catch (ArgumentException exception)
            {
                throw new InvalidDataException("Local provider IPC mutation violates its canonical contract.", exception);
            }
            mutations[i] = mutation;
        }

        if (stream.Position != stream.Length)
            throw new InvalidDataException("Local provider IPC request contains trailing bytes.");
        return (sequence, Array.AsReadOnly(mutations));
    }

    private static byte[] ReadExact(BinaryReader reader, int length)
    {
        byte[] value = reader.ReadBytes(length);
        if (value.Length != length)
            throw new InvalidDataException("Local provider IPC request is truncated.");
        return value;
    }
}
