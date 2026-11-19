using System.Buffers.Binary;
using Fld1.Storage.Engine;
using Fld1.Storage.Ipc;
using Xunit;

namespace Fld1.Storage.Tests;

public sealed class StorageIpcOperationDispatcherTests
{
    [Fact]
    public async Task Put_get_delete_are_dispatched_with_durable_write_replay()
    {
        string directory = Path.Combine(
            Path.GetTempPath(), "fld-storage-ipc-dispatch-" + Guid.NewGuid().ToString("N"));
        const ulong partition = 0x1E01;
        Guid clientId = Guid.NewGuid();
        byte[] key = "dispatch/key/one"u8.ToArray();
        byte[] value = "dispatcher-value"u8.ToArray();
        Guid putOperationId = Guid.NewGuid();
        Guid deleteOperationId = Guid.NewGuid();

        try
        {
            using var database = StorageDatabase.Open(directory, partition);
            var dispatcher = new StorageIpcOperationDispatcher(database);
            var context = new StorageIpcRequestContext(clientId, 17, 1);

            byte[] putEnvelope = StorageIpcOperationProtocol.Encode(
                new StorageIpcOperationEnvelope(
                    putOperationId,
                    StorageIpcOperationCodes.Put,
                    EncodePut(key, value)));

            ReadOnlyMemory<byte> firstPut = await dispatcher.HandleAsync(
                context, putEnvelope, CancellationToken.None);
            Assert.Equal(new byte[] { 1 }, firstPut.ToArray());
            Assert.Equal(1UL, database.LastCommitSeq);

            ReadOnlyMemory<byte> replayedPut = await dispatcher.HandleAsync(
                context with { RequestId = 2 }, putEnvelope, CancellationToken.None);
            Assert.Equal(firstPut.ToArray(), replayedPut.ToArray());
            Assert.Equal(1UL, database.LastCommitSeq);

            byte[] conflictingPut = StorageIpcOperationProtocol.Encode(
                new StorageIpcOperationEnvelope(
                    putOperationId,
                    StorageIpcOperationCodes.Put,
                    EncodePut(key, "different-value"u8.ToArray())));
            await Assert.ThrowsAsync<InvalidOperationException>(async () =>
                await dispatcher.HandleAsync(context with { RequestId = 3 }, conflictingPut, CancellationToken.None));

            byte[] getEnvelope = StorageIpcOperationProtocol.Encode(
                new StorageIpcOperationEnvelope(
                    Guid.NewGuid(), StorageIpcOperationCodes.Get, key));
            ReadOnlyMemory<byte> getResult = await dispatcher.HandleAsync(
                context with { RequestId = 4 }, getEnvelope, CancellationToken.None);
            Assert.Equal(1, getResult.Span[0]);
            Assert.Equal((uint)value.Length, BinaryPrimitives.ReadUInt32LittleEndian(getResult.Span[1..]));
            Assert.Equal(value, getResult.Span[5..].ToArray());
            Assert.Equal(1UL, database.LastCommitSeq);

            byte[] deleteEnvelope = StorageIpcOperationProtocol.Encode(
                new StorageIpcOperationEnvelope(
                    deleteOperationId, StorageIpcOperationCodes.Delete, key));
            ReadOnlyMemory<byte> firstDelete = await dispatcher.HandleAsync(
                context with { RequestId = 5 }, deleteEnvelope, CancellationToken.None);
            Assert.Equal(new byte[] { 1 }, firstDelete.ToArray());
            Assert.Equal(2UL, database.LastCommitSeq);

            ReadOnlyMemory<byte> replayedDelete = await dispatcher.HandleAsync(
                context with { RequestId = 6 }, deleteEnvelope, CancellationToken.None);
            Assert.Equal(new byte[] { 1 }, replayedDelete.ToArray());
            Assert.Equal(2UL, database.LastCommitSeq);

            ReadOnlyMemory<byte> missing = await dispatcher.HandleAsync(
                context with { RequestId = 7 }, getEnvelope, CancellationToken.None);
            Assert.Equal(new byte[] { 0, 0, 0, 0, 0 }, missing.ToArray());
        }
        finally
        {
            try { Directory.Delete(directory, recursive: true); } catch { }
        }
    }

    [Fact]
    public async Task Dispatcher_rejects_invalid_operation_code_and_malformed_put_lengths()
    {
        string directory = Path.Combine(
            Path.GetTempPath(), "fld-storage-ipc-dispatch-invalid-" + Guid.NewGuid().ToString("N"));
        try
        {
            using var database = StorageDatabase.Open(directory, 0x1E02);
            var dispatcher = new StorageIpcOperationDispatcher(database);
            var context = new StorageIpcRequestContext(Guid.NewGuid(), 1, 1);

            byte[] unknown = StorageIpcOperationProtocol.Encode(
                new StorageIpcOperationEnvelope(Guid.NewGuid(), 99, ReadOnlyMemory<byte>.Empty));
            await Assert.ThrowsAsync<InvalidDataException>(async () =>
                await dispatcher.HandleAsync(context, unknown, CancellationToken.None));

            byte[] malformedPut = StorageIpcOperationProtocol.Encode(
                new StorageIpcOperationEnvelope(
                    Guid.NewGuid(), StorageIpcOperationCodes.Put, new byte[7]));
            await Assert.ThrowsAsync<InvalidDataException>(async () =>
                await dispatcher.HandleAsync(context, malformedPut, CancellationToken.None));

            Assert.Equal(0UL, database.LastCommitSeq);
        }
        finally
        {
            try { Directory.Delete(directory, recursive: true); } catch { }
        }
    }

    private static byte[] EncodePut(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
    {
        byte[] payload = new byte[sizeof(uint) * 2 + key.Length + value.Length];
        BinaryPrimitives.WriteUInt32LittleEndian(payload, (uint)key.Length);
        BinaryPrimitives.WriteUInt32LittleEndian(payload.AsSpan(sizeof(uint)), (uint)value.Length);
        key.CopyTo(payload.AsSpan(sizeof(uint) * 2));
        value.CopyTo(payload.AsSpan(sizeof(uint) * 2 + key.Length));
        return payload;
    }
}
