using System.Security.Cryptography;
using Fld1.Storage.Ipc;
using Xunit;

namespace Fld1.Engine.Db.Tests;

public sealed class FldDatabaseIpcOutboxTests
{
    [Fact]
    public async Task Outbox_ipc_chunks_maximum_value_and_fences_previous_consumer()
    {
        string root = Path.Combine(Path.GetTempPath(), "fld-database-outbox-ipc-" + Guid.NewGuid().ToString("N"));
        const uint namespaceId = 0x543;
        byte[] localAuthor = Id(0x61);
        Guid ownerId = Guid.NewGuid();
        Guid firstClientGuid = Guid.NewGuid();
        Guid secondClientGuid = Guid.NewGuid();
        Guid receiptOperationId = Guid.NewGuid();
        byte[] firstClientId = ToNetworkBytes(firstClientGuid);
        byte[] recordId = FldDatabase.NewRecordId();
        byte[] value = new byte[4 * 1024 * 1024];
        for (int i = 0; i < value.Length; i++)
            value[i] = (byte)(i * 31);

        using var database = FldDatabase.Open(root, namespaceId, localAuthor);
        database.Put(recordId, 7, value, keyEpoch: 9);
        Assert.Equal(1ul, database.Commit());

        byte[] fingerprint = SHA256.HashData("independent-ipc-receipt"u8);
        database.CommitWithReceipt(new FldLocalOperationReceipt
        {
            ClientId = firstClientId,
            OperationId = ToNetworkBytes(receiptOperationId),
            ClientSequence = 1,
            Fingerprint = fingerprint,
            Result = "persisted-result"u8.ToArray()
        });

        var dispatcher = new FldDatabaseIpcDispatcher(database);
        using var shutdown = new CancellationTokenSource();
        var server = new StorageIpcServer(
            StorageIpcEndpoint.ComputePipeName(root, 0), dispatcher.HandleAsync);
        Task serverTask = server.RunAsync(shutdown.Token);

        try
        {
            await server.Ready.WaitAsync(TimeSpan.FromSeconds(10));
            StorageIpcEndpoint.Publish(root, StorageIpcEndpoint.Create(
                root, 0, ownerId, Environment.ProcessId));

            await using var firstClient = await FldDatabaseIpcClient.ConnectToDatabaseAsync(
                root, firstClientGuid, TimeSpan.FromSeconds(10));
            Assert.Equal(1ul, await firstClient.GetOutboxHighWatermarkAsync());
            FldDatabaseIpcOutboxLease firstLease = await firstClient.AcquireOutboxLeaseAsync();
            Assert.Equal(1ul, firstLease.Epoch);

            const uint chunkSize = 1024 * 1024;
            var firstChunk = await firstClient.ReadOutboxEntryChunkAsync(
                firstLease, afterSequence: 0, maxChunkBytes: chunkSize);
            Assert.NotNull(firstChunk);
            Assert.Equal(1ul, firstChunk.OutboxSeq);
            Assert.Equal(1ul, firstChunk.CommitSeq);
            Assert.Equal((ushort)7, firstChunk.FieldId);
            Assert.Equal((ushort)9, firstChunk.KeyEpoch);
            Assert.Equal((uint)value.Length, firstChunk.ValueLength);
            Assert.Equal(0u, firstChunk.ValueOffset);
            Assert.Equal((uint)chunkSize, (uint)firstChunk.ValueChunk.Length);
            Assert.False(firstChunk.IsComplete);

            byte[] assembled = new byte[value.Length];
            FldDatabaseIpcOutboxEntryChunk current = firstChunk;
            while (true)
            {
                current.ValueChunk.CopyTo(assembled, checked((int)current.ValueOffset));
                if (current.IsComplete)
                    break;

                uint nextOffset = checked(current.ValueOffset + (uint)current.ValueChunk.Length);
                FldDatabaseIpcOutboxEntryChunk? nextChunk =
                    await firstClient.ReadOutboxEntryChunkAsync(
                        firstLease, afterSequence: 0, entrySequence: current.OutboxSeq,
                        valueOffset: nextOffset, maxChunkBytes: chunkSize);
                Assert.NotNull(nextChunk);
                Assert.Equal(current.OutboxSeq, nextChunk.OutboxSeq);
                Assert.Equal(current.CommitSeq, nextChunk.CommitSeq);
                Assert.Equal(current.ValueLength, nextChunk.ValueLength);
                Assert.Equal(nextOffset, nextChunk.ValueOffset);
                current = nextChunk;
            }
            Assert.Equal(value, assembled);

            await using var secondClient = await FldDatabaseIpcClient.ConnectToDatabaseAsync(
                root, secondClientGuid, TimeSpan.FromSeconds(10));
            FldDatabaseIpcOutboxLease secondLease = await secondClient.AcquireOutboxLeaseAsync();
            Assert.Equal(2ul, secondLease.Epoch);

            await Assert.ThrowsAsync<StorageIpcRemoteException>(() =>
                firstClient.ReadOutboxEntryChunkAsync(firstLease, afterSequence: 0));

            var secondRead = await secondClient.ReadOutboxEntryChunkAsync(
                secondLease, afterSequence: 0);
            Assert.NotNull(secondRead);
            Assert.Equal(1ul, secondRead.OutboxSeq);
            Assert.Equal((uint)FldDatabaseIpcProtocol.MaxOutboxChunkBytes,
                (uint)secondRead.ValueChunk.Length);
            Assert.False(secondRead.IsComplete);

            Assert.True(await secondClient.AcknowledgeOutboxThroughAsync(secondLease, 1));
            Assert.Equal(1ul, await secondClient.GetOutboxHighWatermarkAsync());
            Assert.Equal(1ul, database.GetOutboxWatermark());
            Assert.True(database.TryGetLocalOperationReceipt(
                firstClientId, ToNetworkBytes(receiptOperationId), 1, fingerprint, out var retainedReceipt));
            Assert.Equal("persisted-result"u8.ToArray(), retainedReceipt!.Result);

            Assert.True(await secondClient.ReleaseOutboxLeaseAsync(secondLease));
            await Assert.ThrowsAsync<StorageIpcRemoteException>(() =>
                secondClient.AcknowledgeOutboxThroughAsync(secondLease, 1));
        }
        finally
        {
            shutdown.Cancel();
            try { await serverTask.WaitAsync(TimeSpan.FromSeconds(10)); } catch { }
            StorageIpcEndpoint.DeleteIfOwned(root, 0, ownerId);
            database.Dispose();
            try { Directory.Delete(root, recursive: true); } catch { }
        }
    }

    [Fact]
    public void Outbox_chunk_protocol_rejects_invalid_continuation_and_oversized_chunks()
    {
        var lease = new FldDatabaseIpcOutboxLease(1);
        Assert.Throws<ArgumentException>(() =>
            FldDatabaseIpcProtocol.EncodeReadOutboxEntryChunkPayload(
                lease, afterSequence: 0, entrySequence: 0, valueOffset: 1, maxChunkBytes: 1024));
        Assert.Throws<ArgumentException>(() =>
            FldDatabaseIpcProtocol.EncodeReadOutboxEntryChunkPayload(
                lease, afterSequence: 2, entrySequence: 1, valueOffset: 0, maxChunkBytes: 1024));
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            FldDatabaseIpcProtocol.EncodeReadOutboxEntryChunkPayload(
                lease, afterSequence: 0, entrySequence: 0, valueOffset: 0,
                maxChunkBytes: (uint)(StorageIpcProtocol.MaxPayloadBytes)));
    }

    private static byte[] Id(byte value)
    {
        byte[] id = new byte[16];
        id[0] = value;
        return id;
    }

    private static byte[] ToNetworkBytes(Guid value)
    {
        byte[] bytes = new byte[16];
        value.TryWriteBytes(bytes, bigEndian: true, out _);
        return bytes;
    }
}
