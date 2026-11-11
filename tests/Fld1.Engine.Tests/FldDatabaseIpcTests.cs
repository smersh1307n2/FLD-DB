using Fld1.Storage.Ipc;
using Xunit;

namespace Fld1.Engine.Db.Tests;

public sealed class FldDatabaseIpcTests
{
    [Fact]
    public async Task Ipc_client_commits_retries_reads_acknowledges_and_reopens_without_reapplying()
    {
        string root = Path.Combine(Path.GetTempPath(), "fld-database-ipc-" + Guid.NewGuid().ToString("N"));
        const uint namespaceId = 0x321;
        byte[] localAuthor = Id(0x41);
        Guid ownerId = Guid.NewGuid();
        Guid clientGuid = Guid.NewGuid();
        byte[] clientId = new byte[16];
        clientGuid.TryWriteBytes(clientId, bigEndian: true, out _);

        using var database = FldDatabase.Open(root, namespaceId, localAuthor);
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

            byte[] recordId = FldDatabase.NewRecordId();
            byte[] firstValue = "ipc-write-one"u8.ToArray();
            byte[] secondValue = "ipc-write-two"u8.ToArray();
            Guid firstOperation = Guid.NewGuid();
            Guid secondOperation = Guid.NewGuid();

            await using (var client = await FldDatabaseIpcClient.ConnectToDatabaseAsync(
                root, clientGuid, TimeSpan.FromSeconds(10)))
            {
                var firstMutation = new FldDatabaseIpcMutation(
                    recordId, 1, 7, false, firstValue);
                FldDatabaseIpcWriteResult first = await client.WriteBatchAsync(
                    firstOperation, 1, [firstMutation]);
                Assert.Equal(1ul, first.CommitSeq);
                Assert.False(first.IsDuplicate);
                Assert.Equal(1, first.MutationCount);

                var secondMutation = new FldDatabaseIpcMutation(
                    recordId, 2, 7, false, secondValue);
                FldDatabaseIpcWriteResult second = await client.WriteBatchAsync(
                    secondOperation, 2, [secondMutation]);
                Assert.Equal(2ul, second.CommitSeq);
                Assert.False(second.IsDuplicate);

                FldDatabaseIpcWriteResult retry = await client.WriteBatchAsync(
                    firstOperation, 1, [firstMutation]);
                Assert.Equal(first.CommitSeq, retry.CommitSeq);
                Assert.True(retry.IsDuplicate);

                FldDatabaseIpcReadResult read = await client.ReadFieldAsync(
                    Guid.NewGuid(), 3, recordId, 1);
                Assert.Equal(3ul, read.CommitSeq);
                Assert.True(read.Found);
                Assert.Equal(firstValue, read.Value);

                Assert.True(await client.AcknowledgeThroughAsync(3));
                Assert.False(await client.AcknowledgeThroughAsync(3));
                await Assert.ThrowsAsync<StorageIpcRemoteException>(() =>
                    client.WriteBatchAsync(firstOperation, 1, [firstMutation]));
            }

            shutdown.Cancel();
            await serverTask.WaitAsync(TimeSpan.FromSeconds(10));
            StorageIpcEndpoint.DeleteIfOwned(root, 0, ownerId);

            database.Dispose();
            using var reopened = FldDatabase.Open(root, namespaceId, localAuthor);
            byte[] firstFingerprint = FldDatabaseIpcProtocol.ComputeFingerprint(
                FldDatabaseIpcOperationCodes.WriteBatch,
                FldDatabaseIpcProtocol.EncodeWriteBatchPayload(1,
                [
                    new FldDatabaseIpcMutation(recordId, 1, 7, false, firstValue)
                ]));
            Assert.Throws<InvalidOperationException>(() => reopened.TryGetLocalOperationReceipt(
                clientId, ToNetworkBytes(firstOperation), 1, firstFingerprint, out _));
            Assert.True(reopened.TryGetBytes(recordId, 1, out byte[]? persistedFirst));
            Assert.Equal(firstValue, persistedFirst);
            Assert.True(reopened.TryGetBytes(recordId, 2, out byte[]? persistedSecond));
            Assert.Equal(secondValue, persistedSecond);
            Assert.Equal(2, reopened.ReadOutboxBatch(0, 10).Count);
        }
        finally
        {
            shutdown.Cancel();
            try { await serverTask.WaitAsync(TimeSpan.FromSeconds(10)); } catch { }
            StorageIpcEndpoint.DeleteIfOwned(root, 0, ownerId);
            try { Directory.Delete(root, recursive: true); } catch { }
        }
    }

    [Fact]
    public void Write_batch_protocol_rejects_trailing_bytes_and_invalid_delete_values()
    {
        byte[] recordId = Id(0x51);
        byte[] payload = FldDatabaseIpcProtocol.EncodeWriteBatchPayload(1,
        [
            new FldDatabaseIpcMutation(recordId, 1, 2, false, "v"u8.ToArray())
        ]);
        byte[] trailing = [.. payload, 0];
        Assert.Throws<InvalidDataException>(() =>
            FldDatabaseIpcProtocol.DecodeWriteBatchPayload(trailing));

        Assert.Throws<ArgumentException>(() =>
            FldDatabaseIpcProtocol.EncodeWriteBatchPayload(1,
            [
                new FldDatabaseIpcMutation(recordId, 1, 2, true, "not-empty"u8.ToArray())
            ]));
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
