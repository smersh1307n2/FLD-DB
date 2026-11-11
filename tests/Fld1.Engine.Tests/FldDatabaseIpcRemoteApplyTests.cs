using Fld1.Engine.Db;
using Fld1.Engine.Sync;
using Fld1.Storage.Ipc;
using Xunit;

namespace Fld1.Engine.Db.Tests;

public sealed class FldDatabaseIpcRemoteApplyTests
{
    [Fact]
    public void Remote_apply_codec_roundtrips_all_context_and_relay_subset_without_copying_payloads()
    {
        RemoteBatchCommit batch = CreateBatch(0x51);
        byte[] encoded = FldDatabaseIpcRemoteApplyProtocol.EncodeRequest(batch);
        RemoteBatchCommit decoded = FldDatabaseIpcRemoteApplyProtocol.DecodeRequest(encoded);

        Assert.Equal(batch.SenderPeer, decoded.SenderPeer);
        Assert.Equal(batch.SenderEpoch, decoded.SenderEpoch);
        Assert.Equal(batch.NamespaceId, decoded.NamespaceId);
        Assert.Equal(batch.IssuerNodeId, decoded.IssuerNodeId);
        Assert.Equal(batch.IssuerEpoch, decoded.IssuerEpoch);
        Assert.Equal(batch.BatchId, decoded.BatchId);
        Assert.Equal(batch.CursorStart, decoded.CursorStart);
        Assert.Equal(batch.CursorEnd, decoded.CursorEnd);
        Assert.Equal(batch.PreviousBatchDigest, decoded.PreviousBatchDigest);
        Assert.Equal(batch.BatchDigest, decoded.BatchDigest);
        Assert.Equal(batch.RecordsRoot, decoded.RecordsRoot);
        Assert.Equal(batch.ProtocolStateDelta, decoded.ProtocolStateDelta);
        Assert.Equal(batch.Writes.Count, decoded.Writes.Count);
        Assert.Equal(batch.RelayWrites.Count, decoded.RelayWrites.Count);
        Assert.Equal(batch.Writes[1].ChangeId, decoded.RelayWrites[0].ChangeId);
        Assert.Equal(batch.Writes[1].Value, decoded.RelayWrites[0].Value);

        Assert.Throws<InvalidDataException>(() =>
            FldDatabaseIpcRemoteApplyProtocol.DecodeRequest(encoded.Concat(new byte[] { 0 }).ToArray()));
    }

    [Fact]
    public async Task Remote_apply_ipc_is_atomic_idempotent_and_keeps_local_outbox_separate()
    {
        string root = Path.Combine(Path.GetTempPath(), "fld-remote-apply-ipc-" + Guid.NewGuid().ToString("N"));
        const uint namespaceId = 0x322;
        byte[] localAuthor = Id(0x41);
        Guid ownerId = Guid.NewGuid();
        Guid clientId = Guid.NewGuid();

        using var database = FldDatabase.Open(root, namespaceId, localAuthor);
        using var dispatcher = new FldDatabaseIpcDispatcher(database);
        using var shutdown = new CancellationTokenSource();
        var server = new StorageIpcServer(
            StorageIpcEndpoint.ComputePipeName(root, 0), dispatcher.HandleAsync);
        Task serverTask = server.RunAsync(shutdown.Token);

        try
        {
            await server.Ready.WaitAsync(TimeSpan.FromSeconds(10));
            StorageIpcEndpoint.Publish(root, StorageIpcEndpoint.Create(
                root, 0, ownerId, Environment.ProcessId));

            RemoteBatchCommit batch = CreateBatch(0x52);
            await using var client = await FldDatabaseIpcClient.ConnectToDatabaseAsync(
                root, clientId, TimeSpan.FromSeconds(10));

            RemoteBatchReceipt first = await client.ApplyRemoteBatchAsync(Guid.NewGuid(), batch);
            Assert.False(first.IsDuplicate);
            Assert.Equal(2, first.AppliedCount);
            Assert.Equal(batch.BatchId, first.BatchId);
            Assert.Equal(batch.ProtocolStateDelta, first.ProtocolStateDelta);

            RemoteBatchReceipt retry = await client.ApplyRemoteBatchAsync(Guid.NewGuid(), batch);
            Assert.True(retry.IsDuplicate);
            Assert.Equal(first.CommitSeq, retry.CommitSeq);
            Assert.Equal(first.ProtocolStateDelta, retry.ProtocolStateDelta);

            Assert.True(database.TryGetBytes(batch.Writes[0].RecordId, batch.Writes[0].FieldId, out byte[]? firstValue));
            Assert.Equal(batch.Writes[0].Value, firstValue);
            Assert.True(database.TryGetBytes(batch.Writes[1].RecordId, batch.Writes[1].FieldId, out byte[]? secondValue));
            Assert.Equal(batch.Writes[1].Value, secondValue);
            Assert.Equal(0ul, database.GetOutboxHighWatermark());
            Assert.Single(database.ReadRelayBatches(batch.Writes[1].AuthorNodeId, 0, 10));
        }
        finally
        {
            shutdown.Cancel();
            try { await serverTask.WaitAsync(TimeSpan.FromSeconds(10)); } catch { }
            StorageIpcEndpoint.DeleteIfOwned(root, 0, ownerId);
            dispatcher.Dispose();
            database.Dispose();
            try { Directory.Delete(root, recursive: true); } catch { }
        }
    }

    private static RemoteBatchCommit CreateBatch(byte id)
    {
        byte[] recordA = FldDatabase.NewRecordId();
        byte[] recordB = FldDatabase.NewRecordId();
        RemoteFieldWrite first = new(recordA, 3, "remote-a"u8.ToArray(),
            100, 2, Id(0x61), Id(0x71), keyEpoch: 4);
        RemoteFieldWrite second = new(recordB, 7, "remote-b"u8.ToArray(),
            101, 1, Id(0x62), Id(0x72), keyEpoch: 5, originAuthorEpoch: 2, originSequence: 9);

        return new RemoteBatchCommit(
            senderPeer: Id(0x51),
            senderEpoch: 6,
            namespaceId: 0x322,
            issuerNodeId: Id(0x53),
            issuerEpoch: 8,
            batchId: Id(id),
            cursorStart: 9,
            cursorEnd: 10,
            previousBatchDigest: Id(0x54),
            batchDigest: Id(0x55),
            recordsRoot: Id(0x56),
            writes: [first, second],
            relayWrites: [second],
            protocolStateDelta: [0xA1, 0xB2, 0xC3]);
    }

    private static byte[] Id(byte value)
    {
        byte[] id = new byte[16];
        id[15] = value;
        return id;
    }
}
