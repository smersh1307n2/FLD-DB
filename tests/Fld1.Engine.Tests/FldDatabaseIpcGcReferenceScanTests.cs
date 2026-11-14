using Fld1.Engine.Db;
using Fld1.Engine.Sync;
using Fld1.Storage.Ipc;
using Xunit;

namespace Fld1.Engine.Db.Tests;

public sealed class FldDatabaseIpcGcReferenceScanTests
{
    [Fact]
    public async Task Host_gc_reference_scan_exposes_one_immutable_reference_index_and_is_session_owned()
    {
        string root = Path.Combine(
            Path.GetTempPath(), "fld-ipc-gc-reference-scan-" + Guid.NewGuid().ToString("N"));
        const uint namespaceId = 0x763;
        Guid ownerId = Guid.NewGuid();
        Guid clientAId = Guid.NewGuid();
        Guid clientBId = Guid.NewGuid();
        byte[] author = Id(0x61);
        byte[] sender = Id(0x51);
        byte[] issuer = Id(0x53);
        byte[] retainedRecord = FldDatabase.NewRecordId();
        byte[] retainedChangeId = Id(0x71);
        byte[] batchId = Id(0x52);

        var retained = new RemoteFieldWrite(
            retainedRecord, 5, Array.Empty<byte>(), 100, 1, issuer, retainedChangeId,
            keyEpoch: 0, isTombstone: true, originAuthorEpoch: 8, originSequence: 9);
        var batch = new RemoteBatchCommit(
            senderPeer: sender,
            senderEpoch: 6,
            namespaceId: checked((int)namespaceId),
            issuerNodeId: issuer,
            issuerEpoch: 8,
            batchId: batchId,
            cursorStart: 9,
            cursorEnd: 9,
            previousBatchDigest: Id(0x54),
            batchDigest: Id(0x55),
            recordsRoot: Id(0x56),
            writes: [retained],
            relayWrites: [retained],
            protocolStateDelta: [0xA1]);

        using FldDatabase database = FldDatabase.Open(root, namespaceId, author);
        Assert.False(database.ApplyRemoteBatch(batch).IsDuplicate);
        Assert.NotEqual(0UL, database.Checkpoint());

        var dispatcher = new FldDatabaseIpcDispatcher(database);
        using var shutdown = new CancellationTokenSource();
        var server = new StorageIpcServer(
            StorageIpcEndpoint.ComputePipeName(root, 0),
            dispatcher.HandleAsync,
            clientDisconnected: dispatcher.OnClientDisconnected);
        Task serverTask = server.RunAsync(shutdown.Token);
        FldDatabaseIpcClient? clientA = null;
        FldDatabaseIpcClient? clientB = null;

        try
        {
            await server.Ready.WaitAsync(TimeSpan.FromSeconds(10));
            StorageIpcEndpoint.Publish(root, StorageIpcEndpoint.Create(
                root, 0, ownerId, Environment.ProcessId));
            clientA = await FldDatabaseIpcClient.ConnectToDatabaseAsync(
                root, clientAId, TimeSpan.FromSeconds(10));
            clientB = await FldDatabaseIpcClient.ConnectToDatabaseAsync(
                root, clientBId, TimeSpan.FromSeconds(10));

            Guid lease = await clientA.AcquireGcLeaseAsync();
            FldDatabaseIpcGcReferenceScan scan = await clientA.OpenGcReferenceScanAsync();
            Assert.NotEqual(Guid.Empty, scan.Token);
            Assert.Equal(checked((ushort)namespaceId), scan.NamespaceId);
            Assert.False(scan.HasSnapshotPin);
            Assert.Equal(0UL, scan.UnpublishedLocalDraftHorizon.Count);

            Assert.True(await clientA.HasGcChangeIdReferenceAsync(scan.Token, retainedChangeId));
            Assert.True(await clientA.HasGcRecordReferenceAsync(
                scan.Token, retainedRecord, retainedChangeId));
            Assert.False(await clientA.HasGcReceiptReferenceAsync(
                scan.Token, sender, authorEpoch: 8, transportPeerEpoch: 6, batchId),
                "A sealed ApplyJournal receipt is independent FLD replay evidence, not a BIN-SYNC Metadata reference.");
            Assert.False(await clientA.HasGcChangeIdReferenceAsync(scan.Token, Id(0x79)));

            // The exclusive lease blocks unrelated sessions, but its owner can query
            // the immutable scan without reacquiring the Host boundary.
            Task<byte[]> blocked = clientB.GetLocalAuthorAsync();
            await Task.Delay(100);
            Assert.False(blocked.IsCompleted);
            Assert.True(await clientA.HasGcChangeIdReferenceAsync(scan.Token, retainedChangeId));

            await clientA.ReleaseGcReferenceScanAsync(scan.Token);
            await clientA.ReleaseGcReferenceScanAsync(scan.Token);
            await clientA.ReleaseGcLeaseAsync(lease);
            await clientA.ReleaseGcLeaseAsync(lease);
            Assert.Equal(author, await blocked.WaitAsync(TimeSpan.FromSeconds(5)));

            // A newly appended, unsealed WAL commit is a global fail-closed barrier:
            // even unrelated receipt and ChangeId probes must report a live reference.
            byte[] secondRecord = FldDatabase.NewRecordId();
            byte[] secondChangeId = Id(0x59);
            var secondMutation = new RemoteFieldWrite(
                secondRecord, 6, "unsealed-gc-barrier"u8.ToArray(), 101, 1,
                Id(0x58), secondChangeId, keyEpoch: 0, isTombstone: false,
                originAuthorEpoch: 9, originSequence: 10);
            var secondBatch = new RemoteBatchCommit(
                senderPeer: Id(0x57),
                senderEpoch: 7,
                namespaceId: checked((int)namespaceId),
                issuerNodeId: Id(0x58),
                issuerEpoch: 9,
                batchId: Id(0x5A),
                cursorStart: 1,
                cursorEnd: 1,
                previousBatchDigest: Id(0x5B),
                batchDigest: Id(0x5C),
                recordsRoot: Id(0x5D),
                writes: [secondMutation],
                relayWrites: [],
                protocolStateDelta: [0xA2]);
            Assert.False(database.ApplyRemoteBatch(secondBatch).IsDuplicate);

            Guid secondLease = await clientA.AcquireGcLeaseAsync();
            FldDatabaseIpcGcReferenceScan unsealedScan = await clientA.OpenGcReferenceScanAsync();
            Assert.True(unsealedScan.UnpublishedLocalDraftHorizon.Count > 0);
            Assert.True(await clientA.HasGcReceiptReferenceAsync(
                unsealedScan.Token, Id(0x60), authorEpoch: 12,
                transportPeerEpoch: 13, batchId: Id(0x61)));
            Assert.True(await clientA.HasGcChangeIdReferenceAsync(
                unsealedScan.Token, Id(0x62)));
            await clientA.ReleaseGcReferenceScanAsync(unsealedScan.Token);
            await clientA.ReleaseGcLeaseAsync(secondLease);
        }
        finally
        {
            if (clientA is not null)
                await clientA.DisposeAsync();
            if (clientB is not null)
                await clientB.DisposeAsync();
            shutdown.Cancel();
            try { await serverTask.WaitAsync(TimeSpan.FromSeconds(10)); } catch { }
            StorageIpcEndpoint.DeleteIfOwned(root, 0, ownerId);
            dispatcher.Dispose();
        }

        try { Directory.Delete(root, recursive: true); } catch { }
    }

    [Fact]
    public async Task Host_gc_reference_scan_is_released_when_owning_session_disconnects()
    {
        string root = Path.Combine(
            Path.GetTempPath(), "fld-ipc-gc-reference-disconnect-" + Guid.NewGuid().ToString("N"));
        Guid ownerId = Guid.NewGuid();
        using FldDatabase database = FldDatabase.Open(root, 0x764, Id(0x41));
        var dispatcher = new FldDatabaseIpcDispatcher(database);
        using var shutdown = new CancellationTokenSource();
        var server = new StorageIpcServer(
            StorageIpcEndpoint.ComputePipeName(root, 0),
            dispatcher.HandleAsync,
            clientDisconnected: dispatcher.OnClientDisconnected);
        Task serverTask = server.RunAsync(shutdown.Token);
        FldDatabaseIpcClient? owner = null;
        FldDatabaseIpcClient? waiter = null;

        try
        {
            await server.Ready.WaitAsync(TimeSpan.FromSeconds(10));
            StorageIpcEndpoint.Publish(root, StorageIpcEndpoint.Create(
                root, 0, ownerId, Environment.ProcessId));
            owner = await FldDatabaseIpcClient.ConnectToDatabaseAsync(
                root, Guid.NewGuid(), TimeSpan.FromSeconds(10));
            waiter = await FldDatabaseIpcClient.ConnectToDatabaseAsync(
                root, Guid.NewGuid(), TimeSpan.FromSeconds(10));

            _ = await owner.AcquireGcLeaseAsync();
            _ = await owner.OpenGcReferenceScanAsync();
            await owner.DisposeAsync();
            owner = null;

            Assert.Equal(Id(0x41),
                await waiter.GetLocalAuthorAsync().WaitAsync(TimeSpan.FromSeconds(5)));
        }
        finally
        {
            if (owner is not null)
                await owner.DisposeAsync();
            if (waiter is not null)
                await waiter.DisposeAsync();
            shutdown.Cancel();
            try { await serverTask.WaitAsync(TimeSpan.FromSeconds(10)); } catch { }
            StorageIpcEndpoint.DeleteIfOwned(root, 0, ownerId);
            dispatcher.Dispose();
        }

        try { Directory.Delete(root, recursive: true); } catch { }
    }

    private static byte[] Id(byte value)
    {
        byte[] id = new byte[16];
        id[0] = value;
        return id;
    }
}
