using Fld1.Engine.Db;
using Fld1.Storage.Ipc;
using Xunit;

namespace Fld1.Engine.Db.Tests;

public sealed class FldDatabaseIpcGcLeaseTests
{
    [Fact]
    public async Task Host_gc_lease_blocks_other_sessions_allows_owner_and_releases_on_disconnect()
    {
        string root = Path.Combine(Path.GetTempPath(), "fld-ipc-gc-lease-" + Guid.NewGuid().ToString("N"));
        Guid ownerId = Guid.NewGuid();
        Guid clientAId = Guid.NewGuid();
        Guid clientBId = Guid.NewGuid();
        using var shutdown = new CancellationTokenSource();
        using FldDatabase database = FldDatabase.Open(root, 0x762, Id(0x41));
        var dispatcher = new FldDatabaseIpcDispatcher(database);
        var server = new StorageIpcServer(
            StorageIpcEndpoint.ComputePipeName(root, 0),
            dispatcher.HandleAsync,
            clientDisconnected: dispatcher.OnClientDisconnected);
        Task serverTask = server.RunAsync(shutdown.Token);
        FldDatabaseIpcClient? clientA = null;
        FldDatabaseIpcClient? clientB = null;
        FldDatabaseIpcClient? clientC = null;

        try
        {
            await server.Ready.WaitAsync(TimeSpan.FromSeconds(10));
            StorageIpcEndpoint.Publish(root, StorageIpcEndpoint.Create(
                root, 0, ownerId, Environment.ProcessId));
            clientA = await FldDatabaseIpcClient.ConnectToDatabaseAsync(
                root, clientAId, TimeSpan.FromSeconds(10));
            clientB = await FldDatabaseIpcClient.ConnectToDatabaseAsync(
                root, clientBId, TimeSpan.FromSeconds(10));
            clientC = await FldDatabaseIpcClient.ConnectToDatabaseAsync(
                root, Guid.NewGuid(), TimeSpan.FromSeconds(10));

            Guid lease = await clientA.AcquireGcLeaseAsync();
            Task<byte[]> blockedByLease = clientB.GetLocalAuthorAsync();
            await Task.Delay(100);
            Assert.False(blockedByLease.IsCompleted);

            // A competing data mutation must be excluded by the same Host-wide
            // boundary. Otherwise a new local draft could appear after the final
            // reference snapshot but before the provider's physical tombstone delete.
            byte[] recordId = FldDatabase.NewRecordId();
            var mutation = new FldDatabaseIpcMutation(
                recordId, 7, 1, false, "writer-after-gc"u8.ToArray());
            Task<FldDatabaseIpcWriteResult> blockedWriter = clientC.WriteBatchAsync(
                Guid.NewGuid(), clientSequence: 1, [mutation]);
            await Task.Delay(100);
            Assert.False(blockedWriter.IsCompleted,
                "A second IPC session must not commit a new local draft during the Host GC lease.");

            // The lease owner may query its immutable Host snapshot, but it must not
            // mutate the database through its own IPC session while the scan is active.
            FldDatabaseIpcGcReferenceScan ownerScan =
                await clientA.OpenGcReferenceScanAsync();
            Assert.Equal((ushort)0x762, ownerScan.NamespaceId);
            Assert.Equal(0UL, ownerScan.UnpublishedLocalDraftHorizon.Count);
            Assert.False(await clientA.HasGcChangeIdReferenceAsync(
                ownerScan.Token, Id(0x42)));
            StorageIpcRemoteException ownerWriteError =
                await Assert.ThrowsAsync<StorageIpcRemoteException>(() =>
                    clientA.WriteBatchAsync(
                        Guid.NewGuid(), clientSequence: 1,
                        [new FldDatabaseIpcMutation(
                            FldDatabase.NewRecordId(), 8, 1, false, "owner-write-during-gc"u8.ToArray())]));
            Assert.Contains("may only access its reference scan", ownerWriteError.Message);
            await clientA.ReleaseGcReferenceScanAsync(ownerScan.Token);
            await clientA.ReleaseGcLeaseAsync(lease);
            Assert.Equal(Id(0x41), await blockedByLease.WaitAsync(TimeSpan.FromSeconds(5)));
            FldDatabaseIpcWriteResult committed = await blockedWriter.WaitAsync(TimeSpan.FromSeconds(5));
            Assert.False(committed.IsDuplicate);
            Assert.Equal(1, committed.MutationCount);

            // A crashed/disposed coordinator cannot strand the Host-wide boundary.
            _ = await clientA.AcquireGcLeaseAsync();
            await clientA.DisposeAsync();
            clientA = null;
            Assert.Equal(Id(0x41), await clientB.GetLocalAuthorAsync().WaitAsync(TimeSpan.FromSeconds(5)));
        }
        finally
        {
            if (clientA is not null)
                await clientA.DisposeAsync();
            if (clientB is not null)
                await clientB.DisposeAsync();
            if (clientC is not null)
                await clientC.DisposeAsync();
            shutdown.Cancel();
            try { await serverTask.WaitAsync(TimeSpan.FromSeconds(10)); } catch { }
            StorageIpcEndpoint.DeleteIfOwned(root, 0, ownerId);
            dispatcher.Dispose();
        }

        try { Directory.Delete(root, recursive: true); } catch { }
    }

    [Fact]
    public async Task Host_gc_lease_renewal_extends_idle_deadline_and_preserves_reference_scan()
    {
        string root = Path.Combine(Path.GetTempPath(), "fld-ipc-gc-renew-" + Guid.NewGuid().ToString("N"));
        Guid ownerId = Guid.NewGuid();
        using var shutdown = new CancellationTokenSource();
        using FldDatabase database = FldDatabase.Open(root, 0x763, Id(0x43));
        var dispatcher = new FldDatabaseIpcDispatcher(
            database, gcLeaseIdleTimeout: TimeSpan.FromMilliseconds(300));
        var server = new StorageIpcServer(
            StorageIpcEndpoint.ComputePipeName(root, 0),
            dispatcher.HandleAsync,
            clientDisconnected: dispatcher.OnClientDisconnected);
        Task serverTask = server.RunAsync(shutdown.Token);
        FldDatabaseIpcClient? client = null;

        try
        {
            await server.Ready.WaitAsync(TimeSpan.FromSeconds(10));
            StorageIpcEndpoint.Publish(root, StorageIpcEndpoint.Create(
                root, 0, ownerId, Environment.ProcessId));
            client = await FldDatabaseIpcClient.ConnectToDatabaseAsync(
                root, Guid.NewGuid(), TimeSpan.FromSeconds(10));

            Guid lease = await client.AcquireGcLeaseAsync();
            // Keep the operation alive for longer than the original 300ms idle TTL.
            // Each explicit renewal must move the Host deadline forward.
            for (int i = 0; i < 6; i++)
            {
                await Task.Delay(150);
                await client.RenewGcLeaseAsync(lease);
            }

            FldDatabaseIpcGcReferenceScan scan = await client.OpenGcReferenceScanAsync();
            Assert.Equal((ushort)0x763, scan.NamespaceId);
            Assert.False(await client.HasGcChangeIdReferenceAsync(scan.Token, Id(0x44)));
            await client.ReleaseGcReferenceScanAsync(scan.Token);
            await client.ReleaseGcLeaseAsync(lease);
        }
        finally
        {
            if (client is not null)
                await client.DisposeAsync();
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
