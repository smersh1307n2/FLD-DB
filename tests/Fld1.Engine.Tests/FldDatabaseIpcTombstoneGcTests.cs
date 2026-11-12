using Fld1.Engine.Db;
using Fld1.Engine.Sync;
using Fld1.Storage.Ipc;
using Xunit;

namespace Fld1.Engine.Db.Tests;

public sealed class FldDatabaseIpcTombstoneGcTests
{
    [Fact]
    public async Task Host_revalidates_exact_tombstone_candidates_and_blocks_relay_references_and_snapshot_pins()
    {
        string root = Path.Combine(Path.GetTempPath(), "fld-ipc-tombstone-gc-" + Guid.NewGuid().ToString("N"));
        const uint namespaceId = 0x761;
        Guid ownerId = Guid.NewGuid();
        Guid clientId = Guid.NewGuid();
        byte[] author = Id(0x61);
        byte[] retainedRecord = FldDatabase.NewRecordId();
        byte[] reclaimableRecord = FldDatabase.NewRecordId();
        var retained = new RemoteFieldWrite(
            retainedRecord, 5, Array.Empty<byte>(), 100, 1, author, Id(0x71),
            keyEpoch: 0, isTombstone: true, originAuthorEpoch: 8, originSequence: 9);
        var reclaimable = new RemoteFieldWrite(
            reclaimableRecord, 5, Array.Empty<byte>(), 101, 1, author, Id(0x72),
            keyEpoch: 0, isTombstone: true, originAuthorEpoch: 8, originSequence: 10);
        var batch = new RemoteBatchCommit(
            senderPeer: Id(0x51),
            senderEpoch: 6,
            namespaceId: checked((int)namespaceId),
            issuerNodeId: Id(0x53),
            issuerEpoch: 8,
            batchId: Id(0x52),
            cursorStart: 9,
            cursorEnd: 10,
            previousBatchDigest: Id(0x54),
            batchDigest: Id(0x55),
            recordsRoot: Id(0x56),
            writes: [retained, reclaimable],
            relayWrites: [retained],
            protocolStateDelta: [0xA1, 0xB2]);

        FldDatabase database = FldDatabase.Open(root, namespaceId, Id(0x41));
        var receipt = database.ApplyRemoteBatch(batch);
        Assert.False(receipt.IsDuplicate);
        Assert.NotEqual(0UL, database.Checkpoint());

        var dispatcher = new FldDatabaseIpcDispatcher(database, enableTombstoneGc: true);
        using var shutdown = new CancellationTokenSource();
        var server = new StorageIpcServer(
            StorageIpcEndpoint.ComputePipeName(root, 0), dispatcher.HandleAsync);
        Task serverTask = server.RunAsync(shutdown.Token);

        try
        {
            await server.Ready.WaitAsync(TimeSpan.FromSeconds(10));
            StorageIpcEndpoint.Publish(root, StorageIpcEndpoint.Create(
                root, 0, ownerId, Environment.ProcessId));

            await using var client = await FldDatabaseIpcClient.ConnectToDatabaseAsync(
                root, clientId, TimeSpan.FromSeconds(10));
            FldDatabaseIpcSnapshotHandle discovery = await client.OpenSnapshotAsync(Guid.NewGuid());
            FldProviderSnapshotMetadataPage page = await client.ReadSnapshotPageAsync(discovery, 0, 32);
            Assert.True(await client.ReleaseSnapshotAsync(discovery));

            FldProviderSnapshotEntryMetadata retainedEntry = Assert.Single(page.Entries,
                entry => entry.RecordId.AsSpan().SequenceEqual(retainedRecord));
            FldProviderSnapshotEntryMetadata reclaimableEntry = Assert.Single(page.Entries,
                entry => entry.RecordId.AsSpan().SequenceEqual(reclaimableRecord));
            Assert.True(retainedEntry.IsTombstone);
            Assert.True(reclaimableEntry.IsTombstone);

            FldTombstoneGcCandidate[] approved =
            [
                ToCandidate(retainedEntry),
                ToCandidate(reclaimableEntry)
            ];

            // A separate live provider snapshot pins the old generation. The final
            // Host RPC must refuse the whole pass even though the client approved both.
            FldDatabaseIpcSnapshotHandle pinned = await client.OpenSnapshotAsync(Guid.NewGuid());
            Assert.Equal(0, await client.CompactTombstonesAsync(approved, limit: 8));
            Assert.True(await client.ReleaseSnapshotAsync(pinned));

            // The ApplyJournal still carries the first tombstone as a relay reference.
            // Only the second exact identity is eligible for physical removal.
            Assert.Equal(1, await client.CompactTombstonesAsync(approved, limit: 8));
            Assert.True(database.TryResolve(retainedRecord, 5, out var retainedAfterGc));
            Assert.True(retainedAfterGc.IsTombstone);
            Assert.False(database.TryResolve(reclaimableRecord, 5, out _));
        }
        finally
        {
            shutdown.Cancel();
            try { await serverTask.WaitAsync(TimeSpan.FromSeconds(10)); } catch { }
            StorageIpcEndpoint.DeleteIfOwned(root, 0, ownerId);
            dispatcher.Dispose();
            database.Dispose();
        }

        try
        {
            using var reopened = FldDatabase.Open(root, namespaceId, Id(0x41));
            Assert.True(reopened.TryResolve(retainedRecord, 5, out var persistedRetained));
            Assert.True(persistedRetained.IsTombstone);
            Assert.False(reopened.TryResolve(reclaimableRecord, 5, out _));
        }
        finally
        {
            try { Directory.Delete(root, recursive: true); } catch { }
        }
    }

    private static FldTombstoneGcCandidate ToCandidate(FldProviderSnapshotEntryMetadata entry)
        => new(
            entry.RecordId,
            entry.FieldId,
            entry.ChangeId,
            entry.AuthorNodeId,
            entry.HlcWall,
            entry.HlcLogical,
            entry.OriginAuthorEpoch,
            entry.OriginSequence,
            entry.IsRecordTombstone,
            entry.IsTombstone);

    private static byte[] Id(byte value)
    {
        byte[] id = new byte[16];
        id[0] = value;
        return id;
    }
}
