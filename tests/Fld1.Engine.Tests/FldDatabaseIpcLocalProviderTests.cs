using Fld1.Engine.Db;
using Fld1.Engine.Storage;
using Fld1.Storage.Ipc;
using Xunit;

namespace Fld1.Engine.Db.Tests;

public sealed class FldDatabaseIpcLocalProviderTests
{
    [Fact]
    public void Local_provider_codec_roundtrips_identity_and_rejects_trailing_bytes()
    {
        FldLocalProviderMutation mutation = Mutation();
        byte[] encoded = FldDatabaseIpcLocalProviderProtocol.EncodeRequest(7, [mutation]);
        (ulong sequence, IReadOnlyList<FldLocalProviderMutation> decoded) =
            FldDatabaseIpcLocalProviderProtocol.DecodeRequest(encoded);

        Assert.Equal(7ul, sequence);
        FldLocalProviderMutation actual = Assert.Single(decoded);
        Assert.Equal(mutation.RecordId, actual.RecordId);
        Assert.Equal(mutation.FieldId, actual.FieldId);
        Assert.Equal(mutation.Value, actual.Value);
        Assert.Equal(mutation.ChangeId, actual.ChangeId);
        Assert.Equal(mutation.OriginAuthorEpoch, actual.OriginAuthorEpoch);
        Assert.Equal(mutation.Wall, actual.Wall);
        Assert.Equal(mutation.Logical, actual.Logical);
        Assert.Equal(mutation.KeyEpoch, actual.KeyEpoch);
        Assert.Equal(mutation.IsTombstone, actual.IsTombstone);

        Assert.Throws<InvalidDataException>(() =>
            FldDatabaseIpcLocalProviderProtocol.DecodeRequest(encoded.Concat(new byte[] { 0 }).ToArray()));
    }

    [Fact]
    public void Local_provider_codec_preserves_field_tombstones_and_canonical_record_tombstones()
    {
        var fieldTombstone = new FldLocalProviderMutation(
            FldDatabase.NewRecordId(), 4, Array.Empty<byte>(), Id(0x75),
            OriginAuthorEpoch: 12, Wall: 123457, Logical: 8,
            KeyEpoch: 0, IsTombstone: true);
        var recordTombstone = new FldLocalProviderMutation(
            FldDatabase.NewRecordId(), FldLifecycle.FieldId, FldLifecycle.DeadValue.ToArray(), Id(0x76),
            OriginAuthorEpoch: 12, Wall: 123458, Logical: 9,
            KeyEpoch: 0, IsTombstone: false);

        byte[] encoded = FldDatabaseIpcLocalProviderProtocol.EncodeRequest(
            8, [fieldTombstone, recordTombstone]);
        (ulong sequence, IReadOnlyList<FldLocalProviderMutation> decoded) =
            FldDatabaseIpcLocalProviderProtocol.DecodeRequest(encoded);

        Assert.Equal(8ul, sequence);
        Assert.Equal(2, decoded.Count);
        Assert.True(decoded[0].IsTombstone);
        Assert.Empty(decoded[0].Value);
        Assert.Equal(FldLifecycle.FieldId, decoded[1].FieldId);
        Assert.Equal(FldLifecycle.DeadValue, decoded[1].Value);

        var invalidLifecycle = recordTombstone with { Value = "not-dead"u8.ToArray() };
        Assert.Throws<ArgumentException>(() =>
            FldDatabaseIpcLocalProviderProtocol.EncodeRequest(9, [invalidLifecycle]));
        var invalidFieldDelete = fieldTombstone with { Value = "must-be-empty"u8.ToArray() };
        Assert.Throws<ArgumentException>(() =>
            FldDatabaseIpcLocalProviderProtocol.EncodeRequest(9, [invalidFieldDelete]));
    }

    [Fact]
    public async Task Local_provider_ipc_preserves_change_id_hlc_and_epoch_and_retries_atomically()
    {
        string root = Path.Combine(Path.GetTempPath(), "fld-local-provider-ipc-" + Guid.NewGuid().ToString("N"));
        const uint namespaceId = 0x323;
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

            FldLocalProviderMutation mutation = Mutation();
            Guid operationId = Guid.NewGuid();
            await using var client = await FldDatabaseIpcClient.ConnectToDatabaseAsync(
                root, clientId, TimeSpan.FromSeconds(10));

            FldDatabaseIpcWriteResult first = await client.AppendLocalProviderBatchAsync(
                operationId, 1, [mutation]);
            Assert.False(first.IsDuplicate);
            Assert.Equal(1, first.MutationCount);

            FldDatabaseIpcWriteResult retry = await client.AppendLocalProviderBatchAsync(
                operationId, 1, [mutation]);
            Assert.True(retry.IsDuplicate);
            Assert.Equal(first.CommitSeq, retry.CommitSeq);
            Assert.Equal(1ul, database.GetOutboxHighWatermark());

            var entry = Assert.Single(database.ReadOutboxBatch(0, 10));
            Assert.Equal(mutation.ChangeId, entry.ChangeId);
            Assert.Equal(localAuthor, entry.AuthorNodeId);
            Assert.Equal(mutation.RecordId, entry.RecordId);
            Assert.Equal(mutation.FieldId, entry.FieldId);
            Assert.Equal(mutation.Wall, entry.Wall);
            Assert.Equal(mutation.Logical, entry.Logical);
            Assert.Equal(mutation.KeyEpoch, entry.KeyEpoch);
            Assert.Equal(mutation.Value, entry.Value);

            Assert.True(database.TryResolve(mutation.RecordId, mutation.FieldId, out FieldResolver.Resolved resolved));
            Assert.Equal(mutation.ChangeId, resolved.ChangeId);
            Assert.Equal(mutation.OriginAuthorEpoch, resolved.OriginAuthorEpoch);
            Assert.Equal(0ul, resolved.OriginSequence);
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

    private static FldLocalProviderMutation Mutation()
        => new(
            FldDatabase.NewRecordId(),
            4,
            "stable-provider-value"u8.ToArray(),
            Id(0x74),
            OriginAuthorEpoch: 12,
            Wall: 123456,
            Logical: 7,
            KeyEpoch: 9);

    private static byte[] Id(byte value)
    {
        byte[] id = new byte[16];
        id[15] = value;
        return id;
    }
}
