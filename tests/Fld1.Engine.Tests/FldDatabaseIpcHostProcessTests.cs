using System.Diagnostics;
using System.Globalization;
using Fld1.Engine.Db;
using Fld1.Storage.Ipc;
using Xunit;

namespace Fld1.Engine.Db.Tests;

public sealed class FldDatabaseIpcHostProcessTests
{
    [Fact]
    public async Task Database_host_is_single_owner_and_replays_client_operations_after_restart()
    {
        string root = Path.Combine(Path.GetTempPath(), "fld-database-host-" + Guid.NewGuid().ToString("N"));
        const uint namespaceId = 0x432;
        Guid clientId = Guid.NewGuid();
        Guid firstOperation = Guid.NewGuid();
        FldDatabaseIpcOutboxLease firstOutboxLease = default;
        FldDatabaseIpcSnapshotHandle? firstSnapshot = null;
        FldDatabaseIpcOutboxLease secondOutboxLease = default;
        byte[] recordId = FldDatabase.NewRecordId();
        byte[] value = "durable-database-host"u8.ToArray();
        Guid nonIpcSnapshotGeneration;
        using (var seedDatabase = FldDatabase.Open(root, namespaceId))
        {
            FldProviderSnapshot nonIpcSnapshot = seedDatabase.OpenProviderSnapshot();
            nonIpcSnapshotGeneration = nonIpcSnapshot.Generation;
        }
        Process? firstHost = null;
        Process? competingHost = null;
        Process? secondHost = null;
        Process? thirdHost = null;

        try
        {
            firstHost = StartHost(root, namespaceId);
            StorageIpcEndpointInfo firstEndpoint =
                await WaitForEndpointAsync(root, firstHost, TimeSpan.FromSeconds(45));

            await using (var client = await FldDatabaseIpcClient.ConnectToDatabaseAsync(
                root, clientId, TimeSpan.FromSeconds(10)))
            {
                FldDatabaseIpcWriteResult first = await client.WriteBatchAsync(
                    firstOperation, 1,
                    [new FldDatabaseIpcMutation(recordId, 1, 0, false, value)]);
                Assert.Equal(1ul, first.CommitSeq);
                Assert.False(first.IsDuplicate);
                firstOutboxLease = await client.AcquireOutboxLeaseAsync();
                Assert.Equal(1ul, firstOutboxLease.Epoch);

                firstSnapshot = await client.OpenSnapshotAsync(Guid.NewGuid());
                FldProviderSnapshotMetadataPage firstSnapshotPage =
                    await client.ReadSnapshotPageAsync(firstSnapshot, 0, 16);
                Assert.Single(firstSnapshotPage.Entries);
            }

            competingHost = StartHost(root, namespaceId);
            await competingHost.WaitForExitAsync().WaitAsync(TimeSpan.FromSeconds(45));
            Assert.NotEqual(0, competingHost.ExitCode);
            Assert.Equal(firstEndpoint.OwnerId, StorageIpcEndpoint.Read(root, 0).OwnerId);

            await StopHostAsync(firstHost);
            firstHost = null;
            await WaitForWriterFenceReleaseAsync(root, namespaceId, TimeSpan.FromSeconds(15));

            secondHost = StartHost(root, namespaceId);
            StorageIpcEndpointInfo secondEndpoint = await WaitForEndpointAsync(
                root, secondHost, TimeSpan.FromSeconds(45), firstEndpoint.OwnerId);
            Assert.NotEqual(firstEndpoint.OwnerId, secondEndpoint.OwnerId);

            await using (var client = await FldDatabaseIpcClient.ConnectToDatabaseAsync(
                root, clientId, TimeSpan.FromSeconds(10)))
            {
                Assert.NotNull(firstSnapshot);
                await Assert.ThrowsAsync<StorageIpcRemoteException>(() =>
                    client.ReadSnapshotPageAsync(firstSnapshot!, 0, 16));
                FldDatabaseIpcSnapshotHandle secondSnapshot =
                    await client.OpenSnapshotAsync(Guid.NewGuid());
                Assert.NotEqual(firstSnapshot!.HostGeneration, secondSnapshot.HostGeneration);
                Assert.True(await client.ReleaseSnapshotAsync(secondSnapshot));

                FldDatabaseIpcWriteResult replay = await client.WriteBatchAsync(
                    firstOperation, 1,
                    [new FldDatabaseIpcMutation(recordId, 1, 0, false, value)]);
                Assert.Equal(1ul, replay.CommitSeq);
                Assert.True(replay.IsDuplicate);

                FldDatabaseIpcReadResult read = await client.ReadFieldAsync(
                    Guid.NewGuid(), 2, recordId, 1);
                Assert.True(read.Found);
                Assert.Equal(value, read.Value);
                Assert.True(await client.AcknowledgeThroughAsync(2));

                secondOutboxLease = await client.AcquireOutboxLeaseAsync();
                // Reopening Outbox invalidates a lease left by the previous Host, then acquisition advances once more.
                Assert.Equal(checked(firstOutboxLease.Epoch + 2), secondOutboxLease.Epoch);
                await Assert.ThrowsAsync<StorageIpcRemoteException>(() =>
                    client.ReadOutboxEntryChunkAsync(firstOutboxLease, afterSequence: 0));
                FldDatabaseIpcOutboxEntryChunk? firstOutboxEntry =
                    await client.ReadOutboxEntryChunkAsync(secondOutboxLease, afterSequence: 0);
                Assert.NotNull(firstOutboxEntry);
                Assert.Equal(1ul, firstOutboxEntry.OutboxSeq);
                Assert.True(await client.AcknowledgeOutboxThroughAsync(
                    secondOutboxLease, firstOutboxEntry.OutboxSeq));
                Assert.True(await client.ReleaseOutboxLeaseAsync(secondOutboxLease));
            }

            await StopHostAsync(secondHost);
            secondHost = null;
            await WaitForWriterFenceReleaseAsync(root, namespaceId, TimeSpan.FromSeconds(15));

            thirdHost = StartHost(root, namespaceId);
            await WaitForEndpointAsync(root, thirdHost, TimeSpan.FromSeconds(45), secondEndpoint.OwnerId);
            await using (var client = await FldDatabaseIpcClient.ConnectToDatabaseAsync(
                root, clientId, TimeSpan.FromSeconds(10)))
            {
                await Assert.ThrowsAsync<StorageIpcRemoteException>(() => client.WriteBatchAsync(
                    firstOperation, 1,
                    [new FldDatabaseIpcMutation(recordId, 1, 0, false, value)]));

                FldDatabaseIpcWriteResult next = await client.WriteBatchAsync(
                    Guid.NewGuid(), 3,
                    [new FldDatabaseIpcMutation(recordId, 2, 0, false, "next"u8.ToArray())]);
                Assert.Equal(4ul, next.CommitSeq);

                FldDatabaseIpcOutboxLease thirdOutboxLease = await client.AcquireOutboxLeaseAsync();
                Assert.Equal(checked(secondOutboxLease.Epoch + 2), thirdOutboxLease.Epoch);
                await Assert.ThrowsAsync<StorageIpcRemoteException>(() =>
                    client.ReadOutboxEntryChunkAsync(secondOutboxLease, afterSequence: 1));
                FldDatabaseIpcOutboxEntryChunk? nextOutboxEntry =
                    await client.ReadOutboxEntryChunkAsync(thirdOutboxLease, afterSequence: 1);
                Assert.NotNull(nextOutboxEntry);
                Assert.Equal(2ul, nextOutboxEntry.OutboxSeq);
                Assert.True(await client.AcknowledgeOutboxThroughAsync(
                    thirdOutboxLease, nextOutboxEntry.OutboxSeq));
                Assert.True(await client.ReleaseOutboxLeaseAsync(thirdOutboxLease));
            }

            await StopHostAsync(thirdHost);
            thirdHost = null;
            await WaitForWriterFenceReleaseAsync(root, namespaceId, TimeSpan.FromSeconds(15));

            using var recovered = FldDatabase.Open(root, namespaceId);
            Assert.NotNull(firstSnapshot);
            Assert.False(recovered.HasProviderSnapshot(firstSnapshot!.SnapshotToken));
            Assert.True(recovered.HasProviderSnapshot(nonIpcSnapshotGeneration));
            recovered.ReleaseProviderSnapshot(nonIpcSnapshotGeneration);
            Assert.True(recovered.TryGetBytes(recordId, 1, out byte[]? firstValue));
            Assert.Equal(value, firstValue);
            Assert.True(recovered.TryGetBytes(recordId, 2, out byte[]? secondValue));
            Assert.Equal("next"u8.ToArray(), secondValue);
            Assert.Equal(2ul, recovered.GetOutboxWatermark());
            Assert.Empty(recovered.ReadOutboxBatch(0, 10));
        }
        finally
        {
            if (firstHost is not null) await StopHostAsync(firstHost);
            if (competingHost is not null) await StopHostAsync(competingHost);
            if (secondHost is not null) await StopHostAsync(secondHost);
            if (thirdHost is not null) await StopHostAsync(thirdHost);
            try { Directory.Delete(root, recursive: true); } catch { }
        }
    }

    [Fact]
    public async Task Separate_host_process_serializes_concurrent_provider_clients_and_replays_batches_after_restart()
    {
        string root = Path.Combine(Path.GetTempPath(), "fld-provider-host-process-" + Guid.NewGuid().ToString("N"));
        const uint namespaceId = 0x433;
        Guid clientAId = Guid.NewGuid();
        Guid clientBId = Guid.NewGuid();
        Guid operationA = Guid.NewGuid();
        Guid operationB = Guid.NewGuid();
        byte[] recordA = FldDatabase.NewRecordId();
        byte[] recordB = FldDatabase.NewRecordId();
        byte[] changeA = Id(0x71);
        byte[] changeB = Id(0x72);
        byte[] valueA = "provider-client-a"u8.ToArray();
        byte[] valueB = "provider-client-b"u8.ToArray();
        var mutationA = new FldLocalProviderMutation(
            recordA, 0x0441, valueA, changeA, 7, 223456, 3);
        var mutationB = new FldLocalProviderMutation(
            recordB, 0x0442, valueB, changeB, 7, 223457, 4);

        Process? firstHost = null;
        Process? secondHost = null;
        FldDatabaseIpcOutboxLease firstLease = default;
        try
        {
            firstHost = StartHost(root, namespaceId);
            StorageIpcEndpointInfo firstEndpoint =
                await WaitForEndpointAsync(root, firstHost, TimeSpan.FromSeconds(45));

            Task<FldDatabaseIpcClient> connectA = FldDatabaseIpcClient.ConnectToDatabaseAsync(
                root, clientAId, TimeSpan.FromSeconds(10));
            Task<FldDatabaseIpcClient> connectB = FldDatabaseIpcClient.ConnectToDatabaseAsync(
                root, clientBId, TimeSpan.FromSeconds(10));
            await using (FldDatabaseIpcClient clientA = await connectA)
            await using (FldDatabaseIpcClient clientB = await connectB)
            {
                Task<FldDatabaseIpcWriteResult> writeA = clientA.AppendLocalProviderBatchAsync(
                    operationA, 1, [mutationA]);
                Task<FldDatabaseIpcWriteResult> writeB = clientB.AppendLocalProviderBatchAsync(
                    operationB, 1, [mutationB]);
                FldDatabaseIpcWriteResult[] writes = await Task.WhenAll(writeA, writeB)
                    .WaitAsync(TimeSpan.FromSeconds(20));

                Assert.All(writes, result => Assert.False(result.IsDuplicate));
                Assert.Equal(new ulong[] { 1, 2 }, writes.Select(result => result.CommitSeq).Order().ToArray());

                firstLease = await clientA.AcquireOutboxLeaseAsync();
                Assert.Equal(1ul, firstLease.Epoch);
                FldDatabaseIpcOutboxEntryChunk? firstEntry =
                    await clientA.ReadOutboxEntryChunkAsync(firstLease, afterSequence: 0);
                Assert.NotNull(firstEntry);
                FldDatabaseIpcOutboxEntryChunk? secondEntry =
                    await clientA.ReadOutboxEntryChunkAsync(firstLease, afterSequence: firstEntry.OutboxSeq);
                Assert.NotNull(secondEntry);
                Assert.Null(await clientA.ReadOutboxEntryChunkAsync(
                    firstLease, afterSequence: secondEntry.OutboxSeq));
                Assert.Contains(new[] { firstEntry.ChangeId, secondEntry.ChangeId },
                    item => item.AsSpan().SequenceEqual(changeA));
                Assert.Contains(new[] { firstEntry.ChangeId, secondEntry.ChangeId },
                    item => item.AsSpan().SequenceEqual(changeB));
            }

            await StopHostAsync(firstHost);
            firstHost = null;
            await WaitForWriterFenceReleaseAsync(root, namespaceId, TimeSpan.FromSeconds(15));
            secondHost = StartHost(root, namespaceId);
            await WaitForEndpointAsync(root, secondHost, TimeSpan.FromSeconds(45), firstEndpoint.OwnerId);

            await using (FldDatabaseIpcClient clientA = await FldDatabaseIpcClient.ConnectToDatabaseAsync(
                root, clientAId, TimeSpan.FromSeconds(10)))
            await using (FldDatabaseIpcClient clientB = await FldDatabaseIpcClient.ConnectToDatabaseAsync(
                root, clientBId, TimeSpan.FromSeconds(10)))
            {
                Task<FldDatabaseIpcWriteResult> replayA = clientA.AppendLocalProviderBatchAsync(
                    operationA, 1, [mutationA]);
                Task<FldDatabaseIpcWriteResult> replayB = clientB.AppendLocalProviderBatchAsync(
                    operationB, 1, [mutationB]);
                FldDatabaseIpcWriteResult[] replays = await Task.WhenAll(replayA, replayB)
                    .WaitAsync(TimeSpan.FromSeconds(20));

                Assert.All(replays, result => Assert.True(result.IsDuplicate));
                Assert.Equal(new ulong[] { 1, 2 }, replays.Select(result => result.CommitSeq).Order().ToArray());

                FldDatabaseIpcOutboxLease recoveredLease = await clientA.AcquireOutboxLeaseAsync();
                Assert.Equal(checked(firstLease.Epoch + 2), recoveredLease.Epoch);
                FldDatabaseIpcOutboxEntryChunk? firstRecovered =
                    await clientA.ReadOutboxEntryChunkAsync(recoveredLease, afterSequence: 0);
                Assert.NotNull(firstRecovered);
                FldDatabaseIpcOutboxEntryChunk? secondRecovered =
                    await clientA.ReadOutboxEntryChunkAsync(recoveredLease, afterSequence: firstRecovered.OutboxSeq);
                Assert.NotNull(secondRecovered);
                Assert.Null(await clientA.ReadOutboxEntryChunkAsync(
                    recoveredLease, afterSequence: secondRecovered.OutboxSeq));
                Assert.Contains(new[] { firstRecovered.ChangeId, secondRecovered.ChangeId },
                    item => item.AsSpan().SequenceEqual(changeA));
                Assert.Contains(new[] { firstRecovered.ChangeId, secondRecovered.ChangeId },
                    item => item.AsSpan().SequenceEqual(changeB));
                Assert.True(await clientA.ReleaseOutboxLeaseAsync(recoveredLease));
            }

            await StopHostAsync(secondHost);
            secondHost = null;
            await WaitForWriterFenceReleaseAsync(root, namespaceId, TimeSpan.FromSeconds(15));

            using var recoveredDatabase = FldDatabase.Open(root, namespaceId);
            Assert.Equal(2ul, recoveredDatabase.GetOutboxHighWatermark());
            Assert.Equal(2, recoveredDatabase.ReadOutboxBatch(0, 10).Count);
            Assert.True(recoveredDatabase.TryResolve(recordA, 0x0441, out var resolvedA));
            Assert.Equal(changeA, resolvedA.ChangeId);
            Assert.Equal(7u, resolvedA.OriginAuthorEpoch);
            Assert.Equal(0ul, resolvedA.OriginSequence);
            Assert.Equal(223456ul, resolvedA.Wall);
            Assert.True(recoveredDatabase.TryResolve(recordB, 0x0442, out var resolvedB));
            Assert.Equal(changeB, resolvedB.ChangeId);
            Assert.Equal(7u, resolvedB.OriginAuthorEpoch);
            Assert.Equal(0ul, resolvedB.OriginSequence);
            Assert.Equal(223457ul, resolvedB.Wall);
        }
        finally
        {
            if (firstHost is not null) await StopHostAsync(firstHost);
            if (secondHost is not null) await StopHostAsync(secondHost);
            try { Directory.Delete(root, recursive: true); } catch { }
        }
    }

    private static byte[] Id(byte value)
    {
        byte[] id = new byte[16];
        id[15] = value;
        return id;
    }

    private static Process StartHost(string root, uint namespaceId)
    {
        string repositoryRoot = FindRepositoryRoot();
        string project = Path.Combine(
            repositoryRoot, "src", "Fld1.Database.Host", "Fld1.Database.Host.csproj");
        var start = new ProcessStartInfo("dotnet")
        {
            UseShellExecute = false,
            RedirectStandardOutput = true,
            RedirectStandardError = true
        };
        start.ArgumentList.Add("run");
        start.ArgumentList.Add("--project");
        start.ArgumentList.Add(project);
#if DEBUG
        start.ArgumentList.Add("--configuration");
        start.ArgumentList.Add("Debug");
#else
        start.ArgumentList.Add("--configuration");
        start.ArgumentList.Add("Release");
#endif
        start.ArgumentList.Add("--no-build");
        start.ArgumentList.Add("--no-launch-profile");
        start.ArgumentList.Add("--");
        start.ArgumentList.Add(root);
        start.ArgumentList.Add(namespaceId.ToString(CultureInfo.InvariantCulture));
        return Process.Start(start)
            ?? throw new InvalidOperationException("Could not start Fld1.Database.Host.");
    }

    private static async Task<StorageIpcEndpointInfo> WaitForEndpointAsync(
        string root,
        Process host,
        TimeSpan timeout,
        Guid? previousOwnerId = null)
    {
        DateTime deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            if (host.HasExited)
            {
                string stdout = await host.StandardOutput.ReadToEndAsync();
                string stderr = await host.StandardError.ReadToEndAsync();
                throw new InvalidOperationException(
                    $"Database Host exited with {host.ExitCode}. stdout={stdout} stderr={stderr}");
            }

            if (File.Exists(StorageIpcEndpoint.GetEndpointFilePath(root, 0)))
            {
                StorageIpcEndpointInfo endpoint = StorageIpcEndpoint.Read(root, 0);
                if (previousOwnerId is null || endpoint.OwnerId != previousOwnerId.Value)
                    return endpoint;
            }
            await Task.Delay(100);
        }
        throw new TimeoutException("FLD Database Host did not publish its endpoint before the deadline.");
    }

    private static async Task WaitForWriterFenceReleaseAsync(
        string root, uint namespaceId, TimeSpan timeout)
    {
        DateTime deadline = DateTime.UtcNow + timeout;
        IOException? lastFailure = null;
        while (DateTime.UtcNow < deadline)
        {
            try
            {
                using var probe = FldDatabase.Open(root, namespaceId);
                return;
            }
            catch (IOException exception)
            {
                lastFailure = exception;
                await Task.Delay(50);
            }
        }

        throw new TimeoutException(
            "Database Host exited but its WAL writer fence was not released before the deadline.",
            lastFailure);
    }

    private static async Task StopHostAsync(Process host)
    {
        try
        {
            if (!host.HasExited)
                host.Kill(entireProcessTree: true);
            await host.WaitForExitAsync().WaitAsync(TimeSpan.FromSeconds(15));
        }
        catch (InvalidOperationException) { }
        catch (System.ComponentModel.Win32Exception) { }
        finally
        {
            host.Dispose();
        }
    }

    private static string FindRepositoryRoot()
    {
        DirectoryInfo? directory = new(AppContext.BaseDirectory);
        while (directory is not null)
        {
            if (File.Exists(Path.Combine(directory.FullName, "Fld.Db.slnx")))
                return directory.FullName;
            directory = directory.Parent;
        }
        throw new DirectoryNotFoundException("Could not locate FLD-DB repository root.");
    }
}
