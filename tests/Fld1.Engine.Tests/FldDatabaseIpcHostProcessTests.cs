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
        FldDatabaseIpcOutboxLease secondOutboxLease = default;
        byte[] recordId = FldDatabase.NewRecordId();
        byte[] value = "durable-database-host"u8.ToArray();
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
            }

            competingHost = StartHost(root, namespaceId);
            await competingHost.WaitForExitAsync().WaitAsync(TimeSpan.FromSeconds(45));
            Assert.NotEqual(0, competingHost.ExitCode);
            Assert.Equal(firstEndpoint.OwnerId, StorageIpcEndpoint.Read(root, 0).OwnerId);

            await StopHostAsync(firstHost);
            firstHost = null;

            secondHost = StartHost(root, namespaceId);
            StorageIpcEndpointInfo secondEndpoint = await WaitForEndpointAsync(
                root, secondHost, TimeSpan.FromSeconds(45), firstEndpoint.OwnerId);
            Assert.NotEqual(firstEndpoint.OwnerId, secondEndpoint.OwnerId);

            await using (var client = await FldDatabaseIpcClient.ConnectToDatabaseAsync(
                root, clientId, TimeSpan.FromSeconds(10)))
            {
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
                Assert.Equal(checked(firstOutboxLease.Epoch + 1), secondOutboxLease.Epoch);
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
                Assert.Equal(checked(secondOutboxLease.Epoch + 1), thirdOutboxLease.Epoch);
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

            using var recovered = FldDatabase.Open(root, namespaceId);
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
