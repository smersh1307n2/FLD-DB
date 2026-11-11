using System.Diagnostics;
using System.Globalization;
using System.Buffers.Binary;
using Fld1.Storage.Engine;
using Fld1.Storage.Ipc;
using Xunit;

namespace Fld1.Storage.Tests;

public sealed class StorageIpcHostProcessTests
{
    [Fact]
    public async Task Standalone_host_enforces_single_owner_and_replays_after_process_restart()
    {
        string root = Path.Combine(
            Path.GetTempPath(), "fld-storage-host-process-" + Guid.NewGuid().ToString("N"));
        const ulong partition = 0x2A01;
        Guid clientId = Guid.NewGuid();
        Guid operationId = Guid.NewGuid();
        byte[] key = "host-process-key"u8.ToArray();
        byte[] value = "host-process-value"u8.ToArray();
        byte[] putPayload = new byte[8 + key.Length + value.Length];
        BinaryPrimitives.WriteUInt32LittleEndian(putPayload, (uint)key.Length);
        BinaryPrimitives.WriteUInt32LittleEndian(putPayload.AsSpan(4), (uint)value.Length);
        key.CopyTo(putPayload, 8);
        value.CopyTo(putPayload, 8 + key.Length);
        byte[] operation = StorageIpcOperationProtocol.Encode(
            new StorageIpcOperationEnvelope(operationId, StorageIpcOperationCodes.Put, putPayload));

        Process? firstHost = null;
        Process? competingHost = null;
        Process? restartedHost = null;
        try
        {
            firstHost = StartHost(root, partition);
            StorageIpcEndpointInfo firstEndpoint =
                await WaitForEndpointAsync(root, partition, firstHost, TimeSpan.FromSeconds(45));

            await using (var client = await StorageIpcClientConnection.ConnectToDatabaseAsync(
                root, partition, clientId, TimeSpan.FromSeconds(10)))
            {
                Assert.Equal(new byte[] { 1 }, (await client.RequestAsync(operation)).ToArray());
            }

            competingHost = StartHost(root, partition);
            await competingHost.WaitForExitAsync().WaitAsync(TimeSpan.FromSeconds(45));
            Assert.NotEqual(0, competingHost.ExitCode);
            Assert.Equal(firstEndpoint.OwnerId, StorageIpcEndpoint.Read(root, partition).OwnerId);

            await StopHostAsync(firstHost);
            firstHost = null;

            restartedHost = StartHost(root, partition);
            StorageIpcEndpointInfo restartedEndpoint =
                await WaitForEndpointAsync(root, partition, restartedHost, TimeSpan.FromSeconds(45));
            Assert.NotEqual(firstEndpoint.OwnerId, restartedEndpoint.OwnerId);

            await using (var reconnected = await StorageIpcClientConnection.ConnectToDatabaseAsync(
                root, partition, clientId, TimeSpan.FromSeconds(10)))
            {
                Assert.Equal(new byte[] { 1 }, (await reconnected.RequestAsync(operation)).ToArray());
            }

            await StopHostAsync(restartedHost);
            restartedHost = null;

            using var recovered = StorageDatabase.Open(root, partition);
            Assert.Equal(1UL, recovered.LastCommitSeq);
            Assert.True(recovered.TryGet(key, out byte[]? recoveredValue));
            Assert.Equal(value, recoveredValue);
        }
        finally
        {
            if (firstHost is not null) await StopHostAsync(firstHost);
            if (competingHost is not null) await StopHostAsync(competingHost);
            if (restartedHost is not null) await StopHostAsync(restartedHost);
            try { Directory.Delete(root, recursive: true); } catch { }
        }
    }

    private static Process StartHost(string root, ulong partition)
    {
        string repositoryRoot = FindRepositoryRoot();
        string project = Path.Combine(
            repositoryRoot, "src", "Fld1.Storage.Host", "Fld1.Storage.Host.csproj");
        var start = new ProcessStartInfo("dotnet")
        {
            UseShellExecute = false,
            RedirectStandardOutput = true,
            RedirectStandardError = true
        };
        start.ArgumentList.Add("run");
        start.ArgumentList.Add("--project");
        start.ArgumentList.Add(project);
        start.ArgumentList.Add("--no-launch-profile");
        start.ArgumentList.Add("--");
        start.ArgumentList.Add(root);
        start.ArgumentList.Add(partition.ToString(CultureInfo.InvariantCulture));
        return Process.Start(start)
            ?? throw new InvalidOperationException("Could not start Fld1.Storage.Host.");
    }

    private static async Task<StorageIpcEndpointInfo> WaitForEndpointAsync(
        string root,
        ulong partition,
        Process host,
        TimeSpan timeout)
    {
        DateTime deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            if (host.HasExited)
            {
                string stdout = await host.StandardOutput.ReadToEndAsync();
                string stderr = await host.StandardError.ReadToEndAsync();
                throw new InvalidOperationException(
                    $"Storage Host exited with {host.ExitCode}. stdout={stdout} stderr={stderr}");
            }

            if (File.Exists(StorageIpcEndpoint.GetEndpointFilePath(root, partition)))
                return StorageIpcEndpoint.Read(root, partition);

            await Task.Delay(100);
        }

        throw new TimeoutException("Storage Host did not publish its endpoint before the deadline.");
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

        throw new DirectoryNotFoundException("Could not locate the FLD-DB repository root from the test output directory.");
    }
}
