using System.Globalization;
using Fld1.Storage.Engine;
using Fld1.Storage.Ipc;

internal static class Program
{
    private static async Task<int> Main(string[] args)
    {
        if (args.Length != 2 ||
            !ulong.TryParse(args[1], NumberStyles.Integer, CultureInfo.InvariantCulture, out ulong partitionId))
        {
            Console.Error.WriteLine("Usage: Fld1.Storage.Host <root-directory> <partition-id-decimal>");
            return 64;
        }

        string rootDirectory = Path.GetFullPath(args[0]);
        Directory.CreateDirectory(rootDirectory);

        try
        {
            using var database = StorageDatabase.Open(rootDirectory, partitionId);
            Guid ownerId = Guid.NewGuid();
            using var shutdown = new CancellationTokenSource();
            ConsoleCancelEventHandler cancelHandler = (_, eventArgs) =>
            {
                eventArgs.Cancel = true;
                shutdown.Cancel();
            };
            Console.CancelKeyPress += cancelHandler;

            StorageIpcEndpointInfo endpoint = StorageIpcEndpoint.Create(
                rootDirectory, partitionId, ownerId, Environment.ProcessId);
            var dispatcher = new StorageIpcOperationDispatcher(database);
            var server = new StorageIpcServer(endpoint.PipeName, dispatcher.HandleAsync);
            Task serverTask = server.RunAsync(shutdown.Token);

            try
            {
                await server.Ready.WaitAsync(TimeSpan.FromSeconds(10));
                StorageIpcEndpoint.Publish(rootDirectory, endpoint);
                Console.WriteLine(
                    $"FLD Storage Host ready: partition={partitionId} pipe={endpoint.PipeName} pid={endpoint.ProcessId}");
                await serverTask;
                return 0;
            }
            catch (OperationCanceledException) when (shutdown.IsCancellationRequested)
            {
                return 0;
            }
            catch (Exception exception)
            {
                Console.Error.WriteLine($"FLD Storage Host failed: {exception}");
                return 1;
            }
            finally
            {
                shutdown.Cancel();
                try { await serverTask; } catch { }
                StorageIpcEndpoint.DeleteIfOwned(rootDirectory, partitionId, ownerId);
                Console.CancelKeyPress -= cancelHandler;
            }
        }
        catch (Exception exception)
        {
            Console.Error.WriteLine($"Could not acquire Storage partition {partitionId}: {exception.Message}");
            return 1;
        }
    }
}
