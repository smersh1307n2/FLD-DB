using System.Globalization;
using Fld1.Engine.Db;
using Fld1.Storage.Ipc;

internal static class Program
{
    private static async Task<int> Main(string[] args)
    {
        if (args.Length is < 2 or > 3
            || !uint.TryParse(args[1], NumberStyles.Integer, CultureInfo.InvariantCulture, out uint namespaceId))
        {
            Console.Error.WriteLine("Usage: Fld1.Database.Host <database-root> <namespace-id-decimal> [local-author-hex]");
            return 64;
        }

        string root = Path.GetFullPath(args[0]);
        byte[]? localAuthor = null;
        if (args.Length == 3)
        {
            try { localAuthor = Convert.FromHexString(args[2]); }
            catch (FormatException)
            {
                Console.Error.WriteLine("local-author-hex must contain 32 hexadecimal characters.");
                return 64;
            }
            if (localAuthor.Length != 16)
            {
                Console.Error.WriteLine("local-author-hex must contain exactly 16 bytes.");
                return 64;
            }
        }

        Directory.CreateDirectory(root);
        try
        {
            using var database = FldDatabase.Open(root, namespaceId, localAuthor);
            Guid ownerId = Guid.NewGuid();
            using var shutdown = new CancellationTokenSource();
            ConsoleCancelEventHandler cancelHandler = (_, eventArgs) =>
            {
                eventArgs.Cancel = true;
                shutdown.Cancel();
            };
            Console.CancelKeyPress += cancelHandler;

            var dispatcher = new FldDatabaseIpcDispatcher(database);
            var server = new StorageIpcServer(
                StorageIpcEndpoint.ComputePipeName(root, 0), dispatcher.HandleAsync);
            Task serverTask = server.RunAsync(shutdown.Token);
            try
            {
                await server.Ready.WaitAsync(TimeSpan.FromSeconds(15));
                StorageIpcEndpoint.Publish(root, StorageIpcEndpoint.Create(
                    root, 0, ownerId, Environment.ProcessId));
                Console.WriteLine(
                    $"FLD Database Host ready: namespace={namespaceId} partition=0 pid={Environment.ProcessId}");
                await serverTask;
                return 0;
            }
            catch (OperationCanceledException) when (shutdown.IsCancellationRequested)
            {
                return 0;
            }
            catch (Exception exception)
            {
                Console.Error.WriteLine($"FLD Database Host failed: {exception}");
                return 1;
            }
            finally
            {
                shutdown.Cancel();
                try { await serverTask; } catch { }
                StorageIpcEndpoint.DeleteIfOwned(root, 0, ownerId);
                Console.CancelKeyPress -= cancelHandler;
            }
        }
        catch (Exception exception)
        {
            Console.Error.WriteLine($"Could not acquire FLD Database namespace {namespaceId}: {exception.Message}");
            return 1;
        }
    }
}
