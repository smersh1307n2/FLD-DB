using System.Globalization;
using Fld1.Engine.Db;
using Fld1.Storage.Ipc;

internal static class Program
{
    private static async Task<int> Main(string[] args)
    {
        if (args.Length is < 2 or > 5
            || !uint.TryParse(args[1], NumberStyles.Integer, CultureInfo.InvariantCulture, out uint namespaceId))
        {
            Console.Error.WriteLine("Usage: Fld1.Database.Host <database-root> <namespace-id-decimal> [local-author-hex] [--enable-tombstone-gc] [--test-drop-remote-apply-response]");
            return 64;
        }

        string root = Path.GetFullPath(args[0]);
        byte[]? localAuthor = null;
        bool dropRemoteApplyResponseForTest = false;
        bool enableTombstoneGc = false;
        for (int i = 2; i < args.Length; i++)
        {
            string argument = args[i];
            if (string.Equals(argument, "--test-drop-remote-apply-response", StringComparison.Ordinal))
            {
                if (dropRemoteApplyResponseForTest)
                {
                    Console.Error.WriteLine("Duplicate test failpoint argument.");
                    return 64;
                }
                dropRemoteApplyResponseForTest = true;
                continue;
            }
            if (string.Equals(argument, "--enable-tombstone-gc", StringComparison.Ordinal))
            {
                if (enableTombstoneGc)
                {
                    Console.Error.WriteLine("Duplicate tombstone-GC enablement argument.");
                    return 64;
                }
                enableTombstoneGc = true;
                continue;
            }
            if (localAuthor is not null)
            {
                Console.Error.WriteLine("Unknown optional Database Host argument.");
                return 64;
            }
            try { localAuthor = Convert.FromHexString(argument); }
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

            using var dispatcher = new FldDatabaseIpcDispatcher(
                database, enableTombstoneGc: enableTombstoneGc);
            // The exclusive Database root lease is held; prune only IPC-owned generations
            // from older host generations, preserving durable FULL_REPLACE provider snapshots.
            database.PruneStaleIpcProviderSnapshots(dispatcher.HostGeneration);
            // Explicit process-test failpoint: terminate only after the durable remote
            // APPLY handler has returned its receipt, but before StorageIpcServer can
            // write the response frame. It is opt-in and absent from normal Host runs.
            async ValueTask<ReadOnlyMemory<byte>> HandleIpcRequestAsync(
                StorageIpcRequestContext context,
                ReadOnlyMemory<byte> payload,
                CancellationToken cancellationToken)
            {
                ReadOnlyMemory<byte> response = await dispatcher.HandleAsync(
                    context, payload, cancellationToken).ConfigureAwait(false);
                if (dropRemoteApplyResponseForTest &&
                    StorageIpcOperationProtocol.Decode(payload).OperationCode == 0x210C)
                    Environment.Exit(86);
                return response;
            }

            var server = new StorageIpcServer(
                StorageIpcEndpoint.ComputePipeName(root, 0),
                HandleIpcRequestAsync,
                clientDisconnected: dispatcher.ReleaseClientSession);
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
