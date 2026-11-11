using System.Security.Cryptography;
using System.Text;
using System.Text.Json;

namespace Fld1.Storage.Ipc;

/// <summary>Published discovery record for one single-owner Storage IPC Host.</summary>
public sealed record StorageIpcEndpointInfo(
    ushort ProtocolVersion,
    ulong PartitionId,
    string PipeName,
    Guid OwnerId,
    int ProcessId);

/// <summary>
/// Canonical local endpoint naming and owner-safe discovery file operations.
/// The pipe name is deterministic for a normalized root, partition and OS user.
/// </summary>
public static class StorageIpcEndpoint
{
    public const ushort CurrentProtocolVersion = 1;
    public const string EndpointFileName = ".fld1-storage-ipc.endpoint";

    public static string ComputePipeName(string rootDirectory, ulong partitionId)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(rootDirectory);
        string root = Path.TrimEndingDirectorySeparator(Path.GetFullPath(rootDirectory));
        if (OperatingSystem.IsWindows())
            root = root.ToUpperInvariant();

        string identity = string.Concat(
            "FLD1/STORAGE/IPC/ENDPOINT/V1\n",
            root, "\n",
            partitionId.ToString("X16"), "\n",
            Environment.UserName);
        byte[] digest = SHA256.HashData(Encoding.UTF8.GetBytes(identity));
        return "fld1-" + Convert.ToHexString(digest.AsSpan(0, 16)).ToLowerInvariant();
    }

    public static string GetEndpointFilePath(string rootDirectory)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(rootDirectory);
        return Path.Combine(Path.GetFullPath(rootDirectory), EndpointFileName);
    }

    public static StorageIpcEndpointInfo Create(
        string rootDirectory,
        ulong partitionId,
        Guid ownerId,
        int processId)
    {
        if (ownerId == Guid.Empty)
            throw new ArgumentException("Storage IPC Host owner id must be non-empty.", nameof(ownerId));
        if (processId <= 0)
            throw new ArgumentOutOfRangeException(nameof(processId));

        return new StorageIpcEndpointInfo(
            CurrentProtocolVersion,
            partitionId,
            ComputePipeName(rootDirectory, partitionId),
            ownerId,
            processId);
    }

    /// <summary>
    /// Atomically publishes the endpoint record. The caller must already own the
    /// StorageDatabase partition lease and have started listening on PipeName.
    /// </summary>
    public static void Publish(string rootDirectory, StorageIpcEndpointInfo endpoint)
    {
        ArgumentNullException.ThrowIfNull(endpoint);
        Validate(rootDirectory, endpoint, endpoint.PartitionId);
        Directory.CreateDirectory(Path.GetFullPath(rootDirectory));

        string path = GetEndpointFilePath(rootDirectory);
        string temporary = path + ".tmp." + Guid.NewGuid().ToString("N");
        byte[] bytes = JsonSerializer.SerializeToUtf8Bytes(endpoint);
        try
        {
            using (var stream = new FileStream(
                temporary, FileMode.CreateNew, FileAccess.Write, FileShare.None,
                bufferSize: 4096, FileOptions.WriteThrough))
            {
                stream.Write(bytes);
                stream.Flush(flushToDisk: true);
            }
            File.Move(temporary, path, overwrite: true);
        }
        finally
        {
            try { if (File.Exists(temporary)) File.Delete(temporary); } catch { }
        }
    }

    public static StorageIpcEndpointInfo Read(string rootDirectory, ulong partitionId)
    {
        string path = GetEndpointFilePath(rootDirectory);
        if (!File.Exists(path))
            throw new FileNotFoundException("Storage IPC Host endpoint has not been published.", path);

        try
        {
            byte[] bytes = File.ReadAllBytes(path);
            StorageIpcEndpointInfo? endpoint =
                JsonSerializer.Deserialize<StorageIpcEndpointInfo>(bytes);
            if (endpoint is null)
                throw new InvalidDataException("Storage IPC endpoint record is empty.");
            Validate(rootDirectory, endpoint, partitionId);
            return endpoint;
        }
        catch (JsonException exception)
        {
            throw new InvalidDataException("Storage IPC endpoint record is malformed.", exception);
        }
    }

    /// <summary>Deletes the discovery file only if it is still owned by this Host instance.</summary>
    public static bool DeleteIfOwned(string rootDirectory, Guid ownerId)
    {
        if (ownerId == Guid.Empty)
            throw new ArgumentException("Storage IPC Host owner id must be non-empty.", nameof(ownerId));

        string path = GetEndpointFilePath(rootDirectory);
        if (!File.Exists(path))
            return false;

        StorageIpcEndpointInfo? endpoint;
        try
        {
            endpoint = JsonSerializer.Deserialize<StorageIpcEndpointInfo>(File.ReadAllBytes(path));
        }
        catch (JsonException)
        {
            return false;
        }

        if (endpoint is null || endpoint.OwnerId != ownerId)
            return false;

        File.Delete(path);
        return true;
    }

    private static void Validate(
        string rootDirectory,
        StorageIpcEndpointInfo endpoint,
        ulong partitionId)
    {
        if (endpoint.ProtocolVersion != CurrentProtocolVersion ||
            endpoint.PartitionId != partitionId ||
            endpoint.OwnerId == Guid.Empty ||
            endpoint.ProcessId <= 0 ||
            !string.Equals(
                endpoint.PipeName,
                ComputePipeName(rootDirectory, partitionId),
                StringComparison.Ordinal))
            throw new InvalidDataException("Storage IPC endpoint identity or protocol version is invalid.");
    }
}
