using System.Buffers.Binary;
using Fld1.Storage.Engine;

namespace Fld1.Storage.Ipc;

/// <summary>Built-in operation codes understood by the Storage-only IPC dispatcher.</summary>
public static class StorageIpcOperationCodes
{
    public const ushort Get = 1;
    public const ushort Put = 2;
    public const ushort Delete = 3;
}

/// <summary>
/// Domain-neutral Storage IPC operation dispatcher. It owns no transport or process
/// lifecycle; a single-owner Host supplies the shared StorageDatabase and passes
/// HandleAsync to StorageIpcServer.
/// </summary>
public sealed class StorageIpcOperationDispatcher
{
    private const int MaxKeyBytes = 1024 * 1024;
    private const int PutHeaderBytes = sizeof(uint) * 2;
    private const int MaxCommitRetries = 8;

    private readonly StorageDatabase _database;
    private readonly StorageIpcOperationLedger _ledger;

    public StorageIpcOperationDispatcher(StorageDatabase database)
    {
        _database = database ?? throw new ArgumentNullException(nameof(database));
        _ledger = new StorageIpcOperationLedger(database);
    }

    public ValueTask<ReadOnlyMemory<byte>> HandleAsync(
        StorageIpcRequestContext context,
        ReadOnlyMemory<byte> request,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (context.ClientId == Guid.Empty || context.SessionGeneration == 0 || context.RequestId == 0)
            throw new InvalidDataException("Storage IPC request context is incomplete.");

        StorageIpcOperationEnvelope envelope = StorageIpcOperationProtocol.Decode(request);
        ReadOnlyMemory<byte> response = envelope.OperationCode switch
        {
            StorageIpcOperationCodes.Get => Read(envelope.Payload.Span),
            StorageIpcOperationCodes.Put => Write(
                context.ClientId, envelope, delete: false, cancellationToken),
            StorageIpcOperationCodes.Delete => Write(
                context.ClientId, envelope, delete: true, cancellationToken),
            _ => throw new InvalidDataException(
                $"Unsupported Storage IPC operation code {envelope.OperationCode}.")
        };
        return ValueTask.FromResult(response);
    }

    private ReadOnlyMemory<byte> Read(ReadOnlySpan<byte> key)
    {
        ValidateKey(key);
        if (!_database.TryGet(key, out byte[]? value))
            return new byte[] { 0, 0, 0, 0, 0 };

        byte[] response = new byte[checked(sizeof(byte) + sizeof(uint) + value!.Length)];
        response[0] = 1;
        BinaryPrimitives.WriteUInt32LittleEndian(response.AsSpan(1), (uint)value.Length);
        value.CopyTo(response, 1 + sizeof(uint));
        return response;
    }

    private ReadOnlyMemory<byte> Write(
        Guid clientId,
        StorageIpcOperationEnvelope envelope,
        bool delete,
        CancellationToken cancellationToken)
    {
        byte[] key;
        byte[] value;
        if (delete)
        {
            key = envelope.Payload.ToArray();
            ValidateKey(key);
            value = Array.Empty<byte>();
        }
        else
        {
            (key, value) = DecodePut(envelope.Payload.Span);
        }

        byte[] fingerprint = StorageIpcOperationLedger.ComputeFingerprint(
            envelope.OperationCode, envelope.Payload.Span);
        if (_ledger.TryGetResult(clientId, envelope.OperationId, fingerprint, out byte[]? prior))
            return prior!;

        byte[] result = delete
            ? new byte[] { 0 }
            : new byte[] { 1 };

        for (int attempt = 0; attempt < MaxCommitRetries; attempt++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            using StorageWriteBatch batch = _database.BeginWriteBatch();
            bool existed = false;
            if (delete)
            {
                existed = batch.TryGet(key, out _);
                batch.Delete(key);
                result[0] = existed ? (byte)1 : (byte)0;
            }
            else
            {
                batch.Put(key, value);
            }

            if (!_ledger.TryStageResult(
                batch, clientId, envelope.OperationId, fingerprint, result, out prior))
                return prior!;

            try
            {
                batch.Commit();
                return result;
            }
            catch (StorageConflictException)
            {
                if (_ledger.TryGetResult(clientId, envelope.OperationId, fingerprint, out prior))
                    return prior!;
                if (attempt + 1 == MaxCommitRetries)
                    throw;
            }
        }

        throw new InvalidOperationException("Storage IPC write retry limit was exhausted.");
    }

    private static (byte[] Key, byte[] Value) DecodePut(ReadOnlySpan<byte> payload)
    {
        if (payload.Length < PutHeaderBytes)
            throw new InvalidDataException("Storage IPC PUT payload is truncated.");

        uint keyLength = BinaryPrimitives.ReadUInt32LittleEndian(payload);
        uint valueLength = BinaryPrimitives.ReadUInt32LittleEndian(payload[sizeof(uint)..]);
        ulong expectedLength = (ulong)PutHeaderBytes + keyLength + valueLength;
        if (keyLength is 0 or > MaxKeyBytes ||
            valueLength > StorageIpcOperationProtocol.MaxPayloadBytes ||
            expectedLength != (ulong)payload.Length)
            throw new InvalidDataException("Storage IPC PUT key/value lengths are invalid.");

        byte[] key = payload.Slice(PutHeaderBytes, (int)keyLength).ToArray();
        byte[] value = payload.Slice(PutHeaderBytes + (int)keyLength, (int)valueLength).ToArray();
        return (key, value);
    }

    private static void ValidateKey(ReadOnlySpan<byte> key)
    {
        if (key.Length is 0 or > MaxKeyBytes)
            throw new InvalidDataException("Storage IPC key length is invalid.");
    }
}
