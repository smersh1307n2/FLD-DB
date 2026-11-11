using System.Buffers.Binary;
using System.Security.Cryptography;
using Fld1.Storage.Engine;

namespace Fld1.Storage.Ipc;

/// <summary>
/// Durable idempotency result ledger for operations whose data mutations are
/// committed through one StorageWriteBatch. The operation result row must be
/// staged in the same batch as every mutation caused by that operation.
/// </summary>
public sealed class StorageIpcOperationLedger
{
    public const int FingerprintBytes = 32;
    public const int MaxResultBytes = StorageIpcProtocol.MaxPayloadBytes;

    private const uint KeyMagic = 0x314F4953; // "SIO1"
    private const byte ValueVersion = 1;
    private const int KeyBytes = 36;
    private const int ValueHeaderBytes = 1 + FingerprintBytes + sizeof(int);

    private static ReadOnlySpan<byte> FingerprintDomain => "FLD/STORAGE/IPC-OPERATION/V1"u8;

    private readonly StorageDatabase _database;

    public StorageIpcOperationLedger(StorageDatabase database)
        => _database = database ?? throw new ArgumentNullException(nameof(database));

    /// <summary>
    /// Computes the canonical request fingerprint. OperationId is deliberately
    /// excluded because it is the ledger key; the operation code and exact payload
    /// determine whether a retry is identical or a conflicting reuse.
    /// </summary>
    public static byte[] ComputeFingerprint(ushort operationCode, ReadOnlySpan<byte> payload)
    {
        if (operationCode == 0)
            throw new ArgumentOutOfRangeException(nameof(operationCode));
        if (payload.Length > StorageIpcOperationProtocol.MaxPayloadBytes)
            throw new ArgumentOutOfRangeException(nameof(payload));

        Span<byte> code = stackalloc byte[sizeof(ushort)];
        BinaryPrimitives.WriteUInt16LittleEndian(code, operationCode);

        using IncrementalHash hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        hash.AppendData(FingerprintDomain);
        hash.AppendData(code);
        hash.AppendData(payload);
        return hash.GetHashAndReset();
    }

    /// <summary>
    /// Returns the previously committed result for this exact operation. Reusing
    /// ClientId + OperationId with a different fingerprint fails closed.
    /// </summary>
    public bool TryGetResult(
        Guid clientId,
        Guid operationId,
        ReadOnlySpan<byte> fingerprint,
        out byte[]? result)
    {
        ValidateIdentity(clientId, operationId, fingerprint);
        byte[] key = EncodeKey(clientId, operationId);
        if (!_database.TryGet(key, out byte[]? encoded))
        {
            result = null;
            return false;
        }

        result = DecodeAndVerifyResult(encoded!, fingerprint);
        return true;
    }

    /// <summary>
    /// Stages the completed result in the caller's existing write batch. The caller
    /// must stage the operation's data mutations in that same batch and commit once.
    /// Returns false with the durable prior result when an exact operation already
    /// exists; the caller must abandon the current batch and return that prior result.
    /// A concurrent commit after this check is detected by StorageWriteBatch's
    /// optimistic CommitSeq fence; the caller then re-reads with TryGetResult.
    /// </summary>
    public bool TryStageResult(
        StorageWriteBatch batch,
        Guid clientId,
        Guid operationId,
        ReadOnlySpan<byte> fingerprint,
        ReadOnlySpan<byte> result,
        out byte[]? existingResult)
    {
        ArgumentNullException.ThrowIfNull(batch);
        if (!batch.IsBoundTo(_database))
            throw new ArgumentException(
                "Operation result and data mutations must use a batch from the ledger's exact StorageDatabase.",
                nameof(batch));
        ValidateIdentity(clientId, operationId, fingerprint);
        if (result.Length > MaxResultBytes)
            throw new ArgumentOutOfRangeException(nameof(result));

        byte[] key = EncodeKey(clientId, operationId);
        if (batch.TryGet(key, out byte[]? existing))
        {
            existingResult = DecodeAndVerifyResult(existing!, fingerprint);
            return false;
        }

        batch.Put(key, EncodeResult(fingerprint, result));
        existingResult = null;
        return true;
    }

    private static void ValidateIdentity(
        Guid clientId,
        Guid operationId,
        ReadOnlySpan<byte> fingerprint)
    {
        if (clientId == Guid.Empty)
            throw new ArgumentException("Stable IPC ClientId must be non-empty.", nameof(clientId));
        if (operationId == Guid.Empty)
            throw new ArgumentException("Stable IPC OperationId must be non-empty.", nameof(operationId));
        if (fingerprint.Length != FingerprintBytes)
            throw new ArgumentException("IPC request fingerprint must be a SHA-256 digest.", nameof(fingerprint));
    }

    private static byte[] EncodeKey(Guid clientId, Guid operationId)
    {
        byte[] key = new byte[KeyBytes];
        BinaryPrimitives.WriteUInt32LittleEndian(key, KeyMagic);
        if (!clientId.TryWriteBytes(key.AsSpan(4, 16), bigEndian: true, out int clientBytes) ||
            clientBytes != 16 ||
            !operationId.TryWriteBytes(key.AsSpan(20, 16), bigEndian: true, out int operationBytes) ||
            operationBytes != 16)
            throw new InvalidOperationException("Could not encode IPC operation ledger identity.");
        return key;
    }

    private static byte[] EncodeResult(ReadOnlySpan<byte> fingerprint, ReadOnlySpan<byte> result)
    {
        byte[] encoded = new byte[checked(ValueHeaderBytes + result.Length)];
        encoded[0] = ValueVersion;
        fingerprint.CopyTo(encoded.AsSpan(1, FingerprintBytes));
        BinaryPrimitives.WriteInt32LittleEndian(encoded.AsSpan(1 + FingerprintBytes), result.Length);
        result.CopyTo(encoded.AsSpan(ValueHeaderBytes));
        return encoded;
    }

    private static byte[] DecodeAndVerifyResult(byte[] encoded, ReadOnlySpan<byte> expectedFingerprint)
    {
        if (encoded.Length < ValueHeaderBytes || encoded[0] != ValueVersion)
            throw new InvalidDataException("Stored IPC operation result header is invalid.");

        ReadOnlySpan<byte> fingerprint = encoded.AsSpan(1, FingerprintBytes);
        int resultLength = BinaryPrimitives.ReadInt32LittleEndian(
            encoded.AsSpan(1 + FingerprintBytes));
        if (resultLength < 0 || resultLength > MaxResultBytes ||
            encoded.Length != ValueHeaderBytes + resultLength)
            throw new InvalidDataException("Stored IPC operation result length is invalid.");

        if (!CryptographicOperations.FixedTimeEquals(fingerprint, expectedFingerprint))
            throw new InvalidOperationException(
                "Stable IPC OperationId was reused with a different request fingerprint.");

        return encoded.AsSpan(ValueHeaderBytes, resultLength).ToArray();
    }
}
