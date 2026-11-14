using System.Buffers.Binary;
using Fld1.Storage.Engine;

namespace Fld1.Engine.Db;

public sealed partial class FldDatabase
{
    private static ReadOnlySpan<byte> ProviderPublicationModeMagic => "FLDPUBM1"u8;

    private byte[] ProviderPublicationModeKey()
    {
        byte[] key = new byte[10];
        ProviderPublicationModeMagic.CopyTo(key);
        BinaryPrimitives.WriteUInt16LittleEndian(key.AsSpan(8), checked((ushort)_namespaceId));
        return key;
    }

    /// <summary>Read the immutable Metadata-Store-owned publication cutover for this namespace.</summary>
    public bool TryGetMetadataStoreOwnedPublicationCutover(out ulong nodeOutboxHighWatermark)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        lock (_commitLock)
        {
            EnsureHealthy();
            using StorageSnapshot snapshot = _runStorage.OpenSnapshot();
            if (!snapshot.TryGet(ProviderPublicationModeKey(), out byte[]? value))
            {
                nodeOutboxHighWatermark = 0;
                return false;
            }

            if (value is null || value.Length != sizeof(ulong))
                throw new InvalidDataException("FLD provider publication cutover marker is malformed.");
            nodeOutboxHighWatermark = BinaryPrimitives.ReadUInt64LittleEndian(value);
            return true;
        }
    }

    /// <summary>
    /// Permanently transfers outbound publication authority to the BIN-SYNC Metadata Store.
    /// Cutover is allowed only after the FLD local feed and conservative draft horizon are empty.
    /// </summary>
    public void EnableMetadataStoreOwnedPublication(ulong nodeOutboxHighWatermark)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        lock (_commitLock)
        {
            EnsureHealthy();
            byte[] key = ProviderPublicationModeKey();
            using StorageSnapshot snapshot = _runStorage.OpenSnapshot();
            if (snapshot.TryGet(key, out byte[]? existing))
            {
                if (existing is null || existing.Length != sizeof(ulong))
                    throw new InvalidDataException("FLD provider publication cutover marker is malformed.");
                ulong prior = BinaryPrimitives.ReadUInt64LittleEndian(existing);
                if (prior != nodeOutboxHighWatermark)
                    throw new InvalidOperationException("FLD provider publication cutover watermark is immutable.");
                return;
            }

            FldDraftHorizon horizon = ReadUnpublishedLocalDraftHorizon();
            if (horizon.Count != 0 || _outbox.HighWatermark != _outbox.AckedThrough)
                throw new InvalidOperationException(
                    "FLD local drafts and the provider Outbox must be fully acknowledged before Metadata-Store-owned publication is enabled.");

            using StorageWriteBatch batch = _runStorage.BeginWriteBatch();
            ulong actualBaseCommitSeq = _runStorage.LastCommitSeq;
            if (actualBaseCommitSeq != snapshot.CommitSeq)
                throw new StorageConflictException(snapshot.CommitSeq, actualBaseCommitSeq);

            Span<byte> encoded = stackalloc byte[sizeof(ulong)];
            BinaryPrimitives.WriteUInt64LittleEndian(encoded, nodeOutboxHighWatermark);
            batch.Put(key, encoded);
            batch.Commit();
        }
    }
}
