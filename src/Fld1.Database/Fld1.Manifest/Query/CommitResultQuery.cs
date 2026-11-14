using Fld1.Core.Util;
using Fld1.Manifest.Format;

namespace Fld1.Manifest.Query;

public enum CommitResultStatus
{
    Committed = 0,
    Rejected = 1,
    IdempotentDuplicate = 2,
    UnknownNeverSeen = 3,
    UnknownInFlight = 4,
    UnknownGap = 5,
    Expired = 6,
    UnknownFuture = 7,
    UnknownRebuilding = 8,
}

public readonly struct CommitResultAnswer
{
    public readonly CommitResultStatus Status;
    public readonly ulong CommitSeq;
    public readonly ushort ErrorCode;

    public CommitResultAnswer(CommitResultStatus status, ulong commitSeq = 0, ushort errorCode = 0)
    {
        Status = status;
        CommitSeq = commitSeq;
        ErrorCode = errorCode;
    }
}

/// <summary>
/// QUERY_COMMIT_RESULT semantics. Spec §6.2.
/// </summary>
public static class CommitResultQuery
{
    public static CommitResultAnswer Query(
        ManifestDocument manifest,
        ReadOnlySpan<byte> clientInstanceId,
        ulong requestSeq,
        IReadOnlyDictionary<(Id16Key, ulong), (byte outcome, ulong commitSeq, ushort error)>? memoryLog = null)
    {
        if (clientInstanceId.Length != 16)
            throw new ArgumentException("ClientInstanceId must be 16 bytes");

        ResultClient? client = null;
        foreach (var c in manifest.ResultClients)
        {
            if (ByteUtil.Equals16(c.ClientInstanceId, clientInstanceId))
            {
                client = c;
                break;
            }
        }

        if (client is null)
            return new CommitResultAnswer(CommitResultStatus.UnknownNeverSeen);

        if (requestSeq > client.FinalizedThroughSeq)
            return new CommitResultAnswer(CommitResultStatus.UnknownInFlight);

        if (requestSeq < client.FloorSeq)
        {
            // Check bitmap / provenance for durable proof → EXPIRED, else UNKNOWN_GAP
            if (IsAccepted(client, requestSeq))
                return new CommitResultAnswer(CommitResultStatus.Expired);
            if (FindProvenance(manifest, clientInstanceId, requestSeq) is not null)
                return new CommitResultAnswer(CommitResultStatus.Expired);
            return new CommitResultAnswer(CommitResultStatus.UnknownGap);
        }

        // In [FloorSeq, FinalizedThroughSeq]
        var key = (new Id16Key(clientInstanceId), requestSeq);
        if (memoryLog is not null && memoryLog.TryGetValue(key, out var mem))
        {
            return mem.outcome switch
            {
                ManifestOutcome.Committed => new CommitResultAnswer(CommitResultStatus.Committed, mem.commitSeq),
                ManifestOutcome.Rejected => new CommitResultAnswer(CommitResultStatus.Rejected, 0, mem.error),
                ManifestOutcome.Idempotent => new CommitResultAnswer(CommitResultStatus.IdempotentDuplicate, mem.commitSeq),
                _ => new CommitResultAnswer(CommitResultStatus.UnknownRebuilding)
            };
        }

        var prov = FindProvenance(manifest, clientInstanceId, requestSeq);
        if (prov is not null)
        {
            return prov.Outcome switch
            {
                ManifestOutcome.Committed => new CommitResultAnswer(CommitResultStatus.Committed, prov.CommitSeq),
                ManifestOutcome.Rejected => new CommitResultAnswer(CommitResultStatus.Rejected, 0, prov.ErrorCode),
                ManifestOutcome.Idempotent => new CommitResultAnswer(CommitResultStatus.IdempotentDuplicate, prov.CommitSeq),
                _ => new CommitResultAnswer(CommitResultStatus.Expired)
            };
        }

        if (IsAccepted(client, requestSeq))
            return new CommitResultAnswer(CommitResultStatus.Expired);

        if (requestSeq > client.SeqHi)
            return new CommitResultAnswer(CommitResultStatus.UnknownFuture);

        return new CommitResultAnswer(CommitResultStatus.UnknownRebuilding);
    }

    private static bool IsAccepted(ResultClient client, ulong requestSeq)
    {
        if (requestSeq < client.FloorSeq) return false;
        ulong idx = requestSeq - client.FloorSeq;
        ulong bitLen = (ulong)client.AcceptedBitmap.Length * 8;
        if (idx >= bitLen) return false;
        int byteIndex = (int)(idx / 8);
        int bit = (int)(idx % 8);
        return (client.AcceptedBitmap[byteIndex] & (1 << bit)) != 0;
    }

    private static ProvenanceIndexEntry? FindProvenance(
        ManifestDocument m, ReadOnlySpan<byte> clientId, ulong requestSeq)
    {
        foreach (var p in m.ProvenanceIndex)
        {
            if (p.RequestSeq == requestSeq && ByteUtil.Equals16(p.ClientInstanceId, clientId))
                return p;
        }
        return null;
    }

    /// <summary>16-byte key for dictionary.</summary>
    public readonly struct Id16Key : IEquatable<Id16Key>
    {
        private readonly ulong _a, _b;
        public Id16Key(ReadOnlySpan<byte> id)
        {
            _a = System.Buffers.Binary.BinaryPrimitives.ReadUInt64LittleEndian(id);
            _b = System.Buffers.Binary.BinaryPrimitives.ReadUInt64LittleEndian(id.Slice(8));
        }
        public bool Equals(Id16Key other) => _a == other._a && _b == other._b;
        public override bool Equals(object? obj) => obj is Id16Key k && Equals(k);
        public override int GetHashCode() => HashCode.Combine(_a, _b);
    }
}
