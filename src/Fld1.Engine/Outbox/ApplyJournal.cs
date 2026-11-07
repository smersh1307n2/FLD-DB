using System.Buffers.Binary;
using Fld1.Core.Hash;
using Fld1.Wal.Codec;
using Fld1.Wal.Format;

namespace Fld1.Engine.Outbox;

internal sealed class ApplyJournalRecord
{
    public required ulong JournalSeq { get; init; }
    public required ulong CommitSeq { get; init; }
    public required WalApplyCommit Commit { get; init; }
}

internal sealed class ApplyJournal : IDisposable
{
    private const int HeaderSize = 16;
    private const int RecordHeaderSize = 20;
    private const int RecordCrcSize = 4;
    private const ushort Version = 1;
    private const int SparseIndexStride = 256;
    private static ReadOnlySpan<byte> Magic => "FLDA"u8;

    private readonly object _lock = new();
    private readonly ushort _namespaceId;
    private readonly FileStream _stream;
    private readonly List<IndexAnchor> _anchors = new();
    private readonly Dictionary<BatchKey, IndexEntry> _batches = new();
    private ulong _recordCount;
    private ulong _lastCommitSeq;
    private long _writeOffset;
    private ulong _nextSeq = 1;
    private bool _disposed;
    private bool _faulted;

    private readonly record struct IndexEntry(
        ulong JournalSeq, ulong CommitSeq, long Offset, int PayloadLength, BatchKey Key);
    private readonly record struct IndexAnchor(ulong JournalSeq, long Offset);

    private readonly record struct BatchKey(
        ulong IssuerLo, ulong IssuerHi, uint IssuerEpoch, ushort NamespaceId,
        ulong BatchLo, ulong BatchHi)
    {
        public static BatchKey From(WalApplyCommit commit)
            => new(
                BinaryPrimitives.ReadUInt64LittleEndian(commit.IssuerNodeId),
                BinaryPrimitives.ReadUInt64LittleEndian(commit.IssuerNodeId.AsSpan(8)),
                commit.IssuerEpoch,
                commit.NamespaceId,
                BinaryPrimitives.ReadUInt64LittleEndian(commit.BatchId),
                BinaryPrimitives.ReadUInt64LittleEndian(commit.BatchId.AsSpan(8)));
    }

    private ApplyJournal(string path, ushort namespaceId)
    {
        _namespaceId = namespaceId;
        Directory.CreateDirectory(Path.GetDirectoryName(path)!);
        bool exists = File.Exists(path);
        _stream = new FileStream(path, exists ? FileMode.Open : FileMode.CreateNew,
            FileAccess.ReadWrite, FileShare.Read, 64 * 1024, FileOptions.RandomAccess);
        try
        {
            if (!exists)
            {
                WriteHeader();
                _stream.Flush(flushToDisk: true);
            }
            else
            {
                LoadAndRepairTail();
            }
        }
        catch
        {
            _stream.Dispose();
            throw;
        }
    }

    public static ApplyJournal Open(string directory, uint namespaceId)
    {
        if (namespaceId > ushort.MaxValue) throw new ArgumentOutOfRangeException(nameof(namespaceId));
        string path = Path.Combine(directory, $"ns_{namespaceId}.apply");
        return new ApplyJournal(path, (ushort)namespaceId);
    }

    public ulong HighWatermark
    {
        get { lock (_lock) { EnsureUsable(); return _nextSeq - 1; } }
    }

    public bool TryFindBatch(
        ReadOnlySpan<byte> issuerNodeId, uint issuerEpoch, ushort namespaceId,
        ReadOnlySpan<byte> batchId, out ApplyJournalRecord? record)
    {
        if (issuerNodeId.Length != 16 || batchId.Length != 16)
            throw new ArgumentException("Batch identity fields must be 16 bytes.");
        lock (_lock)
        {
            EnsureUsable();
            var key = new BatchKey(
                BinaryPrimitives.ReadUInt64LittleEndian(issuerNodeId),
                BinaryPrimitives.ReadUInt64LittleEndian(issuerNodeId.Slice(8)),
                issuerEpoch,
                namespaceId,
                BinaryPrimitives.ReadUInt64LittleEndian(batchId),
                BinaryPrimitives.ReadUInt64LittleEndian(batchId.Slice(8)));
            if (!_batches.TryGetValue(key, out var index))
            {
                record = null;
                return false;
            }

            record = ReadRecord(index);
            return true;
        }
    }

    public bool AppendIfMissing(ulong commitSeq, ReadOnlySpan<byte> payload)
    {
        var commit = ApplyCommitCodec.Decode(payload);
        if (commit.NamespaceId != _namespaceId)
            throw new InvalidDataException("APPLY_COMMIT namespace does not match journal namespace.");
        var journalPayload = ApplyCommitCodec.Encode(ToRelayOnly(commit));
        var key = BatchKey.From(commit);

        lock (_lock)
        {
            EnsureUsable();
            if (_batches.TryGetValue(key, out var existing))
            {
                var prior = ApplyCommitCodec.Decode(ReadPayload(existing));
                byte[] priorJournalPayload = ApplyCommitCodec.Encode(ToRelayOnly(prior));
                if (!priorJournalPayload.AsSpan().SequenceEqual(journalPayload))
                    throw new InvalidDataException("Conflicting receipt or relay payload for an existing remote batch identity.");
                return false;
            }

            if (commitSeq == 0 || (_recordCount != 0 && commitSeq < _lastCommitSeq))
                throw new InvalidDataException("Apply journal CommitSeq must be positive and monotonic.");
            ulong seq = _nextSeq;
            if (seq == ulong.MaxValue)
                throw new InvalidOperationException("Apply journal sequence space is exhausted.");

            Span<byte> header = stackalloc byte[RecordHeaderSize];
            BinaryPrimitives.WriteUInt64LittleEndian(header.Slice(0, 8), seq);
            BinaryPrimitives.WriteUInt64LittleEndian(header.Slice(8, 8), commitSeq);
            BinaryPrimitives.WriteUInt32LittleEndian(header.Slice(16, 4), checked((uint)journalPayload.Length));
            uint crc = Hashes.Crc32C(header, journalPayload);
            Span<byte> crcBytes = stackalloc byte[RecordCrcSize];
            BinaryPrimitives.WriteUInt32LittleEndian(crcBytes, crc);

            long offset = _writeOffset;
            long nextOffset = checked(offset + RecordHeaderSize + (long)journalPayload.Length + RecordCrcSize);
            try
            {
                _stream.Position = offset;
                _stream.Write(header);
                _stream.Write(journalPayload);
                _stream.Write(crcBytes);
                var index = new IndexEntry(seq, commitSeq, offset, journalPayload.Length, key);
                if ((_recordCount % SparseIndexStride) == 0)
                    _anchors.Add(new IndexAnchor(seq, offset));
                _batches.Add(key, index);
                _recordCount++;
                _lastCommitSeq = commitSeq;
                _writeOffset = nextOffset;
                _nextSeq = seq + 1;
                return true;
            }
            catch
            {
                _faulted = true;
                throw;
            }
        }
    }

    private static WalApplyCommit ToRelayOnly(WalApplyCommit commit)
        => new()
        {
            SenderPeer = commit.SenderPeer,
            SenderEpoch = commit.SenderEpoch,
            NamespaceId = commit.NamespaceId,
            IssuerNodeId = commit.IssuerNodeId,
            IssuerEpoch = commit.IssuerEpoch,
            BatchId = commit.BatchId,
            CursorStart = commit.CursorStart,
            CursorEnd = commit.CursorEnd,
            PreviousBatchDigest = commit.PreviousBatchDigest,
            BatchDigest = commit.BatchDigest,
            RecordsRoot = commit.RecordsRoot,
            ProtocolStateDelta = commit.ProtocolStateDelta,
            AppliedCount = commit.AppliedCount,
            Fields = commit.RelayFields,
            RelayFields = commit.RelayFields,
            RelayOnly = true
        };

    public IReadOnlyList<ApplyJournalRecord> ReadBatches(ulong afterJournalSeq, int limit)
    {
        lock (_lock)
        {
            EnsureUsable();
            if (limit <= 0) return Array.Empty<ApplyJournalRecord>();
            var result = new List<ApplyJournalRecord>(Math.Min(limit, 256));
            if (afterJournalSeq >= _nextSeq - 1)
                return result;

            int low = 0;
            int high = _anchors.Count;
            while (low < high)
            {
                int middle = low + ((high - low) >> 1);
                if (_anchors[middle].JournalSeq <= afterJournalSeq)
                    low = middle + 1;
                else
                    high = middle;
            }

            long offset = low == 0 ? HeaderSize : _anchors[low - 1].Offset;
            while (offset < _writeOffset && result.Count < limit)
            {
                var (record, nextOffset) = ReadRecordAtOffset(offset);
                offset = nextOffset;
                if (record.JournalSeq > afterJournalSeq)
                    result.Add(record);
            }
            return result;
        }
    }

    public void Flush()
    {
        lock (_lock)
        {
            EnsureUsable();
            try { _stream.Flush(flushToDisk: true); }
            catch { _faulted = true; throw; }
        }
    }

    private (ApplyJournalRecord Record, long NextOffset) ReadRecordAtOffset(long offset)
    {
        Span<byte> header = stackalloc byte[RecordHeaderSize];
        Span<byte> crcBytes = stackalloc byte[RecordCrcSize];
        _stream.Position = offset;
        _stream.ReadExactly(header);
        uint payloadLength = BinaryPrimitives.ReadUInt32LittleEndian(header.Slice(16, 4));
        if (payloadLength > WalConstants.MaxPayloadLen)
            throw new InvalidDataException($"Apply journal payload length exceeds limit at {offset}.");
        byte[] payload = new byte[checked((int)payloadLength)];
        _stream.ReadExactly(payload);
        _stream.ReadExactly(crcBytes);
        if (BinaryPrimitives.ReadUInt32LittleEndian(crcBytes) != Hashes.Crc32C(header, payload))
            throw new InvalidDataException($"Apply journal CRC mismatch at offset {offset}.");

        ulong journalSeq = BinaryPrimitives.ReadUInt64LittleEndian(header.Slice(0, 8));
        ulong commitSeq = BinaryPrimitives.ReadUInt64LittleEndian(header.Slice(8, 8));
        var commit = ApplyCommitCodec.Decode(payload);
        if (commit.NamespaceId != _namespaceId)
            throw new InvalidDataException("Apply journal contains a different namespace.");
        long nextOffset = checked(offset + RecordHeaderSize + (long)payloadLength + RecordCrcSize);
        return (new ApplyJournalRecord
        {
            JournalSeq = journalSeq,
            CommitSeq = commitSeq,
            Commit = commit
        }, nextOffset);
    }

    private ApplyJournalRecord ReadRecord(IndexEntry index)
        => new()
        {
            JournalSeq = index.JournalSeq,
            CommitSeq = index.CommitSeq,
            Commit = ApplyCommitCodec.Decode(ReadPayload(index))
        };

    private byte[] ReadPayload(IndexEntry index)
    {
        Span<byte> header = stackalloc byte[RecordHeaderSize];
        Span<byte> crcBytes = stackalloc byte[RecordCrcSize];
        byte[] payload = new byte[index.PayloadLength];
        _stream.Position = index.Offset;
        _stream.ReadExactly(header);
        if (BinaryPrimitives.ReadUInt64LittleEndian(header.Slice(0, 8)) != index.JournalSeq
            || BinaryPrimitives.ReadUInt64LittleEndian(header.Slice(8, 8)) != index.CommitSeq
            || BinaryPrimitives.ReadUInt32LittleEndian(header.Slice(16, 4)) != index.PayloadLength)
            throw new InvalidDataException("Apply journal index no longer matches its record.");
        _stream.ReadExactly(payload);
        _stream.ReadExactly(crcBytes);
        uint stored = BinaryPrimitives.ReadUInt32LittleEndian(crcBytes);
        if (stored != Hashes.Crc32C(header, payload))
            throw new InvalidDataException($"Apply journal CRC mismatch at offset {index.Offset}.");
        return payload;
    }

    private void WriteHeader()
    {
        Span<byte> header = stackalloc byte[HeaderSize];
        header.Clear();
        Magic.CopyTo(header);
        BinaryPrimitives.WriteUInt16LittleEndian(header.Slice(4, 2), Version);
        BinaryPrimitives.WriteUInt16LittleEndian(header.Slice(6, 2), _namespaceId);
        BinaryPrimitives.WriteUInt32LittleEndian(header.Slice(12, 4), Hashes.Crc32C(header.Slice(0, 12)));
        _stream.Position = 0;
        _stream.Write(header);
        _writeOffset = HeaderSize;
    }

    private void LoadAndRepairTail()
    {
        if (_stream.Length < HeaderSize)
            throw new InvalidDataException("Apply journal header is truncated.");
        Span<byte> header = stackalloc byte[HeaderSize];
        _stream.Position = 0;
        _stream.ReadExactly(header);
        if (!header.Slice(0, 4).SequenceEqual(Magic)
            || BinaryPrimitives.ReadUInt16LittleEndian(header.Slice(4, 2)) != Version
            || BinaryPrimitives.ReadUInt16LittleEndian(header.Slice(6, 2)) != _namespaceId
            || BinaryPrimitives.ReadUInt32LittleEndian(header.Slice(8, 4)) != 0
            || BinaryPrimitives.ReadUInt32LittleEndian(header.Slice(12, 4)) != Hashes.Crc32C(header.Slice(0, 12)))
            throw new InvalidDataException("Apply journal header is invalid.");

        long position = HeaderSize;
        long fileLength = _stream.Length;
        ulong previousSeq = 0;
        ulong previousCommitSeq = 0;
        Span<byte> recordHeader = stackalloc byte[RecordHeaderSize];
        Span<byte> crcBytes = stackalloc byte[RecordCrcSize];
        while (position < fileLength)
        {
            long remaining = fileLength - position;
            if (remaining < RecordHeaderSize + RecordCrcSize)
                break;

            _stream.Position = position;
            _stream.ReadExactly(recordHeader);
            uint payloadLength = BinaryPrimitives.ReadUInt32LittleEndian(recordHeader.Slice(16, 4));
            if (payloadLength > WalConstants.MaxPayloadLen)
                throw new InvalidDataException($"Apply journal payload length exceeds limit at {position}.");
            long recordLength = RecordHeaderSize + (long)payloadLength + RecordCrcSize;
            if (recordLength > remaining)
                break;

            byte[] payload = new byte[checked((int)payloadLength)];
            _stream.ReadExactly(payload);
            _stream.ReadExactly(crcBytes);
            uint storedCrc = BinaryPrimitives.ReadUInt32LittleEndian(crcBytes);
            if (storedCrc != Hashes.Crc32C(recordHeader, payload))
                throw new InvalidDataException($"Apply journal CRC mismatch at {position}.");

            ulong seq = BinaryPrimitives.ReadUInt64LittleEndian(recordHeader.Slice(0, 8));
            ulong commitSeq = BinaryPrimitives.ReadUInt64LittleEndian(recordHeader.Slice(8, 8));
            if (seq == 0 || seq == ulong.MaxValue || seq != previousSeq + 1
                || commitSeq == 0 || (previousCommitSeq != 0 && commitSeq < previousCommitSeq))
                throw new InvalidDataException($"Apply journal sequence discontinuity at {position}.");

            var commit = ApplyCommitCodec.Decode(payload);
            if (commit.NamespaceId != _namespaceId)
                throw new InvalidDataException("Apply journal contains a different namespace.");
            var key = BatchKey.From(commit);
            var index = new IndexEntry(seq, commitSeq, position, (int)payloadLength, key);
            if (!_batches.TryAdd(key, index))
                throw new InvalidDataException("Duplicate remote batch identity in apply journal.");
            if (((seq - 1) % SparseIndexStride) == 0)
                _anchors.Add(new IndexAnchor(seq, position));
            _batches.Add(key, index);
            _recordCount++;
            _lastCommitSeq = commitSeq;
            previousSeq = seq;
            previousCommitSeq = commitSeq;
            position += recordLength;
        }

        if (position != fileLength)
        {
            _stream.SetLength(position);
            _stream.Flush(flushToDisk: true);
        }
        _writeOffset = position;
        _nextSeq = previousSeq + 1;
        _recordCount = previousSeq;
        _lastCommitSeq = previousCommitSeq;
    }

    private void EnsureUsable()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (_faulted)
            throw new InvalidOperationException("Apply journal requires reopen after a failed durable operation.");
    }

    public void Dispose()
    {
        lock (_lock)
        {
            if (_disposed) return;
            _disposed = true;
            _stream.Dispose();
        }
    }
}
