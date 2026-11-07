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
    private const ushort RelayAckVersion = 1;
    private const int RelayAckHeaderSize = 12;
    private const int RelayAckRecordSize = 24;
    private static ReadOnlySpan<byte> Magic => "FLDA"u8;

    private readonly object _lock = new();
    private readonly string _path;
    private readonly string _relayAckPath;
    private readonly ushort _namespaceId;
    private FileStream _stream;
    private readonly Dictionary<AuthorKey, ulong> _relayAckedThrough = new();
    private readonly List<IndexAnchor> _anchors = new();
    private readonly Dictionary<BatchKey, long> _batches = new();
    private ulong _recordCount;
    private ulong _lastCommitSeq;
    private long _writeOffset;
    private ulong _nextSeq = 1;
    private bool _disposed;
    private bool _faulted;

    private readonly record struct IndexAnchor(ulong JournalSeq, long Offset);
    private readonly record struct AuthorKey(ulong Low, ulong High)
    {
        internal static AuthorKey From(ReadOnlySpan<byte> bytes)
            => new(BinaryPrimitives.ReadUInt64LittleEndian(bytes),
                BinaryPrimitives.ReadUInt64LittleEndian(bytes.Slice(8, 8)));

        internal bool IsZero => (Low | High) == 0;

        internal void WriteTo(Span<byte> bytes)
        {
            BinaryPrimitives.WriteUInt64LittleEndian(bytes, Low);
            BinaryPrimitives.WriteUInt64LittleEndian(bytes.Slice(8, 8), High);
        }
    }

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
        _path = path;
        _relayAckPath = path + ".relayack";
        _namespaceId = namespaceId;
        Directory.CreateDirectory(Path.GetDirectoryName(path)!);
        bool exists = File.Exists(path);
        _stream = OpenJournalStream(path);
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
            LoadRelayAckFile();
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

    private static FileStream OpenJournalStream(string path)
        => new(path, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.Read);

    internal bool IsRelayAcknowledged(ReadOnlySpan<byte> authorNodeId, ulong journalSeq)
    {
        if (authorNodeId.Length != 16) return false;
        var key = AuthorKey.From(authorNodeId);
        lock (_lock)
        {
            EnsureUsable();
            return _relayAckedThrough.TryGetValue(key, out ulong safeThrough) &&
                   journalSeq <= safeThrough;
        }
    }

    internal void AcknowledgeRelayBatches(ReadOnlySpan<byte> authorNodeId, ulong safeThroughJournalSeq)
    {
        if (authorNodeId.Length != 16)
            throw new ArgumentException("AuthorNodeId must be 16 bytes.", nameof(authorNodeId));
        var key = AuthorKey.From(authorNodeId);
        if (key.IsZero)
            throw new ArgumentException("AuthorNodeId must be nonzero.", nameof(authorNodeId));
        if (safeThroughJournalSeq == 0) return;

        lock (_lock)
        {
            EnsureUsable();
            ulong highWatermark = _nextSeq - 1;
            if (safeThroughJournalSeq > highWatermark)
                throw new ArgumentOutOfRangeException(nameof(safeThroughJournalSeq),
                    "Relay ACK watermark exceeds the ApplyJournal high watermark.");
            bool hadPrevious = _relayAckedThrough.TryGetValue(key, out ulong previous);
            if (hadPrevious && safeThroughJournalSeq < previous)
                throw new ArgumentOutOfRangeException(nameof(safeThroughJournalSeq),
                    "Relay ACK watermark cannot move backwards.");
            if (hadPrevious && safeThroughJournalSeq == previous) return;

            _relayAckedThrough[key] = safeThroughJournalSeq;
            try
            {
                PersistRelayAckFile();
            }
            catch
            {
                if (hadPrevious) _relayAckedThrough[key] = previous;
                else _relayAckedThrough.Remove(key);
                throw;
            }
        }
    }

    private void LoadRelayAckFile()
    {
        if (!File.Exists(_relayAckPath)) return;
        byte[] bytes = File.ReadAllBytes(_relayAckPath);
        if (bytes.Length < RelayAckHeaderSize + 4 ||
            !bytes.AsSpan(0, 4).SequenceEqual("FLRA"u8) ||
            BinaryPrimitives.ReadUInt16LittleEndian(bytes.AsSpan(4, 2)) != RelayAckVersion ||
            BinaryPrimitives.ReadUInt16LittleEndian(bytes.AsSpan(6, 2)) != _namespaceId)
            throw new InvalidDataException("Invalid ApplyJournal relay-ACK sidecar header.");

        uint count = BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(8, 4));
        if (count > 1_000_000 ||
            bytes.Length != RelayAckHeaderSize + checked((int)count * RelayAckRecordSize) + 4)
            throw new InvalidDataException("Invalid ApplyJournal relay-ACK sidecar length.");
        int entriesLength = checked((int)count * RelayAckRecordSize);
        uint expectedCrc = Hashes.Crc32C(
            bytes.AsSpan(0, RelayAckHeaderSize),
            bytes.AsSpan(RelayAckHeaderSize, entriesLength));
        uint storedCrc = BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(bytes.Length - 4, 4));
        if (storedCrc != expectedCrc)
            throw new InvalidDataException("ApplyJournal relay-ACK sidecar CRC mismatch.");

        int offset = RelayAckHeaderSize;
        for (int i = 0; i < count; i++)
        {
            var key = AuthorKey.From(bytes.AsSpan(offset, 16));
            ulong seq = BinaryPrimitives.ReadUInt64LittleEndian(bytes.AsSpan(offset + 16, 8));
            if (key.IsZero || seq == 0 || !_relayAckedThrough.TryAdd(key, seq))
                throw new InvalidDataException("Invalid or duplicate ApplyJournal relay-ACK entry.");
            offset += RelayAckRecordSize;
        }
    }

    private void PersistRelayAckFile()
    {
        int count = _relayAckedThrough.Count;
        int entriesLength = checked(count * RelayAckRecordSize);
        byte[] bytes = new byte[RelayAckHeaderSize + entriesLength + 4];
        "FLRA"u8.CopyTo(bytes);
        BinaryPrimitives.WriteUInt16LittleEndian(bytes.AsSpan(4, 2), RelayAckVersion);
        BinaryPrimitives.WriteUInt16LittleEndian(bytes.AsSpan(6, 2), checked((ushort)_namespaceId));
        BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan(8, 4), checked((uint)count));
        int offset = RelayAckHeaderSize;
        foreach (var (key, seq) in _relayAckedThrough.OrderBy(x => x.Key.Low).ThenBy(x => x.Key.High))
        {
            key.WriteTo(bytes.AsSpan(offset, 16));
            BinaryPrimitives.WriteUInt64LittleEndian(bytes.AsSpan(offset + 16, 8), seq);
            offset += RelayAckRecordSize;
        }
        uint crc = Hashes.Crc32C(bytes.AsSpan(0, RelayAckHeaderSize),
            bytes.AsSpan(RelayAckHeaderSize, entriesLength));
        BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan(bytes.Length - 4, 4), crc);

        string temp = _relayAckPath + ".tmp";
        try
        {
            using (var output = new FileStream(temp, FileMode.Create, FileAccess.Write,
                       FileShare.None, 4096, FileOptions.WriteThrough))
            {
                output.Write(bytes);
                output.Flush(true);
            }
            File.Move(temp, _relayAckPath, overwrite: true);
        }
        finally
        {
            if (File.Exists(temp)) File.Delete(temp);
        }
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

            record = ReadRecordAtOffset(index).Record;
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
                var prior = ReadRecordAtOffset(existing).Record.Commit;
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
                if ((_recordCount % SparseIndexStride) == 0)
                    _anchors.Add(new IndexAnchor(seq, offset));
                _batches.Add(key, offset);
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

    private static WalApplyCommit ToRelayOnly(
        WalApplyCommit commit, List<WalFieldMutation>? relayFields = null)
    {
        var selected = relayFields ?? commit.RelayFields;
        return new()
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
            Fields = selected,
            RelayFields = selected,
            RelayOnly = true
        };
    }

    internal int CompactAcknowledgedRelayPayloads(ulong sealedThroughCommitSeq)
    {
        lock (_lock)
        {
            EnsureUsable();
            if (sealedThroughCommitSeq == 0 || _recordCount == 0) return 0;

            string tempPath = _path + ".compact.tmp";
            var newAnchors = new List<IndexAnchor>(_anchors.Count);
            var newBatches = new Dictionary<BatchKey, long>(_batches.Count);
            long newWriteOffset = HeaderSize;
            ulong count = 0;
            int removedFields = 0;
            bool changed = false;

            try
            {
                using (var output = new FileStream(tempPath, FileMode.Create, FileAccess.Write,
                           FileShare.None, 64 * 1024, FileOptions.WriteThrough))
                {
                    WriteHeader(output);
                    long sourceOffset = HeaderSize;
                    Span<byte> header = stackalloc byte[RecordHeaderSize];
                    Span<byte> crcBytes = stackalloc byte[RecordCrcSize];
                    while (sourceOffset < _writeOffset)
                    {
                        var (record, nextSourceOffset) = ReadRecordAtOffset(sourceOffset);
                        var commit = record.Commit;
                        var retainedRelayFields = new List<WalFieldMutation>(commit.RelayFields.Count);
                        foreach (var field in commit.RelayFields)
                        {
                            bool acknowledged = false;
                            if (record.CommitSeq <= sealedThroughCommitSeq)
                            {
                                var author = AuthorKey.From(field.AuthorNodeId);
                                acknowledged = _relayAckedThrough.TryGetValue(author, out ulong safeThrough) &&
                                               record.JournalSeq <= safeThrough;
                            }
                            if (acknowledged) removedFields++;
                            else retainedRelayFields.Add(field);
                        }

                        if (!commit.RelayOnly || retainedRelayFields.Count != commit.RelayFields.Count)
                            changed = true;

                        byte[] payload = ApplyCommitCodec.Encode(ToRelayOnly(commit, retainedRelayFields));
                        header.Clear();
                        BinaryPrimitives.WriteUInt64LittleEndian(header.Slice(0, 8), record.JournalSeq);
                        BinaryPrimitives.WriteUInt64LittleEndian(header.Slice(8, 8), record.CommitSeq);
                        BinaryPrimitives.WriteUInt32LittleEndian(header.Slice(16, 4), checked((uint)payload.Length));
                        uint crc = Hashes.Crc32C(header, payload);
                        BinaryPrimitives.WriteUInt32LittleEndian(crcBytes, crc);

                        long recordOffset = newWriteOffset;
                        output.Write(header);
                        output.Write(payload);
                        output.Write(crcBytes);
                        if ((count % SparseIndexStride) == 0)
                            newAnchors.Add(new IndexAnchor(record.JournalSeq, recordOffset));
                        var key = BatchKey.From(commit);
                        if (!newBatches.TryAdd(key, recordOffset))
                            throw new InvalidDataException("Duplicate remote batch identity during ApplyJournal compaction.");
                        newWriteOffset = checked(newWriteOffset + RecordHeaderSize + (long)payload.Length + RecordCrcSize);
                        count++;
                        sourceOffset = nextSourceOffset;
                    }
                    output.Flush(true);
                }

                if (!changed)
                    return 0;

                bool replaced = false;
                long oldWriteOffset = _writeOffset;
                _stream.Dispose();
                try
                {
                    File.Move(tempPath, _path, overwrite: true);
                    replaced = true;
                    _stream = OpenJournalStream(_path);
                    _stream.Position = newWriteOffset;
                }
                catch
                {
                    try
                    {
                        _stream = OpenJournalStream(_path);
                        _stream.Position = replaced ? newWriteOffset : oldWriteOffset;
                        if (replaced)
                        {
                            _anchors.Clear();
                            _anchors.AddRange(newAnchors);
                            _batches.Clear();
                            foreach (var item in newBatches) _batches.Add(item.Key, item.Value);
                            _writeOffset = newWriteOffset;
                        }
                    }
                    catch { _faulted = true; }
                    throw;
                }

                _anchors.Clear();
                _anchors.AddRange(newAnchors);
                _batches.Clear();
                foreach (var item in newBatches) _batches.Add(item.Key, item.Value);
                _writeOffset = newWriteOffset;
                _recordCount = count;
                return removedFields;
            }
            catch
            {
                if (_stream.SafeFileHandle.IsClosed)
                {
                    try
                    {
                        _stream = OpenJournalStream(_path);
                        _stream.Position = _writeOffset;
                    }
                    catch { _faulted = true; }
                }
                throw;
            }
            finally
            {
                if (File.Exists(tempPath)) File.Delete(tempPath);
            }
        }
    }

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


    private void WriteHeader()
    {
        WriteHeader(_stream);
        _writeOffset = HeaderSize;
    }

    private void WriteHeader(Stream stream)
    {
        Span<byte> header = stackalloc byte[HeaderSize];
        header.Clear();
        Magic.CopyTo(header);
        BinaryPrimitives.WriteUInt16LittleEndian(header.Slice(4, 2), Version);
        BinaryPrimitives.WriteUInt16LittleEndian(header.Slice(6, 2), _namespaceId);
        BinaryPrimitives.WriteUInt32LittleEndian(header.Slice(12, 4), Hashes.Crc32C(header.Slice(0, 12)));
        stream.Position = 0;
        stream.Write(header);
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
            if (!_batches.TryAdd(key, position))
                throw new InvalidDataException("Duplicate remote batch identity in apply journal.");
            if (((seq - 1) % SparseIndexStride) == 0)
                _anchors.Add(new IndexAnchor(seq, position));
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
