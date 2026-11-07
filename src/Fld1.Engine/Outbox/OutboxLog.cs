using System.Buffers;
using System.Buffers.Binary;
using Fld1.Core.Hash;

namespace Fld1.Engine.Outbox;

/// <summary>
/// Durable append-only Outbox consumed by BIN-SYNC.
/// OutboxSeq is a monotonic namespace cursor and is deliberately independent of WAL CommitSeq.
/// </summary>
public sealed class OutboxLog : IDisposable
{
    public static ReadOnlySpan<byte> Magic => "FLDO"u8;
    private static ReadOnlySpan<byte> StateMagic => "FLDS"u8;
    private static ReadOnlySpan<byte> LeaseMagic => "FLDL"u8;

    private const ushort Version = 1;
    private const ushort LeaseVersion = 1;
    private const int HeaderSize = 16;
    private const int FixedRecordSize = 88;
    private const int RecordCrcSize = 4;
    private const int AckSize = 12;
    private const int StateSize = 20;
    private const int LeaseSize = 36;
    private const int SparseIndexStride = 64;
    private const uint MaxValueLength = 4 * 1024 * 1024;

    private readonly string _path;
    private readonly string _ackPath;
    private readonly string _statePath;
    private readonly string _leasePath;
    private readonly object _lock = new();

    private FileStream _fs;
    private long _writeOffset;
    private HashSet<EntryKey> _entryKeys;
    private List<IndexPoint> _sparseIndex;
    private int _recordCount;
    private ulong _nextSeq;
    private ulong _ackedThrough;
    private ulong _maxCommitSeq;
    private ulong _materializedThroughCommitSeq;
    private ulong _leaseEpoch;
    private byte[] _leaseOwnerId;
    private bool _disposed;
    private bool _faulted;

    private readonly record struct EntryKey(
        ulong CommitSeq, ulong RecordLo, ulong RecordHi, ushort FieldId)
    {
        public static EntryKey From(OutboxEntry entry)
        {
            if (entry.RecordId.Length != 16)
                throw new ArgumentException("RecordId must be 16 bytes.", nameof(entry));

            return new EntryKey(
                entry.CommitSeq,
                BinaryPrimitives.ReadUInt64LittleEndian(entry.RecordId),
                BinaryPrimitives.ReadUInt64LittleEndian(entry.RecordId.AsSpan(8)),
                entry.FieldId);
        }

        public static EntryKey From(ReadOnlySpan<byte> fixedPart)
            => new(
                BinaryPrimitives.ReadUInt64LittleEndian(fixedPart.Slice(8, 8)),
                BinaryPrimitives.ReadUInt64LittleEndian(fixedPart.Slice(32, 8)),
                BinaryPrimitives.ReadUInt64LittleEndian(fixedPart.Slice(40, 8)),
                BinaryPrimitives.ReadUInt16LittleEndian(fixedPart.Slice(48, 2)));
    }

    private readonly record struct IndexPoint(ulong Seq, long Offset);

    public ulong NextSeq { get { lock (_lock) return _nextSeq; } }
    public ulong HighWatermark { get { lock (_lock) return _nextSeq - 1; } }
    public ulong AckedThrough { get { lock (_lock) return _ackedThrough; } }
    public ulong Watermark => AckedThrough;
    public ulong MaterializedThroughCommitSeq
    {
        get { lock (_lock) return _materializedThroughCommitSeq; }
    }

    private OutboxLog(
        string path,
        string ackPath,
        string statePath,
        string leasePath,
        FileStream fs,
        ulong nextSeq,
        ulong ackedThrough,
        ulong maxCommitSeq,
        ulong materializedThroughCommitSeq,
        HashSet<EntryKey> entryKeys,
        List<IndexPoint> sparseIndex,
        int recordCount,
        ulong leaseEpoch,
        byte[] leaseOwnerId)
    {
        _path = path;
        _ackPath = ackPath;
        _statePath = statePath;
        _leasePath = leasePath;
        _fs = fs;
        _writeOffset = fs.Length;
        _nextSeq = nextSeq;
        _ackedThrough = ackedThrough;
        _maxCommitSeq = maxCommitSeq;
        _materializedThroughCommitSeq = materializedThroughCommitSeq;
        _entryKeys = entryKeys;
        _sparseIndex = sparseIndex;
        _recordCount = recordCount;
        _leaseEpoch = leaseEpoch;
        _leaseOwnerId = leaseOwnerId;
    }

    public static OutboxLog Open(
        string dir, uint namespaceId, ulong checkpointedThroughCommitSeq = 0)
    {
        Directory.CreateDirectory(dir);

        string path = Path.Combine(dir, $"ns_{namespaceId}.obx");
        string ackPath = Path.Combine(dir, $"ns_{namespaceId}.obx.ack");
        string statePath = Path.Combine(dir, $"ns_{namespaceId}.obx.state");
        string leasePath = Path.Combine(dir, $"ns_{namespaceId}.obx.lease");
        var (leaseEpoch, leaseOwnerId) = LoadLease(leasePath);

        ulong ackedThrough = LoadAck(ackPath);
        ulong storedMaterializedThrough = LoadMaterializedWatermark(statePath);
        ulong materializedThrough = Math.Max(storedMaterializedThrough, checkpointedThroughCommitSeq);

        if (!File.Exists(path))
        {
            if (File.Exists(ackPath) || File.Exists(statePath) || File.Exists(leasePath) || checkpointedThroughCommitSeq != 0)
                throw new InvalidDataException(
                    "Outbox log is missing but ACK/checkpoint metadata proves that it previously existed.");

            var created = CreateNew(path);
            return new OutboxLog(
                path, ackPath, statePath, leasePath, created,
                nextSeq: 1,
                ackedThrough: 0,
                maxCommitSeq: 0,
                materializedThroughCommitSeq: 0,
                entryKeys: new HashSet<EntryKey>(),
                sparseIndex: new List<IndexPoint>(),
                recordCount: 0,
                leaseEpoch: leaseEpoch, leaseOwnerId: leaseOwnerId);
        }

        var fs = new FileStream(
            path, FileMode.Open, FileAccess.ReadWrite, FileShare.Read,
            bufferSize: 64 * 1024, FileOptions.None);

        try
        {
            if (fs.Length < HeaderSize)
                throw new InvalidDataException("Outbox header is truncated.");

            Span<byte> header = stackalloc byte[HeaderSize];
            fs.Position = 0;
            fs.ReadExactly(header);

            if (!header.Slice(0, 4).SequenceEqual(Magic))
                throw new InvalidDataException("Outbox magic mismatch.");
            if (BinaryPrimitives.ReadUInt16LittleEndian(header.Slice(4, 2)) != Version)
                throw new InvalidDataException("Unsupported Outbox version.");
            if (header[6] != 0 || header[7] != 0)
                throw new InvalidDataException("Outbox reserved header bytes are non-zero.");

            ulong headerNextSeq = BinaryPrimitives.ReadUInt64LittleEndian(header.Slice(8, 8));
            if (headerNextSeq == 0)
                throw new InvalidDataException("Outbox next sequence is zero.");

            var scan = ScanAndRepairTornTail(fs, materializedThrough);
            ulong scannedNextSeq = scan.LastSeq == 0 ? 1 : checked(scan.LastSeq + 1);
            ulong nextSeq = Math.Max(headerNextSeq, scannedNextSeq);

            if (ackedThrough >= nextSeq)
                throw new InvalidDataException(
                    $"Outbox ACK watermark {ackedThrough} exceeds last assigned sequence {nextSeq - 1}.");

            if (scan.Truncated)
            {
                // A materialized prefix may no longer exist in WAL. Never cut an Outbox
                // tail in that state: the torn bytes may be the only copy of a sync event.
                if (materializedThrough != 0)
                    throw new InvalidDataException(
                        "Outbox has an incomplete tail after WAL materialization; refusing destructive recovery.");

                fs.SetLength(scan.ValidEndOffset);
                fs.Flush(flushToDisk: true);
            }
            else if (materializedThrough > storedMaterializedThrough)
            {
                // Manifest checkpoint metadata is only promoted after all existing Outbox
                // bytes are durable, before the watermark sidecar becomes authoritative.
                fs.Flush(flushToDisk: true);
            }

            if (materializedThrough > storedMaterializedThrough)
                WriteMaterializedWatermark(statePath, materializedThrough);

            // Opening a new Outbox owner invalidates any lease left by a previous
            // process/session before exposing the database to a new consumer.
            if (!IsZero(leaseOwnerId))
            {
                if (leaseEpoch == ulong.MaxValue)
                    throw new InvalidOperationException("Outbox lease epoch space is exhausted.");
                leaseEpoch++;
                leaseOwnerId = new byte[16];
                WriteLease(leasePath, leaseEpoch, leaseOwnerId);
            }

            fs.Seek(0, SeekOrigin.End);
            return new OutboxLog(
                path, ackPath, statePath, leasePath, fs, nextSeq, ackedThrough,
                Math.Max(scan.MaxCommitSeq, materializedThrough),
                materializedThrough, scan.EntryKeys, scan.SparseIndex, scan.RecordCount,
                leaseEpoch, leaseOwnerId);
        }
        catch
        {
            fs.Dispose();
            throw;
        }
    }

    private static FileStream CreateNew(string path)
    {
        var fs = new FileStream(
            path, FileMode.CreateNew, FileAccess.ReadWrite, FileShare.Read,
            bufferSize: 64 * 1024, FileOptions.None);

        Span<byte> header = stackalloc byte[HeaderSize];
        header.Clear();
        Magic.CopyTo(header);
        BinaryPrimitives.WriteUInt16LittleEndian(header.Slice(4, 2), Version);
        BinaryPrimitives.WriteUInt64LittleEndian(header.Slice(8, 8), 1);
        fs.Write(header);
        fs.Flush(flushToDisk: true);
        return fs;
    }

    private static (ulong Epoch, byte[] OwnerId) LoadLease(string path)
    {
        if (!File.Exists(path))
            return (0, new byte[16]);

        byte[] bytes = File.ReadAllBytes(path);
        if (bytes.Length != LeaseSize || !bytes.AsSpan(0, 4).SequenceEqual(LeaseMagic)
            || BinaryPrimitives.ReadUInt16LittleEndian(bytes.AsSpan(4, 2)) != LeaseVersion
            || bytes[6] != 0 || bytes[7] != 0)
            throw new InvalidDataException("Outbox lease file has invalid magic, version, reserved bytes, or length.");

        uint stored = BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(32, 4));
        if (!Hashes.IsCrc32COrLegacy(bytes.AsSpan(0, 32), stored))
            throw new InvalidDataException("Outbox lease CRC mismatch.");

        ulong epoch = BinaryPrimitives.ReadUInt64LittleEndian(bytes.AsSpan(8, 8));
        if (epoch == 0)
            throw new InvalidDataException("Outbox lease epoch is zero.");
        return (epoch, bytes.AsSpan(16, 16).ToArray());
    }

    private static void WriteLease(string path, ulong epoch, ReadOnlySpan<byte> ownerId)
    {
        Span<byte> bytes = stackalloc byte[LeaseSize];
        bytes.Clear();
        LeaseMagic.CopyTo(bytes);
        BinaryPrimitives.WriteUInt16LittleEndian(bytes.Slice(4, 2), LeaseVersion);
        BinaryPrimitives.WriteUInt64LittleEndian(bytes.Slice(8, 8), epoch);
        ownerId.CopyTo(bytes.Slice(16, 16));
        BinaryPrimitives.WriteUInt32LittleEndian(bytes.Slice(32, 4), Hashes.Crc32C(bytes.Slice(0, 32)));
        WriteAtomic(path, bytes);
    }

    private void ValidateLease(OutboxLease lease)
    {
        ArgumentNullException.ThrowIfNull(lease);
        if (lease.Epoch != _leaseEpoch || IsZero(_leaseOwnerId)
            || !lease.OwnerId.SequenceEqual(_leaseOwnerId))
            throw new InvalidOperationException("Outbox lease is stale or is not the current owner.");
    }

    private static bool IsZero(ReadOnlySpan<byte> value)
    {
        int aggregate = 0;
        foreach (byte item in value)
            aggregate |= item;
        return aggregate == 0;
    }

    /// <summary>Acquire a new durable lease and fence all previously issued tokens.</summary>
    public OutboxLease AcquireLease(ReadOnlySpan<byte> ownerId)
    {
        if (ownerId.Length != 16 || IsZero(ownerId))
            throw new ArgumentException("Lease owner must be a non-zero 16-byte id.", nameof(ownerId));

        lock (_lock)
        {
            EnsureUsable();
            if (_leaseEpoch == ulong.MaxValue)
                throw new InvalidOperationException("Outbox lease epoch space is exhausted.");
            ulong nextEpoch = _leaseEpoch + 1;
            byte[] nextOwner = ownerId.ToArray();
            WriteLease(_leasePath, nextEpoch, nextOwner);
            _leaseEpoch = nextEpoch;
            _leaseOwnerId = nextOwner;
            return new OutboxLease(nextEpoch, nextOwner);
        }
    }

    /// <summary>Release the current lease and immediately invalidate its token.</summary>
    public void ReleaseLease(OutboxLease lease)
    {
        lock (_lock)
        {
            EnsureUsable();
            ValidateLease(lease);
            if (_leaseEpoch == ulong.MaxValue)
                throw new InvalidOperationException("Outbox lease epoch space is exhausted.");
            ulong nextEpoch = _leaseEpoch + 1;
            byte[] noOwner = new byte[16];
            WriteLease(_leasePath, nextEpoch, noOwner);
            _leaseEpoch = nextEpoch;
            _leaseOwnerId = noOwner;
        }
    }

    private static ulong LoadAck(string path)
    {
        if (!File.Exists(path))
            return 0;

        byte[] bytes = File.ReadAllBytes(path);
        if (bytes.Length != AckSize)
            throw new InvalidDataException("Outbox ACK file has an invalid length.");

        uint stored = BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(8, 4));
        if (!Hashes.IsCrc32COrLegacy(bytes.AsSpan(0, 8), stored))
            throw new InvalidDataException("Outbox ACK CRC mismatch.");

        return BinaryPrimitives.ReadUInt64LittleEndian(bytes.AsSpan(0, 8));
    }

    private static ulong LoadMaterializedWatermark(string path)
    {
        if (!File.Exists(path))
            return 0;

        byte[] bytes = File.ReadAllBytes(path);
        if (bytes.Length != StateSize || !bytes.AsSpan(0, 4).SequenceEqual(StateMagic))
            throw new InvalidDataException("Outbox state file has invalid magic or length.");
        if (BinaryPrimitives.ReadUInt16LittleEndian(bytes.AsSpan(4, 2)) != Version
            || bytes[6] != 0 || bytes[7] != 0)
            throw new InvalidDataException("Outbox state file version/reserved bytes are invalid.");

        uint stored = BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(16, 4));
        if (!Hashes.IsCrc32COrLegacy(bytes.AsSpan(0, 16), stored))
            throw new InvalidDataException("Outbox state CRC mismatch.");

        return BinaryPrimitives.ReadUInt64LittleEndian(bytes.AsSpan(8, 8));
    }

    private readonly record struct ScanResult(
        HashSet<EntryKey> EntryKeys,
        ulong LastSeq,
        ulong MaxCommitSeq,
        bool Truncated,
        List<IndexPoint> SparseIndex,
        int RecordCount,
        long ValidEndOffset);

    private static ScanResult ScanAndRepairTornTail(FileStream fs, ulong materializedThrough)
    {
        var keys = new HashSet<EntryKey>();
        var sparseIndex = new List<IndexPoint>();
        long position = HeaderSize;
        long fileLength = fs.Length;
        ulong previousSeq = 0;
        ulong maxCommitSeq = 0;
        bool truncated = false;
        int recordCount = 0;
        Span<byte> fixedPart = stackalloc byte[FixedRecordSize];

        while (position < fileLength)
        {
            long remaining = fileLength - position;
            if (remaining < FixedRecordSize + RecordCrcSize)
            {
                truncated = true;
                break;
            }

            fs.Position = position;
            fs.ReadExactly(fixedPart);
            uint valueLength = BinaryPrimitives.ReadUInt32LittleEndian(
                fixedPart.Slice(FixedRecordSize - 4, 4));
            if (valueLength > MaxValueLength)
                throw new InvalidDataException(
                    $"Outbox value length {valueLength} exceeds limit at offset {position}.");

            long recordLength = (long)FixedRecordSize + valueLength + RecordCrcSize;
            if (recordLength > remaining)
            {
                truncated = true;
                break;
            }

            ValidateFixedRecord(fixedPart, position);
            ulong seq = BinaryPrimitives.ReadUInt64LittleEndian(fixedPart.Slice(0, 8));
            if (seq == 0 || (previousSeq != 0 && seq != checked(previousSeq + 1)))
                throw new InvalidDataException($"Outbox sequence discontinuity at offset {position}.");

            int length = checked((int)recordLength);
            byte[] rented = ArrayPool<byte>.Shared.Rent(length);
            try
            {
                fs.Position = position;
                fs.ReadExactly(rented.AsSpan(0, length));
                VerifyRecordCrc(rented.AsSpan(0, length), position);
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(rented);
            }

            ulong commitSeq = BinaryPrimitives.ReadUInt64LittleEndian(fixedPart.Slice(8, 8));
            if (commitSeq > materializedThrough && !keys.Add(EntryKey.From(fixedPart)))
                throw new InvalidDataException($"Duplicate unmaterialized Outbox field mutation at offset {position}.");

            maxCommitSeq = Math.Max(maxCommitSeq, commitSeq);

            if ((recordCount % SparseIndexStride) == 0)
                sparseIndex.Add(new IndexPoint(seq, position));

            previousSeq = seq;
            recordCount++;
            position += recordLength;
        }

        return new ScanResult(
            keys, previousSeq, maxCommitSeq, truncated, sparseIndex, recordCount, position);
    }

    private static void ValidateFixedRecord(ReadOnlySpan<byte> fixedPart, long offset)
    {
        if (fixedPart[50] is not (1 or 2) || fixedPart[51] != 0
            || fixedPart[82] != 0 || fixedPart[83] != 0)
            throw new InvalidDataException($"Outbox op/reserved fields are invalid at offset {offset}.");
    }

    private static void VerifyRecordCrc(ReadOnlySpan<byte> record, long offset)
    {
        uint stored = BinaryPrimitives.ReadUInt32LittleEndian(
            record.Slice(record.Length - RecordCrcSize, RecordCrcSize));
        if (!Hashes.IsCrc32COrLegacy(
                record.Slice(0, record.Length - RecordCrcSize), stored))
            throw new InvalidDataException($"Outbox CRC mismatch at offset {offset}.");
    }

    public ulong Append(OutboxEntry entry)
    {
        lock (_lock)
        {
            EnsureUsable();
            ValidateEntry(entry);
            EntryKey key = EntryKey.From(entry);
            if (_entryKeys.Contains(key))
                throw new InvalidOperationException("Duplicate Outbox field mutation.");

            ulong sequence = AppendUnlocked(entry);
            _entryKeys.Add(key);
            return sequence;
        }
    }

    /// <summary>Append a field mutation only when it is not already materialized.</summary>
    public bool AppendIfMissing(OutboxEntry entry)
    {
        lock (_lock)
        {
            EnsureUsable();
            ValidateEntry(entry);
            EntryKey key = EntryKey.From(entry);
            if (_entryKeys.Contains(key))
                return false;

            AppendUnlocked(entry);
            _entryKeys.Add(key);
            return true;
        }
    }

    private static void ValidateEntry(OutboxEntry entry)
    {
        ArgumentNullException.ThrowIfNull(entry);
        if (entry.ChangeId.Length != 16 || entry.RecordId.Length != 16 || entry.AuthorNodeId.Length != 16)
            throw new ArgumentException("Outbox identifiers must be exactly 16 bytes.");
        if (entry.OpKind is not (1 or 2))
            throw new ArgumentOutOfRangeException(nameof(entry.OpKind));
        if (entry.Value.Length > MaxValueLength)
            throw new ArgumentOutOfRangeException(nameof(entry.Value), "Outbox value exceeds 4 MiB.");
    }

    private ulong AppendUnlocked(OutboxEntry entry)
    {
        ulong sequence = _nextSeq;
        int valueLength = entry.Value.Length;
        Span<byte> fixedPart = stackalloc byte[FixedRecordSize];
        fixedPart.Clear();

        int offset = 0;
        BinaryPrimitives.WriteUInt64LittleEndian(fixedPart.Slice(offset, 8), sequence); offset += 8;
        BinaryPrimitives.WriteUInt64LittleEndian(fixedPart.Slice(offset, 8), entry.CommitSeq); offset += 8;
        entry.ChangeId.CopyTo(fixedPart.Slice(offset, 16)); offset += 16;
        entry.RecordId.CopyTo(fixedPart.Slice(offset, 16)); offset += 16;
        BinaryPrimitives.WriteUInt16LittleEndian(fixedPart.Slice(offset, 2), entry.FieldId); offset += 2;
        fixedPart[offset++] = entry.OpKind;
        fixedPart[offset++] = 0;
        entry.AuthorNodeId.CopyTo(fixedPart.Slice(offset, 16)); offset += 16;
        BinaryPrimitives.WriteUInt64LittleEndian(fixedPart.Slice(offset, 8), entry.Wall); offset += 8;
        BinaryPrimitives.WriteUInt32LittleEndian(fixedPart.Slice(offset, 4), entry.Logical); offset += 4;
        BinaryPrimitives.WriteUInt16LittleEndian(fixedPart.Slice(offset, 2), entry.KeyEpoch); offset += 2;
        offset += 2;
        BinaryPrimitives.WriteUInt32LittleEndian(fixedPart.Slice(offset, 4), (uint)valueLength);

        uint crc = Hashes.Crc32C(fixedPart, entry.Value);
        Span<byte> crcBuffer = stackalloc byte[RecordCrcSize];
        BinaryPrimitives.WriteUInt32LittleEndian(crcBuffer, crc);

        // Exhaustion/offset overflow must be detected before touching the log.
        // Otherwise the final record could be written successfully and only then fail
        // while advancing the in-memory cursor, leaving a log that cannot be reopened.
        if (sequence == ulong.MaxValue)
            throw new InvalidOperationException("Outbox sequence space is exhausted.");

        ulong nextSequence = sequence + 1;
        long recordOffset = _writeOffset;
        long nextWriteOffset = checked(recordOffset + FixedRecordSize + (long)valueLength + RecordCrcSize);
        try
        {
            _fs.Position = recordOffset;
            _fs.Write(fixedPart);
            _fs.Write(entry.Value.AsSpan());
            _fs.Write(crcBuffer);

            if ((_recordCount % SparseIndexStride) == 0)
                _sparseIndex.Add(new IndexPoint(sequence, recordOffset));

            _writeOffset = nextWriteOffset;
            _nextSeq = nextSequence;
            _maxCommitSeq = Math.Max(_maxCommitSeq, entry.CommitSeq);
            _recordCount++;
            return sequence;
        }
        catch
        {
            _faulted = true;
            throw;
        }
    }

    public void Flush()
    {
        lock (_lock)
        {
            EnsureUsable();
            try
            {
                _fs.Flush(flushToDisk: true);
            }
            catch
            {
                _faulted = true;
                throw;
            }
        }
    }

    /// <summary>Enumerate using a current fenced consumer lease.</summary>
    public int VisitBatch(ulong afterSeq, int limit, OutboxLease lease, OutboxEntryVisitor visitor)
    {
        ArgumentNullException.ThrowIfNull(visitor);
        lock (_lock)
        {
            EnsureUsable();
            ValidateLease(lease);
            if (limit <= 0)
                return 0;
            return VisitBatchUnlocked(afterSeq, limit, visitor);
        }
    }

    private int VisitBatchUnlocked(ulong afterSeq, int limit, OutboxEntryVisitor visitor)
    {
        ulong minimumExclusive = Math.Max(afterSeq, _ackedThrough);
        long fileLength = _writeOffset;
        long position = FindReadStartOffset(minimumExclusive);
        Span<byte> fixedPart = stackalloc byte[FixedRecordSize];
        int visited = 0;
        byte[]? rented = null;

        try
        {
            while (position < fileLength && visited < limit)
            {
                long remaining = fileLength - position;
                if (remaining < FixedRecordSize + RecordCrcSize)
                    throw new InvalidDataException($"Outbox has an incomplete record at offset {position}.");

                _fs.Position = position;
                _fs.ReadExactly(fixedPart);
                uint valueLength = BinaryPrimitives.ReadUInt32LittleEndian(
                    fixedPart.Slice(FixedRecordSize - 4, 4));
                if (valueLength > MaxValueLength)
                    throw new InvalidDataException($"Outbox value length {valueLength} exceeds limit at offset {position}.");

                long recordLength = (long)FixedRecordSize + valueLength + RecordCrcSize;
                if (recordLength > remaining)
                    throw new InvalidDataException($"Outbox record is truncated at offset {position}.");

                ValidateFixedRecord(fixedPart, position);
                ulong seq = BinaryPrimitives.ReadUInt64LittleEndian(fixedPart.Slice(0, 8));
                if (seq == 0)
                    throw new InvalidDataException($"Outbox sequence zero at offset {position}.");

                int length = checked((int)recordLength);
                if (rented is null || rented.Length < length)
                {
                    if (rented is not null)
                        ArrayPool<byte>.Shared.Return(rented);
                    rented = ArrayPool<byte>.Shared.Rent(length);
                }

                fixedPart.CopyTo(rented);
                _fs.Position = position + FixedRecordSize;
                _fs.ReadExactly(rented.AsSpan(FixedRecordSize, length - FixedRecordSize));
                ReadOnlySpan<byte> record = rented.AsSpan(0, length);
                VerifyRecordCrc(record, position);

                bool keepReading = true;
                if (seq > minimumExclusive)
                {
                    var view = new OutboxEntryView(record, valueLength);
                    visited++;
                    keepReading = visitor(in view);
                }

                position += recordLength;
                if (!keepReading)
                    break;
            }
        }
        finally
        {
            if (rented is not null)
                ArrayPool<byte>.Shared.Return(rented);
        }

        return visited;
    }

    public List<OutboxEntry> ReadBatch(ulong afterSeq, int limit)
    {
        if (limit <= 0)
            return [];

        lock (_lock)
        {
            EnsureUsable();
            return ReadBatchUnlocked(afterSeq, limit);
        }
    }

    private List<OutboxEntry> ReadBatchUnlocked(ulong afterSeq, int limit)
    {
        var result = new List<OutboxEntry>(Math.Min(limit, 64));

        bool AddEntry(in OutboxEntryView view)
        {
            result.Add(new OutboxEntry
            {
                OutboxSeq = view.OutboxSeq,
                CommitSeq = view.CommitSeq,
                ChangeId = view.ChangeId.ToArray(),
                RecordId = view.RecordId.ToArray(),
                FieldId = view.FieldId,
                OpKind = view.OpKind,
                AuthorNodeId = view.AuthorNodeId.ToArray(),
                Wall = view.Wall,
                Logical = view.Logical,
                KeyEpoch = view.KeyEpoch,
                Value = view.Value.IsEmpty ? Array.Empty<byte>() : view.Value.ToArray()
            });
            return true;
        }

        VisitBatchUnlocked(afterSeq, limit, AddEntry);
        return result;
    }

    private long FindReadStartOffset(ulong minimumExclusive)
    {
        if (_sparseIndex.Count == 0)
            return HeaderSize;

        int low = 0;
        int high = _sparseIndex.Count - 1;
        int candidate = -1;

        while (low <= high)
        {
            int middle = low + ((high - low) >> 1);
            if (_sparseIndex[middle].Seq <= minimumExclusive)
            {
                candidate = middle;
                low = middle + 1;
            }
            else
            {
                high = middle - 1;
            }
        }

        if (candidate < 0)
            candidate = 0;

        return _sparseIndex[candidate].Offset;
    }

    private static OutboxEntry DecodeEntry(ReadOnlySpan<byte> record, uint valueLength)
    {
        int offset = 0;
        ulong seq = BinaryPrimitives.ReadUInt64LittleEndian(record.Slice(offset, 8)); offset += 8;
        ulong commitSeq = BinaryPrimitives.ReadUInt64LittleEndian(record.Slice(offset, 8)); offset += 8;
        byte[] changeId = record.Slice(offset, 16).ToArray(); offset += 16;
        byte[] recordId = record.Slice(offset, 16).ToArray(); offset += 16;
        ushort fieldId = BinaryPrimitives.ReadUInt16LittleEndian(record.Slice(offset, 2)); offset += 2;
        byte opKind = record[offset++];
        offset++;
        byte[] author = record.Slice(offset, 16).ToArray(); offset += 16;
        ulong wall = BinaryPrimitives.ReadUInt64LittleEndian(record.Slice(offset, 8)); offset += 8;
        uint logical = BinaryPrimitives.ReadUInt32LittleEndian(record.Slice(offset, 4)); offset += 4;
        ushort keyEpoch = BinaryPrimitives.ReadUInt16LittleEndian(record.Slice(offset, 2)); offset += 2;
        offset += 2;
        offset += 4;
        byte[] value = valueLength == 0
            ? Array.Empty<byte>()
            : record.Slice(offset, checked((int)valueLength)).ToArray();

        return new OutboxEntry
        {
            OutboxSeq = seq,
            CommitSeq = commitSeq,
            ChangeId = changeId,
            RecordId = recordId,
            FieldId = fieldId,
            OpKind = opKind,
            AuthorNodeId = author,
            Wall = wall,
            Logical = logical,
            KeyEpoch = keyEpoch,
            Value = value
        };
    }

    /// <summary>
    /// Persist that Outbox entries through a WAL CommitSeq are durable before the
    /// corresponding sealed WAL prefix is truncated.
    /// </summary>
    public void MarkMaterialized(ulong throughCommitSeq)
    {
        lock (_lock)
        {
            EnsureUsable();
            if (throughCommitSeq <= _materializedThroughCommitSeq)
                return;

            _fs.Flush(flushToDisk: true);
            WriteMaterializedWatermark(_statePath, throughCommitSeq);
            _materializedThroughCommitSeq = throughCommitSeq;

            _entryKeys.RemoveWhere(key => key.CommitSeq <= throughCommitSeq);
        }
    }

    public void Acknowledge(ulong throughSeq)
    {
        lock (_lock)
        {
            EnsureUsable();
            AcknowledgeUnlocked(throughSeq);
        }
    }

    /// <summary>Advance ACK only while holding the current fenced consumer lease.</summary>
    public void Acknowledge(ulong throughSeq, OutboxLease lease)
    {
        lock (_lock)
        {
            EnsureUsable();
            ValidateLease(lease);
            AcknowledgeUnlocked(throughSeq);
        }
    }

    private void AcknowledgeUnlocked(ulong throughSeq)
    {
        if (throughSeq <= _ackedThrough)
            return;

        ulong lastAssigned = _nextSeq - 1;
        if (throughSeq > lastAssigned)
            throw new ArgumentOutOfRangeException(
                nameof(throughSeq),
                $"ACK {throughSeq} exceeds last assigned OutboxSeq {lastAssigned}.");

        Span<byte> ack = stackalloc byte[AckSize];
        BinaryPrimitives.WriteUInt64LittleEndian(ack.Slice(0, 8), throughSeq);
        BinaryPrimitives.WriteUInt32LittleEndian(
            ack.Slice(8, 4), Hashes.Crc32C(ack.Slice(0, 8)));

        WriteAtomic(_ackPath, ack);
        _ackedThrough = throughSeq;
    }

    /// <summary>
    /// Remove ACKed entries and persist the WAL materialization boundary before replacing
    /// the log. This prevents startup replay from resurrecting changes already ACKed/pruned.
    /// </summary>
    public int Compact()
    {
        lock (_lock)
        {
            EnsureUsable();
            _fs.Flush(flushToDisk: true);

            string tempPath = _path + ".tmp";
            var retainedKeys = new HashSet<EntryKey>();
            var retainedIndex = new List<IndexPoint>();
            int retainedCount = 0;
            ulong maxCommitSeq = _maxCommitSeq;

            using (var output = new FileStream(
                tempPath, FileMode.Create, FileAccess.Write, FileShare.None,
                bufferSize: 64 * 1024, FileOptions.WriteThrough))
            {
                Span<byte> header = stackalloc byte[HeaderSize];
                header.Clear();
                Magic.CopyTo(header);
                BinaryPrimitives.WriteUInt16LittleEndian(header.Slice(4, 2), Version);
                BinaryPrimitives.WriteUInt64LittleEndian(header.Slice(8, 8), _nextSeq);
                output.Write(header);

                long position = HeaderSize;
                long fileLength = _fs.Length;
                Span<byte> fixedPart = stackalloc byte[FixedRecordSize];

                while (position < fileLength)
                {
                    long remaining = fileLength - position;
                    if (remaining < FixedRecordSize + RecordCrcSize)
                        throw new InvalidDataException($"Outbox has an incomplete record at offset {position}.");

                    _fs.Position = position;
                    _fs.ReadExactly(fixedPart);
                    uint valueLength = BinaryPrimitives.ReadUInt32LittleEndian(
                        fixedPart.Slice(FixedRecordSize - 4, 4));
                    if (valueLength > MaxValueLength)
                        throw new InvalidDataException($"Outbox value length {valueLength} exceeds limit at offset {position}.");

                    long recordLength = (long)FixedRecordSize + valueLength + RecordCrcSize;
                    if (recordLength > remaining)
                        throw new InvalidDataException($"Outbox record is truncated at offset {position}.");

                    ValidateFixedRecord(fixedPart, position);
                    int length = checked((int)recordLength);
                    byte[] rented = ArrayPool<byte>.Shared.Rent(length);
                    try
                    {
                        _fs.Position = position;
                        _fs.ReadExactly(rented.AsSpan(0, length));
                        ReadOnlySpan<byte> record = rented.AsSpan(0, length);
                        VerifyRecordCrc(record, position);

                        ulong seq = BinaryPrimitives.ReadUInt64LittleEndian(fixedPart.Slice(0, 8));
                        ulong commitSeq = BinaryPrimitives.ReadUInt64LittleEndian(fixedPart.Slice(8, 8));
                        maxCommitSeq = Math.Max(maxCommitSeq, commitSeq);

                        if (seq > _ackedThrough)
                        {
                            long outputOffset = output.Position;
                            if ((retainedCount % SparseIndexStride) == 0)
                                retainedIndex.Add(new IndexPoint(seq, outputOffset));

                            output.Write(record);
                            retainedKeys.Add(EntryKey.From(fixedPart));
                            retainedCount++;
                        }
                    }
                    finally
                    {
                        ArrayPool<byte>.Shared.Return(rented);
                    }

                    position += recordLength;
                }

                output.Flush(flushToDisk: true);
            }

            if (maxCommitSeq > _materializedThroughCommitSeq)
            {
                WriteMaterializedWatermark(_statePath, maxCommitSeq);
                _materializedThroughCommitSeq = maxCommitSeq;
            }

            _fs.Dispose();
            try
            {
                File.Move(tempPath, _path, overwrite: true);
                _fs = new FileStream(
                    _path, FileMode.Open, FileAccess.ReadWrite, FileShare.Read,
                    bufferSize: 64 * 1024, FileOptions.None);
                _fs.Seek(0, SeekOrigin.End);
                _writeOffset = _fs.Position;
            }
            catch
            {
                if (File.Exists(_path))
                {
                    _fs = new FileStream(
                        _path, FileMode.Open, FileAccess.ReadWrite, FileShare.Read,
                        bufferSize: 64 * 1024, FileOptions.None);
                    _fs.Seek(0, SeekOrigin.End);
                }
                else
                {
                    _faulted = true;
                }
                throw;
            }

            TryFsyncDirectory(Path.GetDirectoryName(_path)!);
            // Every retained entry is now covered by the durable materialization watermark.
            // Keep no per-entry replay-dedupe keys for this sealed prefix.
            _entryKeys = new HashSet<EntryKey>();
            _sparseIndex = retainedIndex;
            _recordCount = retainedCount;
            _maxCommitSeq = maxCommitSeq;
            return retainedCount;
        }
    }

    private static void WriteMaterializedWatermark(string path, ulong commitSeq)
    {
        Span<byte> state = stackalloc byte[StateSize];
        state.Clear();
        StateMagic.CopyTo(state);
        BinaryPrimitives.WriteUInt16LittleEndian(state.Slice(4, 2), Version);
        BinaryPrimitives.WriteUInt64LittleEndian(state.Slice(8, 8), commitSeq);
        BinaryPrimitives.WriteUInt32LittleEndian(
            state.Slice(16, 4), Hashes.Crc32C(state.Slice(0, 16)));
        WriteAtomic(path, state);
    }

    private static void WriteAtomic(string path, ReadOnlySpan<byte> bytes)
    {
        string tempPath = path + ".tmp";
        using (var fs = new FileStream(
            tempPath, FileMode.Create, FileAccess.Write, FileShare.None,
            bufferSize: 4096, FileOptions.WriteThrough))
        {
            fs.Write(bytes);
            fs.Flush(flushToDisk: true);
        }

        File.Move(tempPath, path, overwrite: true);
        TryFsyncDirectory(Path.GetDirectoryName(path)!);
    }

    private static void TryFsyncDirectory(string directory)
    {
        try
        {
            if (OperatingSystem.IsLinux() || OperatingSystem.IsMacOS())
            {
                using var fs = new FileStream(directory, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
                fs.Flush(flushToDisk: true);
            }
        }
        catch
        {
            // Directory handles cannot be flushed on every supported filesystem.
        }
    }

    private void EnsureUsable()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (_faulted)
            throw new InvalidOperationException(
                "Outbox is in an uncertain write state; close and reopen the database.");
    }

    public void Dispose()
    {
        lock (_lock)
        {
            if (_disposed)
                return;

            _disposed = true;
            _fs.Dispose();
        }
    }
}
