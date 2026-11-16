using System.Buffers.Binary;
using System.Security.Cryptography;
using Fld1.Core.Util;

namespace Fld1.Engine.Db;

/// <summary>
/// Host-owned, disposable scratch spool for proof-approved tombstone identities.
/// It is not a recovery journal and is never referenced by the Manifest.
/// </summary>
internal sealed class FldTombstoneGcApprovalSpool : IDisposable
{
    private const int CandidateBytes = 75;
    private readonly string _path;
    private FileStream? _writer;
    private byte[]? _sealedDigest;
    private bool _disposed;
    private bool _sealed;
    private byte[]? _lastRecordId;
    private ushort _lastFieldId;

    public Guid Token { get; }
    public int Count { get; private set; }
    public bool IsSealed => _sealed;
    public string PathName => _path;

    public FldTombstoneGcApprovalSpool(string directory, Guid token)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(directory);
        if (token == Guid.Empty)
            throw new ArgumentException("Approval-spool token must be nonzero.", nameof(token));
        Directory.CreateDirectory(directory);
        Token = token;
        _path = Path.Combine(directory, token.ToString("N") + ".tombstone-gc-approval.tmp");
        _writer = new FileStream(_path, FileMode.CreateNew, FileAccess.Write, FileShare.None,
            64 * 1024, FileOptions.SequentialScan);
    }

    public void Append(IReadOnlyList<FldTombstoneGcCandidate> candidates)
    {
        ThrowIfUnavailable();
        ArgumentNullException.ThrowIfNull(candidates);
        if (_sealed)
            throw new InvalidOperationException("A sealed tombstone-GC approval spool cannot be appended.");
        if (candidates.Count == 0)
            return;
        if (candidates.Count > FldDatabaseIpcProtocol.MaxTombstoneGcCandidates ||
            Count > int.MaxValue - candidates.Count)
            throw new ArgumentOutOfRangeException(nameof(candidates));

        byte[] payload = FldDatabaseIpcProtocol.EncodeTombstoneGcRequest(candidates, candidates.Count);
        ReadOnlySpan<byte> encoded = payload.AsSpan(4);
        for (int i = 0; i < candidates.Count; i++)
        {
            FldTombstoneGcCandidate candidate = candidates[i];
            if (_lastRecordId is not null &&
                CompareKey(_lastRecordId, _lastFieldId, candidate.RecordId.Span, candidate.FieldId) >= 0)
                throw new InvalidDataException(
                    "Tombstone-GC approval spool identities must be strictly ordered and unique.");

            int offset = i * CandidateBytes;
            _writer!.Write(encoded.Slice(offset, CandidateBytes));
            _lastRecordId = candidate.RecordId.ToArray();
            _lastFieldId = candidate.FieldId;
            Count++;
        }
    }

    public byte[] Seal()
    {
        ThrowIfUnavailable();
        if (_sealed)
            return _sealedDigest!.ToArray();

        _writer!.Flush(flushToDisk: true);
        _writer.Dispose();
        _writer = null;

        var info = new FileInfo(_path);
        if (info.Length != checked((long)Count * CandidateBytes))
            throw new InvalidDataException("Tombstone-GC approval spool length does not match its entry count.");

        using FileStream stream = new(_path, FileMode.Open, FileAccess.Read, FileShare.Read | FileShare.Delete,
            64 * 1024, FileOptions.SequentialScan);
        byte[] digest = SHA256.HashData(stream);
        if (digest.Length != 32)
            throw new InvalidDataException("Tombstone-GC approval spool digest is invalid.");

        _sealedDigest = digest;
        _sealed = true;
        return digest.ToArray();
    }

    public Reader OpenReader()
    {
        ThrowIfUnavailable();
        if (!_sealed || _sealedDigest is null)
            throw new InvalidOperationException("Tombstone-GC approval spool must be sealed before consumption.");
        return new Reader(_path, Count, _sealedDigest);
    }

    private static int CompareKey(
        ReadOnlySpan<byte> leftRecordId, ushort leftFieldId,
        ReadOnlySpan<byte> rightRecordId, ushort rightFieldId)
    {
        int compare = ByteUtil.CompareUnsigned16(leftRecordId, rightRecordId);
        return compare != 0 ? compare : leftFieldId.CompareTo(rightFieldId);
    }

    private static FldTombstoneGcCandidate DecodeCandidate(ReadOnlySpan<byte> bytes)
    {
        if (bytes.Length != CandidateBytes)
            throw new InvalidDataException("Tombstone-GC approval spool entry length is invalid.");

        int offset = 0;
        byte[] recordId = bytes.Slice(offset, 16).ToArray();
        offset += 16;
        ushort fieldId = BinaryPrimitives.ReadUInt16LittleEndian(bytes[offset..]);
        offset += 2;
        byte[] changeId = bytes.Slice(offset, 16).ToArray();
        offset += 16;
        byte[] authorNodeId = bytes.Slice(offset, 16).ToArray();
        offset += 16;
        ulong wall = BinaryPrimitives.ReadUInt64LittleEndian(bytes[offset..]);
        offset += 8;
        uint logical = BinaryPrimitives.ReadUInt32LittleEndian(bytes[offset..]);
        offset += 4;
        uint authorEpoch = BinaryPrimitives.ReadUInt32LittleEndian(bytes[offset..]);
        offset += 4;
        ulong originSequence = BinaryPrimitives.ReadUInt64LittleEndian(bytes[offset..]);
        offset += 8;
        byte kind = bytes[offset];
        if (kind is not (1 or 2) || wall == 0 || authorEpoch == 0 || originSequence == 0)
            throw new InvalidDataException("Tombstone-GC approval spool entry identity is invalid.");

        return new FldTombstoneGcCandidate(
            recordId, fieldId, changeId, authorNodeId, wall, logical,
            authorEpoch, originSequence, kind == 1, kind == 2);
    }

    private void ThrowIfUnavailable()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
    }

    public void Dispose()
    {
        if (_disposed)
            return;
        _disposed = true;
        _writer?.Dispose();
        _writer = null;
        try
        {
            if (File.Exists(_path))
                File.Delete(_path);
        }
        finally
        {
            _sealedDigest = null;
        }
    }

    internal sealed class Reader : IDisposable
    {
        private readonly FileStream _stream;
        private readonly int _count;
        private int _read;
        private FldTombstoneGcCandidate? _next;
        private bool _hasNext;
        private bool _disposed;

        public Reader(string path, int count, ReadOnlySpan<byte> expectedDigest)
        {
            if (expectedDigest.Length != 32)
                throw new ArgumentException("Approval-spool digest must be SHA-256.", nameof(expectedDigest));
            _stream = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read | FileShare.Delete,
                64 * 1024, FileOptions.SequentialScan);
            if (_stream.Length != checked((long)count * CandidateBytes))
            {
                _stream.Dispose();
                throw new InvalidDataException("Tombstone-GC approval spool changed after sealing.");
            }

            using (var verify = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read | FileShare.Delete,
                       64 * 1024, FileOptions.SequentialScan))
            {
                byte[] actualDigest = SHA256.HashData(verify);
                if (!CryptographicOperations.FixedTimeEquals(actualDigest, expectedDigest))
                {
                    _stream.Dispose();
                    throw new InvalidDataException("Tombstone-GC approval spool digest changed after sealing.");
                }
            }

            _count = count;
            ReadNext();
        }

        /// <summary>
        /// Matches the current FLD identity against the next approved identity. Stale
        /// spool entries are skipped; an identity mismatch never authorizes deletion.
        /// Both streams must be ordered by RecordId then FieldId.
        /// </summary>
        public bool IsApproved(FldTombstoneGcCandidate current)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            while (_hasNext &&
                   CompareKey(_next!.RecordId.Span, _next.FieldId,
                       current.RecordId.Span, current.FieldId) < 0)
                ReadNext();

            if (!_hasNext)
                return false;

            int compare = CompareKey(_next!.RecordId.Span, _next.FieldId,
                current.RecordId.Span, current.FieldId);
            if (compare > 0)
                return false;

            bool exact = _next.Equals(current);
            ReadNext();
            return exact;
        }

        private void ReadNext()
        {
            if (_read == _count)
            {
                if (_stream.ReadByte() != -1)
                    throw new InvalidDataException("Tombstone-GC approval spool contains trailing bytes.");
                _next = null;
                _hasNext = false;
                return;
            }

            byte[] entry = new byte[CandidateBytes];
            _stream.ReadExactly(entry);
            _next = DecodeCandidate(entry);
            _hasNext = true;
            _read++;
        }

        public void Dispose()
        {
            if (_disposed)
                return;
            _disposed = true;
            _stream.Dispose();
        }
    }
}
