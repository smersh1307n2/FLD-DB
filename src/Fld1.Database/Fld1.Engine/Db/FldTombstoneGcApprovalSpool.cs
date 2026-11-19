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
    private const int MaxEntries = 2_000_000;
    private readonly string _path;
    private FileStream? _writer;
    private byte[]? _sealedDigest;
    private bool _disposed;
    private bool _sealed;
    private readonly byte[] _lastRecordId = new byte[16];
    private bool _hasLastRecordId;
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
            Count > MaxEntries - candidates.Count)
            throw new ArgumentOutOfRangeException(nameof(candidates));

        Span<byte> encoded = stackalloc byte[CandidateBytes];
        for (int i = 0; i < candidates.Count; i++)
        {
            FldTombstoneGcCandidate candidate = candidates[i];
            FldDatabaseIpcProtocol.EncodeTombstoneGcCandidate(encoded, candidate);
            AppendEncodedEntry(encoded);
        }
    }

    public void AppendPacked(ReadOnlySpan<byte> packedCandidates, int count)
    {
        ThrowIfUnavailable();
        if (_sealed)
            throw new InvalidOperationException("A sealed tombstone-GC approval spool cannot be appended.");
        if (count is < 1 or > FldDatabaseIpcProtocol.MaxTombstoneGcCandidates ||
            packedCandidates.Length != checked(count * CandidateBytes) ||
            Count > MaxEntries - count)
            throw new ArgumentOutOfRangeException(nameof(count));

        for (int i = 0; i < count; i++)
            AppendEncodedEntry(packedCandidates.Slice(i * CandidateBytes, CandidateBytes));
    }

    private void AppendEncodedEntry(ReadOnlySpan<byte> encoded)
    {
        if (encoded.Length != CandidateBytes)
            throw new InvalidDataException("Tombstone-GC approval spool entry length is invalid.");

        ReadOnlySpan<byte> recordId = encoded[..16];
        ushort fieldId = BinaryPrimitives.ReadUInt16LittleEndian(encoded[16..]);
        ReadOnlySpan<byte> changeId = encoded.Slice(18, 16);
        ReadOnlySpan<byte> authorNodeId = encoded.Slice(34, 16);
        ulong wall = BinaryPrimitives.ReadUInt64LittleEndian(encoded[50..]);
        uint authorEpoch = BinaryPrimitives.ReadUInt32LittleEndian(encoded[62..]);
        ulong originSequence = BinaryPrimitives.ReadUInt64LittleEndian(encoded[66..]);
        byte kind = encoded[74];
        if (recordId.IndexOfAnyExcept((byte)0) < 0 ||
            changeId.IndexOfAnyExcept((byte)0) < 0 ||
            authorNodeId.IndexOfAnyExcept((byte)0) < 0 ||
            wall == 0 || authorEpoch == 0 || originSequence == 0 ||
            kind is not (1 or 2) ||
            (kind == 1 && fieldId != FldLifecycle.FieldId) ||
            (kind == 2 && fieldId == FldLifecycle.FieldId))
            throw new InvalidDataException("Tombstone-GC approval spool entry identity is invalid.");

        if (_hasLastRecordId &&
            CompareKey(_lastRecordId, _lastFieldId, recordId, fieldId) >= 0)
            throw new InvalidDataException(
                "Tombstone-GC approval spool identities must be strictly ordered and unique.");

        _writer!.Write(encoded);
        recordId.CopyTo(_lastRecordId);
        _lastFieldId = fieldId;
        _hasLastRecordId = true;
        Count++;
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
        catch (IOException)
        {
            // Scratch cleanup is retried at the next Host start; it is not commit authority.
        }
        catch (UnauthorizedAccessException)
        {
            // Scratch cleanup is retried at the next Host start; it is not commit authority.
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
        private readonly byte[] _entryBuffer = new byte[CandidateBytes];
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
            while (_hasNext)
            {
                ushort nextFieldId = BinaryPrimitives.ReadUInt16LittleEndian(_entryBuffer.AsSpan(16, 2));
                if (CompareKey(_entryBuffer.AsSpan(0, 16), nextFieldId,
                    current.RecordId.Span, current.FieldId) >= 0)
                    break;
                ReadNext();
            }

            if (!_hasNext)
                return false;

            ushort approvedFieldId = BinaryPrimitives.ReadUInt16LittleEndian(_entryBuffer.AsSpan(16, 2));
            int compare = CompareKey(_entryBuffer.AsSpan(0, 16), approvedFieldId,
                current.RecordId.Span, current.FieldId);
            if (compare > 0)
                return false;

            bool exact = MatchesCurrent(current);
            ReadNext();
            return exact;
        }

        private bool MatchesCurrent(FldTombstoneGcCandidate current)
        {
            ReadOnlySpan<byte> entry = _entryBuffer;
            if (!entry[..16].SequenceEqual(current.RecordId.Span))
                return false;

            int offset = 16;
            ushort fieldId = BinaryPrimitives.ReadUInt16LittleEndian(entry[offset..]);
            offset += 2;
            if (fieldId != current.FieldId ||
                !entry.Slice(offset, 16).SequenceEqual(current.ChangeId.Span))
                return false;
            offset += 16;
            if (!entry.Slice(offset, 16).SequenceEqual(current.AuthorNodeId.Span))
                return false;
            offset += 16;

            ulong wall = BinaryPrimitives.ReadUInt64LittleEndian(entry[offset..]);
            offset += 8;
            uint logical = BinaryPrimitives.ReadUInt32LittleEndian(entry[offset..]);
            offset += 4;
            uint authorEpoch = BinaryPrimitives.ReadUInt32LittleEndian(entry[offset..]);
            offset += 4;
            ulong originSequence = BinaryPrimitives.ReadUInt64LittleEndian(entry[offset..]);
            offset += 8;
            byte kind = entry[offset];

            return wall == current.HlcWall &&
                logical == current.HlcLogical &&
                authorEpoch == current.OriginAuthorEpoch &&
                originSequence == current.OriginSequence &&
                (kind == 1) == current.IsRecordTombstone &&
                (kind == 2) == current.IsFieldTombstone;
        }

        private void ReadNext()
        {
            if (_read == _count)
            {
                if (_stream.ReadByte() != -1)
                    throw new InvalidDataException("Tombstone-GC approval spool contains trailing bytes.");
                _hasNext = false;
                return;
            }

            _stream.ReadExactly(_entryBuffer);
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
