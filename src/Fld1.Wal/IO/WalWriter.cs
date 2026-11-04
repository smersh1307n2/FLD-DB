using System.Buffers.Binary;
using Fld1.Core.Hash;
using Fld1.Wal.Format;

namespace Fld1.Wal.IO;

/// <summary>
/// Serialized WAL append for one namespace. Spec §5.5.
/// Single-writer: all appends under lock. PublishedWalEnd monotonic.
/// </summary>
public sealed class WalWriter : IDisposable
{
    private readonly FileStream _fs;
    private readonly object _mutex = new();
    private ulong _writeOffset;
    private ulong _publishedWalEnd;
    private ulong _nextCommitSeq;
    private bool _accepting = true;
    private bool _disposed;

    public ulong NamespaceId { get; }
    public ulong PublishedWalEnd => Volatile.Read(ref _publishedWalEnd);
    public ulong WriteOffset
    {
        get { lock (_mutex) return _writeOffset; }
    }
    public ulong NextCommitSeq
    {
        get { lock (_mutex) return _nextCommitSeq; }
    }

    private WalWriter(FileStream fs, ulong namespaceId, ulong writeOffset, ulong firstCommitSeq)
    {
        _fs = fs;
        NamespaceId = namespaceId;
        _writeOffset = writeOffset;
        _publishedWalEnd = writeOffset;
        _nextCommitSeq = firstCommitSeq;
    }

    /// <summary>Create a new WAL file with header. firstCommitSeq is the first seq that will be assigned.</summary>
    public static WalWriter Create(string path, ulong namespaceId, ulong firstCommitSeq = 1)
    {
        var fs = new FileStream(path, FileMode.CreateNew, FileAccess.ReadWrite, FileShare.Read,
            bufferSize: 64 * 1024, FileOptions.None);
        Span<byte> hdr = stackalloc byte[WalHeader.Size];
        WalHeader.Write(hdr, namespaceId, firstCommitSeq);
        fs.Write(hdr);
        fs.Flush(flushToDisk: true);
        return new WalWriter(fs, namespaceId, WalHeader.Size, firstCommitSeq);
    }

    /// <summary>Open existing WAL for append after recovery (writeOffset = end of valid prefix).</summary>
    public static WalWriter OpenForAppend(string path, ulong namespaceId, ulong writeOffset, ulong nextCommitSeq)
    {
        var fs = new FileStream(path, FileMode.Open, FileAccess.ReadWrite, FileShare.Read,
            bufferSize: 64 * 1024, FileOptions.None);
        if (writeOffset < WalHeader.Size)
            throw new ArgumentException("writeOffset < header");
        fs.Seek((long)writeOffset, SeekOrigin.Begin);
        return new WalWriter(fs, namespaceId, writeOffset, nextCommitSeq);
    }

    /// <summary>
    /// Append a record. Returns the assigned CommitSeq.
    /// Kind must be known; payload is opaque bytes (already encoded).
    /// </summary>
    public ulong Append(byte kind, ReadOnlySpan<byte> payload, byte flags = 0)
    {
        if (payload.Length > WalConstants.MaxPayloadLen)
            throw new ArgumentException("payload too large");
        if (kind is not (WalConstants.KindGroupCommit or WalConstants.KindNsCreate
            or WalConstants.KindSnapshotInstall))
            throw new ArgumentException("unknown kind");

        lock (_mutex)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            if (!_accepting)
                throw new InvalidOperationException("WAL not accepting appends");

            ulong commitSeq = _nextCommitSeq++;
            uint len = (uint)payload.Length;
            int recordSize = WalEnvelope.AlignedRecordSize(len);

            // Build: Envelope | Payload | RecordCrc | Padding
            var record = new byte[recordSize];
            WalEnvelope.Write(record.AsSpan(0, WalEnvelope.Size), commitSeq, len, kind, flags);
            payload.CopyTo(record.AsSpan(WalEnvelope.Size, payload.Length));

            // RecordCrc = CRC32C(Envelope[0..16) ‖ Payload)  — bytes before EnvelopeCrc field + payload
            // Spec: computed = CRC32C(Envelope[0..16) ‖ Payload)
            Span<byte> crcInput = record.AsSpan(0, 16 + payload.Length);
            // Wait: Envelope is 20B; [0..16) is through Pad, excluding EnvelopeCrc itself.
            // Then payload follows at offset 20. So CRC input is not contiguous in the record buffer
            // when EnvelopeCrc sits in between. Build contiguous for CRC:
            uint recordCrc;
            {
                var tmp = new byte[16 + payload.Length];
                record.AsSpan(0, 16).CopyTo(tmp);
                payload.CopyTo(tmp.AsSpan(16));
                recordCrc = Hashes.Crc32C(tmp);
            }
            int crcOff = WalEnvelope.Size + payload.Length;
            BinaryPrimitives.WriteUInt32LittleEndian(record.AsSpan(crcOff, 4), recordCrc);
            // padding already zero

            _fs.Write(record);
            // Publish after write visible
            Thread.MemoryBarrier();
            ulong newEnd = _writeOffset + (ulong)recordSize;
            Volatile.Write(ref _publishedWalEnd, newEnd);
            _writeOffset = newEnd;
            return commitSeq;
        }
    }

    /// <summary>
    /// fsync up to current PublishedWalEnd. Returns DurableWalOffset captured before fsync.
    /// Spec [WAL-FLUSH-CAPTURE].
    /// </summary>
    public ulong Flush()
    {
        lock (_mutex)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            ulong target = Volatile.Read(ref _publishedWalEnd);
            _fs.Flush(flushToDisk: true);
            return target;
        }
    }

    public void StopAccepting()
    {
        lock (_mutex) _accepting = false;
    }

    public void Dispose()
    {
        lock (_mutex)
        {
            if (_disposed) return;
            _disposed = true;
            _accepting = false;
            _fs.Dispose();
        }
    }
}
