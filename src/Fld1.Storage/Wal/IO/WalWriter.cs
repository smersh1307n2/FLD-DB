using System.Buffers.Binary;
using Fld1.Storage.Engine;
using Fld1.Storage.Integrity;
using Fld1.Storage.Wal.Format;

namespace Fld1.Storage.Wal.IO;

/// <summary>
/// Serialized WAL append for one namespace. Spec §5.5.
/// Single-writer: all appends under lock. PublishedWalEnd monotonic.
/// </summary>
public sealed class WalWriter : IDisposable
{
    private readonly FileStream _fs;
    private readonly Action<StorageFaultPoint>? _faultInjector;
    private readonly object _mutex = new();
    private ulong _writeOffset;
    private ulong _publishedWalEnd;
    private ulong _nextCommitSeq;
    private bool _accepting = true;
    private bool _disposed;

    public ulong PartitionId { get; }
    public ulong PublishedWalEnd => Volatile.Read(ref _publishedWalEnd);
    public ulong WriteOffset
    {
        get { lock (_mutex) return _writeOffset; }
    }
    public ulong NextCommitSeq
    {
        get { lock (_mutex) return _nextCommitSeq; }
    }

    private WalWriter(FileStream fs, ulong partitionId, ulong writeOffset, ulong firstCommitSeq,
        Action<StorageFaultPoint>? faultInjector)
    {
        _fs = fs;
        _faultInjector = faultInjector;
        PartitionId = partitionId;
        _writeOffset = writeOffset;
        _publishedWalEnd = writeOffset;
        _nextCommitSeq = firstCommitSeq;
    }

    /// <summary>Create a new WAL file with header. firstCommitSeq is the first seq that will be assigned.</summary>
    public static WalWriter Create(string path, ulong partitionId, ulong firstCommitSeq = 1)
        => CreateCore(path, partitionId, firstCommitSeq, faultInjector: null);

    internal static WalWriter CreateForTesting(string path, ulong partitionId, ulong firstCommitSeq,
        Action<StorageFaultPoint> faultInjector)
        => CreateCore(path, partitionId, firstCommitSeq, faultInjector);

    private static WalWriter CreateCore(string path, ulong partitionId, ulong firstCommitSeq,
        Action<StorageFaultPoint>? faultInjector)
    {
        var fs = new FileStream(path, FileMode.CreateNew, FileAccess.ReadWrite, FileShare.Read,
            bufferSize: 64 * 1024, FileOptions.None);
        try
        {
            Span<byte> hdr = stackalloc byte[WalHeader.Size];
            WalHeader.Write(hdr, partitionId, firstCommitSeq);
            fs.Write(hdr);
            fs.Flush(flushToDisk: true);
            return new WalWriter(fs, partitionId, WalHeader.Size, firstCommitSeq, faultInjector);
        }
        catch
        {
            fs.Dispose();
            throw;
        }
    }

    /// <summary>Open existing WAL for append after recovery (writeOffset = end of valid prefix).</summary>
    public static WalWriter OpenForAppend(string path, ulong partitionId, ulong writeOffset, ulong nextCommitSeq)
        => OpenForAppendCore(path, partitionId, writeOffset, nextCommitSeq, faultInjector: null);

    internal static WalWriter OpenForAppendForTesting(string path, ulong partitionId, ulong writeOffset,
        ulong nextCommitSeq, Action<StorageFaultPoint> faultInjector)
        => OpenForAppendCore(path, partitionId, writeOffset, nextCommitSeq, faultInjector);

    private static WalWriter OpenForAppendCore(string path, ulong partitionId, ulong writeOffset,
        ulong nextCommitSeq, Action<StorageFaultPoint>? faultInjector)
    {
        var fs = new FileStream(path, FileMode.Open, FileAccess.ReadWrite, FileShare.Read,
            bufferSize: 64 * 1024, FileOptions.None);
        try
        {
            if (writeOffset < WalHeader.Size)
                throw new ArgumentException("writeOffset < header");
            fs.Seek((long)writeOffset, SeekOrigin.Begin);
            return new WalWriter(fs, partitionId, writeOffset, nextCommitSeq, faultInjector);
        }
        catch
        {
            fs.Dispose();
            throw;
        }
    }

    /// <summary>
    /// Append a record. Returns the assigned CommitSeq.
    /// Kind must be known; payload is opaque bytes (already encoded).
    /// </summary>
    public ulong Append(byte kind, ReadOnlySpan<byte> payload, byte flags = 0)
    {
        if (payload.Length > WalConstants.MaxPayloadLen)
            throw new ArgumentException("payload too large");
        if (kind == 0)
            throw new ArgumentException("record kind zero is reserved", nameof(kind));

        lock (_mutex)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            if (!_accepting)
                throw new InvalidOperationException("WAL not accepting appends");

            if (_nextCommitSeq == ulong.MaxValue)
                throw new OverflowException("WAL CommitSeq exhausted.");

            ulong commitSeq = _nextCommitSeq;
            uint len = (uint)payload.Length;
            int recordSize = WalEnvelope.AlignedRecordSize(len);

            // Build: Envelope | Payload | RecordCrc | Padding
            var record = new byte[recordSize];
            WalEnvelope.Write(record.AsSpan(0, WalEnvelope.Size), commitSeq, len, kind, flags);
            payload.CopyTo(record.AsSpan(WalEnvelope.Size, payload.Length));

            // CRC32C(Envelope[0..16) ‖ Payload), incrementally and without a second payload-sized copy.
            uint recordCrc = StorageChecksums.Crc32C(record.AsSpan(0, 16), payload);
            int crcOff = WalEnvelope.Size + payload.Length;
            BinaryPrimitives.WriteUInt32LittleEndian(record.AsSpan(crcOff, 4), recordCrc);
            // padding already zero

            try
            {
                if (_faultInjector is not null)
                {
                    int partialLength = Math.Max(1, record.Length / 2);
                    _fs.Write(record.AsSpan(0, partialLength));
                    _faultInjector(StorageFaultPoint.AfterWalPartialWrite);
                    _fs.Write(record.AsSpan(partialLength));
                }
                else
                {
                    _fs.Write(record);
                }
            }
            catch
            {
                // A partial write makes the active append position uncertain.
                // Require recovery/reopen before accepting another append.
                _accepting = false;
                throw;
            }

            _nextCommitSeq = commitSeq + 1;

            // Publish after the complete record has been handed to the file stream.
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
            try
            {
                _faultInjector?.Invoke(StorageFaultPoint.BeforeWalFsync);
                _fs.Flush(flushToDisk: true);
                return target;
            }
            catch
            {
                _accepting = false;
                throw;
            }
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
