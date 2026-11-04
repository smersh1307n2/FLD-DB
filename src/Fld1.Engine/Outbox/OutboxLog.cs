using System.Buffers.Binary;
using Fld1.Core.Hash;
using Fld1.Core.Uuid;

namespace Fld1.Engine.Outbox;

/// <summary>
/// Append-only outbox log + ack watermark.
/// Layout per record (LE):
///   OutboxSeq u64 | CommitSeq u64 | ChangeId16 | RecordId16 | FieldId u16 | OpKind u8 | pad1
///   Author16 | Wall u64 | Logical u32 | KeyEpoch u16 | pad2 | ValLen u32 | Value | Crc32C
/// CRC covers all bytes of the record except the CRC itself.
/// Watermark file: u64 ackedThroughSeq + crc32c
/// </summary>
public sealed class OutboxLog : IDisposable
{
    public static ReadOnlySpan<byte> Magic => "FLDO"u8;
    private const ushort Version = 1;
    private const int HeaderSize = 16; // magic4 + ver2 + pad2 + nextSeq u64

    private readonly string _path;
    private readonly string _ackPath;
    private readonly object _lock = new();
    private FileStream _fs;
    private ulong _nextSeq;
    private ulong _ackedThrough;
    private bool _disposed;

    public ulong NextSeq { get { lock (_lock) return _nextSeq; } }
    public ulong AckedThrough { get { lock (_lock) return _ackedThrough; } }
    public ulong Watermark => AckedThrough;

    private OutboxLog(string path, string ackPath, FileStream fs, ulong nextSeq, ulong acked)
    {
        _path = path;
        _ackPath = ackPath;
        _fs = fs;
        _nextSeq = nextSeq;
        _ackedThrough = acked;
    }

    public static OutboxLog Open(string dir, uint namespaceId)
    {
        Directory.CreateDirectory(dir);
        string path = Path.Combine(dir, $"ns_{namespaceId}.obx");
        string ackPath = Path.Combine(dir, $"ns_{namespaceId}.obx.ack");

        ulong acked = 0;
        if (File.Exists(ackPath))
        {
            var ab = File.ReadAllBytes(ackPath);
            if (ab.Length >= 12)
            {
                ulong w = BinaryPrimitives.ReadUInt64LittleEndian(ab.AsSpan(0, 8));
                uint crc = BinaryPrimitives.ReadUInt32LittleEndian(ab.AsSpan(8, 4));
                if (Hashes.Crc32C(ab.AsSpan(0, 8)) == crc)
                    acked = w;
            }
        }

        FileStream fs;
        ulong nextSeq = 1;
        if (!File.Exists(path))
        {
            fs = new FileStream(path, FileMode.CreateNew, FileAccess.ReadWrite, FileShare.Read,
                bufferSize: 64 * 1024, FileOptions.None);
            Span<byte> hdr = stackalloc byte[HeaderSize];
            Magic.CopyTo(hdr);
            BinaryPrimitives.WriteUInt16LittleEndian(hdr.Slice(4), Version);
            BinaryPrimitives.WriteUInt64LittleEndian(hdr.Slice(8), 1); // nextSeq placeholder; rebuilt on scan
            fs.Write(hdr);
            fs.Flush(flushToDisk: true);
            nextSeq = 1;
        }
        else
        {
            fs = new FileStream(path, FileMode.Open, FileAccess.ReadWrite, FileShare.Read,
                bufferSize: 64 * 1024, FileOptions.None);
            if (fs.Length < HeaderSize)
                throw new InvalidDataException("outbox header truncated");
            // Scan to end for nextSeq and integrity
            nextSeq = ScanNextSeq(fs, acked);
            fs.Seek(0, SeekOrigin.End);
        }

        return new OutboxLog(path, ackPath, fs, nextSeq, acked);
    }

    private static ulong ScanNextSeq(FileStream fs, ulong acked)
    {
        fs.Seek(HeaderSize, SeekOrigin.Begin);
        ulong maxSeq = 0;
        var buf = new byte[64 * 1024];
        long pos = HeaderSize;
        long len = fs.Length;
        Span<byte> fixedPart = stackalloc byte[8 + 8 + 16 + 16 + 2 + 1 + 1 + 16 + 8 + 4 + 2 + 2 + 4];
        while (pos + 8 + 8 + 16 + 16 + 2 + 1 + 1 + 16 + 8 + 4 + 2 + 2 + 4 + 4 <= len)
        {
            fs.Seek(pos, SeekOrigin.Begin);
            // read fixed prefix until ValLen (fixedPart rented above loop)
            int need = fixedPart.Length;
            if (pos + need + 4 > len) break;
            fs.ReadExactly(fixedPart);
            ulong seq = BinaryPrimitives.ReadUInt64LittleEndian(fixedPart);
            uint valLen = BinaryPrimitives.ReadUInt32LittleEndian(fixedPart.Slice(need - 4));
            if (valLen > 4 * 1024 * 1024) break;
            long recSize = need + valLen + 4;
            if (pos + recSize > len) break; // torn
            // verify crc
            fs.Seek(pos, SeekOrigin.Begin);
            var rec = new byte[recSize];
            fs.ReadExactly(rec);
            uint stored = BinaryPrimitives.ReadUInt32LittleEndian(rec.AsSpan(rec.Length - 4));
            uint computed = Hashes.Crc32C(rec.AsSpan(0, rec.Length - 4));
            if (stored != computed) break;
            if (seq > maxSeq) maxSeq = seq;
            pos += recSize;
        }
        // truncate torn
        if (pos < len)
        {
            fs.SetLength(pos);
            fs.Flush(flushToDisk: true);
        }
        return maxSeq + 1;
    }

    public ulong Append(OutboxEntry e)
    {
        lock (_lock)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            ulong seq = _nextSeq++;
            int valLen = e.Value.Length;
            int fixedLen = 8 + 8 + 16 + 16 + 2 + 1 + 1 + 16 + 8 + 4 + 2 + 2 + 4;
            int recSize = fixedLen + valLen + 4;
            var rec = new byte[recSize];
            int o = 0;
            BinaryPrimitives.WriteUInt64LittleEndian(rec.AsSpan(o), seq); o += 8;
            BinaryPrimitives.WriteUInt64LittleEndian(rec.AsSpan(o), e.CommitSeq); o += 8;
            e.ChangeId.CopyTo(rec.AsSpan(o, 16)); o += 16;
            e.RecordId.CopyTo(rec.AsSpan(o, 16)); o += 16;
            BinaryPrimitives.WriteUInt16LittleEndian(rec.AsSpan(o), e.FieldId); o += 2;
            rec[o++] = e.OpKind;
            rec[o++] = 0;
            e.AuthorNodeId.CopyTo(rec.AsSpan(o, 16)); o += 16;
            BinaryPrimitives.WriteUInt64LittleEndian(rec.AsSpan(o), e.Wall); o += 8;
            BinaryPrimitives.WriteUInt32LittleEndian(rec.AsSpan(o), e.Logical); o += 4;
            BinaryPrimitives.WriteUInt16LittleEndian(rec.AsSpan(o), e.KeyEpoch); o += 2;
            BinaryPrimitives.WriteUInt16LittleEndian(rec.AsSpan(o), 0); o += 2;
            BinaryPrimitives.WriteUInt32LittleEndian(rec.AsSpan(o), (uint)valLen); o += 4;
            e.Value.CopyTo(rec.AsSpan(o, valLen)); o += valLen;
            uint crc = Hashes.Crc32C(rec.AsSpan(0, o));
            BinaryPrimitives.WriteUInt32LittleEndian(rec.AsSpan(o), crc);
            _fs.Write(rec);
            return seq;
        }
    }

    public void Flush()
    {
        lock (_lock)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            _fs.Flush(flushToDisk: true);
        }
    }

    /// <summary>Read entries with OutboxSeq &gt; afterSeq, up to limit (skips ≤ AckedThrough).</summary>
    public List<OutboxEntry> ReadBatch(ulong afterSeq, int limit)
    {
        if (limit <= 0) return [];
        lock (_lock)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            return ReadBatchUnlocked(afterSeq, limit);
        }
    }

    private List<OutboxEntry> ReadBatchUnlocked(ulong afterSeq, int limit)
    {
        ulong minSeq = Math.Max(afterSeq, _ackedThrough);
        var result = new List<OutboxEntry>(Math.Min(limit, 64));
        long pos = HeaderSize;
        long len = _fs.Length;
        int fixedLen = 8 + 8 + 16 + 16 + 2 + 1 + 1 + 16 + 8 + 4 + 2 + 2 + 4;
        while (pos + fixedLen + 4 <= len && result.Count < limit)
        {
            _fs.Seek(pos, SeekOrigin.Begin);
            var fixedPart = new byte[fixedLen];
            _fs.ReadExactly(fixedPart);
            ulong seq = BinaryPrimitives.ReadUInt64LittleEndian(fixedPart);
            uint valLen = BinaryPrimitives.ReadUInt32LittleEndian(fixedPart.AsSpan(fixedLen - 4));
            long recSize = fixedLen + valLen + 4;
            if (pos + recSize > len) break;
            var rec = new byte[recSize];
            _fs.Seek(pos, SeekOrigin.Begin);
            _fs.ReadExactly(rec);
            uint stored = BinaryPrimitives.ReadUInt32LittleEndian(rec.AsSpan(rec.Length - 4));
            if (Hashes.Crc32C(rec.AsSpan(0, rec.Length - 4)) != stored) break;

            if (seq > minSeq)
            {
                int o = 0;
                o += 8;
                ulong commitSeq = BinaryPrimitives.ReadUInt64LittleEndian(rec.AsSpan(o)); o += 8;
                var changeId = rec.AsSpan(o, 16).ToArray(); o += 16;
                var recordId = rec.AsSpan(o, 16).ToArray(); o += 16;
                ushort fieldId = BinaryPrimitives.ReadUInt16LittleEndian(rec.AsSpan(o)); o += 2;
                byte op = rec[o++]; o++;
                var author = rec.AsSpan(o, 16).ToArray(); o += 16;
                ulong wall = BinaryPrimitives.ReadUInt64LittleEndian(rec.AsSpan(o)); o += 8;
                uint logical = BinaryPrimitives.ReadUInt32LittleEndian(rec.AsSpan(o)); o += 4;
                ushort keyEpoch = BinaryPrimitives.ReadUInt16LittleEndian(rec.AsSpan(o)); o += 2;
                o += 2;
                o += 4;
                var value = valLen == 0 ? Array.Empty<byte>() : rec.AsSpan(o, (int)valLen).ToArray();
                result.Add(new OutboxEntry
                {
                    OutboxSeq = seq,
                    CommitSeq = commitSeq,
                    ChangeId = changeId,
                    RecordId = recordId,
                    FieldId = fieldId,
                    OpKind = op,
                    AuthorNodeId = author,
                    Wall = wall,
                    Logical = logical,
                    KeyEpoch = keyEpoch,
                    Value = value
                });
            }
            pos += recSize;
        }
        _fs.Seek(0, SeekOrigin.End);
        return result;
    }

    public void Acknowledge(ulong throughSeq)
    {
        lock (_lock)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            if (throughSeq <= _ackedThrough) return;
            _ackedThrough = throughSeq;
            Span<byte> ab = stackalloc byte[12];
            BinaryPrimitives.WriteUInt64LittleEndian(ab, throughSeq);
            uint crc = Hashes.Crc32C(ab.Slice(0, 8));
            BinaryPrimitives.WriteUInt32LittleEndian(ab.Slice(8), crc);
            File.WriteAllBytes(_ackPath, ab.ToArray());
        }
    }


    /// <summary>
    /// Rewrite log keeping only entries with OutboxSeq &gt; AckedThrough.
    /// Returns number of retained entries. Call after Acknowledge when file is large.
    /// </summary>
    public int Compact()
    {
        lock (_lock)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            var keep = ReadBatchUnlocked(afterSeq: _ackedThrough, limit: int.MaxValue);
            string tmp = _path + ".tmp";
            using (var nfs = new FileStream(tmp, FileMode.Create, FileAccess.ReadWrite, FileShare.None,
                       bufferSize: 64 * 1024, FileOptions.None))
            {
                Span<byte> hdr = stackalloc byte[HeaderSize];
                hdr.Clear();
                Magic.CopyTo(hdr);
                BinaryPrimitives.WriteUInt16LittleEndian(hdr.Slice(4), Version);
                BinaryPrimitives.WriteUInt64LittleEndian(hdr.Slice(8), _nextSeq);
                nfs.Write(hdr);
                int fixedLen = 8 + 8 + 16 + 16 + 2 + 1 + 1 + 16 + 8 + 4 + 2 + 2 + 4;
                foreach (var e in keep)
                {
                    int valLen = e.Value.Length;
                    int recSize = fixedLen + valLen + 4;
                    var rec = new byte[recSize];
                    int o = 0;
                    BinaryPrimitives.WriteUInt64LittleEndian(rec.AsSpan(o), e.OutboxSeq); o += 8;
                    BinaryPrimitives.WriteUInt64LittleEndian(rec.AsSpan(o), e.CommitSeq); o += 8;
                    e.ChangeId.CopyTo(rec.AsSpan(o, 16)); o += 16;
                    e.RecordId.CopyTo(rec.AsSpan(o, 16)); o += 16;
                    BinaryPrimitives.WriteUInt16LittleEndian(rec.AsSpan(o), e.FieldId); o += 2;
                    rec[o++] = e.OpKind;
                    rec[o++] = 0;
                    e.AuthorNodeId.CopyTo(rec.AsSpan(o, 16)); o += 16;
                    BinaryPrimitives.WriteUInt64LittleEndian(rec.AsSpan(o), e.Wall); o += 8;
                    BinaryPrimitives.WriteUInt32LittleEndian(rec.AsSpan(o), e.Logical); o += 4;
                    BinaryPrimitives.WriteUInt16LittleEndian(rec.AsSpan(o), e.KeyEpoch); o += 2;
                    BinaryPrimitives.WriteUInt16LittleEndian(rec.AsSpan(o), 0); o += 2;
                    BinaryPrimitives.WriteUInt32LittleEndian(rec.AsSpan(o), (uint)valLen); o += 4;
                    e.Value.CopyTo(rec.AsSpan(o, valLen)); o += valLen;
                    uint crc = Hashes.Crc32C(rec.AsSpan(0, o));
                    BinaryPrimitives.WriteUInt32LittleEndian(rec.AsSpan(o), crc);
                    nfs.Write(rec);
                }
                nfs.Flush(flushToDisk: true);
            }
            _fs.Dispose();
            File.Move(tmp, _path, overwrite: true);
            _fs = new FileStream(_path, FileMode.Open, FileAccess.ReadWrite, FileShare.Read,
                bufferSize: 64 * 1024, FileOptions.None);
            _fs.Seek(0, SeekOrigin.End);
            return keep.Count;
        }
    }

    public void Dispose()
    {
        lock (_lock)
        {
            if (_disposed) return;
            _disposed = true;
            _fs.Dispose();
        }
    }
}
