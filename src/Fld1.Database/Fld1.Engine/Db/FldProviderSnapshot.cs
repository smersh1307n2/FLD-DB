using System.Buffers.Binary;
using Fld1.Engine.Storage;
using Fld1.Run.Reader;

namespace Fld1.Engine.Db;

/// <summary>One current FLD field winner captured by an immutable provider snapshot.</summary>
public sealed record FldProviderSnapshotEntry(
    byte[] RecordId,
    ushort FieldId,
    byte[] Value,
    ulong HlcWall,
    uint HlcLogical,
    byte[] AuthorNodeId,
    ushort KeyEpoch,
    bool IsTombstone,
    byte[] ChangeId,
    uint OriginAuthorEpoch,
    ulong OriginSequence,
    bool IsRecordTombstone);

/// <summary>A bounded page from one generation-pinned FLD provider snapshot.</summary>
public sealed record FldProviderSnapshotPage(
    int NextIndex,
    bool HasMore,
    IReadOnlyList<FldProviderSnapshotEntry> Entries);

/// <summary>
/// Immutable, ordered FLD current-state view for BIN-SYNC provider snapshots.
/// The snapshot retains immutable Run buffers and a point-in-time overlay copy;
/// only the compact RecordId index is materialized eagerly. Field payloads are
/// resolved and copied only for the requested page.
/// </summary>
public sealed class FldProviderSnapshot : IDisposable
{
    private readonly object _gate = new();
    private readonly Action<FldProviderSnapshot>? _onDispose;
    private RunReader[]? _runs;
    private Dictionary<MemFieldKey, MemFieldValue>? _overlayByKey;
    private Dictionary<(ulong Low, ulong High), ushort[]>? _overlayFieldsByRecord;
    private byte[][]? _recordIds;
    private int _recordIndex;
    private int _nextIndex;
    private byte[]? _currentRecordId;
    private ushort[]? _currentFieldIds;
    private int _fieldIndex;
    private int _lastPageStart = -1;
    private FldProviderSnapshotPage? _lastPage;
    private bool _disposed;

    internal FldProviderSnapshot(
        Guid generation,
        ulong sourceFeedWatermark,
        RunReader[] runs,
        KeyValuePair<MemFieldKey, MemFieldValue>[] overlay,
        byte[][] recordIds,
        Action<FldProviderSnapshot> onDispose)
    {
        if (generation == Guid.Empty) throw new ArgumentException("Snapshot generation must be non-empty.", nameof(generation));
        Generation = generation;
        SourceFeedWatermark = sourceFeedWatermark;
        _runs = runs;
        _overlayByKey = new Dictionary<MemFieldKey, MemFieldValue>(overlay.Length);
        var overlayFields = new Dictionary<(ulong Low, ulong High), List<ushort>>();
        foreach (var pair in overlay)
        {
            _overlayByKey.Add(pair.Key, pair.Value);
            var recordKey = (pair.Key.RecLo, pair.Key.RecHi);
            if (!overlayFields.TryGetValue(recordKey, out List<ushort>? fields))
                overlayFields.Add(recordKey, fields = new List<ushort>(4));
            fields.Add(pair.Key.FieldId);
        }
        _overlayFieldsByRecord = new Dictionary<(ulong Low, ulong High), ushort[]>(overlayFields.Count);
        foreach (var pair in overlayFields)
        {
            pair.Value.Sort();
            _overlayFieldsByRecord.Add(pair.Key, pair.Value.ToArray());
        }
        _recordIds = recordIds;
        _onDispose = onDispose;
    }

    /// <summary>Unique identity of this in-process immutable snapshot generation.</summary>
    public Guid Generation { get; }

    /// <summary>FLD local Outbox high watermark captured with the state generation.</summary>
    public ulong SourceFeedWatermark { get; }

    /// <summary>Number of distinct RecordIds in the compact snapshot index.</summary>
    public int RecordCount
    {
        get { lock (_gate) { EnsureOpen(); return _recordIds!.Length; } }
    }

    /// <summary>
    /// Reads a bounded page. Pages are consumed in increasing NextIndex order;
    /// retrying the immediately preceding page returns the same immutable result.
    /// </summary>
    public FldProviderSnapshotPage ReadPage(int afterIndex, int limit = 1024)
    {
        if (limit <= 0 || limit > 4096) throw new ArgumentOutOfRangeException(nameof(limit));
        lock (_gate)
        {
            EnsureOpen();
            if (afterIndex == _lastPageStart) return _lastPage!;
            if (afterIndex != _nextIndex)
                throw new InvalidOperationException("Provider snapshot pages must be read sequentially from the returned NextIndex.");

            var entries = new List<FldProviderSnapshotEntry>(limit);
            while (entries.Count < limit)
            {
                if (_currentFieldIds is null || _fieldIndex >= _currentFieldIds.Length)
                {
                    if (!TryLoadNextRecord())
                        break;
                    if (_currentFieldIds!.Length == 0)
                        continue;
                }

                ushort fieldId = _currentFieldIds![_fieldIndex++];
                if (TryResolve(_currentRecordId!, fieldId, out FldProviderSnapshotEntry? entry))
                    entries.Add(entry);
            }

            int start = _nextIndex;
            _nextIndex = checked(_nextIndex + entries.Count);
            bool hasMore = _currentFieldIds is not null && _fieldIndex < _currentFieldIds.Length
                || _recordIndex < _recordIds!.Length;
            var page = new FldProviderSnapshotPage(_nextIndex, hasMore, entries);
            _lastPageStart = start;
            _lastPage = page;
            return page;
        }
    }

    public void Dispose()
    {
        Action<FldProviderSnapshot>? callback;
        lock (_gate)
        {
            if (_disposed) return;
            _disposed = true;
            _runs = null;
            _overlayByKey = null;
            _overlayFieldsByRecord = null;
            _recordIds = null;
            _currentRecordId = null;
            _currentFieldIds = null;
            _lastPage = null;
            callback = _onDispose;
        }
        callback?.Invoke(this);
    }

    private bool TryLoadNextRecord()
    {
        EnsureOpen();
        if (_recordIndex >= _recordIds!.Length)
        {
            _currentRecordId = null;
            _currentFieldIds = null;
            return false;
        }

        byte[] recordId = _recordIds[_recordIndex++];
        var fields = new HashSet<ushort>();
        ulong low = BinaryPrimitives.ReadUInt64LittleEndian(recordId);
        ulong high = BinaryPrimitives.ReadUInt64LittleEndian(recordId.AsSpan(8));
        if (_overlayFieldsByRecord!.TryGetValue((low, high), out ushort[]? overlayFields))
        {
            foreach (ushort fieldId in overlayFields)
                fields.Add(fieldId);
        }
        foreach (RunReader run in _runs!)
            run.ScanFieldIds(recordId, fields.Add);

        _currentRecordId = recordId;
        _currentFieldIds = fields.Order().ToArray();
        _fieldIndex = 0;
        return true;
    }

    private bool TryResolve(byte[] recordId, ushort fieldId, out FldProviderSnapshotEntry? entry)
    {
        EnsureOpen();
        bool have = false;
        ulong bestWall = 0;
        uint bestLogical = 0;
        byte[]? bestAuthor = null;
        byte[]? bestValue = null;
        byte[]? bestChangeId = null;
        uint bestOriginEpoch = 0;
        ulong bestOriginSequence = 0;
        ushort bestKeyEpoch = 0;
        bool bestTombstone = false;

        var key = new MemFieldKey(recordId, fieldId);
        if (_overlayByKey!.TryGetValue(key, out MemFieldValue? mem))
        {
            have = true;
            bestWall = mem.Wall;
            bestLogical = mem.Logical;
            bestAuthor = mem.AuthorNodeId;
            bestValue = mem.Value;
            bestChangeId = mem.ChangeId;
            bestOriginEpoch = mem.OriginAuthorEpoch;
            bestOriginSequence = mem.OriginSequence;
            bestKeyEpoch = mem.KeyEpoch;
            bestTombstone = mem.IsTombstone;
        }

        foreach (RunReader run in _runs!)
        {
            if (!run.TryGetField(recordId, fieldId, out RunFieldState state) || !state.Found)
                continue;
            byte[] author = state.AuthorNodeId.ToArray();
            if (!have || FieldResolver.IsNewer(
                    state.Wall, state.Logical, author,
                    bestWall, bestLogical, bestAuthor ?? ReadOnlySpan<byte>.Empty))
            {
                have = true;
                bestWall = state.Wall;
                bestLogical = state.Logical;
                bestAuthor = author;
                bestValue = state.IsTombstone ? Array.Empty<byte>() : state.Value.ToArray();
                bestChangeId = state.ChangeId.ToArray();
                bestOriginEpoch = state.OriginAuthorEpoch;
                bestOriginSequence = state.OriginSequence;
                bestKeyEpoch = state.KeyEpoch;
                bestTombstone = state.IsTombstone;
            }
        }

        if (!have)
        {
            entry = null;
            return false;
        }

        bool recordTombstone = fieldId == Fld1.Engine.Sync.FldLifecycle.FieldId &&
            !bestTombstone &&
            bestValue is not null &&
            bestValue.AsSpan().SequenceEqual(Fld1.Engine.Sync.FldLifecycle.DeadValue);
        entry = new FldProviderSnapshotEntry(
            recordId.ToArray(),
            fieldId,
            recordTombstone || bestTombstone ? Array.Empty<byte>() : bestValue?.ToArray() ?? Array.Empty<byte>(),
            bestWall,
            bestLogical,
            bestAuthor?.ToArray() ?? Array.Empty<byte>(),
            recordTombstone ? (ushort)0 : bestKeyEpoch,
            bestTombstone,
            bestChangeId?.ToArray() ?? Array.Empty<byte>(),
            bestOriginEpoch,
            bestOriginSequence,
            recordTombstone);
        return true;
    }

    private void EnsureOpen() => ObjectDisposedException.ThrowIf(_disposed, this);
}

public sealed partial class FldDatabase
{
    private readonly HashSet<FldProviderSnapshot> _providerSnapshots = new();

    /// <summary>
    /// Opens a stable, ordered snapshot over the current FLD generation. Immutable
    /// Run buffers are retained by the returned handle; overlay values are captured
    /// by reference because MemFieldValue instances are immutable after publication.
    /// </summary>
    public FldProviderSnapshot OpenProviderSnapshot()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        lock (_commitLock)
        {
            EnsureHealthy();
            var runs = new RunReader[_runs.Count];
            var recordIds = new List<byte[]>();
            var seen = new HashSet<(ulong Low, ulong High)>();
            Span<byte> recordId = stackalloc byte[16];

            for (int i = 0; i < _runs.Count; i++)
            {
                byte[] bytes = EnsureRunCached(i);
                runs[i] = new RunReader(bytes);
                for (uint index = 0; index < runs[i].NumClusters; index++)
                {
                    if (!runs[i].TryReadRecordIdAt(index, recordId))
                        throw new InvalidDataException("Run cluster index changed while opening a provider snapshot.");
                    AddRecordId(recordId, seen, recordIds);
                }
            }

            KeyValuePair<MemFieldKey, MemFieldValue>[] overlay = _overlay.Snapshot().ToArray();
            foreach (var pair in overlay)
            {
                pair.Key.WriteRecordId(recordId);
                AddRecordId(recordId, seen, recordIds);
            }

            recordIds.Sort(static (left, right) => left.AsSpan().SequenceCompareTo(right));
            var snapshot = new FldProviderSnapshot(
                Guid.NewGuid(),
                _outbox.HighWatermark,
                runs,
                overlay,
                recordIds.ToArray(),
                OnProviderSnapshotDisposed);
            _providerSnapshots.Add(snapshot);
            return snapshot;
        }
    }

    private static void AddRecordId(
        ReadOnlySpan<byte> recordId,
        HashSet<(ulong Low, ulong High)> seen,
        List<byte[]> recordIds)
    {
        ulong low = BinaryPrimitives.ReadUInt64LittleEndian(recordId);
        ulong high = BinaryPrimitives.ReadUInt64LittleEndian(recordId.Slice(8));
        if (seen.Add((low, high)))
            recordIds.Add(recordId.ToArray());
    }

    private void OnProviderSnapshotDisposed(FldProviderSnapshot snapshot)
    {
        lock (_commitLock)
            _providerSnapshots.Remove(snapshot);
    }

    private void DisposeProviderSnapshotsUnderLock()
    {
        foreach (FldProviderSnapshot snapshot in _providerSnapshots.ToArray())
            snapshot.Dispose();
        _providerSnapshots.Clear();
    }
}
