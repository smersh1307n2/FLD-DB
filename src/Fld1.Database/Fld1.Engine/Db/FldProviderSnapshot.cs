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

public sealed record FldProviderSnapshotEntryMetadata(
    byte[] RecordId,
    ushort FieldId,
    uint ValueLength,
    ulong HlcWall,
    uint HlcLogical,
    byte[] AuthorNodeId,
    ushort KeyEpoch,
    bool IsTombstone,
    byte[] ChangeId,
    uint OriginAuthorEpoch,
    ulong OriginSequence,
    bool IsRecordTombstone);

/// <summary>A bounded metadata-only page from an immutable provider snapshot.</summary>
public sealed record FldProviderSnapshotMetadataPage(
    int NextIndex,
    bool HasMore,
    IReadOnlyList<FldProviderSnapshotEntryMetadata> Entries);

/// <summary>One bounded value slice from an immutable provider snapshot.</summary>
public sealed record FldProviderSnapshotValueChunk(
    uint ValueLength,
    uint ValueOffset,
    byte[] ValueChunk)
{
    public bool IsComplete => (ulong)ValueOffset + (uint)ValueChunk.Length == ValueLength;
}

/// <summary>
/// Immutable, ordered FLD current-state view for BIN-SYNC provider snapshots.
/// The snapshot retains immutable Run buffers referenced by a durable generation
/// manifest plus a compact snapshot Overlay Run. Only the RecordId index is
/// materialized eagerly; field payloads are resolved and copied for requested pages.
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
    private int _lastPageLimit;
    private bool _lastPageIncludesValues;
    private object? _lastPage;
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

    /// <summary>Durable identity of this immutable snapshot generation.</summary>
    public Guid Generation { get; }

    /// <summary>FLD local Outbox high watermark captured with the state generation.</summary>
    public ulong SourceFeedWatermark { get; }

    /// <summary>Number of distinct RecordIds in the compact snapshot index.</summary>
    public int RecordCount
    {
        get { lock (_gate) { EnsureOpen(); return _recordIds!.Length; } }
    }

    /// <summary>
    /// Reads a bounded page at an absolute entry offset. Sequential reads are O(page);
    /// backward/forward seeks reset or advance the immutable cursor. The immediately
    /// preceding page is cached so a retry returns byte-identical entries.
    /// </summary>
    public FldProviderSnapshotPage ReadPage(int afterIndex, int limit = 1024)
    {
        if (afterIndex < 0) throw new ArgumentOutOfRangeException(nameof(afterIndex));
        if (limit <= 0 || limit > 4096) throw new ArgumentOutOfRangeException(nameof(limit));
        lock (_gate)
        {
            EnsureOpen();
            if (afterIndex == _lastPageStart && _lastPageLimit == limit &&
                _lastPageIncludesValues && _lastPage is FldProviderSnapshotPage cached)
                return cached;
            if (afterIndex < _nextIndex)
                ResetCursor();

            while (_nextIndex < afterIndex)
            {
                if (!TryReadNextEntry(includeValues: true, out _, out _, out _))
                    throw new ArgumentOutOfRangeException(nameof(afterIndex));
            }

            int start = _nextIndex;
            var entries = new List<FldProviderSnapshotEntry>(limit);
            while (entries.Count < limit &&
                   TryReadNextEntry(includeValues: true, out FldProviderSnapshotEntry? entry, out _, out _))
                entries.Add(entry!);

            bool hasMore = (_currentFieldIds is not null && _fieldIndex < _currentFieldIds.Length)
                || _recordIndex < _recordIds!.Length;
            var page = new FldProviderSnapshotPage(_nextIndex, hasMore, entries);
            _lastPageStart = start;
            _lastPageLimit = limit;
            _lastPageIncludesValues = true;
            _lastPage = page;
            return page;
        }
    }

    /// <summary>
    /// Reads a bounded page of snapshot metadata without copying field values.
    /// Values are retrieved separately with ReadValueChunk so a page cannot exceed
    /// the IPC frame limit merely because it contains several large fields.
    /// </summary>
    public FldProviderSnapshotMetadataPage ReadMetadataPage(int afterIndex, int limit = 256)
    {
        if (afterIndex < 0) throw new ArgumentOutOfRangeException(nameof(afterIndex));
        if (limit <= 0 || limit > 4096) throw new ArgumentOutOfRangeException(nameof(limit));
        lock (_gate)
        {
            EnsureOpen();
            if (afterIndex == _lastPageStart && _lastPageLimit == limit &&
                !_lastPageIncludesValues && _lastPage is FldProviderSnapshotMetadataPage cached)
                return cached;
            if (afterIndex < _nextIndex)
                ResetCursor();

            while (_nextIndex < afterIndex)
            {
                if (!TryReadNextEntry(includeValues: false, out _, out _, out _))
                    throw new ArgumentOutOfRangeException(nameof(afterIndex));
            }

            int start = _nextIndex;
            var entries = new List<FldProviderSnapshotEntryMetadata>(limit);
            while (entries.Count < limit &&
                   TryReadNextEntry(includeValues: false, out _, out FldProviderSnapshotEntryMetadata? metadata, out _))
                entries.Add(metadata!);

            bool hasMore = (_currentFieldIds is not null && _fieldIndex < _currentFieldIds.Length)
                || _recordIndex < _recordIds!.Length;
            var page = new FldProviderSnapshotMetadataPage(_nextIndex, hasMore, entries);
            _lastPageStart = start;
            _lastPageLimit = limit;
            _lastPageIncludesValues = false;
            _lastPage = page;
            return page;
        }
    }

    /// <summary>
    /// Copies only the requested value slice from the immutable snapshot generation.
    /// The RecordId/FieldId pair is resolved against the pinned Runs and overlay;
    /// no mutable current-database state is consulted.
    /// </summary>
    public FldProviderSnapshotValueChunk? ReadValueChunk(
        ReadOnlySpan<byte> recordId,
        ushort fieldId,
        uint valueOffset,
        uint maxChunkBytes)
    {
        if (recordId.Length != 16) throw new ArgumentException("RecordId must be 16 bytes.", nameof(recordId));
        if (maxChunkBytes == 0 || maxChunkBytes > int.MaxValue)
            throw new ArgumentOutOfRangeException(nameof(maxChunkBytes));
        lock (_gate)
        {
            EnsureOpen();
            if (!TryResolve(recordId.ToArray(), fieldId, includeValues: false, includeMetadata: false,
                    out _, out _, out ReadOnlyMemory<byte> value))
                return null;
            uint valueLength = checked((uint)value.Length);
            if (valueOffset > valueLength)
                throw new ArgumentOutOfRangeException(nameof(valueOffset));
            int offset = checked((int)valueOffset);
            int length = Math.Min(checked((int)maxChunkBytes), value.Length - offset);
            return new FldProviderSnapshotValueChunk(
                valueLength, valueOffset, value.Slice(offset, length).ToArray());
        }
    }

    private bool TryReadNextEntry(
        bool includeValues,
        out FldProviderSnapshotEntry? entry,
        out FldProviderSnapshotEntryMetadata? metadata,
        out ReadOnlyMemory<byte> resolvedValue)
    {
        EnsureOpen();
        while (true)
        {
            if (_currentFieldIds is null || _fieldIndex >= _currentFieldIds.Length)
            {
                if (!TryLoadNextRecord())
                {
                    entry = null;
                    metadata = null;
                    resolvedValue = ReadOnlyMemory<byte>.Empty;
                    return false;
                }
                if (_currentFieldIds!.Length == 0)
                    continue;
            }

            ushort fieldId = _currentFieldIds![_fieldIndex++];
            if (!TryResolve(_currentRecordId!, fieldId, includeValues, includeMetadata: !includeValues,
                    out entry, out metadata, out resolvedValue))
                continue;
            _nextIndex = checked(_nextIndex + 1);
            return true;
        }
    }

    private void ResetCursor()
    {
        _recordIndex = 0;
        _nextIndex = 0;
        _currentRecordId = null;
        _currentFieldIds = null;
        _fieldIndex = 0;
        _lastPageStart = -1;
        _lastPageLimit = 0;
        _lastPageIncludesValues = false;
        _lastPage = null;
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
            run.ScanFieldIds(recordId, fieldId => { fields.Add(fieldId); });

        _currentRecordId = recordId;
        _currentFieldIds = fields.Order().ToArray();
        _fieldIndex = 0;
        return true;
    }

    private bool TryResolve(
        byte[] recordId,
        ushort fieldId,
        bool includeValues,
        bool includeMetadata,
        out FldProviderSnapshotEntry? entry,
        out FldProviderSnapshotEntryMetadata? metadata,
        out ReadOnlyMemory<byte> resolvedValue)
    {
        EnsureOpen();
        bool have = false;
        ulong bestWall = 0;
        uint bestLogical = 0;
        ReadOnlyMemory<byte> bestAuthor = ReadOnlyMemory<byte>.Empty;
        ReadOnlyMemory<byte> bestValue = ReadOnlyMemory<byte>.Empty;
        ReadOnlyMemory<byte> bestChangeId = ReadOnlyMemory<byte>.Empty;
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
            if (!have || FieldResolver.IsNewer(
                    state.Wall, state.Logical, state.AuthorNodeId.Span,
                    bestWall, bestLogical, bestAuthor.Span))
            {
                have = true;
                bestWall = state.Wall;
                bestLogical = state.Logical;
                bestAuthor = state.AuthorNodeId;
                bestValue = state.Value;
                bestChangeId = state.ChangeId;
                bestOriginEpoch = state.OriginAuthorEpoch;
                bestOriginSequence = state.OriginSequence;
                bestKeyEpoch = state.KeyEpoch;
                bestTombstone = state.IsTombstone;
            }
        }

        if (!have)
        {
            entry = null;
            metadata = null;
            resolvedValue = ReadOnlyMemory<byte>.Empty;
            return false;
        }

        bool recordTombstone = false;
        if (fieldId == FldLifecycle.FieldId)
        {
            if (bestTombstone || bestValue.IsEmpty)
                throw new InvalidDataException("FLD lifecycle state must be a live canonical lifecycle value.");
            if (bestValue.Span.SequenceEqual(FldLifecycle.AliveValue))
            {
                entry = null;
                metadata = null;
                resolvedValue = ReadOnlyMemory<byte>.Empty;
                return false;
            }
            if (!bestValue.Span.SequenceEqual(FldLifecycle.DeadValue))
                throw new InvalidDataException("FLD lifecycle value is neither Alive nor Dead.");
            recordTombstone = true;
        }

        resolvedValue = recordTombstone || bestTombstone
            ? ReadOnlyMemory<byte>.Empty
            : bestValue;
        if (includeValues)
        {
            entry = new FldProviderSnapshotEntry(
                recordId.ToArray(),
                fieldId,
                resolvedValue.ToArray(),
                bestWall,
                bestLogical,
                bestAuthor.ToArray(),
                recordTombstone ? (ushort)0 : bestKeyEpoch,
                bestTombstone,
                bestChangeId.ToArray(),
                bestOriginEpoch,
                bestOriginSequence,
                recordTombstone);
            metadata = null;
        }
        else if (includeMetadata)
        {
            entry = null;
            metadata = new FldProviderSnapshotEntryMetadata(
                recordId.ToArray(),
                fieldId,
                checked((uint)resolvedValue.Length),
                bestWall,
                bestLogical,
                bestAuthor.ToArray(),
                recordTombstone ? (ushort)0 : bestKeyEpoch,
                bestTombstone,
                bestChangeId.ToArray(),
                bestOriginEpoch,
                bestOriginSequence,
                recordTombstone);
        }
        else
        {
            entry = null;
            metadata = null;
        }
        return true;
    }

    private void EnsureOpen() => ObjectDisposedException.ThrowIf(_disposed, this);
}

public sealed partial class FldDatabase
{
    private readonly Dictionary<Guid, FldProviderSnapshot> _providerSnapshots = new();

    /// <summary>
    /// Opens a stable, ordered snapshot over the current FLD generation. The durable
    /// manifest pins source Run IDs and stores the current overlay as a separate Run,
    /// allowing the same generation to be restored after sender process restart.
    /// </summary>
    public FldProviderSnapshot OpenProviderSnapshot()
        => OpenProviderSnapshotPersisted();

    internal FldProviderSnapshot OpenProviderSnapshotForIpc(Guid clientId, Guid hostGeneration)
    {
        if (clientId == Guid.Empty)
            throw new ArgumentException("IPC client id is required.", nameof(clientId));
        if (hostGeneration == Guid.Empty)
            throw new ArgumentException("IPC host generation is required.", nameof(hostGeneration));
        return OpenProviderSnapshotPersisted(clientId, hostGeneration);
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
            _providerSnapshots.Remove(snapshot.Generation);
    }

    private void DisposeProviderSnapshotsUnderLock()
    {
        foreach (FldProviderSnapshot snapshot in _providerSnapshots.Values.ToArray())
            snapshot.Dispose();
        _providerSnapshots.Clear();
    }
}
