using System.Buffers.Binary;
using System.Security.Cryptography;
using Fld1.Run.Builder;
using Fld1.Run.Reader;
using Fld1.Manifest.Format;

namespace Fld1.Engine.Db;

/// <summary>Durable progress for one staged provider-generation replacement.</summary>
public sealed record FldFullReplaceProgress(
    Guid Generation,
    ulong SourceFeedWatermark,
    int ExpectedEntryCount,
    int NextIndex,
    byte[] ExpectedDigest,
    bool IsCommitted);

public sealed partial class FldDatabase
{
    private const uint FullReplaceStageMagic = 0x31524646; // "FFR1"
    private const ushort FullReplaceStageVersion = 1;
    private const int FullReplaceStagePageLimit = 4096;
    private const int FullReplaceStagePageMaxBytes = 16 * 1024 * 1024;

    private sealed record FullReplaceStageRun(
        int StartIndex, int EntryCount, ulong RunId, byte[] MinRecordId, byte[] MaxRecordId);

    private sealed record FullReplaceStageState(
        Guid Generation,
        ulong BaseManifestGeneration,
        ulong BaseLastSealedCommitSeq,
        ulong SourceFeedWatermark,
        int ExpectedEntryCount,
        int NextIndex,
        byte[] ExpectedDigest,
        byte[] LastSortKey,
        ulong[] BaseRunIds,
        FullReplaceStageRun[] Runs,
        bool IsCommitted,
        bool IsAborting,
        int AbortRunIndex);

    /// <summary>
    /// Begin or idempotently resume a durable FLD provider replacement. The expected
    /// digest is opaque to FLD; the protocol adapter verifies it before commit.
    /// </summary>
    public void BeginProviderReplacement(
        Guid generation,
        ulong sourceFeedWatermark,
        int expectedEntryCount,
        ReadOnlyMemory<byte> expectedDigest)
    {
        if (generation == Guid.Empty) throw new ArgumentException("Generation is required.", nameof(generation));
        if (expectedEntryCount < 0) throw new ArgumentOutOfRangeException(nameof(expectedEntryCount));
        if (expectedDigest.Length != 32) throw new ArgumentException("Expected digest must be SHA-256.", nameof(expectedDigest));
        ObjectDisposedException.ThrowIf(_disposed, this);
        lock (_commitLock)
        {
            EnsureHealthy();
            string path = FullReplaceStagePath(generation);
            if (File.Exists(path))
            {
                FullReplaceStageState existing = ReadFullReplaceStage(path);
                if (existing.IsAborting)
                    throw new InvalidOperationException("FULL_REPLACE generation is being aborted and cannot be resumed.");
                if (existing.Generation != generation ||
                    existing.SourceFeedWatermark != sourceFeedWatermark ||
                    existing.ExpectedEntryCount != expectedEntryCount ||
                    !existing.ExpectedDigest.AsSpan().SequenceEqual(expectedDigest.Span))
                    throw new InvalidDataException("FULL_REPLACE generation was reused with conflicting manifest data.");
                return;
            }

            ManifestDocument manifest = _manifestStore.Load();
            ulong[] baseRuns = manifest.Runs.Where(run => run.Ns == _namespaceId)
                .Select(run => run.RunId).OrderBy(id => id).ToArray();
            var state = new FullReplaceStageState(
                generation, manifest.Generation, _lastSealedCommitSeq,
                sourceFeedWatermark, expectedEntryCount, 0,
                expectedDigest.ToArray(), new byte[19], baseRuns,
                Array.Empty<FullReplaceStageRun>(), false, false, 0);
            WriteFullReplaceStage(path, state);
        }
    }

    /// <summary>Persist one contiguous, canonically ordered page as an immutable FLD Run.</summary>
    public void StageProviderReplacementPage(
        Guid generation,
        int startIndex,
        IReadOnlyList<FldProviderSnapshotEntry> entries)
    {
        ArgumentNullException.ThrowIfNull(entries);
        if (generation == Guid.Empty) throw new ArgumentException("Generation is required.", nameof(generation));
        if (startIndex < 0) throw new ArgumentOutOfRangeException(nameof(startIndex));
        if (entries.Count == 0 || entries.Count > FullReplaceStagePageLimit)
            throw new ArgumentOutOfRangeException(nameof(entries), "Replacement page must contain 1..4096 entries.");
        ObjectDisposedException.ThrowIf(_disposed, this);
        lock (_commitLock)
        {
            EnsureHealthy();
            string statePath = FullReplaceStagePath(generation);
            if (!File.Exists(statePath)) throw new InvalidOperationException("FULL_REPLACE generation has not been begun.");
            FullReplaceStageState state = ReadFullReplaceStage(statePath);
            if (state.IsAborting)
                throw new InvalidOperationException("Aborting FULL_REPLACE generation cannot accept more pages.");
            if (IsFullReplaceStageCommitted(state, _manifestStore.Load()))
                throw new InvalidOperationException("Committed FULL_REPLACE generation cannot accept more pages.");
            if (startIndex != state.NextIndex)
                throw new InvalidDataException("FULL_REPLACE page is not contiguous with the durable staging cursor.");
            if (entries.Count > state.ExpectedEntryCount - state.NextIndex)
                throw new InvalidDataException("FULL_REPLACE page exceeds the declared snapshot entry count.");

            var builder = new RunBuilder(_namespaceId);
            byte[] lastSortKey = state.LastSortKey.ToArray();
            int pageBytes = 0;
            ValidateReplacementEntry(entries[0]);
            byte[] firstRecord = entries[0].RecordId;
            byte[] lastRecord = entries[^1].RecordId;
            for (int i = 0; i < entries.Count; i++)
            {
                FldProviderSnapshotEntry entry = entries[i]
                    ?? throw new ArgumentException("Replacement page contains a null entry.", nameof(entries));
                ValidateReplacementEntry(entry);
                pageBytes = checked(pageBytes + entry.Value.Length + 128);
                if (pageBytes > FullReplaceStagePageMaxBytes)
                    throw new InvalidDataException("FULL_REPLACE staging page exceeds the bounded 16 MiB payload budget.");
                byte[] sortKey = FullReplaceSortKey(entry);
                if (state.NextIndex + i > 0 && lastSortKey.AsSpan().SequenceCompareTo(sortKey) >= 0)
                    throw new InvalidDataException("FULL_REPLACE entries must be strictly ordered and unique.");
                byte[] value = entry.IsRecordTombstone
                    ? FldLifecycle.DeadValue.ToArray()
                    : entry.IsTombstone ? Array.Empty<byte>() : entry.Value;
                builder.Add(new FieldWrite(
                    entry.RecordId, entry.FieldId, value,
                    entry.HlcWall, entry.HlcLogical, entry.AuthorNodeId,
                    entry.IsRecordTombstone ? (ushort)0 : entry.KeyEpoch,
                    isTombstone: !entry.IsRecordTombstone && entry.IsTombstone,
                    changeId: entry.ChangeId,
                    originAuthorEpoch: entry.OriginAuthorEpoch,
                    originSequence: entry.OriginSequence));
                lastSortKey = sortKey;
            }

            byte[] runBytes = builder.Build();
            var reader = new RunReader(runBytes);
            if (!reader.TryValidate(out var verify) || reader.NamespaceId != _namespaceId)
                throw new InvalidDataException($"FULL_REPLACE staging produced an invalid Run: {verify.Status} {verify.Detail}");
            ulong runId = AllocateFullReplaceRunId();
            string runPath = Path.Combine(_dbDir, "runs", $"{runId}.fld1");
            WriteRunAtomically(runPath, runBytes);

            var stagedRun = new FullReplaceStageRun(
                startIndex, entries.Count, runId, firstRecord.ToArray(), lastRecord.ToArray());
            FullReplaceStageState updated = state with
            {
                NextIndex = checked(state.NextIndex + entries.Count),
                LastSortKey = lastSortKey,
                Runs = state.Runs.Append(stagedRun).ToArray()
            };
            WriteFullReplaceStage(statePath, updated);
        }
    }

    /// <summary>Read a bounded page from the durable staged generation for digest verification.</summary>
    public FldProviderSnapshotPage ReadProviderReplacementPage(Guid generation, int afterIndex, int limit = 1024)
    {
        if (generation == Guid.Empty) throw new ArgumentException("Generation is required.", nameof(generation));
        if (afterIndex < 0) throw new ArgumentOutOfRangeException(nameof(afterIndex));
        if (limit <= 0 || limit > FullReplaceStagePageLimit) throw new ArgumentOutOfRangeException(nameof(limit));
        ObjectDisposedException.ThrowIf(_disposed, this);
        lock (_commitLock)
        {
            EnsureHealthy();
            string path = FullReplaceStagePath(generation);
            if (!File.Exists(path)) throw new KeyNotFoundException("FULL_REPLACE staging generation does not exist.");
            FullReplaceStageState state = ReadFullReplaceStage(path);
            if (state.IsAborting) throw new InvalidOperationException("FULL_REPLACE generation is being aborted.");
            if (afterIndex > state.NextIndex) throw new ArgumentOutOfRangeException(nameof(afterIndex));
            var result = new List<FldProviderSnapshotEntry>(Math.Min(limit, state.NextIndex - afterIndex));
            foreach (FullReplaceStageRun run in state.Runs)
            {
                int runEnd = checked(run.StartIndex + run.EntryCount);
                if (runEnd <= afterIndex || run.StartIndex >= state.NextIndex) continue;
                IReadOnlyList<FldProviderSnapshotEntry> page = ReadFullReplaceRun(run);
                int offset = Math.Max(0, afterIndex - run.StartIndex);
                int count = Math.Min(page.Count - offset, limit - result.Count);
                for (int i = 0; i < count; i++) result.Add(page[offset + i]);
                if (result.Count == limit) break;
            }
            int next = checked(afterIndex + result.Count);
            return new FldProviderSnapshotPage(next, next < state.NextIndex, result);
        }
    }

    public bool TryGetProviderReplacementProgress(Guid generation, out FldFullReplaceProgress? progress)
    {
        if (generation == Guid.Empty) throw new ArgumentException("Generation is required.", nameof(generation));
        ObjectDisposedException.ThrowIf(_disposed, this);
        lock (_commitLock)
        {
            EnsureHealthy();
            string path = FullReplaceStagePath(generation);
            if (!File.Exists(path)) { progress = null; return false; }
            FullReplaceStageState state = ReadFullReplaceStage(path);
            if (state.IsAborting) throw new InvalidOperationException("Aborting FULL_REPLACE generation cannot be committed.");
            ManifestDocument manifest = _manifestStore.Load();
            bool committed = IsFullReplaceStageCommitted(state, manifest);
            if (committed && !state.IsCommitted)
            {
                state = state with { IsCommitted = true };
                WriteFullReplaceStage(path, state);
            }
            progress = new FldFullReplaceProgress(
                generation, state.SourceFeedWatermark, state.ExpectedEntryCount,
                state.NextIndex, state.ExpectedDigest.ToArray(), committed);
            return true;
        }
    }

    /// <summary>
    /// Atomically replaces the active namespace Run set. The caller must verify the
    /// staged canonical provider digest before passing the matching digest here.
    /// </summary>
    public void CommitProviderReplacement(Guid generation, ReadOnlyMemory<byte> verifiedDigest)
    {
        if (generation == Guid.Empty) throw new ArgumentException("Generation is required.", nameof(generation));
        if (verifiedDigest.Length != 32) throw new ArgumentException("Verified digest must be SHA-256.", nameof(verifiedDigest));
        ObjectDisposedException.ThrowIf(_disposed, this);
        lock (_commitLock)
        {
            EnsureHealthy();
            string statePath = FullReplaceStagePath(generation);
            if (!File.Exists(statePath)) throw new InvalidOperationException("FULL_REPLACE generation has not been begun.");
            FullReplaceStageState state = ReadFullReplaceStage(statePath);
            ManifestDocument manifest = _manifestStore.Load();
            if (IsFullReplaceStageCommitted(state, manifest)) return;
            if (!state.ExpectedDigest.AsSpan().SequenceEqual(verifiedDigest.Span))
                throw new InvalidDataException("FULL_REPLACE commit proof does not match the staged manifest digest.");
            if (state.NextIndex != state.ExpectedEntryCount)
                throw new InvalidOperationException("FULL_REPLACE snapshot is incomplete.");
            if (_dirty.Count != 0 || _overlay.Count != 0 ||
                _pendingFlushedCommitSeq != _lastSealedCommitSeq)
                throw new InvalidOperationException("FLD local writes must be committed and checkpointed before FULL_REPLACE generation switch.");
            if (_databaseHasUnpublishedLocalChanges())
                throw new InvalidOperationException("FLD unpublished Outbox entries block FULL_REPLACE generation switch.");

            ulong[] currentRuns = manifest.Runs.Where(run => run.Ns == _namespaceId)
                .Select(run => run.RunId).OrderBy(id => id).ToArray();
            if (!currentRuns.AsSpan().SequenceEqual(state.BaseRunIds) ||
                _lastSealedCommitSeq != state.BaseLastSealedCommitSeq)
                throw new InvalidOperationException("FLD active namespace generation changed while FULL_REPLACE was staged.");

            if (state.Runs.Length == 0)
            {
                byte[] emptyRun = new RunBuilder(_namespaceId).Build();
                ulong markerRunId = AllocateFullReplaceRunId();
                string markerPath = Path.Combine(_dbDir, "runs", $"{markerRunId}.fld1");
                WriteRunAtomically(markerPath, emptyRun);
                state = state with
                {
                    Runs = [new FullReplaceStageRun(0, 0, markerRunId, new byte[16], new byte[16])]
                };
                WriteFullReplaceStage(statePath, state);
            }

            var replacementEntries = state.Runs.Select(run => new RunEntry
            {
                RunId = run.RunId,
                Level = 0,
                Ns = _namespaceId,
                MinRec = run.MinRecordId,
                MaxRec = run.MaxRecordId,
                SeqLo = 0,
                SeqHi = state.BaseLastSealedCommitSeq
            }).ToList();
            var oldRuns = _runs.Select(run => (run.runId, run.path)).ToArray();
            var updatedRuns = manifest.Runs.Where(run => run.Ns != _namespaceId).ToList();
            updatedRuns.AddRange(replacementEntries);
            _manifestStore.Update(current => CloneManifest(
                current, lastFlushed: state.BaseLastSealedCommitSeq, runs: updatedRuns));

            _runs.Clear();
            foreach (FullReplaceStageRun run in state.Runs)
            {
                string runPath = Path.Combine(_dbDir, "runs", $"{run.RunId}.fld1");
                _runs.Add((run.RunId, runPath, File.ReadAllBytes(runPath)));
            }
            state = state with { IsCommitted = true };
            try { WriteFullReplaceStage(statePath, state); }
            catch { _faulted = true; throw; }

            HashSet<ulong>? pinned = GetPinnedProviderSnapshotRunIds();
            foreach (var oldRun in oldRuns)
            {
                if (pinned is null || pinned.Contains(oldRun.runId)) continue;
                try { if (File.Exists(oldRun.path)) File.Delete(oldRun.path); }
                catch { /* The manifest is authoritative; orphan collection retries later. */ }
            }
        }
    }

    /// <summary>Discard a staged, uncommitted generation in bounded run-file batches.</summary>
    public bool AbortProviderReplacement(Guid generation, int limit)
    {
        if (generation == Guid.Empty) throw new ArgumentException("Generation is required.", nameof(generation));
        if (limit <= 0 || limit > FullReplaceStagePageLimit) throw new ArgumentOutOfRangeException(nameof(limit));
        ObjectDisposedException.ThrowIf(_disposed, this);
        lock (_commitLock)
        {
            EnsureHealthy();
            string path = FullReplaceStagePath(generation);
            if (!File.Exists(path)) return true;
            FullReplaceStageState state = ReadFullReplaceStage(path);
            if (state.IsCommitted || IsFullReplaceStageCommitted(state, _manifestStore.Load()))
                throw new InvalidOperationException("Committed FULL_REPLACE generation cannot be aborted.");
            if (!state.IsAborting)
            {
                state = state with { IsAborting = true, AbortRunIndex = 0 };
                WriteFullReplaceStage(path, state);
            }
            int end = Math.Min(state.Runs.Length, checked(state.AbortRunIndex + limit));
            for (int i = state.AbortRunIndex; i < end; i++)
            {
                string runPath = Path.Combine(_dbDir, "runs", $"{state.Runs[i].RunId}.fld1");
                try { if (File.Exists(runPath)) File.Delete(runPath); }
                catch { return false; }
            }
            if (end == state.Runs.Length)
            {
                File.Delete(path);
                return true;
            }
            WriteFullReplaceStage(path, state with { AbortRunIndex = end });
            return false;
        }
    }

    private bool _databaseHasUnpublishedLocalChanges()
    {
        FldDraftHorizon horizon = ReadUnpublishedLocalDraftHorizon();
        return horizon.Count != 0 || GetOutboxHighWatermark() != GetOutboxWatermark();
    }

    private ulong AllocateFullReplaceRunId()
    {
        string runsDirectory = Path.Combine(_dbDir, "runs");
        while (true)
        {
            ulong candidate = _nextRunId++;
            if (candidate == 0) continue;
            if (!File.Exists(Path.Combine(runsDirectory, $"{candidate}.fld1")))
                return candidate;
        }
    }

    private string FullReplaceStagePath(Guid generation)
        => Path.Combine(_dbDir, "full-replace-staging", generation.ToString("N") + ".bin");

    private static byte[] FullReplaceSortKey(FldProviderSnapshotEntry entry)
    {
        byte[] key = new byte[19];
        entry.RecordId.CopyTo(key, 0);
        key[16] = entry.IsRecordTombstone ? (byte)0 : (byte)1;
        BinaryPrimitives.WriteUInt16BigEndian(key.AsSpan(17), entry.IsRecordTombstone ? (ushort)0 : entry.FieldId);
        return key;
    }

    private static void ValidateReplacementEntry(FldProviderSnapshotEntry entry)
    {
        if (entry.RecordId is null || entry.RecordId.Length != 16 ||
            entry.AuthorNodeId is null || entry.AuthorNodeId.Length != 16 ||
            entry.Value is null || entry.ChangeId is null ||
            entry.ChangeId.Length is not (0 or 16) ||
            (entry.OriginAuthorEpoch == 0) != (entry.OriginSequence == 0) ||
            (entry.OriginAuthorEpoch != 0 && entry.ChangeId.Length != 16) ||
            entry.HlcWall == 0)
            throw new InvalidDataException("FULL_REPLACE entry violates the canonical FLD provider snapshot contract.");
        if (entry.IsRecordTombstone)
        {
            if (entry.FieldId != FldLifecycle.FieldId || entry.IsTombstone ||
                entry.KeyEpoch != 0 || entry.Value.Length != 0)
                throw new InvalidDataException("FULL_REPLACE record tombstone is not canonical.");
        }
        else if (entry.FieldId == FldLifecycle.FieldId ||
            (entry.IsTombstone && entry.Value.Length != 0))
            throw new InvalidDataException("FULL_REPLACE field entry is not canonical.");
    }

    private IReadOnlyList<FldProviderSnapshotEntry> ReadFullReplaceRun(FullReplaceStageRun run)
    {
        string path = Path.Combine(_dbDir, "runs", $"{run.RunId}.fld1");
        if (!File.Exists(path)) throw new InvalidDataException("FULL_REPLACE staged Run is missing.");
        var reader = new RunReader(File.ReadAllBytes(path));
        if (!reader.TryValidate(out var verify) || reader.NamespaceId != _namespaceId)
            throw new InvalidDataException($"FULL_REPLACE staged Run is invalid: {verify.Status} {verify.Detail}");
        var result = new List<FldProviderSnapshotEntry>(run.EntryCount);
        byte[] recordId = new byte[16];
        for (uint i = 0; i < reader.NumClusters; i++)
        {
            if (!reader.TryReadRecordIdAt(i, recordId))
                throw new InvalidDataException("FULL_REPLACE staged Run has an invalid RecordId index.");
            var recordEntries = new List<FldProviderSnapshotEntry>();
            reader.ScanFieldIds(recordId, fieldId =>
            {
                if (!reader.TryGetField(recordId, fieldId, out RunFieldState field))
                    throw new InvalidDataException("FULL_REPLACE staged Run field index is inconsistent.");
                if (fieldId == FldLifecycle.FieldId)
                {
                    if (field.Value.Span.SequenceEqual(FldLifecycle.DeadValue))
                    {
                        recordEntries.Add(new FldProviderSnapshotEntry(
                            recordId.ToArray(), FldLifecycle.FieldId, Array.Empty<byte>(),
                            field.Wall, field.Logical, field.AuthorNodeId.ToArray(), 0,
                            false, field.ChangeId.ToArray(), field.OriginAuthorEpoch,
                            field.OriginSequence, true));
                    }
                    else if (!field.Value.Span.SequenceEqual(FldLifecycle.AliveValue))
                        throw new InvalidDataException("FULL_REPLACE staged lifecycle value is not canonical.");
                    return;
                }
                recordEntries.Add(new FldProviderSnapshotEntry(
                    recordId.ToArray(), fieldId, field.Value.ToArray(),
                    field.Wall, field.Logical, field.AuthorNodeId.ToArray(), field.KeyEpoch,
                    field.IsTombstone, field.ChangeId.ToArray(), field.OriginAuthorEpoch,
                    field.OriginSequence, false));
            });
            recordEntries.Sort(static (left, right) =>
            {
                if (left.IsRecordTombstone != right.IsRecordTombstone)
                    return left.IsRecordTombstone ? -1 : 1;
                return left.FieldId.CompareTo(right.FieldId);
            });
            result.AddRange(recordEntries);
        }
        if (result.Count != run.EntryCount)
            throw new InvalidDataException("FULL_REPLACE staged Run entry count differs from its durable page index.");
        return result;
    }

    /// <summary>
    /// Recover the commit marker if the atomic manifest switch completed immediately
    /// before a process crash. Called during database open before orphan Run collection.
    /// </summary>
    private bool RecoverFullReplaceStageCommitMarkers()
    {
        string directory = Path.Combine(_dbDir, "full-replace-staging");
        if (!Directory.Exists(directory)) return true;
        try
        {
            ManifestDocument manifest = _manifestStore.Load();
            foreach (string path in Directory.EnumerateFiles(directory, "*.bin"))
            {
                FullReplaceStageState state = ReadFullReplaceStage(path);
                if (!state.IsCommitted && IsFullReplaceStageCommitted(state, manifest))
                    WriteFullReplaceStage(path, state with { IsCommitted = true });
            }
            return true;
        }
        catch
        {
            // Corrupt stage metadata must never make its Run files eligible for GC.
            return false;
        }
    }

    private HashSet<ulong>? GetUncommittedFullReplaceRunIds()
    {
        if (!RecoverFullReplaceStageCommitMarkers()) return null;
        string directory = Path.Combine(_dbDir, "full-replace-staging");
        var result = new HashSet<ulong>();
        if (!Directory.Exists(directory)) return result;
        try
        {
            foreach (string path in Directory.EnumerateFiles(directory, "*.bin"))
            {
                FullReplaceStageState state = ReadFullReplaceStage(path);
                if (state.IsCommitted) continue;
                foreach (FullReplaceStageRun run in state.Runs)
                    result.Add(run.RunId);
            }
            return result;
        }
        catch
        {
            return null;
        }
    }

    private bool IsFullReplaceStageCommitted(FullReplaceStageState state, ManifestDocument manifest)
    {
        if (state.IsCommitted) return true;
        if (state.IsAborting) return false;
        var active = manifest.Runs.Where(run => run.Ns == _namespaceId)
            .Select(run => run.RunId).ToHashSet();
        return state.Runs.Length != 0 &&
            state.Runs.All(run => active.Contains(run.RunId)) &&
            state.BaseRunIds.All(runId => !active.Contains(runId));
    }

    private static void WriteFullReplaceStage(string path, FullReplaceStageState state)
    {
        Directory.CreateDirectory(Path.GetDirectoryName(path)!);
        byte[] bytes = EncodeFullReplaceStage(state);
        string temporary = path + ".tmp";
        using (var stream = new FileStream(temporary, FileMode.Create, FileAccess.Write, FileShare.None))
        {
            stream.Write(bytes);
            stream.Flush(true);
        }
        File.Move(temporary, path, true);
    }

    private static FullReplaceStageState ReadFullReplaceStage(string path)
        => DecodeFullReplaceStage(File.ReadAllBytes(path));

    private static byte[] EncodeFullReplaceStage(FullReplaceStageState state)
    {
        int length = checked(4 + 2 + 2 + 16 + 8 + 8 + 8 + 4 + 4 + 4 + 32 + 19 +
            4 + state.BaseRunIds.Length * 8 + 4 + state.Runs.Length * 48 + 32);
        byte[] bytes = new byte[length];
        int offset = 0;
        BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan(offset), FullReplaceStageMagic); offset += 4;
        BinaryPrimitives.WriteUInt16LittleEndian(bytes.AsSpan(offset), FullReplaceStageVersion); offset += 2;
        ushort flags = (ushort)((state.IsCommitted ? 1 : 0) | (state.IsAborting ? 2 : 0));
        BinaryPrimitives.WriteUInt16LittleEndian(bytes.AsSpan(offset), flags); offset += 2;
        state.Generation.TryWriteBytes(bytes.AsSpan(offset, 16)); offset += 16;
        BinaryPrimitives.WriteUInt64LittleEndian(bytes.AsSpan(offset), state.BaseManifestGeneration); offset += 8;
        BinaryPrimitives.WriteUInt64LittleEndian(bytes.AsSpan(offset), state.BaseLastSealedCommitSeq); offset += 8;
        BinaryPrimitives.WriteUInt64LittleEndian(bytes.AsSpan(offset), state.SourceFeedWatermark); offset += 8;
        BinaryPrimitives.WriteInt32LittleEndian(bytes.AsSpan(offset), state.ExpectedEntryCount); offset += 4;
        BinaryPrimitives.WriteInt32LittleEndian(bytes.AsSpan(offset), state.NextIndex); offset += 4;
        BinaryPrimitives.WriteInt32LittleEndian(bytes.AsSpan(offset), state.AbortRunIndex); offset += 4;
        state.ExpectedDigest.CopyTo(bytes, offset); offset += 32;
        state.LastSortKey.CopyTo(bytes, offset); offset += 19;
        BinaryPrimitives.WriteInt32LittleEndian(bytes.AsSpan(offset), state.BaseRunIds.Length); offset += 4;
        foreach (ulong runId in state.BaseRunIds) { BinaryPrimitives.WriteUInt64LittleEndian(bytes.AsSpan(offset), runId); offset += 8; }
        BinaryPrimitives.WriteInt32LittleEndian(bytes.AsSpan(offset), state.Runs.Length); offset += 4;
        foreach (FullReplaceStageRun run in state.Runs)
        {
            BinaryPrimitives.WriteInt32LittleEndian(bytes.AsSpan(offset), run.StartIndex); offset += 4;
            BinaryPrimitives.WriteInt32LittleEndian(bytes.AsSpan(offset), run.EntryCount); offset += 4;
            BinaryPrimitives.WriteUInt64LittleEndian(bytes.AsSpan(offset), run.RunId); offset += 8;
            run.MinRecordId.CopyTo(bytes, offset); offset += 16;
            run.MaxRecordId.CopyTo(bytes, offset); offset += 16;
        }
        SHA256.HashData(bytes.AsSpan(0, offset)).CopyTo(bytes, offset);
        return bytes;
    }

    private static FullReplaceStageState DecodeFullReplaceStage(byte[] bytes)
    {
        if (bytes.Length < 4 + 2 + 2 + 16 + 8 + 8 + 8 + 4 + 4 + 4 + 32 + 19 + 4 + 4 + 32)
            throw new InvalidDataException("FULL_REPLACE stage state is truncated.");
        int checksumOffset = bytes.Length - 32;
        byte[] checksum = SHA256.HashData(bytes.AsSpan(0, checksumOffset));
        if (!CryptographicOperations.FixedTimeEquals(checksum, bytes.AsSpan(checksumOffset)))
            throw new InvalidDataException("FULL_REPLACE stage state checksum mismatch.");
        int offset = 0;
        if (BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(offset)) != FullReplaceStageMagic) throw new InvalidDataException("FULL_REPLACE stage magic mismatch."); offset += 4;
        if (BinaryPrimitives.ReadUInt16LittleEndian(bytes.AsSpan(offset)) != FullReplaceStageVersion) throw new InvalidDataException("FULL_REPLACE stage version mismatch."); offset += 2;
        ushort flags = BinaryPrimitives.ReadUInt16LittleEndian(bytes.AsSpan(offset)); offset += 2;
        if ((flags & ~3) != 0) throw new InvalidDataException("FULL_REPLACE stage flags are invalid.");
        Guid generation = new(bytes.AsSpan(offset, 16)); offset += 16;
        ulong baseManifestGeneration = BinaryPrimitives.ReadUInt64LittleEndian(bytes.AsSpan(offset)); offset += 8;
        ulong baseSealed = BinaryPrimitives.ReadUInt64LittleEndian(bytes.AsSpan(offset)); offset += 8;
        ulong watermark = BinaryPrimitives.ReadUInt64LittleEndian(bytes.AsSpan(offset)); offset += 8;
        int expected = BinaryPrimitives.ReadInt32LittleEndian(bytes.AsSpan(offset)); offset += 4;
        int next = BinaryPrimitives.ReadInt32LittleEndian(bytes.AsSpan(offset)); offset += 4;
        int abortRunIndex = BinaryPrimitives.ReadInt32LittleEndian(bytes.AsSpan(offset)); offset += 4;
        byte[] digest = bytes.AsSpan(offset, 32).ToArray(); offset += 32;
        byte[] lastSortKey = bytes.AsSpan(offset, 19).ToArray(); offset += 19;
        int baseCount = BinaryPrimitives.ReadInt32LittleEndian(bytes.AsSpan(offset)); offset += 4;
        if (generation == Guid.Empty || expected < 0 || next < 0 || next > expected || abortRunIndex < 0 || baseCount < 0 || baseCount > 1_000_000)
            throw new InvalidDataException("FULL_REPLACE stage scalar fields are invalid.");
        if (offset + (long)baseCount * 8 + 4 > checksumOffset)
            throw new InvalidDataException("FULL_REPLACE stage base-run index is truncated.");
        var baseRuns = new ulong[baseCount];
        ulong previousBaseRun = 0;
        for (int i = 0; i < baseCount; i++)
        {
            baseRuns[i] = BinaryPrimitives.ReadUInt64LittleEndian(bytes.AsSpan(offset)); offset += 8;
            if (baseRuns[i] == 0 || (i != 0 && baseRuns[i] <= previousBaseRun))
                throw new InvalidDataException("FULL_REPLACE base Run index is not strictly ordered.");
            previousBaseRun = baseRuns[i];
        }
        int runCount = BinaryPrimitives.ReadInt32LittleEndian(bytes.AsSpan(offset)); offset += 4;
        if (runCount < 0 || runCount > 1_000_000 || offset + (long)runCount * 48 != checksumOffset)
            throw new InvalidDataException("FULL_REPLACE stage run index is invalid.");
        var runs = new FullReplaceStageRun[runCount];
        int expectedStart = 0;
        for (int i = 0; i < runCount; i++)
        {
            int start = BinaryPrimitives.ReadInt32LittleEndian(bytes.AsSpan(offset)); offset += 4;
            int count = BinaryPrimitives.ReadInt32LittleEndian(bytes.AsSpan(offset)); offset += 4;
            ulong runId = BinaryPrimitives.ReadUInt64LittleEndian(bytes.AsSpan(offset)); offset += 8;
            byte[] min = bytes.AsSpan(offset, 16).ToArray(); offset += 16;
            byte[] max = bytes.AsSpan(offset, 16).ToArray(); offset += 16;
            if (start != expectedStart || count < 0 || runId == 0 || min.Length != 16 || max.Length != 16)
                throw new InvalidDataException("FULL_REPLACE stage page index is not canonical.");
            expectedStart = checked(expectedStart + count);
            runs[i] = new FullReplaceStageRun(start, count, runId, min, max);
        }
        if (expectedStart != next || abortRunIndex > runCount || digest.Length != 32 || lastSortKey.Length != 19)
            throw new InvalidDataException("FULL_REPLACE stage cursor does not match its page index.");
        return new FullReplaceStageState(
            generation, baseManifestGeneration, baseSealed, watermark, expected, next,
            digest, lastSortKey, baseRuns, runs, (flags & 1) != 0, (flags & 2) != 0, abortRunIndex);
    }
}
