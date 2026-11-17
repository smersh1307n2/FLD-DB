namespace Fld1.Storage.Engine;

/// <summary>One sorted mutation. Key/value memory must remain immutable until ApplyBatch returns.</summary>
internal readonly record struct StorageTreeMutation(
    ReadOnlyMemory<byte> Key,
    ReadOnlyMemory<byte> Value,
    bool IsTombstone);

/// <summary>
/// Immutable path-copy updater. Mutations are grouped by their current leaf so each
/// affected leaf and each ancestor path is copied once per group. New leaves are
/// unlinked (NextPageId=0); readers advance through the internal tree, not leaf links.
/// </summary>
internal static class StorageBTreeUpdater
{
    public static StorageBTreeManifestV2 ApplyBatch(
        in StorageBTreeManifestV2 current,
        StorageBTreeGenerationFiles files,
        IReadOnlyList<StorageTreeMutation> mutations)
    {
        ArgumentNullException.ThrowIfNull(files);
        ArgumentNullException.ThrowIfNull(mutations);
        if (files.PartitionId != current.PartitionId || files.Generation != current.Generation)
            throw new InvalidDataException("B+ tree updater generation identity mismatch.");
        if (files.PageFileLength != current.CommittedPageFileLength ||
            files.DataFileLength != current.CommittedDataFileLength)
            throw new InvalidDataException("B+ tree updater requires files truncated to the current committed root.");
        if (current.CommittedPageFileLength % StoragePageCodec.PageSize != 0)
            throw new InvalidDataException("Current B+ tree committed page length is not aligned.");
        if (mutations.Count == 0)
            return current;

        for (int i = 0; i < mutations.Count; i++)
        {
            StorageTreeMutation mutation = mutations[i];
            if (mutation.Key.IsEmpty)
                throw new InvalidDataException("B+ tree mutation keys cannot be empty.");
            if (mutation.IsTombstone && !mutation.Value.IsEmpty)
                throw new InvalidDataException("Tombstone mutations cannot carry value bytes.");
            if (i > 0 && mutations[i - 1].Key.Span.SequenceCompareTo(mutation.Key.Span) >= 0)
                throw new InvalidDataException("B+ tree mutation batch must be strictly sorted with no duplicate keys.");
        }

        // A bounded per-batch page cache avoids allocating and decoding the same
        // immutable internal/leaf pages for every sorted mutation in a delete batch.
        // It is deliberately scoped to this ApplyBatch call: failed batches may reuse
        // uncommitted page IDs after truncation, so no stale page can survive a retry.
        var pageReadCache = new BatchPageReadCache();

        ulong nextCommitSeq = checked(current.CommitSeq + 1);
        ulong nextPageId = checked(current.CommittedPageFileLength / StoragePageCodec.PageSize + 1);
        ulong rootPageId = current.RootPageId;

        try
        {
            int start = 0;
            while (start < mutations.Count)
            {
                var path = new List<StorageTreePathFrame>();
                ulong leafId = rootPageId == 0
                    ? 0
                    : FindLeaf(rootPageId, mutations[start].Key.Span, files, path, pageReadCache);

                int end = start + 1;
                var candidatePath = new List<StorageTreePathFrame>();
                while (end < mutations.Count)
                {
                    candidatePath.Clear();
                    ulong candidate = rootPageId == 0
                        ? 0
                        : FindLeaf(rootPageId, mutations[end].Key.Span, files, candidatePath, pageReadCache);
                    if (candidate != leafId)
                        break;
                    end++;
                }

                StorageTreeLeafEntry[] existing = leafId == 0
                    ? Array.Empty<StorageTreeLeafEntry>()
                    : StorageBTreePageCodec.DecodeLeaf(
                        pageReadCache.Read(files, leafId, files.PageFileLength), files.Generation, leafId).Entries;
                ValidateLeafEntries(existing, files);

                List<StorageTreeLeafEntry> merged = MergeLeaf(existing, mutations, start, end, files);
                List<StorageTreeChild> replacements = WriteLeafLevel(merged, files, ref nextPageId);
                rootPageId = RewritePath(path, leafId, replacements, files, ref nextPageId, pageReadCache);
                start = end;
            }

            return new StorageBTreeManifestV2(
                current.PartitionId,
                current.Generation,
                nextCommitSeq,
                rootPageId,
                files.PageFileLength,
                files.DataFileLength,
                StoragePageCodec.PageSize,
                FormatFlags: 0);
        }
        catch (Exception error)
        {
            try
            {
                files.TruncateToCommittedLengths(
                    current.CommittedPageFileLength,
                    current.CommittedDataFileLength);
            }
            catch (Exception rollbackError)
            {
                throw new AggregateException(
                    "B+ tree COW batch failed and its uncommitted file tails could not be rolled back.",
                    error,
                    rollbackError);
            }
            throw;
        }
    }

    /// <summary>
    /// Applies a batch and publishes its root. The caller must first durably append
    /// the matching WAL batch at current.CommitSeq + 1. Any publication exception
    /// requires reopening the root before deciding whether to truncate or retry.
    /// </summary>
    public static StorageBTreeManifestV2 ApplyAndPublish(
        string databaseDirectory,
        in StorageBTreeManifestV2 current,
        StorageBTreeGenerationFiles files,
        IReadOnlyList<StorageTreeMutation> mutations,
        Func<ulong> appendAndFlushWal,
        Action<StorageFaultPoint>? faultInjector = null)
    {
        ArgumentNullException.ThrowIfNull(appendAndFlushWal);
        ulong expectedCommitSeq = checked(current.CommitSeq + 1);
        ulong durableCommitSeq = appendAndFlushWal();
        if (durableCommitSeq != expectedCommitSeq)
            throw new InvalidDataException(
                $"B+ tree COW batch requires durable WAL CommitSeq {expectedCommitSeq}, received {durableCommitSeq}.");

        // WAL is the recovery authority. If page writing or root publication fails
        // after this point, reopen/recovery must replay the durable batch or publish
        // a root at this CommitSeq; never truncate the WAL entry as an ordinary retry.
        StorageBTreeManifestV2 next = ApplyBatch(current, files, mutations);
        StorageBTreeRootPublisher.Publish(databaseDirectory, next, files, faultInjector);
        return next;
    }

    private static List<StorageTreeLeafEntry> MergeLeaf(
        StorageTreeLeafEntry[] existing,
        IReadOnlyList<StorageTreeMutation> mutations,
        int mutationStart,
        int mutationEnd,
        StorageBTreeGenerationFiles files)
    {
        var merged = new List<StorageTreeLeafEntry>(existing.Length + mutationEnd - mutationStart);
        int e = 0;
        int m = mutationStart;

        while (e < existing.Length || m < mutationEnd)
        {
            if (e >= existing.Length)
            {
                merged.Add(CreateNewEntry(mutations[m++], files));
                continue;
            }
            if (m >= mutationEnd)
            {
                merged.Add(existing[e++]);
                continue;
            }

            StorageTreeLeafEntry current = existing[e];
            StorageTreeMutation mutation = mutations[m];
            ReadOnlyMemory<byte> existingKey = ReadKey(files, current.Key);
            int comparison = existingKey.Span.SequenceCompareTo(mutation.Key.Span);
            if (comparison < 0)
            {
                merged.Add(current);
                e++;
            }
            else if (comparison > 0)
            {
                merged.Add(CreateNewEntry(mutation, files));
                m++;
            }
            else
            {
                merged.Add(new StorageTreeLeafEntry(current.Key, CreateValueReference(mutation, files)));
                e++;
                m++;
            }
        }

        return merged;
    }

    private static StorageTreeLeafEntry CreateNewEntry(
        StorageTreeMutation mutation,
        StorageBTreeGenerationFiles files)
    {
        ulong keyOffset = files.AppendData(mutation.Key.Span);
        var key = new StorageTreeKeyReference(keyOffset, mutation.Key.Length);
        return new StorageTreeLeafEntry(key, CreateValueReference(mutation, files));
    }

    private static StorageTreeValueReference CreateValueReference(
        StorageTreeMutation mutation,
        StorageBTreeGenerationFiles files)
    {
        if (mutation.IsTombstone)
            return new StorageTreeValueReference(files.DataFileLength, 0, true);

        ulong valueOffset = files.AppendData(mutation.Value.Span);
        return new StorageTreeValueReference(valueOffset, mutation.Value.Length, false);
    }

    private static List<StorageTreeChild> WriteLeafLevel(
        IReadOnlyList<StorageTreeLeafEntry> entries,
        StorageBTreeGenerationFiles files,
        ref ulong nextPageId)
    {
        if (entries.Count == 0)
            throw new InvalidDataException("A non-empty COW batch cannot produce an empty leaf.");

        int maximum = StorageBTreePageCodec.MaxLeafEntries;
        int groupCount = (entries.Count + maximum - 1) / maximum;
        int baseSize = entries.Count / groupCount;
        int extra = entries.Count % groupCount;
        var result = new List<StorageTreeChild>(groupCount);
        int start = 0;

        for (int group = 0; group < groupCount; group++)
        {
            int count = baseSize + (group < extra ? 1 : 0);
            var pageEntries = new StorageTreeLeafEntry[count];
            for (int i = 0; i < count; i++)
                pageEntries[i] = entries[start + i];

            ulong pageId = Allocate(ref nextPageId);
            files.AppendPage(pageId,
                StorageBTreePageCodec.EncodeLeaf(files.Generation, pageId, 0, pageEntries));
            result.Add(new StorageTreeChild(pageId, pageEntries[0].Key));
            start += count;
        }

        return result;
    }

    private static ulong RewritePath(
        List<StorageTreePathFrame> path,
        ulong oldLeafId,
        List<StorageTreeChild> replacements,
        StorageBTreeGenerationFiles files,
        ref ulong nextPageId,
        BatchPageReadCache pageReadCache)
    {
        ulong oldChildId = oldLeafId;
        for (int i = path.Count - 1; i >= 0; i--)
        {
            StorageTreePathFrame frame = path[i];
            StorageTreeInternalPage parent =
                pageReadCache.ReadInternal(files, frame.PageId, files.PageFileLength);

            if (StorageBTreePageCodec.GetInternalChild(parent, frame.ChildIndex) != oldChildId)
                throw new InvalidDataException("B+ tree COW path no longer points at the expected child.");

            List<StorageTreeChild> children = ExpandChildren(parent, files, pageReadCache);
            children.RemoveAt(frame.ChildIndex);
            children.InsertRange(frame.ChildIndex, replacements);
            replacements = WriteInternalLevel(children, files, ref nextPageId);
            oldChildId = frame.PageId;
        }

        while (replacements.Count > StorageBTreePageCodec.MaxInternalChildren)
            replacements = WriteInternalLevel(replacements, files, ref nextPageId);

        if (replacements.Count == 1)
            return replacements[0].PageId;

        List<StorageTreeChild> root = WriteInternalLevel(replacements, files, ref nextPageId);
        if (root.Count != 1)
            throw new InvalidOperationException("B+ tree root construction did not converge.");
        return root[0].PageId;
    }

    private static List<StorageTreeChild> WriteInternalLevel(
        IReadOnlyList<StorageTreeChild> children,
        StorageBTreeGenerationFiles files,
        ref ulong nextPageId)
    {
        if (children.Count == 0)
            throw new InvalidDataException("B+ tree internal node cannot have zero children.");
        if (children.Count == 1)
            return new List<StorageTreeChild> { children[0] };

        int maximum = StorageBTreePageCodec.MaxInternalChildren;
        int groupCount = (children.Count + maximum - 1) / maximum;
        int baseSize = children.Count / groupCount;
        int extra = children.Count % groupCount;
        var result = new List<StorageTreeChild>(groupCount);
        int start = 0;

        for (int group = 0; group < groupCount; group++)
        {
            int count = baseSize + (group < extra ? 1 : 0);
            var pageChildren = new StorageTreeChild[count];
            for (int i = 0; i < count; i++)
                pageChildren[i] = children[start + i];

            ulong pageId = Allocate(ref nextPageId);
            var separators = new StorageTreeChild[pageChildren.Length - 1];
            Array.Copy(pageChildren, 1, separators, 0, separators.Length);
            files.AppendPage(pageId,
                StorageBTreePageCodec.EncodeInternal(files.Generation, pageId, pageChildren[0].PageId, separators));
            result.Add(new StorageTreeChild(pageId, pageChildren[0].FirstKey));
            start += count;
        }

        return result;
    }

    private static List<StorageTreeChild> ExpandChildren(
        StorageTreeInternalPage node,
        StorageBTreeGenerationFiles files,
        BatchPageReadCache pageReadCache)
    {
        var children = new List<StorageTreeChild>(node.Separators.Length + 1)
        {
            new StorageTreeChild(node.FirstChild, FindFirstKey(node.FirstChild, files, pageReadCache))
        };
        children.AddRange(node.Separators);
        return children;
    }

    private static StorageTreeKeyReference FindFirstKey(
        ulong pageId,
        StorageBTreeGenerationFiles files,
        BatchPageReadCache pageReadCache)
    {
        if (pageReadCache.TryGetFirstKey(pageId, out StorageTreeKeyReference cached))
            return cached;

        ulong id = pageId;
        for (int depth = 0; depth < 64; depth++)
        {
            StoragePageView page = pageReadCache.Decode(files, id, files.PageFileLength);
            if (page.Kind == StoragePageKind.Leaf)
            {
                byte[] bytes = pageReadCache.Read(files, id, files.PageFileLength);
                StorageTreeLeafPage leaf = StorageBTreePageCodec.DecodeLeaf(bytes, files.Generation, id);
                if (leaf.Entries.Length == 0)
                    throw new InvalidDataException("B+ tree contains an empty leaf.");
                StorageTreeKeyReference firstKey = leaf.Entries[0].Key;
                pageReadCache.RememberFirstKey(pageId, firstKey);
                return firstKey;
            }

            StorageTreeInternalPage node =
                pageReadCache.ReadInternal(files, id, files.PageFileLength);
            id = node.FirstChild;
        }

        throw new InvalidDataException("B+ tree depth exceeds the supported maximum or contains a cycle.");
    }

    private static ulong FindLeaf(
        ulong rootPageId,
        ReadOnlySpan<byte> key,
        StorageBTreeGenerationFiles files,
        List<StorageTreePathFrame> path,
        BatchPageReadCache pageReadCache)
    {
        ulong id = rootPageId;
        for (int depth = 0; depth < 64; depth++)
        {
            StoragePageView page = pageReadCache.Decode(files, id, files.PageFileLength);
            if (page.Kind == StoragePageKind.Leaf)
                return id;

            StorageTreeInternalPage node =
                pageReadCache.ReadInternal(files, id, files.PageFileLength);
            int childIndex = StorageBTreePageCodec.SelectInternalChildIndex(
                page, key, reference => ReadKey(files, reference));
            path.Add(new StorageTreePathFrame(id, childIndex));
            id = StorageBTreePageCodec.GetInternalChild(node, childIndex);
        }

        throw new InvalidDataException("B+ tree depth exceeds the supported maximum or contains a cycle.");
    }

    private static void ValidateInternalNode(
        StorageTreeInternalPage node,
        StorageBTreeGenerationFiles files)
    {
        var children = new HashSet<ulong> { node.FirstChild };
        StorageTreeKeyReference? previous = null;
        foreach (StorageTreeChild separator in node.Separators)
        {
            if (previous.HasValue &&
                ReadKey(files, previous.Value).Span.SequenceCompareTo(ReadKey(files, separator.FirstKey).Span) >= 0)
                throw new InvalidDataException("B+ tree internal separators are not strictly ordered.");
            if (!children.Add(separator.PageId))
                throw new InvalidDataException("B+ tree internal page contains a duplicate child reference.");
            previous = separator.FirstKey;
        }
    }

    private static void ValidateLeafEntries(
        StorageTreeLeafEntry[] entries,
        StorageBTreeGenerationFiles files)
    {
        for (int i = 1; i < entries.Length; i++)
        {
            if (ReadKey(files, entries[i - 1].Key).Span.SequenceCompareTo(
                    ReadKey(files, entries[i].Key).Span) >= 0)
                throw new InvalidDataException("B+ tree leaf keys are not strictly ordered.");
        }
    }

    private static ReadOnlyMemory<byte> ReadKey(
        StorageBTreeGenerationFiles files,
        StorageTreeKeyReference reference)
        => files.ReadData(reference.Offset, reference.Length, files.DataFileLength);

    /// <summary>
    /// FIFO cache for immutable pages touched during one COW batch. The cap bounds
    /// temporary memory even when a large mutation batch spans many thousands of leaves.
    /// </summary>
    private sealed class BatchPageReadCache
    {
        private const int Capacity = 4096;
        private readonly Dictionary<ulong, PageEntry> _pages = new();
        private readonly Queue<ulong> _order = new();

        public byte[] Read(
            StorageBTreeGenerationFiles files,
            ulong pageId,
            ulong committedPageFileLength)
            => GetEntry(files, pageId, committedPageFileLength).Bytes;

        public StoragePageView Decode(
            StorageBTreeGenerationFiles files,
            ulong pageId,
            ulong committedPageFileLength)
        {
            PageEntry entry = GetEntry(files, pageId, committedPageFileLength);
            if (!entry.DecodedPage.HasValue)
                entry.DecodedPage = StoragePageCodec.Decode(entry.Bytes, files.Generation, pageId);
            return entry.DecodedPage.Value;
        }

        public StorageTreeInternalPage ReadInternal(
            StorageBTreeGenerationFiles files,
            ulong pageId,
            ulong committedPageFileLength)
        {
            PageEntry entry = GetEntry(files, pageId, committedPageFileLength);
            if (entry.InternalPage is not null)
                return entry.InternalPage;

            StoragePageView page = entry.DecodedPage ??
                (entry.DecodedPage = StoragePageCodec.Decode(entry.Bytes, files.Generation, pageId)).Value;
            StorageTreeInternalPage node = StorageBTreePageCodec.DecodeInternal(page);
            ValidateInternalNode(node, files);
            entry.InternalPage = node;
            return node;
        }

        public bool TryGetFirstKey(ulong pageId, out StorageTreeKeyReference firstKey)
        {
            if (_pages.TryGetValue(pageId, out PageEntry? entry) && entry.FirstKey.HasValue)
            {
                firstKey = entry.FirstKey.Value;
                return true;
            }
            firstKey = default;
            return false;
        }

        public void RememberFirstKey(ulong pageId, StorageTreeKeyReference firstKey)
        {
            if (_pages.TryGetValue(pageId, out PageEntry? entry))
                entry.FirstKey = firstKey;
        }

        private PageEntry GetEntry(
            StorageBTreeGenerationFiles files,
            ulong pageId,
            ulong committedPageFileLength)
        {
            if (_pages.TryGetValue(pageId, out PageEntry? cached))
                return cached;

            var entry = new PageEntry(files.ReadPage(pageId, committedPageFileLength));
            if (_pages.Count == Capacity)
            {
                ulong oldest = _order.Dequeue();
                _pages.Remove(oldest);
            }

            _pages.Add(pageId, entry);
            _order.Enqueue(pageId);
            return entry;
        }

        private sealed class PageEntry(byte[] bytes)
        {
            public byte[] Bytes { get; } = bytes;
            public StoragePageView? DecodedPage { get; set; }
            public StorageTreeInternalPage? InternalPage { get; set; }
            public StorageTreeKeyReference? FirstKey { get; set; }
        }
    }

    private static ulong Allocate(ref ulong nextPageId)
    {
        ulong result = nextPageId;
        if (result == 0)
            throw new InvalidOperationException("B+ tree page ID zero is reserved.");
        nextPageId = checked(result + 1);
        return result;
    }
}
