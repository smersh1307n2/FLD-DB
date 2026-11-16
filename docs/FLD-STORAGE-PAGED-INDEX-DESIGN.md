# FLD Storage Core — paged ordered index design

Status: Stage C immutable B+ tree builder/reader implemented; production tree integration not started
Date: 2026-11-16
Scope: Fld1.Storage physical key/value engine only. No BIN-SYNC protocol changes and no FLD field/tombstone policy changes.

## 1. Why the current engine must change

The current implementation is correct for small embedded datasets but is not out-of-core:

- StorageDatabase owns every live key/value in Dictionary<StorageKey, byte[]>.
- Opening a partition loads the complete v1 snapshot and replays the WAL into that dictionary.
- OpenSnapshot constructs a complete SortedList copy of the key index. Cached snapshots avoid repeated sorting, but the first snapshot is O(N) memory and construction.
- Checkpoint makes another sorted copy and StorageSnapshotFile.EncodeSnapshot materializes the complete encoded snapshot in a MemoryStream, then a second complete byte array.
- StorageSnapshotFile.Load uses File.ReadAllBytes for both manifest and snapshot.
- WAL open/recovery currently supplies a materialized record collection; this must be revisited before bounded recovery can be claimed.
- A StorageSnapshot is currently a managed sorted-list view. It does not pin a disk generation because no disk-backed index is used yet.

The existing 64 MiB FLD Run cache and mmap Run work do not bound this independent Storage Core heap.

## 2. Target properties

1. Dataset cardinality must not determine steady-state managed heap.
2. Point lookup must be ordered-index lookup, not a scan of all keys.
3. Prefix/range scans must be forward-only and page-bounded.
4. The page cache has an explicit byte budget and deterministic eviction.
5. A write batch remains atomic and WAL remains the recovery authority.
6. A snapshot sees one commit sequence and remains valid across later commits and compaction.
7. A crash at any publication boundary recovers either the previous committed root plus WAL replay, or the new committed root; never a partially published tree.
8. Existing v1 snapshot files remain readable. Migration is explicit and crash-safe.
9. Large keys and values remain supported within existing format/API limits; no silent narrowing of limits.
10. No background maintenance thread is required for correctness. Compaction can be explicitly scheduled by the owning database/host.

## 3. Proposed on-disk structure

Use a copy-on-write B+ tree rooted by a small atomic manifest. The tree is immutable after a root is published; commits append new pages and publish a new root. This gives snapshots a stable root without copying the entire key set.

### 3.1 Files per partition

- snapshots/p_<partition>.manifest: current durable root and commit sequence.
- snapshots/p_<partition>.<generation>.pages: fixed-size B+ tree pages for one immutable generation.
- snapshots/p_<partition>.<generation>.data: append-only key/value records referenced by tree pages.
- Existing v1 p_<partition>.<generation>.snap files remain readable during migration.

The first implementation should use a 16 KiB logical page size, configurable only internally until benchmark evidence justifies a change. Page IDs are generation-local and page offsets use checked arithmetic. Every page has a format/version, page kind, generation, page ID, payload length and CRC32C. Invalid page identity, bounds, ordering, child references or checksum is a hard corruption error; do not silently skip a bad page.

### 3.2 Data records

Tree pages store compact references to immutable records in the generation's data file. A record contains flags, key length, value length, key bytes and (for a put) value bytes. A delete is represented as an explicit tombstone record until compaction proves that no older reachable page can expose the deleted key.

Keeping variable-length key/value bytes in the data file avoids imposing a page-sized key/value limit. Existing maximum key and WAL payload constraints remain authoritative. Record reads validate offsets and lengths against the manifest's committed data length before allocating or exposing bytes.

### 3.3 B+ tree pages

- Internal pages contain ordered separator-key references and child page IDs.
- Leaf pages contain ordered key references and value/tombstone record references.
- Leaf sibling links are generation-local and support forward range scans; cursors must still validate page identity and ordering.
- Splits and root growth create new pages. Unchanged subtrees are shared by the new root.
- A multi-key batch builds all changed paths before publishing one new root. No root is visible until every referenced page and data record has been flushed.
- Page cache entries are keyed by (generation, pageId), never by page ID alone.

The first version is single-writer, consistent with the existing partition writer fence. Readers use immutable roots. No concurrent in-place page mutation is permitted.

## 4. Commit and recovery ordering

A commit remains one atomic WAL batch. Proposed order:

1. Validate the batch and encode the WAL record.
2. Append the WAL record and flush it to stable storage.
3. Append immutable data records and copy-on-write tree pages for the entire batch.
4. Flush page and data files.
5. Atomically publish the manifest containing the new root page, generation, commit sequence and committed file lengths; flush the containing directory using the existing platform-specific durability helper.
6. Publish the new in-memory root/cache view.
7. Reclaim WAL segments only after the manifest root is durable and covers their commit sequence.

Crash interpretation:

- Before step 2: no committed operation.
- After step 2 but before step 5: old manifest is authoritative; recovery replays the WAL suffix and publishes a replacement root.
- After step 5: new manifest is authoritative; WAL records at or below its commit sequence are ignored during replay.
- An unreferenced page/data tail is an orphan, never a root. It may be truncated only after the manifest and WAL recovery chain have been validated.
- A failed commit faults the open instance; reopen is required, preserving the current API rule.

WAL replay must be streamed in commit order. It must not first materialize all WAL records or the full decoded dataset. The first recovery implementation may build bounded batches of COW pages; if the replay suffix exceeds the configured work budget, it must flush/publish intermediate roots only at complete WAL batch boundaries.

## 5. Snapshot and page-cache lifetime

A snapshot captures (generation, rootPageId, commitSeq) and increments a generation pin. It does not own a full sorted key/value copy.

- Point lookups traverse immutable pages through the shared bounded page cache.
- Prefix cursors hold a generation pin and a bounded cursor stack / current leaf page. They do not materialize all keys.
- A returned StorageEntry remains an owned copy, as today.
- Borrowed StorageEntryView remains valid only during its callback. The cursor may pin the current page while the callback executes; it must not expose a span after page eviction or cursor advance.
- Large values that do not fit in a page are materialized for the existing TryGet/visitor API. A future explicit streaming-value API may avoid that copy; do not change existing semantics in the first migration.
- Disposing a snapshot releases its pin. Compaction may retire an old generation only after all snapshot/cursor pins and in-flight page reads have drained.
- Database disposal prevents new operations but must not invalidate already-open snapshots; this matches the intended stable-snapshot contract and must be tested explicitly.

Initial cache target: 64 MiB total page payload, with a small bounded metadata allowance. The limit is global to one StorageDatabase instance, not per generation. Cache eviction must never delete durable pages. Cache miss I/O is synchronous initially; async prefetch is a later optimization only if measured.

## 6. Compaction and old-generation reclamation

Compaction creates a new generation by ordered traversal of the current root, writes a dense tree/data layout, flushes it, then atomically publishes a manifest pointing at the new root. It does not rewrite or mutate a generation visible to a snapshot.

After publication:
- the old generation is marked retired;
- it remains on disk while any snapshot/cursor pin exists;
- once the pin count reaches zero, files can be deleted and the directory flushed;
- crash recovery treats the manifest as the only root and cleans orphan generations only when no live process can hold a pin (the existing exclusive writer fence helps prevent a second writer, but cross-process readers must be accounted for before enabling deletion).

Do not add TTL-based deletion. Do not infer that a generation is unpinned merely because a timer elapsed.

## 7. V1 compatibility and migration

- Keep the current v1 snapshot reader intact as a compatibility path.
- On open, inspect manifest magic/version and dispatch to v1 or paged-index v2.
- A v1 partition opens with the legacy reader initially; it must not be silently rewritten during open.
- The first explicit checkpoint/migration builds a v2 generation from a sorted traversal of the v1 state, flushes it, then atomically publishes a v2 manifest.
- Preserve the old v1 generation until the v2 manifest is durable and no legacy snapshot handle can refer to it.
- Add fixtures generated by the old implementation and test open, point lookup, prefix scan, commit, checkpoint, reopen and crash recovery after migration.
- Downgrade behavior must be explicit: older binaries must not open a v2 manifest as if it were v1. Fail closed with a clear unsupported-format error.

## 8. Implementation stages

### Stage A — measurement and contracts
- Correct the Storage index benchmark so every case produces a real result; reject NA rows in CI.
- Record 1k / 10k / 100k latency and allocation, then extend to 1M once runtime is practical.
- Add tests for snapshot lifetime, cursor lifetime, large key/value behavior and current v1 compatibility before changing storage internals.

### Stage B — page primitives
- [x] Add page codec, CRC/identity validation, bounded page cache, generation pin registry and positional random-access file abstraction.
- [x] Unit-test page identity/CRC validation, short reads, invalid offsets, cache eviction, pinned-page budget behavior, generation retirement and concurrent pin accounting.
- [x] Add B+ tree leaf/internal node codecs and split/fan-out coverage in Stage C.
- No production routing through the new tree yet. Cache misses are currently serialized under the cache lock for duplicate-load safety; parallel miss loading is a later optimization only if profiling justifies it.

### Stage C — immutable B+ tree builder/reader
- [x] Build immutable leaf and internal pages from a strictly sorted input stream.
- [x] Implement point lookup and forward prefix cursor over linked leaves; cursor retains only the current leaf and detects link cycles through strict key ordering.
- [x] Keep variable-length keys and values external to index pages using generation-local offset/length references.
- [x] Randomized reference-model tests, tombstones, duplicate/unsorted-input rejection, multi-level splits, empty tree, 1 MiB key reference, bounded cursor state and leaf-cycle detection.
- [x] Add BenchmarkDotNet cases for bulk build, warm/random point lookup, prefix scan, large-value references and a four-page cache-pressure path alongside the existing Storage baseline matrix.
- [ ] Complete the exact-head benchmark workflow and compare measured time/allocations; no performance win is assumed.
- No production routing through the tree yet.

### Stage D — WAL integration
- Add copy-on-write batch application and atomic root publication.
- Stream WAL recovery and prove commit-sequence continuity.
- Add deterministic fault injection at every flush/rename/manifest boundary and crash/reopen tests.
- Do not remove the v1 path.

### Stage E — snapshot pinning and compaction
- Make snapshots/cursors pin roots/generations.
- Add generation retirement and safe reclamation.
- Test compaction while snapshots and cursors are alive, database disposal with live snapshots, and restart with orphan generations.

### Stage F — cutover and bounded-memory proof
- Route new partitions to v2; migrate v1 only at explicit checkpoint.
- Remove the monolithic dictionary and full SortedList path only after all compatibility and recovery gates pass.
- Measure managed heap, process RSS/working set, page faults and p95/p99 at 100k / 1M / larger representative cardinalities.
- Demonstrate the page cache and recovery memory budgets under stress. BenchmarkDotNet cumulative allocations alone are not a bounded-memory proof.

## 9. Non-goals and guardrails

- No change to BIN-SYNC delivery/ACK/causality semantics.
- No duplicate metadata journal or second pending-change history.
- No physical BIN-SYNC GC enablement as part of this Storage Core work.
- No multi-writer support; the existing single-writer partition fence remains.
- No compression, encryption, async prefetch or background compaction until baseline measurements show a need.
- Do not claim out-of-core or bounded-memory until the dictionary, full snapshot decode, full WAL materialization and unbounded snapshot index are all removed from the active path and measured.

## 10. Acceptance criteria

The paged index is not complete until:

- all existing Storage and FLD integration tests pass on Windows, Linux and macOS;
- v1 fixtures open and migrate without data loss;
- WAL fault-injection matrix proves atomic commit/root publication;
- concurrent readers retain stable snapshots through commits and compaction;
- cache size remains within its explicit budget plus documented fixed overhead;
- open/recovery peak heap does not grow linearly with total key/value bytes;
- point lookup, ordered scan and checkpoint performance are compared to the corrected baseline;
- old generation deletion is proven safe across active readers and process restart;
- docs and on-disk format spec are updated before enabling v2 by default.


### 2026-11-16 benchmark review — hot-path defect found and fixed

The first complete 30-case run (workflow 43875781259, code HEAD daf9dffcbbbabb76ddc475929bfbf458586e96a9) passed the verifier but exposed an implementation defect:
- 100k-entry prefix scan: 51.22 s and 2.097 GB allocated per operation.
- 100k-entry B+ tree point lookup: 1.135 ms and 24.9 KB allocated per operation.
- 100k-entry bulk build: 76.95 ms and 8.31 MB allocated per operation.

The prefix cursor decoded and allocated the entire current leaf on every TryReadNext, i.e. once per returned entry rather than once per leaf. Point lookup also decoded an entire leaf-entry array for a single key. Both hot paths have now been changed: cursor caches its decoded leaf until advancing to the next leaf; point lookup performs binary search directly in the validated leaf payload without allocating the full entry array. Internal-node traversal now decodes each page only once and bounds traversal depth to 64 to fail closed on cycles/corrupt trees.

These are baseline defect measurements, not acceptable target performance. Exact-head cross-platform CI and a fresh 30-case benchmark after the fix are mandatory. Production integration remains blocked.
