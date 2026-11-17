# ADR: Durable B+ tree root publication and generation lifetime

Status: Accepted design; Stage D.1 durability primitives implemented behind internal APIs, not wired into production Storage
Date: 2026-11-16
Scope: FLD Storage physical index only. This ADR does not change BIN-SYNC delivery semantics or FLD tombstone policy.

## Context

The current `StorageDatabase` remains the authoritative v1 implementation: a managed Dictionary, a lazily built SortedList snapshot index, a v1 snapshot file, and WAL replay. Stage B supplies page-cache, generation-pin and positional-I/O primitives. Stage C supplies a standalone immutable B+ tree builder/reader. Neither is wired into production Storage.

The latest one-million-entry standalone scale gate passed at Stage C.1 HEAD (workflow 43913448959): 1,535 16 KiB pages, 171.9 ms build, 185.6 ms for the deterministic 10,000-lookup pass, 49.8 ms full scan, 48.3 MiB build allocations and 167.0 MiB process maximum RSS. This remains a standalone in-memory scale gate, not an on-disk durability or bounded-recovery proof.

## Decision

Use an immutable copy-on-write B+ tree with append-only key/value data records, one atomic manifest root, WAL as the only recovery journal, and explicit generation pin leases. Keep the current v1 path readable and authoritative until every cutover gate passes.

No second pending journal, change-history table, or duplicated recovery metadata is introduced.

### 1. Manifest v2 authority

Use a fixed-size, checksummed manifest with:
- magic and explicit format version;
- partition ID and generation ID;
- committed Storage CommitSeq;
- root page ID (zero only for an empty tree);
- committed page-file length and data-file length;
- page-size/format flags and reserved-zero bytes;
- CRC32C over the complete manifest body.

The manifest is the sole durable root authority. File tails beyond committed lengths are uncommitted orphans and must never be traversed. A manifest that references a missing, short, mismatched, or corrupt page/data file fails closed.

Do not overload the existing v1 manifest layout. Open dispatches by magic/version; v1 parsing remains unchanged. Older binaries must reject v2 explicitly rather than misread it.

### 2. Commit protocol

A successful write batch follows this exact ordering:

1. Validate and encode the existing atomic WAL batch.
2. Append the WAL record and flush it to stable storage.
3. Append immutable key/value records and copy-on-write index pages for the complete batch.
4. Flush data and page files.
5. Write a complete temporary v2 manifest and flush it.
6. Atomically replace the manifest and flush the containing directory.
7. Publish the new in-memory root/commit sequence.
8. Reclaim only WAL segments fully covered by the durable manifest.

The WAL remains the recovery authority until step 6 completes. A failed operation faults the open instance; recovery/reconciliation occurs only on reopen.

### 3. Crash interpretation

| Crash boundary | Authoritative state after reopen |
|---|---|
| Before WAL flush | Previous manifest/root; torn WAL tail is handled by existing WAL recovery rules |
| After WAL flush, before data/page completion | Previous manifest/root plus replay of the complete WAL batch |
| After data/page flush, before manifest replacement | Previous manifest/root plus WAL replay; appended file tails are unreachable orphans |
| After manifest replacement and directory flush | New manifest/root; WAL records at or below its CommitSeq are already represented |
| During/after WAL truncation | New manifest/root remains authoritative; only fully covered sealed segments may be absent |

Never infer commit from file length, a page ID, or an unreferenced root candidate. The manifest replacement is the commit-publication boundary after the WAL durability boundary.

### 4. Recovery must become streaming

Current `WalManager.Open` collects all recovered records into a List and `WalRecovery.Recover` materializes each segment's records. This is incompatible with a bounded-memory recovery claim.

Before production routing:
- introduce a validated sequential WAL record visitor/reader that preserves segment continuity, torn-tail policy and CommitSeq checks;
- expose at most one decoded atomic batch (or a configured bounded batch window) to Storage at a time;
- apply each complete batch copy-on-write;
- publish a new manifest root at complete batch boundaries during recovery, so a second crash resumes from the last published CommitSeq;
- do not truncate WAL until the manifest durably covers the relevant CommitSeq.

Do not publish a root for a partial WAL batch. The existing materializing API can remain for legacy callers/tests until all consumers migrate, but it must not sit on the v2 recovery path.

### 5. Snapshot and generation lifetime

A snapshot captures immutable `(generation, rootPageId, commitSeq)` and owns one generation pin. A prefix cursor either owns its own pin or is explicitly scoped to a snapshot whose lifetime is guaranteed for the cursor; do not rely on caller discipline without an API-enforced lifetime.

Compaction:
1. Build a new generation from an ordered traversal of the currently published root.
2. Flush all new data/pages.
3. Atomically publish a manifest pointing at the new generation/root.
4. Mark the old generation retired, rejecting new pins.
5. Delete old generation files only after every snapshot, cursor and in-flight read lease has released its pin.

A process-local pin registry is sufficient only while Storage guarantees there are no cross-process readers. If read-only multi-process opens are later introduced, generation reclamation requires a cross-process lease protocol; the existing single-writer fence does not by itself protect external readers.

### 6. Required deterministic fault points and tests

Add fault injection around:
- WAL append/flush;
- partial and complete data-record writes;
- partial and complete page writes;
- data/page flush;
- temporary manifest write/flush;
- manifest atomic replace;
- containing-directory flush;
- in-memory root publication;
- WAL segment reclamation;
- generation retirement and deletion.

For each boundary, run a child process that exits/crashes, reopen the database, and assert exact key/value state, CommitSeq continuity, manifest/file-length validity, and no exposure of uncommitted pages. Include repeated crashes during recovery. Test empty roots, root growth, split boundaries, tombstones, 1 MiB keys, large values, torn WAL tails, and orphan generation cleanup.

### 7. Implementation sequence and release gates

**Stage C.1 — reader performance prerequisite**
- Replace per-comparison byte-array key materialization with allocation-free comparison over a borrowed-memory or accessor abstraction.
- Define and test the exact lifetime of borrowed key bytes; cursor/snapshot generation pins must outlive every comparison.
- Re-run 1M scale and 1k/10k/100k comparative matrix. No production work proceeds if warm point lookup regresses materially against the current Storage baseline.

**Stage D.1 — codecs and files, still standalone**
- Implement manifest v2 codec and append-only data/page file abstractions.
- Add strict committed-length, identity, CRC and reserved-field validation.
- Keep v1 StorageDatabase routing unchanged.

**Stage D.2 — streaming WAL recovery — implemented 2026-11-17**
- Add a sequential validated WAL scanner and corruption/torn-tail tests.
- Prove recovery memory is bounded by one record/batch plus page/cache budgets.
- The active StorageDatabase open path now uses a two-pass recovery plan: first validate segment continuity and torn-tail boundaries while retaining only segment summaries; then replay one decoded WAL record at a time after loading the snapshot.
- Sealed-segment inspection during checkpoint also uses the streaming scanner. The compatibility WalManager.Open API still materializes records for existing internal callers/tests.
- The Storage dictionary and v1 snapshot remain fully materialized; this stage removes the duplicate all-WAL-record collection only. It does not yet prove whole-database bounded-memory recovery.

**Stage D.3 — root publication transaction**
- Implement copy-on-write batch application and manifest root publication behind an internal/test-only path.
- Add the full crash/reopen fault matrix before any public opt-in.

**Stage E — snapshot pins and compaction**
- Connect snapshot/cursor lifetime to generation pins.
- Add compaction, retirement, deferred deletion and crash cleanup tests.

**Stage F — migration and cutover**
- Read v1 unchanged; migrate only at explicit checkpoint.
- Keep old v1 data until v2 publication is durable and no legacy handle can refer to it.
- Enable v2 for new partitions only after all Windows/Linux/macOS tests, 1M+ performance/RSS tests, recovery fault tests and migration fixtures pass.

## Explicit non-authorization

This ADR does not authorize production B+ tree routing, v1 mirror deletion, WAL format changes, physical generation deletion, or BIN-SYNC GC activation. Those require the gates above and a separate reviewed implementation result.


## Stage C.1 implementation status — 2026-11-17

The borrowed-memory key reader, bounded per-reader hot reference cache, per-resident-page decoded view cache, bulk-builder no-copy path and leaf-boundary cursor validation are implemented in the standalone prototype. The exact-head comparative BenchmarkDotNet matrix passed (workflow 43913464032):

| Cardinality | Storage warm point | B+ tree warm point | Storage prefix scan | B+ tree prefix scan |
|---:|---:|---:|---:|---:|
| 1k | 53.09 ns | 18.02 ns | 6.411 µs | 9.457 µs |
| 10k | 46.06 ns | 17.97 ns | 65.695 µs | 62.806 µs |
| 100k | 44.89 ns | 28.65 ns | 676.561 µs | 594.368 µs |

The 1k prefix scan regresses; the 100k B+ tree prefix scan is about 12.1% faster. The B+ tree random lookup and cache-pressure paths measured 1.177 µs and 2.565 µs respectively. These are benchmark-fixture results, not production on-disk measurements. The per-reader hot cache must become an aggregate database-level budget when snapshots are integrated.

The latest 1M scale gate (workflow 43913448959) passed: 1,000,000 entries, 1,535 pages, 171.9 ms build, 185.6 ms deterministic lookup pass, 49.8 ms scan, 48.3 MiB allocations and 167.0 MiB maximum RSS. Stage C.1 is complete as a standalone prototype only; Stage D is now active. Production routing remains unauthorized.


## Stage D.1 implementation record — 2026-11-17

The first durability implementation block is committed as standalone infrastructure only. The manifest layout is 80 bytes: FLD2 magic, version/header size, partition/generation/CommitSeq/root page, committed page/data lengths, page size, zero-only flags/reserved bytes, and CRC32C. FLDM remains recognized as the legacy v1 manifest and is never decoded using the v2 layout.

Generation files use sequential page IDs and append-only data offsets. Reads require the caller to supply the manifest's committed high-water mark; physical file tails cannot become visible merely because bytes exist. TruncateToCommittedLengths is an explicit recovery operation and its contract requires WAL continuity/replay validation first.

No production code calls these primitives yet. No v2 manifest is published, no WAL segment is reclaimed on their authority, and no v1 snapshot or mirror is removed. Windows/Linux/macOS CI is the acceptance gate for this block; passing unit tests does not establish power-loss durability on every filesystem.


### Stage D.1 exact-head verification — 2026-11-17

The standalone manifest/file primitive block passed workflow 43984533449 on Ubuntu, macOS 15 and Windows: 322 tests passed on each platform, with zero failures or skips. The independent 1M B+ tree scale gate on the same code lineage (workflow 43984502223) also passed. These results validate the isolated codec/file primitives and existing regression suites only; no production root publication, streaming WAL replay, snapshot pin integration or generation reclamation has been enabled.


### Stage D.2 implementation record — 2026-11-17

WAL recovery now has a streaming visitor that validates the same header, envelope CRC, record CRC, padding, sequence order, corruption and torn-tail rules as the materializing reader. It retains one decoded record payload at a time. WalManager performs a validation pass over all segments, records only per-segment sequence/count/end-offset summaries, then returns a recovery plan that re-scans and replays records synchronously. The second pass verifies that the segment summaries still match the validated pass. StorageDatabase.Open now applies snapshot state first and then replays through this plan; sealed-segment checkpoint inspection also avoids materializing a record list.

The compatibility WalManager.Open and WalRecovery.Recover methods remain available internally and retain their materializing behavior. The production StorageDatabase path no longer uses them. This does not remove the v1 Dictionary or SortedList materialization and therefore is not a bounded-memory database-open claim. Next: D.3 copy-on-write batch application and durable root publication, with the full flush/replace/crash matrix.
