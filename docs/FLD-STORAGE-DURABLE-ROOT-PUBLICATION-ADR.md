# ADR: Durable B+ tree root publication and generation lifetime

Status: Accepted design; implementation not started
Date: 2026-11-16
Scope: FLD Storage physical index only. This ADR does not change BIN-SYNC delivery semantics or FLD tombstone policy.

## Context

The current `StorageDatabase` remains the authoritative v1 implementation: a managed Dictionary, a lazily built SortedList snapshot index, a v1 snapshot file, and WAL replay. Stage B supplies page-cache, generation-pin and positional-I/O primitives. Stage C supplies a standalone immutable B+ tree builder/reader. Neither is wired into production Storage.

The one-million-entry standalone scale gate passed (workflow 43906081415): 1,535 16 KiB pages, 259.4 ms bulk build, 126.1 ms full prefix scan, 124.6 MiB cumulative build allocations and 176.9 MiB process maximum RSS. The 10,000 point lookups took 326.5 ms; the harness currently allocates a byte array for every key-reference comparison. This is a known reader hot-path blocker, not an acceptable production target.

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

**Stage D.2 — streaming WAL recovery**
- Add a sequential validated WAL scanner and corruption/torn-tail tests.
- Prove recovery memory is bounded by one record/batch plus page/cache budgets.

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


## Stage C.1 implementation status — 2026-11-16

The borrowed-memory key reader, bounded per-reader hot reference cache, per-resident-page decoded view cache, bulk-builder no-copy path and leaf-boundary cursor validation are now implemented in the standalone prototype. The exact 1M-entry result is recorded in `FLD-STORAGE-PAGED-INDEX-DESIGN.md`; warm repeated 10k random lookups measured about 94 ns each, with a 45.9 ms warm scan over one million entries. This is promising but not a production acceptance result: the comparative 1k/10k/100k BenchmarkDotNet matrix at HEAD 8a78251 is pending, and a per-reader hot cache must become an aggregate database-level budget when snapshots are integrated.

Stage D remains blocked until the comparative matrix shows acceptable hot/random lookup and prefix-scan behavior. The durable manifest/WAL/crash protocol below remains design-only; no production Storage routing is authorized.
