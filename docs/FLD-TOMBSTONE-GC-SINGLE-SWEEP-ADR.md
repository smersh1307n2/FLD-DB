# FLD Tombstone GC: single-sweep bounded-memory compaction

**Status:** Accepted implementation direction; implementation and validation pending  
**Date:** 2026-11-16  
**Scope:** FLD Database physical compaction for protocol-authorized field and record tombstone reclamation  
**Related:** `SYNC-STORAGE-COMPACTION-ROADMAP.md`, `FLD-STORAGE-CORE-ARCHITECTURE.md`, `FLD-STORAGE-PHYSICAL-FORMAT-ADR.md`, BIN-SYNC `BIN-SYNC-SPECIFICATION.txt`, `BIN-SYNC-V3.1-HOST-ORCHESTRATION.md`

## 1. Decision

Replace the current tombstone-GC implementation that applies a candidate-count limit to a complete namespace rewrite on every invocation.

The target is **one logical namespace sweep, one source-generation scan, bounded working memory, chunked immutable Run output, and one Manifest publication**. The candidate predicate remains the final authority for each candidate. The GC implementation must not weaken, approximate, cache beyond its valid lease, or replace the existing signed retention and Host reference proofs.

The output writer may emit multiple sorted FLD1 Runs to keep each in-memory build buffer bounded. Those Runs collectively represent one replacement generation and become visible together through the existing Manifest publication point.

This is a physical compaction change only. It does not change BIN-SYNC wire formats, NRS2/OFR2/RCP2/FRA2 semantics, ChangeId identity, peer ACK rules, tombstone meaning, Storage opacity, or the two-runtime-DLL boundary.

## 2. Evidence and current defect

At the current FLD implementation baseline, `FldDatabase.CompactTombstones` creates a `TombstoneGcFilter` with a candidate limit and calls `CompactUnderLock`. The latter:

1. Builds a namespace-wide `Dictionary<(RecordId), FieldIdSet>` and a complete RecordId list from the overlay and every Run.
2. Resolves current fields and lifecycle state for every record.
3. Builds a complete replacement Run in one `RunBuilder`.
4. Writes the replacement and publishes a new Manifest.
5. Retires the old namespace Runs.

The filter stops accepting candidates after `limit`, but it does not stop the full scan or rewrite. Repeating the bounded call therefore repeats namespace-wide work.

The recorded 250k FLD-only benchmark in BIN-SYNC Roadmap §1.165 measured approximately:

- repeated 4,096-candidate calls: 28.254 s and 9.20 GB cumulative managed allocations;
- one full rewrite: 248.692 ms and 87.94 MB cumulative managed allocations.

A later rerun in §1.169 measured 34.037 s / 9.21 GB versus 266.557 ms / 87.94 MB. These are cumulative managed allocations, not peak live heap or process RSS. The comparison nevertheless demonstrates the algorithmic defect: the repeated path performs roughly one full namespace rewrite per candidate page.

## 3. Required algorithm

### 3.1 Stable source view and exclusion

- Enter through the existing FLD commit/maintenance synchronization boundary. Hold the composed Host application guard, Metadata writer-admission/maintenance lease, and FLD Host IPC commit lease for the entire logical tombstone sweep, through the final Manifest publication or abort.
- Under the FLD database commit gate, capture the exact namespace Manifest generation, ordered source Run IDs, and sealed commit/WAL boundary. The overlay must be empty and the source view must be immutable for the operation.
- Existing provider snapshot pins and reference-scan rules remain global vetoes where currently specified. Do not reclaim a candidate while a relevant immutable snapshot pin exists.
- No new writer may mutate the source namespace between source capture and publication. The initial implementation deliberately prefers this simple, auditable exclusion model over a multi-invocation resumable stage.
- If the required lease/gate cannot be acquired, the operation does no physical work and reports a blocked result. If cancellation, I/O failure, proof failure, or source-generation mismatch occurs before publication, abandon the unpublished output and leave the old Manifest authoritative.

### 3.2 One ordered merge, not a namespace-wide index

- Add sequential per-Run cursors over the existing canonical RecordId slot order (including use of `RunReader.TryReadRecordIdAt` where suitable).
- Perform a k-way ordered merge of source Run RecordIds. Process one RecordId at a time; collect only that record's bounded FieldId/resolution state from the Runs that contain it.
- Resolve the winning field value, tombstone, HLC, author, ChangeId and immutable Origin using the same comparator and semantics as ordinary FLD reads/compaction.
- Evaluate record tombstone and field tombstone candidates exactly once against the supplied authoritative predicate. Preserve the existing rule that a record tombstone which is not reclaimable prevents removal of its older field state; preserve newer-than-record-delete behavior.
- Do not create a namespace-wide RecordId dictionary, a complete RecordId list, or a complete output-record list.
- Complexity target: one sequential visit to each source Run's record slots plus bounded per-record merge/resolve work. The implementation must report counters that make repeated full scans detectable.

A single record can itself contain many fields. Its temporary state is bounded by the existing FLD record/Run format limits, not by total namespace cardinality; this worst case must be measured and documented.

### 3.3 Bounded output generation

- Replace the one-namespace `RunBuilder.Build()` output with a chunked writer that holds at most one target-sized Run build buffer plus the current record's bounded state.
- Flush a completed chunk as a new immutable FLD1 Run; each Run must pass the existing structural verifier, namespace check, canonical ordering and provenance validation before it can be referenced by the Manifest.
- Preserve exact winning field bytes, tombstone flags, HLC, author, KeyEpoch, ChangeId and Origin. No renumbering or synthesized identity is permitted.
- Keep Run ID allocation root-global through `AllocateRunId()`. Publish blob pages before the compatibility mirror and before the Manifest, following the existing FLD physical-format ADR.
- The chunk target is an implementation/benchmark parameter, not a semantic page limit. It must be chosen from measured allocation, write amplification and lookup/read behavior.
- An empty output generation is valid: the single Manifest update removes this namespace's old Run entries without adding a replacement Run.

### 3.4 Single publication and reclamation

- Validate all output Runs and their complete ordered coverage before publication.
- Revalidate the captured source generation and the proof/lease binding while the composed guard is still held.
- Perform exactly one Manifest update that replaces only the target namespace's Run set. Preserve other namespaces, WAL, Outbox, ApplyJournal, root-global sequence state and provider snapshot semantics.
- The Manifest is the only visibility switch. No partially built output Run may become visible through ordinary reads.
- Retire old Runs only after successful publication and existing snapshot-pin/lifetime rules permit it. Cleanup remains retryable after restart; failure to remove an old file/blob must not roll back or corrupt the committed generation.

## 4. Crash, cancellation and recovery model

This first implementation is a single logical operation, not a resumable multi-call cursor protocol.

- Before Manifest publication, all newly written Runs are unpublished output. A crash or cancellation leaves the old Manifest authoritative; existing orphan-Run/blob recovery must reclaim the unreferenced output without deleting a live or FULL_REPLACE-staged Run.
- After Manifest publication, the new generation is authoritative. A crash during old-Run cleanup is handled by existing orphan cleanup on reopen/maintenance.
- Fault injection is required after each output Run becomes durable, after all output Runs validate, immediately before Manifest publication, immediately after Manifest publication, and during old-Run cleanup.
- The operation must never report successful reclamation before the Manifest switch succeeds.
- Cancellation is observed between record groups and output chunks. Once Manifest publication starts, complete or fail according to the existing manifest transaction semantics; do not leave an ambiguous half-publication.
- Windows directory-entry durability is not upgraded by this ADR. The existing explicit Windows NTFS power-loss release gate remains open.

If future measurements show that holding the composed lease for one full sweep is operationally unacceptable, a separately reviewed resumable-generation design may be proposed. Such a design must persist a source-generation identity and progress, pin the source generation, bind an immutable proof/reference snapshot or safely revalidate every previously accepted candidate, and abort on any source/proof/reference-generation change. It must not be added by simply releasing the current leases between pages.

## 5. API and Host semantics

- The current `limit` means maximum number of reclaimed candidates, not maximum scan/read/write work. It must not remain the apparent bound for this new operation.
- Replace the tombstone maintenance contract with an explicit whole-sweep operation/result. The result should distinguish completed, blocked, cancelled and failed outcomes and include reclaimed candidate count plus internal work counters.
- Remove or migrate every caller that loops `CompactTombstones(limit)` until no more candidates. There must be no compatibility shim that silently repeats full sweeps.
- Receipt and ChangeId Metadata compactors retain their own independent bounded-page/cursor semantics. Do not change their limits or combine their retention domains with FLD tombstone compaction.
- Host must acquire the exact Tombstone domain durable maintenance lease and hold it, together with the composed application guard, through the entire FLD sweep. Emergency Stop/cancellation must prevent publication if it arrives before the publication point.
- Keep all physical-GC feature switches false by default. Ghost GC remains excluded.

## 6. Required tests

### Correctness and semantics
- Mixed Run v1/v2 and provenance preservation, multiple namespaces, empty namespace output, field tombstones, record lifecycle Dead/Alive, LWW winners/losers, record-delete/newer-field ordering.
- Candidate predicate called once per eligible candidate; candidates beyond any former page boundary are not skipped.
- No safe candidate: no Manifest publication and no false reclaimed count.
- Source-generation, snapshot-pin, Host reference, outstanding reservation, pending apply and signed-proof blockers remain fail-closed.

### Work bound and memory
- For a sweep over N source records, assert one visit per source Run slot/record cursor (allowing documented k-way comparison overhead), one logical resolution per stored key as designed, one Manifest publication, and no namespace-sized RecordId/FieldId aggregate.
- Instrument source records/bytes read, output bytes/Run count, candidate checks, allocations and sweep duration.
- Test cardinalities 1k, 10k, 50k, 100k, 250k and 1M. Include high Run-count and large-value/high-field-count cases.
- Measure cumulative allocations separately from peak managed heap and OS process peak RSS/working set. Never label cumulative allocations as peak RAM.
- Regression threshold must reject the previous O(number-of-candidate-pages × namespace-size) behavior.

### Crash and concurrency
- Inject failure at every output/manifest/cleanup boundary listed above; reopen and prove either the complete old generation or the complete new generation is visible.
- Prove competing Host IPC writers, provider mutations, Metadata writers and policy updates remain excluded for the complete sweep and are admitted after release.
- Prove snapshot slices remain valid and old Run/blob reclamation waits for all existing pins.
- Prove Emergency Stop/cancellation before publication leaves old generation authoritative.

## 7. Performance acceptance

The full sweep must scale approximately with one source scan plus one output write, not with the number of candidate pages multiplied by namespace size.

Report separately:
- elapsed time;
- cumulative managed allocations;
- peak managed heap;
- OS-observed process peak RSS/working set and sampling limitations;
- source bytes/records visited;
- output bytes and Run count;
- Manifest publication count;
- candidate checks and reclaimed count;
- lock/Host-lease duration.

Benchmark both FLD-only and the real signed-proof + Metadata + Host IPC reference-scan + `FldBinSyncAsyncHostAdapter` path. The composed benchmark must use the real reference scanner, not a delegate/no-op substitute.

The prior single-rewrite benchmark is a useful baseline, not an acceptance proof. Do not enable any production physical-GC switch based on the old benchmark.

## 8. Non-goals and invariants

- No BIN-SYNC protocol or signed-proof redesign.
- No TTL, LRU, wall-clock, single-peer-ACK or transport-ACK deletion authority.
- No duplicate journal, pending table, per-peer payload copy or new Storage tombstone semantics.
- No change to FLD1 Run v1/v2 encoding or two-DLL split.
- No Ghost GC.
- No production GC enablement.

## 9. Implementation order

1. Add internal ordered Run cursors and a chunked Run output writer; preserve existing Run validation and provenance.
2. Implement one-pass merge/resolve/filter with internal work counters; do not route production calls to it until tests pass.
3. Integrate one Manifest publication, root-global Run allocation, old-generation pinning and orphan recovery.
4. Replace the old candidate-page Host/API loop and migrate all callers.
5. Add adversarial semantic, crash, cancellation, writer-exclusion and complexity tests.
6. Add FLD-only and composed Host/Metadata benchmarks.
7. Run exact-head CI and all benchmark gates; leave every physical-GC switch default-off.

