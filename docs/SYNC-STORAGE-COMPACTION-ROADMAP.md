# Sync Storage Compaction Roadmap — FLD-DB + BIN-SYNC

**Date:** 2026-11-07  
**Status:** BIN-SYNC metadata P1 (current NodePersistence/Node implementation) and FLD remote APPLY P2 slices are complete on their working branches without a DELTA_BATCH wire-format change. This is not yet a completed neutral Metadata Store integration. Remaining cross-repository work: executable Metadata Store conformance fixture, Data Provider boundary, BIN-SYNC persistence refactor, FLD two-DLL Storage Core extraction and Storage-only role, then the Storage-only and synchronized-Database adapters. PendingLocal coalescing, FLD Outbox physical encoding/benchmarks, and protocol-safe receipt/dedup/tombstone reclamation remain separate follow-on gates.  
**Primary audit:** [Sync History Storage Audit](SYNC-HISTORY-STORAGE-AUDIT.md).  
**Tombstone research:** [Tombstone Retention and Safe Garbage Collection](TOMBSTONE-RETENTION-RESEARCH.md) — Microsoft ForgottenKnowledge, CRDT causal stability/commitment, Cassandra, CouchDB and Couchbase.  
**Companion documents:** [FLD Storage Core](FLD-STORAGE-CORE-ARCHITECTURE.md), [BIN-SYNC Metadata Storage Architecture](https://github.com/smersh1307n2/BIN-SYNC/blob/feature/fld-external-storage/docs/METADATA-STORAGE-ARCHITECTURE.md), [Metadata Store Semantic Contract and Conformance Plan](https://github.com/smersh1307n2/BIN-SYNC/blob/feature/fld-external-storage/docs/METADATA-STORE-CONFORMANCE.md), [Change Staging and Retention](https://github.com/smersh1307n2/BIN-SYNC/blob/feature/fld-external-storage/docs/CHANGE-STAGING-AND-RETENTION.md), [FLD-DB Integration Contract](https://github.com/smersh1307n2/BIN-SYNC/blob/feature/fld-external-storage/docs/FLD-DB-INTEGRATION-CONTRACT.md).

## 1. Goal

Build a compact, crash-safe storage model without copying another synchronization framework's architecture. Optimize the complete lifecycle, not isolated record sizes.

- Store each logical published operation payload once per local replica.
- Never create per-peer payload copies or per-operation ACK bitmaps.
- Retain only metadata needed for identity, retry, ordering, relay and recovery.
- Coalesce only while an operation is provably unpublished.
- Give every metadata category an explicit independent reclamation condition.
- Preserve the current wire contract unless a separately reviewed protocol change is proven necessary.

A physical store may combine records with the same transaction boundary, but logical ownership and retention remain separate.

## 2. Target decisions

### 2.1 Logical data planes

1. **Current state:** one current winner per canonical field key, plus lifecycle/tombstone data required by merge and digest semantics. No duplicate full ChangeId history in current-state rows.
2. **Pending local staging:** at most one latest pending LWW operation per canonical field key while it is guaranteed unpublished. Its payload survives independently of the current visible winner.
3. **Published operation payload:** immutable after freeze/publication. One payload copy per unique ChangeId retained for outbound delivery or relay.
4. **Protocol metadata:** compact receipts, dedup/provenance facts, peer/author progress and replay checkpoints. Never retain full field payloads solely to answer duplicate batch retries.

Remote relay operations remain distinct from local pending staging. Preserve relay-eligible LWW losers. Do not coalesce remote relay history as unpublished local state.

### 2.2 Metadata cardinality and lifetime

| Logical fact | Cardinality/key | Minimum retained data | Reclamation condition |
|---|---|---|---|
| Current field state | scope/namespace/record/field | winner, value/tombstone, HLC, author, epoch/lifecycle | replaced by valid newer state and physical compaction |
| Pending local LWW | one per coalescible field key | latest unpublished operation and payload/reference | atomically frozen or safely superseded |
| Published local operation | one per ChangeId/author position | immutable identity and payload | all applicable peers pass safe contiguous ACK |
| Relay operation | one per relay-eligible ChangeId | original identity/provenance and payload | validated downstream safe watermark |
| Global ChangeId ledger | one per replica/scope/namespace/ChangeId | canonical fingerprint and conflict facts | protocol-safe dedup horizon; conservative retention until defined |
| Batch receipt | one per issuer/scope/namespace/epoch/BatchId | cursor range, author, digest/RecordsRoot, outcome and commit marker needed for retry ACK | explicit Inbox/retry horizon |
| Peer progress | one per peer/scope/namespace/author | pushed/acked/offset and digest checkpoint | fenced peer removal/reset |
| Outgoing replay cache | one per unacknowledged peer/author/cursor range | BatchId and previous digest needed for deterministic retry | validated contiguous ACK covers range |
| WAL replay marker | per local commit/replay boundary | commit identity and recovery facts | WAL replay horizon closes |
| Relay selection | compact indexes into accepted batch | bitmap/index list, not duplicate payload | relay payload promotion is durable |

These are logical records, not a mandate for separate files/tables. Co-locate only when atomicity and lifecycle align.

### 2.3 One operation, many peers

BIN-SYNC already has the correct high-level shape: one shared namespace Outbox and peer-specific per-author cursors. Preserve it. No change-by-peer delivery table or payload copies.

Peer progress remains approximately O(P × A); operation payload storage remains O(C), not O(C × P).

### 2.4 Identity stays conservative

Keep the current immutable 128-bit ChangeId and canonical operation fingerprint in the global dedup ledger. Same ID + same fingerprint is a duplicate regardless of route; same ID + different fingerprint is a protocol conflict.

Do not substitute current relay-local Seq, FLD OutboxSeq or WAL CommitSeq for original operation identity. Content-derived ChangeId and OriginSeq are research gates, not approved changes.

### 2.5 Publication is the immutability boundary

A local LWW operation may be replaced only while no sender could have exposed it to any peer. Freeze atomically assigns stable ChangeId/stream sequence, preserves payload and removes/moves pending staging. After freeze, retries send the same identity and bytes even if an ACK is lost.

Existing in-memory dirty-set coalescing before one Commit remains valid. Durable coalescing across multiple commits is separate work; never rewrite a frozen Outbox tail.

### 2.6 Two-DLL database architecture (binding constraint)

The FLD database runtime is deliberately split into **exactly two runtime DLLs**. This is an architectural constraint for the storage work, not an optional packaging optimization:

1. **`Fld1.Storage.dll`** — standalone, domain-neutral byte-oriented embedded storage engine: WAL, atomic byte batches, recovery, pages/runs, manifest, snapshots, checkpoints, ordered scans and low-level reclamation. It must not know FLD records/fields, HLC/LWW, tombstones, ChangeId, Outbox, ApplyJournal or BIN-SYNC protocol types.
2. **`Fld1.Database.dll`** — FLD database layer built on Storage: `FldDatabase`, records/fields, HLC/LWW, tombstones, field codecs, indexes/projections and FLD-specific Outbox/APPLY integration. BIN-SYNC integration is performed at this semantic layer or through its adapter boundary; protocol semantics must not leak into Storage.

Dependency direction is strictly one-way: **`Fld1.Database.dll → Fld1.Storage.dll`**. Storage has no dependency on Database. Do not introduce a third Common/Shared runtime DLL. Consolidate existing modules into these two distributable assemblies; classify each type by ownership rather than moving whole assemblies blindly. In particular, an existing FLD-aware Run implementation must not be moved wholesale into Storage: generic byte-KV/run primitives belong in Storage, while field-aware encoding/projection belongs in Database.

Both DLLs are separately buildable artifacts. They may be delivered together in one convenient package, but a Storage-only consumer must not acquire Database as a transitive runtime dependency. This split is independent of the logical metadata lifecycle described above and must remain true during all compaction phases.

## 3. Implementation roadmap

### Phase 0 — Contracts and baseline measurements

- Preserve the two-DLL boundary while inventorying current modules and assigning every type to Storage or Database; do not create a third Common/Shared runtime assembly.
- Count disk and RAM separately: WAL, local Outbox, APPLY WAL, ApplyJournal, relay payload, receipts, dedup indexes, PeerState, replay cache, current-state runs and sidecars.
- Establish a reproducible workload: 1,000 records × 20 fields, 10 peers, 100,000 changes.
- Include repeated local writes before publication and across commits, remote batches, multi-hop relay, LWW winners/losers, tombstones, lost ACK/retry, peer lag/removal.
- Report steady-state bytes, transient WAL/checkpoint amplification, unacknowledged backlog, RAM/index cost, compaction CPU and recovery time separately.
- Compare current implementation, each optimization and target. Any Sync Framework comparison must use equivalent retention semantics.

**Exit:** reproducible baseline; every counted byte has an owner and retention rule; estimates are not presented as measurements.

### Phase 1 — BIN-SYNC correctness and metadata lifecycle — implementation complete, GC open

**1.1 Global cross-peer ChangeId dedup**
- Add a replica/scope/namespace-scoped ChangeId ledger with canonical operation fingerprint.
- Sender is not part of logical operation identity.
- Keep issuer-specific batch receipts separate; keep local WAL replay markers separate.
- Test same ChangeId/same content through alternate routes, including after first relay payload compaction; reject same ID with conflicting content.

**1.2 Replay-cache pruning**
- In the same durable ACK transition, remove PeerState.Ids entries only when validated contiguous ACK covers cursorEnd.
- Preserve the digest-chain checkpoint. Never prune on pushed watermark or non-contiguous ACK.

**1.3 Receipt consolidation**
- Trace Inbox, InboxAuthors and ReceivedStreams through all retry/ACK paths.
- Define one compact metadata-only receipt per accepted batch.
- Remove overlapping facts only after duplicate replay and ACK tests prove equivalence.
- Receipt lifetime remains independent from dedup and relay payload lifetimes.

Implemented on `smersh1307n2/BIN-SYNC` `feature/fld-external-storage`:
- Route-independent BLAKE3-128 ChangeId fingerprint ledger; alternate-route duplicates suppressed and same-ID equivocation rejected before WAL/state publication.
- Local WAL replay markers remain a separate lifetime; remote peer-scoped SeenChanges rows removed.
- ACK-covered replay mappings pruned only after validated contiguous author ACK; digest-chain checkpoint preserved.
- Inbox + InboxAuthors consolidated into one receipt map; v8 header commitment preserves duplicate ACK/cursor semantics.
- PeerAuthorProgress combines ACK/pushed/offset watermarks; ReceivedStreams key omits only the redundant namespace component.
- NodePersistence v10 groups receipt and global-ledger scope keys; v11 removes repeated local NodeId from WAL replay marker keys; v12 adaptively selects flat/author-grouped receipt encoding. v3-v11 snapshots remain readable.
- The in-memory Inbox receipt index is adaptive (singleton map promoted to author/session groups), and global ChangeLedgerValue is reduced to one 16-byte Digest16 with zero reserved for unknown legacy markers.
- BIN-SYNC exposes the public per-author safe Outbox ACK watermark and retained (author, ChangeId) → OutboxSeq resolver. The durable FLD JournalSeq provenance map and atomic scan-cursor integration remain adapter work.
- Resource caps cover global dedup, Inbox receipts, local WAL markers, Outbox and records; caps are opt-in through NodeOptions.Limits.
- Tombstone age-only deletion is disabled fail-closed until a durable safe watermark/full-resync gate exists.

**Remaining Phase 1 gate:** receipt and ChangeId-ledger GC horizons are not yet proven. No TTL or destructive eviction is enabled. This is a retention-protocol gate, not a reason to weaken the implemented dedup/receipt semantics.

### Phase 2 — FLD remote APPLY payload amplification — core implementation complete, retention/index GC open

**2.1 Single APPLY payload — implemented**
- APPLY_COMMIT body v3 stores every incoming operation once and represents relay selection as canonical varint indexes into Fields.
- The decoder remains backward-compatible with v2 payloads, validates that relay indexes are unique/in-range and reconstructs RelayFields as references to the original decoded field objects.
- Relay-only journal records use a compact flag and store the selected operations once, without a second index list.

**2.2 Metadata receipt plus relay-only materialization — implemented; per-author relay payload GC implemented, receipt GC open**
- ApplyJournal now stores batch receipt metadata and only relay-selected operations. Non-relay field payloads are not copied into the journal.
- The full APPLY WAL record is flushed first; the compact journal materialization is then flushed before ApplyRemoteBatch returns. This makes relay payload durable before a later checkpoint can truncate the WAL.
- Journal materialization is idempotent by batch identity and compares canonical receipt metadata plus relay payload. WAL recovery rebuilds the same compact journal representation from both v2 and v3 APPLY_COMMIT records.
- Relay-eligible LWW losers remain in the relay-only materialization.
- Receipt metadata and relay payload remain in one journal record/file, but relay fields are now independently reclaimable per original author. A CRC-protected per-author safe JournalSeq sidecar is persisted before reclamation.
- After the APPLY WAL prefix is sealed and truncated, `CompactAcknowledgedRelayPayloads` atomically rewrites the journal, removing only relay fields covered by that author's safe watermark while preserving receipt metadata and every other author's payload. The receipt record remains for duplicate ACK handling.
- Recovery checks the sealed WAL boundary before attempting to rematerialize a batch already present in the compacted journal; this prevents a full pre-compaction WAL payload from resurrecting ACKed relay fields. If the journal receipt is missing, WAL recovery rebuilds it.
- Compaction currently rewrites the full journal (O(N) I/O); receipt rows and the BatchKey index remain unbounded. No receipt GC is implemented.

**2.3 Bounded indexes and seekable paging — partially implemented**
- Implemented a sparse in-memory anchor every 256 JournalSeq records. ReadBatches binary-searches the anchors and scans at most one bounded stride before reading the requested page; it no longer retains a full IndexEntry List or rescans the entire consumed prefix.
- The BatchKey→offset Dictionary and on-disk receipt history still grow with every batch; a disk-backed/rebuildable batch index and safe receipt GC remain open.
- `ReadRelayBatchPage` now returns `ScannedThroughJournalSeq`, including ranges with no matching author operations. The adapter must persist this scan cursor atomically with all returned relay operations; it is not a safe ACK watermark.
- Before relay payload GC, persist source provenance mapping (author, JournalSeq, ChangeId → BIN-SYNC OutboxSeq) across restart. BIN-SYNC now exposes the per-author minimum safe ACK query and ChangeId→OutboxSeq resolver; the adapter must still persist the mapping and scan cursor atomically.
- The adapter must gate all outbound publication for a namespace until provenance pages are durable, including after restart. It must translate safe OutboxSeq to the first contiguous safe source JournalSeq, persist FLD relay ACK before pruning mappings, and fail closed if a retained identity cannot be resolved. Do not hold FLD/Node internal locks across I/O; use an adapter-level gate. Receipt expiry remains independent.

**Implemented on `smersh1307n2/FLD-DB` `fix/crash-consistency-outbox`:**
- APPLY_COMMIT v3 encodes the incoming operation array once; relay selection uses canonical indexes; v2 remains readable.
- ApplyJournal stores metadata receipt plus only relay-selected operations in a relay-only form, flushed after WAL and before ApplyRemoteBatch returns.
- Promotion is idempotent; WAL recovery rebuilds the relay journal; LWW-losing relay-eligible operations are preserved.
- ReadBatches uses sparse JournalSeq anchors (stride 256) and bounded scan; full IndexEntry List removed.
- BatchKey index values are 64-bit offsets, not duplicated BatchKey+IndexEntry structs.
- ReadRelayBatchPage reports ScannedThroughJournalSeq across nonmatching author ranges. Adapter must persist this scan cursor atomically with relay operations; it is not an ACK watermark. The adapter publication gate and ACK translation order are specified in BIN-SYNC's FLD integration contract §3.1.2.
- A per-author relay ACK sidecar records safe-through JournalSeq. Checkpoint/reopen compaction physically reclaims acknowledged author payloads while retaining receipts and other authors' payload.

**Remaining Phase 2 gate:** relay payload reclamation is implemented per author, but receipt retention remains unbounded and shares the journal's physical record envelope. The BatchKey→offset dictionary still has one in-memory entry per receipt; disk-backed/rebuildable bounded indexing and independent receipt GC remain open. Full-file rewrite cost must be benchmarked at large journal sizes.


### Phase 3 — Durable local PendingLocal coalescing

- Add durable staging keyed by canonical scope/namespace/record/field.
- Coalesce only local LWW operations never exposed to a sender.
- Use canonical LWW comparison, not arrival/lock order.
- Keep pending payload independent of current visible winner because remote APPLY may win before freeze.
- Freeze atomically into immutable Outbox before send.
- Preserve event/CRDT/non-coalescible operations and explicit audit-history requests.

**Exit:** multiple commits to one field before publication produce one pending operation; crash before/during freeze yields complete pending or complete frozen state, never loss/duplication; remote winner does not erase eligible pending local operation; published operations are never rewritten.

### Phase 4 — Versioned compact physical encodings

**DLL boundary gate**
- Keep exactly two FLD runtime DLLs: Fld1.Storage and Fld1.Database.
- Keep dependency one-way from Database to Storage; Storage public APIs remain byte-oriented and BCL-only.
- Consolidate existing modules by type ownership; do not move field-aware RunBuilder/RunReader wholesale into Storage and do not introduce a third Common DLL.

**BIN-SYNC**
- Encode StreamAcked, StreamPushed and StreamOffsets as one per-author tuple with presence flags instead of three maps repeating author IDs.
- Use bounded varints/deltas only when measured.
- Compact receipts and indexes; retain legacy readers/migration.

**FLD Outbox**
- Remove per-row AuthorNodeId only if immutable LocalAuthor in file/header is validated.
- Consider OutboxSeq = segment base + ordinal only after torn-tail, sparse-index, compaction, reopen and no-reuse proofs.
- Group repeated CommitSeq by commit group; evaluate bounded value-length encoding.
- Keep ChangeId; never replace it with OutboxSeq.
- Include group framing, CRC, alignment, indexes and sidecars in measurements. The earlier 60–65 B fixed-record target is a hypothesis, not a result.

**Exit:** versioned migration and crash tests pass; old/new formats produce identical streams/ACK behavior; measured total bytes improve after all overhead.

### Phase 5 — Safe GC and snapshot/new-peer semantics

- Define independent safe horizons for ChangeId dedup, batch receipts, peer replay state and relay payload.
- Relay payload GC requires a validated downstream watermark.
- Specify whether new peers need current-state bootstrap only or historical relay/event provenance too.
- Snapshot is not proof that LWW losers or historical relay operations were delivered without a per-author cut/tail proof.
- No TTL-only deletion of ChangeId identity or opaque ProtocolStateDelta.

**Exit:** each GC decision has a durable watermark/epoch/fence or explicit protocol horizon; new-peer bootstrap is not conflated with history delivery; no crash or alternate route can resurrect compacted duplicates or lose required relay history.

### Phase 6 — Optional WAL-backed local Outbox

Only after Phases 2–4 and only if the Phase 5 retention contract is clear. While a WAL segment is retained, Outbox may reference immutable WAL payload. Before WAL truncation, promote only still-unacknowledged payloads. ACKed entries need not be copied. Measure pinned-segment impact; reject if peak or steady-state storage/checkpoint behavior worsens. This phase is optional.


## 4. Priority order

| Priority | Work |
|---|---|
| P0 | Baseline measurements and invariants |
| P1 | BIN-SYNC global ChangeId dedup, ACK replay pruning, unified InboxReceipt, compact PeerState v9, grouped persistence v10, compact local markers v11 and admission caps — implemented; receipt/dedup GC remains open |
| P2 | FLD APPLY v3 single-encoding, relay-only journal, sparse paging, offset-only batch index and explicit scan cursor — implemented; physical compaction/independent GC remain open |
| P3 | Durable pre-publication PendingLocal coalescing — design contract recorded; implementation next |
| P4 | Versioned compact FLD Outbox encoding and measured storage/RAM footprint |
| P5 | Safe receipt/dedup/tombstone GC and snapshot/new-peer contract |
| P6 | Optional WAL-backed Outbox |

Design the GC contract early, but do not ship destructive cleanup until its proof exists.

## 5. Non-goals and prohibited shortcuts

- Do not copy Microsoft Sync Framework's replica/knowledge architecture.
- Do not create per-peer operation copies or per-operation ACK bitmaps.
- Do not retain unpublished local LWW intermediate edits unnecessarily.
- Do not coalesce published/unacknowledged operations.
- Do not drop relay-eligible LWW losers merely because they are not current winners.
- Do not merge receipt, dedup, peer progress and payload into one record with one maximum lifetime.
- Do not use TTL alone as proof that ChangeId or relay data is safe to delete.
- Do not replace ChangeId with relay Seq, OutboxSeq or CommitSeq.
- Do not change wire format as a side effect of internal compaction.
- Do not add a third FLD Common DLL or protocol dependencies to Fld1.Storage.

## 6. Cross-document ownership

- This roadmap owns cross-repository sequencing and acceptance gates.
- Sync History Storage Audit owns code-backed findings and current-vs-target distinctions.
- BIN-SYNC Metadata Storage Architecture owns protocol metadata, atomic transitions and Metadata Store responsibilities.
- Change Staging and Retention owns pending/frozen/relay lifecycle and coalescing.
- FLD-DB Integration Contract owns cross-layer transaction/receipt/relay boundaries.
- FLD Storage Core Architecture owns the neutral physical engine and the binding two-DLL dependency boundary: Fld1.Storage plus Fld1.Database, with one-way Database → Storage dependency.
- FLD Kernel Specification remains normative for current format/protocol compatibility; this roadmap does not amend its wire contract.

## 7. Change discipline

The original audit records FLD-DB implementation baseline 196628a49d6034559a7041237af7feec36fd9b20 (audited branch ref b488dafcfda81d6383717ceaf05a3717a3959923) and BIN-SYNC audited code baseline dd2dd969d70b6fd46371679903855a69ae9399c3. Later commits exist; before each code phase, re-audit current branch HEAD and compare it with the audited source baseline.

Every implementation batch must state exact code baseline and files changed, add focused correctness/crash tests, run relevant tests/benchmarks, report measured disk/RAM/CPU separately from estimates, and update this roadmap plus the owning contract.

**Current status:** target architecture and order are documented. No code, persistence format or wire format is changed by this roadmap.


### 7.1 Revalidation before implementation (2026-11-07)

The current working refs were rechecked against the audited code baselines before starting implementation:

- FLD-DB working ref `fix/crash-consistency-outbox` is at `625b678632a0e7d35b3caa6225396f66b9a65c56`; comparison from audited code baseline `196628a49d6034559a7041237af7feec36fd9b20` shows 28 commits ahead, 0 behind, and the changed paths are documentation-only (README and docs; no `src/` or `tests/` paths).
- BIN-SYNC working ref `feature/fld-external-storage` is at `d2e884a7f63e323da93efe3c20f72d08b64044b2`; comparison from audited code baseline `dd2dd969d70b6fd46371679903855a69ae9399c3` shows 6 commits ahead, 0 behind, and changed paths are documentation-only.
- Backup refs `backup/pre-compaction-2026-11-07` in both repositories were compared to the working refs and are identical at the time of this revalidation.

Therefore the source-code findings in the audit remain applicable to the current working branches. No tests or benchmarks have been run in this revalidation; it verifies source baselines and changed-path scope only.


### 7.2 First implementation slice — replay-cache reclamation (2026-11-07)

Implemented in BIN-SYNC:
- `Node.ProcessAckInternal` now removes deterministic `PeerState.Ids` mappings only when the validated per-author contiguous ACK covers the cached cursor range end.
- The independent `LastStreamDigest` checkpoint is deliberately preserved; it is not replay-cache payload and is needed to seed subsequent digest-chain batches.
- The test `Ack_watermark_and_pushed_stream_survive_restart_and_duplicate_ack_is_idempotent` now verifies cache removal, digest-checkpoint preservation and the persisted post-ACK state.

Commits: `dc9a1304de00e75bacb0d2bdd864e0f48e687361` (implementation), `402b801e4b2d2781e048f3785766c0abf0425016` (test). GitHub Actions run [42342495367](https://github.com/smersh1307n2/BIN-SYNC/actions/runs/42342495367) completed successfully: .NET 10 build/tests, public samples, coverage threshold, fuzz/lab/hardening jobs all passed.

This closes only the replay-cache pruning subtask. Global route-independent ChangeId dedup and receipt consolidation remain open. No wire format or NodePersistence version changed.

### 7.3 Global ChangeId deduplication implementation (2026-11-07)

BIN-SYNC `feature/fld-external-storage` now implements the route-independent per-replica ledger keyed by `(namespaceId, ChangeId)`. Each entry stores a canonical BLAKE3-128 fingerprint over the resolved-author FULL_AUTHOR_ID operation, namespace, RecordId, OpKind and normalized causal dependency set. Same-ID/same-fingerprint operations are suppressed after Outbox compaction and on local-origin echoes; same-ID/different-known-fingerprint batches are rejected before WAL/state publication.

Local operations, remote WAL commit/replay and NodePersistence v6 are integrated. v3-v5 state migration preserves explicit unknown markers and reconstructs fingerprints only from complete retained Outbox payloads. Causal dependencies are included in identity and survive relay and WAL replay. No ChangeId or wire format changed. No dedup GC/TTL is implemented because the protocol-safe retention horizon remains unproven.

Focused tests cover cross-route duplicate suppression after origin Outbox compaction, returned local echoes, conflict rejection without partial mutation, v5 migration, v6 persistence, causal metadata relay and WAL recovery. GitHub Actions run [42344100307](https://github.com/smersh1307n2/BIN-SYNC/actions/runs/42344100307) passed .NET 10 build/tests, public samples, coverage and fuzz/lab hardening. Code commit at the time of that CI run: `468d26988dee370aef4f7266b76bcbc6a5e5c77a`; later documentation-only changes do not alter that tested code.

This closes the global-dedup implementation gate, not the full BIN-SYNC metadata audit. Next: remove redundant Inbox/InboxAuthors key duplication after auditing every duplicate-ACK/recovery path, then address APPLY payload amplification and relay/receipt separation.

### 7.4 Inbox receipt key consolidation (2026-11-07)

BIN-SYNC now stores each accepted-batch receipt key only once: one dictionary maps `(issuerNodeId, namespaceId, issuerEpoch, batchId)` to the exact durable author stream required for duplicate ACKs. The former Inbox HashSet is now only a read-only key view over that dictionary and contributes no second retained key set. Duplicate ACK handling reads the author from this map and no longer infers it from LastStreamAuthor.

NodePersistence is v7. v3-v6 loaders read the former Inbox key set and InboxAuthors map, validate exact parity (including duplicate/orphan/missing/zero-author rejection), then migrate into one map. WAL replay and atomic state publication also carry only that dictionary. The serialized saving is one duplicate receipt key per accepted batch: approximately 34 bytes for the common one-byte namespace/epoch varints, plus the removed in-memory HashSet entry/buckets. This is a format-derived estimate, not a benchmark.

GitHub Actions run [42344555045](https://github.com/smersh1307n2/BIN-SYNC/actions/runs/42344555045) passed .NET 10 build/tests, public samples, coverage and fuzz/lab hardening. Code commit tested: `9d58f8d6791c7cb03e36e8d63f4696449aa7a354`.

This does not implement the complete receipt tuple (cursor range, batch/records/previous digests, expiry and WAL generation), conflicting payload validation under an already accepted BatchId, or receipt GC. ReceivedStreams remains a separate author-stream cursor. Next: complete receipt-tuple audit (range/digest/expiry semantics), then APPLY WAL/ApplyJournal payload de-duplication.

### 7.5 Remove redundant remote SeenChanges provenance (2026-11-07)

Once the global ChangeId ledger became the remote dedup authority, per-peer remote `SeenChanges(senderPeer, ns, ChangeId)` rows no longer contributed correctness. New ApplyBatch paths no longer insert or consult them. `SeenChanges` is now only the local WAL replay marker set; this preserves its separate replay lifetime. The global ledger already records local ChangeIds as well, so a remote echo returning to its origin is suppressed without a peer-specific marker.

NodePersistence remains v7. v3-v5 migration first folds every old SeenChanges identity into an explicit unknown global ledger marker, then retains only local-node replay markers. v6-v7 already contain the global ledger, so remote SeenChanges rows can be dropped while local markers survive. `MaxSeenChanges` now caps local WAL markers; `MaxGlobalChangeLedgerEntries` caps global identities. No wire or persistence layout change.

GitHub Actions run [42345011358](https://github.com/smersh1307n2/BIN-SYNC/actions/runs/42345011358) passed both .NET 10 and fuzz/lab jobs. The raw duplicate remote key was approximately 33 bytes per received operation for common varint sizes (peer 16 B + ChangeId 16 B + namespace varint), plus HashSet overhead; this estimate is derived from serialization, not a benchmark.

### 7.6 Compact header-bound InboxReceipt (2026-11-07)

BIN-SYNC now stores a single v8 InboxReceipt per `(issuerNodeId, namespaceId, issuerEpoch, batchId)`: durable author, original cursorEnd, BLAKE3-128 header fingerprint, appliedAt and expiresAt. CursorStart, previous/records/batch digests, schema id/version, merge policy and flags are committed by the fingerprint; cursorEnd and timestamps use varint persistence encoding. The receipt does not retain full entry payloads or causal dependencies.

Duplicate BatchId replay compares the stored header fingerprint and returns ACK using stored cursorEnd/author. It intentionally does not decode or apply replay-supplied entry payloads/ChangeIds/CausalDeps, preserving the existing duplicate-before-causal-gate contract. Same BatchId with unchanged committed header is acknowledged as duplicate even if its replay payload differs; changed committed header is rejected before ACK. Legacy v3-v7 receipts migrate with explicit unknown fingerprint/cursorEnd markers and preserve old duplicate behavior.

New receipts currently set `expiresAt=0` (no expiry scheduled). No receipt GC or WAL-generation/materialization field is implemented; these require a safe log-segment/peer-confirmation horizon. This v8 receipt adds metadata to the old key-to-author map, so do not claim a net serialized-byte saving versus v6 solely from key consolidation; cursor/timestamp varints reduce the added cost. The key-set duplication and in-memory second collection are removed.

The spec §10.2 and §10.2.2 now describe this receipt contract. No wire-format change. GitHub Actions run [42345734949](https://github.com/smersh1307n2/BIN-SYNC/actions/runs/42345734949) passed the v8 header-commitment code; a follow-up CI run is required for the final cursorEnd-varint encoding refinement.

### 7.7 Compact peer progress and received-stream keys (2026-11-07)

BIN-SYNC now stores ACK, pushed and offset watermarks in one `PeerAuthorProgress` value per author instead of three dictionaries repeating the same author key. Presence flags preserve the distinction between an absent watermark and a real zero. `ReceivedStreams` drops its repeated namespace component because its owning `PeerState` is already keyed by (peer, namespace); the key is now (senderPeer, issuerEpoch, author).

NodePersistence was bumped from v8 to v9 (compact peer progress), v10 (grouped receipt/ledger scopes) and v11 (compact local WAL markers). v3-v10 files remain readable: the legacy receive-stream namespace is checked against its owning PeerState and the three old watermark maps are migrated into the combined per-author entry. v9 persists one author key, flags and only the present watermark values; v10 groups receipt and ChangeId-ledger scope keys; v11 removes the repeated NodeId from local WAL replay marker keys. The reader validates flags, nonnegative values, ACK≤pushed and offset arithmetic. This is an internal persistence/in-memory change; no wire format change.

The structural reduction is deterministic but its byte impact depends on sparsity and dictionary overhead. Do not state a fixed percentage until measured against representative peer/author cardinalities. CI run [42346869314](https://github.com/smersh1307n2/BIN-SYNC/actions/runs/42346869314) passed both .NET 10 tests/public samples/coverage and fuzz/lab hardening at BIN-SYNC commit `8cf01094ac196ece91e6a7cead60634811bf4704`.

### 7.8 BIN-SYNC tombstone GC safety gate (2026-11-07)

The BIN-SYNC audit found that the existing GC path is not safe to use as tombstone reclamation: `GcNotice.Apply` does not persist/enforce `TombstoneCutoffWall`; `WatermarkGate` is not wired into DELTA_BATCH/reconcile/snapshot; and the former `GcNotice.PurgeBelowCutoff` deleted all old `Record.State` fields by HLC without checking whether they were tombstones or dominated ghosts. It left `FieldTombs` and lifecycle tombstones untouched. That destructive implementation is now disabled (the method returns zero until the safe gate exists), and tests assert that old live values and both tombstone forms remain intact.

Do not enable destructive tombstone GC until a monotonic per-namespace cutoff is durable, peers behind it are forced through full resync/snapshot, and deletion is limited to tombstone/ghost state proven safe. Ordinary live fields must never be deleted merely because their HLC is old. Inbox receipt GC and ChangeId-ledger GC remain independent. See BIN-SYNC `METADATA-STORAGE-ARCHITECTURE.md` §8.3.

### 7.9 Inbox receipt admission cap (2026-11-07)

BIN-SYNC now exposes `ResourceLimits.MaxInboxReceipts` (default 1,000,000; zero disables the cap). A newly accepted batch consumes one receipt even when all its ChangeIds are global duplicates, so the cap is checked before WAL commit and rejects the entire batch without publishing record, cursor, receipt, ledger or relay changes. This is admission backpressure only: no receipts are evicted and no disk space is reclaimed. The dedicated test covers a new BatchId carrying an already-known ChangeId and verifies atomic rejection at capacity. GitHub Actions run [42347223586](https://github.com/smersh1307n2/BIN-SYNC/actions/runs/42347223586) passed .NET 10 tests/public samples/coverage and fuzz/lab hardening at BIN-SYNC commit `59e896a89a9b26ed57854df0d70ace5787f5f405`.

### 7.10 Fail-closed tombstone GC until safe watermark enforcement (2026-11-07)

The former BIN-SYNC `GcNotice.PurgeBelowCutoff` deleted ordinary field values based only on HLC age. It was not tombstone-specific and could remove current live state while leaving field/record tombstones behind. Since `TombstoneCutoffWall` is not durably enforced against DELTA_BATCH/reconcile/snapshot peers, there is no proof that stale operations cannot reintroduce deleted state.

The destructive helper is now disabled and returns zero; regression tests assert that old live values, field tombstones and record lifecycle tombstones remain intact. This is a safety stop, not completion of tombstone GC. The protocol still needs a durable per-namespace safe cutoff, stale-peer full-resync enforcement and tombstone/ghost-specific reclamation. CI run [42347618510](https://github.com/smersh1307n2/BIN-SYNC/actions/runs/42347618510) passed .NET 10 tests/public samples/coverage and fuzz/lab hardening at BIN-SYNC commit `6767e2da14c2951ac440ed173e9fd796fee54469`.

### 7.11 APPLY single-copy encoding and relay-only journal (2026-11-07)

Implemented:
- `ApplyCommitCodec` v3 encodes the incoming `Fields` array once and represents `RelayFields` as canonical varint indexes. v2 payloads remain readable.
- `ApplyJournal.AppendIfMissing` materializes only receipt metadata and the relay-selected fields. Non-relay operation payloads are no longer duplicated into the sidecar.
- The journal uses a relay-only APPLY_COMMIT form; the WAL is flushed first, then the compact journal record is appended and flushed before `ApplyRemoteBatch` returns. WAL checkpoint/truncation therefore cannot lose the only durable relay copy.
- Journal promotion is idempotent by batch identity and compares canonical receipt metadata plus relay payload. WAL replay can reconstruct the compact sidecar from old v2 and new v3 WAL records.
- Tests verify that the sidecar is substantially smaller than the full WAL for eight incoming operations with one relay-selected operation, and that the relay subset survives restart.

GitHub Actions run [42348013521](https://github.com/smersh1307n2/FLD-DB/actions/runs/42348013521) passed Release build and all tests at FLD-DB commit `4efc1985d4da633aa97a7e53d0cf9f44186257b0`.

Still open: receipt and relay payload share one append-only journal file; no independent GC/compaction, bounded index or seekable paging exists. The relay payload must remain until downstream safe ACK; the receipt has its own retry/expiry horizon. Do not conflate their lifetimes.

### 7.12 Seekable ApplyJournal paging (2026-11-07)

`ApplyJournal.ReadBatches(afterJournalSeq, limit)` now performs a lower-bound binary search over the monotonic JournalSeq index and reads only the requested page. This removes the prior O(history) skip scan on every page; expected selection cost is O(log N + page size). A regression test writes four relay batches and verifies that paging after JournalSeq 2 returns only entries 3 and 4. The in-memory List and BatchKey Dictionary still grow with the complete journal; bounded/rebuildable indexes and compaction remain open.

GitHub Actions run [42348362149](https://github.com/smersh1307n2/FLD-DB/actions/runs/42348362149) passed Release build and all tests at FLD-DB commit `6586c3afe19dfb34a36ebdc70ee7725e2198acfc`.

### 7.13 Group Inbox receipts and global ChangeId ledger in NodePersistence v10 (2026-11-07)

BIN-SYNC NodePersistence now writes accepted-batch receipts grouped by `(issuer, namespace, epoch)`, so the shared issuer/session key is stored once for all BatchIds in that group. Global ChangeId fingerprints are grouped by namespace, storing the namespace once for the group's identities. The logical dictionaries, receipt idempotency semantics, fingerprints and network wire format are unchanged.

For each additional receipt in a shared group, the raw key saving is approximately 16 bytes plus namespace and epoch varint widths; for each additional ledger entry in a namespace, it is the namespace varint width. Group count fields offset a small amount, and singleton groups may be slightly larger. These are format-derived estimates, not measured benchmark results. The v10 reader accepts v3-v9 flat layouts and validates receipt cursor overflow, expiry ordering, group counts and identity fields. Tests cover multi-group roundtrip, unknown legacy markers, expiry fields, v9 compatibility and malformed cursor rejection.

GitHub Actions run [42348683438](https://github.com/smersh1307n2/BIN-SYNC/actions/runs/42348683438) passed .NET 10 build/tests/public samples/coverage and fuzz/lab hardening at BIN-SYNC commit `287cb86c349859a934f8fc33b6f62e13c28f6f17`.

### 7.13 Sparse ApplyJournal index (2026-11-07)

The full per-record `_entries` List was removed. ApplyJournal now keeps one sparse `JournalSeq→offset` anchor per 256 records and uses lower-bound binary search to find the nearest anchor, then scans at most one bounded stride to the requested page. The `BatchKey` dictionary now stores only the 64-bit record offset rather than a full repeated IndexEntry; it remains one entry per receipt and is still unbounded. Disk-backed/rebuildable receipt indexing and independent receipt GC remain open.

A restart regression test verifies page reads after JournalSeq 2. The regression test was expanded to 260 batches so it crosses the second sparse anchor and verifies restart reconstruction. The BatchKey dictionary value was then reduced to a 64-bit offset. GitHub Actions run [42348983102](https://github.com/smersh1307n2/FLD-DB/actions/runs/42348983102) passed Release build and all tests at FLD-DB commit `044272f56415270cc332e6e8a8bde16890cfba50`.

### 7.14 Relay page scan cursor (2026-11-07)

Added `FldDatabase.ReadRelayBatchPage(author, afterJournalSeq, limit)`, returning both matching relay batches and `ScannedThroughJournalSeq`. The cursor advances across nonmatching batches as well, so an author with sparse relay traffic does not repeatedly rescan the same empty tail. The legacy `ReadRelayBatches` API remains as a compatibility wrapper. A 260-record test verifies sparse-anchor paging after restart, a one-item page cursor, and an empty-author scan advancing to JournalSeq 260.

The integration contract now requires BIN-SYNC to persist the scan cursor atomically with all relay operations returned in the page. Scan progress is not a downstream-safe ACK and cannot authorize payload reclamation. GitHub Actions run [42349125133](https://github.com/smersh1307n2/FLD-DB/actions/runs/42349125133) passed Release build and all tests at FLD-DB commit `18bf4022c8178e7c342d704e88b74b4fa51d9497`.

### 7.14 Compact local WAL replay markers in NodePersistence v11 (2026-11-07)

After remote peer-scoped `SeenChanges` rows were removed, the remaining set contained only local WAL replay markers, but each tuple still stored the immutable owning NodeId. BIN-SYNC now uses `(namespace, ChangeId)` in memory and in v11 snapshots, removing 16 raw bytes per marker plus tuple/hash-entry overhead. The global ChangeId ledger remains a separate structure because it has a different semantic fingerprint and retention horizon.

NodePersistence v11 reads v3-v10 snapshots by retaining only old `SeenChanges` rows whose peer matches the snapshot NodeId; v3-v5 migration still folds every historical route marker into the global unknown-fingerprint ledger before filtering local markers. WAL replay preflight, rollback and persistence roundtrip tests were updated. No wire-format change. GitHub Actions run [42349243189](https://github.com/smersh1307n2/BIN-SYNC/actions/runs/42349243189) passed .NET 10 tests/public samples/coverage and fuzz/lab hardening at BIN-SYNC commit `ab0f2a8bf2dd`.

### 7.15 Durable per-author relay ACK and post-WAL-seal compaction (2026-11-07)

Implemented the FLD side of safe relay retention:
- `FldDatabase.AcknowledgeRelayBatches(author, safeThroughJournalSeq)` validates a monotonic per-author watermark against the journal high-water mark and persists it in an atomic CRC-protected `.relayack` sidecar.
- `ReadRelayBatchPage` suppresses acknowledged author payload immediately, even before physical compaction; other authors in the same source batch remain readable.
- Physical removal is deferred until the APPLY commit is sealed and its WAL segment is truncated. Checkpoint then rewrites ApplyJournal atomically, removing only acknowledged author payload while retaining the batch receipt and unacknowledged authors' payloads.
- WAL replay checks sealed batch receipts before attempting to append/compare the full WAL payload, so a compacted relay subset cannot conflict with a still-present sealed WAL frame. If the receipt is missing but the WAL frame remains, replay can rebuild it and the post-replay seal pass compacts again.
- The regression test acknowledges one of two authors, restarts before checkpoint, verifies sidecar filtering, checkpoints, verifies journal size reduction, verifies the other author's relay payload remains, and confirms duplicate BatchId still returns the receipt.

Recovery validates persisted relay ACK watermarks against the recovered JournalSeq high-water mark, and the API rejects a watermark beyond the current journal high-water mark. GitHub Actions run [42349914253](https://github.com/smersh1307n2/FLD-DB/actions/runs/42349914253) passed Release build and all tests at FLD-DB commit `0c2542593849faba8fd3b38ed165b5c8f2597c38`.

Remaining integration work: BIN-SYNC must durably map source JournalSeq to ChangeId/local OutboxSeq and compute the minimum contiguous safe ACK across all interested peers. FLD does not infer peer safety. Receipt GC remains independent and is not implemented by relay ACK.

### 7.15 PendingLocal coalescing — durable API and crash protocol (design, not yet implemented)

The next phase is an additive API so the current immediate-publication contract is not silently broken:

- `CommitPending()` durably commits local writes into a per-(namespace, RecordId, FieldId) PendingLocal map without assigning a published stream sequence or exposing them to transport. Repeated calls coalesce only local LWW operations by the canonical comparator; the staged payload is retained independently of the visible overlay.
- `FreezePending()` publishes the current staged set. A `PENDING_FREEZE` WAL record carries the stage-through watermark and final immutable GroupCommit payload. It is flushed before Outbox append/flush; recovery replays the freeze idempotently and removes only staged entries covered by that watermark.
- Existing `Commit()` remains immediate for compatibility. If PendingLocal exists, it merges staged values and current dirty values (dirty values win only by canonical LWW order) into one freeze transaction. `ReadOutboxBatch`, `VisitOutboxBatch`, `AcquireOutboxLease` and `GetOutboxHighWatermark` freeze first, so a sender cannot observe a watermark while committed staged work remains hidden. `AcknowledgeOutbox` and `CompactOutbox` do not implicitly freeze.
- ChangeId and OutboxSeq become immutable only at freeze. Pending values have no stream position and are never included in peer progress. Remote relay and CRDT/event operations are not coalesced.

Durability/checkpoint design: introduce WAL kinds for pending commit, pending freeze and pending snapshot commit. At checkpoint, write an immutable generation-specific PendingLocal snapshot (sorted keys, exact payload/HLC/author/key epoch and stage watermark), fsync it, rotate to a fresh WAL segment, append+flush a snapshot-commit marker containing generation/hash/through-sequence, then truncate only earlier sealed segments. The marker segment remains retained. Recovery requires the referenced snapshot when a marker exists, loads it as the pending base, and replays only later pending mutations/freezes; missing or mismatched snapshot fails closed. Old snapshot generations are deleted only after the new marker is durable and the corresponding WAL prefix is safely truncated. This avoids pinning an unbounded WAL prefix while keeping crash windows recoverable.

Required crash tests: after pending WAL flush; after snapshot write before marker; after marker before truncation; after truncation; after freeze WAL flush before Outbox append; after Outbox flush before in-memory stage removal; mixed pending+dirty commit; remote LWW winner after pending local write; checkpoint with pending entries but empty visible overlay; restart with several coalesced commits. Baseline comparison: four separate immediate `Commit()` calls remain four immutable operations; four `CommitPending()` calls to the same field followed by one freeze produce exactly one operation with the final local winner.

The baseline half is now executable: `PendingLocalBaselineTests` proves that four separate immediate commits currently produce four distinct immutable Outbox operations, while four Put calls inside one commit already coalesce to one final value. GitHub Actions run [42350334573](https://github.com/smersh1307n2/FLD-DB/actions/runs/42350334573) passed the new baseline tests and the full .NET 10 suite at commit `77236494106bd8b3b48ff4fa17dd17c6993cc5d8`.

### 7.16 Normative ChangeId identity and equivocation contract (BIN-SYNC E-24, 2026-11-07)

BIN-SYNC CORE specification is now v2.4.18 FINAL. E-24 formalizes the implemented replica-local identity key `(namespaceId, ChangeId)`, alias/route-independent semantic fingerprint after author resolution, same-ID/same-fingerprint duplicate suppression, same-ID/different-fingerprint whole-batch rejection, unknown-but-seen legacy migration and the prohibition on TTL/ACK-based dedup GC before a safe horizon exists. The fingerprint remains local metadata (BLAKE3-128 in the current implementation), not a DELTA_BATCH field; no wire layout changed.

A focused test now verifies alias independence and sensitivity to author, namespace, RecordId, KeyEpoch, value bytes, causal dependency set, FieldTomb and RecordTombstone semantics. The existing integration tests cover alternate-route delivery, local self-echo after Outbox compaction, equivocation atomicity, persistence migration and WAL recovery. This closes the normative-spec gap for Phase 1; independent multi-implementation conformance vectors remain a release gate. BIN-SYNC CI run [42354862144](https://github.com/smersh1307n2/BIN-SYNC/actions/runs/42354862144) passed 609 tests, public samples, coverage and fuzz/lab hardening at `b3d65223629080b7488ff431343061ae6fe21e28`; FLD roadmap documentation CI runs [42354937088](https://github.com/smersh1307n2/FLD-DB/actions/runs/42354937088) and [42354933626](https://github.com/smersh1307n2/FLD-DB/actions/runs/42354933626) also passed. Source JournalSeq provenance/adapter transaction and protocol-safe receipt/dedup GC remain open and must not be represented as completed by this erratum.

### 7.17 Normative tombstone retention safety (BIN-SYNC E-25, 2026-11-07)

BIN-SYNC CORE v2.4.19 E-25 now makes the fail-closed rule normative: a seven-day TTL or scalar HLC cutoff/GC_NOTICE is not proof that a tombstone can be deleted. Reclamation requires a durable namespace frontier covering all relevant author streams, confirmation by every peer in the current baseline, stale-peer fencing and mandatory full resync, plus an atomic snapshot/feed barrier. Until that protocol is implemented and enforced on every delta/snapshot path, field tombstones, record lifecycle tombstones and ghost state remain durable; the current PurgeBelowCutoff helper is intentionally a no-op. BIN-SYNC GitHub Actions run [42355154761](https://github.com/smersh1307n2/BIN-SYNC/actions/runs/42355154761) passed 609 .NET 10 tests, public samples, coverage and fuzz/lab hardening at commit `e398c49be95950e98528cc617d24650b91dae5f1`.

This also fixes the Storage ownership distinction: `Fld1.Storage.dll` stores opaque durable bytes/pages and does not interpret tombstones. `Fld1.Database.dll` owns tombstone/ghost semantics, merge visibility and any future safe reclamation decision. Storage must faithfully persist the Database's tombstone representation; it must not independently TTL-delete it.


### 7.18 Tombstone retention research (2026-11-07)

Added [Tombstone Retention and Safe Garbage Collection](TOMBSTONE-RETENTION-RESEARCH.md), comparing Microsoft Sync Framework ForgottenKnowledge, WinFS P2P knowledge/vector summaries, CRDT causal stability versus commitment, Cassandra grace-period behavior, CouchDB revision/purge semantics and Couchbase Sync Gateway mobile-client retention.

The research reinforces E-25 and clarifies the adaptation:

- Microsoft does not equate tombstone cleanup with forgetting the deletion. It records cleaned tombstone versions in ForgottenKnowledge, detects stale destinations and invokes full enumeration; it also checks item creation versions to prevent resurrection.
- CRDT literature distinguishes collecting metadata for causally stable updates from removing tombstones/resetting replicated state, which requires a commitment protocol and known membership assumptions.
- Cassandra's ten-day grace period is an operational repair window, not protection against replicas absent longer than the configured period.
- CouchDB distinguishes replicated deletion from physical purge; its internal purge uses sequence/checkpoint propagation, while external purge is not replicated automatically.
- No finite TTL is safe for an indefinitely disconnected peer that remains authorized to submit old state.

**Recommended direction:** durable namespace membership/baseline generation + explicit stale-peer fencing + mandatory authoritative full enumeration/bootstrap + atomic snapshot/feed barrier + compact forgotten-frontier knowledge. Full recovery must reconcile destination-only stale items as absent; a merge-only current-winner snapshot is insufficient. Preserve offline local edits through an explicit conflict/quarantine/rebase path before authoritative replacement. The frontier must be scoped to the node/namespace/generation that owns its sequence positions. Do not treat local OutboxSeq, JournalSeq, CommitSeq or relay-local Seq as a globally comparable origin sequence. Do not add OriginSeq or change ChangeId generation as part of this work.

Keep five lifetimes independent: current field/record tombstones, relay payload, global ChangeId dedup ledger, Inbox batch receipts, and forgotten frontier/GC epoch. Relay ACK does not authorize dedup-ledger or receipt deletion. TTL may mark a tombstone as a GC candidate but cannot authorize physical purge.

The existing fail-closed behavior remains mandatory. The research note is a design proposal, not implementation approval. Tombstone GC remains disabled until membership fencing, snapshot/feed enforcement, durable forgotten-frontier recovery and adversarial tests cover every delta/reconcile/snapshot acceptance path.


### 7.19 Durable BIN-SYNC ↔ FLD provenance adapter — required later integration gate (2026-11-07)

This section preserves the adapter requirements from the integration handoff. The provenance mapping remains mandatory, but it is **not the next cross-repository implementation task**. The higher-level Metadata Store architecture sequence takes precedence: define and test neutral semantic operations; define Data Provider atomicity modes; move BIN-SYNC persistence behind that boundary; extract FLD Storage-only semantics; implement the Metadata Store adapter; then complete this FLD synchronized-database provenance integration. PendingLocal remains deferred until the applicable storage/adapter gates are closed.

**Verified branch checkpoint:**
- FLD-DB fix/crash-consistency-outbox: code baseline eacff36; current branch head d4627626 contains documentation-only commits after that code baseline.
- BIN-SYNC feature/fld-external-storage: code baseline d2e0313; current branch head 8c836f84 contains documentation-only commits after that code baseline.
- No adapter implementation or code changes are present after those code baselines. Existing public APIs and FLD journal/ACK implementation are the starting point, not a completed adapter.

**Existing primitives verified in source:**
- FLD ReadRelayBatchPage(author, afterJournalSeq, limit) returns author-filtered relay batches plus ScannedThroughJournalSeq, advancing across nonmatching ApplyJournal records.
- FLD AcknowledgeRelayBatches(author, safeThroughJournalSeq) persists the per-author JournalSeq watermark in the CRC-protected relay ACK sidecar. Physical relay payload compaction remains deferred until the corresponding APPLY WAL prefix is sealed/truncated.
- BIN-SYNC Node.GetSafeOutboxAckedThrough(namespaceId, author) returns the minimum local OutboxSeq acknowledged by all interested peers; zero is fail-closed when there is no peer baseline or a peer lacks durable PeerState.
- BIN-SYNC Node.TryResolveOutboxSequences(namespaceId, identities, sequences) resolves retained (author, ChangeId) identities to local OutboxSeq and clears the output/returns false if any identity is no longer retained.
- OutboxChangeIdentity is (NodeId AuthorNodeId, BatchId ChangeId). ChangeId remains the stable operation identity; OutboxSeq is only the local author-stream retention coordinate.

**Implementation boundary:** this particular JournalSeq → ChangeId → OutboxSeq adapter is for the FLD *synchronized-database mode* and therefore coordinates the FLD Database durability boundary with BIN-SYNC. It is distinct from the BIN-SYNC Metadata Store adapter for FLD *Storage-only mode*. The latter depends on the neutral `Fld1.Storage` API and BIN-SYNC contracts, must not instantiate `FldDatabase`, and must not create synthetic HLC/author/Outbox state. Neither adapter may be placed inside `Fld1.Storage.dll`; neither may make `Fld1.Storage.dll` depend on BinSync.Core. The two-DLL database architecture remains unchanged; adapters are optional integration packages, not additional database runtime assemblies.

**Required durable adapter state, scoped by namespace and original author:**
- ScannedThroughJournalSeq;
- ordered provenance rows (JournalSeq, ChangeId, OutboxSeq);
- a versioned, checksummed page-commit boundary so the scan cursor and every mapping produced by that page are one durable transaction.

Author identity may be stored once per author group. Mapping rows must remain until FLD has durably accepted the translated relay ACK. The state must be restartable without relying on current field winners or re-decoding deleted APPLY payloads.

**Required processing order:**
1. Close the namespace outbound publication gate before any newly produced relay Outbox entry can be leased, batched, sent or ACKed. The adapter must own all transport scheduling for this Node/namespace; bypassing the adapter is unsupported.
2. Read one FLD author page at the persisted scan cursor.
3. Resolve every (author, ChangeId) in the returned relay subset through TryResolveOutboxSequences. Any missing/conflicting identity fails closed; do not advance the cursor or reconstruct a sequence from field state.
4. Validate strictly monotonic OutboxSeq progression across increasing source JournalSeq groups for the same author. Preserve all entries sharing one JournalSeq as a group.
5. Persist the complete page mapping and ScannedThroughJournalSeq atomically and durably.
6. Only after step 5 may outbound publication resume for the mapped entries.
7. For ACK translation, read BIN-SYNC's safe OutboxSeq watermark. Find the first mapped JournalSeq group containing an OutboxSeq greater than that watermark; the FLD safe-through is the preceding JournalSeq. If no such group exists, the safe-through may advance to the persisted scanned cursor. A zero safe OutboxSeq never authorizes a positive relay ACK.
8. Persist the translated source watermark in FLD first by calling AcknowledgeRelayBatches. Only after FLD confirms durability may the adapter prune covered provenance rows and durably persist that pruning. A crash between these two steps leaves harmless stale mapping rows; reversing the order can lose the only mapping and is forbidden.
9. On restart, load and validate adapter state before opening outbound publication. Re-read any page whose mapping+cursor transaction did not commit. If any retained identity cannot be resolved, keep the gate closed and require explicit reconciliation.

**Important upgrade condition:** if relay entries may already have been published before this adapter gate existed, startup cannot assume the provenance history is complete. A missing Outbox identity is a hard recovery condition, not permission to skip it. The first adapter rollout must either start before any relay publication or perform a separately specified reconciliation that proves all historical mappings.

**Required tests before enabling transport:**
- multiple relay fields for one author in one JournalSeq group;
- multiple authors in the same ApplyJournal batch, each with independent cursor/mapping;
- empty/nonmatching pages advancing the cursor;
- page limit reached mid-history and exact resume without gap/duplicate;
- crash before page-state append, during torn append, after durable page commit and before first send;
- restart with already committed mappings and no duplicate remapping;
- Outbox identity missing/compacted or author/ChangeId conflict: gate remains closed;
- monotonic sequence violation across JournalSeq groups: gate remains closed;
- unequal peer ACKs and a peer missing PeerState: safe-through does not advance;
- ACK translation with no mapping above the safe watermark and with a partially covered JournalSeq group;
- crash after FLD relay ACK persistence but before adapter mapping prune;
- crash during adapter mapping compaction/rewrite;
- ensure no public transport path can call Node batch creation/send outside the adapter-owned gate.

**Exit:** durable mapping and scan cursor recover correctly; all outbound publication is gated until mapping is durable; safe ACK translation is contiguous and monotonic; FLD ACK precedes mapping pruning; all adversarial/crash tests pass. Only then resume Phase 3 PendingLocal work.


### 7.20 Correct cross-repository implementation order — Metadata Store boundary first (2026-11-07)

A source and architecture revalidation found that the provenance adapter requirements in 7.19 are correct but were incorrectly promoted to the immediate next implementation task. The controlling sequence is BIN-SYNC Metadata Storage Architecture §16 and FLD-DB Integration Contract §9.

**New durable design artifact:** BIN-SYNC `docs/METADATA-STORE-CONFORMANCE.md` defines provider-independent semantic operations and a conformance matrix. It explicitly separates Data Provider, Metadata Store, physical Storage and BIN-SYNC Runtime. It is a semantic contract, not a frozen public API.

**Required order:**

1. Approve the Metadata Store semantic contract and resolve remaining transaction/recovery decisions.
2. Finalize local change staging/coalescing and its crash/relay tests.
3. Implement a provider-independent conformance fixture plus a test-only volatile Metadata Store. Volatile storage is for semantic tests only and must not advertise restart durability.
4. Define Data Provider change capture and shared-transaction / separate-store idempotent apply modes.
5. Refactor BIN-SYNC persistence behind the neutral Metadata Store boundary without changing wire behavior; existing Node/WAL tests remain regression coverage, not proof of arbitrary-store conformance.
6. Extract FLD's two-assembly physical Storage Core and implement explicit Storage-only semantics.
7. Implement the BIN-SYNC Metadata Store adapter over neutral Storage APIs.
8. Separately complete the FLD synchronized-database integration from 7.19, including durable provenance mapping, publication gate and ACK translation.
9. Validate external SQL, file, array and custom-provider scenarios.
10. Freeze public API/package contracts only after conformance passes.

**Two FLD modes must remain distinct:**

- *Storage-only Metadata Store:* uses `Fld1.Storage.dll` directly; no `FldDatabase`, LocalAuthor, HLC, user records or FLD Outbox. BIN-SYNC protocol metadata is stored as opaque records through the neutral semantic adapter.
- *Synchronized FLD Database:* uses `Fld1.Database.dll` as both Data Provider and transaction owner for field state plus protocol metadata. The 7.19 relay provenance adapter belongs here.

The current FLD source tree still builds `Fld1.Core`, `Fld1.Engine`, `Fld1.Manifest`, `Fld1.Run` and `Fld1.Wal` as separate projects; it does not yet match the approved two-public-DLL packaging boundary. The current BIN-SYNC Node remains backed by internal in-memory collections plus NodePersistence/WAL. Therefore, do not create a direct Node↔FldDatabase callback bridge or a premature NuGet integration project. First establish the neutral semantic boundary and actual Storage API. This avoids locking the adapter to the current pre-extraction FLD project graph or duplicating durable user field state inside Node.

**Exit condition for this phase:** the semantic contract has an executable provider-independent conformance suite; transaction modes and recovery behavior are testable; existing Node/WAL behavior is preserved as a regression baseline. Only then proceed to the persistence refactor and Storage-only implementation.
