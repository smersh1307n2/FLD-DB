# Sync Storage Compaction Roadmap — FLD-DB + BIN-SYNC

**Date:** 2026-11-07  
**Status:** agreed target architecture; BIN-SYNC P1 global dedup and replay-cache pruning are implemented without a wire-format change; receipt consolidation and APPLY payload compaction remain next.  
**Primary audit:** [Sync History Storage Audit](SYNC-HISTORY-STORAGE-AUDIT.md).  
**Companion documents:** [FLD Storage Core](FLD-STORAGE-CORE-ARCHITECTURE.md), [BIN-SYNC Metadata Storage Architecture](https://github.com/smersh1307n2/BIN-SYNC/blob/feature/fld-external-storage/docs/METADATA-STORAGE-ARCHITECTURE.md), [Change Staging and Retention](https://github.com/smersh1307n2/BIN-SYNC/blob/feature/fld-external-storage/docs/CHANGE-STAGING-AND-RETENTION.md), [FLD-DB Integration Contract](https://github.com/smersh1307n2/BIN-SYNC/blob/feature/fld-external-storage/docs/FLD-DB-INTEGRATION-CONTRACT.md).

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

### Phase 1 — BIN-SYNC correctness and metadata lifecycle

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

**Exit:** route-independent dedup; conflicting ID rejected; duplicate BatchId after restart returns same valid ACK; acknowledged replay entries pruned safely; receipt is not used as global dedup.

### Phase 2 — FLD remote APPLY payload amplification

**2.1 Single APPLY payload**
- Encode each incoming operation once in authoritative APPLY WAL.
- Replace duplicate full RelayFields array with compact indexes/bitmap into Fields.
- Validate relay selection before durable commit.

**2.2 Metadata-only receipt and relay-only payload**
- Receipt stores batch identity, cursor/author, digest-chain/RecordsRoot, outcome and commit marker needed for duplicate ACK; no full field payload.
- Before truncating an APPLY WAL segment, durably and idempotently promote only relay-selected operations to a relay payload log, including eligible LWW losers.
- Promotion is keyed by CommitSeq + batch identity.
- Do not retain non-relay fields solely for duplicate retries.
- Add validated downstream safe watermark before relay payload GC.

**2.3 Bounded indexes and seekable paging**
- Replace unbounded all-history in-memory List/Dictionary assumptions with bounded/rebuildable indexes.
- Seek from JournalSeq/cursor; do not rescan history from the beginning on every page.

**Exit:** each incoming payload encoded once; relay selection stores indexes; WAL truncation cannot lose the only relay copy; receipt survives without non-relay payload; crash at every promotion boundary rebuilds exactly once; paging cost is independent of already-consumed history.

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
| P1 | Global ChangeId dedup and ACK-covered replay-cache pruning — implemented; receipt-path audit remains open |
| P2 | Inbox/InboxAuthors receipt consolidation, APPLY payload dedup, receipt/relay split, bounded paging |
| P3 | Durable pre-publication PendingLocal coalescing |
| P4 | Versioned compact peer-state and FLD Outbox encodings |
| P5 | Safe GC and snapshot/new-peer contract |
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
