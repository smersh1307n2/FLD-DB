# Sync Storage Compaction Roadmap — FLD-DB + BIN-SYNC

**Date:** 2026-11-07  
**Status:** agreed target architecture and implementation roadmap; no code or wire-format change is authorized.  
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

## 3. Implementation roadmap

### Phase 0 — Contracts and baseline measurements

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

### Phase 5 — Optional WAL-backed local Outbox

Only after Phases 2–4. While a WAL segment is retained, Outbox may reference immutable WAL payload. Before WAL truncation, promote only still-unacknowledged payloads. ACKed entries need not be copied. Measure pinned-segment impact; reject if peak or steady-state storage/checkpoint behavior worsens. This phase is optional.

### Phase 6 — Safe GC and snapshot/new-peer semantics

- Define independent safe horizons for ChangeId dedup, batch receipts, peer replay state and relay payload.
- Relay payload GC requires a validated downstream watermark.
- Specify whether new peers need current-state bootstrap only or historical relay/event provenance too.
- Snapshot is not proof that LWW losers or historical relay operations were delivered without a per-author cut/tail proof.
- No TTL-only deletion of ChangeId identity or opaque ProtocolStateDelta.

**Exit:** each GC decision has a durable watermark/epoch/fence or explicit protocol horizon; new-peer bootstrap is not conflated with history delivery; no crash or alternate route can resurrect compacted duplicates or lose required relay history.

## 4. Priority order

| Priority | Work |
|---|---|
| P0 | Baseline measurements and invariants |
| P1 | Global ChangeId dedup, replay-cache pruning, receipt-path audit |
| P2 | APPLY payload dedup, receipt/relay split, bounded paging |
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
- FLD Storage Core Architecture owns the neutral physical engine and two-DLL dependency boundary.
- FLD Kernel Specification remains normative for current format/protocol compatibility; this roadmap does not amend its wire contract.

## 7. Change discipline

The original audit records FLD-DB implementation baseline 196628a49d6034559a7041237af7feec36fd9b20 (audited branch ref b488dafcfda81d6383717ceaf05a3717a3959923) and BIN-SYNC audited code baseline dd2dd969d70b6fd46371679903855a69ae9399c3. Later commits exist; before each code phase, re-audit current branch HEAD and compare it with the audited source baseline.

Every implementation batch must state exact code baseline and files changed, add focused correctness/crash tests, run relevant tests/benchmarks, report measured disk/RAM/CPU separately from estimates, and update this roadmap plus the owning contract.

**Current status:** target architecture and order are documented. No code, persistence format or wire format is changed by this roadmap.
