> **BIN-SYNC v3.0.0 alignment (2026-11-08):** This roadmap is the FLD storage/compaction work log. For protocol ownership, BIN-SYNC-SPECIFICATION.txt v3.0.0 is authoritative. FLD OutboxSeq, FLD ApplyJournal JournalSeq and WAL CommitSeq are local coordinates, not OriginSequence. BIN-SYNC Metadata Store allocates OriginSequence at durable freeze and owns published ChangeId/Origin, receipts, per-peer delivery state and relay payload. Do not add a duplicate PendingLocal journal or a second authoritative per-source relay history. Tombstones/ghosts remain until the independent v3 membership/full-resync proof authorizes reclamation; TTL/HLC age alone is insufficient. The two runtime DLLs remain Fld1.Storage.dll and Fld1.Database.dll.

# Sync Storage Compaction Roadmap — FLD-DB + BIN-SYNC

**Date:** 2026-11-08  
**Status (2026-11-08):** BIN-SYNC v3.0.0 implementation branch `feature/fld-external-storage` is at exact green code/test head `7cfddd45dec342f916a08f77a9521c831f6f8af7`: Core 665/665, Fuzz/Lab 31/31, Storage Integration 168/168 independently on Ubuntu, Windows and macOS; in-process/TCP/ScenarioHarness samples and the coverage threshold pass. The Durable Metadata Bench workflow completed successfully, including the one-million-row freeze benchmark: 1,024 local operations over 1,000,000 existing ChangeId rows measured 218.1 µs / 7.77 KB. The earlier 196.9 µs / 7.77 KB measurement is faster; no optimization claim is made from the latest run. BIN-SYNC now includes the shared Core ProviderMutation codec/streaming FULL_REPLACE digest builder, generation-scoped provider staging/switch and conservative receipt-reservation retirement proof. Proposed signed namespace roster/frontier/checkpoint and all-member FULL_REPLACE ACK design is documented in `docs/BIN-SYNC-v3.1-ROSTER-GC-ADDENDUM.md`; it is not implemented, so ChangeId and tombstone/Ghost GC remain disabled. Latest FLD-DB code/test head `fd9f48f5ef7ec181bcacf2891bc45d8c1c11b28c` passes 169 tests independently on Ubuntu, Windows and macOS, including 37/37 `Fld1.Storage.Tests` on each OS. Storage source was ported into the authoritative FLD project, then vendored byte-for-byte into BIN-SYNC from source commit `a3abab3e0197f98aac12e9fdafcbfa204d9fbd77`; the BIN-SYNC integration workflow passed again at `7cfddd45dec342f916a08f77a9521c831f6f8af7`. The Storage Core has cross-process single-owner fencing, path-alias handling, deterministic WAL/snapshot publication fault points, injected partial-write/fsync/directory-sync failures, explicit Unix directory-entry fsync and crash/reopen coverage; Windows directory-entry durability is not claimed. This is not yet multi-application shared-file access. Remaining cross-repository work: complete FLD Database persistence migration onto the two-DLL Storage Core; implement the FLD Shared Engine/IPC path required for multiple applications to use one physical database file; implement the FLD JournalSeq provenance adapter and atomic publication gate; implement FLD-specific durable PendingLocal/freeze recovery; extend injected crash states to partial OS writes, device/fsync failures and power-loss; add controlled million-row/deployment-hardware benchmarks; implement the negotiated v3.1 roster/frontier/retention protocol and prove receipt/ChangeId/tombstone reclamation. The BIN-SYNC outbound cutover is a provider-feed/Metadata Store milestone; it does not by itself complete FLD Database integration or FLD's durable PendingLocal transaction protocol.  
**Primary audit:** [Sync History Storage Audit](SYNC-HISTORY-STORAGE-AUDIT.md).  
**FLD normative design:** [FLD Kernel Specification v1.5.9](FLD-KERNEL-SPEC-v1.5.9.md) and [FLD Storage Core Architecture](FLD-STORAGE-CORE-ARCHITECTURE.md).  
**BIN-SYNC normative protocol:** [BIN-SYNC CORE v3.0.0](https://github.com/smersh1307n2/BIN-SYNC/blob/feature/fld-external-storage/BIN-SYNC-SPECIFICATION.txt). Proposed signed roster/frontier/retention extension: [BIN-SYNC v3.1 Roster/GC Addendum](https://github.com/smersh1307n2/BIN-SYNC/blob/feature/fld-external-storage/docs/BIN-SYNC-v3.1-ROSTER-GC-ADDENDUM.md). v3.0 receipt GC is conservative; ChangeId and tombstone/Ghost GC remain disabled pending v3.1.  
**Cross-layer contracts:** [BIN-SYNC Metadata Storage Architecture](https://github.com/smersh1307n2/BIN-SYNC/blob/feature/fld-external-storage/docs/METADATA-STORAGE-ARCHITECTURE.md), [Metadata Store Semantic Contract and Conformance Plan](https://github.com/smersh1307n2/BIN-SYNC/blob/feature/fld-external-storage/docs/METADATA-STORE-CONFORMANCE.md), [Data Provider Semantic Contract and Conformance Plan](https://github.com/smersh1307n2/BIN-SYNC/blob/feature/fld-external-storage/docs/DATA-PROVIDER-CONFORMANCE.md), [Change Staging and Retention](https://github.com/smersh1307n2/BIN-SYNC/blob/feature/fld-external-storage/docs/CHANGE-STAGING-AND-RETENTION.md), and [FLD-DB Integration Contract](https://github.com/smersh1307n2/BIN-SYNC/blob/feature/fld-external-storage/docs/FLD-DB-INTEGRATION-CONTRACT.md).  
**Retention research:** [Tombstone Retention and Safe Garbage Collection](TOMBSTONE-RETENTION-RESEARCH.md) — Microsoft ForgottenKnowledge, CRDT causal stability/commitment, Cassandra, CouchDB and Couchbase. These sources support the E-25 design; the normative protocol rule remains BIN-SYNC E-25.

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

### 2.7 Multi-application access to one database file (binding product requirement)

The original FLD design explicitly requires multiple independent applications/processes to use one physical database file. `FLD-KERNEL-SPEC-v1.5.9.md` §0.1–0.2 and PART III specify one FLD Shared Engine process per physical database, with application clients connected over IPC/shared memory. The Engine is the only owner of mutable WAL/manifest/L0/compaction state; clients submit writes to it, group commit is performed by the Engine, and immutable published runs are read through stable read-only views. This requirement remains binding after Storage extraction.

This does **not** mean multiple processes independently call direct `StorageDatabase.Open` on the same partition. Direct embedded mode remains a single-owner mode protected by the OS writer fence; a competing direct opener must fail before WAL/snapshot recovery. Multi-process shared-file mode must route every client through one Engine owner. Do not remove or weaken the writer fence to simulate concurrency.

The Shared Engine Host is an executable/process, not a third runtime DLL. Keep exactly `Fld1.Storage.dll` and `Fld1.Database.dll`: generic Storage IPC/client capability belongs in Storage; FLD-facing client/facade belongs in Database. The old `Fld.Client.dll` label in the v1.5.9 conceptual diagram denotes the client role and must not become a third distributable runtime assembly. Storage-only consumers must be able to connect without loading Database; full FLD clients use Database over Storage. Dependency remains Database → Storage only.

**Implementation status:** architecture requirement/design recovered and now cross-referenced from `FLD-STORAGE-CORE-ARCHITECTURE.md`; current code implements direct single-process Storage plus cross-process single-owner fencing only. The existing `Fld1.Engine` project is explicitly a single-process engine; `FldDatabase.Open` still composes legacy Engine/WAL/Run/Manifest projects directly. The new `Fld1.Storage` project is not yet the persistence backend of `FldDatabase`, and the source tree contains no Shared Engine Host, IPC client or SHM client implementation. The two-DLL boundary is an approved architecture/documentation decision, not yet the actual public project/package layout. Shared Engine bootstrap/discovery, versioned IPC, bounded request/session lifecycle, group-commit request correlation, stable snapshot/MMF exposure, client reconnect/ambiguous-commit recovery, Storage migration and two-process integration tests remain unimplemented. Do not claim multi-application shared-file access complete until these gates pass.

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


### Phase 3 — Durable local PendingLocal coalescing (design gate; implementation deferred)

- Add durable staging keyed by canonical scope/namespace/record/field after the neutral Data Provider/Metadata Store boundaries and Storage transaction API are defined.
- Coalesce only local LWW operations never exposed to a sender.
- Use canonical LWW comparison, not arrival/lock order.
- Keep pending payload independent of current visible winner because remote APPLY may win before freeze.
- Freeze atomically into immutable Outbox before send.
- Preserve event/CRDT/non-coalescible operations and explicit audit-history requests.

**Exit:** multiple commits to one field before publication produce one pending operation; crash before/during freeze yields complete pending or complete frozen state, never loss/duplication; remote winner does not erase eligible pending local operation; published operations are never rewritten.

### Phase 4 — Versioned compact physical encodings and two-DLL Storage extraction

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
| P0 | Revalidate all normative/design/audit documents and establish reproducible disk/RAM/CPU baseline; documentation reconciliation is complete, quantitative workload baseline remains open |
| P1 | BIN-SYNC global ChangeId dedup, ACK replay pruning, unified InboxReceipt, compact peer progress, grouped persistence v10–v12, compact local WAL markers and admission caps — implemented; receipt/dedup GC remains open |
| P2 | FLD APPLY v3 single-encoding, relay-only journal, sparse paging, offset-only batch index, explicit scan cursor, per-author ACK sidecar and post-WAL-seal relay payload compaction — implemented; receipt GC/index bounds and cross-system provenance adapter remain open |
| P3 | Provider-independent Metadata Store contract/conformance — 20 semantic tests plus durability-profile assertion pass in the volatile reference model; no durable store/restart guarantee yet |
| P4 | Data Provider semantic contract and six volatile conformance tests are now recorded; remaining: durable provider/reopen tests, end-to-end feed-to-metadata staging boundary and dedicated PendingLocal crash/relay conformance |
| P5 | Refactor BIN-SYNC persistence behind Metadata Store; extract exactly two FLD runtime DLLs and implement generic Storage-only semantics/Metadata Store adapter |
| P6 | Complete synchronized-FLD Database provenance mapping, publication gate and ACK translation; then implement durable PendingLocal and versioned compact FLD Outbox encoding with measurements |
| P7 | Safe receipt/dedup/tombstone GC and snapshot/new-peer contract; optional WAL-backed Outbox only after retention proof |

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

#### 7.15.1 PendingLocal semantic details (implementation contract)

The implementation must preserve the following distinctions; none may be inferred from the current visible field winner:

- **Canonical key:** `(namespaceId, RecordId, FieldId)`. There is at most one pending local LWW operation per key. Field tombstones are ordinary LWW operations for this purpose and may replace an earlier unpublished value for the same key.
- **Stage ordering:** every durable `CommitPending()` assigns a monotonically increasing namespace-local `StageSeq`. The staged value retains its original HLC, local author, key epoch, tombstone bit and owned payload. Replacement uses the same canonical `FieldResolver.IsNewer` ordering as current-state resolution; arrival order alone is not a substitute for the comparator. The winning staged value records the StageSeq that supplied it.
- **Independent payload plane:** PendingLocal is not reconstructed from the visible overlay or Runs. A remote APPLY may become the visible winner while an older local operation remains pending; that local payload is still a required outbound operation and must survive checkpoint/reopen until freeze.
- **Freeze boundary:** `FreezePending()` captures a fixed `stageThrough` watermark and freezes every current staged winner at or below it under the database commit lock. It assigns final ChangeId and OutboxSeq only at freeze. The durable freeze record contains the exact immutable operation payloads and the stage watermark. Recovery must replay the freeze idempotently into Outbox, then remove only pending entries covered by that watermark. No operation at or below the watermark may be re-created as a new ChangeId on retry.
- **Mixed immediate commit:** existing `Commit()` remains immediate. If pending entries exist, it freezes the pending winners together with current dirty local writes in one publication transaction. Dirty writes are compared against staged values by the canonical LWW comparator for the same key; the final local winner is published once. This must not compare a pending local operation against a later remote visible winner and silently discard it.
- **Transport visibility:** every operation that exposes outbound data or a high watermark (`ReadOutboxBatch`, `VisitOutboxBatch`, `AcquireOutboxLease`, `GetOutboxHighWatermark`) first completes the freeze transition. ACK and compaction APIs do not implicitly freeze. This prevents a sender from observing a watermark that excludes committed staged work.
- **Non-coalescible operations:** remote relay operations and future CRDT/event operations bypass PendingLocal's LWW map. They retain their original identity and independent relay retention lifecycle.

The crash matrix in §7.15 is mandatory for the implementation. Add explicit assertions for: exact freeze retry preserving ChangeId/OutboxSeq; partial or conflicting freeze retry failing closed; a remote winner not deleting a pending local payload; a staged tombstone replacing an earlier staged value; and a checkpoint with pending entries but no visible overlay. The snapshot's generation, checksum and stage watermark are authoritative together; a missing or mismatched referenced snapshot must fail closed rather than silently start with an empty pending map.
The baseline half is now executable: `PendingLocalBaselineTests` proves across close/reopen that four separate immediate commits produce four distinct immutable Outbox operations with stable sequence order, while four Put calls inside one commit already coalesce to one final value. `SyncApplyTests` also verifies the adjacent ownership invariant across close/reopen: a local dirty write remains a local-author Outbox operation even when a later remote APPLY wins the visible field state, and the remote relay remains independently retained. These are compatibility/crash-recovery baselines, not PendingLocal cross-commit coalescing. `SyncApplyTests` also verifies the adjacent ownership invariant across close/reopen: a local dirty write remains a local-author Outbox operation even when a later remote APPLY wins the visible field state, and the remote relay remains independently retained. This is a compatibility/crash-recovery baseline, not PendingLocal cross-commit coalescing. GitHub Actions run [42350334573](https://github.com/smersh1307n2/FLD-DB/actions/runs/42350334573) passed the new baseline tests and the full .NET 10 suite at commit `77236494106bd8b3b48ff4fa17dd17c6993cc5d8`.

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
- FLD-DB fix/crash-consistency-outbox: latest verified code baseline `81cb93fae0d6` (the later `eacff36` and subsequent commits in this checkpoint sequence are documentation-only).
- BIN-SYNC feature/fld-external-storage: latest verified code baseline `b3d652236290` (the later `d2e0313` and subsequent commits in this checkpoint sequence are documentation-only).
- No adapter implementation or source-code changes are present after those code baselines. Existing public APIs and FLD journal/ACK implementation are the starting point, not a completed adapter.

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
3. **Initial semantic slice implemented and extended:** BIN-SYNC now has a provider-independent test adapter contract, inherited conformance suite and volatile reference adapter; twenty semantic tests plus a separate volatile-profile assertion pass. Actions run `42359081094` passed all 630 tests, public samples, 72.2% line coverage and fuzz/lab hardening at commit `992305bc53f7cc06e85f75a6d46fbe860f454c93`. The suite now covers frozen Outbox/lease fencing, safe outbound ACK, snapshot read-view stability under logical append/ACK/compaction and pending-apply provider-token/finalization semantics. The reference adapter explicitly provides no restart durability: physical snapshot-generation pinning and crash/reopen persistence remain unproven. The helper assembly is referenced by the BIN-SYNC test project, not run as a standalone solution test target.
4. **Data Provider contract and initial tests added:** BIN-SYNC `docs/DATA-PROVIDER-CONFORMANCE.md` now defines stable source mapping, durable local feed, snapshot/feed watermark barrier, idempotent remote apply, provider-side apply receipts and local/remote-origin separation. A test-only `IDataProviderConformanceAdapter` and volatile reference provider exercise six semantic scenarios. Combined BIN-SYNC Actions run `42360397431` passed 637 tests, public samples, fuzz/lab hardening and 72.5% line coverage at commit `504a09092302f137d222421f6929533b95188318`. The reference provider is not durable; end-to-end feed ACK after durable staging and real provider reopen/fault-injection remain open. PendingLocal coalescing remains a separate provider/FLD staging contract and is not covered by either current volatile suite.
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

**Exit condition for this phase:** the volatile harnesses now cover initial Metadata Store semantics (20 tests) and Data Provider semantics (six tests), including outbound freeze/lease/ACK, logical snapshot view, pending-apply retry, source-feed paging, snapshot/feed watermark, idempotent remote apply and local/remote-origin separation. They do not prove durable restart, physical snapshot-generation pinning or real provider idempotency. The neutral Data Provider contract is documented; the next open contract/test gate is PendingLocal/source-page staging, LWW coalescing and freeze-retry crash/relay behavior. Then refactor BIN-SYNC persistence behind the boundary and require durable adapters to pass reopen/fault-injection tests. Existing Node/WAL behavior remains a regression baseline.


### 7.21 Cross-repository documentation map and authority (2026-11-07)

Storage optimization is specified across multiple documents. This map prevents implementation work from omitting a contract or treating a research note, historical addendum or stale audit statement as normative.

#### 7.21.1 Source-of-truth map

| Document | Authority / responsibility | Use in this roadmap |
|---|---|---|
| BIN-SYNC `BIN-SYNC-SPECIFICATION.txt` — **v2.4.19 FINAL** | Normative wire protocol and protocol-level identity, ACK, snapshot, ChangeId and tombstone-retention rules; E-24 and E-25 are active | Check before changing protocol-visible identity, cursor, dedup, snapshot or GC behavior. No storage optimization may weaken E-24/E-25. |
| FLD-DB `FLD-KERNEL-SPEC-v1.5.9.md` | Normative FLD1 physical format and FLD database/engine behavior, including WAL, APPLY, snapshots, digest boundaries and BIN-SYNC integration obligations | Defines what FLD persists and which semantics belong to Database rather than generic Storage. Must align with the current BIN-SYNC protocol. |
| FLD-DB `FLD-STORAGE-CORE-ARCHITECTURE.md` | Binding assembly/dependency decision and extraction map | Exactly two runtime DLLs: `Fld1.Storage.dll` and `Fld1.Database.dll`; Database → Storage only; no third Common DLL; Storage-only consumers must not load Database or BIN-SYNC. |
| BIN-SYNC `docs/METADATA-STORAGE-ARCHITECTURE.md` | Cross-role architecture: Data Provider, Metadata Store, Physical Storage and Runtime; scope/identity inventory; transaction modes and implementation sequence | Governs the Metadata Store boundary and shared-transaction versus separate-store recovery. It is not a physical Storage format specification. |
| BIN-SYNC `docs/CHANGE-STAGING-AND-RETENTION.md` | Logical change lifecycle, safe LWW coalescing, immutable freeze boundary, payload ownership, metadata cardinality and independent retention | Governs pending versus frozen changes, one-payload/many-peers model, and which metadata can be compacted independently. |
| BIN-SYNC `docs/FLD-DB-INTEGRATION-CONTRACT.md` | Specialized contract when FLD Database is the synchronized Data Provider/transaction owner | Governs Outbox/ApplyJournal cursor domains, durable relay provenance mapping, publication gate, ACK translation and atomic APPLY. Does not define Storage-only mode. |
| BIN-SYNC `docs/METADATA-STORE-CONFORMANCE.md` | Provider-independent semantic contract and executable conformance requirements | Governs required behavior and crash/reopen tests before a durable Metadata Store or provider adapter can be called conformant. Existing Node tests remain regression tests, not a substitute. |
| BIN-SYNC `docs/DATA-PROVIDER-CONFORMANCE.md` | Provider-side source mapping, durable feed, snapshot/feed barrier, remote apply idempotency and origin separation | Governs Data Provider behavior independently from Metadata Store semantics; its volatile reference tests do not prove durable provider recovery. |
| FLD-DB `docs/SYNC-HISTORY-STORAGE-AUDIT.md` | Code-backed findings, logical representation accounting, source hot spots and optimization candidates | Evidence and implementation audit. Estimates are not benchmarks; recommendations become binding only when accepted into this roadmap or a normative contract. |
| FLD-DB `docs/TOMBSTONE-RETENTION-RESEARCH.md` | External research and rationale for safe deletion/forgotten-history design | Research input to E-25 and Phase 5; it does not independently authorize tombstone, receipt or dedup GC. |
| BIN-SYNC `docs/PUBLIC-API.md` | Shipped BinSync.Core 3.0.0 public API and persistence behavior | Distinguishes the current NodePersistence/WAL API from the planned Metadata Store/Data Provider boundaries; the new contracts are test-only and are not shipped public APIs. |
| BIN-SYNC `docs/PRODUCTION-STATUS.md` and `docs/CHANGELOG.md` | Implementation/release status and historical change trace | Useful for checking current implementation state and migration history; neither overrides the normative protocol or semantic contracts. |

Historical BIN-SYNC `BIN-SYNC-v2.3.7-ADDENDUM.txt` and `docs/ERRATA-v2.3.7.md` are retained for provenance only. Their requirements were consolidated into the current specification; do not apply them as a second independent normative layer over v2.4.19.

#### 7.21.2 Authority and conflict resolution

Resolve conflicts by subject:

1. **Wire/protocol semantics:** current BIN-SYNC specification v2.4.19 FINAL (including E-24/E-25).
2. **FLD file format and FLD-owned semantics:** FLD Kernel Specification, constrained by the two-DLL Storage Core architecture.
3. **Cross-component API/transaction behavior:** Metadata Store Conformance for generic provider semantics; FLD-DB Integration Contract for the synchronized-FLD-specific path.
4. **Logical storage lifecycle and minimization:** Change Staging and Retention.
5. **Execution order and acceptance gates:** this roadmap.
6. **Evidence and rationale:** Sync History Storage Audit and Tombstone Retention Research.

A code audit can reveal that implementation has changed, but cannot silently amend a normative contract. A design document also does not prove that code or tests implement it. Update the relevant contract and matching tests whenever an implementation changes an observable guarantee.

#### 7.21.3 Storage optimization topics that must remain connected

- **Metadata cardinality:** one shared operation payload for many peers; per-peer/per-author progress; compact issuer-specific batch receipts; route-independent global ChangeId ledger; bounded outgoing replay mappings; separate local WAL replay markers.
- **Pending/frozen lifecycle:** coalesce only unpublished local LWW changes; preserve pending payload independently from the current winner; freeze atomically; never rewrite a published Outbox entry.
- **Payload amplification:** APPLY_COMMIT v3 stores incoming operations once; relay selection uses indexes; ApplyJournal retains receipt metadata plus relay-only payload; relay payload ACK/compaction is independent from receipt retention.
- **Physical encoding:** compact peer progress, receipt grouping, author/sequence/CommitSeq encoding, bounded indexes, sparse journal anchors, versioned formats and legacy readers. Each candidate requires byte accounting and workload benchmarks before adoption.
- **Retention:** separate lifetimes for Outbox payload, relay payload, Inbox receipts, global ChangeId facts, replay cache, WAL replay markers, peer membership and forgotten frontier. No one ACK, snapshot completion, TTL or HLC timestamp is a universal GC proof.
- **Provider/Storage boundaries:** Data Provider captures and applies user changes; Metadata Store owns BIN-SYNC durable state; Physical Storage owns generic atomic byte durability. Do not conflate Storage-only metadata mode with synchronized FldDatabase mode.
- **Crash consistency:** every freeze, incoming acceptance, pending-apply transition, snapshot lifetime, provenance page, ACK and compaction boundary needs deterministic fault injection and restart assertions.

#### 7.21.4 Reconciliation findings and corrections

1. **Corrected in this documentation pass:** FLD Kernel Specification now names BIN-SYNC v2.4.19 FINAL and explicitly includes E-24/E-25. FLD physical format version remains FLD1 binaryVersion 1; this compatibility update does not itself change the FLD1 Run/WAL format.
2. **Corrected in this documentation pass:** FLD Kernel Specification now states the binding two-DLL boundary: `Fld1.Storage.dll` and `Fld1.Database.dll`, with Database → Storage dependency and no third Common/Shared runtime DLL.
3. **Corrected in this documentation pass:** Sync History Storage Audit now distinguishes the implemented FLD per-author JournalSeq ACK sidecar and post-WAL-seal payload compaction from the not-yet-implemented cross-system JournalSeq → ChangeId → OutboxSeq provenance adapter, minimum peer-ACK translation and publication gate.
4. The provenance mapping, atomic scan-cursor persistence and outbound publication gate remain a separate integration gate as specified in §7.19 and FLD-DB Integration Contract §3.1.1–3.1.2. The audit is evidence, not a substitute for the contract or crash tests.


### 7.22 PendingLocal checkpoint and recovery boundary (2026-11-07)

The source audit of `FldDatabase`, `WalManager`, `OutboxLog` and the manifest confirms that a PendingLocal implementation cannot be represented as an ordinary additional WAL record and then rely on the existing Run checkpoint. A Run contains the visible field winners; it does not preserve a staged local payload that has subsequently lost the visible LWW comparison to a remote APPLY. Existing checkpoint logic may seal and truncate the WAL prefix once its Run/ApplyJournal effects are materialized. Therefore, an unpromoted staged operation must have an independent durable recovery representation before it can safely survive checkpoint.

#### 7.22.1 Required separation

- Visible field state and PendingLocal are separate logical state planes. A remote APPLY may replace the visible winner but must not erase or rewrite the staged local operation.
- PendingLocal is keyed by the canonical field key `(NamespaceId, RecordId, FieldId)`; its stored winner is selected by the existing `FieldResolver.IsNewer` ordering, not arrival order.
- The PendingLocal checkpoint watermark is a StageSeq (or equivalent staging generation), not WAL CommitSeq, Run SeqHi, OutboxSeq or a peer-relative cursor. These sequence domains must not be conflated.
- A visible Run checkpoint must not claim to cover PendingLocal merely because both refer to the same field key.

#### 7.22.2 Snapshot publication and reclamation protocol

Any compacted PendingLocal snapshot must be immutable and self-validating. Its durable identity must include at least namespace, monotonically increasing generation, covered StageSeq watermark, record count/length and checksum (or a cryptographically strong content digest if already available in the storage layer).

The required publication order is:

1. Write the complete new snapshot to a temporary generation file and flush its contents to stable storage.
2. Atomically publish a manifest reference containing the generation, covered StageSeq watermark and checksum; flush the manifest and its containing directory where supported.
3. Only after the manifest reference is durable may covered staging-log segments/records be reclaimed.
4. On open, load exactly the generation referenced by the manifest and validate its identity, watermark, length/count and checksum before accepting it as the recovery base.
5. If a referenced generation is missing, truncated or mismatched, fail closed. Do not silently fall back to an older generation or an empty pending set if later staging records may already have been reclaimed.

A marker stored only in a WAL segment is insufficient as the sole snapshot reference: that segment may itself be removed by a later WAL rotation/truncation. The manifest (or an equivalent independently durable root) must own the current snapshot reference.

#### 7.22.3 Freeze/replay interaction

- Freeze must capture a fixed StageSeq watermark while holding the database serialization boundary. Concurrent later staging belongs to a later freeze.
- The immutable freeze intent must carry enough information to reproduce the exact operation identities and payloads after a crash. Replaying the same intent must not allocate new ChangeIds or OutboxSeq values.
- Promotion to Outbox must be idempotent across crashes before Outbox append, after append but before its durability flush, and after Outbox durability but before pending-state cleanup.
- Pending entries may be removed only when the durable freeze record proves that their StageSeq is covered by the completed immutable promotion. A later staged replacement must remain pending.
- Outbox publication APIs must not expose a high watermark/page/lease that omits eligible pending work merely because no explicit caller invoked freeze. The API gate and its lock ordering must be specified and tested before enabling the optimized path.

The exact physical representation (shared Storage transaction, independent staging journal plus recoverable promotion record, or another design) remains an implementation choice. It must satisfy the above ordering and pass fault-injection/reopen tests. This section does not authorize a new FLD1 Run or WAL binary-format version by itself.

#### 7.22.4 Minimum crash matrix before implementation is considered complete

- crash during snapshot write; old manifest root and old log remain authoritative;
- crash after snapshot flush but before manifest publication; orphan snapshot is ignored/reclaimed only after validation;
- crash after manifest publication but before old-log reclamation; recovery loads the new snapshot and safely tolerates the retained covered log;
- missing/corrupt referenced snapshot; open fails closed;
- crash before freeze intent durability; pending state remains unchanged;
- crash after freeze intent durability but before Outbox append; recovery promotes the exact intent;
- crash after Outbox append but before flush; recovery deduplicates and completes the same promotion;
- crash after Outbox flush but before pending cleanup; recovery does not duplicate or re-sequence the operation;
- later staged replacement survives replay of an earlier freeze watermark;
- remote APPLY winning the visible field does not change the pending snapshot or its payload;
- checkpoint/rotation with pending entries and no corresponding visible overlay preserves those entries;
- repeated open/checkpoint/reopen cycles preserve the same pending winners and frozen ChangeIds/OutboxSeq values.

**Implementation gate:** do not add `CommitPending` or alter the public commit semantics until the snapshot root, staging-log retention and freeze-to-Outbox replay protocol are implemented together. The current immediate `Commit()` path remains the compatibility baseline.


### 7.23 Generic Storage core: first executable implementation slice (2026-11-07)

The separate `Fld1.Storage.dll` project now contains a domain-neutral WAL and an initial byte-key/value engine. It has no project reference to `Fld1.Core`, `Fld1.Engine`, `Fld1.Wal`, BIN-SYNC or FLD Database.

Implemented surface:
- opaque non-zero WAL record kinds and arbitrary payloads; generic partition identity rather than FLD namespace identity;
- atomic byte-key/value batches with deterministic key ordering and last-write coalescing inside one batch;
- durable append+flush before publishing in-memory state;
- optimistic batch conflict detection and read-your-writes;
- immutable in-memory read snapshots and ordered prefix scans;
- immutable on-disk snapshot generations rooted by an independently atomic manifest;
- checkpoint order: flush/rotate WAL → write and flush snapshot → publish and flush manifest → truncate only fully covered sealed WAL segments;
- missing or corrupt referenced snapshot/manifest fails closed;
- recovery skips only WAL records already covered by the validated snapshot and requires the remaining CommitSeq chain to be contiguous.

The Storage test project currently covers 12 cases: opaque WAL replay and rotation, durable batch recovery, repeated-key coalescing, delete recovery, partition isolation, stable snapshot/prefix paging, checkpoint/reopen, repeated checkpoint generations, and missing/corrupt snapshot failure.

CI on FLD-DB commit `81e95fbd0e763ee5e6660ab09fa16cc8fe71ccce` passed: Storage 12/12 and the existing FLD test projects remained green. BIN-SYNC's pinned integration snapshot was compiled in its own workflow; its durable Data Provider conformance adapter passed 8/8 and the Storage-backed array synchronization sample returned PASS.

**Not yet complete:** the new Storage engine is not yet the persistence backend of the existing FLD Database engine; current FLD Engine/WAL/Run/Manifest projects remain in place. The next FLD-side work is to harden Storage checkpoint/fault-injection coverage, define the production transaction/snapshot API boundary, and then migrate Database-owned persistence without introducing a third runtime DLL or breaking FLD1 format compatibility. The cross-repository Metadata Store adapter remains an open integration gate as recorded in BIN-SYNC `METADATA-STORAGE-ARCHITECTURE.md`.


#### 7.23.1 Cross-repository metadata integration update

The BIN-SYNC integration branch now also has a durable Metadata Store conformance adapter backed by `Fld1.Storage`. Its physical layout stores each receipt, cursor, ChangeId fingerprint, relay/outbound item, ACK/high-watermark, lease, pending-apply intent and completed receipt under a separate logical key. Each mutation computes a key-level diff and commits the changed entries atomically; it does not rewrite the entire partition payload.

The BIN-SYNC integration workflow reports 30/30 passing tests across Data Provider and Metadata Store conformance plus restart/compaction coverage. The Storage-backed array example also completes a two-replica exchange, closes/reopens both Storage and Node state, applies a later update and reports PASS.

The adapter is still test-only. Public production APIs and direct Node runtime integration remain outstanding; do not mark the overall BIN-SYNC + Storage integration complete until those APIs, fault-injection/reopen tests and production array example are in place.

#### 7.23.2 Snapshot-generation reclamation

After the new snapshot file is flushed and the atomic manifest has been published and directory-flushed, `StorageSnapshotFile.Store` now reclaims obsolete snapshot generations for that partition. This is safe for the current API because `StorageSnapshot` owns an in-memory copy rather than a disk-generation pin. A crash before manifest publication leaves the old root authoritative; a crash after publication leaves the new root authoritative and may leave only reclaimable orphan files. The repeated-checkpoint test asserts that one current snapshot generation remains.


#### 7.23.3 BIN-SYNC public runtime adapter milestone

The BIN-SYNC integration branch has promoted the provider and metadata-store contracts into `BinSync.Core` as public `IDataProvider` and `IMetadataStore` interfaces. Runtime implementations are in `BinSync.Storage.dll`, which references Core and the neutral `Fld1.Storage.dll`. The public `StorageSyncCoordinator` enforces local feed → Node write with original HLC/author → caller's durable Node checkpoint → provider ACK; remote field/tombstone materialization uses deterministic apply tokens and does not enter the local feed.

The public array sample now exercises the runtime adapters and coordinator across two nodes, two independent Storage partitions, close/reopen and a later remote update. BIN-SYNC integration CI passed 32/32 tests at the current adapter milestone.

**Boundary still open:** the legacy Node/Transport path has not yet been migrated to use `IMetadataStore` for its own freeze, receive/relay, ACK/lease and Pending Apply transitions. The explicit coordinator does not replace that protocol-owned metadata path. Complete the migration only with wire-compatibility, restart and fault-injection coverage; do not claim automatic NodeBuilder/Transport integration yet.

### 7.23.4 Storage-backed real TCP array sample (2026-11-07)

The BIN-SYNC integration branch now includes `samples/StorageArrayTcpSync`, executed by the FLD Storage Integration workflow. Unlike the earlier `StorageArraySync` sample, this one uses `TcpSyncListener`, `TcpSync.ConnectAsync` and the production transport-independent `SyncSession` path over a real loopback TCP connection.

The sample keeps one independent `Fld1.Storage` database per replica and stores the `StorageDataProvider`, `StorageMetadataStore` and complete Node checkpoint in that database. It verifies:

- bidirectional synchronization of two array items with multiple stable fields;
- receiver ordering through the TCP `beforeAck` boundary: Node checkpoint → provider apply → Metadata Store PendingApply finalization → ACK;
- draining all available batches before closing a session (one `PushBatchAsync` call is one batch attempt, not a complete synchronization round);
- close/reopen of both databases and Node checkpoints, then a later field update on the same author stream;
- record lifecycle tombstone propagation over a subsequent TCP session.

At BIN-SYNC commit `aa5c52e2daa85638326f4df94c0fdec5e312b467`, FLD Storage Integration workflow run [42377636914](https://github.com/smersh1307n2/BIN-SYNC/actions/runs/42377636914) passed: 47/47 integration tests, the existing in-process StorageArraySync sample, and the new real-TCP sample. The TCP sample reported PASS for initial bidirectional exchange, restart + subsequent update, and record tombstone propagation.

**Boundary unchanged:** this proves the public Storage adapters and coordinator can participate in real TCP synchronization with durable reopen. It does not make `IMetadataStore` the authoritative source for Node's own Outbox, receive/relay, ACK/lease or peer-progress state. NodePersistence remains a complete Node checkpoint stored in Storage, and the direct Node runtime migration remains the next architectural gate.


### 7.24 Provider-authoritative outbound publication in BIN-SYNC (2026-11-08)

The BIN-SYNC Storage adapter now has an explicit, durable outbound authority cutover. This advances the cross-repository metadata boundary but does not mark the FLD Database adapter or Phase 3 complete.

Verified implementation on `smersh1307n2/BIN-SYNC` `feature/fld-external-storage`, code/test head `016ddb553a6d2c84abe2027145dfdbd92f229d35`:

- `StorageDataProvider` persists a stable 128-bit ChangeId in each local-feed row and a durable one-way publication cutover watermark.
- Cutover requires a fully acknowledged/empty provider feed and exact alignment between Node and Metadata Store outbound high-watermarks. The Node Outbox sequence becomes a fence after cutover.
- New provider changes are frozen directly into Metadata Store; Metadata Store allocates the author outbound sequence. The provider feed is ACKed only after the metadata freeze commits. Node is rebuilt from a stable provider snapshot as a data cache.
- The coordinator consults the global ChangeId fingerprint ledger on retry. It preserves already-frozen operations and coalesces only the not-yet-frozen suffix, including the case where metadata freeze succeeded, provider ACK failed, and a newer edit arrived before retry. The optional lookup returns a read-only memory view of the immutable fingerprint, avoiding an additional 16-byte copy per already-frozen feed row; this structural allocation reduction awaits BenchmarkDotNet measurement.
- The TCP sample uses the direct Metadata Store transport overload and persists an explicit empty initial peer baseline before sending incremental deltas.
- CI: 644/644 BIN-SYNC core, 103/103 Storage Integration, 31/31 Fuzz/Lab, 70.8% line coverage; both in-process and real-TCP Storage samples pass. The durable `AutoReconcileWithMetadataAsync` entry point now detects the provider cutover marker and rehydrates a stale Node checkpoint from the provider snapshot before anti-entropy. Cutover removes duplicate local-author Outbox payload rows from Node while retaining the sequence fence and WAL/ChangeId replay identities. Tests cover stale checkpoint replacement, local Outbox cleanup, provider ACK interruption, post-ACK checkpoint failure and missing-marker fail-closed behavior.

**Interpretation for the FLD roadmap:** the provider feed can serve as the durable PendingLocal journal for a generic external provider, but this is not yet the FLD Database's atomic pending-staging/freeze protocol. Phase 3 remains open until FLD commits, concurrent remote APPLY, snapshot/checkpoint and Outbox promotion satisfy the StageSeq/freeze-intent crash matrix in §7.22. Do not add a second per-edit metadata table or weaken the rule that frozen operations are immutable.

The next FLD-specific integration gate remains: persist the mapping `(author, JournalSeq, ChangeId) → BIN-SYNC OutboxSeq` with the relay scan cursor and publication gate, then test restart, partial page, ACK interruption, remote winner and compaction/reopen. Keep the exact two-DLL dependency boundary: `Fld1.Database.dll → Fld1.Storage.dll`; no third Common/Shared runtime DLL.


### 7.25 Storage transaction serialization and WAL single-writer fence (2026-11-08)

The BIN-SYNC Storage adapter now serializes all in-process transactions attached to the same `StorageDatabase` object through a shared gate used by `StorageDataProvider`, `StorageMetadataStore` and `StorageNodePersistence.Save`. A concurrency regression runs 64 provider appends alongside 64 metadata freezes and verifies all 128 commits complete without stale-base conflicts.

The generic Storage engine uses a named OS-managed Mutex for each normalized WAL directory/partition before reading the snapshot or replaying WAL. A process-wide manager thread owns/releases named mutexes so disposal can occur on any caller thread. The first FileShare.None lock-file experiment was abandoned after macOS integration jobs stalled while trying to open an already-locked file. The actual abrupt-process-kill regression now records and kills the owning testhost PID (rather than only its `dotnet test` parent); with the named Mutex restored, Storage Integration passes 122/122 on Ubuntu and Windows. The macOS-15 run is queued. The implementation remains BCL-only, with no native interop.

This is a **single-writer partition contract**, not multi-writer concurrency. Remaining validation: independent-process contention/termination tests on each supported OS, symlink/path alias canonicalization, and power-loss cuts around WAL rotation and checkpoint replacement. The shared adapter gate is process-local; separate StorageDatabase objects for the same partition are excluded by the named mutex.


### 7.26 Cross-platform WAL fencing, crash-owner recovery and ChangeId lookup benchmark (2026-11-08)

The follow-up validation closes the cross-process writer-fence and path-alias checks that were open in §7.25.

- `WalWriterFence` now resolves symbolic-link targets through their ancestor path components before deriving the partition lock identity. This handles macOS paths whose symlink target itself contains an aliased ancestor (for example, `/var` → `/private/var`).
- Path identity preserves case-sensitive filesystem distinctions while resolving actual directory-entry casing on case-insensitive filesystems; Windows volume roots are normalized separately.
- The Storage Integration workflow now runs on `ubuntu-latest`, `windows-latest` and `macos-latest`.
- Each OS passed 107/107 Storage Integration tests. The cross-process regression starts a second test host, proves a competing open is rejected while the child owns the fence, terminates the actual owner host, and verifies WAL recovery plus the committed value after reopen. Symlink-alias rejection, same-process rejection and case-alias rejection where supported also pass.
- Both Storage array samples pass on all three OSes, including real TCP bidirectional exchange, restart + later update and record tombstone propagation.
- Verified BIN-SYNC head: `8e6833e5aa5fe7086dbd72b18cf71e45f30169c9`; core 647/647, Fuzz/Lab 31/31, line coverage 70.8%. PR #16 remains Draft/open/unmerged.
- The direct frozen ChangeId fingerprint lookup benchmark at 256/1,024/4,096 ledger rows measured 57.02/57.85/59.57 ns per lookup and 0 B allocated per lookup in GitHub Actions (.NET 10.0.14, Ubuntu 26.04.1, x64). This measures the direct lookup only, not the full coordinator retry scan.

**Still open:** deterministic physical Storage crash/power-loss cuts around WAL append, rotation and checkpoint replacement; end-to-end coordinator retry-scan benchmarks; runtime-wide removal of legacy Node/Transport metadata authority and other-author relay Outbox state; receipt/ChangeId/tombstone GC horizons; cross-implementation conformance; and the FLD Database `JournalSeq` provenance adapter plus atomic publication/freeze integration. Preserve the two-DLL boundary `Fld1.Database.dll → Fld1.Storage.dll`; do not add a third shared runtime DLL.


### 7.27 Coordinator retry allocation baseline and Node snapshot rehydration cost (2026-11-08)

The provider retry benchmark is now an end-to-end coordinator scenario rather than a lookup-only microbenchmark.

- Setup uses the real `StorageDataProvider` and `StorageMetadataStore`; the first publication freezes immutable ChangeIds and then injects a provider ACK interruption.
- The measured retry scans the same still-pending frozen feed, validates the canonical fingerprints, ACKs the feed and refreshes the Node cache from the provider snapshot.
- Results from GitHub Actions run `42439312729` (.NET 10.0.14, Ubuntu 26.04.1, Intel Xeon Platinum 8573C): 256 rows = 2.112 ms / 1.52 MB; 1,024 rows = 12.768 ms / 6.47 MB; 4,096 rows = 35.213 ms / 33.92 MB.
- These are full-path figures, not scan-only figures. They include `InstallProviderSnapshot` → `RestoreProviderSnapshot`, which rebuilds the namespace dictionary, validates uniqueness with temporary sets, materializes operation objects and copies field values. The 1,024-row case was very noisy (about 78.8% relative standard deviation); 4,096 rows was more stable (about 2.5%).
- The separate frozen ChangeId lookup remained allocation-free at all sizes: 81.70/81.78/80.00 ns at 256/1,024/4,096 ledger rows.

**Implementation completed in BIN-SYNC:** ordinary provider-authoritative publication now applies the exact scanned provider-feed prefix incrementally to the Node data cache after Metadata Store freeze, checkpoints Node before provider ACK, and retains full snapshot installation for initial cutover/startup recovery. Copy-on-write staging ensures malformed or failed cache application does not partially publish changed records. ACK retry replays the same HLC mutations idempotently. The first end-to-end incremental retry benchmark is recorded in §7.28; the matched-runner full-snapshot comparison is recorded in §7.30.

**Authority caveat resolved in §7.29:** PendingApply now returns per-operation accepted/duplicate decisions from the Metadata Store ledger. Node applies only new operations in Metadata-Store-authoritative receive mode and does not recreate the global identity ledger or duplicate RemoteBatch WAL record.

### 7.28 Incremental provider-feed application and retry benchmark (2026-11-08)

The per-publication full Node snapshot rebuild identified in §7.27 has been replaced in BIN-SYNC's Metadata-Store-owned outbound path.

- After scanning the provider feed and freezing new immutable operations into Metadata Store, StorageSyncCoordinator applies the exact scanned feed prefix to the Node application-data cache using Node.ApplyProviderChanges.
- The Node method clones only changed records, applies feed operations in source-sequence order using existing HLC/tombstone merge rules, and publishes the changed records only after the full prefix has validated/applied. It creates no Node Outbox entries or duplicate ChangeId identity-ledger rows.
- The coordinator persists the Node checkpoint before acknowledging the provider feed. If checkpoint persistence fails, the provider feed remains pending. If ACK fails, retry recognizes already-frozen ChangeIds, reapplies the same HLC mutations idempotently, checkpoints again and ACKs without reinstalling a full provider snapshot. Provider mutation identity, field/tombstone shape and HLC are preflight-validated before FreezeOutbound; a malformed-provider regression proves invalid input cannot advance the durable outbound watermark.
- Full provider snapshot replacement remains for explicit cutover/startup recovery and stale-checkpoint reconciliation; it is not removed.
- Regression coverage verifies that the Node cache is updated before ACK, ACK failure with a newer edit preserves the already-frozen operation and publishes only the unfrozen suffix, checkpoint failure leaves the feed replayable across restart, and startup rehydration drains the frozen feed before snapshot replacement.
- Verified BIN-SYNC code/test head `e5f7c493d461671c8b0b88940127a702ee2a4dd9`: core 648/648, Fuzz/Lab 31/31, coverage 70.4%; Storage Integration 110/110 on Ubuntu, Windows and macOS; both in-process and real-TCP Storage samples pass on all three OSes. PR #16 remains Draft/open/unmerged.
- Incremental retry benchmark (GitHub Actions run `42440960333`, .NET 10.0.14, Ubuntu 26.04.1, AMD EPYC 9V74): 256 rows = 1.523 ms / 875.93 KB; 1,024 rows = 3.829 ms / 3,516.38 KB; 4,096 rows = 15.869 ms / 14,148.12 KB.
- The first incremental run used AMD EPYC 9V74 while the exploratory full-snapshot baseline (§7.27) used Intel Xeon Platinum 8573C, so that initial cross-runner comparison was directional only. The matched-runner comparison and its performance decision are recorded in §7.30; see BIN-SYNC docs/METADATA-STORE-BENCHMARKS.md for raw reports and caveats.

**Remaining gates:** reduce feed materialization/operation-encoding/copy-on-write allocations if profiling justifies it; deterministic physical Storage crash cuts; legacy Node/Transport state migration and receipt/dedup/tombstone GC; and FLD's `(author, JournalSeq, ChangeId) → BIN-SYNC OutboxSeq` provenance plus atomic staging/freeze integration. Keep exactly two DLLs: `Fld1.Database.dll → Fld1.Storage.dll`; do not add a third shared runtime DLL.

### 7.29 Metadata Store as the sole ChangeId authority for receive (2026-11-08)

The Storage-backed receive path now avoids keeping a second global ChangeId identity ledger in Node.

- `IMetadataStore.BeginPendingApply` returns a read-only per-operation apply mask derived from the durable global `(namespaceId, ChangeId)` ledger: `true` means this exact fingerprint is new; `false` means the operation was already accepted and Node must skip it.
- The decision mask is not persisted as another metadata table. PendingApply itself reserves its ChangeIds until finalization. A second PendingApply with the same namespace/ChangeId is rejected until the first finalizes; the legacy immediate `AcceptIncomingBatch` path checks the same reservation. On restart, `ReadPendingApplyIntents` recomputes the same mask from the durable ChangeId ledger plus exclusive pending reservations.
- `SyncSession.ReceiveBatchAsync` passes the mask from the durable before-apply callback into Node. In Metadata-Store-authoritative receive mode, Node applies only operations marked new, does not read or update Node `GlobalChangeLedger`, and does not append a duplicate `RemoteBatch` record to Node WAL. PendingApply is the protocol WAL/recovery source; provider application and Metadata Store receipt/dedup/relay finalization remain ordered before network ACK.
- The legacy Node-owned receive/transport path still uses Node `GlobalChangeLedger` and Node WAL. This change does not yet remove legacy protocol state or migrate old Node-owned receive history; the two paths remain explicitly separated.
- Conformance regressions verify mixed duplicate/new masks, stable mask reconstruction from pending intents, reservation of unfinalized ChangeIds against both another PendingApply and immediate accept, and conflict rejection. Storage integration verifies metadata receive does not create Node global-ledger rows, lost-ACK replay remains a durable duplicate, and incremental provider cache preserves field/record tombstone ordering.
- Verified BIN-SYNC code/test head `e5f7c493d461671c8b0b88940127a702ee2a4dd9`: core 648/648, Storage Integration 110/110 on Ubuntu/Windows/macOS, Fuzz/Lab 31/31, 70.4% line coverage. PR #16 remains Draft/open/unmerged.

**Remaining gates:** deterministic crash cuts at every boundary from PendingApply staging through Node checkpoint, provider apply, Metadata Store finalization and ACK; legacy Node-ledger/WAL migration or explicit fail-closed fencing before authority switch; physical Storage/power-loss testing; receipt/dedup/tombstone GC horizons; and FLD JournalSeq provenance/atomic staging integration.
### 7.30 Matched-runner full-snapshot vs incremental retry benchmark (2026-11-08)

GitHub Actions workflow run `42444167368` checked out the full-snapshot baseline at `8e6833e5aa5fe7086dbd72b18cf71e45f30169c9` and the incremental implementation at `69ed0b5c964357a3c90552b9d697026d86280447`, then ran both sequentially on the same Ubuntu 26.04.1 / AMD EPYC 7763 / .NET 10.0.14 runner with the same BenchmarkDotNet 0.14.0 configuration.

| Pending provider-feed rows | Full snapshot mean | Incremental mean | Mean latency reduction | Full snapshot allocation | Incremental allocation | Allocation reduction |
|---:|---:|---:|---:|---:|---:|---:|
| 256 | 3.796 ms | 2.308 ms | 39.2% | 1.52 MB | 876.14 KB | 43.7% |
| 1,024 | 7.384 ms | 4.746 ms | 35.7% | 6.47 MB | 3,516.05 KB | 46.9% |
| 4,096 | 40.997 ms | 17.522 ms | 57.3% | 33.92 MB | 14,147.82 KB | 59.3% |

Relative standard deviations were 36.9% / 16.1% / 2.16% for the full-snapshot path and 12.3% / 12.8% / 2.13% for the incremental path at 256/1,024/4,096 rows. The 256-row baseline is noisy; the 4,096-row comparison is stable. BenchmarkDotNet still warns that stateful single-invocation iterations are below its recommended 100 ms window.

**Decision:** retain incremental publication. The matched-runner comparison confirms lower mean latency and managed allocation at all three tested feed sizes, with the strongest stable result at 4,096 rows. This closes the same-runner retry benchmark gate; continue profiling remaining feed materialization, operation encoding and changed-record copy-on-write allocations before further optimization. The full snapshot remains required for initial cutover and startup recovery.

Raw CSV/Markdown reports are attached to BIN-SYNC workflow run `42444167368` as artifact `metadata-retry-comparison`.

### 7.31 HLC-coalesced incremental Node cache and latest cross-platform verification (2026-11-08)

The latest BIN-SYNC code/test head is `010742085147b582ba551e3d2df1a65b0b595394`.

- The incremental provider-feed path now maintains a separate coalesced winner map for the Node data cache, keyed by canonical record, field and record-tombstone identity. For repeated changes to one field, the cache receives only the maximum HLC winner; a later source-feed sequence with an older HLC cannot overwrite a newer value.
- The optimization shares the outbound winner map until the first already-frozen ChangeId is encountered. At that immutable boundary, it forks the Node-cache winner map: Node retains the maximum-HLC winners across the complete scanned prefix, while outbound publication discards the already-frozen prefix and coalesces only the not-yet-frozen suffix. This preserves the freeze/retry immutability rule.
- The regression `Incremental_provider_cache_coalesces_repeated_field_edits_by_maximum_HLC` verifies provider state, Node cache state, one outbound sequence for two repeated edits, and feed acknowledgement when the later feed row carries an older HLC.
- Latest verified CI on this code head: BIN-SYNC Core Release build and 648/648 tests; Fuzz + Lab 31/31; 70.4% line coverage; FLD Storage Integration 111/111 independently on Ubuntu, Windows and macOS. All jobs succeeded.
- The matched-runner retry benchmark decision in §7.30 remains valid. The HLC coalescing change has regression coverage but no separate before/after benchmark; do not attribute additional measured speedup or allocation reduction to this change.

**Remaining gates:** profile feed materialization, operation encoding and changed-record copy-on-write; deterministic physical Storage crash cuts and power-loss testing; legacy Node-owned receive/transport and other-author relay state migration; receipt/dedup/tombstone GC horizons; and FLD `(author, JournalSeq, ChangeId) → BIN-SYNC OutboxSeq` provenance with atomic staging/freeze recovery. Keep exactly two DLLs: `Fld1.Database.dll → Fld1.Storage.dll`; do not add a third shared runtime DLL.


### 7.32 Deterministic on-disk WAL/snapshot crash-state matrix (2026-11-08)

BIN-SYNC adds `StorageCrashConsistencyTests` in `BinSync.Net/tests/BinSync.Storage.Integration`. These tests reconstruct the physical file layouts that can remain after interruption at key Storage commit/checkpoint boundaries; they exercise the real `Fld1.Storage` engine and files rather than a mocked adapter.

- **Final torn WAL append:** an incomplete final envelope tail is truncated to the last valid record; the database accepts a new commit and survives another reopen.
- **Committed-record corruption:** a complete record with a damaged CRC fails closed and the WAL file is not truncated.
- **Non-final torn segment:** a torn tail in a sealed/non-final segment fails closed rather than discarding later history.
- **Interrupted WAL rotation:** an orphan `.log.tmp` file is ignored during recovery and removed when the next rotation retries publication.
- **Snapshot before manifest publication:** the old manifest and old snapshot remain authoritative; a complete but unreferenced new snapshot plus orphan `.tmp` files are ignored, and the WAL replays the post-snapshot commit.
- **Snapshot after manifest publication but before WAL reclamation:** the new manifest/snapshot is authoritative; retained old WAL records at or below the snapshot CommitSeq are skipped, and recovery remains contiguous.
- **Manifest corruption or missing referenced generation:** both cases fail closed; no silent fallback to an older WAL/snapshot state is attempted.

**Verification:** GitHub Actions run `42446095432` passed 119/119 Storage Integration tests independently on Ubuntu, Windows and macOS. On the same code/test head `2667fb81f41f726d2c286b98f34acd7857c2a7eb`, run `42446095441` passed 648/648 BIN-SYNC Core tests, 31/31 Fuzz/Lab cases and the 70.4% line-coverage gate. PR #16 remains Draft/open/unmerged.

**Scope limitation:** these are deterministic reconstructed on-disk crash states, not forced power loss or injected OS/filesystem failures at each `Write`, `Flush(true)`, `Move` or directory-fsync call. True power-loss/I/O fault injection and the coordinator-level PendingApply → provider effect → Metadata Store finalize → Node checkpoint → ACK process-kill matrix remain open. Do not mark the full crash-consistency audit complete.

### 7.33 WAL writer fence: correct process-kill test and revert blocking file-share experiment (2026-11-08)

The first abrupt-process-kill test killed the `dotnet test` parent rather than the actual testhost that owned the WAL partition. That left the true owner alive and produced a false recovery failure. The test now writes `Environment.ProcessId` from the owner testhost, kills that PID explicitly, reaps the parent process, and retries opening until the OS has released the fence.

- The attempted persistent `FileShare.None` lock-file lease was reverted: the macOS integration jobs stalled when a second opener tried to access an already-locked file. Do not use this blocking file-share primitive for non-blocking single-writer rejection.
- `WalWriterFence` is back to a named OS-managed Mutex with a process-wide owner thread, preserving non-blocking `WaitOne(0)` rejection and thread-independent disposal. An abandoned owner process is handled before snapshot/WAL recovery.
- A separate regression verifies disposal from a different thread; the existing second-process and path-alias tests remain.
- Latest Storage Integration run at code/test head `75133279fc9eca941983fe1ffcce94f63a1e0f70` passed 122/122 on Ubuntu and Windows. The macOS-15 job is queued; keep the cross-platform gate open until it passes.# Sync Storage Compaction Roadmap — FLD-DB + BIN-SYNC

**Date:** 2026-11-08  
**Status:** BIN-SYNC metadata P1 and FLD remote APPLY P2 remain implemented on their working branches without a DELTA_BATCH wire-format change. The neutral `Fld1.Storage` engine has an executable byte-key/value WAL/snapshot/checkpoint slice, and BIN-SYNC has public durable Storage adapters, provider/metadata conformance, PendingApply recovery, real-TCP examples and an opt-in provider-authoritative outbound cutover. The latest verified BIN-SYNC code/test head is `75133279fc9eca941983fe1ffcce94f63a1e0f70` (Core/Fuzz 648/648 and 31/31 at `aafff5c5`; Storage Integration 122/122 on Ubuntu and Windows, with the macOS-15 run queued). The branch's current code CI is green; PR #16 remains Draft/open/unmerged. Remaining cross-repository work: complete FLD Database persistence migration onto the two-DLL Storage Core, implement the FLD JournalSeq provenance adapter and its atomic publication gate, implement FLD-specific durable PendingLocal/freeze recovery, extend reconstructed on-disk crash-state tests to true physical power-loss/I/O fault injection, and add controlled large-partition benchmarks, and prove protocol-safe receipt/dedup/tombstone reclamation. The BIN-SYNC outbound cutover is a provider-feed/Metadata Store milestone; it does not by itself complete FLD Database integration or FLD's durable PendingLocal transaction protocol.  
**Primary audit:** [Sync History Storage Audit](SYNC-HISTORY-STORAGE-AUDIT.md).  
**FLD normative design:** [FLD Kernel Specification v1.5.9](FLD-KERNEL-SPEC-v1.5.9.md) and [FLD Storage Core Architecture](FLD-STORAGE-CORE-ARCHITECTURE.md).  
**BIN-SYNC normative protocol:** [BIN-SYNC CORE v2.4.20 FINAL](https://github.com/smersh1307n2/BIN-SYNC/blob/feature/fld-external-storage/BIN-SYNC-SPECIFICATION.txt), including E-24 (global ChangeId identity/equivocation) and E-25 (fail-closed tombstone retention).  
**Cross-layer contracts:** [BIN-SYNC Metadata Storage Architecture](https://github.com/smersh1307n2/BIN-SYNC/blob/feature/fld-external-storage/docs/METADATA-STORAGE-ARCHITECTURE.md), [Metadata Store Semantic Contract and Conformance Plan](https://github.com/smersh1307n2/BIN-SYNC/blob/feature/fld-external-storage/docs/METADATA-STORE-CONFORMANCE.md), [Data Provider Semantic Contract and Conformance Plan](https://github.com/smersh1307n2/BIN-SYNC/blob/feature/fld-external-storage/docs/DATA-PROVIDER-CONFORMANCE.md), [Change Staging and Retention](https://github.com/smersh1307n2/BIN-SYNC/blob/feature/fld-external-storage/docs/CHANGE-STAGING-AND-RETENTION.md), and [FLD-DB Integration Contract](https://github.com/smersh1307n2/BIN-SYNC/blob/feature/fld-external-storage/docs/FLD-DB-INTEGRATION-CONTRACT.md).  
**Retention research:** [Tombstone Retention and Safe Garbage Collection](TOMBSTONE-RETENTION-RESEARCH.md) — Microsoft ForgottenKnowledge, CRDT causal stability/commitment, Cassandra, CouchDB and Couchbase. These sources support the E-25 design; the normative protocol rule remains BIN-SYNC E-25.

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


### Phase 3 — Durable local PendingLocal coalescing (design gate; implementation deferred)

- Add durable staging keyed by canonical scope/namespace/record/field after the neutral Data Provider/Metadata Store boundaries and Storage transaction API are defined.
- Coalesce only local LWW operations never exposed to a sender.
- Use canonical LWW comparison, not arrival/lock order.
- Keep pending payload independent of current visible winner because remote APPLY may win before freeze.
- Freeze atomically into immutable Outbox before send.
- Preserve event/CRDT/non-coalescible operations and explicit audit-history requests.

**Exit:** multiple commits to one field before publication produce one pending operation; crash before/during freeze yields complete pending or complete frozen state, never loss/duplication; remote winner does not erase eligible pending local operation; published operations are never rewritten.

### Phase 4 — Versioned compact physical encodings and two-DLL Storage extraction

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
| P0 | Revalidate all normative/design/audit documents and establish reproducible disk/RAM/CPU baseline; documentation reconciliation is complete, quantitative workload baseline remains open |
| P1 | BIN-SYNC global ChangeId dedup, ACK replay pruning, unified InboxReceipt, compact peer progress, grouped persistence v10–v12, compact local WAL markers and admission caps — implemented; receipt/dedup GC remains open |
| P2 | FLD APPLY v3 single-encoding, relay-only journal, sparse paging, offset-only batch index, explicit scan cursor, per-author ACK sidecar and post-WAL-seal relay payload compaction — implemented; receipt GC/index bounds and cross-system provenance adapter remain open |
| P3 | Provider-independent Metadata Store contract/conformance — 20 semantic tests plus durability-profile assertion pass in the volatile reference model; no durable store/restart guarantee yet |
| P4 | Data Provider semantic contract and six volatile conformance tests are now recorded; remaining: durable provider/reopen tests, end-to-end feed-to-metadata staging boundary and dedicated PendingLocal crash/relay conformance |
| P5 | Refactor BIN-SYNC persistence behind Metadata Store; extract exactly two FLD runtime DLLs and implement generic Storage-only semantics/Metadata Store adapter |
| P6 | Complete synchronized-FLD Database provenance mapping, publication gate and ACK translation; then implement durable PendingLocal and versioned compact FLD Outbox encoding with measurements |
| P7 | Safe receipt/dedup/tombstone GC and snapshot/new-peer contract; optional WAL-backed Outbox only after retention proof |

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

#### 7.15.1 PendingLocal semantic details (implementation contract)

The implementation must preserve the following distinctions; none may be inferred from the current visible field winner:

- **Canonical key:** `(namespaceId, RecordId, FieldId)`. There is at most one pending local LWW operation per key. Field tombstones are ordinary LWW operations for this purpose and may replace an earlier unpublished value for the same key.
- **Stage ordering:** every durable `CommitPending()` assigns a monotonically increasing namespace-local `StageSeq`. The staged value retains its original HLC, local author, key epoch, tombstone bit and owned payload. Replacement uses the same canonical `FieldResolver.IsNewer` ordering as current-state resolution; arrival order alone is not a substitute for the comparator. The winning staged value records the StageSeq that supplied it.
- **Independent payload plane:** PendingLocal is not reconstructed from the visible overlay or Runs. A remote APPLY may become the visible winner while an older local operation remains pending; that local payload is still a required outbound operation and must survive checkpoint/reopen until freeze.
- **Freeze boundary:** `FreezePending()` captures a fixed `stageThrough` watermark and freezes every current staged winner at or below it under the database commit lock. It assigns final ChangeId and OutboxSeq only at freeze. The durable freeze record contains the exact immutable operation payloads and the stage watermark. Recovery must replay the freeze idempotently into Outbox, then remove only pending entries covered by that watermark. No operation at or below the watermark may be re-created as a new ChangeId on retry.
- **Mixed immediate commit:** existing `Commit()` remains immediate. If pending entries exist, it freezes the pending winners together with current dirty local writes in one publication transaction. Dirty writes are compared against staged values by the canonical LWW comparator for the same key; the final local winner is published once. This must not compare a pending local operation against a later remote visible winner and silently discard it.
- **Transport visibility:** every operation that exposes outbound data or a high watermark (`ReadOutboxBatch`, `VisitOutboxBatch`, `AcquireOutboxLease`, `GetOutboxHighWatermark`) first completes the freeze transition. ACK and compaction APIs do not implicitly freeze. This prevents a sender from observing a watermark that excludes committed staged work.
- **Non-coalescible operations:** remote relay operations and future CRDT/event operations bypass PendingLocal's LWW map. They retain their original identity and independent relay retention lifecycle.

The crash matrix in §7.15 is mandatory for the implementation. Add explicit assertions for: exact freeze retry preserving ChangeId/OutboxSeq; partial or conflicting freeze retry failing closed; a remote winner not deleting a pending local payload; a staged tombstone replacing an earlier staged value; and a checkpoint with pending entries but no visible overlay. The snapshot's generation, checksum and stage watermark are authoritative together; a missing or mismatched referenced snapshot must fail closed rather than silently start with an empty pending map.
The baseline half is now executable: `PendingLocalBaselineTests` proves across close/reopen that four separate immediate commits produce four distinct immutable Outbox operations with stable sequence order, while four Put calls inside one commit already coalesce to one final value. `SyncApplyTests` also verifies the adjacent ownership invariant across close/reopen: a local dirty write remains a local-author Outbox operation even when a later remote APPLY wins the visible field state, and the remote relay remains independently retained. These are compatibility/crash-recovery baselines, not PendingLocal cross-commit coalescing. `SyncApplyTests` also verifies the adjacent ownership invariant across close/reopen: a local dirty write remains a local-author Outbox operation even when a later remote APPLY wins the visible field state, and the remote relay remains independently retained. This is a compatibility/crash-recovery baseline, not PendingLocal cross-commit coalescing. GitHub Actions run [42350334573](https://github.com/smersh1307n2/FLD-DB/actions/runs/42350334573) passed the new baseline tests and the full .NET 10 suite at commit `77236494106bd8b3b48ff4fa17dd17c6993cc5d8`.

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
- FLD-DB fix/crash-consistency-outbox: latest verified code baseline `81cb93fae0d6` (the later `eacff36` and subsequent commits in this checkpoint sequence are documentation-only).
- BIN-SYNC feature/fld-external-storage: latest verified code baseline `b3d652236290` (the later `d2e0313` and subsequent commits in this checkpoint sequence are documentation-only).
- No adapter implementation or source-code changes are present after those code baselines. Existing public APIs and FLD journal/ACK implementation are the starting point, not a completed adapter.

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
3. **Initial semantic slice implemented and extended:** BIN-SYNC now has a provider-independent test adapter contract, inherited conformance suite and volatile reference adapter; twenty semantic tests plus a separate volatile-profile assertion pass. Actions run `42359081094` passed all 630 tests, public samples, 72.2% line coverage and fuzz/lab hardening at commit `992305bc53f7cc06e85f75a6d46fbe860f454c93`. The suite now covers frozen Outbox/lease fencing, safe outbound ACK, snapshot read-view stability under logical append/ACK/compaction and pending-apply provider-token/finalization semantics. The reference adapter explicitly provides no restart durability: physical snapshot-generation pinning and crash/reopen persistence remain unproven. The helper assembly is referenced by the BIN-SYNC test project, not run as a standalone solution test target.
4. **Data Provider contract and initial tests added:** BIN-SYNC `docs/DATA-PROVIDER-CONFORMANCE.md` now defines stable source mapping, durable local feed, snapshot/feed watermark barrier, idempotent remote apply, provider-side apply receipts and local/remote-origin separation. A test-only `IDataProviderConformanceAdapter` and volatile reference provider exercise six semantic scenarios. Combined BIN-SYNC Actions run `42360397431` passed 637 tests, public samples, fuzz/lab hardening and 72.5% line coverage at commit `504a09092302f137d222421f6929533b95188318`. The reference provider is not durable; end-to-end feed ACK after durable staging and real provider reopen/fault-injection remain open. PendingLocal coalescing remains a separate provider/FLD staging contract and is not covered by either current volatile suite.
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

**Exit condition for this phase:** the volatile harnesses now cover initial Metadata Store semantics (20 tests) and Data Provider semantics (six tests), including outbound freeze/lease/ACK, logical snapshot view, pending-apply retry, source-feed paging, snapshot/feed watermark, idempotent remote apply and local/remote-origin separation. They do not prove durable restart, physical snapshot-generation pinning or real provider idempotency. The neutral Data Provider contract is documented; the next open contract/test gate is PendingLocal/source-page staging, LWW coalescing and freeze-retry crash/relay behavior. Then refactor BIN-SYNC persistence behind the boundary and require durable adapters to pass reopen/fault-injection tests. Existing Node/WAL behavior remains a regression baseline.


### 7.21 Cross-repository documentation map and authority (2026-11-07)

Storage optimization is specified across multiple documents. This map prevents implementation work from omitting a contract or treating a research note, historical addendum or stale audit statement as normative.

#### 7.21.1 Source-of-truth map

| Document | Authority / responsibility | Use in this roadmap |
|---|---|---|
| BIN-SYNC `BIN-SYNC-SPECIFICATION.txt` — **v2.4.19 FINAL** | Normative wire protocol and protocol-level identity, ACK, snapshot, ChangeId and tombstone-retention rules; E-24 and E-25 are active | Check before changing protocol-visible identity, cursor, dedup, snapshot or GC behavior. No storage optimization may weaken E-24/E-25. |
| FLD-DB `FLD-KERNEL-SPEC-v1.5.9.md` | Normative FLD1 physical format and FLD database/engine behavior, including WAL, APPLY, snapshots, digest boundaries and BIN-SYNC integration obligations | Defines what FLD persists and which semantics belong to Database rather than generic Storage. Must align with the current BIN-SYNC protocol. |
| FLD-DB `FLD-STORAGE-CORE-ARCHITECTURE.md` | Binding assembly/dependency decision and extraction map | Exactly two runtime DLLs: `Fld1.Storage.dll` and `Fld1.Database.dll`; Database → Storage only; no third Common DLL; Storage-only consumers must not load Database or BIN-SYNC. |
| BIN-SYNC `docs/METADATA-STORAGE-ARCHITECTURE.md` | Cross-role architecture: Data Provider, Metadata Store, Physical Storage and Runtime; scope/identity inventory; transaction modes and implementation sequence | Governs the Metadata Store boundary and shared-transaction versus separate-store recovery. It is not a physical Storage format specification. |
| BIN-SYNC `docs/CHANGE-STAGING-AND-RETENTION.md` | Logical change lifecycle, safe LWW coalescing, immutable freeze boundary, payload ownership, metadata cardinality and independent retention | Governs pending versus frozen changes, one-payload/many-peers model, and which metadata can be compacted independently. |
| BIN-SYNC `docs/FLD-DB-INTEGRATION-CONTRACT.md` | Specialized contract when FLD Database is the synchronized Data Provider/transaction owner | Governs Outbox/ApplyJournal cursor domains, durable relay provenance mapping, publication gate, ACK translation and atomic APPLY. Does not define Storage-only mode. |
| BIN-SYNC `docs/METADATA-STORE-CONFORMANCE.md` | Provider-independent semantic contract and executable conformance requirements | Governs required behavior and crash/reopen tests before a durable Metadata Store or provider adapter can be called conformant. Existing Node tests remain regression tests, not a substitute. |
| BIN-SYNC `docs/DATA-PROVIDER-CONFORMANCE.md` | Provider-side source mapping, durable feed, snapshot/feed barrier, remote apply idempotency and origin separation | Governs Data Provider behavior independently from Metadata Store semantics; its volatile reference tests do not prove durable provider recovery. |
| FLD-DB `docs/SYNC-HISTORY-STORAGE-AUDIT.md` | Code-backed findings, logical representation accounting, source hot spots and optimization candidates | Evidence and implementation audit. Estimates are not benchmarks; recommendations become binding only when accepted into this roadmap or a normative contract. |
| FLD-DB `docs/TOMBSTONE-RETENTION-RESEARCH.md` | External research and rationale for safe deletion/forgotten-history design | Research input to E-25 and Phase 5; it does not independently authorize tombstone, receipt or dedup GC. |
| BIN-SYNC `docs/PUBLIC-API.md` | Shipped BinSync.Core 3.0.0 public API and persistence behavior | Distinguishes the current NodePersistence/WAL API from the planned Metadata Store/Data Provider boundaries; the new contracts are test-only and are not shipped public APIs. |
| BIN-SYNC `docs/PRODUCTION-STATUS.md` and `docs/CHANGELOG.md` | Implementation/release status and historical change trace | Useful for checking current implementation state and migration history; neither overrides the normative protocol or semantic contracts. |

Historical BIN-SYNC `BIN-SYNC-v2.3.7-ADDENDUM.txt` and `docs/ERRATA-v2.3.7.md` are retained for provenance only. Their requirements were consolidated into the current specification; do not apply them as a second independent normative layer over v2.4.19.

#### 7.21.2 Authority and conflict resolution

Resolve conflicts by subject:

1. **Wire/protocol semantics:** current BIN-SYNC specification v2.4.19 FINAL (including E-24/E-25).
2. **FLD file format and FLD-owned semantics:** FLD Kernel Specification, constrained by the two-DLL Storage Core architecture.
3. **Cross-component API/transaction behavior:** Metadata Store Conformance for generic provider semantics; FLD-DB Integration Contract for the synchronized-FLD-specific path.
4. **Logical storage lifecycle and minimization:** Change Staging and Retention.
5. **Execution order and acceptance gates:** this roadmap.
6. **Evidence and rationale:** Sync History Storage Audit and Tombstone Retention Research.

A code audit can reveal that implementation has changed, but cannot silently amend a normative contract. A design document also does not prove that code or tests implement it. Update the relevant contract and matching tests whenever an implementation changes an observable guarantee.

#### 7.21.3 Storage optimization topics that must remain connected

- **Metadata cardinality:** one shared operation payload for many peers; per-peer/per-author progress; compact issuer-specific batch receipts; route-independent global ChangeId ledger; bounded outgoing replay mappings; separate local WAL replay markers.
- **Pending/frozen lifecycle:** coalesce only unpublished local LWW changes; preserve pending payload independently from the current winner; freeze atomically; never rewrite a published Outbox entry.
- **Payload amplification:** APPLY_COMMIT v3 stores incoming operations once; relay selection uses indexes; ApplyJournal retains receipt metadata plus relay-only payload; relay payload ACK/compaction is independent from receipt retention.
- **Physical encoding:** compact peer progress, receipt grouping, author/sequence/CommitSeq encoding, bounded indexes, sparse journal anchors, versioned formats and legacy readers. Each candidate requires byte accounting and workload benchmarks before adoption.
- **Retention:** separate lifetimes for Outbox payload, relay payload, Inbox receipts, global ChangeId facts, replay cache, WAL replay markers, peer membership and forgotten frontier. No one ACK, snapshot completion, TTL or HLC timestamp is a universal GC proof.
- **Provider/Storage boundaries:** Data Provider captures and applies user changes; Metadata Store owns BIN-SYNC durable state; Physical Storage owns generic atomic byte durability. Do not conflate Storage-only metadata mode with synchronized FldDatabase mode.
- **Crash consistency:** every freeze, incoming acceptance, pending-apply transition, snapshot lifetime, provenance page, ACK and compaction boundary needs deterministic fault injection and restart assertions.

#### 7.21.4 Reconciliation findings and corrections

1. **Corrected in this documentation pass:** FLD Kernel Specification now names BIN-SYNC v2.4.19 FINAL and explicitly includes E-24/E-25. FLD physical format version remains FLD1 binaryVersion 1; this compatibility update does not itself change the FLD1 Run/WAL format.
2. **Corrected in this documentation pass:** FLD Kernel Specification now states the binding two-DLL boundary: `Fld1.Storage.dll` and `Fld1.Database.dll`, with Database → Storage dependency and no third Common/Shared runtime DLL.
3. **Corrected in this documentation pass:** Sync History Storage Audit now distinguishes the implemented FLD per-author JournalSeq ACK sidecar and post-WAL-seal payload compaction from the not-yet-implemented cross-system JournalSeq → ChangeId → OutboxSeq provenance adapter, minimum peer-ACK translation and publication gate.
4. The provenance mapping, atomic scan-cursor persistence and outbound publication gate remain a separate integration gate as specified in §7.19 and FLD-DB Integration Contract §3.1.1–3.1.2. The audit is evidence, not a substitute for the contract or crash tests.


### 7.22 PendingLocal checkpoint and recovery boundary (2026-11-07)

The source audit of `FldDatabase`, `WalManager`, `OutboxLog` and the manifest confirms that a PendingLocal implementation cannot be represented as an ordinary additional WAL record and then rely on the existing Run checkpoint. A Run contains the visible field winners; it does not preserve a staged local payload that has subsequently lost the visible LWW comparison to a remote APPLY. Existing checkpoint logic may seal and truncate the WAL prefix once its Run/ApplyJournal effects are materialized. Therefore, an unpromoted staged operation must have an independent durable recovery representation before it can safely survive checkpoint.

#### 7.22.1 Required separation

- Visible field state and PendingLocal are separate logical state planes. A remote APPLY may replace the visible winner but must not erase or rewrite the staged local operation.
- PendingLocal is keyed by the canonical field key `(NamespaceId, RecordId, FieldId)`; its stored winner is selected by the existing `FieldResolver.IsNewer` ordering, not arrival order.
- The PendingLocal checkpoint watermark is a StageSeq (or equivalent staging generation), not WAL CommitSeq, Run SeqHi, OutboxSeq or a peer-relative cursor. These sequence domains must not be conflated.
- A visible Run checkpoint must not claim to cover PendingLocal merely because both refer to the same field key.

#### 7.22.2 Snapshot publication and reclamation protocol

Any compacted PendingLocal snapshot must be immutable and self-validating. Its durable identity must include at least namespace, monotonically increasing generation, covered StageSeq watermark, record count/length and checksum (or a cryptographically strong content digest if already available in the storage layer).

The required publication order is:

1. Write the complete new snapshot to a temporary generation file and flush its contents to stable storage.
2. Atomically publish a manifest reference containing the generation, covered StageSeq watermark and checksum; flush the manifest and its containing directory where supported.
3. Only after the manifest reference is durable may covered staging-log segments/records be reclaimed.
4. On open, load exactly the generation referenced by the manifest and validate its identity, watermark, length/count and checksum before accepting it as the recovery base.
5. If a referenced generation is missing, truncated or mismatched, fail closed. Do not silently fall back to an older generation or an empty pending set if later staging records may already have been reclaimed.

A marker stored only in a WAL segment is insufficient as the sole snapshot reference: that segment may itself be removed by a later WAL rotation/truncation. The manifest (or an equivalent independently durable root) must own the current snapshot reference.

#### 7.22.3 Freeze/replay interaction

- Freeze must capture a fixed StageSeq watermark while holding the database serialization boundary. Concurrent later staging belongs to a later freeze.
- The immutable freeze intent must carry enough information to reproduce the exact operation identities and payloads after a crash. Replaying the same intent must not allocate new ChangeIds or OutboxSeq values.
- Promotion to Outbox must be idempotent across crashes before Outbox append, after append but before its durability flush, and after Outbox durability but before pending-state cleanup.
- Pending entries may be removed only when the durable freeze record proves that their StageSeq is covered by the completed immutable promotion. A later staged replacement must remain pending.
- Outbox publication APIs must not expose a high watermark/page/lease that omits eligible pending work merely because no explicit caller invoked freeze. The API gate and its lock ordering must be specified and tested before enabling the optimized path.

The exact physical representation (shared Storage transaction, independent staging journal plus recoverable promotion record, or another design) remains an implementation choice. It must satisfy the above ordering and pass fault-injection/reopen tests. This section does not authorize a new FLD1 Run or WAL binary-format version by itself.

#### 7.22.4 Minimum crash matrix before implementation is considered complete

- crash during snapshot write; old manifest root and old log remain authoritative;
- crash after snapshot flush but before manifest publication; orphan snapshot is ignored/reclaimed only after validation;
- crash after manifest publication but before old-log reclamation; recovery loads the new snapshot and safely tolerates the retained covered log;
- missing/corrupt referenced snapshot; open fails closed;
- crash before freeze intent durability; pending state remains unchanged;
- crash after freeze intent durability but before Outbox append; recovery promotes the exact intent;
- crash after Outbox append but before flush; recovery deduplicates and completes the same promotion;
- crash after Outbox flush but before pending cleanup; recovery does not duplicate or re-sequence the operation;
- later staged replacement survives replay of an earlier freeze watermark;
- remote APPLY winning the visible field does not change the pending snapshot or its payload;
- checkpoint/rotation with pending entries and no corresponding visible overlay preserves those entries;
- repeated open/checkpoint/reopen cycles preserve the same pending winners and frozen ChangeIds/OutboxSeq values.

**Implementation gate:** do not add `CommitPending` or alter the public commit semantics until the snapshot root, staging-log retention and freeze-to-Outbox replay protocol are implemented together. The current immediate `Commit()` path remains the compatibility baseline.


### 7.23 Generic Storage core: first executable implementation slice (2026-11-07)

The separate `Fld1.Storage.dll` project now contains a domain-neutral WAL and an initial byte-key/value engine. It has no project reference to `Fld1.Core`, `Fld1.Engine`, `Fld1.Wal`, BIN-SYNC or FLD Database.

Implemented surface:
- opaque non-zero WAL record kinds and arbitrary payloads; generic partition identity rather than FLD namespace identity;
- atomic byte-key/value batches with deterministic key ordering and last-write coalescing inside one batch;
- durable append+flush before publishing in-memory state;
- optimistic batch conflict detection and read-your-writes;
- immutable in-memory read snapshots and ordered prefix scans;
- immutable on-disk snapshot generations rooted by an independently atomic manifest;
- checkpoint order: flush/rotate WAL → write and flush snapshot → publish and flush manifest → truncate only fully covered sealed WAL segments;
- missing or corrupt referenced snapshot/manifest fails closed;
- recovery skips only WAL records already covered by the validated snapshot and requires the remaining CommitSeq chain to be contiguous.

The Storage test project currently covers 12 cases: opaque WAL replay and rotation, durable batch recovery, repeated-key coalescing, delete recovery, partition isolation, stable snapshot/prefix paging, checkpoint/reopen, repeated checkpoint generations, and missing/corrupt snapshot failure.

CI on FLD-DB commit `81e95fbd0e763ee5e6660ab09fa16cc8fe71ccce` passed: Storage 12/12 and the existing FLD test projects remained green. BIN-SYNC's pinned integration snapshot was compiled in its own workflow; its durable Data Provider conformance adapter passed 8/8 and the Storage-backed array synchronization sample returned PASS.

**Not yet complete:** the new Storage engine is not yet the persistence backend of the existing FLD Database engine; current FLD Engine/WAL/Run/Manifest projects remain in place. The next FLD-side work is to harden Storage checkpoint/fault-injection coverage, define the production transaction/snapshot API boundary, and then migrate Database-owned persistence without introducing a third runtime DLL or breaking FLD1 format compatibility. The cross-repository Metadata Store adapter remains an open integration gate as recorded in BIN-SYNC `METADATA-STORAGE-ARCHITECTURE.md`.


#### 7.23.1 Cross-repository metadata integration update

The BIN-SYNC integration branch now also has a durable Metadata Store conformance adapter backed by `Fld1.Storage`. Its physical layout stores each receipt, cursor, ChangeId fingerprint, relay/outbound item, ACK/high-watermark, lease, pending-apply intent and completed receipt under a separate logical key. Each mutation computes a key-level diff and commits the changed entries atomically; it does not rewrite the entire partition payload.

The BIN-SYNC integration workflow reports 30/30 passing tests across Data Provider and Metadata Store conformance plus restart/compaction coverage. The Storage-backed array example also completes a two-replica exchange, closes/reopens both Storage and Node state, applies a later update and reports PASS.

The adapter is still test-only. Public production APIs and direct Node runtime integration remain outstanding; do not mark the overall BIN-SYNC + Storage integration complete until those APIs, fault-injection/reopen tests and production array example are in place.

#### 7.23.2 Snapshot-generation reclamation

After the new snapshot file is flushed and the atomic manifest has been published and directory-flushed, `StorageSnapshotFile.Store` now reclaims obsolete snapshot generations for that partition. This is safe for the current API because `StorageSnapshot` owns an in-memory copy rather than a disk-generation pin. A crash before manifest publication leaves the old root authoritative; a crash after publication leaves the new root authoritative and may leave only reclaimable orphan files. The repeated-checkpoint test asserts that one current snapshot generation remains.


#### 7.23.3 BIN-SYNC public runtime adapter milestone

The BIN-SYNC integration branch has promoted the provider and metadata-store contracts into `BinSync.Core` as public `IDataProvider` and `IMetadataStore` interfaces. Runtime implementations are in `BinSync.Storage.dll`, which references Core and the neutral `Fld1.Storage.dll`. The public `StorageSyncCoordinator` enforces local feed → Node write with original HLC/author → caller's durable Node checkpoint → provider ACK; remote field/tombstone materialization uses deterministic apply tokens and does not enter the local feed.

The public array sample now exercises the runtime adapters and coordinator across two nodes, two independent Storage partitions, close/reopen and a later remote update. BIN-SYNC integration CI passed 32/32 tests at the current adapter milestone.

**Boundary still open:** the legacy Node/Transport path has not yet been migrated to use `IMetadataStore` for its own freeze, receive/relay, ACK/lease and Pending Apply transitions. The explicit coordinator does not replace that protocol-owned metadata path. Complete the migration only with wire-compatibility, restart and fault-injection coverage; do not claim automatic NodeBuilder/Transport integration yet.

### 7.23.4 Storage-backed real TCP array sample (2026-11-07)

The BIN-SYNC integration branch now includes `samples/StorageArrayTcpSync`, executed by the FLD Storage Integration workflow. Unlike the earlier `StorageArraySync` sample, this one uses `TcpSyncListener`, `TcpSync.ConnectAsync` and the production transport-independent `SyncSession` path over a real loopback TCP connection.

The sample keeps one independent `Fld1.Storage` database per replica and stores the `StorageDataProvider`, `StorageMetadataStore` and complete Node checkpoint in that database. It verifies:

- bidirectional synchronization of two array items with multiple stable fields;
- receiver ordering through the TCP `beforeAck` boundary: Node checkpoint → provider apply → Metadata Store PendingApply finalization → ACK;
- draining all available batches before closing a session (one `PushBatchAsync` call is one batch attempt, not a complete synchronization round);
- close/reopen of both databases and Node checkpoints, then a later field update on the same author stream;
- record lifecycle tombstone propagation over a subsequent TCP session.

At BIN-SYNC commit `aa5c52e2daa85638326f4df94c0fdec5e312b467`, FLD Storage Integration workflow run [42377636914](https://github.com/smersh1307n2/BIN-SYNC/actions/runs/42377636914) passed: 47/47 integration tests, the existing in-process StorageArraySync sample, and the new real-TCP sample. The TCP sample reported PASS for initial bidirectional exchange, restart + subsequent update, and record tombstone propagation.

**Boundary unchanged:** this proves the public Storage adapters and coordinator can participate in real TCP synchronization with durable reopen. It does not make `IMetadataStore` the authoritative source for Node's own Outbox, receive/relay, ACK/lease or peer-progress state. NodePersistence remains a complete Node checkpoint stored in Storage, and the direct Node runtime migration remains the next architectural gate.


### 7.24 Provider-authoritative outbound publication in BIN-SYNC (2026-11-08)

The BIN-SYNC Storage adapter now has an explicit, durable outbound authority cutover. This advances the cross-repository metadata boundary but does not mark the FLD Database adapter or Phase 3 complete.

Verified implementation on `smersh1307n2/BIN-SYNC` `feature/fld-external-storage`, code/test head `016ddb553a6d2c84abe2027145dfdbd92f229d35`:

- `StorageDataProvider` persists a stable 128-bit ChangeId in each local-feed row and a durable one-way publication cutover watermark.
- Cutover requires a fully acknowledged/empty provider feed and exact alignment between Node and Metadata Store outbound high-watermarks. The Node Outbox sequence becomes a fence after cutover.
- New provider changes are frozen directly into Metadata Store; Metadata Store allocates the author outbound sequence. The provider feed is ACKed only after the metadata freeze commits. Node is rebuilt from a stable provider snapshot as a data cache.
- The coordinator consults the global ChangeId fingerprint ledger on retry. It preserves already-frozen operations and coalesces only the not-yet-frozen suffix, including the case where metadata freeze succeeded, provider ACK failed, and a newer edit arrived before retry. The optional lookup returns a read-only memory view of the immutable fingerprint, avoiding an additional 16-byte copy per already-frozen feed row; this structural allocation reduction awaits BenchmarkDotNet measurement.
- The TCP sample uses the direct Metadata Store transport overload and persists an explicit empty initial peer baseline before sending incremental deltas.
- CI: 644/644 BIN-SYNC core, 103/103 Storage Integration, 31/31 Fuzz/Lab, 70.8% line coverage; both in-process and real-TCP Storage samples pass. The durable `AutoReconcileWithMetadataAsync` entry point now detects the provider cutover marker and rehydrates a stale Node checkpoint from the provider snapshot before anti-entropy. Cutover removes duplicate local-author Outbox payload rows from Node while retaining the sequence fence and WAL/ChangeId replay identities. Tests cover stale checkpoint replacement, local Outbox cleanup, provider ACK interruption, post-ACK checkpoint failure and missing-marker fail-closed behavior.

**Interpretation for the FLD roadmap:** the provider feed can serve as the durable PendingLocal journal for a generic external provider, but this is not yet the FLD Database's atomic pending-staging/freeze protocol. Phase 3 remains open until FLD commits, concurrent remote APPLY, snapshot/checkpoint and Outbox promotion satisfy the StageSeq/freeze-intent crash matrix in §7.22. Do not add a second per-edit metadata table or weaken the rule that frozen operations are immutable.

The next FLD-specific integration gate remains: persist the mapping `(author, JournalSeq, ChangeId) → BIN-SYNC OutboxSeq` with the relay scan cursor and publication gate, then test restart, partial page, ACK interruption, remote winner and compaction/reopen. Keep the exact two-DLL dependency boundary: `Fld1.Database.dll → Fld1.Storage.dll`; no third Common/Shared runtime DLL.


### 7.25 Storage transaction serialization and WAL single-writer fence (2026-11-08)

The BIN-SYNC Storage adapter now serializes all in-process transactions attached to the same `StorageDatabase` object through a shared gate used by `StorageDataProvider`, `StorageMetadataStore` and `StorageNodePersistence.Save`. A concurrency regression runs 64 provider appends alongside 64 metadata freezes and verifies all 128 commits complete without stale-base conflicts.

The generic Storage engine uses a named OS-managed Mutex for each normalized WAL directory/partition before reading the snapshot or replaying WAL. A process-wide manager thread owns/releases named mutexes so disposal can occur on any caller thread. The first FileShare.None lock-file experiment was abandoned after macOS integration jobs stalled while trying to open an already-locked file. The actual abrupt-process-kill regression now records and kills the owning testhost PID (rather than only its `dotnet test` parent); with the named Mutex restored, Storage Integration passes 122/122 on Ubuntu and Windows. The macOS-15 run is queued. The implementation remains BCL-only, with no native interop.

This is a **single-writer partition contract**, not multi-writer concurrency. Remaining validation: independent-process contention/termination tests on each supported OS, symlink/path alias canonicalization, and power-loss cuts around WAL rotation and checkpoint replacement. The shared adapter gate is process-local; separate StorageDatabase objects for the same partition are excluded by the named mutex.


### 7.26 Cross-platform WAL fencing, crash-owner recovery and ChangeId lookup benchmark (2026-11-08)

The follow-up validation closes the cross-process writer-fence and path-alias checks that were open in §7.25.

- `WalWriterFence` now resolves symbolic-link targets through their ancestor path components before deriving the partition lock identity. This handles macOS paths whose symlink target itself contains an aliased ancestor (for example, `/var` → `/private/var`).
- Path identity preserves case-sensitive filesystem distinctions while resolving actual directory-entry casing on case-insensitive filesystems; Windows volume roots are normalized separately.
- The Storage Integration workflow now runs on `ubuntu-latest`, `windows-latest` and `macos-latest`.
- Each OS passed 107/107 Storage Integration tests. The cross-process regression starts a second test host, proves a competing open is rejected while the child owns the fence, terminates the actual owner host, and verifies WAL recovery plus the committed value after reopen. Symlink-alias rejection, same-process rejection and case-alias rejection where supported also pass.
- Both Storage array samples pass on all three OSes, including real TCP bidirectional exchange, restart + later update and record tombstone propagation.
- Verified BIN-SYNC head: `8e6833e5aa5fe7086dbd72b18cf71e45f30169c9`; core 647/647, Fuzz/Lab 31/31, line coverage 70.8%. PR #16 remains Draft/open/unmerged.
- The direct frozen ChangeId fingerprint lookup benchmark at 256/1,024/4,096 ledger rows measured 57.02/57.85/59.57 ns per lookup and 0 B allocated per lookup in GitHub Actions (.NET 10.0.14, Ubuntu 26.04.1, x64). This measures the direct lookup only, not the full coordinator retry scan.

**Still open:** deterministic physical Storage crash/power-loss cuts around WAL append, rotation and checkpoint replacement; end-to-end coordinator retry-scan benchmarks; runtime-wide removal of legacy Node/Transport metadata authority and other-author relay Outbox state; receipt/ChangeId/tombstone GC horizons; cross-implementation conformance; and the FLD Database `JournalSeq` provenance adapter plus atomic publication/freeze integration. Preserve the two-DLL boundary `Fld1.Database.dll → Fld1.Storage.dll`; do not add a third shared runtime DLL.


### 7.27 Coordinator retry allocation baseline and Node snapshot rehydration cost (2026-11-08)

The provider retry benchmark is now an end-to-end coordinator scenario rather than a lookup-only microbenchmark.

- Setup uses the real `StorageDataProvider` and `StorageMetadataStore`; the first publication freezes immutable ChangeIds and then injects a provider ACK interruption.
- The measured retry scans the same still-pending frozen feed, validates the canonical fingerprints, ACKs the feed and refreshes the Node cache from the provider snapshot.
- Results from GitHub Actions run `42439312729` (.NET 10.0.14, Ubuntu 26.04.1, Intel Xeon Platinum 8573C): 256 rows = 2.112 ms / 1.52 MB; 1,024 rows = 12.768 ms / 6.47 MB; 4,096 rows = 35.213 ms / 33.92 MB.
- These are full-path figures, not scan-only figures. They include `InstallProviderSnapshot` → `RestoreProviderSnapshot`, which rebuilds the namespace dictionary, validates uniqueness with temporary sets, materializes operation objects and copies field values. The 1,024-row case was very noisy (about 78.8% relative standard deviation); 4,096 rows was more stable (about 2.5%).
- The separate frozen ChangeId lookup remained allocation-free at all sizes: 81.70/81.78/80.00 ns at 256/1,024/4,096 ledger rows.

**Implementation completed in BIN-SYNC:** ordinary provider-authoritative publication now applies the exact scanned provider-feed prefix incrementally to the Node data cache after Metadata Store freeze, checkpoints Node before provider ACK, and retains full snapshot installation for initial cutover/startup recovery. Copy-on-write staging ensures malformed or failed cache application does not partially publish changed records. ACK retry replays the same HLC mutations idempotently. The first end-to-end incremental retry benchmark is recorded in §7.28; the matched-runner full-snapshot comparison is recorded in §7.30.

**Authority caveat resolved in §7.29:** PendingApply now returns per-operation accepted/duplicate decisions from the Metadata Store ledger. Node applies only new operations in Metadata-Store-authoritative receive mode and does not recreate the global identity ledger or duplicate RemoteBatch WAL record.

### 7.28 Incremental provider-feed application and retry benchmark (2026-11-08)

The per-publication full Node snapshot rebuild identified in §7.27 has been replaced in BIN-SYNC's Metadata-Store-owned outbound path.

- After scanning the provider feed and freezing new immutable operations into Metadata Store, StorageSyncCoordinator applies the exact scanned feed prefix to the Node application-data cache using Node.ApplyProviderChanges.
- The Node method clones only changed records, applies feed operations in source-sequence order using existing HLC/tombstone merge rules, and publishes the changed records only after the full prefix has validated/applied. It creates no Node Outbox entries or duplicate ChangeId identity-ledger rows.
- The coordinator persists the Node checkpoint before acknowledging the provider feed. If checkpoint persistence fails, the provider feed remains pending. If ACK fails, retry recognizes already-frozen ChangeIds, reapplies the same HLC mutations idempotently, checkpoints again and ACKs without reinstalling a full provider snapshot. Provider mutation identity, field/tombstone shape and HLC are preflight-validated before FreezeOutbound; a malformed-provider regression proves invalid input cannot advance the durable outbound watermark.
- Full provider snapshot replacement remains for explicit cutover/startup recovery and stale-checkpoint reconciliation; it is not removed.
- Regression coverage verifies that the Node cache is updated before ACK, ACK failure with a newer edit preserves the already-frozen operation and publishes only the unfrozen suffix, checkpoint failure leaves the feed replayable across restart, and startup rehydration drains the frozen feed before snapshot replacement.
- Verified BIN-SYNC code/test head `e5f7c493d461671c8b0b88940127a702ee2a4dd9`: core 648/648, Fuzz/Lab 31/31, coverage 70.4%; Storage Integration 110/110 on Ubuntu, Windows and macOS; both in-process and real-TCP Storage samples pass on all three OSes. PR #16 remains Draft/open/unmerged.
- Incremental retry benchmark (GitHub Actions run `42440960333`, .NET 10.0.14, Ubuntu 26.04.1, AMD EPYC 9V74): 256 rows = 1.523 ms / 875.93 KB; 1,024 rows = 3.829 ms / 3,516.38 KB; 4,096 rows = 15.869 ms / 14,148.12 KB.
- The first incremental run used AMD EPYC 9V74 while the exploratory full-snapshot baseline (§7.27) used Intel Xeon Platinum 8573C, so that initial cross-runner comparison was directional only. The matched-runner comparison and its performance decision are recorded in §7.30; see BIN-SYNC docs/METADATA-STORE-BENCHMARKS.md for raw reports and caveats.

**Remaining gates:** reduce feed materialization/operation-encoding/copy-on-write allocations if profiling justifies it; deterministic physical Storage crash cuts; legacy Node/Transport state migration and receipt/dedup/tombstone GC; and FLD's `(author, JournalSeq, ChangeId) → BIN-SYNC OutboxSeq` provenance plus atomic staging/freeze integration. Keep exactly two DLLs: `Fld1.Database.dll → Fld1.Storage.dll`; do not add a third shared runtime DLL.

### 7.29 Metadata Store as the sole ChangeId authority for receive (2026-11-08)

The Storage-backed receive path now avoids keeping a second global ChangeId identity ledger in Node.

- `IMetadataStore.BeginPendingApply` returns a read-only per-operation apply mask derived from the durable global `(namespaceId, ChangeId)` ledger: `true` means this exact fingerprint is new; `false` means the operation was already accepted and Node must skip it.
- The decision mask is not persisted as another metadata table. PendingApply itself reserves its ChangeIds until finalization. A second PendingApply with the same namespace/ChangeId is rejected until the first finalizes; the legacy immediate `AcceptIncomingBatch` path checks the same reservation. On restart, `ReadPendingApplyIntents` recomputes the same mask from the durable ChangeId ledger plus exclusive pending reservations.
- `SyncSession.ReceiveBatchAsync` passes the mask from the durable before-apply callback into Node. In Metadata-Store-authoritative receive mode, Node applies only operations marked new, does not read or update Node `GlobalChangeLedger`, and does not append a duplicate `RemoteBatch` record to Node WAL. PendingApply is the protocol WAL/recovery source; provider application and Metadata Store receipt/dedup/relay finalization remain ordered before network ACK.
- The legacy Node-owned receive/transport path still uses Node `GlobalChangeLedger` and Node WAL. This change does not yet remove legacy protocol state or migrate old Node-owned receive history; the two paths remain explicitly separated.
- Conformance regressions verify mixed duplicate/new masks, stable mask reconstruction from pending intents, reservation of unfinalized ChangeIds against both another PendingApply and immediate accept, and conflict rejection. Storage integration verifies metadata receive does not create Node global-ledger rows, lost-ACK replay remains a durable duplicate, and incremental provider cache preserves field/record tombstone ordering.
- Verified BIN-SYNC code/test head `e5f7c493d461671c8b0b88940127a702ee2a4dd9`: core 648/648, Storage Integration 110/110 on Ubuntu/Windows/macOS, Fuzz/Lab 31/31, 70.4% line coverage. PR #16 remains Draft/open/unmerged.

**Remaining gates:** deterministic crash cuts at every boundary from PendingApply staging through Node checkpoint, provider apply, Metadata Store finalization and ACK; legacy Node-ledger/WAL migration or explicit fail-closed fencing before authority switch; physical Storage/power-loss testing; receipt/dedup/tombstone GC horizons; and FLD JournalSeq provenance/atomic staging integration.
### 7.30 Matched-runner full-snapshot vs incremental retry benchmark (2026-11-08)

GitHub Actions workflow run `42444167368` checked out the full-snapshot baseline at `8e6833e5aa5fe7086dbd72b18cf71e45f30169c9` and the incremental implementation at `69ed0b5c964357a3c90552b9d697026d86280447`, then ran both sequentially on the same Ubuntu 26.04.1 / AMD EPYC 7763 / .NET 10.0.14 runner with the same BenchmarkDotNet 0.14.0 configuration.

| Pending provider-feed rows | Full snapshot mean | Incremental mean | Mean latency reduction | Full snapshot allocation | Incremental allocation | Allocation reduction |
|---:|---:|---:|---:|---:|---:|---:|
| 256 | 3.796 ms | 2.308 ms | 39.2% | 1.52 MB | 876.14 KB | 43.7% |
| 1,024 | 7.384 ms | 4.746 ms | 35.7% | 6.47 MB | 3,516.05 KB | 46.9% |
| 4,096 | 40.997 ms | 17.522 ms | 57.3% | 33.92 MB | 14,147.82 KB | 59.3% |

Relative standard deviations were 36.9% / 16.1% / 2.16% for the full-snapshot path and 12.3% / 12.8% / 2.13% for the incremental path at 256/1,024/4,096 rows. The 256-row baseline is noisy; the 4,096-row comparison is stable. BenchmarkDotNet still warns that stateful single-invocation iterations are below its recommended 100 ms window.

**Decision:** retain incremental publication. The matched-runner comparison confirms lower mean latency and managed allocation at all three tested feed sizes, with the strongest stable result at 4,096 rows. This closes the same-runner retry benchmark gate; continue profiling remaining feed materialization, operation encoding and changed-record copy-on-write allocations before further optimization. The full snapshot remains required for initial cutover and startup recovery.

Raw CSV/Markdown reports are attached to BIN-SYNC workflow run `42444167368` as artifact `metadata-retry-comparison`.

### 7.31 HLC-coalesced incremental Node cache and latest cross-platform verification (2026-11-08)

The latest BIN-SYNC code/test head is `010742085147b582ba551e3d2df1a65b0b595394`.

- The incremental provider-feed path now maintains a separate coalesced winner map for the Node data cache, keyed by canonical record, field and record-tombstone identity. For repeated changes to one field, the cache receives only the maximum HLC winner; a later source-feed sequence with an older HLC cannot overwrite a newer value.
- The optimization shares the outbound winner map until the first already-frozen ChangeId is encountered. At that immutable boundary, it forks the Node-cache winner map: Node retains the maximum-HLC winners across the complete scanned prefix, while outbound publication discards the already-frozen prefix and coalesces only the not-yet-frozen suffix. This preserves the freeze/retry immutability rule.
- The regression `Incremental_provider_cache_coalesces_repeated_field_edits_by_maximum_HLC` verifies provider state, Node cache state, one outbound sequence for two repeated edits, and feed acknowledgement when the later feed row carries an older HLC.
- Latest verified CI on this code head: BIN-SYNC Core Release build and 648/648 tests; Fuzz + Lab 31/31; 70.4% line coverage; FLD Storage Integration 111/111 independently on Ubuntu, Windows and macOS. All jobs succeeded.
- The matched-runner retry benchmark decision in §7.30 remains valid. The HLC coalescing change has regression coverage but no separate before/after benchmark; do not attribute additional measured speedup or allocation reduction to this change.

**Remaining gates:** profile feed materialization, operation encoding and changed-record copy-on-write; deterministic physical Storage crash cuts and power-loss testing; legacy Node-owned receive/transport and other-author relay state migration; receipt/dedup/tombstone GC horizons; and FLD `(author, JournalSeq, ChangeId) → BIN-SYNC OutboxSeq` provenance with atomic staging/freeze recovery. Keep exactly two DLLs: `Fld1.Database.dll → Fld1.Storage.dll`; do not add a third shared runtime DLL.


### 7.32 Deterministic on-disk WAL/snapshot crash-state matrix (2026-11-08)

BIN-SYNC adds `StorageCrashConsistencyTests` in `BinSync.Net/tests/BinSync.Storage.Integration`. These tests reconstruct the physical file layouts that can remain after interruption at key Storage commit/checkpoint boundaries; they exercise the real `Fld1.Storage` engine and files rather than a mocked adapter.

- **Final torn WAL append:** an incomplete final envelope tail is truncated to the last valid record; the database accepts a new commit and survives another reopen.
- **Committed-record corruption:** a complete record with a damaged CRC fails closed and the WAL file is not truncated.
- **Non-final torn segment:** a torn tail in a sealed/non-final segment fails closed rather than discarding later history.
- **Interrupted WAL rotation:** an orphan `.log.tmp` file is ignored during recovery and removed when the next rotation retries publication.
- **Snapshot before manifest publication:** the old manifest and old snapshot remain authoritative; a complete but unreferenced new snapshot plus orphan `.tmp` files are ignored, and the WAL replays the post-snapshot commit.
- **Snapshot after manifest publication but before WAL reclamation:** the new manifest/snapshot is authoritative; retained old WAL records at or below the snapshot CommitSeq are skipped, and recovery remains contiguous.
- **Manifest corruption or missing referenced generation:** both cases fail closed; no silent fallback to an older WAL/snapshot state is attempted.

**Verification:** GitHub Actions run `42446095432` passed 119/119 Storage Integration tests independently on Ubuntu, Windows and macOS. On the same code/test head `2667fb81f41f726d2c286b98f34acd7857c2a7eb`, run `42446095441` passed 648/648 BIN-SYNC Core tests, 31/31 Fuzz/Lab cases and the 70.4% line-coverage gate. PR #16 remains Draft/open/unmerged.

**Scope limitation:** these are deterministic reconstructed on-disk crash states, not forced power loss or injected OS/filesystem failures at each `Write`, `Flush(true)`, `Move` or directory-fsync call. True power-loss/I/O fault injection and the coordinator-level PendingApply → provider effect → Metadata Store finalize → Node checkpoint → ACK process-kill matrix remain open. Do not mark the full crash-consistency audit complete.


### 7.33 WAL writer-fence hang diagnosis and bounded test cleanup (2026-11-08)

The interrupted continuation exposed a long-running Storage Integration test around cross-process WAL writer ownership. The investigation isolated the regression to an experimental OS file-share lease: it could stall the test/lock lifecycle. That implementation was reverted; the current implementation is the named OS-managed Mutex with a dedicated process-wide owner thread. Do not reintroduce the file-share lease without a separate cross-platform proof.

- The manager thread owns every named Mutex acquisition/release, so callers may dispose a StorageDatabase from a different thread without violating Mutex thread affinity.
- The cross-process tests distinguish normal owner-process termination from forcibly killing the actual testhost process that owns the database. The latter writes its PID explicitly; killing only the outer dotnet test process is insufficient.
- Both child-process tests now have unconditional cleanup: signal the child to exit, kill its process tree if it remains alive, wait with a bounded timeout, and only then remove the temporary directory.
- Storage Integration now runs dotnet test with --blame-hang-timeout 3m --blame-hang-dump-type mini, matching the core test workflow. A stuck test therefore produces hang diagnostics instead of silently consuming the whole 30-minute job timeout.
- The cross-thread release test comment was corrected to describe the named-Mutex manager-thread implementation rather than the reverted file-share lease.

**Verification on BIN-SYNC HEAD 532baa0010140b799f3759e1c88d0c722cdf6727:** Core Release 648/648; Fuzz + Lab 31/31; line coverage 70.4%; Storage Integration 122/122 on Ubuntu and Windows; in-memory and TCP samples pass. The macOS-15 Storage Integration job is still queued, so cross-platform completion is not yet claimed. PR #16 remains Draft/open/unmerged.

**Next gates remain:** coordinator-level process-kill/reopen cuts through PendingApply staging, Node checkpoint, provider apply, Metadata Store finalization and ACK; true physical Storage/power-loss I/O fault injection; retirement of legacy Node-owned receive/transport and other-author relay state; receipt/dedup/tombstone GC horizons; and FLD (author, JournalSeq, ChangeId) → BIN-SYNC OutboxSeq provenance with atomic staging/freeze recovery. Keep exactly two DLLs: Fld1.Storage.dll and Fld1.Database.dll; do not add a third shared runtime DLL.




### 7.34 Coordinator process-kill recovery across the receive durability sequence (2026-11-08)

Added real subprocess crash tests around the Storage-backed receive coordinator. The parent test starts a filtered child testhost, waits for a durable crash-cut marker, kills the actual process holding the WAL Mutex, reopens the same physical database, and verifies the state appropriate to that commit boundary.

Four boundaries are now exercised by process termination rather than only injected exceptions:

- **After PendingApply staging, before Node apply:** the intent and exact wire payload are durable; the Node checkpoint has no incoming field and provider state is absent. Recovery replays the immutable intent.
- **After Node checkpoint, before provider apply:** PendingApply and the applied Node checkpoint are durable; provider state is absent. Recovery completes provider application and metadata finalization.
- **After provider commit, before Metadata Store finalization:** Node checkpoint and provider mutation are durable while PendingApply remains. Recovery repeats provider apply using the same ApplyToken, recognizes the idempotent receipt and finalizes metadata.
- **After Metadata Store finalization, before network ACK:** Node/provider state and the durable receive cursor/receipt are committed, PendingApply is already gone, but the callback blocks before ACK. After process death, recovery correctly finds no pending intent and the committed cursor remains advanced.

The process-kill harness terminates the actual testhost PID, then reaps the outer dotnet test process. On Windows, the parent retries WAL reopen for up to ten seconds because the OS may release the killed process's WAL file handle slightly after process exit. Child cleanup remains unconditional and bounded.

**Session checkpoint contract:** a successful SyncSession handshake mutates the Node's negotiated alias table and peer epoch. These values are required to decode a staged wire payload during restart recovery. Therefore StorageNodePersistence.Save must be called after each successful handshake and before the first inbound durable batch. The persistence API documentation and both Storage array/TCP samples now demonstrate this ordering.

Storage Integration runs with --blame-hang-timeout 3m --blame-hang-dump-type mini so a future stalled test yields diagnostics.

**Verification on BIN-SYNC HEAD 354eab38dc859e2f26838efb332c14310aa17022:** Core Release 648/648; Fuzz + Lab 31/31; line coverage 70.4%; Storage Integration 127/127 independently on Ubuntu, Windows and macOS-15; both in-memory and real-TCP samples pass on all three platforms. PR #16 remains Draft/open/unmerged.

**Remaining:** true physical power-loss/I/O fault injection; retirement of legacy Node-owned receive/transport and other-author relay state; receipt/dedup/tombstone GC horizons; and FLD (author, JournalSeq, ChangeId) → BIN-SYNC OutboxSeq provenance with atomic staging/freeze. Keep exactly two DLLs: Fld1.Storage.dll and Fld1.Database.dll; do not add a third shared runtime DLL.


### 7.35 FLD Storage WAL writer-fence parity with BIN-SYNC (2026-11-08)

A source comparison found that the standalone `src/Fld1.Storage` copy in FLD-DB had not yet received the single-writer fence already verified in BIN-SYNC's `external/Fld1.Storage` copy. The FLD version also loaded the snapshot manifest **before** opening/fencing the WAL, allowing a second process to race snapshot recovery against a live checkpoint.

The source parity gap is now closed:

- Ported the named OS-managed `WalWriterFence` and fenced `WalManager.Open` implementation from the verified BIN-SYNC Storage copy.
- `StorageDatabase.Open` now acquires the partition writer fence through `WalManager.Open` before loading the snapshot manifest or replaying WAL.
- A dedicated process-wide manager thread owns Mutex acquisition/release. This preserves Mutex thread affinity while allowing database disposal from another caller thread; abandoned ownership is recovered before WAL replay.
- Physical path canonicalization covers symlink targets and path/case aliases; a second same-process or cross-process writer is rejected.
- Added FLD Storage tests for same-partition second-open rejection, cross-thread disposal/reopen and abandoned-mutex recovery.
- No FLD1 run/WAL/manifest wire format or database-level field semantics changed.

**Verification:** FLD-DB code/test head `0efa1cdb8287ee82082186a5dcb8c163baf3528a` built successfully and passed 163/163 tests independently on Ubuntu, Windows and macOS-15: Core 23, Engine 58, Manifest 9, Run 25, Storage 31 and WAL 17. The FLD workflow runs the full solution test suite on all three operating systems.

**Backup:** `backup/pre-fld-storage-wal-fence-port-2026-11-08` preserves the FLD branch before this port.

**Next:** physical Storage write/flush/rename failure injection and power-loss testing remain open. The writer fence is a single-writer correctness prerequisite, not proof of power-loss durability.


### 7.36 Deterministic Storage durability-boundary fault injection (2026-11-08)

The physical on-disk crash matrix exposed one retry gap: after a crash between publishing a snapshot generation and publishing its manifest, the unreferenced generation could remain on disk and block the next checkpoint from reusing that generation. A failed checkpoint also left the live StorageDatabase instance usable even though its in-memory generation no longer represented the authoritative manifest/WAL pair.

The generic Storage implementation now addresses both cases:

- On open, after validating the authoritative manifest and referenced snapshot, `StorageSnapshotFile.Load` removes unreferenced snapshot generations and abandoned `.snap.tmp` / `.manifest.tmp` files for that partition. If no manifest exists, every snapshot generation is orphaned and is removed. Cleanup is partition-scoped and does not run when a referenced manifest/snapshot is corrupt; corruption remains fail-closed.
- `StorageDatabase.Checkpoint` marks the live instance faulted after any checkpoint exception. Reopen re-reads the manifest/WAL pair and is the only path back to a usable instance.
- Added internal-only deterministic fault points after WAL append/before flush, after WAL flush/before in-memory publication, after snapshot temp-file flush, after snapshot rename, after manifest temp-file flush and after manifest rename/before WAL reclamation. These hooks are not public Storage API.
- Regressions inject each boundary failure, verify the live instance rejects further access, reopen, verify the committed value is recovered exactly once, and successfully run another checkpoint. A separate regression proves that an orphan snapshot generation and temp files are cleaned so checkpoint generation reuse succeeds.

**Verification:** FLD-DB code/test head `0efa1cdb8287ee82082186a5dcb8c163baf3528a`: 163/163 tests on Ubuntu, Windows and macOS-15. BIN-SYNC code/test head `1961a84a90aca48c8aacf2c0c39cff4008a208aa`: Core 648/648, Fuzz/Lab 31/31, Storage Integration 135/135 independently on Ubuntu, Windows and macOS-15; line coverage 70.4%.

**Limit:** these are deterministic exceptions at successful I/O boundaries plus reconstructed on-disk crash states. They do not yet inject partial OS writes, real fsync/device errors, directory-entry durability loss or actual power failure. Those remain open; do not mark the physical I/O/power-loss gate complete.


### 7.37 Namespace-scoped Node authority fence includes sealed local writes (2026-11-08)

The BIN-SYNC provider-cutover fence now rejects `WriteFieldSealed` in addition to ordinary local field/record writes, direct/manual Outbox publication, public Node batch creation and the legacy Node-owned transport builder. The check is applied at the Node API boundary and is namespace-scoped: other namespaces on the same Node retain their independent Node-owned path. Metadata-reserved outbound sends remain allowed. The durable provider cutover marker reinstalls the fence when the coordinator is reconstructed.

- Regression coverage configures an E2EE key ring and verifies `WriteFieldSealed` fails closed after cutover.
- Latest BIN-SYNC code/test head at this checkpoint: `92f6631871072799087ee1732f4d65bdaeeff523`.
- Verified CI: Core 648/648, Fuzz + Lab 31/31, Storage Integration 136/136 on Ubuntu, Windows and macOS-15; line coverage 70.4%.
- PR #16 remains Draft/open/unmerged.

**Next:** finish the explicit policy for legacy Node-owned mode in namespaces that never enter provider cutover (including volatile/non-Storage use). Keep receipt/ChangeId/tombstone GC disabled until contiguous-prefix/snapshot-barrier and external peer-retirement proofs exist. Physical partial-write/fsync/device/power-loss injection, the 1,000,000-row mutation benchmark and full normative cross-implementation audit remain open. Do not begin FLD JournalSeq provenance integration until BIN-SYNC and Storage gates are complete.


### 7.38 Complete namespace cutover authority and relay cache cleanup (2026-11-08)

The provider-authoritative cutover now removes every retained Node Outbox payload projection in the namespace, not only local-author rows. This includes legacy relay payloads left by pre-cutover Node-owned receive. Per-author sequence high-watermarks and WAL/ChangeId replay identities remain intact; relay payloads already committed to Metadata Store are retained and remain the only downstream delivery source. The same cleanup is repeated during startup/reconciliation when a stale pre-cleanup Node checkpoint is loaded.

The Node namespace fence now rejects legacy Node-owned receive validation in addition to local writes, manual Outbox publication and the legacy Node-owned outbound batch builder. `StorageSyncCoordinator.ReceiveBatchBeforeAckAsync` remains available after cutover and is tested. The real-TCP array sample now uses that Metadata-Store-authoritative receive path instead of the legacy Node-owned receive callback.

- Added regression coverage for dropping a stale legacy relay cache row while preserving the Metadata Store relay, and for successful Metadata-Store-authoritative receive after cutover.
- Verification at BIN-SYNC code/test head `92f6631871072799087ee1732f4d65bdaeeff523`: Core 648/648, Fuzz + Lab 31/31, Storage Integration 136/136 on Ubuntu, Windows and macOS-15; both in-memory and real-TCP samples pass on all three platforms; line coverage 70.4%.
- Metadata freeze cardinality benchmark at 10k/100k unrelated ledger rows: 506.8/431.0 µs mean and 9.63/10.46 KB allocated. This is a short-window structural check only; 1m rows and deployment hardware remain open.
- Backup branches created in both repositories: `backup/post-cutover-receive-fence-2026-11-08` (BIN-SYNC snapshot at the verified code head; FLD-DB snapshot at the current roadmap head).

**Remaining before FLD integration:** legacy Node-owned mode policy for non-cutover/volatile namespaces; physical partial-write/fsync/device/power-loss proof; external peer-removal and safe GC horizons; 1m/deployment benchmark; requirement-ID and cross-implementation normative audit. The two-DLL decision remains `Fld1.Storage.dll` plus `Fld1.Database.dll`.


## 2026-11-08 — BIN-SYNC v3 cross-repository alignment

**Normative references:** BIN-SYNC CORE v3.0.0 in `smersh1307n2/BIN-SYNC`, `docs/BIN-SYNC-ROADMAP-v3.md`; current BIN-SYNC implementation branch `feature/fld-external-storage`, exact green code/test head `3b9db5bf97550036fce6b5b145a8717b806f6d72`. Proposed v3.1 signed roster/GC design is in `docs/BIN-SYNC-v3.1-ROSTER-GC-ADDENDUM.md` and is not yet implemented. FLD implementation branch remains `fix/crash-consistency-outbox` at `fd9f48f5ef7ec181bcacf2891bc45d8c1c11b28c`; no FLD source-code changes are included in this BIN-SYNC pass.

- [x] Preserve exactly two runtime DLLs: `Fld1.Storage.dll` (opaque byte engine) and `Fld1.Database.dll` (field semantics, Database → Storage).
- [x] Correct the integration contract: FLD `OutboxSeq`, APPLY `JournalSeq` and WAL `CommitSeq` are local source/recovery coordinates, never BIN-SYNC `OriginSequence`.
- [x] Record that BIN-SYNC Metadata Store allocates durable `OriginSequence` at publication freeze and owns published ChangeId/Origin identity, receipts, peer delivery state and relay payload.
- [x] Record that no separate duplicate PendingLocal journal is permitted; coalescing ends at durable publication freeze.
- [x] Make tombstone/ghost GC contingent on the independent v3 membership, author-epoch retirement, complete-resync and safe-frontier proof; TTL/HLC age alone is insufficient.
- [ ] Implement the BIN-SYNC Metadata Store adapter over the neutral FLD Storage API (including BMS3 identity, transaction semantics, ordered scans and compact per-peer/per-author state).
- [ ] Implement the FLD Data Provider adapter for durable local feed, snapshots and idempotent remote apply without making FLD Storage depend on BIN-SYNC.
- [ ] Remove the legacy FLD ApplyJournal/RelayJournal payload path as an independent authoritative relay history after the v3 adapter becomes authoritative; retain only required field-state recovery and provider apply receipts.
- [ ] Prove crash consistency across FLD field APPLY and BIN-SYNC PendingApply/receipt when both share one Storage transaction domain; define idempotent recovery when stores are separate.
- [ ] Implement and test v3 receipt, ChangeId and tombstone/ghost GC horizons independently.
- [ ] Run cross-platform crash/reopen, multi-hop relay, duplicate/equivocation and 500/10k/100k/1M metadata cardinality benchmarks before declaring FLD integration complete.


### 7.39 BIN-SYNC v3.0 implementation checkpoint and v3.1 GC design (2026-11-08)

- [x] Verified exact BIN-SYNC code/test head `3b9db5bf97550036fce6b5b145a8717b806f6d72`: Core 665/665, Fuzz/Lab 31/31, Storage Integration 168/168 on Ubuntu/Windows/macOS.
- [x] Promoted ProviderMutation canonical codec and streaming FULL_REPLACE digest builder into the BIN-SYNC Core assembly; StorageDataProvider now delegates to the shared codec. This is the intended dependency seam for the future FLD provider adapter.
- [x] Implemented Storage provider generation-scoped state, strict BSP3 format marker, atomic FULL_REPLACE switch, bounded staging/abort, and old-generation retention. Field/record tombstone preservation tests pass; no physical generation GC is enabled.
- [x] Implemented conservative receipt retirement: pending outbound reservation must be exactly ACKed before the epoch fence or be covered by a completed replacement baseline with the same author epoch; short/different-epoch baselines block retirement.
- [x] Added the v3.1 signed roster/frontier/retention-checkpoint/FULL_REPLACE installed-ACK design. It preserves one shared ChangeId row per operation and O(members × authors) frontier metadata, not per-change/per-peer history.
- [x] Finished the one-million-row BIN-SYNC metadata benchmark at exact head `3b9db5b`: 1,024-operation freeze over 1,000,000 existing ChangeId rows measured 218.1 µs / 7.77 KB. This is slower than the earlier 196.9 µs / 7.77 KB run, so it is recorded as evidence, not a performance improvement.
- [ ] Implement v3.1 negotiated roster/frontier/checkpoint wire/storage path and adversarial tests. Keep ChangeId and tombstone/Ghost GC disabled until complete.
- [ ] Implement the BIN-SYNC Metadata Store and Data Provider adapters over `Fld1.Storage` without adding a reverse dependency from Storage to Database/BIN-SYNC.
- [ ] Map FLD `JournalSeq` to local source-feed sequence only; BIN-SYNC allocates `OriginSequence` at durable freeze. Add one atomic publication gate across FLD pending state and BIN-SYNC freeze/ACK, with crash recovery.
- [ ] Complete FLD Database migration to `Fld1.Storage`/`Fld1.Database`, then implement Shared Engine/IPC for multiple applications. Direct embedded Storage remains single-writer fenced.
- [ ] Extend FLD fault injection to partial writes, fsync/device errors, directory-entry durability and actual power-loss testing; run deployment-hardware benchmarks.


### 7.40 Storage durability gap closure — partial writes and directory entries (2026-11-08)

- [x] Compared BIN-SYNC's vendored Storage source against FLD-DB's authoritative `src/Fld1.Storage`; identified a divergence in WAL fault injection, partial-write handling and directory-entry durability. Avoided blind copy in either direction.
- [x] Ported the tested vendor-side implementation into FLD-DB Storage: durable nested directory creation; explicit Unix `open(O_DIRECTORY)` + `fsync` + `close`; directory fsync after WAL segment create/rename/reclamation and snapshot/manifest publication; Windows explicitly makes no directory-entry durability claim.
- [x] Added injectable `AfterWalPartialWrite`, `BeforeWalFsync`, and `BeforeWalDirectoryFsync` failure boundaries. Partial writes and fsync failures stop further writes and require reopen; failed post-delete directory sync faults the WAL manager for reopen/recovery.
- [x] Expanded `StorageCrashConsistencyTests` to cover nested directory creation, Unix directory-fsync error handling, post-reclamation directory-fsync failure, partial WAL write truncation and WAL fsync failure semantics.
- [x] Exact-head FLD CI passed at code/test head `fd9f48f`: Ubuntu/macOS/Windows all green; `Fld1.Storage.Tests` 37/37 on all three OS; all six test assemblies passed (169 total on Windows).
- [ ] Add actual device/fsync/power-loss testing; process-level injected fsync failures do not prove a particular physical device's post-power-loss state.
- [ ] Refresh BIN-SYNC's `external/Fld1.Storage` snapshot from the exact tested FLD source commit and update its source pin; then rerun BIN-SYNC Core + Storage Integration on all platforms.
- [ ] Implement the Shared Engine/IPC multi-application access path; single-owner fencing remains the current embedded mode.


### 7.41 FLD host-side draft horizon and live-reference primitives (2026-11-09)

- [x] Added \`FldDatabase.AcquireCommitGuard()\` to expose the existing FLD commit monitor as a same-thread disposable lease. The BIN-SYNC host adapter composes this after coordinator/Node gates and before the shared BIN-SYNC Storage gate.
- [x] Added \`ReadUnpublishedLocalDraftHorizon()\`: counts uncommitted dirty overlay operations plus the unacknowledged Outbox suffix and returns the maximum HLC. A healthy \`FldDatabase\` has already reconciled any WAL→Outbox gap during open/recovery; a faulted database fails closed.
- [x] Added \`HasPendingReceiptReference()\` over the durable ApplyJournal batch identity (sender peer, sender transport epoch, issuer author epoch, namespace, batch id). Any retained ApplyJournal entry blocks receipt reclamation; an unsealed WAL prefix also blocks conservatively because it may replay an APPLY receipt.
- [x] Added \`HasPendingChangeIdReference()\` and \`HasPendingRecordReference()\` over dirty local state, unacknowledged Outbox entries and retained ApplyJournal field/relay payloads. Any unsealed WAL prefix blocks conservatively until checkpoint/seal; malformed/faulted state throws rather than authorizing deletion.
- [~] These are neutral FLD engine primitives only. The thin adapter implementing BIN-SYNC \`IApplicationGcReferenceScanner\` belongs in the integration layer and must forward the partition identity on every call; FLD does not reference BIN-SYNC types.
- [~] Exact-head CI is running for this code slice. Do not treat these APIs as validated until all FLD test assemblies pass on the exact branch head.


### 7.42 Zero-copy host safety scans over the Outbox suffix (2026-11-09)

- [x] Added read-only \`OutboxLog.VisitUnackedBatch\` using borrowed \`OutboxEntryView\` spans. It does not acquire/fence a consumer lease and cannot mutate or ACK the Outbox.
- [x] Reworked FLD draft-horizon, ChangeId and RecordId scans to consume borrowed Outbox views in bounded 512-entry pages. Large field values are not copied merely to inspect IDs/HLC; the cursor advances only through visited entries.
- [x] ApplyJournal reference scans remain bounded in 256-record pages. Any retained receipt/relay payload remains a live reference; any unsealed WAL prefix blocks ChangeId/record/receipt reclamation conservatively.
- [~] Benchmark allocations and CPU on large Outbox values and 1M-entry histories in the T/O pass. Exact-head CI is pending for this commit.


### 7.43 Indexed immutable reference snapshot for host adapters (2026-11-09)

- [x] Added \`FldDatabase.OpenReferenceSnapshot()\` and immutable \`FldReferenceSnapshot\`. One pass indexes ChangeIds, RecordIds and receipt identities from dirty local state, unacknowledged Outbox and retained ApplyJournal fields/relay payloads.
- [x] The snapshot also carries the unpublished draft horizon (dirty edits + pending Outbox suffix). If an unsealed WAL prefix remains, it forces a non-zero conservative horizon because recovery may reconstruct acknowledged/compacted Outbox rows after restart. The scan uses borrowed Outbox views and bounded 512-entry pages; ApplyJournal is read in bounded 256-record pages.
- [x] Any unsealed WAL prefix is a conservative global live-reference result for receipt/ChangeId/RecordId queries until checkpoint/seal. A malformed or faulted source throws; no query can accidentally authorize deletion.
- [x] Restored \`ReadUnpublishedLocalDraftHorizon()\` as a narrow scan that does not unnecessarily build the full ApplyJournal reference index.
- [~] The BIN-SYNC adapter should open one FLD reference snapshot per \`IApplicationGcReferenceScan\` and reuse it across all candidates. Allocation/throughput benchmarks and exact-head CI remain open.


### 7.44 Unsealed WAL blocks final-author closure even after Outbox ACK (2026-11-09)

- [x] Draft horizon now remains non-zero while \`PendingFlushedCommitSeq > LastSealedCommitSeq\`, even when all Outbox entries appear ACKed. A crash/reopen before checkpoint may reconstruct compacted Outbox rows from WAL; those rows must not be reinterpreted under a rejoined author epoch.
- [x] The reference snapshot also returns live for every receipt/ChangeId/RecordId query while WAL is unsealed. The host must checkpoint/seal the source WAL before a zero-draft final-author report or destructive GC proof can proceed.
- [x] The unsealed-WAL barrier change passed exact-head CI on Ubuntu, Windows and macOS at `0db64f8d8f78f714fb197e0f375a536dcdec47d6`.


### 7.45 BIN-SYNC durable provenance-map primitive (2026-11-09)

BIN-SYNC `feature/fld-external-storage` now contains `BinSync.Storage.StorageRelayProvenanceMap`, a neutral Storage-backed primitive for the later synchronized-FLD adapter:

- Per-(scope, local replica, namespace, original author) durable `JournalSeq ↔ OutboxSeq` mapping and scan/ACK cursor. ChangeId→OutboxSeq remains in the global BIN-SYNC ChangeId ledger and is resolved before mapping append.
- Atomic Storage write batch for mapping rows + scanned cursor + RPM3 mapping-set checksum; restart recomputes the SHA-256/XOR checksum over bounded 1,024-row pages and fails closed on mismatch.
- OutboxSeq-ordered keys, lower-bound O(log N) safe-ACK translation, same-JournalSeq group retention, and no payload/OriginSequence/per-peer ACK duplication.
- Durable prune is explicitly after FLD `AcknowledgeRelayBatches` succeeds; the public sample demonstrates restart and partial JournalSeq-group ACK boundaries.

This closes only the reusable BIN-SYNC mapping-store primitive. It does **not** implement the FLD synchronized-database adapter, outbound publication gate, ChangeId resolver call, FLD commit-guard composition or concrete `IApplicationGcReferenceScanner`. The sequencing in §7.20 remains controlling: do not add a direct Node↔FldDatabase bridge or integration NuGet project until the neutral semantic/Data Provider boundary, BIN-SYNC persistence refactor and approved two-public-DLL Storage/Database extraction are complete. No FLD project dependency or public assembly boundary is changed by this primitive.


### 7.46 Recovery checkpoint branch (2026-11-09)

Created `backup/pre-binsync-provenance-map-2026-11-09` at `a962371ec293708f61f02453f9bc7c441eafa015` before further cross-repository integration work. Earlier FLD backup branches remain unchanged.


### 7.47 Current FLD relay/reference API surface confirmed (2026-11-09)

The current pre-extraction `Fld1.Engine.Db.FldDatabase` already exposes the core semantic operations needed by the later neutral adapter:

- `ReadRelayBatchPage(ReadOnlySpan<byte> authorNodeId, ulong afterJournalSeq, int limit)` returns whole ApplyJournal batches for one original author and a global `ScannedThroughJournalSeq`, including gaps with no matching author.
- `AcknowledgeRelayBatches(ReadOnlySpan<byte> authorNodeId, ulong safeThroughJournalSeq)` persists the per-author source ACK.
- `AcquireCommitGuard()` supplies the thread-affine application commit lease.
- `ReadUnpublishedLocalDraftHorizon()` reports dirty overlay + unacknowledged Outbox + conservative unsealed-WAL barrier.
- `OpenReferenceSnapshot()` builds one immutable index of ChangeId, RecordId and receipt references. `FldReferenceSnapshot` exposes `UnpublishedLocalDraftHorizon`, `HasChangeIdReference`, `HasRecordReference` and `HasReceiptReference`.

This means the semantic operations and reference-index implementation exist in the current Engine project; they are not yet extracted behind the approved neutral/two-DLL public boundary. The BIN-SYNC adapter can be a thin wrapper after that extraction, but must not be added as a direct Engine→BinSync dependency or premature integration package now. The existing roadmap order in §7.20 remains unchanged.


### 7.48 RPM3 removes duplicated ChangeId from provenance rows (2026-11-09)

The BIN-SYNC provenance map was tightened after byte-cost review. RPM3 persists only OutboxSeq in the ordered key and JournalSeq in the value; it does not copy the 16-byte ChangeId already retained by the global BIN-SYNC ChangeId ledger. The synchronized-FLD adapter must still call `Node.TryResolveOutboxSequences` for every (author, ChangeId) before appending the resolved sequence pair. Strictly increasing OutboxSeq plus the global ledger preserve identity uniqueness across restart without a second ChangeId index in the map.

The compact map row is 84 bytes (76-byte key + 8-byte value), down from 100 bytes. The 60-byte RPM3 cursor retains the atomic SHA-256/XOR mapping-set checksum. This is a storage/metadata reduction only; the §7.20 two-DLL and adapter sequencing remains unchanged.

### 7.49 BIN-SYNC coordinator-side RPM3 ChangeId resolver (2026-11-09)

BIN-SYNC `feature/fld-external-storage` now exposes `StorageSyncCoordinator.AppendRelayProvenancePage` as the first concrete resolver/append primitive for the later synchronized-FLD adapter:

- It accepts only transient `(JournalSeq, ChangeId)` source identities and resolves them through `Node.TryResolveOutboxSequences` while the composed coordinator→Node→FLD commit guard is held.
- It validates the RPM3 map's scope, local-replica and shared `StorageDatabase` binding; rejects zero/decreasing/out-of-cursor JournalSeq values and pages larger than 4,096 operations before resolver allocations.
- It writes only `(JournalSeq, OutboxSeq)` rows and the scanned JournalSeq cursor atomically. ChangeId remains solely in the global BIN-SYNC identity ledger.
- Missing/compacted Node Outbox identity returns false without advancing the RPM3 cursor. Empty pages can advance the source scan cursor without allocating resolver arrays.

This is **not** the finished synchronized-FLD adapter. The FLD Database still needs to be extracted into the approved `Fld1.Database.dll` boundary; the host must then read whole `RelayJournalBatch` pages under `FldDatabase.AcquireCommitGuard`, call the coordinator helper before send, translate downstream safe Outbox ACK through RPM3, durably call `AcknowledgeRelayBatches`, and only then prune RPM3 mappings. The concrete `IApplicationGcReferenceScanner` wrapper must similarly hold the FLD guard while using one `FldReferenceSnapshot`.

The current `RelayProvenanceMap` BIN-SYNC sample exercises the low-level RPM3 storage codec with already-resolved sequence pairs. It is not an end-to-end FLD integration sample. No direct Engine→BIN-SYNC dependency or third runtime DLL is permitted.
### 7.50 Two-DLL extraction source audit: actual project graph and storage-engine seam (2026-11-09)

The current source graph was inspected directly; the two-DLL target is not yet achieved and cannot be closed by renaming assemblies or adding project references:

- `Fld1.Storage` is already an independent project with a domain-neutral byte-key/value `StorageDatabase`, atomic `StorageWriteBatch`, snapshots/prefix scans, and its own generic WAL. Its public API uses opaque byte spans/arrays and storage-native commit sequences.
- `Fld1.Engine` currently references `Fld1.Core`, `Fld1.Run`, `Fld1.Wal`, and `Fld1.Manifest`; `FldDatabase` directly imports their domain-specific HLC/UUID, RunBuilder/Reader, Manifest and WAL codec/recovery namespaces.
- `Fld1.Run` is not a generic storage run: it encodes FLD1 RecordId/FieldId/HLC/author/cluster/PGM semantics. `Fld1.Manifest` carries FLD run catalog and commit-result semantics. `Fld1.Wal` currently contains FLD commit payload/recovery paths. These cannot be moved wholesale into `Fld1.Storage`.
- `Fld1.Core` mixes potentially generic byte/hash helpers with FLD HLC, UUIDv7, IDs and canonical field semantics. It must be classified by type/dependency, not treated as a ready-made shared assembly.
- There is no `Fld1.Database.csproj` yet. The current `Fld1.Storage` project is not a replacement for `Fld1.Engine` and `FldDatabase` does not currently use `StorageDatabase` as its storage backend.

Required extraction work before creating the public `Fld1.Database.dll`:

1. Freeze the physical compatibility decision: whether FLD1 Run/Manifest/WAL remains the Database-owned file format layered over generic Storage coordination, or whether FLD data is migrated to opaque Storage keys/values. Do not silently replace the FLD1 format.
2. Split generic WAL framing/flush/recovery and file-generation coordination from FLD-specific commit codecs/replay handlers. Generic Storage must never decode a FLD field mutation.
3. Classify `Fld1.Core` types; move only genuinely domain-neutral byte/checksum primitives into Storage, and keep HLC/UUID/field identity in Database.
4. Create one `Fld1.Database` project and physically consolidate Database-owned Engine/Run/Manifest/FLD-WAL sources there; do not use linked source compilation or retain separate public runtime DLLs.
5. Make `Fld1.Database` reference only `Fld1.Storage` plus BCL/required packages; audit all public signatures so Storage exposes no FLD/BIN-SYNC types.
6. Re-home tests into Storage-only and Database integration suites; verify Storage-only consumers can build/run without loading Database. Only then add the BIN-SYNC adapter assembly outside both FLD DLLs.

**Status:** [x C] source audit/migration order documented; step 4 (one physical `Fld1.Database` project) is now implemented in §7.54/§7.60. [~ C] The physical-format choice details, generic WAL/file-generation extraction, Core type classification, actual `StorageDatabase` backend reuse, public API audit, test-suite re-homing and independent package proof remain open. The old FLD1 format must not be silently replaced; no third `Common` DLL or linked-source shortcut is allowed.
### 7.51 Architecture document status corrected to distinguish target from implementation (2026-11-09)

Updated `FLD-STORAGE-CORE-ARCHITECTURE.md` §2.1 to state explicitly that the two-DLL design is normative target architecture, not the current build graph. There is no `Fld1.Database.csproj`; `FldDatabase` still uses the Engine/Run/Manifest/WAL/Core projects and does not use `Fld1.Storage` as its physical backend. This prevents future roadmap readers from mistaking the architectural decision for completed extraction.

The physical compatibility gate remains mandatory: preserve the frozen FLD1 Run/Manifest/WAL format unless a separate versioned format decision authorizes a change. This historical status was superseded by the physical project move in §7.54/§7.60; generic Storage backend reuse remains incomplete. No third Common DLL and no linked-source compilation shortcut.
### 7.52 Recovery checkpoint after two-DLL architecture audit (2026-11-09)

Created `backup/cpass-2026-11-09-0915` at `94fd1ebdfa15b95b55184a8eee92a5ebea0a4d5e`, preserving the current FLD branch including the source graph audit and architecture-document correction. The latest code-equivalent CI remains green at `d49f710c1e2992f753278836d521157813d8b503`; changes after that point are documentation-only.
### 7.53 RPM3 adapter page bound is whole FLD journal group (2026-11-09)

Updated the cross-repository integration contract to read one `RelayJournalBatch` per page and reject a group with more than 4,096 relay operations before allocating the transient `(JournalSeq, ChangeId)` array. A JournalSeq group is atomic in the RPM3 mapping: it must not be split across independent mapping/cursor commits. The 4,096 limit was initially only an adapter bound. It is now enforced by `RemoteBatchCommit` and by aggregate `ReadRelayBatchPage` operation count in §7.57/§7.58. The latter preserves whole JournalSeq boundaries and resumes before an unconsumed batch.
### 7.54 Two-runtime-assembly physical project consolidation (2026-11-09)

- [x C] Added `src/Fld1.Database/Fld1.Database.csproj` and physically moved all 40 Core, Manifest, Run, WAL and Engine source files into it. It references the independent generic `Fld1.Storage.dll`; no linked-source Compile items are used.
- [x C] Changed `Fld.Db.slnx` runtime surface to the two approved source projects: `Fld1.Storage` and `Fld1.Database`. Component projects remain in the tree as historical source/project boundaries but are no longer solution runtime outputs.
- [x C] Routed Core/Engine/Manifest/Run/WAL test projects and both benchmark projects through the consolidated Database project; Storage tests continue to reference Storage directly.
- [~ T] The physical move and project graph are committed, but exact-head three-OS CI remains red and its diagnostic artifacts have not been returned by the connector. Treat the two-DLL structure as source-complete but build-unverified.
- [~ C] Remaining extraction: resolve CI/build issues, ensure publish/package output contains exactly the two runtime DLLs, and complete the generic Storage/legacy FLD WAL+Run reuse strategy. `Fld1.Database` still uses its FLD-specific WAL/Run/Manifest path; the separate generic `Fld1.Storage` engine is not yet the physical backend for `FldDatabase`.

### 7.55 Exact-head build state after two-assembly consolidation (2026-11-09)

- [x C] Branch HEAD before this note: `f971bbe2653415efb0bcb5468ec66bfa658dac2b`; the scaffold commits `Fld1.Database.csproj`, routes Core/Engine/Manifest/Run/WAL tests and benchmarks through it, and reduces the solution's runtime source projects to `Fld1.Database` + `Fld1.Storage`.
- [~ T] The previous exact-head workflow on `4f248e0edee5506d74f63ee5913ed3de193231f5` failed on Ubuntu, macOS and Windows. GitHub's job-log endpoint currently returns BlobNotFound, so the root cause is not yet verified. Do not attribute those failures to the new extraction without reading the new-head logs.
- [~ T] The new consolidated project has not yet completed a build/test run. Validate linked source globs, package graph, generated assembly metadata, all test projects and both benchmark projects before treating the two-DLL extraction as done.
- [~ C] Remaining: finish consumer/public API migration, make exactly two runtime assemblies the supported package output, retire or explicitly mark old component csproj entry points as internal-only, update docs and integration adapters. Keep FLD semantics independent of BIN-SYNC protocol types.

### 7.56 Consolidated assembly exact-head CI result (2026-11-09)

- [x C] The README now describes the two-runtime-assembly solution accurately and marks the consolidation as committed but not yet validated.
- [~ T] Exact HEAD `a0ea90b9490cc1d93eafd048008327a5bcb4a54f` produced two failed PR CI runs (three OS jobs each). Check-run annotations exist (1–2 per job), but the connected log/annotation endpoints returned unavailable data; root cause is not confirmed. Do not assume this is a source compile failure or blame a specific project file without diagnostics.
- [~ T] The current branch has advanced to `ec28803a9eb0ef26a8c3313be5ed75e97f18d104` with the roadmap note; then `a0ea90b9490cc1d93eafd048008327a5bcb4a54f` is the last commit with visible check-run diagnostics. Exact current-head status must be checked again after the next source/CI run.
- [~ C] Before further package/entry-point cleanup, obtain actionable build diagnostics for the consolidated project. The two-DLL structure is a scaffold, not accepted completion until Ubuntu/macOS/Windows pass.

### 7.57 Enforce the cross-layer remote apply operation bound (2026-11-09)

- [x C] `RemoteBatchCommit` now enforces `MaximumOperationsPerBatch = 4096` in its shared write-copy boundary, before allocating copied arrays. Both `Writes` and `RelayWrites` are independently bounded; relay subset validation remains unchanged.
- [x C] This makes the FLD durable ingress honor the same 4,096-operation bound used by the BIN-SYNC relay adapter instead of accepting an unbounded caller-supplied `IReadOnlyList`.
- [~ T] Add exact boundary tests for 4,096 accepted and 4,097 rejected for both Writes and RelayWrites, including an oversized lazy/custom IReadOnlyList to prove rejection precedes element access/copy.

### 7.58 Bound FLD relay read pages by total operations, not only batch count (2026-11-09)

- [x C] `ReadRelayBatchPage` now caps the aggregate matching relay operations at `RemoteBatchCommit.MaximumOperationsPerBatch` (4,096), while preserving complete JournalSeq batch boundaries.
- [x C] When the next whole journal batch would exceed the page operation budget, the cursor is deliberately left before that batch and the page returns immediately. The caller can resume from the returned cursor without splitting a JournalSeq or skipping data.
- [x C] Nonmatching and already-acknowledged journal rows may still advance the scan cursor; only an included complete relay batch advances beyond its own JournalSeq.
- [~ T] Add a test with multiple source batches whose aggregate exceeds 4,096 and prove the second page starts at the unconsumed batch, with no gap/duplicate and no split.

### 7.59 CI diagnostics retained for the two-DLL migration (2026-11-09)

- [x C] FLD CI now tees restore/build/test output into per-OS log files and uploads them even when a job fails. This addresses the current connector limitation where job-log retrieval returns BlobNotFound and lets the migration be debugged from artifacts without guessing.
- [~ T] Wait for the new exact-head run and inspect the diagnostic artifacts; do not continue changing the project graph blind while all three OS jobs are red.

### 7.60 Physical Database source consolidation — no linked-source shortcut (2026-11-09)

- [x C] Replaced the linked-source scaffold with a physical move of all 40 Core/Engine/Manifest/Run/WAL source files into `src/Fld1.Database/{Core,Engine,Manifest,Run,Wal}/...`, preserving namespaces and internal folder structure.
- [x C] Removed the five old component `.csproj` entry points; the solution and all tests/benchmarks already target `Fld1.Database` plus `Fld1.Storage`. No Core/Engine/Manifest/Run/WAL runtime DLLs are built from the solution.
- [x C] `Fld1.Database.csproj` now uses normal SDK compile discovery over physically owned source; no linked Compile items. It references only generic `Fld1.Storage` and required hashing packages.
- [~ T] Exact-head restore/build/test remains red/unverified; validate project references, package output and all three OS CI jobs after this physical move.
- [~ C] Finish public package layout and architecture-doc/API migration; confirm Storage-only consumers can load `Fld1.Storage.dll` without loading `Fld1.Database.dll`.

### 7.61 Corrected extraction status: physical source move vs generic engine reuse (2026-11-09)

- [x C] The prior linked-source scaffold was superseded by commit `d63a8c49a889c28838305ce15aa4ca5e0b189201`: all 40 source files were moved into `src/Fld1.Database`; five component project files were removed; the project now uses default compile discovery over physically owned files.
- [x C] Tests/benchmarks and `Fld.Db.slnx` now target the two runtime projects. README and this roadmap have been corrected to describe physical ownership rather than linked sources.
- [~ T] Current branch HEAD `e4a8cfcabea489b0f337839a759de654002f696d` still has failed Ubuntu/macOS/Windows CI. The attempted log-artifact instrumentation also produced no retrievable artifacts; do not infer the cause.
- [~ C] Important distinction: this closes the physical project-consolidation step, not the full Storage engine extraction. `Fld1.Database` still contains the field-aware legacy WAL/Run/Manifest path and `FldDatabase` is not yet backed by `StorageDatabase`. The generic Storage API/backend migration and physical-format compatibility plan remain open.

### 7.62 Recovery point after physical consolidation (2026-11-09)

- [x C] Created `backup/post-two-dll-consolidation-2026-11-09` at `7105cdcd0c6e709681389b18adc066f589744d59` before continuing with the generic Storage backend migration.
- [x C] The earlier pre-work checkpoint `backup/cpass-2026-11-09-0915` at `94fd1ebdfa15b95b55184a8eee92a5ebea0a4d5e` remains intact.
- [~ T] The active branch still has red cross-platform CI; no new backend migration should be merged without restoring actionable diagnostics and a passing baseline.

### 7.63 Avoid materializing a relay batch that cannot fit the page budget (2026-11-09)

- [x C] `ReadRelayBatchPage` now counts author-matching relay operations before allocating the `RemoteFieldWrite[]`. If the next whole JournalSeq group would exceed the remaining 4,096-operation budget, it returns with the cursor still before that group and performs no projection allocation for it.
- [x C] For accepted groups, the code fills one exact-size array in a direct loop instead of LINQ `Where/Select/ToArray`; semantics and whole-group ordering are unchanged.
- [~ O] Allocation/throughput benefit is structurally clear for a page that reaches its cap, but no benchmark claim is made until the relay paging benchmark is run on a stable green build.
- [~ T] Add boundary/resume tests for a page that is nearly full and a following group that does not fit; verify cursor remains before that group.
- [~ T] Exact current HEAD `4c1393a08f51b9bab751d870a2a5282e155c6c7d` has failed PR/push CI on Ubuntu, macOS and Windows. The connector still does not return job logs/artifacts for this private-repo run; no root cause is asserted. This code change is therefore build/test unverified.


### 7.64 Recovery point after bounded relay-page projection (2026-11-09)

- [x C] Created `backup/after-relay-page-bounds-2026-11-09` at `98cf8dbf7f696b63e960a379be12fd77ef817dea` before proceeding to the next cross-store integration slice.
- [x C] Retained earlier recovery points `backup/cpass-2026-11-09-0915` and `backup/post-two-dll-consolidation-2026-11-09`.
- [~ T] Current CI remains red on all three OS jobs, with diagnostic artifacts unavailable; keep the new relay paging changes provisional until exact-head validation.

### 7.65 Make diagnostic CI shell portable across Windows and Unix (2026-11-09)

- [x C] Set explicit `shell: bash` on restore/build/test diagnostic steps. The prior `mkdir -p`/`tee` instrumentation implicitly used PowerShell on Windows and was not portable; this could fail before producing logs.
- [~ T] The new three-OS run on `71dfbf2b1238d5cfd78d84755cc366c04b1a0c2d` still concluded failure, but GitHub's job API returned `logs_url: null`, `steps: null`, and no artifacts for all three jobs. This indicates no retrievable runner diagnostics, not a verified source/compiler failure. Keep the build status unverified and stop changing code based on guesses.

### 7.66 BIN-SYNC relay coordinator and host reference-scan adapters landed (2026-11-09)

- [x C] BIN-SYNC `feature/fld-external-storage` now exposes `ReadAndAppendRelayProvenancePage` and `AcknowledgeRelayProvenance` on `StorageSyncCoordinator`. The former composes the host FLD commit guard, reads a complete source page, resolves each transient ChangeId through the retained Node Outbox and atomically commits RPM3 sequence mappings plus the JournalSeq scan cursor before publication. The latter computes the per-author downstream-safe Outbox watermark, translates it to a whole FLD JournalSeq, invokes the durable FLD relay ACK callback, and only then prunes mapping rows.
- [x C] BIN-SYNC now provides `StorageRelayPublicationGate` to serialize page publication, outbound scheduling and safe-ACK work, plus `DelegateApplicationGcReferenceScanner` / `DelegateApplicationGcReferenceScan` to adapt one host-owned immutable reference snapshot while retaining the FLD commit lease.
- [~ C] The host application still must compose these APIs with `FldDatabase.AcquireCommitGuard`, `ReadRelayBatchPage`, `AcknowledgeRelayBatches`, and `OpenReferenceSnapshot`; flatten relay writes preserving whole JournalSeq boundaries; route every namespace sender through the same gate; and reconcile RPM3/FLD/Node cursors before enabling send after restart. No FLD→BIN-SYNC dependency or third FLD runtime DLL is allowed.
- [~ T] BIN-SYNC code after tested baseline `4382c49bffe1608ebabc4a483771b83242cbc81d` (publication gate + delegate scan adapter) is not yet exact-head CI validated. On the baseline, both BIN-SYNC builds passed; Core was 672/678, Storage Integration 91/171, and Fuzz/Lab passed. The failures are stale fixture/provider-contract migrations, not grounds to weaken runtime checks.
- [~ C] FLD's physical `Fld1.Database` project consolidation is complete as a source move, but generic `Fld1.Storage` backend reuse, frozen FLD1 physical-format compatibility decision, public/package audit and three-OS green CI remain open. Do not describe the two-project layout as a completed backend migration.

### 7.67 BIN-SYNC closes the apply→publish gate contract (2026-11-09)

- [x C] BIN-SYNC `StorageRelayPublicationGate.ApplyAndPublishPageAsync` now serializes durable remote APPLY, FLD relay-page read, ChangeId→OutboxSeq resolution, RPM3 mapping/cursor commit and downstream sender callback under one per-namespace async gate. `PublishPageAsync` is the startup/recovery variant for already durable pages; `AcknowledgeAsync` shares the gate and performs FLD durable ACK before map pruning.
- [x C] `DelegateApplicationGcReferenceScanner` / `DelegateApplicationGcReferenceScan` now provide a domain-neutral host bridge: the FLD host opens `FldDatabase.AcquireCommitGuard()` then exactly one `FldDatabase.OpenReferenceSnapshot()`, maps its pre-indexed predicates into BIN-SYNC delegates, and transfers ownership of both resources to the scan wrapper.
- [~ C] The remaining host work is concrete application wiring: one gate per namespace shared by every receive/send/ACK path; startup reconciliation of FLD journal, RPM3 cursor/mappings and Node Outbox before enabling send; callbacks flattening whole `RelayJournalBatch` groups; reference-snapshot delegate mapping; and failure/retry policy. These callbacks belong to the host integration layer, not either FLD runtime DLL.
- [~ T] The latest BIN-SYNC code after `4382c49bffe1608ebabc4a483771b83242cbc81d` is not yet CI validated. That baseline compiled on all three OS jobs, but Core had 6 stale-fixture failures, Storage Integration 80 stale provider-contract failures and Fuzz/Lab passed.
- [~ C] FLD generic Storage backend migration and three-OS CI repair remain separate open items; physical source consolidation into `Fld1.Database` does not mean `FldDatabase` uses `StorageDatabase` yet.

### 7.68 Startup recovery now validates the RPM3 source cursor (2026-11-09)

- [x C] BIN-SYNC added `ValidateRelayProvenanceRecovery` and `RecoverAndPublishPageAsync`. Before a namespace sender is opened, the host supplies `FldDatabase.GetRelayJournalHighWatermark()`; BIN-SYNC verifies that persisted RPM3 scan/ACK JournalSeq cursors do not exceed the durable FLD ApplyJournal high watermark.
- [x C] The startup path then reads the next complete `ReadRelayBatchPage` page, resolves ChangeIds through the retained Node Outbox, persists the sequence mapping and cursor, and only then invokes the sender callback. A replaced/shortened source journal fails closed.
- [~ C] Host composition remains required: keep every other sender disabled until this method succeeds; provide the real FLD guard/read/high-watermark/ACK callbacks; and use one publication gate instance across remote apply, sends and ACK.
- [~ T] Startup regression/replacement and crash/replay tests remain open. BIN-SYNC code after the last tested baseline `4382c49bffe1608ebabc4a483771b83242cbc81d` has not yet passed exact-head CI.
- [~ C] Generic Storage backend reuse and FLD physical-format/public-package audit remain open independently of the relay adapter.

### 7.69 Idempotent retry of signed frontier transport (2026-11-09)

- [x C] BIN-SYNC now exposes `SendAcceptedAppliedOriginFrontierAsync`: after a transport failure, the host can resend the latest durable local OFR2 report without minting a new report sequence or changing signed history.
- [x C] The method re-verifies the persisted report against the accepted roster and checks that the supplied member seed reproduces its stored signature before the session sends it.
- [~ C] Host scheduler should use the accepted-report resend path for retries; it must not call `PublishAndSendAppliedOriginFrontierAsync` merely to retry a failed send, because that intentionally creates a new report sequence.
- [~ T] Lost-send/retry, wrong-key, stale-roster and member-epoch rollover cases remain T-pass coverage.

### 7.70 Enforce one shared relay gate per BIN-SYNC coordinator (2026-11-09)

- [x C] BIN-SYNC now exposes `StorageRelayPublicationGate.For(coordinator)` backed by a `ConditionalWeakTable`; the constructor is private, so all host components using the supported factory share the same async publication/ACK gate for one coordinator namespace.
- [~ C] The host must keep one `StorageSyncCoordinator` instance per Node/provider/namespace and must not bypass the gate with direct sender calls. This is an application composition rule, not something FLD can enforce without taking a BIN-SYNC dependency.
- [~ T] Concurrency, cancellation and failure-release behavior remain T-pass tests.

### 7.71 RPM3 resolver follows the active BIN-SYNC publication owner (2026-11-09)

- [x C] BIN-SYNC relay mapping now resolves ChangeIds through Node Outbox before metadata-owned publication cutover, and through `StorageMetadataStore.TryResolveRelaySequences` after cutover. This is required because the cutover intentionally drains/fences Node Outbox while the durable Metadata Store assigns the relay author-stream sequence.
- [x C] Safe relay ACK translation now reads `Node.GetSafeOutboxAckedThrough` before cutover and `StorageMetadataStore.GetSafePeerStreamCompactionCursor` after cutover. The already committed RPM3 safe watermark remains a monotonic lower bound across peer retirement/rejoin/baseline transitions.
- [x C] The Metadata Store resolver reads existing immutable relay rows by ChangeId and returns their sequence; it creates no second ChangeId ledger or payload copy. Missing/compacted rows fail closed.
- [~ T] Verify both modes, cutover, retained/missing relay rows, peer baseline rollover and map reopen/recovery. No exact-head CI yet for the new implementation.
- [~ C] Host integration still must select and keep the matching coordinator/provider publication mode stable, and serialize all receive/send/ACK paths through the shared gate.

### 7.72 Drain all FLD relay pages before downstream publication (2026-11-09)

- [x C] BIN-SYNC publication gate now drains all FLD relay pages through the durable `GetRelayJournalHighWatermark()` before allowing any sender callback. It loops across the 4,096-operation aggregate page cap and commits each page's sequence mappings/cursor before proceeding.
- [x C] The read callback receives the prior RPM3 cursor and returns the page's scanned cursor plus source high watermark from the same FLD commit-guarded read. Non-progress with source history remaining, cursor regression or cursor beyond source high fails closed.
- [x C] The gate starts closed on process startup and remains closed after unresolved ChangeId, uncertain APPLY outcome, or recovery validation failure. Only successful full `RecoverAndPublishAsync` reconciliation opens outbound scheduling.
- [~ T] Multi-page resume, no-unmapped-send, failure/recovery, empty-author pages and operation-cap boundaries remain T-pass requirements.
- [~ C] Host must use one gate for all remote apply and sender paths, provide the cursor-aware page callback and avoid direct session sends outside the gate.

### 7.73 Recovery branches before the next host-integration slice (2026-11-09)

- [x C] Created `backup/cpass-2026-11-09-relay-gate-metadata-mode` at `6af05cd007baa3bd246450b986598e54fd9eaa57`, preserving the FLD branch and its cross-repository relay gate contract.
- [x C] Retained `backup/post-two-dll-consolidation-2026-11-09` and `backup/cpass-2026-11-09-0915`.
- [~ T] This backup captures the current red/unverified CI state; it is a recovery point, not a green baseline.

### 7.74 Enumerate every pending relay author for namespace-wide startup recovery (2026-11-09)

- [x C] FLD now exposes `FldDatabase.ReadUnacknowledgedRelayAuthors()`. It scans the ApplyJournal under the FLD commit lock, returns the distinct remote author identities with at least one unacknowledged RelayFields operation, and persists no duplicate author index.
- [x C] BIN-SYNC now exposes `StorageRelayProvenanceMap.ReadAuthors(namespaceId)` to enumerate durable RPM3 cursor authors, including the FLD-ACK-before-map-prune crash window.
- [x C] `StorageRelayPublicationGate.ApplyAndPublishAllAsync` and `RecoverAndPublishAllAsync` now discover the author union after durable APPLY / during startup, drain every page for every author to the source high watermark, and only then run the namespace-wide sender callback. A single-author scan can no longer accidentally authorize a sender that chooses another author stream.
- [~ T] Test author discovery after reopen, map-only author recovery after FLD ACK crash, multiple authors with >4,096 operations, empty author pages, and no sender callback until every author is fully mapped.
- [~ C] Host supplies `fld.ReadUnacknowledgedRelayAuthors()` as `readRelayAuthors`; the BIN-SYNC gate itself unions this with `map.ReadAuthors(namespaceId)`, including map-only authors after an FLD-ACK-before-prune crash. Host still converts 16-byte identities canonically and routes all sender paths through the shared gate.

### 7.75 Cache pending relay-author discovery without a durable duplicate index (2026-11-09)

- [x C] `FldDatabase.ReadUnacknowledgedRelayAuthors()` now builds its author→maximum-unacknowledged-JournalSeq set once from ApplyJournal, incrementally adds authors after each durable remote APPLY, and removes an author when a durable ACK covers its last unacknowledged JournalSeq.
- [x C] The cache is process-local/rebuildable; it adds no persistent author-index rows and is reconstructed after reopen from the existing ApplyJournal/ACK sidecar. This avoids rescanning the full journal on every remote batch.
- [~ T] Test reopen rebuild, apply append, partial ACK retention, final ACK removal, ACK then new batch re-add, and unsealed-WAL recovery before author enumeration.

### 7.76 All-author relay ACK and bounded discovery cache (2026-11-09)

- [x C] FLD pending relay-author discovery is now a rebuildable process-local cache: it is populated from the ApplyJournal once, updated after durable remote APPLY, and pruned only when the durable per-author ACK covers the last pending relay JournalSeq. No duplicate persistent index is written.
- [x C] BIN-SYNC `StorageRelayPublicationGate` unions FLD-discovered authors with RPM3 cursor authors itself under the composed commit guard; the host only supplies the FLD author callback.
- [x C] BIN-SYNC `AcknowledgeAllAsync` applies safe ACK per author in order: compute per-author downstream-safe sequence, invoke idempotent FLD `AcknowledgeRelayBatches`, then prune RPM3. Partial failure can be retried using remaining map cursors.
- [~ T] Verify author cache across restart, partial ACK, new apply after ACK, FLD-ACK-before-map-prune crash, multi-author partial failure and retry.
- [~ C] Host still must provide the real callbacks and use one shared coordinator/gate for the namespace; no BIN-SYNC dependency is added to FLD.

### 7.77 Exact-head CI triage after two-project FLD consolidation (2026-11-09)

- [x C] Confirmed branch HEAD `df44f3530d1581ea3b6edc8133c762d1e49850ed` is a documentation-only refresh after the physical `Fld1.Database` source consolidation and relay paging code.
- [x C] The latest `.NET CI` run (`42561687647`) reports all three OS jobs failed, but GitHub's Jobs API shows `runner_id=0`, empty `steps`, identical ~2-second start/completion windows, no uploaded artifacts, and no retrievable logs. This is an infrastructure/dispatch-level failure signal, not evidence of a C# compiler or test failure. Do not diagnose source code from it.
- [~ T] Need a run that actually allocates runners and executes Restore/Build/Test on Windows, Ubuntu and macOS. Existing diagnostic workflow already uses explicit Bash and uploads logs; no diagnostic artifacts were produced in the failed run.
- [~ C] Physical source move into `Fld1.Database` is complete, but generic `Fld1.Storage` backend reuse is not yet established: `Fld1.Database.csproj` references `Fld1.Storage`, while the database's core storage implementation still needs an audited migration/adapter. Preserve FLD1 file-format compatibility; do not replace the database storage engine with generic key-value storage by assumption.
- [~ C] Cross-repository host adapter remains in the host integration layer: compose FLD guard/reference snapshot with BIN-SYNC relay gate, startup all-author reconciliation, publication, safe ACK, FLD durable ACK, then RPM3 prune.
- [~ T] Verify project references and all tests after a runner-backed build; do not mark two-DLL extraction validated merely because the solution lists two runtime source projects.


### 7.78 Progress estimate and remaining FLD gates (2026-11-09)

Estimated completion (engineering assessment, not CI pass percentage):

- Physical two-project source split (`Fld1.Database` + `Fld1.Storage`): **~90%** at source/project-layout level; runner-backed build and public/package audit still open.
- Generic Storage backend/API integration and FLD1 compatibility audit: **~55%**; project reference exists, but the database engine's WAL/Run/Manifest path still needs explicit architecture/conformance review before claiming reuse.
- FLD relay paging, ACK and reference-snapshot primitives: **~85%**; host composition with BIN-SYNC remains open.
- Three-OS build/test acceptance: **~30%**; the latest workflows fail before runner steps (`steps=null`, no artifacts/logs), so no source-level conclusion can be drawn.

Overall FLD integration contribution is estimated at **~75–80%**. Next C work: finish the storage/database contract audit and host binding; next T work must obtain a runner-backed three-OS execution and repair actual compiler/test failures if any. Preserve FLD1 persisted-format compatibility. No physical GC authorization until the composed host scan proves receipts, ChangeId and tombstone horizons.

### 7.79 Audit correction: project reference is not generic backend reuse (2026-11-09)

- [x C] Inspected the consolidated `FldDatabase` public path: it still owns `_overlay`, `_dirty`, `WalManager`, `OutboxLog`, `ApplyJournal`, `AtomicManifestStore`, and Run readers/builders directly. `FldDatabase.cs` does not consume `StorageDatabase` APIs.
- [x C] `Fld1.Database.csproj` references `Fld1.Storage`, but the reference alone does not prove that the database is backed by the generic Storage engine. The two-project assembly split is real; backend reuse/overlay composition is not yet complete.
- [~ C] Do not replace FLD WAL/Run/Manifest persistence with `StorageDatabase` without a separate format/recovery ADR and compatibility proof. Preserve FLD1 on-disk bytes, WAL recovery semantics and compaction behavior. The remaining task is to define and implement the intended reusable storage contract without duplicating WAL/Outbox metadata or introducing a third FLD runtime assembly.

### 7.80 Host composition contract now has a concrete cross-repository guide (2026-11-09)

- [x C] BIN-SYNC `feature/fld-external-storage` added `docs/FLD-BINSYNC-HOST-COMPOSITION.md`, binding the real FLD public methods to BIN-SYNC relay gate and GC delegate APIs.
- [x C] The guide explicitly preserves dependency direction: FLD runtime has no BIN-SYNC dependency; the application host composes the two assemblies. It documents whole-JournalSeq projection, shared gate, all-author recovery, durable FLD ACK before RPM3 prune, and reference snapshot ownership.
- [~ C] Neither repository currently contains the user's actual host executable. Therefore the guide is not a substitute for compiling the callback binding in that host; no third FLD runtime assembly is introduced.
- [~ T] Host-level crash/reopen and concurrency tests remain required before the adapter or GC can be accepted.


### 7.74 Reuse canonical relay-author index keys during discovery (2026-11-09)

- [x C] `FldDatabase.ReadUnacknowledgedRelayAuthors` now orders the existing dictionary entries by their canonical hex keys instead of recomputing `Convert.ToHexString` for every author on every discovery call. This removes one temporary string allocation per author while preserving the exact ordinal byte-identity ordering and returned API shape.
- [~ T] Validate deterministic ordering and returned author identities in the pending T-pass; this is a source-level allocation reduction, not a benchmark claim.
- [~ O] Include startup/recovery author-discovery allocation and CPU in the representative multi-author measurement.

Code commit: `bf72f0d8c4e82303612f079e3aefa790d6794451`. Exact-head CI remains unverified; no test result is inferred from this change.


### 7.75 Per-author relay publication ceiling contract (2026-11-09)

- [x C] Updated the BIN-SYNC integration contract so every sender callback receives the durable per-author `StorageRelayAuthorPublicationWatermark` and sends only listed authors through `PublishedThroughSequence`. New local FLD commits made after the last mapping page remain unpublished until the next reconciliation pass.
- [x C] The host's per-page FLD commit lease is released before network I/O; the explicit durable sequence ceiling, not a long-held database lock, prevents unmapped Outbox rows from escaping during that gap.
- [~ T] Migrate the actual host sender callbacks and add a concurrent local-write race case between final mapping commit and sender invocation. This is a BIN-SYNC host API migration and remains unvalidated.
- [~ T] Verify both Node-Outbox and Metadata-Store-owned publication modes use the same ceiling semantics.

BIN-SYNC implementation commits: `9d33cddbbcf2af5281fe580d6fdd5885f3efe9b1`, `60489796218cdd39a84f8cbc93ce4f53be90dfd3`, `97609fb3c91e15aac27ff8d2eccf576381993c1c`; contract commit `87a8b7a0d364f80982696f1b748294bf042424a4`.


### 7.76 Recovery checkpoint after publication-ceiling contract (2026-11-09)

- [x C] Created FLD-DB `backup/cpass-2026-11-09-publication-ceilings` at `01617ecaa06fef4bd4ec16e913f24ffd449158ae`.
- [x C] Matching BIN-SYNC backup branch is `backup/cpass-2026-11-09-publication-ceilings` at `701077ed9a13ce476aaa1e679d9c17f16f0ced5f`.
- [~ T] This is a recovery point for the unvalidated C-pass/API migration state, not a passing test checkpoint.


### 7.77 BIN-SYNC map ownership validation before host publication (2026-11-09)

- [x C] BIN-SYNC now validates that the RPM3 map belongs to the coordinator's metadata scope, local replica and exact durable StorageDatabase before every gate operation, including startup recovery and outbound-only scheduling.
- [x C] Recovery checkpoint: `smersh1307n2/BIN-SYNC:backup/cpass-2026-11-09-publication-ceilings-v2` at `0f46e444529b77997ce1ba190838de3c4f4ecb6d`.
- [~ T] Host callback/API migration and negative map-binding cases remain deferred to the T-pass. No FLD runtime dependency on BIN-SYNC is introduced.


### 7.78 BIN-SYNC RPM3 author enumeration allocation reduction (2026-11-09)

- [x C] BIN-SYNC introduced a shared allocation-free `NodeIdCanonicalComparer` and uses it for both relay gate author ordering and RPM3 cursor author enumeration. The FLD runtime is unchanged and remains free of BIN-SYNC references.
- [x C] BIN-SYNC recovery branch: `backup/cpass-2026-11-09-nodeid-ordering` at `2c7007f059c2da1878c24e64a76dec902f9e8c73`.
- [~ T] Canonical-order equivalence, RPM3 reopen and malformed/duplicate cursor cases remain untested until T-pass.


### 7.79 Storage source parity and bounded visitor/ACK paths (2026-11-09)

- [x C] FLD's authoritative `src/Fld1.Storage/Engine/StorageDatabase.cs` and BIN-SYNC's `external/Fld1.Storage/Engine/StorageDatabase.cs` now match byte-for-byte (blob `fa31c0e6395ad6bc999bf4a3fee8570b6acef20a`). The source includes immutable-value snapshot sharing, sorted prefix cursors and borrowed `StorageEntryView`/`VisitPage` scans.
- [x C] BIN-SYNC metadata partition reopen now consumes 512-row pages and builds its persisted-entry index during decode instead of holding an unbounded full-partition row list.
- [x C] Provider feed reads use borrowed views; local ACK cleanup is bounded to 4,096 deletes per transaction with the ACK watermark committed atomically with each page. Restart can resume from the last committed ACK page without skipping data.
- [~ T] Verify FLD/BIN source parity, visitor lifetime, cursor ordering, ACK page crash cuts, and large-feed recovery on all supported OSes.
- [~ O] Measure peak memory and throughput; snapshot sorted-index construction remains O(number of keys), and no O(page)-total-memory claim is made.


### 7.80 Single-entry borrowed Storage cursor and BIN-SYNC provider use (2026-11-09)

- [x C] `StorageSnapshot.PrefixCursor.TryReadNext` returns a borrowed `StorageEntryView` for one entry without allocating a page or copying key/value bytes. `VisitPage` delegates to the same cursor primitive.
- [x C] FLD authoritative Storage source and BIN-SYNC vendor are byte-identical at blob `a28cd58fbb72a1453a3ffb04a1ab1d5ed43ddaa1`.
- [x C] BIN-SYNC provider snapshot merge consumes field and record-tombstone streams one row at a time, retaining only the current decoded mutation from each stream.
- [~ T] Cross-check stream merge order, reset/reopen, disposed cursor and concurrent immutable snapshot behavior; no test run is claimed.


### 7.81 FLD Database uses the generic Storage writer-fence seam (2026-11-09)

- [x C] `Fld1.Storage` exposes `StorageDatabase.AcquirePartitionLease` / `StoragePartitionLease` backed by the same `WalWriterFence` as generic Storage, but without creating or opening a generic Storage WAL.
- [x C] `FldDatabase.Open` acquires a root-wide reserved partition lease before reading/updating Manifest or replaying the FLD-specific WAL, Outbox, ApplyJournal and Runs. The lease is released after file handles on dispose and on every failed open path.
- [x C] No FLD1 WAL/Run/Manifest bytes or record semantics were replaced. FLD remains protocol-neutral; exactly two runtime DLLs.
- [~ T] Add FLD Database multi-process/namespace lock tests, open-failure cleanup and abandoned-owner recovery. Exact-head build/CI not yet run.
- [~ C] This does not yet migrate FLD-specific WAL/Run/Manifest persistence onto generic `StorageDatabase`; that requires a separate on-disk format/recovery compatibility ADR and remains open.


### 7.82 Cache immutable snapshot index within each commit epoch (2026-11-09)

- [x C] FLD `StorageDatabase` now caches the immutable sorted snapshot index and shares it across snapshots until a successful commit. A commit invalidates only the live cache; snapshots already opened retain their previous index/value generation.
- [x C] BIN-SYNC's vendored `StorageDatabase.cs` mirrors the exact FLD source (blob `d5fee8be06b9193a294ce4f7c37ba84b5f37da15`).
- [~ T] Snapshot stability across update/delete/checkpoint/reopen and cache invalidation boundary tests remain pending.
- [~ O] First snapshot after a commit epoch still builds an O(keys) sorted index; repeated snapshots within the epoch reuse it.


### 7.83 Canonical Storage project injection for cross-repository host integration (2026-11-09)

- [x C] `Fld1.Database.csproj` now accepts the `Fld1StorageProject` MSBuild property; its default remains the adjacent canonical `src/Fld1.Storage/Fld1.Storage.csproj`, so normal FLD builds are unchanged.
- [x C] The optional BIN-SYNC `samples/FldBinSyncHost` sets the same property for both `BinSync.Storage` and `Fld1.Database`, ensuring a single canonical Storage project/assembly in the cross-repository host build.
- [x C] Corrected the XML documentation ownership for `AcquireReferenceSnapshotGuard` and `OpenReferenceSnapshot`; no runtime behavior or FLD1 disk format changed.
- [x C] Created `backup/cpass-2026-11-09-host-adapter-prep` at `cf4c454d539f96766ef66febb3d117c9a7c5965b` before the optional host integration layer.
- [~ T] Build the cross-repository sample with explicit paths and validate project de-duplication; run normal FLD three-OS CI and test the snapshot-guard release path. No test/build pass is claimed for this C-pass.
- [~ C] FLD Database remains protocol-neutral; its WAL/Run/Manifest/ApplyJournal format is not migrated to generic Storage. The two FLD runtime DLLs remain exactly `Fld1.Storage.dll` and `Fld1.Database.dll`.


### 7.84 BIN-SYNC publication ceilings preserve both sides of the FLD cutover (2026-11-09)

- [x C] The optional BIN-SYNC host adapter now freezes FLD local provider changes before each publication-gate entry point, then captures a complete local+relay author watermark set under the composed FLD/Node/Storage commit guard.
- [x C] The local ceiling is sourced from Node Outbox before provider-authoritative cutover and from Metadata Store after cutover. RPM3 relay ceilings retain their active OutboxSeq/Metadata-relay sequence authority.
- [x C] BIN-SYNC Core has ceiling-aware sender overloads for both Node-owned and Metadata Store-owned publication. The mode bit is captured with the ceilings, preventing a concurrent cutover from mixing sequence domains.
- [~ T] Exact-head build and both-mode tests are pending; no change to FLD field WAL/Run/Manifest bytes was made.
- [~ C] Generic ordered byte-KV page/run backend and FLD1 compatibility strategy remain a separate architecture decision under `FLD-STORAGE-CORE-ARCHITECTURE.md` §7. The assembly split alone does not authorize a physical-format migration.


### 7.85 Cross-repository host adapter build gate (2026-11-09)

- [x C] BIN-SYNC CI now checks out `fix/crash-consistency-outbox` as a sibling dependency and builds `samples/FldBinSyncHost` against the canonical FLD `Fld1.Database` and `Fld1.Storage` projects.
- [~ T] The first exact-head build result is pending; this is a compile gate only and does not yet prove the full `FldDatabase`→`IDataProvider` snapshot/local-feed/remote-apply contract.


### 7.87 Host contextual apply store identity and receive-before-ACK composition (2026-11-09)

- [x C] The BIN-SYNC host sample now validates that `FldRemoteBatchApplyStore` and `StorageSyncCoordinator` reference the exact same FLD database/capability object before accepting any calls.
- [x C] The concrete receive helper routes accepted batches through `ReceiveBatchBeforeAckAsync` and then the relay publication gate; this preserves full sender/issuer/cursor/digest context in FLD ApplyJournal before the source ACK.
- [~ C] FLD still does not implement `IDataProvider`: generation-pinned full snapshot, Outbox→provider feed/author epoch, key resolver and `TryReadCurrent` mapping remain open. No third FLD runtime DLL and no FLD→BIN-SYNC dependency are introduced.
- [~ T] Cross-repository host build and crash/reopen tests are pending; last CI host build skipped private FLD checkout without credential.


### 7.88 Metadata Store owns v3 relay payload after provider-authoritative cutover (2026-11-09)

- [x C] BIN-SYNC now has a direct metadata-owned relay path. Its contextual FLD `RemoteBatchCommit` intentionally carries no duplicate `RelayWrites`; the Metadata Store finalizes relay-eligible operations into its single durable relay stream/author sequence.
- [x C] The host no longer treats FLD `ReadRelayBatchPage`/`AcknowledgeRelayBatches` or RPM3 as the post-cutover relay authority. Per-peer ACK stays in BIN-SYNC Metadata Store; relay payload compacts through the minimum durable peer ACK. The legacy FLD JournalSeq/RPM3 path remains only before provider cutover.
- [x C] Host startup now recovers PendingApply and freezes the local provider feed under the composed FLD/Node/Storage guard before enabling Metadata Store-owned publication.
- [~ T] Verify ACK aggregation, restart after ACK-before-compaction, duplicate ChangeId suppression and send retry on exact-head BIN-SYNC + FLD integration builds. No new FLD database disk-format change was made by this slice.
- [~ C] FLD's generation-pinned provider snapshot, stable current-state identity/origin mapping and the remainder of the `IDataProvider` bridge are still open. Do not reintroduce a second FLD relay-payload journal to close that gap.
