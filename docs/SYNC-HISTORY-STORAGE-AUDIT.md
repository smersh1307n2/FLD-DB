# Audit: compact change history and multi-peer synchronization storage

**Date:** 2026-11-07  
**Status:** code-backed architecture audit; optimization proposals are not implementation decisions unless explicitly marked otherwise.  
**Scope:** FLD-DB local Outbox and remote APPLY journal; BIN-SYNC shared Outbox, peer/author cursors, replay caches, cross-peer deduplication, receipts and relay retention.  
**No wire-format change is authorized by this document.**

## 1. Executive conclusion

The idea “one logical change, many synchronization destinations” is already substantially implemented in BIN-SYNC:

- The operation is stored once in the namespace Outbox.
- The same operation is read for different peers.
- Delivery progress is kept separately in each peer's per-author stream state.
- Compaction waits until every currently interested peer has acknowledged the operation's author-stream position.

Therefore, **do not create one operation/payload row per peer and do not add an ACK bitmap to every operation**. That would turn an approximately O(changes + peers × authors) model into O(changes × peers).

The largest concrete storage amplification found is elsewhere:

1. FLD remote APPLY serializes Fields and the exact subset RelayFields as two payload arrays.
2. The same full APPLY_COMMIT payload is persisted in both WAL and ApplyJournal.
3. ApplyJournal retains all fields although relay enumeration needs only the relay subset and duplicate-batch handling needs a compact receipt.
4. ApplyJournal has no ACK/compaction boundary, retains a per-batch in-memory list and dictionary indefinitely, and pages by scanning the list from its beginning.
5. FLD local commits also persist mutation payloads in WAL and Outbox at the same time. This is correct, but creates a second copy until WAL checkpoint/truncation.

A second important issue is identity rather than payload size: BIN-SYNC's current SeenChanges key is peer-scoped. The target cross-peer dedup ledger described in the metadata documents is not yet represented by the current runtime key. A ChangeId arriving through a different peer can therefore create a second seen fact; the in-memory Outbox queue prevents a duplicate relay enqueue only while the first copy remains queued. A durable global ChangeId ledger is needed to make dedup independent of route and Outbox compaction.

The recommended target is:

- one immutable payload per published/relay-eligible logical operation;
- one global dedup/provenance fact per unique ChangeId in a replica/scope/namespace;
- one compact receipt per accepted batch;
- one progress tuple per (peer, namespace, author stream);
- one replay-cache entry per unacknowledged outgoing batch range, removed after its ACK;
- separate retention lifetimes for payload, batch receipt, dedup identity and peer replay state.

These are distinct logical facts. They may share a physical log or transaction, but must not be forced into one monolithic record whose lifetime is the maximum of all of them.

## 2. Audited baselines

- FLD-DB audited code ref: branch fix/crash-consistency-outbox at b488dafcfda81d6383717ceaf05a3717a3959923. The implementation baseline is 196628a49d6034559a7041237af7feec36fd9b20; the 17 commits between those refs are documentation-only.
- FLD-DB documentation update: this audit was added in commit 87dfc769882390b714776c73659a5235b0927d21, then linked from the Storage Core architecture and kernel specification in documentation-only commits. The source baseline remains unchanged; no source code changed.
- BIN-SYNC audited code ref: branch feature/fld-external-storage at dd2dd969d70b6fd46371679903855a69ae9399c3.
- BIN-SYNC final documentation HEAD after this audit: d868f47953f1d3ef94d590169694e0cb66342e78. Three documentation files were updated; no source code or wire specification changed.
- Audited BIN-SYNC files include Node.DeltaBatch.cs, Node.ApplyBatch.cs, Node.LocalWrites.cs, NodePersistence.cs, Node.BatchCodec.cs, Wal.cs, METADATA-STORAGE-ARCHITECTURE.md and CHANGE-STAGING-AND-RETENTION.md.

Updated companion documents:
- [BIN-SYNC Metadata Storage Architecture](https://github.com/smersh1307n2/BIN-SYNC/blob/feature/fld-external-storage/docs/METADATA-STORAGE-ARCHITECTURE.md)
- [BIN-SYNC Change Staging and Retention](https://github.com/smersh1307n2/BIN-SYNC/blob/feature/fld-external-storage/docs/CHANGE-STAGING-AND-RETENTION.md)
- [BIN-SYNC FLD-DB Integration Contract](https://github.com/smersh1307n2/BIN-SYNC/blob/feature/fld-external-storage/docs/FLD-DB-INTEGRATION-CONTRACT.md)
- [FLD-DB Kernel Specification](https://github.com/smersh1307n2/FLD-DB/blob/fix/crash-consistency-outbox/docs/FLD-KERNEL-SPEC-v1.5.9.md)
- [FLD Storage Core Architecture](https://github.com/smersh1307n2/FLD-DB/blob/fix/crash-consistency-outbox/docs/FLD-STORAGE-CORE-ARCHITECTURE.md)

This was a source/document audit. No code was changed and no tests or benchmarks were run. Any size estimate below is logical format accounting, not a benchmark.

## 3. One operation for many peers: what already exists

### 3.1 Shared Outbox, not per-peer copies

BIN-SYNC keeps one Outbox collection per namespace. An entry carries its author, author-stream sequence, ChangeId, record identity, operation kind and payload. The collection can contain entries from different authors, including local operations and relayed operations.

For each target peer, CreateDeltaBatchInternal / the transport batch builder:

1. reads that peer's PeerState;
2. selects an author stream using that peer's StreamAcked and StreamOffsets;
3. builds a batch for one author stream;
4. records the peer's pushed watermark;
5. reuses the same source Outbox entry for other peers.

The protocol deliberately keeps one author per DELTA_BATCH. This does not mean one Outbox copy per author or per peer: it is a batching/ordering boundary. A physical group may contain many operations, but a wire batch must preserve the one-author stream contract.

### 3.2 Per-peer progress is the right abstraction

The logical progress key is:

(scope, peer, namespace, authorNodeId)

The relevant progress consists of pushed/acked positions and a peer-relative stream offset. This is approximately O(P × A) progress state, where P is the number of peers and A is the number of author streams, not O(P × C) delivery records for C changes.

Keep this separation. Do not put recipient lists, ACK bitsets or a copy of the operation payload inside every change record.

### 3.3 Current compaction cost

CompactOutbox evaluates each Outbox entry against every interested peer and checks that peer's ACK for the entry's author stream. This is correct but can cost approximately O(C × P) per compaction pass.

A semantics-preserving optimization is to calculate, once per author, the minimum safe local ACK across all interested peers:

- a peer with no PeerState for the namespace contributes an ACK of zero / “not acknowledged”;
- convert peer-relative ACK to the local stream position using that peer's StreamOffset;
- retain an entry when its sequence is greater than the minimum safe ACK for its author.

Then scan the Outbox once. Expected work becomes approximately O(P × A + C). The implementation must preserve the current interested-peer set and the “no peers means keep the log” rule. This is a CPU optimization, not a new retention policy.

### 3.4 New-peer semantics must remain explicit

The current compaction rule is based on currently known/interested peers. A peer that joins later is not retroactively a recipient of every operation already compacted. It needs the protocol's initial-state/bootstrap path.

However, a current-state snapshot is not a proof that every historical relay-eligible operation (especially LWW losers) has been delivered to a future peer. The product contract must explicitly say whether future peers need only current replicated state or also historical relay/event provenance. Do not silently promise the latter while compacting the former.

## 4. BIN-SYNC metadata growth and consolidation

### 4.1 Outgoing batch replay cache: concrete unbounded growth

Each PeerState.Ids entry maps (author, cursorStart, cursorEnd) to (BatchId, previousBatchDigest). Its logical key/value content is about 64 bytes before dictionary/object overhead:

- author: 16 bytes;
- cursorStart + cursorEnd: 16 bytes;
- BatchId + previous digest: 32 bytes.

The map is serialized by NodePersistence. Current ProcessAckInternal advances StreamAcked and calls CompactOutbox, but does not prune PeerState.Ids.

**Required optimization:** in the same durable ACK transition, remove replay-cache entries for that peer/namespace/author whose cursorEnd is fully covered by the validated contiguous ACK. Retain entries for unacknowledged ranges. Keep the current per-stream digest-chain checkpoint so the next batch remains deterministic. Do not prune based on a pushed watermark or a non-contiguous ACK.

This is a high-confidence metadata-growth fix and should precede speculative ChangeId redesign.

### 4.2 Author IDs repeated in three progress maps

NodePersistence serializes StreamAcked, StreamPushed and StreamOffsets as three separate maps. Each map repeats the same 16-byte author key when the author exists in all three.

A compact versioned representation can encode one author ID and a presence mask followed by the present values:

authorId | flags | acked? | pushed? | offset?

This preserves sparse maps and their independent semantics while removing up to 32 repeated author-ID bytes per author per peer/namespace. The exact encoding should use bounded varints only if benchmarks justify them. Keep legacy readers for migration.

### 4.3 Inbox and received-stream metadata

The runtime persists an Inbox idempotency key (issuer, namespace, epoch, BatchId), a separate InboxAuthors mapping with the same key plus resolved author, and a per-peer ReceivedStreams mapping that also associates a batch identity with a cursor end.

These facts overlap, but they are not automatically interchangeable. Audit all duplicate-replay and ACK paths before merging them. The target should be one compact durable batch receipt per accepted batch, containing the facts required to validate a retry and reproduce the correct ACK: issuer/namespace/epoch/BatchId, cursor range, resolved author, digest-chain fields, RecordsRoot, ACK outcome and applicable expiry/commit marker. The receipt must not retain the full field payload.

Receipt GC is independent of relay payload GC and ChangeId dedup retention.

### 4.4 SeenChanges is currently peer-scoped

The current runtime uses a key shaped like (senderPeer, namespace, ChangeId). This records that a change was seen through a particular peer, not that the logical change was globally seen by this replica.

The current ApplyBatch path also builds a set of ChangeIds already present in the live Outbox to avoid enqueuing a second relay copy. That protects the currently queued copy, but after Outbox compaction the queue is no longer the durable source of truth. The same ChangeId arriving by another route can create another peer-scoped SeenChanges fact and can be eligible for relay again.

The target contract already described in the Metadata Storage Architecture is the correct direction:

(scope, localReplicaId, namespace, ChangeId) -> canonical operation fingerprint

- The immediate sender is not part of the global operation identity.
- The canonical fingerprint includes every semantic operation field needed to detect ChangeId reuse with different content.
- The same ChangeId and same fingerprint is a duplicate, regardless of route.
- The same ChangeId with a different fingerprint is a protocol conflict.
- Local WAL replay markers remain separate and are reclaimed only after their WAL replay horizon closes.
- Batch receipts remain peer/issuer-specific because each sender needs its own retry/ACK result.

This is one of the most direct ways to stop creating multiple durable “seen” records for the same logical change arriving from different sources.

### 4.5 ChangeId representation remains an open optimization gate

BIN-SYNC currently creates random ChangeIds. Keep the 128-bit ChangeId until a replacement has a complete identity and migration proof.

Two candidates remain:

1. **Content-derived ChangeId:** BLAKE3-128 over a domain-separated canonical operation (including namespace, record, operation kind, permanent author, HLC/key epoch and canonical value/tombstone bytes). The receiver recomputes and validates it. This could remove the separate 128-bit fingerprint from the exact dedup index, reducing its logical per-operation data from about 32 to 16 bytes. It changes ID-generation semantics and needs compatibility, provider-supplied ID, encryption, snapshot and causal-metadata analysis.
2. **Origin sequence:** not the same as current Seq, FLD OutboxSeq or WAL CommitSeq. Current relay nodes allocate their own local author-stream sequence for relayed entries. It is therefore not a stable original-operation identity. A true OriginSeq would need to be assigned by the original author and preserved through every relay, with incarnation/recovery/cloning rules and equivocation handling. It is a protocol extension, not a field rename.

A sequence watermark alone cannot detect that an old sequence was reused with different content. Do not trade away the current same-ID/different-payload conflict rule without an explicit cryptographic commitment or an accepted weakening of the contract.

## 5. FLD-DB local Outbox: verified size opportunities

The current FLD Outbox record is 88 fixed bytes plus a 4-byte record CRC, before value bytes. Its fixed fields include:

- OutboxSeq: 8;
- CommitSeq: 8;
- ChangeId: 16;
- RecordId: 16;
- FieldId + operation/reserved: 4;
- AuthorNodeId: 16;
- HLC wall + logical: 12;
- KeyEpoch/reserved: 4;
- value length: 4.

The Outbox file is namespace-scoped. FLD persists one LocalAuthor for the database, and the FLD Outbox is the local-origin stream; remote relay data is currently in the separate ApplyJournal. Therefore:

1. **AuthorNodeId per row is redundant.** Store/resolve the LocalAuthor once in a durable file/header and validate it on open. This saves 16 bytes per Outbox entry.
2. **OutboxSeq per row is a candidate for ordinal encoding.** Records are append ordered and compaction removes an acknowledged prefix while preserving the remaining sequence values. A versioned segment/header can store a base sequence and derive each entry sequence from its ordinal. Prove this against torn-tail recovery, ACK, compaction, sparse-index rebuild and reopen. Do not reuse or renumber a sequence.
3. **CommitSeq is repeated across fields from one commit.** Encode commit groups with one CommitSeq per group, or use a bounded delta/varint. CommitSeq remains distinct from OutboxSeq.
4. **Value length** may become a bounded varint; measure before adopting.
5. **Keep ChangeId** until the identity contract is changed deliberately. Do not replace it with OutboxSeq.

Removing AuthorNodeId (16), deriving OutboxSeq (8), and moving CommitSeq to a group header/delta (typically saving about 7 bytes per entry) gives a rough fixed-record target near 60–65 bytes before group framing/CRC overhead. This is a design estimate, not a benchmark.

FLD Outbox has one consumer ACK watermark and a fenced consumer lease. That is appropriate if BIN-SYNC owns all peer-level delivery state and only sends FLD a safe aggregate ACK. Do not copy per-peer cursors into FLD merely to duplicate BIN-SYNC's responsibility.

## 6. FLD remote APPLY: payload amplification and current reduction

### 6.1 APPLY_COMMIT v3 stores incoming operations once

RemoteBatchCommit still validates RelayWrites as an exact subset of Writes by ChangeId and byte-identical operation data. ApplyCommitCodec v3 now serializes Fields once and stores the relay subset as canonical varint indexes into that array. Decode reconstructs RelayFields as references to the corresponding Fields entries; it does not allocate a second set of payload arrays.

The codec remains backward-compatible with v2 payloads. It validates unique/in-range relay indexes and validates the old v2 RelayFields array as an exact subset during migration/read.

### 6.2 ApplyJournal now stores receipt metadata plus relay-only payload

ApplyRemoteBatch still flushes the full authoritative APPLY_COMMIT to WAL first. ApplyJournal.AppendIfMissing then canonicalizes the commit into a relay-only form:
- batch identity, cursor range, issuer, digest-chain fields, RecordsRoot, ProtocolStateDelta, AppliedCount and commit sequence metadata remain available for duplicate receipt lookup;
- only RelayFields payloads are materialized into the journal;
- non-relay Fields are not copied into ApplyJournal;
- the relay-only form is encoded once, without a second field array or relay-index list.

This is flushed before ApplyRemoteBatch returns, so a later checkpoint/WAL truncation cannot remove the only durable copy of relay-eligible operations. Journal promotion is idempotent by batch identity; when an existing record is encountered, canonical receipt metadata plus relay payload are compared. WAL recovery rebuilds the compact journal representation from both old v2 and new v3 APPLY_COMMIT payloads. Relay-eligible LWW losers remain retained.

The operation-payload multiplicity is now:
- non-relay operation: one WAL copy until WAL checkpoint/truncation, then no journal copy;
- relay-selected operation: one WAL copy plus one relay-only journal copy until WAL checkpoint/truncation, then one journal copy.

The receipt and relay payload are logically distinct but still physically co-located in the same ApplyJournal record/file. Per-author relay ACK and physical relay-payload reclamation are now implemented: a CRC-protected sidecar persists safe JournalSeq watermarks; reads suppress acknowledged payload immediately; checkpoint compacts the journal only after the corresponding WAL commits are sealed/truncated. Compaction removes only acknowledged authors' relay fields and preserves receipt metadata plus other authors' payload. Receipt GC remains independent and is not implemented.

### 6.3 ApplyJournal index growth remains unbounded; paging scan is fixed

ApplyJournal no longer keeps a full List of every historical IndexEntry. It keeps one sparse JournalSeq anchor per 256 records and uses lower-bound binary search to find the nearest anchor, then scans at most one bounded 256-record stride before returning the requested page. Page selection is O(log(N/256) + 256 + page size), rather than O(history + page size). A regression test verifies that a request after JournalSeq 2 returns only later records after restart.

The BatchKey→offset Dictionary and on-disk receipt history still grow with every accepted batch. Per-author relay ACK and payload compaction now exist, but they do not remove receipt records or bound the BatchKey index. A disk-backed/rebuildable batch index and receipt GC remain open.

`ReadRelayBatchPage` additionally returns `ScannedThroughJournalSeq`, allowing an adapter to persist scan progress across batches with no matching author operations. This is only a read cursor; it must be committed atomically with returned relay operations and must never be treated as downstream-safe ACK.

Receipt lookup and relay payload still share one physical journal file. Their logical lifetimes are now separated: compaction can remove acknowledged author payload fields while preserving the batch receipt and other authors' payload. The current implementation rewrites the whole journal (O(N)); it does not yet reclaim receipt records or independently reclaim segments.

### 6.4 Implemented representation and remaining target

1. **WAL is authoritative during the uncheckpointed interval.** Implemented: one Fields array plus compact relay indexes in APPLY_COMMIT v3.
2. **Batch receipt is metadata-only with respect to operation payload.** Implemented in ApplyJournal: it retains batch identity, cursor range, issuer, digest-chain fields, RecordsRoot, ProtocolStateDelta and applied result, but no non-relay operation payload.
3. **Relay payload is relay-only.** Implemented: ApplyJournal stores only selected operations, including eligible LWW losers. Durable WAL flush precedes journal materialization; promotion is idempotent by batch identity.
4. **Then truncate WAL.** Existing checkpoint/recovery path keeps relay-only materialization durable before WAL truncation; tests cover journal rebuild from WAL.
5. **Reclaim relay payload only after a validated downstream safe watermark.** The FLD half is implemented per original author: `AcknowledgeRelayBatches` persists a CRC-protected per-author JournalSeq watermark; checkpoint/reopen compaction runs only after the matching APPLY WAL prefix is sealed/truncated and removes only covered author fields. Receipt metadata and other authors' payload remain. The cross-system adapter half is **not implemented**: durable source JournalSeq → ChangeId → BIN-SYNC OutboxSeq provenance, minimum peer-ACK translation and the outbound publication gate remain required work under roadmap §7.19 and BIN-SYNC FLD-DB Integration Contract §3.1.1–3.1.2. Existing FLD tests cover sidecar restart, author isolation, duplicate receipts, journal shrink and fail-closed sidecar validation; they do not prove the missing adapter. Reclaim batch receipts according to independent retry/Inbox expiry rules. Keep ChangeId dedup facts for their separate protocol-safe horizon.

The reduction is an internal storage-format change and does not require changing DELTA_BATCH. Relay payload reclamation is implemented, but full-file rewrite cost must be measured. Remaining gates are receipt GC, bounded/rebuildable receipt indexing, segment-level journal compaction and protocol-safe receipt/dedup/tombstone retention.

## 7. Local WAL and Outbox payload duplication

FLD local Commit currently appends and flushes the WAL group commit, then appends each field mutation (including value bytes) to Outbox and flushes Outbox. The same local mutation payload therefore exists in WAL and Outbox until the sealed WAL prefix is truncated. After checkpoint, the Outbox remains as the durable outbound copy until ACK/compaction.

A later optimization candidate is a WAL-backed Outbox reference:

- while the WAL record is retained, Outbox metadata references the immutable WAL record/operation ordinal rather than copying the value bytes;
- the consumer can read the referenced payload for delivery;
- at checkpoint, before WAL truncation, unacknowledged references are atomically promoted to the Outbox payload log;
- already ACKed operations need not be copied;
- recovery must distinguish referenced, promoted and ACKed states.

This can reduce transient duplicate payload storage, but it is more invasive than removing APPLY duplication. It must not be attempted until the simpler Outbox format and APPLY retention work is measured.

## 7.1 Outbox replay dedup key offset correction (2026-11-07)

A restart regression test exposed an incorrect field offset in the Outbox scanner's reconstruction of its in-memory deduplication key. The on-disk fixed record stores ChangeId at bytes 16–31 and RecordId at bytes 32–47; the scanner had read the two 64-bit RecordId halves from offsets 24 and 32. After reopen, replay of an unmaterialized WAL group commit therefore failed to recognize its already-appended Outbox entry and could append a duplicate.

The scanner now reads the RecordId halves from offsets 32 and 40, matching EntryKey.From(OutboxEntry) and the writer layout. This does not change the Outbox v1 bytes or require migration. Regression coverage verifies both several separate commits and a local write followed by a remote APPLY winner across close/reopen; the Outbox must contain exactly the original local operations, with stable sequence order and no replay duplicates.
## 8. Pending local coalescing remains valid, but only before publication

Current FLD _dirty collapses repeated writes to the same field before one Commit. Four Put calls followed by one Commit produce one field mutation in that commit. Four separate Put+Commit cycles produce four Outbox entries.

A durable PendingLocal staging layer can coalesce repeated local LWW writes across commits, but only while they have never been frozen/exposed to any sender. Freeze must atomically assign immutable ChangeId and stream sequence and move the payload into the published journal before transport can send it.

Do not rewrite an unacknowledged Outbox entry. A lost ACK does not prove that the peer did not receive it. Do not coalesce remote relay operations or CRDT/event operations using LWW rules. A pending local operation must retain its own payload even if a later remote APPLY wins the visible LWW state.

## 9. Session alias and author grouping

BIN-SYNC batches one author stream at a time and resolves the target session alias once for that stream, then stamps the same alias into each operation payload. The alias is a session mapping, not permanent operation identity.

Moving the alias from every operation payload into the DELTA_BATCH context is a plausible wire-size optimization: one author per batch means one alias value can serve all entries. It is not an internal-only change: the wire codec, digest/canonicalization rules, spec, compatibility and tests must change together. Keep it as a later protocol-version candidate; do not mix it into the storage-only work.

Likewise, physical grouping may share author/CommitSeq headers only within a homogeneous group. Never group operations from different authors into a single author-stream batch or discard original author identity.

## 10. Target logical model

| Fact | Cardinality/key | Owner | Retention |
|---|---|---|---|
| Current field winner | one per scope/namespace/record/field | FLD/data provider | until safely superseded/checkpointed |
| Pending local LWW operation | at most one per coalescible field key | FLD/provider staging | until freeze |
| Frozen operation payload | one per published ChangeId | one authoritative outbound/relay payload store | until all applicable peers safely ACK |
| Cross-peer dedup identity | one per scope/local replica/namespace/ChangeId, with fingerprint or validated content commitment | BIN-SYNC metadata | no finite GC proof in current protocol |
| Batch receipt | one per issuer/namespace/epoch/BatchId | BIN-SYNC metadata | duplicate-retry/recovery horizon |
| Peer progress | one per peer/namespace/author stream | BIN-SYNC metadata | while peer participates / retention requires |
| Batch replay mapping | one per peer/author/cursor range while unacknowledged | BIN-SYNC metadata | delete after validated ACK covers range |
| Relay selection | compact index into canonical operation payloads | FLD/BIN-SYNC adapter boundary | until relay payload promotion/retention is safe |
| Local WAL replay marker | one per replay-relevant WAL operation/commit | FLD WAL recovery | until WAL replay horizon closes |

Physical files may combine several facts when their lifetimes and crash transitions match. Do not combine facts with independent lifetimes into one record that prevents independent GC.

## 11. Priority order

1. **P0 — Cross-peer dedup correctness:** implement the exact global ChangeId identity/fingerprint contract; keep local WAL replay markers separate. Add alternate-route-after-Outbox-compaction tests.
2. **P1 — APPLY payload amplification:** encode Fields once plus relay selection indexes; split compact batch receipts from relay-only payload retention; make ApplyJournal paging bounded/seekable and add ACK/compaction.
3. **P2 — Peer replay-cache retention:** prune PeerState.Ids atomically when validated StreamAcked covers the range.
4. **P3 — Peer metadata packing:** group per-author pushed/acked/offset state; consolidate receipt facts after full usage audit.
5. **P4 — FLD Outbox vNext:** remove repeated local AuthorNodeId; evaluate base-sequence ordinal and CommitSeq groups; dual-version recovery and benchmarks.
6. **P5 — PendingLocal:** coalesce unpublished local LWW operations across commits, with crash-safe freeze.
7. **P6 — Optional deeper savings:** WAL-backed Outbox references, content-derived ChangeId, batch-level alias wire encoding, and OriginSeq only after protocol proofs.

Do not begin by changing ChangeId or introducing OriginSeq. First remove known duplicate payloads and unbounded caches while keeping the wire protocol stable.

## 12. Required conformance and crash tests

- One change delivered to N peers: one payload record, N independent peer cursors, no payload duplication.
- Same ChangeId arriving via A then B, including after Outbox compaction: one global dedup fact and at most one relay operation.
- Same ChangeId with a different canonical fingerprint: reject as conflict.
- Lost ACK and retransmission: same BatchId, previous digest and payload bytes until ACK.
- ACK range validation: only contiguous ranges advance; replay-cache entries are pruned only after durable ACK.
- Peer with no PeerState: contributes “not ACKed” to safe retention.
- No peers: preserve the current log/sneakernet behavior.
- New peer after compaction: verify the documented bootstrap guarantee and explicitly test whether historical relay losers are or are not required.
- Remote batch with relay subset: every operation encoded once in WAL; relay index identifies exactly the selected subset.
- Crash after WAL append, after receipt append, after relay promotion, after Outbox ACK, during compaction and before WAL truncation.
- ApplyJournal large-N test: bounded memory and cursor paging proportional to requested page, not total historical prefix.
- PendingLocal: multiple local commits to one field coalesce only before freeze; crash/reopen preserves pending payload; remote LWW winner cannot erase the pending local relay obligation.
- Snapshot: no ChangeId-ledger or relay GC based solely on current-state snapshot completion.

## 13. Decisions not made

- No ChangeId format change.
- No OriginSeq wire field.
- No removal of ChangeId from published operations.
- No per-peer operation copies or per-operation ACK bitmap.
- No in-place rewrite of frozen Outbox entries.
- No TTL-based ChangeId ledger GC.
- No snapshot-based Outbox/relay/dedup GC until a real per-author cut vector and completion/fencing contract exists.
- No new Storage dependency on FLD or BIN-SYNC semantics.

**Core principle:** store the logical operation once; store the identity once; store batch receipts once per batch; store delivery progress once per peer/author stream. Preserve independent lifetimes and never confuse an operation identity, a stream cursor, a WAL commit sequence or a batch idempotency key.

## 12. Compaction decisions and implementation roadmap (2026-11-07)

The cross-repository target and phase order are maintained in [Sync Storage Compaction Roadmap](SYNC-STORAGE-COMPACTION-ROADMAP.md). This section records decisions closing the audit's open design direction; it does not claim implementation.

- Keep current field state, unpublished local staging, immutable published payload, relay payload and protocol metadata as distinct logical data planes.
- Current state stores the winner, not a duplicate full operation history. Pending local LWW stores at most one latest unpublished operation per canonical field key. Freeze/publication is the immutability boundary.
- One published payload is shared across all peers. Per-peer/per-author progress remains O(P × A), not one delivery record or payload copy per operation/peer. No per-operation ACK bitmap.
- Global ChangeId identity/fingerprint is independent of sender route. Batch receipt remains issuer-specific. Peer progress is a compact per-peer/namespace/author tuple.
- Outgoing replay mappings exist only for unacknowledged cursor ranges; prune atomically with validated contiguous ACK while preserving the digest-chain checkpoint.
- Relay selection references accepted APPLY operations by compact indexes/bitmap; it does not duplicate payload. WAL replay markers remain separate.
- Payload, receipt, dedup, replay-cache and peer-state retention have independent safe reclamation rules. Physical co-location is acceptable only when atomicity and lifetime remain enforceable.

Implementation order: (1) reproducible baseline; (2) route-independent ChangeId dedup, replay-cache pruning and receipt audit; (3) APPLY payload dedup, metadata-only receipt, relay-only payload and bounded paging; (4) durable PendingLocal coalescing before publication; (5) versioned compact peer-state and FLD Outbox encodings; (6) safe GC and snapshot/new-peer semantics; (7) optional WAL-backed Outbox.

Open gates: no protocol-safe global dedup GC horizon is proven; relay GC requires a validated downstream watermark; snapshot completion does not prove delivery of relay-eligible LWW losers without a per-author cut/tail proof; the 60–65-byte Outbox estimate is unmeasured and must include framing/CRC/index overhead. ChangeId generation, OriginSeq and wire format remain unchanged.


## 13. Implementation follow-up — outgoing replay-cache pruning (2026-11-07)

The BIN-SYNC implementation now prunes `PeerState.Ids` entries when and only when a validated contiguous per-author ACK covers the cached cursor range end. The per-peer `LastStreamDigest` chain checkpoint is retained. This was implemented in `dc9a1304de00e75bacb0d2bdd864e0f48e687361` and covered by restart/idempotent-ACK assertions in `402b801e4b2d2781e048f3785766c0abf0425016`. GitHub Actions run 42342495367 passed all jobs.

The original code baseline remains the audit reference; this is a narrowly scoped post-audit code delta. Global ChangeId dedup, APPLY payload dedup and receipt/relay split are not implemented by this change.

## 14. Global ChangeId ledger implementation follow-up (2026-11-07)

The BIN-SYNC runtime now closes the previously open route-independent dedup correctness gate:
- ledger key: `(namespaceId, ChangeId)`, with local replica identity implicit in the Node;
- value: BLAKE3-128 canonical semantic fingerprint over namespace, RecordId, OpKind, resolved permanent author and FULL_AUTHOR_ID operation bytes, plus sorted unique causal dependencies;
- same ID/same fingerprint: no re-apply and no second relay, including after origin Outbox compaction;
- same ID/different known fingerprint: reject before WAL append and state/cursor publication;
- local commit and remote WAL/replay paths update the ledger atomically;
- NodePersistence v6 migrates v3-v5 SeenChanges into explicit unknown markers and reconstructs only complete retained legacy Outbox payloads;
- causal dependencies are retained through relay encoding and WAL recovery.

Tests cover second-route duplicate arrival, echo back to the original author after local Outbox compaction, equivocation rejection without partial state, v5 migration, v6 persistence, causal trailer relay and WAL replay. GitHub Actions run [42344100307](https://github.com/smersh1307n2/BIN-SYNC/actions/runs/42344100307) passed both .NET 10 and fuzz/lab jobs. Tested code commit: `468d26988dee370aef4f7266b76bcbc6a5e5c77a`.

The ledger is intentionally not garbage-collected: ACK, Inbox expiry, tombstone GC or TTL do not prove that an old operation cannot arrive through a newly discovered route. Legacy entries with unavailable payload fingerprints remain unknown and suppress relay conservatively until a matching arrival upgrades them. A later optimization may remove redundant peer-scoped remote SeenChanges entries, but only after preserving the distinct local WAL replay marker role.

The next BIN-SYNC metadata slice consolidated Inbox and InboxAuthors: one dictionary now stores each issuer/namespace/epoch/BatchId key with its exact durable ACK author. NodePersistence v7 migrates v3-v6 only after validating old key-set/map parity; WAL replay and duplicate ACK handling use the one map. GitHub Actions run [42344555045](https://github.com/smersh1307n2/BIN-SYNC/actions/runs/42344555045) passed both jobs; tested code commit: `9d58f8d6791c7cb03e36e8d63f4696449aa7a354`. This removes one duplicate key per receipt (about 34 serialized bytes for common varint sizes) and one in-memory HashSet entry plus its bucket overhead; the byte saving is estimated from the encoding, not benchmarked.

A further pass removed redundant peer-scoped remote SeenChanges rows after global ChangeId dedup became authoritative. New remote ApplyBatch no longer writes or probes per-peer ChangeId markers. `SeenChanges` now retains only local WAL replay markers; v3-v5 migration folds all old identities into unknown global ledger entries before dropping remote rows, while v6-v7 migration retains the global ledger and filters old remote markers. GitHub Actions run [42345011358](https://github.com/smersh1307n2/BIN-SYNC/actions/runs/42345011358) passed both jobs. This avoids about 33 raw serialized bytes plus HashSet overhead per remote provenance row; the estimate is encoding-derived, not benchmarked.

A subsequent BIN-SYNC pass implemented NodePersistence v8 InboxReceipt: durable author, original cursorEnd, BLAKE3-128 header fingerprint, appliedAt and expiresAt. The fingerprint commits to cursorStart/end, previous/records/batch digests, schema/mode fields and flags. Duplicate BatchId replay compares this header commitment and ACKs using stored cursorEnd/author; it deliberately does not decode or apply replay-supplied entries/ChangeIds/causal dependencies, preserving duplicate-before-causal-gate semantics. Legacy v3-v7 receipts migrate with explicit unknown cursor/fingerprint markers. The normative BIN-SYNC specification §10.2/§10.2.2 was updated to this contract.

Receipt expiry/GC and WAL-generation/materialization references remain open. New receipts use `expiresAt=0`, meaning no expiry is scheduled. The v8 receipt metadata offsets some of the raw bytes saved by collapsing the old Inbox+InboxAuthors keys; varint cursor/timestamp encoding keeps the additional cost down. Do not claim net disk reduction versus v6 without a measured comparison. Next BIN-SYNC work: prove safe receipt expiry/GC and remove APPLY WAL/ApplyJournal payload duplication, then split relay payload retention from metadata-only receipts.

## 15. Cross-repository implementation checkpoint (2026-11-08)

The 2026-11-07 findings above are historical audit evidence. The current BIN-SYNC implementation has advanced beyond the old NodePersistence-only checkpoint:

- **One logical operation, many peers:** the Storage-backed BIN-SYNC path stores outbound/relay payload once per namespace/author stream. Peer-specific pushed/acked progress and exact batch reservations remain separate; the interested-peer roster gates payload compaction at the minimum safe ACK. Do not add per-peer payload copies or an ACK bitmap to every operation.
- **Global dedup identity:** StorageMetadataStore uses one route-independent `(namespaceId, ChangeId)` ledger entry per local replica partition, with canonical BLAKE3-128 fingerprint and local/remote origin. The operation payload is not duplicated in the dedup index. Peer-scoped remote SeenChanges rows are no longer authoritative; local WAL replay markers remain distinct.
- **PendingLocal and freeze:** StorageDataProvider's durable feed is the pending-local journal. The coordinator validates source operations, drains across pages, coalesces unpublished LWW changes by canonical record/field/lifecycle key using the HLC/author winner, and freezes only winners. Provider feed ACK follows durable Metadata Store freeze and Node checkpoint. Already-frozen ChangeIds remain immutable on retry.
- **Receive authority:** for the Storage-backed path, Metadata Store PendingApply, full-header receipt fingerprint and global ChangeId ledger validate/deduplicate before Node mutation. Node receives an operation decision mask and does not append duplicate global ChangeId/RemoteBatch receipt state. The legacy non-Storage Node-owned receive/transport path remains during migration.
- **Metadata size:** local-origin and fingerprint are folded into one ChangeId ledger row; per-author ACK/high-watermark values are combined into one row. A direct frozen-ChangeId lookup returns a read-only fingerprint view and measured 0 B allocation at 256, 1,024 and 4,096 ledger rows (about 80–82 ns per lookup in the recorded Ubuntu BenchmarkDotNet run). This does not measure the whole coordinator retry scan.
- **FLD remote APPLY:** ApplyCommitCodec v3 encodes incoming operations once and stores only relay-selected payloads in ApplyJournal alongside compact receipt metadata. The full WAL commit is flushed before relay-only materialization. ApplyJournal index growth and independent receipt-vs-relay physical GC remain open.
- **FLD local Outbox:** `FldDatabase.Put` calls within one `Commit()` are coalesced by the dirty overlay, but repeated commits to the same field still produce multiple Outbox entries. A durable cross-commit PendingLocal staging area is still a separate FLD Database design/implementation stage; do not rewrite already published Outbox entries.
- **Verification:** BIN-SYNC Core 648/648, Fuzz/Lab 31/31 and 70.4% line coverage; Storage Integration 122/122 on Ubuntu and Windows at code/test head `75133279fc9eca941983fe1ffcce94f63a1e0f70`. The macOS-15 run is queued after restoring the named-Mutex WAL fence and correcting the process-kill test to terminate the actual testhost PID.

**Retention remains conservative:** the exact global ChangeId ledger is not reclaimed by TTL or a single peer ACK. The current protocol/runtime still lacks a proven per-author snapshot barrier and immutable per-operation origin sequence that prove every active peer has crossed a contiguous prefix. E-25 tombstone/ghost GC remains disabled. Stale peer epochs may retain metadata indefinitely until an explicit retirement protocol forces full rebootstrap.

**Two-DLL decision remains:** keep `Fld1.Storage.dll` as the reusable BCL-only storage/WAL/snapshot engine and `Fld1.Database.dll` as the FLD database layer/overlay. Keep the source organization separable so the storage DLL can be reused independently; do not add a third shared runtime DLL.

**Open gates:** membership and peer-epoch retirement; receipt/dedup/tombstone GC horizons; legacy non-Storage Node/Transport authority retirement; true physical power-loss/I/O fault injection; complete coordinator process-kill/reopen matrix; FLD cross-commit PendingLocal and `JournalSeq → ChangeId → OutboxSeq` atomic provenance adapter.

## 16. WAL writer-fence hang follow-up (2026-11-08)

The file-share WAL lease experiment was reverted after it stalled the cross-process test/lock lifecycle. The implementation is again a named OS-managed Mutex owned and released by a dedicated process-wide manager thread. The process-kill regression now terminates the actual testhost PID that owns the StorageDatabase, not only the outer dotnet test process. Child-process cleanup is unconditional and bounded, and the Storage Integration workflow now uses a three-minute dotnet test --blame-hang-timeout with a mini dump.

On BIN-SYNC head 532baa0010140b799f3759e1c88d0c722cdf6727, verification passed: Core 648/648, Fuzz/Lab 31/31, 70.4% line coverage, Storage Integration 122/122 on Ubuntu and Windows, and both in-memory/TCP samples. macOS-15 is still queued. This supersedes the prior 122/122 checkpoint at 75133279fc9eca941983fe1ffcce94f63a1e0f70; do not mark the three-OS gate complete until macOS finishes.




## 17. Coordinator process-kill receive crash cuts and session checkpoint ordering (2026-11-08)

The Storage Integration suite kills the actual testhost process at all four receive durability boundaries and reopens the same physical Storage database:

1. **After PendingApply staging, before Node apply:** the intent/wire payload is durable; Node and provider have not applied the incoming field. Recovery replays the intent.
2. **After Node checkpoint, before provider apply:** Node is durable and provider is not. Recovery applies the provider mutation and finalizes metadata.
3. **After provider commit, before Metadata Store finalization:** provider and Node are durable while PendingApply remains. Recovery repeats the same ApplyToken idempotently and finalizes the receipt.
4. **After Metadata Store finalization, before network ACK:** Node/provider state and receive cursor/receipt are durable, PendingApply is gone, and the process is killed while the before-ACK callback is blocked. Reopen observes the committed cursor and no pending work.

A handshake commits the negotiated alias map and peer epoch to Node memory. Since restart replay decodes the original wire payload, StorageNodePersistence.Save is required after successful SyncSession establishment and before the first inbound durable batch. This requirement is documented in the persistence API and demonstrated in both Storage array samples. Windows process-kill recovery retries WAL reopen briefly to account for delayed OS handle release; the test harness cleanup is bounded and the workflow emits a mini dump after a three-minute hang.

Verified on BIN-SYNC head 354eab38dc859e2f26838efb332c14310aa17022: Storage Integration 127/127 independently on Ubuntu, Windows and macOS-15; Core 648/648; Fuzz/Lab 31/31; 70.4% line coverage; in-memory and real-TCP samples pass on all three platforms.

The coordinator-level receive process-kill sequence is now covered end to end. True physical power-loss/I/O fault injection remains a separate, open validation gate.
