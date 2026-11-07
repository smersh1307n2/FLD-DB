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

## 6. FLD remote APPLY: largest confirmed payload amplification

### 6.1 Duplicate relay subset inside APPLY_COMMIT

RemoteBatchCommit validates RelayWrites as an exact subset of Writes by ChangeId and byte-identical operation data. But ApplyCommitCodec v2 serializes both Fields and RelayFields as separate full arrays.

Thus every relay-selected operation is serialized twice inside the same APPLY_COMMIT payload. A non-relay operation is serialized once.

### 6.2 Same full payload in WAL and ApplyJournal

The same encoded APPLY_COMMIT payload is then persisted in the WAL and in ApplyJournal. ApplyJournal retains the complete Fields array even though relay enumeration needs only RelayFields and duplicate-batch lookup needs a compact receipt.

At the operation-payload level, before counting the current-state run/overlay:

- non-relay operation: two copies (WAL + ApplyJournal);
- relay-selected operation: four copies (Fields + RelayFields in WAL, and the same two arrays in ApplyJournal).

This is the clearest large storage-amplification target found in the current code.

### 6.3 ApplyJournal also has unbounded index and paging costs

The current ApplyJournal maintains a List of every historical index entry and a Dictionary keyed by batch identity. It has no public ACK/compaction/truncation boundary. ReadBatches(afterJournalSeq, limit) iterates the list from its beginning and skips entries until it reaches the requested cursor. Repeated paging therefore becomes increasingly expensive, while the in-memory list/dictionary grow with every accepted batch.

A receipt lookup and a relay payload log must have separate retention. Do not retain all non-relay fields forever merely to answer duplicate-batch retries.

### 6.4 Preferred target decomposition

Do not make a second full copy of APPLY_COMMIT in a permanent relay journal.

1. **WAL is authoritative during the uncheckpointed interval.** Encode each incoming operation once. Store relay selection as a compact bitmap or delta-encoded operation indexes, not as a second full operation array.
2. **Batch receipt is metadata-only.** Store batch identity, cursor range, issuer/author, digest-chain fields, RecordsRoot, applied result and the durable commit marker needed for duplicate ACK.
3. **Relay payload is relay-only.** Before truncating the WAL segment that contains an accepted batch, materialize only relay-selected operations into a compact relay payload log (or retain a safe immutable reference to their bytes). This promotion must be durable and idempotent by CommitSeq/batch identity.
4. **Then truncate WAL.** Never truncate the only durable copy of a relay-eligible operation.
5. **Reclaim relay payload only after BIN-SYNC supplies a validated safe downstream retention watermark.** Reclaim batch receipts according to their independent retry/Inbox expiry rules. Keep ChangeId dedup facts for their separate protocol-safe horizon.

The relay-selection bitmap/index and receipt can be committed atomically with the APPLY result. The relay payload log can use one operation record per selected ChangeId. It must include LWW losers whenever BIN-SYNC selects them for relay.

This is an internal storage-format redesign. It does not require changing DELTA_BATCH.

## 7. Local WAL and Outbox payload duplication

FLD local Commit currently appends and flushes the WAL group commit, then appends each field mutation (including value bytes) to Outbox and flushes Outbox. The same local mutation payload therefore exists in WAL and Outbox until the sealed WAL prefix is truncated. After checkpoint, the Outbox remains as the durable outbound copy until ACK/compaction.

A later optimization candidate is a WAL-backed Outbox reference:

- while the WAL record is retained, Outbox metadata references the immutable WAL record/operation ordinal rather than copying the value bytes;
- the consumer can read the referenced payload for delivery;
- at checkpoint, before WAL truncation, unacknowledged references are atomically promoted to the Outbox payload log;
- already ACKed operations need not be copied;
- recovery must distinguish referenced, promoted and ACKed states.

This can reduce transient duplicate payload storage, but it is more invasive than removing APPLY duplication. It must not be attempted until the simpler Outbox format and APPLY retention work is measured.

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
