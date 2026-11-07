# Tombstone Retention and Safe Garbage Collection — Research Note

**Date:** 2026-11-07  
**Status:** research-backed design proposal for BIN-SYNC E-25; not an implementation or wire-format approval.  
**Applies to:** BIN-SYNC CORE v2.4.19, FLD-DB v1.5.9-DRAFT, FLD Storage Core.  
**Ownership:** BIN-SYNC owns protocol safety; FLD Database owns field/record tombstone semantics and physical reclamation requests; Storage Core persists opaque bytes and performs generic reclamation only.

## 1. Executive conclusion

There is no universally safe finite tombstone TTL for a peer-to-peer system that permits replicas to remain disconnected for an unbounded period and later rejoin as ordinary writable peers.

A time window can be an operational policy, but it is not a correctness proof. Safe reclamation requires one of two explicit recovery models:

1. **Causal-stability / membership model:** prove that every replica in a defined membership baseline has durably processed the deletion, or durably fence replicas that did not; force fenced replicas through a full snapshot/bootstrap before accepting further writes from them.
2. **Forgotten-knowledge model:** after removing individual tombstones, retain compact knowledge of the forgotten versions and use it to detect stale replicas or resurrection attempts; recover those replicas by full enumeration/reconciliation.

Microsoft Sync Framework is particularly relevant because it implements the second model. It does not merely keep tombstones forever or rely on a long TTL: it records versions of cleaned tombstones in a separate ForgottenKnowledge structure. It compares this knowledge with a destination replica's knowledge and initiates full enumeration when the destination is out of date. It also checks an item's creation version to detect an attempted resurrection after the tombstone itself has been forgotten.

For BIN-SYNC, the recommended direction is a conservative hybrid: **durable per-namespace safe frontier + membership generation/fencing + mandatory full resync + atomic snapshot/feed barrier**, with compact forgotten-frontier knowledge retained after physical tombstone removal. A wall-clock TTL or HLC cutoff may be a minimum operational delay or storage-pressure trigger; it must never authorize deletion on its own.

Until this mechanism is implemented and enforced on every delta, reconcile and snapshot path, tombstone/ghost GC remains fail-closed. The current no-op PurgeBelowCutoff behavior is correct and must remain.

## 2. Research and production-system findings

### 2.1 Microsoft Sync Framework: ForgottenKnowledge

Microsoft Sync Framework distinguishes an item's tombstone from the knowledge retained after that tombstone is cleaned up.

Documented behavior:

- A deleted item remains represented by metadata called a tombstone.
- A provider may clean tombstones locally, for example based on age or storage pressure.
- When a tombstone is removed, the provider records its version in ForgottenKnowledge.
- ForgottenKnowledge is deliberately an overestimate: it may describe versions whose tombstones were removed even if some corresponding active metadata still exists.
- During synchronization, the framework compares the source's forgotten knowledge with the destination's current knowledge.
- If the destination does not contain the forgotten knowledge, it is considered out of date and a full enumeration/recovery synchronization is initiated.
- When a source sends an item that a destination previously knew but has since deleted and forgotten, the destination compares the item's **creation version** with its current knowledge. If that creation version is already known, the item is treated as an update-delete conflict rather than a new item.

The central lesson: **physical tombstone removal is safe only because a separate compact knowledge structure preserves the information needed to detect stale replicas and resurrection attempts.** ForgottenKnowledge is not equivalent to a TTL, tombstone-count limit, or ACK cache.

Important limitation for adaptation: Sync Framework relies on version/knowledge semantics and item creation versions. BIN-SYNC must not claim equivalent protection until it can identify the relevant stable operation/version for every accepted update and validate stale-peer recovery on all receive paths.

References:
- Microsoft Learn, *Managing Tombstones*: https://learn.microsoft.com/en-us/previous-versions/sql/synchronization/sync-framework-2.0/bb902838(v=sql.105)
- Microsoft Learn, *ForgottenKnowledge Class*: https://learn.microsoft.com/en-us/previous-versions/sql/synchronization/sync-framework-2.0/bb903502(v=sql.105)
- Microsoft Learn, *IForgottenKnowledge*: https://learn.microsoft.com/en-us/windows/win32/api/winsync/nn-winsync-iforgottenknowledge
- Microsoft Research, *Peer-to-Peer Replication in WinFS* (MSR-TR-2006-78): https://www.microsoft.com/en-us/research/wp-content/uploads/2006/06/tr-2006-78.pdf

### 2.2 CRDT research: causal stability versus commitment

Shapiro et al., *CRDTs: Consistency without Concurrency Control* (2011), section 4, distinguishes two GC classes:

- Metadata attached to an update may be removed when the update is stable: all relevant replicas have received it and all concurrent updates that could affect its interpretation have taken effect.
- Removing tombstones from a replicated set, resetting version-vector state, or otherwise changing the replicated payload requires a commitment protocol.

The paper notes that stability detection assumes a known replica set and that replicas do not crash permanently in an undetectable way. A reliable causal broadcast layer can provide useful stability information, but BIN-SYNC's disconnected asynchronous P2P environment must not assume such a transport guarantee.

Reference: https://pages.lip6.fr/Marc.Shapiro/papers/CRDTs-beatcs-2011-06.pdf

Bauwens and Gonzalez Boix, *From Causality to Stability: Understanding and Reducing Meta-Data in CRDTs* (MPLR 2020), demonstrates that reliable causal delivery can allow metadata for causally stable operations to be removed earlier. This is useful for understanding the trade-off, but its reliable-causal-broadcast assumptions are not a blanket property of BIN-SYNC.

Reference: https://doi.org/10.1145/3426182.3426183

### 2.3 Cassandra: grace period is an operational bound, not an absolute guarantee

Cassandra stores timestamped tombstones and retains them for gc_grace_seconds (default ten days) before they become eligible for compaction. The grace period exists so unavailable replicas can recover and receive deletes. If a replica remains unavailable beyond that interval, the tombstone may be purged before that replica has learned about the deletion; when the old replica returns, deleted data can be repaired back as a zombie.

Cassandra also requires compaction to see the tombstone and older data it shadows before purging it. A tombstone's age alone does not prove that every older value it suppresses has been physically eliminated.

This is a valid operational strategy where repair cadence and maximum replica outage are controlled. It is not sufficient for arbitrary long-lived offline peers.

Reference: https://cassandra.apache.org/doc/latest/cassandra/managing/operating/compaction/tombstones.html

### 2.4 CouchDB: distinguish compaction, deletion and purge

CouchDB normal deletion creates a new deleted revision (_deleted=true) so replication can propagate the deletion. Database compaction can discard old revision bodies while retaining revision-tree metadata needed for conflict resolution; revision pruning bounds that history.

A purge is stronger: it removes revision references. CouchDB's internal clustered purge tracks purge sequence/checkpoints and removes purge history only after internal replication jobs and secondary indexes have processed it. By contrast, external replication does not replicate purge operations; a purge must be applied separately to each external database.

This is a useful warning for BIN-SYNC: **a local physical purge is not automatically a replicated semantic operation.** If we introduce a forgotten frontier or namespace GC epoch, it must be propagated/validated through the protocol or stale peers must be fenced and forced through bootstrap.

References:
- Replication: https://docs.couchdb.org/en/stable/replication/index.html
- Conflict/revision model: https://docs.couchdb.org/en/latest/replication/conflicts.html
- Purge API and external replication limitation: https://docs.couchdb.org/en/latest/api/database/misc.html
- Clustered purge/checkpoints: https://docs.couchdb.org/en/stable/cluster/purging.html

### 2.5 Couchbase Sync Gateway

Sync Gateway uses tombstones so intermittently connected mobile clients can learn that documents were deleted. Its metadata purge interval must reflect expected client synchronization frequency; a client that does not sync before purge can miss the deletion. The documented default purge interval in the cited Sync Gateway material is three days.

This illustrates the practical cost of choosing a short retention horizon for mobile/offline replicas.

Reference: https://docs.couchbase.com/sync-gateway/3.0/managing-tombstones.html

## 3. What these systems teach us — and what not to copy

| System | Useful idea | Limitation for BIN-SYNC |
|---|---|---|
| Microsoft Sync Framework | ForgottenKnowledge, stale-replica detection, full enumeration, creation-version check | Requires compatible knowledge/version and creation-version semantics; do not assume BIN-SYNC already has them |
| WinFS P2P | Knowledge-driven state-based P2P, compact vector-set summaries, metadata proportional to database state rather than update rate | A research design, not a drop-in BIN-SYNC protocol |
| CRDT causal stability | Remove metadata only after causal dependencies are stable | Requires known membership/delivery assumptions; tombstone removal itself may need commitment |
| Cassandra | Explicit grace period and repair discipline; compaction must not expose older shadowed data | A finite grace period cannot protect a replica absent longer than the grace period |
| CouchDB | Keep revision metadata separate from bodies; distinguish normal replicated deletion from purge; checkpoint purge propagation | External purge is not propagated automatically |
| Couchbase Sync Gateway | Retention must account for infrequent mobile clients | Time-based policy remains a risk if client outage exceeds the interval |

Do not transplant Sync Framework wholesale. Reuse the invariant behind ForgottenKnowledge, not necessarily its object model. In particular, do not introduce a large per-item version history if BIN-SYNC can express the required safe frontier more compactly.

## 4. Proposed BIN-SYNC safe-reclamation model

### 4.1 Keep five lifetimes independent

1. **Current field/record tombstone or ghost:** retained while required for merge visibility, conflict semantics, stale-state rejection or digest compatibility.
2. **Relay payload:** retained until the validated downstream safe ACK frontier permits removal. This is partly implemented in FLD through per-author JournalSeq ACK and post-WAL-seal compaction; the BIN-SYNC provenance adapter remains open.
3. **Global ChangeId dedup ledger:** retained according to its own protocol-safe duplicate/equivocation horizon. Relay ACK does not prove that the same ChangeId cannot be replayed through another route later.
4. **Inbox batch receipt:** retained according to the sender retry/duplicate-batch horizon. It is not a tombstone and must not inherit relay payload retention.
5. **Forgotten frontier / GC epoch:** compact knowledge that allows a replica to reject or recover from state older than the purge boundary. It outlives individual tombstone payloads and is reclaimed only by a later safe epoch/snapshot transition.

No one TTL or ACK should control all five.

### 4.2 Candidate eligibility conditions for a specific tombstone

A tombstone may become a purge candidate only when all applicable conditions are true:

1. **Exact identity:** the database can identify the tombstone's semantic operation, author and ChangeId, and resolve it to the retained local outbound stream position. HLC wall time alone is not an identity or a stream frontier.
2. **Durable peer baseline:** the namespace has a durable membership/baseline generation identifying the peers whose state must be protected.
3. **Contiguous processed ACK:** every peer still in that baseline has durably acknowledged a contiguous stream position covering the tombstone. An absent peer state, missing ACK, unresolved provenance entry or non-contiguous range blocks reclamation.
4. **Explicit fencing for exclusions:** a peer removed from the baseline is durably fenced from sending old-generation deltas. On return it must complete a full snapshot/bootstrap before ordinary incremental traffic or old local state is accepted.
5. **Snapshot/feed barrier:** the snapshot and its feed/frontier watermark are captured at one atomic logical barrier. A snapshot of current winners alone must not silently imply that LWW-loser relay history or unprocessed operations were delivered.
6. **Type-specific proof:** only field tombstones, record lifecycle tombstones and ghost state covered by the proof may be reclaimed. Ordinary live values must never be deleted because their HLC is old.
7. **Durable forgotten knowledge:** the frontier/epoch that explains the purge is persisted before physical tombstone removal becomes irreversible.

If any condition cannot be checked, retain the tombstone.

### 4.3 Membership and offline peers

An offline peer must not be silently ignored because it has not synchronized recently.

There are only two safe choices:

- Keep it in the current baseline, in which case its missing ACK blocks the relevant reclamation.
- Explicitly remove/fence it by advancing the namespace membership/GC generation. A peer carrying an older generation must not be allowed to merge its old delta as an ordinary current-generation update. It must obtain a full current snapshot and establish a new baseline first.

A time-based offline threshold may trigger an administrative proposal to fence a peer, but the threshold itself is not the proof. The durable generation transition and receive-path enforcement are the proof.

New peers should enter through a snapshot/bootstrap contract. They are not retroactive recipients of every historical relay operation unless the product explicitly promises event-history replication.

### 4.4 Forgotten frontier representation

Prefer a compact monotonic structure over one tombstone ID per deleted field, but do not assume the current OutboxSeq, JournalSeq, CommitSeq or relay-local Seq is a globally stable origin sequence.

The existing audit explicitly distinguishes these counters. A relay node can assign its own local stream sequence to a relayed operation; that sequence is not a stable origin identity. BIN-SYNC's current ChangeId is the stable operation identity, while the current per-author safe Outbox ACK is a local retention position.

Therefore:

- The first implementation should use the existing local safe-ACK/provenance mapping and a durable namespace membership generation.
- A vector/frontier may summarize local per-author stream positions only within the exact node/namespace/generation that owns those positions.
- Do not compare or merge local OutboxSeq values from different replicas as though they were globally comparable.
- Do not add OriginSeq or change ChangeId generation as part of this GC task; those remain separate protocol research gates.
- If a future protocol version adds a stable origin sequence/incarnation, it can make ForgottenKnowledge more compact and portable, but it needs a separate identity, migration and equivocation proof.

The persisted forgotten frontier should be conservative: it may claim that more history has been forgotten than has actually been purged, as Microsoft does, but it must never understate the set of versions that a stale replica could reintroduce.

### 4.5 State transition and crash ordering

The intended logical transition is:

**Retained tombstone → eligible under baseline/frontier → forgotten frontier durably advanced → tombstone payload physically reclaimed**

The frontier and membership/fence state must be durable before physical purge can become irreversible. A crash must never leave the tombstone removed while the metadata needed to reject/recover stale peers is absent.

The operation should be idempotent. Recovery must either observe the old tombstone and old frontier, or the advanced frontier and the reclaimed tombstone; intermediate state must fail closed.

### 4.6 TTL policy

A configured retention duration can still be useful operationally:

- avoid immediately purging newly created tombstones;
- allow routine replication/repair to run;
- reduce churn from short-lived delete/recreate patterns;
- alert when peer lag approaches the configured operational limit.

But TTL expiry must only set GC_CANDIDATE; it must not directly delete. If a peer is offline beyond the configured window, either keep the tombstone or execute the explicit fence + full-resync protocol.

## 5. Mapping to the current project

Already established in the current roadmap/branches:

- BIN-SYNC E-25 disables TTL/HLC-only tombstone deletion.
- The old PurgeBelowCutoff path is a no-op; regression tests preserve old live values, field tombstones and record lifecycle tombstones.
- Node.GetSafeOutboxAckedThrough(namespace, author) computes the minimum local OutboxSeq acknowledged by all interested peers, returning zero if no peer baseline can safely authorize reclamation.
- Node.TryResolveOutboxSequences resolves retained (author, ChangeId) identities to local OutboxSeq values.
- FLD persists per-author relay ACK watermarks and reclaims acknowledged relay payload only after the corresponding APPLY WAL boundary is sealed/truncated.
- The adapter still needs durable JournalSeq → (author, ChangeId) → OutboxSeq provenance, an atomic scan cursor, and an outbound publication gate.
- The two-DLL rule remains binding: Fld1.Storage.dll stores opaque bytes and does not interpret tombstones; Fld1.Database.dll owns tombstone semantics and the decision to reclaim them.

Still missing for actual tombstone GC:

- durable namespace membership/baseline generation and stale-peer fence enforcement;
- a safe per-namespace/per-author frontier with an exact definition of which peer ACKs cover each tombstone;
- an atomic snapshot/feed barrier used by all reconcile and snapshot paths;
- full-resync enforcement for stale peers before accepting old-generation writes;
- durable forgotten-frontier persistence and crash recovery;
- type-specific purge tests for field tombstones, record lifecycle tombstones and ghost state;
- alternate-route, long-offline-peer, membership-removal, crash-at-every-transition and snapshot-concurrency tests.

## 6. Required adversarial test matrix

1. Peer A receives a tombstone; peer B is offline and has no ACK: no purge.
2. Peer B returns before fencing and presents pre-delete state: deletion wins; no resurrection.
3. Peer B is durably fenced into a newer generation and attempts an old-generation DELTA_BATCH: reject or require full bootstrap; do not apply its old state.
4. Peer B completes full snapshot/bootstrap at a consistent snapshot/feed barrier: it may join the new baseline.
5. Two peers acknowledge unequal author-stream positions: the minimum contiguous safe frontier blocks purge until both cover the tombstone.
6. A peer has pushed but not ACKed the tombstone: no purge.
7. A peer ACK is non-contiguous or its durable PeerState is missing: no purge.
8. The tombstone is a relay-selected LWW loser: its payload/identity is retained until downstream safe ACK even if it is not the current field winner.
9. A snapshot is taken concurrently with a delete and later update: no gap or duplicate between snapshot and tail; a snapshot alone does not retire unrepresented relay history.
10. Crash after forgotten-frontier persistence but before tombstone removal: recovery is idempotent and may finish reclamation.
11. Crash after tombstone removal but before frontier persistence must be impossible by transaction ordering; inject the boundary and verify recovery fails closed.
12. Old live values, field tombstones, record lifecycle tombstones and ghosts are tested independently; an old HLC never deletes a live field.
13. A duplicate ChangeId arrives through a different peer after tombstone payload compaction: global dedup/equivocation semantics remain intact.
14. A newly joined peer receives current-state bootstrap without being promised historical relay/event provenance unless that contract is explicitly enabled.

## 7. Decision recommendation

Adopt the following as the design direction, subject to protocol review and executable model/tests:

- No TTL-only or HLC-only tombstone GC.
- No assumption that every disconnected peer will return within a finite grace period.
- Use a durable membership baseline and explicit generation fencing to define which replicas must have processed the deletion.
- Use contiguous per-author safe ACK plus exact provenance to prove delivery to the current baseline.
- Require a consistent snapshot/feed barrier and mandatory full resync for stale/fenced peers.
- Persist compact forgotten-frontier/epoch knowledge before removing tombstone payload.
- Keep ChangeId ledger, Inbox receipts, relay payload and tombstone lifetimes independent.
- Keep the current GC helper disabled until every condition is enforced by the protocol and covered by crash/adversarial tests.

This preserves the existing E-25 safety rule while providing a practical route to bounded metadata without pretending that a timeout can solve indefinite disconnection.
