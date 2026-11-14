# FLD Database Host and IPC contract

## Scope and assembly boundary

The Database Host is a process boundary around one `FldDatabase` namespace. The runtime assembly boundary remains exactly two DLLs:

- `Fld1.Storage.dll`: generic byte-key/value engine and local IPC transport primitives.
- `Fld1.Database.dll`: FLD field database, WAL/manifest integration, operation receipts and the Database IPC protocol/dispatcher/client facade.

`Fld1.Storage.Host` remains a Storage-only executable and must not reference or load `Fld1.Database.dll`. `Fld1.Database.Host` is a separate executable referencing the Database layer; it is not another runtime library.

The first Database Host currently owns partition 0. Its endpoint discovery record is published only after the IPC listener is ready. The endpoint record contains the protocol version, partition, deterministic pipe name, owner id and process id. Endpoint cleanup is owner-checked, so an old process cannot remove a newer Host's discovery record. The underlying database namespace lease remains the authoritative single-owner fence.

## Operation identity and retry contract

A client identity is a stable, non-empty 16-byte ClientId represented by Guid. The caller persists this identity across process restarts and reconnects. Each operation carries:

- a unique, non-empty OperationId;
- a canonical request fingerprint (SHA-256 over the little-endian operation code followed by the exact operation payload);
- for caller-sequenced operations, a nonzero ClientSequence contiguous per ClientId;
- an opaque result retained in the same durable GROUP_COMMIT as field mutations, or in a receipt-only GROUP_COMMIT for reads.

Caller-sequenced operations (WriteBatch, ReadField, and the explicit-sequence provider append) require the caller to persist OperationId and ClientSequence before transmission. The auto-sequence provider append (0x2111) instead sends ClientSequence = 0; the Host assigns the next contiguous sequence under the Database commit lock and returns that ClientSequence together with CommitSeq. The caller must durably persist the returned sequence before acknowledging receipts.

If a connection is lost after the Host commits but before the caller receives the response, the caller retries the auto-sequence operation with the same OperationId and exact payload. The Host returns the original CommitSeq and ClientSequence and does not append a second Outbox entry. Reusing an identity with different bytes, skipping a caller-supplied sequence, or retrying a sequence at/below the durable acknowledgement watermark fails closed.

Receipt acknowledgement advances only a contiguous prefix. The Host persists the watermark in WAL before retiring acknowledged operation results; replay after restart preserves both the watermark and the next expected sequence. A repeated ACK of an already acknowledged prefix is an idempotent no-op. Outbox ACK/compaction is separate and must not be used as a substitute for IPC receipt acknowledgement.

## Current operation payloads

All integers are little-endian. The Storage IPC operation envelope carries OperationId and OperationCode; the Database payloads are:

| Operation | Code | Payload |
|---|---:|---|
| ReadField | 0x2101 | ClientSequence (u64), RecordId (16 bytes), FieldId (u16) |
| WriteBatch | 0x2102 | ClientSequence (u64), mutation count (u32), then mutations |
| AcknowledgeReceipts | 0x2103 | acknowledged-through ClientSequence (u64) |
| AcquireOutboxLease | 0x2104 | empty payload; response is LeaseEpoch (u64) |
| ReadOutboxEntryChunk | 0x2105 | LeaseEpoch (u64), cursor AfterSequence (u64), EntrySequence (u64; zero means next), ValueOffset (u32), MaxChunkBytes (u32) |
| AcknowledgeOutbox | 0x2106 | LeaseEpoch (u64), through OutboxSeq (u64) |
| ReleaseOutboxLease | 0x2107 | LeaseEpoch (u64) |
| OpenSnapshot | 0x2108 | empty; operation id is the idempotent open key |
| ReadSnapshotPage | 0x2109 | HostGeneration (16 B), SnapshotToken (16 B), AfterIndex (i32), Limit (u16) |
| ReadSnapshotValueChunk | 0x210A | HostGeneration (16 B), SnapshotToken (16 B), RecordId (16 B), FieldId (u16), ValueOffset (u32), MaxChunkBytes (u32) |
| ReleaseSnapshot | 0x210B | HostGeneration (16 B), SnapshotToken (16 B) |
| ApplyRemoteBatch | 0x210C | versioned RemoteBatchCommit envelope; defined below (implemented in the current branch) |
| AppendLocalProviderBatch | 0x210D | ClientSequence (u64), mutation count (u32), then identity-preserving local draft mutations |
| GetOutboxHighWatermark | 0x210E | empty payload; response is durable Outbox HighWatermark (u64) |
| AppendLocalProviderBatchAutoSequence | 0x2111 | Versioned identity-preserving local provider batch with ClientSequence = 0; Host allocates the next contiguous sequence under the commit lock and returns it with CommitSeq, duplicate flag and mutation count |
| GetLocalAuthor | 0x2113 | empty payload; response is the authoritative nonzero LocalAuthor (16 bytes) configured by the Database Host |
| CommitPublishedOrigins | 0x2114 | Versioned bounded list of RecordId/FieldId/ChangeId/OriginAuthorEpoch/OriginSequence stamps; identity-only and idempotent |
| TryGetMetadataStoreOwnedPublicationCutover | 0x2115 | empty payload; returns Exists (u8) + NodeOutboxHighWatermark (u64) |
| EnableMetadataStoreOwnedPublication | 0x2116 | NodeOutboxHighWatermark (u64); immutable durable cutover, empty response |
| CompactTombstones | 0x2117 | Bounded exact-candidate list; Host revalidates under the database commit monitor |
| AcquireGcLease | 0x2118 | empty; returns a session-owned Host-wide exclusive GC lease token |
| ReleaseGcLease | 0x2119 | GC lease token (16 B); releases the Host-wide boundary |
| OpenGcReferenceScan | 0x211A | empty; requires the GC lease and returns scan token + draft horizon + snapshot-pin state |
| QueryGcReferenceScan | 0x211B | Reference kind + scan token + exact ChangeId, RecordId/ChangeId pair, or receipt identity; returns one boolean |
| ReleaseGcReferenceScan | 0x211C | scan token (16 B); releases the immutable Host reference index |
| ReadCurrentMetadata | 0x210F | RecordId (16 bytes), FieldId (u16); fixed metadata response includes value length, HLC, author, key epoch, tombstone, ChangeId and origin coordinates |
| ReadCurrentValueChunk | 0x2110 | RecordId (16 bytes), FieldId (u16), expected ChangeId (16 bytes), ValueOffset (u32), MaxChunkBytes (u32); returns Data, Missing or Changed plus one bounded value slice |

Each write mutation is RecordId (16 bytes), FieldId (u16), KeyEpoch (u16), kind (u8: 0=Put, 1=Delete), value length (u32), then value bytes. A Delete must have a zero value length. A batch is one atomic Database operation and must contain at least one mutation. The frame limit is enforced before sending and while decoding; malformed lengths, trailing bytes, invalid mutation kinds and zero identifiers are rejected.

The canonical request fingerprint includes the operation code and the complete encoded payload. A retry therefore cannot silently change a field value, mutation order, key epoch or ClientSequence while retaining the same operation identity.

## Outbox lease and chunk protocol

The Outbox consumer lease is owned by the IPC ClientId. Acquiring a lease durably increments its epoch and fences every previous token. When a Host reopens an Outbox whose persisted lease still has a nonzero owner, `OutboxLog.Open` advances the epoch and clears that owner before serving clients; the reconnecting consumer must acquire a new lease. Explicit release also advances the epoch and clears ownership. A stale lease is rejected by reads, ACK and release.

`ReadOutboxEntryChunk` returns one entry at a time. With EntrySequence zero, the Host selects the next entry after AfterSequence. For continuation, the client repeats the returned OutboxSeq and supplies the next ValueOffset; the client facade normalizes the cursor to OutboxSeq - 1. The response carries OutboxSeq, CommitSeq, ChangeId, RecordId, FieldId, OpKind, AuthorNodeId, HLC, KeyEpoch, total value length, offset and the copied value chunk. A one-byte status-zero response means no entry remains after the cursor.

The Storage IPC frame is limited to 4 MiB and Outbox values may also reach 4 MiB. The default chunk is therefore the largest response payload that fits after the fixed 94-byte response header. Callers may request smaller chunks, but should prefer the default for throughput: the current synchronous Outbox visitor validates and reads the complete durable record before copying the requested slice. Continuations are bounded and never retain the borrowed `OutboxEntryView` beyond its visitor callback.

The consumer persists the downstream result before calling `AcknowledgeOutboxThroughAsync`. The lease-fenced Outbox ACK advances only the synchronization Outbox watermark; it does not retire local IPC operation receipts. Conversely, `AcknowledgeThroughAsync` does not acknowledge the synchronization Outbox. Keep these two acknowledgements independent.

## Commit and Outbox semantics

A WriteBatch is applied to the private Database overlay while the commit lock is held, then committed together with its receipt as one GROUP_COMMIT/WAL record. The receipt's CommitSeq is the durable commit sequence returned to the client. The regular FLD Outbox is populated from the local field mutations once; the IPC receipt is not a second change journal.

ReadField is serialized under the same Database commit lock. Its encoded found/value response is committed as a receipt-only operation, so retry returns the original read result even if the field changes later. A receipt-only read does not fabricate a local field mutation or Outbox entry.

AcknowledgeReceipts advances the client's durable contiguous receipt watermark. It does not acknowledge or compact the synchronization Outbox. The two mechanisms have different consumers and retention semantics.

## Host GC reference-scan IPC (implemented)

Physical-GC clients first acquire the exclusive Host-wide GC lease (0x2118). While
that authenticated IPC session owns the lease, it may open one immutable reference
index (0x211A), issue bounded exact-identity queries (0x211B), and release the index
(0x211C). The index captures the unpublished local draft horizon, unsealed-WAL
barrier, active/persisted provider snapshot-pin state, Outbox identities and
ApplyJournal field/receipt references in one Host commit-lock snapshot.

Scan tokens are bound to both ClientId and the live IPC session generation. A token
from another client/session, an unknown token, or a query without the owning GC lease
is rejected. Receipt queries are scoped to the Database Host's configured namespace.
The caller must release the scan before releasing the GC lease. If the client
disconnects or the Host stops, the dispatcher disposes every scan and releases the
exclusive boundary; tokens are process-local and are never resumed after restart.

The Host GC lease excludes other IPC sessions for the entire reference-scan and
metadata-compaction interval, while allowing the lease owner to issue scan queries.
The reference index is immutable and query operations do not rescan WAL, Outbox or
ApplyJournal per candidate. A positive query means the exact identity remains
referenced; callers must fail closed on any IPC error or uncertain completion.
This mechanism supplies reference evidence, not a retention horizon: signed roster,
frontier, checkpoint and feature-flag checks remain independently mandatory.

## Snapshot IPC (implemented)

The current branch already exposes immutable provider snapshots through operations 0x2108–0x210B. The snapshot-open operation id is the idempotency key: an uncertain retry with the same (ClientId, OperationId) returns the same live handle on the same Host generation. A released handle cannot be reopened with the same operation id.

The returned handle binds HostGeneration and SnapshotToken; every page, value-chunk and release request must present both. Handles are client-bound, host-generation-bound and capped at 64 per Host / 4 per Client. Host disposal releases every remaining handle. Metadata pages are bounded to 256 entries; values are read separately in bounded chunks. The immutable snapshot generation and its source-feed watermark are captured at open, so page traversal cannot drift as live writes arrive.

The host creates the snapshot from its provider-snapshot mechanism; IPC does not expose raw FldDatabase or borrowed storage views. Consumers must release handles in finally and discard handles after a Host-generation change.

**Important lifetime boundary:** the IPC handle and the `(ClientId, OpenOperationId) → SnapshotToken` replay map are owned by the live `FldDatabaseIpcDispatcher`, not by a durable IPC handle registry. Dispatcher disposal releases all remaining handles and clears the replay map; a new dispatcher receives a new HostGeneration. Therefore a handle is reconnect-safe only while the same Host generation remains alive. It is not a restart-resumable token, and replaying the same OpenOperationId after Host restart can open a new snapshot at a new feed watermark. The client must detect HostGeneration changes, discard the old token/cursor, and restart snapshot enumeration from a newly opened snapshot. Do not advertise IPC snapshot open as exactly-once across Host restart. This is distinct from durable local write receipts, which are stored in FLD WAL and survive Host restart.

## Provider current-state reads and feed watermark (point read + snapshot; implemented)

The IPC facade does not need a separate high-watermark operation for a provider snapshot. `OpenSnapshotAsync` returns `SourceFeedWatermark`, captured by `FldDatabase.OpenProviderSnapshotForIpc` from the same immutable generation as the snapshot index and overlay. This is the source-feed ceiling that a snapshot-based provider must use; it must not fetch a later live Outbox watermark and combine it with an older snapshot.

For ordinary provider point reads, `ReadCurrentMetadata` (0x210F) resolves exactly one RecordId/FieldId through the existing indexed LWW resolver across the mutable overlay and immutable Runs. It returns value length, HLC, AuthorNodeId, KeyEpoch, tombstone state, ChangeId, OriginAuthorEpoch and OriginSequence. It does not scan or materialize the complete snapshot and does not create a WAL receipt for a read-only query.

`ReadCurrentValueChunk` (0x2110) requires the ChangeId returned by the metadata query. Before copying each bounded value slice, the Host resolves the field again and compares its current ChangeId. It returns an explicit Changed status if the field was superseded between metadata and chunk requests; callers must restart from metadata rather than assemble bytes from different versions. Missing fields are reported separately. Each request takes only the ordinary short database read lock; no lock or borrowed view is held across IPC calls. Tombstones are returned as metadata with zero value length.

Snapshot pages still return metadata for every visible field winner, including record-lifecycle tombstone state, and snapshot value chunks read from that same pinned generation. Use snapshots when a stable whole-database view is required; use the addressed operations for point reads. The existing snapshot handle already carries the source-feed watermark; do not combine it with a later live Outbox watermark or add another redundant watermark RPC.

The remaining material provider gaps are published-origin stamping, durable author-epoch/publication-mode operations, full-replace/snapshot-state capabilities, durable async remote-apply mapping, and a cross-call async commit barrier.

## Identity-preserving local provider writes (0x210D; implemented)

The generic WriteBatch operation is intended for ordinary local database writes; it generates local HLC and ChangeId inside FLD. A BIN-SYNC provider draft must instead preserve the caller's stable ChangeId, HLC and active OriginAuthorEpoch. AppendLocalProviderBatch provides that separate atomic boundary.

The versioned payload begins with Version (u8 = 1), ClientSequence (u64) and mutation count (u32). Each mutation is RecordId (16 B), FieldId (u16), KeyEpoch (u16), IsTombstone (u8), OriginAuthorEpoch (u32), Wall (u64), Logical (u32), ChangeId (16 B), ValueLength (u32), Value bytes. All integers are little-endian. OriginAuthorEpoch and Wall must be nonzero; ChangeId and RecordId must be nonzero; tombstones have an empty value; duplicate RecordId/FieldId pairs and duplicate ChangeIds in one operation are rejected. OriginSequence is deliberately not transmitted: it remains zero until the BIN-SYNC publication freeze assigns the immutable origin sequence.

The Host supplies its own LocalAuthorNodeId to PutWithIdentity; the IPC caller cannot choose the author identity. The operation commits all mutations and the ordinary local Outbox entries together with one durable local IPC receipt in one WAL GROUP_COMMIT. An exact retry returns the original CommitSeq/result and does not append a second Outbox entry. The receipt sequence remains contiguous per ClientId and is acknowledged independently from the Outbox.

### Auto-sequenced local provider append (0x2111; implemented)

The regular 0x210D operation remains available when a caller durably manages its own ClientSequence. The 0x2111 variant uses the same canonical provider-mutation payload but requires the encoded ClientSequence field to be zero. The Database Host assigns the next contiguous sequence for the stable ClientId while holding the same commit lock that checks the operation receipt and commits the local mutations.

The caller supplies a stable, nonzero OperationId. The IPC client adapter derives it from the immutable ChangeId for a single-mutation provider append. The durable identity is the existing (ClientId, OperationId, request fingerprint) receipt; no second client-side sequence journal or pending table is introduced. An exact retry returns the original CommitSeq/result, including after Database Host restart. Reusing the same identity with different payload bytes fails closed. The caller must keep ClientId stable across reconnects and must not retry an operation after its receipt has been acknowledged/retired.

This variant closes the response-loss window without asking an async provider to persist a second sequence allocator. It does not change Outbox sequencing: OutboxSeq remains the local feed coordinate, not OriginSequence. OriginSequence is still assigned only at BIN-SYNC publication freeze.

### Published-origin stamping (0x2114; implemented)

`CommitPublishedOrigins` applies a bounded batch of immutable publication stamps through the existing FLD identity-stamp WAL record. Each item is bound to `(RecordId, FieldId, ChangeId, OriginAuthorEpoch, OriginSequence)`. If the field has already been superseded by another ChangeId, the stale stamp is ignored; an exact repeat is idempotent; a conflicting origin for the same current ChangeId fails closed.

The operation does not append an Outbox row, change field value/HLC, or create a second pending journal. It updates only the current-state identity projection. The IPC call itself is asynchronous and is exposed by the sample's `IAsyncProviderPublishedOriginStore` capability. This operation alone does not create a cross-store transaction with BIN-SYNC Metadata Store; the async composed commit barrier remains a separate coordinator gate.

### Metadata-Store-owned publication cutover (0x2115–0x2116; implemented)

The cutover marker is stored durably in the FLD storage-core keyspace under a namespace-scoped key. It is an immutable single watermark, not a second operation journal. `TryGetMetadataStoreOwnedPublicationCutover` returns whether the marker exists and the Node Outbox high-watermark captured by the coordinator. `EnableMetadataStoreOwnedPublication` is idempotent for the same watermark and rejects a conflicting value.

The Host permits first-time enablement only when the FLD local draft horizon is empty and the provider Outbox high-watermark equals its acknowledged-through cursor. This prevents switching publication authority while local changes remain unpublished. The marker survives Host restart and is exposed through the sample `IAsyncProviderPublicationMode` capability. It does not implement author-epoch closure, FULL_REPLACE, GC, or a cross-store transaction; those remain separate capabilities.

### Host identity discovery (0x2113; implemented)

GetLocalAuthor returns the Database Host's configured nonzero LocalAuthor identity. An IPC-backed provider must compare it with the BIN-SYNC local replica identity before accepting local drafts. The Host remains authoritative when writing provider mutations; the IPC caller cannot select or override LocalAuthor.
## Contextual remote APPLY IPC (0x210C; implemented)

The Database facade exposes the existing FldDatabase.ApplyRemoteBatch(RemoteBatchCommit) transaction without translating it into local WriteBatch operations. Local writes and remote applies have different authorship, Outbox and durability semantics.

The request is a bounded, versioned serialization of the complete RemoteBatchCommit:

- sender peer (16 B), sender transport epoch (u32), namespace id (u16);
- issuer node id (16 B), issuer epoch (u32), batch id (16 B);
- cursor start/end (u64 each), previous batch digest, batch digest and records root (16 B each, matching RemoteBatchCommit identity widths);
- opaque ProtocolStateDelta (u32 length + bytes);
- Writes (u32 count; each entry is RecordId (16 B), FieldId (u16), Wall (u64), Logical (u32), AuthorNodeId (16 B), ChangeId (16 B), KeyEpoch (u16), tombstone flag (u8), OriginAuthorEpoch (u32), OriginSequence (u64), value length (u32), value bytes);
- RelayWrites as a bounded list of u32 indices into Writes, not duplicated field payloads. The decoder must reject duplicate/out-of-range indices and the constructor must still enforce exact subset semantics.

The response returns CommitSeq (u64), CursorStart/End (u64 each), BatchId (16 B), AppliedCount (u32), IsDuplicate (u8) and the exact opaque ProtocolStateDelta (u32 length + bytes). A duplicate batch returns the original durable receipt/delta; conflicting metadata for an already committed issuer/epoch/namespace/batch id fails closed.

The dispatcher calls ApplyRemoteBatch exactly once. That method owns the atomic WAL APPLY_COMMIT, apply-journal materialization, protocol delta, relay journal and overlay update. It must not create local-author Outbox entries, local IPC operation receipts, or a second pending journal. The IPC envelope OperationId is transport/request correlation only; durable remote idempotency remains (NamespaceId, IssuerNodeId, IssuerEpoch, BatchId) as enforced by ApplyJournal.

The full encoded request and response must fit the Storage IPC frame limit before allocation/transmission. Reject oversized batches before copying; do not silently split one atomic remote batch across IPC calls. If a valid BIN-SYNC batch cannot fit, define an explicit future streaming/staging transaction with begin/chunk/commit/abort and crash cleanup; never approximate it with several independent APPLY commits. The host must verify namespace equality and all RemoteBatchCommit invariants before WAL mutation.

## Current facade boundary

Implemented in the current facade:

- atomic WriteBatch with durable local operation receipt and contiguous-prefix receipt acknowledgement;
- identity-preserving local provider draft batches with stable ChangeId/HLC/OriginAuthorEpoch and atomic Outbox + receipt commit, including Host-assigned receipt sequencing for retry-safe async callers;
- authoritative Host LocalAuthor discovery for IPC client identity validation;
- idempotent ChangeId-bound publication-origin stamping in the current-state projection;
- immutable namespace-scoped Metadata-Store-owned publication cutover, guarded by an empty local draft horizon and fully acknowledged provider Outbox;
- receipt-backed ReadField;
- lease-fenced Outbox acquisition, bounded entry chunks, durable Outbox high-watermark query, Outbox ACK and release;
- immutable provider snapshots over IPC: idempotent open within one live Host generation, bounded metadata pages/value chunks, client/Host-generation fencing and release (handles are not restart-resumable);
- addressed current-field metadata reads and ChangeId-fenced bounded value chunks; read requests do not create durable receipts or WAL records;
- retry/recovery across Host restart for durable local write receipts and Outbox lease epoch fencing; IPC snapshot handles themselves are invalidated on Host restart;
- single-owner Host endpoint publication.

Not exposed by this facade yet:

- author-epoch closure, FULL_REPLACE and snapshot-state capability operations;
- subscriptions or streaming change feeds;
- multi-entry Outbox aggregation beyond the one-entry chunk cursor;
- a complete asynchronous synchronization coordinator and cross-call commit-lease composition. The sample async provider and contextual remote-apply adapter are implemented in BIN-SYNC, but author-epoch closure, snapshot-state, FULL_REPLACE and GC capabilities remain unavailable.

Do not emulate remote APPLY or missing provider capabilities by issuing separate Put/Commit requests: that would lose remote authorship/causality and expose intermediate state. They require explicit facade operations and their own durable contracts. The synchronous BIN-SYNC IDataProvider contract must not call this async client through sync-over-async; a separate async provider/coordinator boundary is required before production process isolation.
