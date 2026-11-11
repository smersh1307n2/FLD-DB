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
- a nonzero ClientSequence, contiguous per ClientId;
- a canonical request fingerprint (SHA-256 over the little-endian operation code followed by the exact operation payload);
- an opaque result retained in the same durable GROUP_COMMIT as the field mutations, or in a receipt-only GROUP_COMMIT for reads.

The caller must persist OperationId and ClientSequence before transmission. If a connection is lost after the Host commits but before the caller receives the response, the caller resends the same OperationId, ClientSequence and exact request bytes. The Host returns the original CommitSeq/result and does not stage or publish the field changes again. Reusing an identity with different bytes, skipping a sequence, or retrying a sequence at/below the durable acknowledgement watermark fails closed.

The client must durably persist a successful response before acknowledging it. Acknowledgement advances only a contiguous prefix. The Host persists the watermark in WAL before retiring the acknowledged operation results; replay after restart preserves both the watermark and the next expected sequence. Outbox ACK/compaction is separate and must not be used as a substitute for IPC receipt acknowledgement.

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

## Snapshot IPC (implemented)

The current branch already exposes immutable provider snapshots through operations 0x2108–0x210B. The snapshot-open operation id is the idempotency key: an uncertain retry with the same (ClientId, OperationId) returns the same live handle on the same Host generation. A released handle cannot be reopened with the same operation id.

The returned handle binds HostGeneration and SnapshotToken; every page, value-chunk and release request must present both. Handles are client-bound, host-generation-bound and capped at 64 per Host / 4 per Client. Host disposal releases every remaining handle. Metadata pages are bounded to 256 entries; values are read separately in bounded chunks. The immutable snapshot generation and its source-feed watermark are captured at open, so page traversal cannot drift as live writes arrive.

The host creates the snapshot from its persisted provider-snapshot mechanism; IPC does not expose raw FldDatabase or borrowed storage views. Consumers must release handles in finally and discard handles after a Host-generation change.

## Identity-preserving local provider writes (0x210D; implemented)

The generic WriteBatch operation is intended for ordinary local database writes; it generates local HLC and ChangeId inside FLD. A BIN-SYNC provider draft must instead preserve the caller's stable ChangeId, HLC and active OriginAuthorEpoch. AppendLocalProviderBatch provides that separate atomic boundary.

The payload begins with ClientSequence (u64) and mutation count (u32). Each mutation is RecordId (16 B), FieldId (u16), KeyEpoch (u16), IsTombstone (u8), OriginAuthorEpoch (u32), Wall (u64), Logical (u32), ChangeId (16 B), ValueLength (u32), Value bytes. All integers are little-endian. OriginAuthorEpoch and Wall must be nonzero; ChangeId and RecordId must be nonzero; tombstones have an empty value; duplicate RecordId/FieldId pairs and duplicate ChangeIds in one operation are rejected. OriginSequence is deliberately not transmitted: it remains zero until the BIN-SYNC publication freeze assigns the immutable origin sequence.

The Host supplies its own LocalAuthorNodeId to PutWithIdentity; the IPC caller cannot choose the author identity. The operation commits all mutations and the ordinary local Outbox entries together with one durable local IPC receipt in one WAL GROUP_COMMIT. An exact retry returns the original CommitSeq/result and does not append a second Outbox entry. The receipt sequence remains contiguous per ClientId and is acknowledged independently from the Outbox.

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
- identity-preserving local provider draft batches with stable ChangeId/HLC/OriginAuthorEpoch and atomic Outbox + receipt commit;
- receipt-backed ReadField;
- lease-fenced Outbox acquisition, bounded entry chunks, Outbox ACK and release;
- immutable persisted provider snapshots over IPC: idempotent open, bounded metadata pages, bounded value chunks, client/Host-generation fencing and release;
- retry and recovery across Host restart, including Outbox lease epoch fencing;
- single-owner Host endpoint publication.

Not exposed by this facade yet:

- contextual remote APPLY (0x210C): versioned, bounded codec and client/dispatcher route to the existing atomic ApplyRemoteBatch WAL transaction;
- subscriptions or streaming change feeds;
- multi-entry Outbox aggregation beyond the one-entry chunk cursor.

Do not emulate remote APPLY or missing streaming operations by issuing separate Put/Commit requests: that would lose remote authorship/causality and expose intermediate state. They require explicit facade operations and their own durable contracts.
