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

## Current facade boundary

Implemented in the current facade:

- atomic WriteBatch with durable operation receipt;
- receipt-backed ReadField;
- contiguous-prefix receipt acknowledgement;
- lease-fenced Outbox acquisition, bounded entry chunks, Outbox ACK and release;
- retry and recovery across Host restart, including Outbox lease epoch fencing;
- single-owner Host endpoint publication.

Not exposed by this facade yet:

- stable pinned/paged Database snapshots and snapshot release;
- contextual remote APPLY;
- subscriptions or streaming change feeds;
- multi-entry batch aggregation beyond the one-entry chunk cursor.

Do not emulate these missing operations by issuing separate Put/Commit requests: that would expose intermediate overlay state and violate atomic batch semantics. They require explicit facade operations and their own durable contracts.
