# ADR: FLD provider snapshot provenance and FLD1 Run v2

**Status:** Accepted for implementation (C-pass). This ADR authorizes a versioned, backward-readable FLD Run extension and WAL payload extensions. It does not authorize replacing the FLD field engine with the generic Storage page/run backend; that remains a separate physical-engine decision under `FLD-STORAGE-CORE-ARCHITECTURE.md` §7.

## Context and confirmed loss boundary

The current FLD Database has stable `ChangeId` values in `MemFieldValue`, GROUP_COMMIT body v2 and APPLY_COMMIT body v3. Local Commit also materializes the identity into the durable FLD Outbox. This is sufficient while the operation remains in overlay/WAL/Outbox.

The current FLD1 Run v1 is a field-state projection. `RunBuilder.FieldWrite` and `RunReader.RunFieldState` preserve RecordId, FieldId, value/tombstone, HLC, author and key epoch, but not ChangeId or immutable `OperationOrigin`. `CheckpointUnderLock` and `CompactUnderLock` therefore discard current-winner provenance when they rewrite the Run. Outbox compaction can later remove the acknowledged operation row; WAL and ApplyJournal payloads can also be reclaimed. At that point the current field value remains readable but its original ChangeId/Origin cannot be reconstructed. Generating a replacement ChangeId during snapshot is forbidden.

This is a correctness blocker for `IDataProvider.TryReadCurrent`, generation-pinned FULL_REPLACE snapshots and the cross-store reference proof. A point-in-time value snapshot alone is not enough.

## Decision

### 1. Versioned Run provenance section

Introduce FLD1 Run binary version 2 while retaining the existing v1 reader/import path.

- Run v2 keeps the existing field cluster, PGM, value and footer semantics.
- A separately bounded, sorted per-field provenance section stores `RecordId[16] | FieldId[u16] | ChangeId[16] | OriginAuthorEpoch[u32] | OriginSequence[u64]`.
- The section is ordered by unsigned RecordId bytes, then FieldId; duplicate keys, malformed lengths, zero ChangeId in a non-legacy row, and half-populated origin tuples are rejected.
- Existing Run v1 files remain readable. A v1 field has legacy/materialized identity (zero ChangeId, epoch and sequence); no synthetic identity is assigned. Such rows may be included as legacy materialized snapshot state where the provider contract permits it, but they are not eligible as immutable outbound feed entries or as proof that a ChangeId is no longer referenced.
- New checkpoints/compactions emit v2. A compaction can upgrade v1 rows only as legacy rows; it cannot invent their lost source identity.

### 2. Durable WAL origin fields and post-freeze stamping

- Extend GROUP_COMMIT and APPLY_COMMIT field records with `OriginAuthorEpoch` and `OriginSequence`, using new body versions and retaining decoders for existing body versions.
- Local provider drafts carry a nonzero active author epoch and a deferred zero OriginSequence. BIN-SYNC assigns the immutable Origin only during outbound freeze.
- Add a dedicated WAL identity-stamp record for `ChangeId → OperationOrigin`. It updates identity metadata only when the current field winner still has the same ChangeId; a stale stamp for an already superseded value is an idempotent no-op. It never appends a second Outbox operation or changes the field's HLC/value.
- Replay applies stamps after the preceding field writes and before sealing. Checkpoint persists the stamp in Run v2. A crash after freeze but before provider-feed ACK replays the same stamp and the same ChangeId/Origin.
- Conflicting stamps for one ChangeId fail closed. Exact repeats are idempotent.

### 3. Outbox and retention ownership

- FLD Outbox remains the sole local unpublished-change feed. It owns ChangeId and local feed sequence; it is not a second pending journal.
- BIN-SYNC Metadata Store remains the sole post-cutover delivery/peer-ACK/causality authority. FLD ApplyJournal remains the pre-cutover remote-apply receipt/relay source and may compact only under its existing safe-ACK/checkpoint rules.
- The current-state Run provenance section is a projection of the winning field operation, not a duplicate relay payload history. The WAL identity-stamp record is the durable transition needed to project the immutable Origin into that current-state record.
- No physical receipt, ChangeId, tombstone or Ghost GC is enabled by this ADR. Their three independent safe horizons still require the full v3.1 T-pass.

### 4. Generation-pinned provider snapshot

Implement an FLD snapshot handle that captures one immutable run/overlay generation under `FldDatabase.AcquireCommitGuard`. It must:
- provide stable ordered, bounded pages of current field winners;
- preserve ChangeId and Origin for v2/current rows, and explicitly mark v1 rows as legacy materialized;
- pin the referenced immutable run generation so compaction cannot delete a run while a snapshot is using it;
- release pins exactly once on snapshot release, cancellation, failed open and database disposal;
- avoid copying all field values into one in-memory list. An O(keys) compact index is acceptable as an initial implementation; page payloads must be materialized only for the requested page.

A later optimization may replace the run index with a generic Storage page cursor, but must preserve this snapshot API and its generation lifetime.

### 5. Compatibility and rollout

1. Readers accept Run v1 and v2; writers emit v2 only after the v2 codec and recovery path are complete.
2. WAL readers accept the current legacy bodies plus the new origin-bearing bodies; writers emit the new body version after all replay paths understand it.
3. Mixed directories containing v1 and v2 runs are valid during rolling migration.
4. Existing v1 winners remain legacy materialized. They are never assigned synthetic ChangeIds. A full source resynchronization may replace them with newly received operations, after which v2 provenance is available.
5. The database still ships exactly two FLD runtime DLLs: `Fld1.Storage.dll` and `Fld1.Database.dll`. This ADR introduces no third runtime assembly and no FLD→BIN-SYNC reference.

## Required implementation slices

- [x] WAL body versioning for origin fields with legacy decode (GROUP_COMMIT v3; APPLY_COMMIT v4; v1/v2 and v2/v3 readers retained).
- [x] Identity-stamp WAL record, replay and idempotency/conflict validation; `FldDatabase.CommitPublishedOrigins` writes identity-only records and never duplicates Outbox payload.
- [x] Run v2 provenance section writer/reader/verifier and v1 compatibility; v1 remains a legacy identity-zero row.
- [x] Propagate origin through `MemFieldValue`, remote APPLY, WAL replay, checkpoint, compaction and LWW resolution.
- [x] Generation-pinned, paged FLD provider snapshot: captures immutable RunReader buffers + overlay generation, uses an O(keys) compact RecordId index and bounded page payload materialization, preserves v2 ChangeId/Origin and v1 identity-zero rows, and releases retained buffers exactly once on handle disposal or database disposal.
- [x] Optional host `IDataProvider` adapter for direct RecordId/FieldId resolution, local Outbox feed, published-origin commit and current-state reads; the exact contextual `FldRemoteBatchApplyStore` is composed separately so the full remote receipt context is retained.
- [ ] Exact-head build plus crash/restart, mixed-v1/v2, cutover and identity-retention T-pass.

This ADR is a C-pass implementation decision, not a claim that any of these code gates have already passed.

## 6. IPC snapshot handle contract — 2026-11-11

The Database IPC facade exposes the existing generation-pinned provider snapshot; it does not create a second snapshot engine or copy the whole namespace per client.

- OpenSnapshot creates one durable provider snapshot. An uncertain open retry returns the same live handle within one host generation by (ClientId, OperationId); callers use a fresh operation id after releasing that handle.
- The returned opaque handle contains HostGeneration, SnapshotToken, SourceFeedWatermark and RecordCount. The token maps to the underlying FldProviderSnapshot.Generation.
- Every page, value-chunk and release request carries the handle. The dispatcher binds it to the connection-stable IPC ClientId from the transport request context and its own process-lifetime HostGeneration. A different client cannot use the handle.
- HostGeneration is stable across client reconnects to the same dispatcher and changes when the Database host process/dispatcher restarts. SessionGeneration is per connection and is deliberately not used as the snapshot lifetime.
- Host restart invalidates IPC handles; they are not restored. Each IPC snapshot writes an atomic ipc.handle descriptor (SnapshotGeneration, ClientId, HostGeneration) before the provider manifest is published. After acquiring the exclusive Database root lease, the host creates its new dispatcher generation and calls PruneStaleIpcProviderSnapshots before publishing the IPC endpoint. This removes only stale IPC-owned generations and incomplete unpublished directories; non-IPC provider snapshots, including BIN-SYNC FULL_REPLACE generations, are preserved.
- Graceful dispatcher disposal releases every active IPC snapshot. Abrupt process termination leaves the durable provider manifest in place until the next host acquires the root lease and prunes stale generations.
- Snapshot pages are metadata-only and capped at 256 entries. Field values are fetched separately by (RecordId, FieldId, ValueOffset) in bounded chunks. This keeps page responses independent of value size and never materializes a page's full value payloads.
- The provider cursor retry cache is keyed by (afterIndex, limit, includeValues). A retry of the same page is byte-identical; a different page limit recomputes from the requested absolute entry offset.
- Limits are four active snapshots per client and 64 per dispatcher. Release is idempotent. The first implementation deliberately has no idle timeout; clients must release handles, and the host restart is the crash-recovery boundary.
- The public API remains in Fld1.Database.dll; no third FLD runtime DLL and no FLD-to-BIN-SYNC dependency are introduced.

Required IPC tests: metadata-only pages, chunk boundaries, same-cursor/different-limit retry, snapshot stability across writes/checkpoint/compaction, open-operation retry, cross-client rejection, same-client reconnect, idempotent release, and host-restart invalidation/pruning.

