# FLD Storage Physical Format Decision

**Status:** Accepted architecture decision; implementation is incremental and not yet complete.  
**Date:** 2026-11-09  
**Runtime boundary:** exactly two DLLs: `Fld1.Storage.dll` and `Fld1.Database.dll`.

## Decision

Choose **Option B: a generic opaque page/blob durability engine in Storage, with the FLD1 Run/Manifest/WAL codecs and field semantics owned by Database**.

Do not replace FLD1 Run v1/v2 with a new field-keyed Storage KV schema in this pass. Do not move `RunBuilder`, `RunReader`, FLD manifest entries, HLC/LWW, lifecycle, PGM, author or ChangeId semantics into Storage.

The generic Storage layer owns:
- opaque immutable pages/blobs addressed by caller-supplied stable IDs;
- bounded page writes and ordered page reads, with page payloads in immutable files and only the bounded catalog/publication metadata in StorageDatabase/WAL;
- per-blob length/page-count/digest metadata and atomic publication marker;
- generic WAL durability, snapshot/checkpoint and storage-native CommitSeq;
- snapshot pin lifetime and explicit bounded deletion/orphan-page collection.

The Database layer owns:
- the canonical FLD1 Run v1/v2 byte encoding and verification;
- RecordId/FieldId/HLC/author/KeyEpoch/ChangeId/Origin and tombstone semantics;
- the FLD manifest/catalog mapping each namespace-scoped RunId to an opaque blob ID derived from the root-global RunId;
- FLD WAL commit payloads, replay, Outbox, ApplyJournal and compaction policy.

A Run is an opaque blob to Storage. Storage must never parse its FLD1 header, cluster, FieldId, HLC, tombstone or provenance section. The existing FLD1 Run bytes remain unchanged inside the blob. The Database manifest remains the logical namespace-scoped Run catalog; because RunId allocation is root-global, the opaque blob ID is deterministically derived from RunId alone, so no field-aware lookup leaks into Storage.

## Why B, not A

1. FLD1 Run v1/v2 already contains the required field-aware sorted clusters, tombstone representation, HLC/author ordering and Run v2 ChangeId/Origin provenance. Replacing it with opaque key/value rows would be a second database-format redesign, not merely a Storage extraction.
2. The current StorageDatabase is a generic atomic byte KV with WAL and snapshot checkpoints, but its in-memory ordered snapshot index is O(number of keys) and each WAL commit is bounded by the generic WAL payload limit. It is suitable as the generic durability/catalog substrate, but large Run payloads need chunked immutable blobs rather than one giant KV value.
3. Preserving FLD1 Run bytes keeps legacy readers, existing manifest semantics and Run v1/v2 compatibility. A new physical blob container can be introduced without changing the FLD field format.
4. The manifest remains the publication point: write and durably complete all opaque blob pages first, then atomically publish the FLD manifest that references the Run IDs. A crash before manifest publication leaves unreferenced blobs; a crash after publication must find every referenced blob complete. Old blobs are reclaimed only after manifest and provider-snapshot pins prove they are no longer live.

## Required blob contract

- Blob IDs are stable 128-bit opaque identifiers; FLD derives them deterministically from the root-global monotonic RunId (which is unique across namespaces in one FLD root). The Storage API accepts only the opaque ID and bytes.
- Default page size is 1 MiB; page size is configurable and persisted per blob. Readers and interrupted-write resume always use the stored page geometry, independent of a later process's configured default. Each page and each Storage catalog write batch must stay below the generic WAL record limit.
- A blob is invisible to readers until every page file is individually flushed, the page directory is flushed once after all page renames, and a complete metadata record (length, page size/count, SHA-256) is atomically published. The pending catalog row remains until the directory flush succeeds, so interrupted writes are resumable without a per-page directory-fsync cost.
- Repeated Put with the same ID and identical content is idempotent. Reuse of an ID with different length/digest/page bytes fails closed.
- An interrupted pending blob is resumable with the same ID/content; it is never returned by OpenRead until complete.
- A reader owns a stable Storage catalog snapshot plus an in-process page-directory pin and validates page count, exact final-page length and whole-blob SHA-256 when streamed.
- Delete first commits removal of the publication marker, then reclaims page files. Readers already opened keep their page-directory pin; a pinned blob cannot be deleted. If physical cleanup is interrupted after catalog deletion, bounded orphan-directory collection retries it. The logical visibility change is atomic; physical page reclamation is deliberately separate.
- On Linux/macOS, page-file rename and newly created directory entries are flushed through the shared native `StorageDirectorySync`; an fsync error before blob completion fails closed. Windows directory-entry durability is not claimed. StorageDatabase WAL still supplies the durable catalog commit sequence.
- Storage APIs remain byte/Guid based and contain no FLD or BIN-SYNC types.

## Migration sequence

1. Add and independently validate the generic Storage immutable-blob/page API in `Fld1.Storage`. The first implementation stores large immutable page payloads as fsynced files and keeps only the compact blob catalog in StorageDatabase/WAL, avoiding a large in-memory KV index.
2. Add the FLD Database adapter that maps each namespace-scoped manifest Run entry to an opaque blob ID derived from the root-global RunId; retain legacy `runs/{RunId}.fld1` reads as fallback.
3. New Run writes publish blob pages before manifest update. Checkpoint, compaction and FULL_REPLACE staging use the same root-global Run ID allocator; it checks both legacy Run files and the blob catalog so uncommitted staged IDs cannot be reused after restart. During the compatibility period, the exact same FLD1 Run bytes are also materialized at `runs/{RunId}.fld1`; on Linux/macOS the shared native directory-sync helper flushes the mirror rename before the manifest/stage reference can publish, and a sync error fails closed. The manifest remains the single FLD visibility switch, and old FLD1 binaries can still read the mirror. Windows directory-entry durability remains unclaimed. New code prefers the generic blob and falls back to the legacy file only when no blob record exists. Existing legacy Run files are imported lazily or during compaction; no in-place rewrite of a live manifest. Removing the compatibility mirror requires a separate explicit manifest/storage-format version transition and benchmark.
4. Extend orphan collection to union active manifest Run IDs, durable provider snapshot Run pins and incomplete staging generations before deleting either legacy files or Storage blobs.
5. After Run storage is migrated and proven, separately evaluate moving FLD WAL records to generic Storage WAL as opaque Database-owned commit payloads. Do not combine that migration with the Run/blob cutover.
6. Benchmark write amplification, point/range reads, checkpoint time, peak memory and on-disk size against the current legacy Run files. Do not claim an optimization unless the full workload improves.

## Compatibility and non-goals

- No third Common/Shared runtime DLL.
- No FLD→BIN-SYNC dependency.
- No BIN-SYNC protocol types in Storage.
- No change to Run v1/v2 bytes or existing field/tombstone semantics.
- No silent automatic conversion of an existing database.
- No weakening of the single-owner partition fence.
- Shared multi-application Engine/IPC mode remains a separate requirement and is not implied by this blob API.
- This decision does not authorize physical receipt, ChangeId or tombstone/Ghost GC; all such GC flags remain disabled until v3.1 proofs and exact-head T-pass validation.
