# FLD Run payloads in mapped Storage blobs

**Status:** implemented on `feature/run-blob-single-file-mapping`; awaiting Storage index scaling benchmark and merge into `fix/crash-consistency-outbox`.

## Ownership and physical layout

The Storage catalog is authoritative for immutable Run payload bytes. New blobs use metadata format v2 and a single contiguous `content.blob` file under the blob directory. The catalog retains the blob length, page geometry and SHA-256 digest; page geometry remains available for bounded range reads.

New checkpoint, compaction and FULL_REPLACE publication paths write the immutable Run payload to Storage only. They do not create a persistent `.fld1` compatibility mirror.

The FLD Database manifest continues to own Run identity/order and the Run reader continues to own FLD field semantics. This cutover changes payload ownership and mapping, not the manifest or Run wire/on-disk format.

## Compatibility and migration

- Metadata v1 page-based blobs remain readable.
- A page-based blob is migrated by streaming its pages into a temporary contiguous file using a pooled 64 KiB buffer.
- The new file is flushed and its full digest/length are verified before atomic publication and catalog switch to metadata v2.
- Old page files are removed only after the catalog switch. If a crash interrupts cleanup, a retry verifies the authoritative file before deleting leftover pages.
- Existing snapshot pins fence page cleanup. A retry converges after the last old reader releases its pin.
- A legacy `.fld1` file remains a read-only fallback only when the Storage blob is absent.
- When a Storage blob exists, FLD maps it and verifies both the blob digest and Run header/namespace before deleting any legacy mirror. Corrupt or missing blob content therefore does not cause the last legacy mirror to be discarded.

## Mapping lifetime

`StorageBlobReader.OpenMappedView()` creates a read-only memory mapping over the contiguous file and transfers the reader's immutable snapshot pin to the returned mapped view. The view releases the pin only when disposed.

The FLD Run mapping owner retains this view for as long as active or retired Run spans may be referenced. Compaction can retire a mapping without invalidating outstanding spans. Blob deletion remains deferred while any mapping pin is active; orphan reclamation converges on a later open after those pins drain.

## Allocation and I/O impact

- Removes the persistent second copy of each newly published Run.
- Removes the full managed `byte[]` materialization previously used by FULL_REPLACE pre-switch Run validation.
- Run lookup maps the authoritative Storage blob directly; it does not recreate a mirror.
- Legacy page-to-file migration streams through a pooled buffer rather than allocating a whole-Run array.

This is not a claim of a bounded process working set: memory-mapped pages can still become resident in the OS page cache. Peak RSS and out-of-core cache budgets remain separate release gates.

## Validation

The implementation adds tests for:

- metadata v1 page-blob migration to v2;
- interrupted page cleanup and retry convergence;
- page cleanup waiting for active snapshot pins;
- mapped view digest verification and pin lifetime;
- legacy Run fallback when no Storage blob exists;
- preserving a legacy mirror when contiguous blob validation fails;
- checkpoint, compaction and FULL_REPLACE without new `.fld1` mirrors.

Exact-head .NET CI on `30881891a6083d6300e1fdd81a129e0fe2e14022` passed Windows, Ubuntu and macOS. The independent Storage index scaling benchmark was still running at this checkpoint.

## Remaining gates

1. Finish the Storage index scaling benchmark on the final PR head.
2. Merge only into `fix/crash-consistency-outbox`, never `main`.
3. Run the existing Run Mapping Benchmark on the merged working branch and compare cold open/first lookup, warm lookup and allocations against the prior baseline.
4. Sync the exact `Fld1.Storage` source and metadata manifest into BIN-SYNC's `feature/fld-external-storage`, then pass BinSync.Net and Storage Integration CI.
5. Keep Windows power-loss evidence, peak RSS, long-history scale and staged canary as independent release gates.
