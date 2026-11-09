# FLD Storage Core — extraction and dependency architecture

**Status:** Target architecture decision v0.2 — two physical runtime projects are present; generic Storage backend reuse by FLD Database remains incomplete and CI is currently red.  
**Purpose:** make the durable storage engine independently reusable without loading or depending on the FLD field-database implementation.  
**Packaging decision:** two DLLs from the first implementation: `Fld1.Storage.dll` and `Fld1.Database.dll`. They are separate projects, build outputs, and packageable artifacts.

## 1. Decision

FLD-DB is organized as two assemblies:

1. **`Fld1.Storage.dll`** — a small, domain-neutral embedded storage engine.
2. **`Fld1.Database.dll`** — FLD field/document semantics implemented on top of Storage.

Both assemblies are separate projects and build outputs from the first implementation. `Fld1.Database` references `Fld1.Storage`; the reverse reference is forbidden. Storage must build, test, run and be consumable without compiling, loading or deploying any FLD Database code. A third `Common` assembly is not introduced: genuinely generic primitives belong in Storage; FLD semantics belong in Database. Extracting a third assembly later requires evidence of a stable, independently useful shared contract, not merely duplicated helper methods.

The Storage Core is not a thin wrapper around `FldDatabase`. It is the lower-level engine used by `FldDatabase` and independently by applications that need durable metadata or another compact embedded store.

## 2. Target dependency graph

```text
Application
  |                          BIN-SYNC integration (separate adapter/package)
  |                                      |
  +---- Fld1.Database -------------------+---- BinSync.Core contracts
  |       |
  |       +---- field/record model
  |       +---- LWW/lifecycle/HLC
  |       +---- local Outbox / remote APPLY semantics
  |       +---- FldDatabase facade and convenience API
  |
  +---- Fld1.Storage
          |
          +---- atomic byte-key/value transactions
          +---- WAL, recovery and checkpointing
          +---- immutable runs/pages and ordered scans
          +---- snapshots and generation lifetime
          +---- file/manifest primitives and integrity checks
          +---- optional writer coordination / leases
```

Allowed dependency direction:

- `Fld1.Database -> Fld1.Storage`.
- A BIN-SYNC adapter may depend on `Fld1.Storage` and `BinSync.Core`.
- `Fld1.Storage` MUST NOT depend on `Fld1.Database`, `FldDatabase`, BIN-SYNC, or the adapter.
- BIN-SYNC Core MUST NOT depend on FLD.

The adapter should normally live outside FLD-DB (for example, in a BIN-SYNC integration package). This prevents the storage engine from acquiring protocol dependencies just because its first consumer is BIN-SYNC.

## 2.1 Current source graph — physical projects present, backend reuse incomplete (2026-11-09)

The source project graph now has the approved two physical runtime projects:

- `Fld1.Storage`: an independent opaque byte-key/value engine with atomic batches, snapshots/prefix scans and generic WAL.
- `Fld1.Database`: one physical project that owns the former Core, Engine, Manifest, Run and WAL source files under separate code folders/namespaces. The five component project files were removed; no linked-source compilation shortcut is used.

This closes the physical two-project consolidation, but it does **not** complete generic engine reuse. `FldDatabase` still uses the legacy field-aware Run/Manifest/WAL path now housed inside `Fld1.Database`; it does not yet use `StorageDatabase` as its physical backend. The generic Storage engine and FLD Database currently have separate durability implementations. The remaining extraction must classify and migrate generic mechanics without moving FLD-specific codecs/semantics into Storage, while preserving the frozen FLD1 physical format and recovery compatibility.

The source-backed extraction order and compatibility gates are tracked in `SYNC-STORAGE-COMPACTION-ROADMAP.md` §7.50 and §7.57. No third `Common` runtime DLL and no linked-source compilation shortcut are permitted. CI for the consolidated project is currently red/unverified; do not claim a validated release build until the exact-head cross-platform run passes.

## 3. What belongs in Storage Core

Storage Core owns only physical durability and generic storage mechanics:

- opening/closing a storage directory and validating its format;
- atomic write batches and commit ordering;
- WAL append, flush, recovery and torn/corrupt record handling;
- immutable run/page creation, manifest publication and checkpointing;
- byte-oriented keys and values, ordered point/range/prefix reads;
- bounded batch writes and efficient visitor-based scans;
- stable read snapshots and safe reclamation of old generations;
- storage-native commit sequence numbers;
- optional append-only streams or log primitives with explicit sequence and compaction boundaries;
- fenced leases/ownership primitives where required by the storage engine;
- checksums, format versions, crash consistency and file lifecycle;
- bounded indexes and seek-from-cursor paging.

A caller must be able to store opaque metadata records without asking the engine to understand their meaning. The engine may optimize byte keys, values, batches, prefixes and ranges, but it must not interpret protocol or field semantics.

### 3.1 Minimal data model

The initial reusable surface should be byte-oriented:

- a logical store/partition identifier;
- an opaque ordered key;
- an opaque value or immutable value reference;
- atomic batches containing put/delete operations;
- read snapshots;
- ordered range/prefix scans;
- storage-native commit/checkpoint identifiers.

The minimal public capability surface should be:

- open/close a storage instance from storage-only options;
- open a stable read snapshot;
- begin a serialized write transaction;
- read opaque key/value entries inside that transaction;
- put/delete opaque entries and commit them atomically and durably;
- perform ordered range/prefix scans through a borrowed visitor;
- expose a storage-native CommitSeq/checkpoint watermark;
- checkpoint/compact and release snapshots so old generations can be reclaimed safely.

A write transaction must provide read-your-writes and serialize conflicting updates (or expose explicit expected-version conflicts). It must not require callers to manually coordinate WAL and manifest writes. The engine's commit is the atomic durability boundary. The adapter builds higher-level inbox/relay/peer transactions from these primitives.

The exact public type and method names are a later API-design step. Do not prematurely expose a large generic ORM, LINQ provider, schema registry, object serializer or arbitrary plug-in framework.

Metadata adapters can encode compact composite keys and values on top of this surface. For example, an adapter can keep peer state, inbox receipts, ChangeId indexes and relay records in separate key prefixes or logical stores while updating related records in one atomic batch.

The engine must not require a user-data table, RecordId, FieldId, NodeId, HLC or ChangeId.

## 4. What does not belong in Storage Core

The following are FLD Database or BIN-SYNC responsibilities, not storage primitives:

- RecordId / FieldId and field/document APIs;
- HLC generation or comparison;
- NodeId, author identity, ChangeId, BatchId and namespace protocol rules;
- LWW comparison, lifecycle/ghost/tombstone semantics;
- schema mapping, CRDT merge policies and canonical protocol payloads;
- local pending-change coalescing and frozen Outbox policy;
- Inbox duplicate rules, SeenChanges/ChangeIdLedger semantics and peer cursors;
- relay eligibility, author streams, ACK validation and protocol GC;
- network frames, sessions, transport, cryptography and peer state machines;
- LINQ/typed-record convenience APIs.

Storage Core must durably persist opaque bytes supplied by these layers, including the encoded representation of FLD field/record tombstones, but must not construct, validate or reinterpret their semantics. Storage-native deletion markers (for removing a byte key from its own index) are a separate physical-engine concern and must not be confused with FLD lifecycle tombstones. FLD Database decides when a field/record tombstone is safe to reclaim; Storage only performs the requested physical reclamation after that decision.

## 5. Source and project layout

Create two actual projects and build outputs, not merely namespaces inside one assembly:

```text
src/
  Fld1.Storage/
    Abstractions/
    Engine/
    Wal/
    Runs/
    Manifest/
    Snapshots/
    Indexes/
    Coordination/
    Integrity/
  Fld1.Database/
    Records/
    Fields/
    Merge/
    Lifecycle/
    Outbox/
    Apply/
    FldDatabase.cs
```

`Fld1.Storage.csproj` compiles only `src/Fld1.Storage/**`; `Fld1.Database.csproj` compiles only `src/Fld1.Database/**` and references Storage. Tests are split into Storage-only and Database integration suites. Namespaces mirror assembly names. Do not use linked source files or duplicate compilation to simulate sharing.

Do not leave domain-specific types in a catch-all `Fld1.Core` namespace merely because both layers currently compile into one DLL. Audit the existing Core helpers individually:

- byte encoding, checksum and storage-format helpers may belong in Storage;
- HLC, UUIDv7 generation, author ordering and field-specific comparison belong above Storage;
- a helper belongs in a shared namespace only if it has no dependency on either domain and is used by both.

Storage implementation details remain internal. The small public Storage API is the extraction boundary; Database code must use that API rather than reaching into Storage implementation classes. This is what makes later project separation mechanical instead of a redesign.

## 5.1. Migration map for the current FLD modules

The existing module names are not proof that their current contents are already domain-neutral. Classify types by dependencies before moving code:

| Existing module | Storage Core candidates | Keep in FLD Database / optional layer |
|---|---|---|
| `Fld1.Core` | `ByteUtil`'s domain-neutral byte/endian primitives; CRC32C/CRC32 and xxHash only where required by the generic Storage format | HLC, UUIDv7, `Id16`/`NamespaceId` domain types, FLD canonical tags and BLAKE3 state/protocol digests unless the Storage format independently requires them |
| `Fld1.Run` | none may be moved wholesale based on its current API | the current RunBuilder/RunReader encode a field-specific FLD1 run (RecordId, FieldId, LWW tuple, PGM/cluster layout); keep this codec in Database unless it is deliberately split from a new generic byte-KV page/run format |
| `Fld1.Wal` | generic WAL envelope, append/flush, framing, checksum, sequence validation, recovery scanner and torn-record handling after extraction | GroupCommit/ApplyCommit payload codecs and domain-specific replay handlers; split codec from generic WAL mechanics |
| `Fld1.Manifest` | atomic file replacement and generic durable catalog/checkpoint publication mechanics after extraction | current RunEntry/namespace descriptors and CommitResult semantics until separated from FLD-specific metadata |
| `Fld1.Engine` | only independently reusable file/generation coordination and transaction scheduling after a type-level audit | current MemFieldKey/MemOverlay/FieldResolver, FldDatabase, HLC/LWW, LocalAuthorStore, Outbox, ApplyJournal and RemoteBatchCommit are Database/sync semantics |
| `Fld1.Client` | only if required: transport-neutral IPC primitives for an optional shared engine | shared-engine client protocol and process-specific conveniences; do not make them mandatory for embedded single-process Storage |

This is a classification target, not an assertion that every existing class already satisfies it. Before moving a type, inspect its full dependency graph and split mixed files at the lowest stable seam. A generic WAL envelope must not call a field decoder; a generic run reader must not know RecordId; a generic manifest must not manufacture a NamespaceId.

## 6. Assembly and package boundaries

`Fld1.Storage.dll` and `Fld1.Database.dll` are independently built artifacts. The Storage public API must not expose any FLD Database or BIN-SYNC type in method signatures, generic constraints, events, exceptions or callbacks. Use opaque byte keys/values and storage-native identifiers. Its public dependencies should be BCL-only (including `System.IO.Hashing` where appropriate); do not require BinSync.Core, JSON serializers, a database facade, a network stack or a plugin host.

The project graph is deliberately simple:

- `Fld1.Storage.csproj`: no project reference to Database or BIN-SYNC.
- `Fld1.Database.csproj`: one-way project reference to Storage.
- Optional BIN-SYNC integration is a separate adapter/package and references the neutral Storage API plus BIN-SYNC contracts; it is not folded into either assembly by default.

Both DLLs may be published together as a convenient FLD-DB package, while Storage can also be published independently. Consumers choosing Storage only must not acquire `Fld1.Database.dll` as a transitive runtime dependency. Avoid circular references and `InternalsVisibleTo` as a permanent way for Database to reach into Storage internals.

## 7. Physical format and engine reuse

Do not create a second storage engine solely for Storage-only mode. The same WAL/run/manifest/recovery machinery should serve both modes.

- FLD Database stores its field-state records using its own key/value encoding over Storage Core.
- A metadata-only consumer stores opaque metadata records using its own compact key/value encoding.
- Both use the same atomic batch, WAL, checkpoint and snapshot guarantees.
- Storage-native CommitSeq is not a BIN-SYNC author cursor, OutboxSeq, HLC or peer cursor.
- Storage-only writes do not synthesize a LocalAuthor, HLC, ChangeId, Outbox entry or field lifecycle state.

**Important current-code finding:** the existing `Fld1.Run` implementation is not a generic immutable byte-KV run.

**Current implementation checkpoint (2026-11-09):** GROUP_COMMIT body v3 and APPLY_COMMIT body v4 now carry origin epoch/sequence while decoders preserve the previous body versions. RunBuilder emits the sorted 48-byte provenance table when current winners have stable ChangeIds; RunVerify and RunReader understand v1/v2, and checkpoint/compaction pass winner provenance into the Run. A dedicated WAL identity-stamp record persists the immutable origin assigned after downstream freeze without writing another Outbox event. These changes are C-pass source implementation only; no build/test pass is claimed. Snapshot run pins and the `IDataProvider` adapter remain open.

**Provider provenance decision (2026-11-09):** FLD1 Run v1 is retained as a legacy reader, and new Run v2 will add a sorted per-field provenance section carrying stable ChangeId plus immutable author epoch/sequence. New WAL body versions will preserve Origin, and a dedicated idempotent identity-stamp WAL record will persist the Origin assigned after provider publication without appending another Outbox operation. Existing v1 rows remain legacy-materialized with zero identity; no ChangeId is synthesized. The binding decision and full compatibility rules are in [FLD-PROVIDER-SNAPSHOT-PROVENANCE-ADR.md](FLD-PROVIDER-SNAPSHOT-PROVENANCE-ADR.md). This is a Database-owned FLD field format extension, not an assertion that the generic Storage page/run backend migration is complete.
 `RunBuilder` and `RunReader` are field-aware: they encode/decode `RecordId`, `FieldId`, HLC/author/key epoch, tombstones, PGM projection and field clusters. Moving the existing `Fld1.Run.dll` wholesale into `Fld1.Storage.dll` would make Storage depend on FLD semantics and violate the two-assembly contract.

A genuinely reusable Storage engine therefore needs a generic ordered byte-key/value page/run representation. As of 2026-11-09, `Fld1.Database.csproj` and BIN-SYNC's `BinSync.Storage.csproj` accept the same `Fld1StorageProject` override, and the optional `samples/FldBinSyncHost` composes both against one canonical Storage project. This proves a clean cross-repository project/assembly reference seam only; it does not make FLD's field-aware Run/Manifest/WAL codecs generic or complete the shared physical backend. FLD Database must encode its field key and field state as opaque bytes on top of that engine, or retain a clearly separated legacy FLD1 codec with an explicit compatibility/migration path. Do not hide the field-aware run behind a nominally generic interface and call that a reusable Storage engine. Before implementation, decide the physical-format compatibility strategy: (A) new generic Storage format plus legacy FLD1 reader/import; or (B) a lower-level generic page/blob engine with a Database-owned FLD1 index/codec. Benchmark write amplification, point/range reads, checkpoint cost and on-disk size for both. No physical format change is authorized by this assembly decision alone.

### 7.1 Durability and injected failure model

Storage creates database, WAL and snapshot directories through `StorageDirectorySync.CreateDirectoryDurable`. On Linux and macOS, newly created parent-directory entries are explicitly flushed with native `open(O_DIRECTORY)` + `fsync` + `close`; newly published WAL segment and snapshot/manifest directory entries are flushed after their atomic rename/create/delete boundaries. On Windows, directory-entry durability is not claimed by this implementation; file contents are still flushed through the normal file-handle durability path. Do not describe the Windows directory metadata boundary as equivalent to Unix directory fsync.

WAL append and flush are fail-closed. If an injected or real partial write occurs, the active writer stops accepting further appends; the database is faulted and must be reopened so recovery can truncate a torn final record. If WAL fsync fails, the writer stops accepting appends and the database requires reopen; process-level tests may observe a complete record after reopen, but that does not prove it survived device cache loss or power failure. If directory fsync fails after a segment was created, renamed or reclaimed, the manager is faulted and must be reopened to re-establish the authoritative segment set.

Deterministic fault points cover partial WAL write, pre-WAL-fsync, WAL directory fsync, WAL append/flush publication, snapshot-file flush/rename, manifest-file flush/rename and WAL truncation. These tests validate software failure containment and recovery transitions. They do not replace device-level I/O error injection, filesystem crash testing or physical power-loss testing; those remain deployment validation gates.

Do not create dummy namespaces or synthetic field records to represent metadata.

The single-process embedded Storage path must remain usable without starting the FLD Shared Engine process. However, the product requirement for multiple independent applications/processes to access the same physical FLD database file is served by the FLD Shared Engine mode described in §14 below; it is not implemented by opening the same Storage partition independently from every process. The shared Engine is optional for single-process consumers but required for multi-process shared-file access.

## 8. Performance and API constraints

- Prefer `ReadOnlySpan<byte>`, `ReadOnlyMemory<byte>`, bounded batch APIs and visitor-based scans on hot paths.
- Avoid mandatory object materialization for each key/value during scans.
- Avoid copying large values when an immutable page/blob reference can safely pin its generation.
- Do not hold writer locks while callers perform network I/O or long-running processing over a snapshot.
- Keep indexes bounded or rebuildable; never retain one in-memory object per historical journal entry without a retention bound.
- Separate WAL recovery lifetime from receipt, relay and application metadata lifetimes.
- Events/diagnostics are optional and must not run synchronously on the commit/read hot path.
- Fail closed on checksum, manifest, sequence or recovery inconsistencies.

## 9. Dependency and conformance tests

Before treating Storage Core as reusable, add tests that prove:

1. Storage Core can be opened and used without constructing `FldDatabase`.
2. A small standalone consumer can write, commit, close, reopen, read and scan opaque records using only the Storage API.
3. Crash/reopen tests pass for atomic batches, WAL recovery and manifest/checkpoint publication.
4. A snapshot remains readable while newer commits/checkpoints occur; old generations are reclaimed only after the last snapshot releases them.
5. Range/prefix scans are ordered and page from the requested cursor without rescanning the full historical prefix.
6. Storage-only writes create no field/HLC/author/Outbox metadata.
7. The Storage namespace/public API has no reference to Database or BIN-SYNC domain types.
8. A dependency-architecture test rejects references from Storage source/types to Database namespaces and BIN-SYNC assemblies.
9. The FLD Database uses the Storage public surface; it does not bypass it to manipulate WAL/manifest internals.
10. Storage can be compiled into a separate project in a test branch without moving or rewriting database algorithms.

## 10. Implementation order

1. Inventory existing source files and classify every type as Storage, Database or genuinely shared.
2. Establish namespaces/source folders and dependency tests without changing physical behavior.
3. Separate generic durability primitives from FLD-specific run/manifest codecs; explicitly decide the generic Storage page/run format and legacy FLD1 compatibility path.
4. Define the minimal byte-oriented Storage API around generic WAL/catalog/page mechanics, not around field-aware RunBuilder/RunReader.
5. Add a standalone Storage-only sample/test that never references FldDatabase.
6. Implement metadata key/value encoding and the BIN-SYNC adapter outside the field database layer.
7. The runtime boundary is exactly two separately buildable DLLs from the first implementation: Fld1.Storage.dll and Fld1.Database.dll. They may be shipped together in one convenience package; independent publication of Storage is optional. Do not introduce a third Common/Shared runtime DLL.

**Core rule:** the FLD database runtime consists of exactly two DLLs: `Fld1.Storage.dll` (neutral byte engine) and `Fld1.Database.dll` (FLD field/database semantics and FLD/BIN-SYNC integration boundary). The only dependency direction is `Fld1.Database.dll → Fld1.Storage.dll`. No third Common/Shared runtime DLL. Storage must never need to know that a field database or a synchronization protocol exists.


## 11. Related synchronization-history audit

The code-backed analysis of FLD local Outbox, remote APPLY/relay retention, BIN-SYNC cross-peer deduplication and peer cursor storage is maintained in [SYNC-HISTORY-STORAGE-AUDIT.md](SYNC-HISTORY-STORAGE-AUDIT.md). This is a Database/BIN-SYNC integration concern, not a reason to add protocol or FLD field semantics to Storage Core. Storage remains a neutral byte engine; higher layers own ChangeId, receipts, peer cursors, relay eligibility and retention policy.

## 12. Relationship to sync metadata compaction

The cross-layer optimization plan is maintained in the FLD-DB [Sync Storage Compaction Roadmap](SYNC-STORAGE-COMPACTION-ROADMAP.md), with code-backed findings in [Sync History Storage Audit](SYNC-HISTORY-STORAGE-AUDIT.md).

This roadmap does not move BIN-SYNC protocol semantics into Storage Core. Fld1.Storage remains domain-neutral: it provides atomic byte batches, WAL/recovery, snapshots, ordered scans, checkpoints and safe reclamation. ChangeId identity, batch receipts, peer cursors, relay eligibility, protocol retention and PendingLocal coalescing remain in FLD Database/BIN-SYNC semantic layers or their adapter. The same physical engine may host these records, but Storage Core must not interpret them.


## 13. WAL single-writer ownership and recovery ordering

Every Storage partition must acquire its OS-managed single-writer fence before reading the snapshot manifest or replaying WAL. The fence belongs to the generic Storage engine, not to FLD field semantics or BIN-SYNC protocol code.

The current implementation uses a named OS Mutex keyed by the canonical physical WAL directory and partition ID.

The consolidated `FldDatabase` now acquires `StorageDatabase.AcquirePartitionLease` using one reserved root-wide partition identity before it reads the shared manifest or replays any namespace WAL/Outbox/ApplyJournal state. This reuses the generic `WalWriterFence` without opening a generic Storage WAL and therefore preserves the existing FLD1 on-disk record format. The lease is released only after the FLD file handles close, and failed-open paths release it in `finally`. This is single-owner compatibility wiring, not the multi-application Shared Engine/IPC implementation and not a migration of FLD's field-aware WAL/Run/Manifest codecs into generic Storage.
 A process-wide manager thread owns acquisition and release so a StorageDatabase may be disposed from any caller thread without violating Mutex thread affinity. If the previous process terminates while holding the mutex, the next owner accepts the abandoned-mutex handoff and performs normal fail-closed WAL/snapshot recovery while holding the fence.

Path identity is canonicalized across symlinks and aliases before deriving the mutex name. A second direct opener/writer for the same physical partition must fail before it loads the snapshot or changes WAL state. This is the safety rule for direct embedded mode; it is not the multi-application sharing mechanism. In shared-file mode, all mutating clients submit requests to the one FLD Shared Engine process, which alone owns the physical Storage partitions and WAL. Do not weaken the mutex or let clients bypass the Engine. Do not replace this with a file-share lease without a separate cross-platform proof; the earlier FileShare-based experiment stalled the integration lifecycle.

The FLD-DB Storage source now matches the implementation verified in BIN-SYNC.

The current snapshot implementation shares immutable stored value arrays rather than cloning every value into each snapshot. `OpenSnapshot` materializes a sorted key/reference index (`SortedList`) so its remaining index memory is O(number of keys), not O(page). `PrefixCursor.VisitPage` exposes borrowed read-only key/value spans for bounded scans; the view is valid only during the callback. `PrefixCursor.TryReadNext` also exposes one borrowed entry directly for merge algorithms that need a single lookahead from multiple ordered streams; `VisitPage` is built on the same primitive. `ReadPage` remains the copying convenience API. Metadata recovery and provider feed paths should prefer the visitor when the caller can consume spans synchronously. This improves peak memory but does not claim fully O(page) snapshot memory. Repeated snapshots in the same commit epoch now share a cached sorted index; each successful commit invalidates the cache, while snapshots already opened retain their prior index. Thus the O(number-of-keys) index is built at most once per unchanged epoch, not once per snapshot.
 Regression coverage includes same-partition writer rejection, cross-thread disposal/reopen, abandoned-owner recovery, symlink aliases and case aliases on case-insensitive filesystems. Snapshot recovery removes unreferenced generations and abandoned temp files only after validating the authoritative root; failed checkpoint operations fault the live database and require reopen. Internal deterministic fault points cover WAL append/flush and snapshot/manifest publication boundaries. FLD-DB CI passed 163/163 tests independently on Ubuntu, Windows and macOS-15 at code/test head `0efa1cdb8287ee82082186a5dcb8c163baf3528a`. These controls do not replace partial-write, real filesystem/device error, directory durability or power-loss testing.

## 14. Multi-application access to one physical database file

**Binding product requirement:** independent applications/processes must be able to open and use one physical FLD database without independently owning or mutating its files. This requirement predates the Storage extraction and is part of the original FLD Shared Engine design in `FLD-KERNEL-SPEC-v1.5.9.md` §0.1–0.2 and PART III. The shared-file requirement must not be lost when the physical engine is split into Storage and Database layers.

### 14.1 Two operating modes

1. **Direct embedded mode (one owning process).** An application opens `Fld1.Storage` directly (or through `Fld1.Database`). The process owns the partition writer fence and performs local reads/writes. A second process cannot independently open the same physical partition for writing; it fails before manifest/WAL recovery. This is the current implemented Storage ownership model.
2. **Shared-file mode (multiple client processes).** Exactly one FLD Shared Engine process owns a physical database. Independent applications connect to it through local IPC/shared-memory interfaces. The Engine is the only process allowed to mutate WAL, manifest, mutable L0/catalog state and compaction state. All writes are submitted to the Engine and may be combined into group commits. Clients read a consistent published view; immutable run/page generations may be exposed through read-only memory mapping with explicit snapshot/epoch lifetime protection.

These modes share the same Storage durability engine and on-disk format. Shared-file mode is not implemented by relaxing or bypassing `WalWriterFence`.

### 14.2 Preserve exactly two runtime DLLs

The assembly boundary remains exactly:

- `Fld1.Storage.dll`: physical byte engine plus generic local/IPC client capability. It knows storage keys, values, transactions, snapshots and storage-native sequence numbers only.
- `Fld1.Database.dll`: FLD records/fields, HLC/LWW, lifecycle, indexes/projections and the FLD-facing client/facade. It may use the Storage client capability when connected to a shared Engine.

The Shared Engine Host is a process/executable, not a third runtime DLL. It hosts the Storage owner and, for a full FLD database, the Database semantic layer. A Storage-only deployment must be able to host/use Storage without loading `Fld1.Database.dll`. The old `Fld.Client.dll` label in the v1.5.9 conceptual diagram is a logical client role, not authorization to add a third runtime DLL: its generic Storage-facing implementation belongs in Storage and its FLD-facing facade belongs in Database.

Dependency direction remains `Fld1.Database.dll → Fld1.Storage.dll`; Storage must not depend on Database. BIN-SYNC remains an external adapter/consumer and must not become a dependency of either Storage or the Shared Engine's generic IPC contract.

### 14.3 Shared Engine invariants

- One Engine owner per canonical physical database identity; startup/bootstrap must prevent two owners from racing to open the same files.
- Clients never append/truncate WAL, publish manifests, run compaction, or mutate the physical database files directly.
- Request submission is bounded and session-fenced. Client death/timeout must not leave a permanently owned request slot or writer lease.
- A successful write response is returned only after the Engine's documented durability boundary (including required WAL flush) has been met.
- Group commit may combine independent client requests, but transaction atomicity and result correlation remain per request; unrelated calls must not be reported as one application transaction.
- Reads use a stable published snapshot. Long-running reads pin generations/epochs; they must not hold the writer lock while clients perform application work.
- Engine restart recovers the database before accepting clients. Client reconnect/retry must distinguish a committed request from an unknown outcome and use durable request identity where retry safety requires it.
- IPC authorization is scoped to the local machine/user/session and physical database access rights. Network filesystems (NFS/SMB) remain outside the current v1.5.9 non-goals unless separately designed and tested.

### 14.4 Implementation boundary and acceptance tests

The current named OS Mutex is the physical single-owner fence beneath both modes. It protects the Engine owner; it is not a substitute for IPC or a multi-writer database protocol. The following work remains before shared-file mode can be claimed implemented:

1. Define the Engine Host lifecycle, discovery/bootstrap, canonical database identity and stale-owner recovery.
2. Define versioned local IPC request/response framing, bounded queues, session generation, cancellation and request-result recovery.
3. Define how Storage snapshots and immutable generations are safely exposed to clients (read-only MMF/SHM or an equivalent implementation).
4. Place generic Storage client operations in Storage and FLD Database client operations in Database without introducing a third runtime DLL.
5. Add two-independent-process tests: concurrent reads; concurrent writes from both clients; group-commit correctness; one durable result per request; client termination during queued/in-flight work; Engine termination/restart; reconnect after ambiguous commit; snapshot pinning during compaction; and proof that a second Engine owner cannot start.
6. Verify a Storage-only client can use the shared Engine without loading or referencing `Fld1.Database.dll`.

Until these gates pass, describe the current implementation as **single-process direct Storage with cross-process single-owner fencing**, and describe multi-application shared-file access as a preserved architectural requirement/design, not a completed feature. More specifically, the current `Fld1.Engine` project description says “single-process path”; `FldDatabase.Open` still constructs the legacy Engine/WAL/Run/Manifest components directly, and the repository has no Shared Engine Host, IPC client or shared-memory client implementation. The current `Fld1.Storage` project is a separate generic direct-storage implementation with a writer fence; it is not yet the physical backend of `FldDatabase`.
, and new Run v2 will add a sorted per-field provenance section carrying stable ChangeId plus immutable author epoch/sequence. New WAL body versions will preserve Origin, and a dedicated idempotent identity-stamp WAL record will persist the Origin assigned after provider publication without appending another Outbox operation. Existing v1 rows remain legacy-materialized with zero identity; no ChangeId is synthesized. The binding decision and full compatibility rules are in [FLD-PROVIDER-SNAPSHOT-PROVENANCE-ADR.md](FLD-PROVIDER-SNAPSHOT-PROVENANCE-ADR.md). This is a Database-owned FLD field format extension, not an assertion that the generic Storage page/run backend migration is complete.
 `RunBuilder` and `RunReader` are field-aware: they encode/decode `RecordId`, `FieldId`, HLC/author/key epoch, tombstones, PGM projection and field clusters. Moving the existing `Fld1.Run.dll` wholesale into `Fld1.Storage.dll` would make Storage depend on FLD semantics and violate the two-assembly contract.

A genuinely reusable Storage engine therefore needs a generic ordered byte-key/value page/run representation. As of 2026-11-09, `Fld1.Database.csproj` and BIN-SYNC's `BinSync.Storage.csproj` accept the same `Fld1StorageProject` override, and the optional `samples/FldBinSyncHost` composes both against one canonical Storage project. This proves a clean cross-repository project/assembly reference seam only; it does not make FLD's field-aware Run/Manifest/WAL codecs generic or complete the shared physical backend. FLD Database must encode its field key and field state as opaque bytes on top of that engine, or retain a clearly separated legacy FLD1 codec with an explicit compatibility/migration path. Do not hide the field-aware run behind a nominally generic interface and call that a reusable Storage engine. Before implementation, decide the physical-format compatibility strategy: (A) new generic Storage format plus legacy FLD1 reader/import; or (B) a lower-level generic page/blob engine with a Database-owned FLD1 index/codec. Benchmark write amplification, point/range reads, checkpoint cost and on-disk size for both. No physical format change is authorized by this assembly decision alone.

### 7.1 Durability and injected failure model

Storage creates database, WAL and snapshot directories through `StorageDirectorySync.CreateDirectoryDurable`. On Linux and macOS, newly created parent-directory entries are explicitly flushed with native `open(O_DIRECTORY)` + `fsync` + `close`; newly published WAL segment and snapshot/manifest directory entries are flushed after their atomic rename/create/delete boundaries. On Windows, directory-entry durability is not claimed by this implementation; file contents are still flushed through the normal file-handle durability path. Do not describe the Windows directory metadata boundary as equivalent to Unix directory fsync.

WAL append and flush are fail-closed. If an injected or real partial write occurs, the active writer stops accepting further appends; the database is faulted and must be reopened so recovery can truncate a torn final record. If WAL fsync fails, the writer stops accepting appends and the database requires reopen; process-level tests may observe a complete record after reopen, but that does not prove it survived device cache loss or power failure. If directory fsync fails after a segment was created, renamed or reclaimed, the manager is faulted and must be reopened to re-establish the authoritative segment set.

Deterministic fault points cover partial WAL write, pre-WAL-fsync, WAL directory fsync, WAL append/flush publication, snapshot-file flush/rename, manifest-file flush/rename and WAL truncation. These tests validate software failure containment and recovery transitions. They do not replace device-level I/O error injection, filesystem crash testing or physical power-loss testing; those remain deployment validation gates.

Do not create dummy namespaces or synthetic field records to represent metadata.

The single-process embedded Storage path must remain usable without starting the FLD Shared Engine process. However, the product requirement for multiple independent applications/processes to access the same physical FLD database file is served by the FLD Shared Engine mode described in §14 below; it is not implemented by opening the same Storage partition independently from every process. The shared Engine is optional for single-process consumers but required for multi-process shared-file access.

## 8. Performance and API constraints

- Prefer `ReadOnlySpan<byte>`, `ReadOnlyMemory<byte>`, bounded batch APIs and visitor-based scans on hot paths.
- Avoid mandatory object materialization for each key/value during scans.
- Avoid copying large values when an immutable page/blob reference can safely pin its generation.
- Do not hold writer locks while callers perform network I/O or long-running processing over a snapshot.
- Keep indexes bounded or rebuildable; never retain one in-memory object per historical journal entry without a retention bound.
- Separate WAL recovery lifetime from receipt, relay and application metadata lifetimes.
- Events/diagnostics are optional and must not run synchronously on the commit/read hot path.
- Fail closed on checksum, manifest, sequence or recovery inconsistencies.

## 9. Dependency and conformance tests

Before treating Storage Core as reusable, add tests that prove:

1. Storage Core can be opened and used without constructing `FldDatabase`.
2. A small standalone consumer can write, commit, close, reopen, read and scan opaque records using only the Storage API.
3. Crash/reopen tests pass for atomic batches, WAL recovery and manifest/checkpoint publication.
4. A snapshot remains readable while newer commits/checkpoints occur; old generations are reclaimed only after the last snapshot releases them.
5. Range/prefix scans are ordered and page from the requested cursor without rescanning the full historical prefix.
6. Storage-only writes create no field/HLC/author/Outbox metadata.
7. The Storage namespace/public API has no reference to Database or BIN-SYNC domain types.
8. A dependency-architecture test rejects references from Storage source/types to Database namespaces and BIN-SYNC assemblies.
9. The FLD Database uses the Storage public surface; it does not bypass it to manipulate WAL/manifest internals.
10. Storage can be compiled into a separate project in a test branch without moving or rewriting database algorithms.

## 10. Implementation order

1. Inventory existing source files and classify every type as Storage, Database or genuinely shared.
2. Establish namespaces/source folders and dependency tests without changing physical behavior.
3. Separate generic durability primitives from FLD-specific run/manifest codecs; explicitly decide the generic Storage page/run format and legacy FLD1 compatibility path.
4. Define the minimal byte-oriented Storage API around generic WAL/catalog/page mechanics, not around field-aware RunBuilder/RunReader.
5. Add a standalone Storage-only sample/test that never references FldDatabase.
6. Implement metadata key/value encoding and the BIN-SYNC adapter outside the field database layer.
7. The runtime boundary is exactly two separately buildable DLLs from the first implementation: Fld1.Storage.dll and Fld1.Database.dll. They may be shipped together in one convenience package; independent publication of Storage is optional. Do not introduce a third Common/Shared runtime DLL.

**Core rule:** the FLD database runtime consists of exactly two DLLs: `Fld1.Storage.dll` (neutral byte engine) and `Fld1.Database.dll` (FLD field/database semantics and FLD/BIN-SYNC integration boundary). The only dependency direction is `Fld1.Database.dll → Fld1.Storage.dll`. No third Common/Shared runtime DLL. Storage must never need to know that a field database or a synchronization protocol exists.


## 11. Related synchronization-history audit

The code-backed analysis of FLD local Outbox, remote APPLY/relay retention, BIN-SYNC cross-peer deduplication and peer cursor storage is maintained in [SYNC-HISTORY-STORAGE-AUDIT.md](SYNC-HISTORY-STORAGE-AUDIT.md). This is a Database/BIN-SYNC integration concern, not a reason to add protocol or FLD field semantics to Storage Core. Storage remains a neutral byte engine; higher layers own ChangeId, receipts, peer cursors, relay eligibility and retention policy.

## 12. Relationship to sync metadata compaction

The cross-layer optimization plan is maintained in the FLD-DB [Sync Storage Compaction Roadmap](SYNC-STORAGE-COMPACTION-ROADMAP.md), with code-backed findings in [Sync History Storage Audit](SYNC-HISTORY-STORAGE-AUDIT.md).

This roadmap does not move BIN-SYNC protocol semantics into Storage Core. Fld1.Storage remains domain-neutral: it provides atomic byte batches, WAL/recovery, snapshots, ordered scans, checkpoints and safe reclamation. ChangeId identity, batch receipts, peer cursors, relay eligibility, protocol retention and PendingLocal coalescing remain in FLD Database/BIN-SYNC semantic layers or their adapter. The same physical engine may host these records, but Storage Core must not interpret them.


## 13. WAL single-writer ownership and recovery ordering

Every Storage partition must acquire its OS-managed single-writer fence before reading the snapshot manifest or replaying WAL. The fence belongs to the generic Storage engine, not to FLD field semantics or BIN-SYNC protocol code.

The current implementation uses a named OS Mutex keyed by the canonical physical WAL directory and partition ID.

The consolidated `FldDatabase` now acquires `StorageDatabase.AcquirePartitionLease` using one reserved root-wide partition identity before it reads the shared manifest or replays any namespace WAL/Outbox/ApplyJournal state. This reuses the generic `WalWriterFence` without opening a generic Storage WAL and therefore preserves the existing FLD1 on-disk record format. The lease is released only after the FLD file handles close, and failed-open paths release it in `finally`. This is single-owner compatibility wiring, not the multi-application Shared Engine/IPC implementation and not a migration of FLD's field-aware WAL/Run/Manifest codecs into generic Storage.
 A process-wide manager thread owns acquisition and release so a StorageDatabase may be disposed from any caller thread without violating Mutex thread affinity. If the previous process terminates while holding the mutex, the next owner accepts the abandoned-mutex handoff and performs normal fail-closed WAL/snapshot recovery while holding the fence.

Path identity is canonicalized across symlinks and aliases before deriving the mutex name. A second direct opener/writer for the same physical partition must fail before it loads the snapshot or changes WAL state. This is the safety rule for direct embedded mode; it is not the multi-application sharing mechanism. In shared-file mode, all mutating clients submit requests to the one FLD Shared Engine process, which alone owns the physical Storage partitions and WAL. Do not weaken the mutex or let clients bypass the Engine. Do not replace this with a file-share lease without a separate cross-platform proof; the earlier FileShare-based experiment stalled the integration lifecycle.

The FLD-DB Storage source now matches the implementation verified in BIN-SYNC.

The current snapshot implementation shares immutable stored value arrays rather than cloning every value into each snapshot. `OpenSnapshot` materializes a sorted key/reference index (`SortedList`) so its remaining index memory is O(number of keys), not O(page). `PrefixCursor.VisitPage` exposes borrowed read-only key/value spans for bounded scans; the view is valid only during the callback. `PrefixCursor.TryReadNext` also exposes one borrowed entry directly for merge algorithms that need a single lookahead from multiple ordered streams; `VisitPage` is built on the same primitive. `ReadPage` remains the copying convenience API. Metadata recovery and provider feed paths should prefer the visitor when the caller can consume spans synchronously. This improves peak memory but does not claim fully O(page) snapshot memory. Repeated snapshots in the same commit epoch now share a cached sorted index; each successful commit invalidates the cache, while snapshots already opened retain their prior index. Thus the O(number-of-keys) index is built at most once per unchanged epoch, not once per snapshot.
 Regression coverage includes same-partition writer rejection, cross-thread disposal/reopen, abandoned-owner recovery, symlink aliases and case aliases on case-insensitive filesystems. Snapshot recovery removes unreferenced generations and abandoned temp files only after validating the authoritative root; failed checkpoint operations fault the live database and require reopen. Internal deterministic fault points cover WAL append/flush and snapshot/manifest publication boundaries. FLD-DB CI passed 163/163 tests independently on Ubuntu, Windows and macOS-15 at code/test head `0efa1cdb8287ee82082186a5dcb8c163baf3528a`. These controls do not replace partial-write, real filesystem/device error, directory durability or power-loss testing.

## 14. Multi-application access to one physical database file

**Binding product requirement:** independent applications/processes must be able to open and use one physical FLD database without independently owning or mutating its files. This requirement predates the Storage extraction and is part of the original FLD Shared Engine design in `FLD-KERNEL-SPEC-v1.5.9.md` §0.1–0.2 and PART III. The shared-file requirement must not be lost when the physical engine is split into Storage and Database layers.

### 14.1 Two operating modes

1. **Direct embedded mode (one owning process).** An application opens `Fld1.Storage` directly (or through `Fld1.Database`). The process owns the partition writer fence and performs local reads/writes. A second process cannot independently open the same physical partition for writing; it fails before manifest/WAL recovery. This is the current implemented Storage ownership model.
2. **Shared-file mode (multiple client processes).** Exactly one FLD Shared Engine process owns a physical database. Independent applications connect to it through local IPC/shared-memory interfaces. The Engine is the only process allowed to mutate WAL, manifest, mutable L0/catalog state and compaction state. All writes are submitted to the Engine and may be combined into group commits. Clients read a consistent published view; immutable run/page generations may be exposed through read-only memory mapping with explicit snapshot/epoch lifetime protection.

These modes share the same Storage durability engine and on-disk format. Shared-file mode is not implemented by relaxing or bypassing `WalWriterFence`.

### 14.2 Preserve exactly two runtime DLLs

The assembly boundary remains exactly:

- `Fld1.Storage.dll`: physical byte engine plus generic local/IPC client capability. It knows storage keys, values, transactions, snapshots and storage-native sequence numbers only.
- `Fld1.Database.dll`: FLD records/fields, HLC/LWW, lifecycle, indexes/projections and the FLD-facing client/facade. It may use the Storage client capability when connected to a shared Engine.

The Shared Engine Host is a process/executable, not a third runtime DLL. It hosts the Storage owner and, for a full FLD database, the Database semantic layer. A Storage-only deployment must be able to host/use Storage without loading `Fld1.Database.dll`. The old `Fld.Client.dll` label in the v1.5.9 conceptual diagram is a logical client role, not authorization to add a third runtime DLL: its generic Storage-facing implementation belongs in Storage and its FLD-facing facade belongs in Database.

Dependency direction remains `Fld1.Database.dll → Fld1.Storage.dll`; Storage must not depend on Database. BIN-SYNC remains an external adapter/consumer and must not become a dependency of either Storage or the Shared Engine's generic IPC contract.

### 14.3 Shared Engine invariants

- One Engine owner per canonical physical database identity; startup/bootstrap must prevent two owners from racing to open the same files.
- Clients never append/truncate WAL, publish manifests, run compaction, or mutate the physical database files directly.
- Request submission is bounded and session-fenced. Client death/timeout must not leave a permanently owned request slot or writer lease.
- A successful write response is returned only after the Engine's documented durability boundary (including required WAL flush) has been met.
- Group commit may combine independent client requests, but transaction atomicity and result correlation remain per request; unrelated calls must not be reported as one application transaction.
- Reads use a stable published snapshot. Long-running reads pin generations/epochs; they must not hold the writer lock while clients perform application work.
- Engine restart recovers the database before accepting clients. Client reconnect/retry must distinguish a committed request from an unknown outcome and use durable request identity where retry safety requires it.
- IPC authorization is scoped to the local machine/user/session and physical database access rights. Network filesystems (NFS/SMB) remain outside the current v1.5.9 non-goals unless separately designed and tested.

### 14.4 Implementation boundary and acceptance tests

The current named OS Mutex is the physical single-owner fence beneath both modes. It protects the Engine owner; it is not a substitute for IPC or a multi-writer database protocol. The following work remains before shared-file mode can be claimed implemented:

1. Define the Engine Host lifecycle, discovery/bootstrap, canonical database identity and stale-owner recovery.
2. Define versioned local IPC request/response framing, bounded queues, session generation, cancellation and request-result recovery.
3. Define how Storage snapshots and immutable generations are safely exposed to clients (read-only MMF/SHM or an equivalent implementation).
4. Place generic Storage client operations in Storage and FLD Database client operations in Database without introducing a third runtime DLL.
5. Add two-independent-process tests: concurrent reads; concurrent writes from both clients; group-commit correctness; one durable result per request; client termination during queued/in-flight work; Engine termination/restart; reconnect after ambiguous commit; snapshot pinning during compaction; and proof that a second Engine owner cannot start.
6. Verify a Storage-only client can use the shared Engine without loading or referencing `Fld1.Database.dll`.

Until these gates pass, describe the current implementation as **single-process direct Storage with cross-process single-owner fencing**, and describe multi-application shared-file access as a preserved architectural requirement/design, not a completed feature. More specifically, the current `Fld1.Engine` project description says “single-process path”; `FldDatabase.Open` still constructs the legacy Engine/WAL/Run/Manifest components directly, and the repository has no Shared Engine Host, IPC client or shared-memory client implementation. The current `Fld1.Storage` project is a separate generic direct-storage implementation with a writer fence; it is not yet the physical backend of `FldDatabase`.
