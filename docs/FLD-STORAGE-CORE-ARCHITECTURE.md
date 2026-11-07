# FLD Storage Core — extraction and dependency architecture

**Status:** Architecture decision v0.2 — two independently buildable assemblies.  
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

Storage Core may persist opaque bytes supplied by these layers. It must not construct, validate or reinterpret their values.

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
| `Fld1.Core` | byte readers/writers, endian helpers, checksums and generic hashing | UUIDv7 policy, HLC, author ordering, RecordId/FieldId and field comparators |
| `Fld1.Run` | generic immutable page/run framing, page checksums, file readers/writers and generation lifetime | field-record codec, LWW tuples, RecordId/FieldId projection and field-specific indexes |
| `Fld1.Wal` | WAL envelope, append/flush, sequence validation, recovery scanner and torn-record handling | GroupCommit/ApplyCommit payload codecs and domain-specific replay handlers |
| `Fld1.Manifest` | atomic manifest file replacement, generation catalog and generic checkpoint publication | FLD namespace/schema/run descriptors that encode field-database meaning |
| `Fld1.Engine` | generic writer serialization, snapshot lifetime, run selection, checkpoint/recovery orchestration | FldDatabase facade, field overlay, merge and Outbox/Apply behavior |
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

The existing FLD1 physical format can be reused only where its primitives are genuinely domain-neutral. If the current run format hardcodes field-specific assumptions, split the generic page/record mechanics from the field codec rather than forcing metadata records through fake RecordId/FieldId values. Do not create dummy namespaces or synthetic field records to represent metadata.

The single-process storage path must remain usable without starting the FLD Shared Engine process. Multi-process coordination may be an optional layer over Storage Core; it must not become a mandatory dependency for a simple embedded consumer.

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
3. Define the minimal byte-oriented Storage API around the current WAL/run/manifest implementation.
4. Move the FLD engine internals behind that API; keep one DLL and preserve the current public FldDatabase API.
5. Add a standalone Storage-only sample/test that never references FldDatabase.
6. Implement metadata key/value encoding and the BIN-SYNC adapter outside the field database layer.
7. Only after the boundary is stable, consider publishing a separate Storage assembly/package.

**Core rule:** `Fld1.Storage.dll` is the durable byte engine; `Fld1.Database.dll` is one consumer of it. Storage must never need to know that a field database or a synchronization protocol exists.
