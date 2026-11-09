# FLD-DB

Embedded field-store with multi-process access via **FLD Shared Engine**.

Implements [FLD KERNEL SPECIFICATION v1.5.9-DRAFT](docs/FLD-KERNEL-SPEC-v1.5.9.md) and integrates with [BIN-SYNC](https://github.com/smersh1307n2/BIN-SYNC).


Storage Core extraction and dependency boundaries are defined in [FLD Storage Core architecture](docs/FLD-STORAGE-CORE-ARCHITECTURE.md). The solution now exposes the approved two-runtime-assembly shape: `Fld1.Storage.dll` (domain-neutral byte engine) and `Fld1.Database.dll` (FLD field database, referencing Storage). The Database project physically owns Core/Run/WAL/Manifest/Engine source under `src/Fld1.Database`; no linked-source shortcut or intermediate runtime DLLs remain in the solution. Storage-only consumers must not load the Database assembly. The consolidated project and solution wiring are committed, but exact-head build/test validation is still pending.

Architecture for using FLD-DB as a generic Metadata Store (including the Storage-only role) is described in the [BIN-SYNC Metadata Storage and External Data Provider Architecture](https://github.com/smersh1307n2/BIN-SYNC/blob/feature/fld-external-storage/docs/METADATA-STORAGE-ARCHITECTURE.md). The associated [Metadata Store conformance contract](https://github.com/smersh1307n2/BIN-SYNC/blob/feature/fld-external-storage/docs/METADATA-STORE-CONFORMANCE.md) and [Data Provider conformance contract](https://github.com/smersh1307n2/BIN-SYNC/blob/feature/fld-external-storage/docs/DATA-PROVIDER-CONFORMANCE.md) define their separate semantic boundaries. The [change staging, coalescing and retention policy](https://github.com/smersh1307n2/BIN-SYNC/blob/feature/fld-external-storage/docs/CHANGE-STAGING-AND-RETENTION.md) defines which unpublished local changes may be collapsed and when Outbox entries become immutable.

The cross-repository [sync storage compaction roadmap](docs/SYNC-STORAGE-COMPACTION-ROADMAP.md) owns sequencing and acceptance gates; the [sync history storage audit](docs/SYNC-HISTORY-STORAGE-AUDIT.md) records code-backed findings; [tombstone retention research](docs/TOMBSTONE-RETENTION-RESEARCH.md) provides the rationale for fail-closed E-25 behavior.

| Layer | Version |
|-------|---------|
| FLD format / kernel | v1.5.9-DRAFT (+compat-BinSync.Core-3.0.0) |
| BIN-SYNC **protocol** | CORE v2.4.19 FINAL |
| `BinSync.Core` **package** | 3.0.0 |

**Target:** .NET 10 / C# 13+

## Roadmap (API)

1. **Now — imperative field API** (`FldDatabase`): Put / Delete / batch / Commit / Checkpoint / Get / Scan — fast, explicit, optimizable.
2. **Later — convenience layer** (LINQ-style / typed records): built *on top* of the same engine, not instead of it.

## Status

| Module | Status |
|--------|--------|
| `Fld1.Storage.dll` | Separate generic byte-key/value engine; first runtime assembly |
| `Fld1.Database.dll` | Second runtime assembly; links Core, Run, WAL, Manifest and Engine source into one deployable database layer |
| Database source areas | Core (Uuid7/HLC/Hash), Run (builder/reader), WAL, Manifest and Engine are physically consolidated under `src/Fld1.Database` while retaining their namespace/folder organization |

## Simple API

```csharp
using var db = FldDatabase.Open(new FldOpenOptions
{
    Path = "/data/mydb",
    NamespaceId = 0,
    AutoCheckpointOverlayFields = 10_000, // optional
});

var rec = FldDatabase.NewRecordId();
db.Apply(new FldWriteBatch()
    .Put(rec, fieldId: 1, "hello"u8.ToArray())
    .Put(rec, fieldId: 2, "world"u8.ToArray()));
db.Commit();           // durable WAL
db.Checkpoint();       // seal to immutable .fld1 (or auto)

db.TryGet(rec, 1, out ReadOnlySpan<byte> v);
db.Delete(rec, 2);
db.ScanRecord(rec, (fieldId, value) => { /* ... */ });
```

Durability: **Commit** = WAL fsync (manifest watermark deferred). **Checkpoint** = immutable run + manifest.

## Build

```bash
dotnet restore && dotnet build -c Release && dotnet test -c Release
```
