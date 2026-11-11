# FLD-DB

Embedded field-store with multi-process access via **FLD Shared Engine**.

Implements [FLD KERNEL SPECIFICATION v1.5.9-DRAFT](docs/FLD-KERNEL-SPEC-v1.5.9.md) and integrates with [BIN-SYNC](https://github.com/smersh1307n2/BIN-SYNC).


Storage Core extraction and dependency boundaries are defined in [FLD Storage Core architecture](docs/FLD-STORAGE-CORE-ARCHITECTURE.md). The solution now exposes the approved two-runtime-assembly shape: `Fld1.Storage.dll` (domain-neutral byte engine) and `Fld1.Database.dll` (FLD field database, referencing Storage). The Database project physically owns Core/Run/WAL/Manifest/Engine source under `src/Fld1.Database`; no linked-source shortcut or intermediate runtime DLLs remain in the solution. Storage-only consumers must not load the Database assembly. The consolidated project and solution wiring are validated by the BIN-SYNC cross-repository CI on both Linux and Windows: the Database Host and Storage-only Host build and their process/integration suites pass. The standalone FLD-DB workflow currently terminates before runner allocation; its red check is not a compile/test result.

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
| `Fld1.Storage.Host` | Separate Storage-only process; does not load Database |
| `Fld1.Database.Host` | Separate single-owner Database process; current IPC facade supports atomic WriteBatch, receipt-backed ReadField and receipt ACK |
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

## Local Database Host and IPC client

`Fld1.Database.Host` is the single-owner process for a FLD namespace. It owns the `FldDatabase` instance and exposes the first Database IPC operations over the local named-pipe / Unix-domain-socket transport implemented by `Fld1.Storage`. It is a separate executable, not a third runtime DLL:

- `Fld1.Storage.dll` remains domain-neutral; `Fld1.Storage.Host` remains Storage-only and does not load Database.
- `Fld1.Database.dll` contains the FLD field database and references Storage.
- `Fld1.Database.Host` references Database and owns one namespace/partition while it is running.

Start a Host after building the solution:

```bash
dotnet build Fld.Db.slnx -c Release
dotnet run --project src/Fld1.Database.Host/Fld1.Database.Host.csproj -c Release -- /data/mydb 0
```

The arguments are the database root and decimal namespace id; an optional third argument is the 16-byte local author id encoded as 32 hexadecimal characters. The current Host publishes the partition-0 endpoint in the database root. A second Host cannot own the same database namespace concurrently. Clients must use the same normalized root and partition when discovering that endpoint.

A client keeps one stable `Guid` ClientId across reconnects. Before sending an operation, it must durably retain the `OperationId` and the next contiguous nonzero `ClientSequence`; an ambiguous timeout is retried with the exact same identity and request bytes. A successful response is persisted by the client before it advances the contiguous acknowledgement watermark with `AcknowledgeThroughAsync`. Acknowledged results are retired and late retries are rejected.

The current `FldDatabaseIpcClient` surface is deliberately narrow:

- `WriteBatchAsync` applies all field mutations and its operation receipt in one GROUP_COMMIT/WAL transaction. Deletes carry no value.
- `ReadFieldAsync` stores the exact read result as a receipt-only operation, so a lost response can be replayed without re-reading a changed value.
- `AcknowledgeThroughAsync` retires a contiguous prefix of the client's operation receipts after the client has persisted those results.

A receipt-only read does not create an Outbox entry. A write batch creates the ordinary local field changes and Outbox entries exactly once. Outbox delivery/ACK, snapshots, paged reads, remote APPLY and subscriptions are not yet part of this first Database IPC facade; they remain later facade work. See [the Database IPC contract](docs/FLD-DATABASE-IPC.md) for wire and retry details.

## Build

```bash
dotnet restore && dotnet build -c Release && dotnet test -c Release
```
