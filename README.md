# FLD-DB

Embedded field-store with multi-process access via **FLD Shared Engine**.

Implements [FLD KERNEL SPECIFICATION v1.5.8-DRAFT](docs/FLD-KERNEL-SPEC-v1.5.8.md) and integrates with [BIN-SYNC](https://github.com/smersh1307n2/BIN-SYNC).

| Layer | Version |
|-------|---------|
| FLD format / kernel | v1.5.8-DRAFT (+compat-2.6.5) |
| BIN-SYNC **protocol** | CORE v2.3.6 + errata v2.3.7 |
| `BinSync.Core` **package** | ≥ 2.6.5 |

**Target:** .NET 10 / C# 13+

## Roadmap (API)

1. **Now — imperative field API** (`FldDatabase`): Put / Delete / batch / Commit / Checkpoint / Get / Scan — fast, explicit, optimizable.
2. **Later — convenience layer** (LINQ-style / typed records): built *on top* of the same engine, not instead of it.

## Status

| Module | Status |
|--------|--------|
| `Fld1.Core` | Uuid7, HLC, ByteUtil, Hashes |
| `Fld1.Run` | Builder / reader / verify / scan |
| `Fld1.Wal` | Micro-WAL v2 + HLC fields |
| `Fld1.Manifest` | Atomic store + commit-result query |
| `Fld1.Engine` | Single-process DB + simple write API |

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
