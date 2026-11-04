# FLD-DB

Embedded field-store with multi-process access via **FLD Shared Engine**.

Implements [FLD KERNEL SPECIFICATION v1.5.8-DRAFT](docs/FLD-KERNEL-SPEC-v1.5.8.md) and integrates with [BIN-SYNC](https://github.com/smersh1307n2/BIN-SYNC).

| Layer | Version |
|-------|---------|
| FLD format / kernel | v1.5.8-DRAFT (+compat-2.6.5) |
| BIN-SYNC **protocol** | CORE v2.3.6 + errata v2.3.7 |
| `BinSync.Core` **package** | ≥ 2.6.5 |

**Target:** .NET 10 / C# 13+

## Status

| Module | Status |
|--------|--------|
| `Fld1.Core` | Uuid7, HLC, ByteUtil, Hashes, Ids |
| `Fld1.Run` | STATE run builder/reader/verify (+ ArrayPool digest) |
| `Fld1.Wal` | Micro-WAL v2 |
| `Fld1.Manifest` | Binary manifest + atomic store |
| `Fld1.Engine` | Single-process DB: overlay, WAL commit+replay, checkpoint→run, get |

## Engine (current)

```csharp
using var db = FldDatabase.Open("/path/to/db", namespaceId: 0);
db.Put(recordId, fieldId, value);
db.Commit();       // durable WAL group commit
db.Checkpoint();   // seal overlay → immutable .fld1 + manifest
db.TryGet(recordId, fieldId, out var span);
```

Optimizations: span get, run byte[] cache, value-type `MemFieldKey`, pooled BLAKE3 input.

Crash recovery: WAL GROUP_COMMIT fields carry HLC+author; Open replays CommitSeq > last sealed run into overlay.

Rough micro-timing (Release, single field/rec hot path): Put ~1.5M ops/s, overlay Get ~7M ops/s, run Get ~0.7M ops/s.

## Build

```bash
dotnet restore && dotnet build -c Release && dotnet test -c Release
```
