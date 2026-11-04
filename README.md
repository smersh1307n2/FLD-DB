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
| `Fld1.Core` | Uuid7, HLC, ByteUtil (RecordId BE + Author=NodeId LE limbs), Hashes, Ids |
| `Fld1.Run` | Full STATE run: empty/non-empty, inline+external, multi-seg PGM, Get, Verify |
| `Fld1.Wal` | Micro-WAL v2: header, envelope, GROUP_COMMIT, writer, recovery |
| `Fld1.Manifest` | Binary manifest, atomic swap, RunEntry, QUERY_COMMIT_RESULT |
| `Fld1.Engine` / `Fld1.Client` | Planned (v1.5 Shared Engine) |

## Spec

[`docs/FLD-KERNEL-SPEC-v1.5.8.md`](docs/FLD-KERNEL-SPEC-v1.5.8.md) — PART XI: compat notes for BinSync.Core 2.6.5.

## Build

```bash
dotnet restore
dotnet build -c Release
dotnet test -c Release
```

## Layout

```
docs/                  FLD kernel specification
src/Fld1.Core/         primitives
src/Fld1.Run/          FLD1 run format
src/Fld1.Wal/          Micro-WAL v2
src/Fld1.Manifest/     binary manifest + atomic store
tests/                 xUnit
packages/              local nupkgs (BinSync.Core)
```
