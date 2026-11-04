# FLD-DB

Embedded field-store with multi-process access via **FLD Shared Engine**.

Implements [FLD KERNEL SPECIFICATION v1.5.8-DRAFT](docs/FLD-KERNEL-SPEC-v1.5.8.md) and integrates with [BIN-SYNC](https://github.com/smersh1307n2/BIN-SYNC) (v2.3.6 + addendum v2.3.7).

**Target:** .NET 10 / C# 12+

## Status

| Module | Status |
|--------|--------|
| `Fld1.Core` | In progress — Uuid7, HLC, ByteUtil, Hashes, Ids |
| `Fld1.Run` | Planned |
| `Fld1.Wal` | Planned |
| `Fld1.Manifest` | Planned |
| `Fld1.Engine` / `Fld1.Client` | Planned (v1.5 Shared Engine) |

## Build

```bash
dotnet restore
dotnet build -c Release
dotnet test -c Release
```

## Layout

```
src/Fld1.Core/     primitives (Uuid7, HLC, ByteUtil, Canon, Hashes)
tests/             xUnit
bench/             BenchmarkDotNet
packages/          local nupkgs (BinSync.Core)
```

## License

TBD (aligned with BIN-SYNC dual licensing intent).
