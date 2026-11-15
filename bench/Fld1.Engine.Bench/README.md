# FLD Engine benchmarks

Run the existing Engine and Outbox microbenchmarks:

```sh
dotnet run --project bench/Fld1.Engine.Bench/Fld1.Engine.Bench.csproj -c Release
```

Run the bounded physical Tombstone-GC benchmark only:

```sh
dotnet run --project bench/Fld1.Engine.Bench/Fld1.Engine.Bench.csproj -c Release -- --tombstone-gc-only
```

## Tombstone-GC benchmark

The benchmark seeds a real on-disk FLD namespace with one field per record, checkpoints the live generation, writes and checkpoints a field tombstone for every record, and acknowledges the complete Outbox prefix. Setup is outside the measured invocation.

It then measures exactly one call to `FldDatabase.CompactTombstones` for each parameter pair:

- Namespace cardinality: 1,000 / 10,000 / 50,000 tombstones
- Bounded reclamation page: 256 / 4,096 tombstones

The invocation verifies that the number physically reclaimed equals `min(namespace cardinality, page limit)`. BenchmarkDotNet reports elapsed time and managed allocation; the harness also emits `TOMBSTONE_GC_ALLOCATED_BYTES` for each case.

The same command also runs `TombstoneGcSweepBench`: for 1k / 10k / 50k tombstones it repeatedly calls the physical compactor with the production page limit of 4,096 until every tombstone has been reclaimed. It verifies the exact page count and reports total sweep time and allocations (`TOMBSTONE_GC_SWEEP_ALLOCATED_BYTES`). This captures repeated whole-namespace rewrites that the single-page benchmark does not.

This isolates FLD's physical compaction cost. It does not benchmark signature verification, namespace-roster exchange, Metadata reference scans, transport, or the end-to-end BIN-SYNC proof path. A passing benchmark is not authorization to enable GC. All production GC feature switches remain default-off.
