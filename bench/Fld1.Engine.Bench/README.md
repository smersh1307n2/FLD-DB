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

The invocation verifies that the number physically reclaimed equals `min(namespace cardinality, page limit)`. The single-page matrix uses 2 warmup and 10 measured iterations; the full-sweep matrix uses 1 warmup and 5 measured iterations to keep the larger namespaces practical. BenchmarkDotNet reports elapsed time and managed allocation; the harness also emits `TOMBSTONE_GC_ALLOCATED_BYTES` for each case.

The same command also runs `TombstoneGcSweepBench` at 1k / 10k / 50k / 100k / 250k tombstones using two strategies: (1) repeated 4,096-candidate calls, which expose the cost of repeated whole-namespace rewrites, and (2) one full-sweep call with a candidate budget equal to the namespace cardinality, which performs one namespace rewrite. Both methods verify the exact reclaimed count and report total sweep time and allocations (`TOMBSTONE_GC_SWEEP_ALLOCATED_BYTES`). The public API accepts a bounded candidate budget up to 1,000,000; callers should keep 4,096 for latency-bounded maintenance pages and use a larger budget only when a complete rewrite is operationally acceptable.

This isolates FLD's physical compaction cost. It does not benchmark signature verification, namespace-roster exchange, Metadata reference scans, transport, or the end-to-end BIN-SYNC proof path. A passing benchmark is not authorization to enable GC. All production GC feature switches remain default-off.
