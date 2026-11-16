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


### Latest single-rewrite and candidate-identity allocation results (2026-11-15)

The paired full-sweep run on FLD-DB commit `201710cfa461b3f94013cd0e273c4a6beda2cdcb` completed successfully (GitHub Actions run `43692777196`, artifact `14153956925`). It includes the internal owned-array candidate factory, which avoids cloning identity arrays a second time while preserving the public candidate constructor's defensive-copy contract.

| Tombstones | Repeated 4,096-page sweep | Single rewrite | Repeated allocations | Single-rewrite allocations |
|---:|---:|---:|---:|---:|
| 1,000 | 5.748 ms | 6.062 ms | 477.57 KB | 477.57 KB |
| 10,000 | 70.414 ms | 28.205 ms | 13,430.7 KB | 4,145.43 KB |
| 50,000 | 940.132 ms | 33.279 ms | 366,656.58 KB | 18,925.49 KB |
| 100,000 | 4,137.862 ms | 66.951 ms | 1,475,011.36 KB | 38,246.41 KB |
| 250,000 | 28,253.695 ms | 248.692 ms | 9,203,877.92 KB | 87,937.09 KB |

The 250k single rewrite is about 114x faster than repeating 4,096-entry pages and allocates about 105x less than that repeated-page strategy. Against the previous single-rewrite run before the owned-array factory (250k: 252.594 ms / 117,233.96 KB), the candidate identity change reduced cumulative allocation by about 25.0%, with no timing regression (248.692 ms; the small time difference is not treated as a speed claim). At 50k and 100k, cumulative allocation fell by about 23.6% and 23.5%, respectively.

BenchmarkDotNet settings are 1 warmup and 5 measured iterations for this sweep. The small 1k case is noisy. All allocation values are cumulative managed bytes per measured operation, not peak live heap or process working set. This benchmark remains FLD-only and does not include BIN-SYNC signed proof, Metadata reference scan, transport, or Host orchestration. It is not GC enablement approval.

### Sampled peak-memory diagnostic

The full-sweep matrix also includes `ReclaimAllTombstonesInSinglePassWithPeakSampling`. It forces a full collection after setup, records baseline process working set and managed heap, then samples both every 2 ms while one single-pass rewrite runs. The output line `TOMBSTONE_GC_PEAK_SAMPLE` reports baseline, sampled peak and delta for each measure.

This is a diagnostic estimate, not an exact allocator high-water mark: a 2 ms sampler can miss shorter-lived peaks, and the background sampler adds some CPU and memory overhead. Do not use this method's BenchmarkDotNet timing as a performance comparison. Its purpose is to distinguish peak working-set/heap behavior from `MemoryDiagnoser` cumulative allocation.

The corrected isolated child-process run completed on FLD-DB HEAD `8109aecb61045a151f31905c1bf0f8cacd0931d6` (GitHub Actions run `43699646746`, artifact `14155834102`). Each cardinality has eight samples. Mean sampled managed-heap delta after opening the prepared namespace:

| Tombstones | Mean managed-heap delta | Samples |
|---:|---:|---:|
| 1,000 | 1.09 MiB | 8 |
| 10,000 | 5.93 MiB | 8 |
| 50,000 | 46.05 MiB | 8 |
| 100,000 | 88.30 MiB | 8 |
| 250,000 | 190.91 MiB | 8 |

All samples reported `WorkingSetDeltaBytes=0`. This is an inconclusive working-set measurement, not evidence of zero resident-memory growth. The current sampler/platform high-water reporting does not provide a trustworthy process peak; a reliable OS-level peak metric is still required before making physical-memory capacity claims. Managed-heap deltas are sampled live-heap observations and must not be confused with cumulative allocations in the BenchmarkDotNet tables above.

### Reproducibility rerun on FLD-DB HEAD 6c5e195 (2026-11-15)

The dedicated benchmark was rerun after the documentation update (GitHub Actions run [43701205145](https://github.com/smersh1307n2/FLD-DB/actions/runs/43701205145), artifact [14157261073](https://github.com/smersh1307n2/FLD-DB/actions/runs/43701205145)). The artifact completed successfully and includes both the single-page and full-sweep matrices.

The latest full-sweep run reported:

| Tombstones | Repeated 4,096-page sweep | Single rewrite | Repeated allocations | Single-rewrite allocations |
|---:|---:|---:|---:|---:|
| 1,000 | 8.804 ms | 8.585 ms | 477.57 KB | 477.57 KB |
| 10,000 | 78.973 ms | 39.163 ms | 13,428.57 KB | 4,145.43 KB |
| 50,000 | 1,223.288 ms | 42.882 ms | 366,657.19 KB | 18,925.49 KB |
| 100,000 | 5,062.187 ms | 93.866 ms | 1,474,991.23 KB | 38,246.41 KB |
| 250,000 | 34,036.699 ms | 266.557 ms | 9,206,992.61 KB | 87,937.09 KB |

At 250k, this rerun measured approximately 128x lower elapsed time and 105x lower cumulative managed allocation for one rewrite than for repeated 4,096-entry pages. The small 1k result remains noise-sensitive. These values are a separate run, not a replacement for the earlier baseline table above.

The child-process diagnostic again produced eight samples per cardinality. At 250k, sampled managed-heap delta ranged around 190.1–191.0 MiB (mean approximately 190.9 MiB). Every sample still reported `WorkingSetDeltaBytes=0`; process RSS remains unmeasured reliably. The diagnostic must not be used as an RSS/capacity bound.

This remains an FLD-only physical compaction measurement. The signed Metadata proof + real Host IPC reference scan + coordinator scheduling benchmark is still open and is tracked in BIN-SYNC Roadmap V3 §1.168.


### Demand-paged Run mapping benchmark

Run the dedicated mapping matrix with:

```bash
dotnet run --project bench/Fld1.Engine.Bench/Fld1.Engine.Bench.csproj --configuration Release -- --run-mapping-only
```

The fixture contains 1 / 16 / 64 immutable Runs, each with 128 records and 64-byte values. It reports two distinct paths:

- `FreshDatabaseOpenAndFirstMappedLookup`: opens a new database instance and performs the first lookup, including lazy Run mapping, structural validation and comparison with the authoritative Storage blob digest.
- `WarmMappedLookups`: measures one lookup in every Run after all process mappings have already been established.

BenchmarkDotNet reports elapsed time and managed allocations. "Fresh" means a new process-local mapping/database instance, **not a cold operating-system page cache**; the OS may retain file pages between invocations. This matrix does not claim peak RSS or cold-storage latency. A separate isolated-process OS-level peak-working-set measurement is still required before making capacity claims. The workflow is `.github/workflows/run-mapping-benchmark.yml` and uploads its BenchmarkDotNet artifact.
