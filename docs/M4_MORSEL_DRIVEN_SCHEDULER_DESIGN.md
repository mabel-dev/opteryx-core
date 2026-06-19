# M4 — Morsel-Driven Parallel Scheduler (Design)

**Status:** DESIGN — awaiting architect sign-off on the radix route (§7). Architect
has locked: full scheduler **including high-card**, radix-partitioned combine, and
this session **owns** the scheduler. **Date:** 2026-06-18.

Supersedes the Stage-0/1 scheduler in
[`parallel_engine.py`](../opteryx/managers/execution/parallel_engine.py)
(round-robin + serial shuffle), which gave geomean 0.98× on high-card ClickBench
and is now **broken at W>1** against the CxxMorsel substrate
(`CxxMorsel.column: not found`). That code is deleted by this rebuild.

## 1. Why this is now viable (rebaseline)

The thing that capped the old scheduler — GIL-serialized workers + a serial
Python scan pull — has been removed by two landed initiatives:

- **Native zero-Python, thread-safe-concurrent-pull scan**
  ([`native_scan_morsel_path`]): `ParquetReadNode.next_morsel()` returns
  `shared_ptr[CxxMorsel]`; the single-pass path is **reentrant** — N threads can
  call it concurrently on one scan and get disjoint morsels (the C++
  `IpcRowGroupSource.next_vectors()` is thread-safe; assembly is thread-local;
  only a telemetry commit is under `_scan_mtx`). Pull is still GIL-held (S-B.2
  nogil pending) but decode is parallel below.
- **GIL-off grouped-agg ingest** (`cpp_morsel_design` S-B.3c):
  `GroupHashEngine._ingest_cxx_span(...) noexcept nogil`, single+multi-col, all
  cardinalities (parvi+carchar), concurrency-safe, **measured 5.3×/8-thread**.
  Per-engine, no shared state across instances.

End-to-end query speedup is **not yet realized** — that is exactly this
scheduler's job.

## 2. The model — morsel-driven, radix-partitioned combine (DuckDB/ClickHouse)

Not a serial shuffle. Not one main-thread puller. The model:

```
        ┌── worker 0 ──┐   ┌── worker 1 ──┐        ┌── worker W-1 ─┐
scan ──▶│ pull morsel  │   │ pull morsel  │  ...   │ pull morsel   │   (concurrent
 (1)    │ (next_morsel)│   │ (next_morsel)│        │ (next_morsel) │    self-pull)
        │ route rows   │   │ route rows   │        │ route rows    │
        │ by hash&(P-1)│   │ by hash&(P-1)│        │ by hash&(P-1) │   (nogil ingest
        │ → P part-agg │   │ → P part-agg │        │ → P part-agg  │    per partition)
        └──────────────┘   └──────────────┘        └───────────────┘
                  ╲               │                      ╱
                   ╲              │                     ╱   combine: P parallel
                    ▼             ▼                    ▼    tasks, task p merges
            ┌─ combine p=0 ─┐ ┌─ combine p=1 ─┐ ... every worker's partition p
            │ merge W parts │ │ merge W parts │       (disjoint p → no contention,
            │ finalize      │ │ finalize      │        NO serial merge wall)
            └───────────────┘ └───────────────┘
                          concatenate → post-agg (sort/limit) → cursor
```

Three properties, each grounded in landed code:
1. **Self-pull** removes the serial main-thread pull (concurrent `next_morsel`).
2. **Per-worker nogil ingest** removes the GIL serialization (`_ingest_cxx_span`).
3. **Radix-partitioned combine** removes the `merge()` Amdahl wall — partition
   *p* across all workers is merged by one task; the P combines run in parallel
   on the pool; `merge_group_state` is the per-group primitive reused.

This is the structure DuckDB (`RadixPartitionedHashTable`) and ClickHouse
(two-level tables) converge on.

## 3. Worker pool & self-pull

- The vendored **`CppThreadPool`** (BS::thread_pool), query-scoped, W =
  `min(MAX_EXECUTION_WORKERS, cpu-2, 8)`.
- **Each worker loops `scan.next_morsel()`** (concurrent-safe) until NULL, instead
  of a main thread pulling and dispatching. Pull is GIL-held today; the ingest
  span it feeds is nogil, so workers overlap on the aggregation (the dominant
  cost). When S-B.2 lands (nogil pull) the pull overlaps too.
- Latmat two-pass scans are **not reentrant** → those queries fall back to serial
  (honest, telemetry-flagged), until a parallel two-pass path exists.
- Push into the (cloned) chain via `push_one` / `push_left_one` / `push_right_one`
  (the sanctioned carrier entries).

## 4. Partition count P (the architect's NDV table)

P (radix partitions) is **decoupled from W** and sized by **NDV** (group-key
cardinality), per the locked table — it controls combine granularity + cache-fit,
not parallelism width:

| rows / NDV | P |
|---|---|
| < 250k rows | serial (no split) |
| NDV ≤ 250k | 2 |
| ≤ 1m | 4 |
| ≤ 10m | 8 |
| ≤ 100m | 16 |
| ≤ 1b | 32 | (×2 per ~decade)

NDV from a **runtime sample-KMV** over the first ~250k buffered rows (optimizer
NDV when present); bias up when the sample is all-distinct; the radix combine is
robust to an under-estimate (a fat partition is still correct, just less
balanced). `P = hash & (P-1)` (power of two) consistent with `cxx_hash_c` /
`partition_by_hash` (same `draken_hash`+`simd_mix_hash`).

## 5. Recombination per breaker

- **Grouped aggregate (mergeable):** radix combine above (P parallel
  `merge`/`merge_group_state` tasks), finalize P, concatenate. No serial merge.
- **Non-mergeable aggregates** (COUNT DISTINCT, median, decimal/string MIN-MAX —
  `is_mergeable()`=False): radix still works because each partition owns disjoint
  keys → finalize per partition independently, **no merge needed at all**
  (concatenate). This is a *bonus*: radix parallelises strictly more aggregates
  than the old merge path. (Confirm each finalizes correctly per-partition.)
- **Sort/limit after the agg:** the P finalized streams feed the post-agg operator
  (single EOS) — serial tail, unchanged.
- **Joins / union / distinct:** out of scope for this milestone's first cut
  (grouped-agg pipeline only); they reuse the self-pull + per-worker-clone
  machinery in a follow-up.

## 6. The broken old code

Delete `_grouped_agg_stream` / `_shuffle_agg_stream` (round-robin + serial
shuffle) from `parallel_engine.py`. **Keep** `resolve_worker_count`,
`identify_segments`, `Segment`, the shape detection (`_find_parallel_grouped_agg`),
`ExchangeNode` + `Morsel.partition_by_hash` (reused by Option 1 routing), and the
`MAX_EXECUTION_WORKERS` / `PARALLEL_*` config + the engine-selection seam.

## 7. THE DECISION — radix route (Option 1 vs Option 2)

Both give per-partition parallel combine; they differ in where the partitioning
happens and the risk.

**Option 1 — W×P independent `GroupHashEngine` instances (zero engine change).**
Each worker holds P engines; per morsel it calls `partition_by_hash(key, P)` (my
native nogil kernel) → P sub-morsels → ingests sub-morsel *p* into engine[w][p].
Combine: P tasks, task *p* does `engine[0][p].merge(engine[w][p])` for w>0.
- ✅ **Zero change to the delicate nogil ingest span** — it just gets instantiated
  W×P times. Lowest P0 risk.
- ✅ Reuses `partition_by_hash` (built, tested) + the existing `merge`.
- ❌ **Per-morsel row copy** into P sub-morsels (`take` per bin) — the cost that
  hurt wide-key q34. O(rows), but real.
- ❌ W×P engine objects (memory; manageable with per-engine reserve = NDV/P).

**Option 2 — one engine with P internal radix indices (no row copy).**
Refactor `GroupHashEngine` to hold `vector[CarcharIndex*]` (+ partitioned
KeyStore); `_ingest_cxx_span` routes each row to partition `hash & (P-1)` (no
sub-morsel materialization); `merge`/`finalize` become per-partition.
- ✅ **No row copy** — routes by hash internally during ingest. The DuckDB model.
- ✅ One engine per worker (W, not W×P).
- ❌ **~250-line refactor of the just-landed, P0-prone nogil span** + KeyStore
  partitioning. Highest risk in the initiative.

**Recommendation: Option 1 first, gated and measured; Option 2 as the
copy-elimination optimization if the partition copy shows up in the wide-bench.**
Rationale: it delivers correct high-card parallel combine end-to-end with zero
risk to the delicate nogil ingest, and we get a real high-card number before
betting on the bigger refactor. Both reach the same place; Option 1 is the safe
on-ramp. (If you'd rather pay the refactor now for the cleaner endpoint, Option 2.)

## 8. Backpressure, errors, cancellation, thread-safety

- **Backpressure:** with self-pull there is no central queue — workers pull at
  their own rate; the scan's bounded in-flight (rugo) is the natural throttle. No
  per-worker queue needed (simpler than the old model).
- **Errors:** each worker captures its exception; the driver joins all futures and
  re-raises the first (the carrier `PipelineContext._exc` model; per-worker
  engines so no shared agg state).
- **Cancellation:** `ctx.is_terminated()` checked in the worker loop; LIMIT
  short-circuit; `scan.close_source()` in a finally (once, after join).
- **Thread-safety:** per-worker engines (proven concurrency-safe). The shared
  reads are the scan (thread-safe single-pass) and the read-only bytecode/config.
  Mirror `test_grouped_engine_concurrency.py` at the pipeline level.

## 9. Staged build (each stage gates make q 190 / tpch 22 / clickbench 43)

- **S1 — self-pull, single per-worker engine, low/med-card only** (NDV-gated, P=1
  effectively = round-robin-by-self-pull + merge). Proves the self-pull + nogil
  ingest end-to-end win that was impossible before. Delete old broken code. Wide
  bench low/med-card.
- **S2 — radix combine (Option 1)** for high-card: W×P engines, `partition_by_hash`
  routing, P-parallel combine. Wide bench the high-card regressors (q16/q17/q34) —
  the result neither prior iteration achieved.
- **S3 — NDV→P sizing** (sample-KMV) + the serial/parallel + low/high gate unified
  under one selector.
- **S4 — (optional) Option 2** internal radix if the partition copy is shown to
  bottleneck wide keys.
- **S5 — default-on proposal** if the cumulative wide bench is neutral-or-better.

Validation bar: `make q` 190 / `make tpch` 22 identical across W∈{1,4,8} and
strategies; pipeline-level concurrency stress test; wide ClickBench is the perf
gate; ships flag-off until S5.
