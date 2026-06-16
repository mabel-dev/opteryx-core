# M4 — Central Parallel Execution Scheduler (Design)

**Status:** Stage 0 + Stage 1 BUILT & MEASURED (✅ make q 182/182, tpch 22/22, both at W=1 and force-parallel W=4). Stage 1 is CORRECT but geomean 0.98× on the high-card ClickBench GROUP BY battery — default STAYS OFF; decision point on direction (see §11). **Date:** 2026-06-15.

**Locked decisions (2026-06-15):** build order **Stage 0 → Stage 1** (mergeable
agg pipeline) first; engagement gate is a **row-floor only** (no NDV gate);
worker primitive is the **vendored `CppThreadPool`** (BS::thread_pool, C++17),
not stdlib threads; flag `MAX_EXECUTION_WORKERS`, default 1, cap 8.

This is a *milestone* design, not a single change. It supersedes the
intra-operator parallelism direction, which was built, measured, and
**falsified** (see the verdict boxes in
[`M4_PARALLEL_GROUP_AGG_DESIGN.md`](M4_PARALLEL_GROUP_AGG_DESIGN.md) and
[`M4_HASH_PARTITION_AGG_DESIGN.md`](M4_HASH_PARTITION_AGG_DESIGN.md)). It builds
directly on:

- [`EXECUTION_THREAD_SAFETY_CONTRACT.md`](EXECUTION_THREAD_SAFETY_CONTRACT.md) — the contract a parallel engine must honour, plus the **M4 enforcement checklist**.
- [`WP6_GIL_PROFILE.md`](WP6_GIL_PROFILE.md) — every operator class is now ≥50% GIL-released; kernels scale 5–7×. The ceiling is per-morsel Python *orchestration*, not the kernels.
- [`M4_PARALLEL_AGG_PROTOTYPE.md`](M4_PARALLEL_AGG_PROTOTYPE.md) — the cardinality cliff: low-card agg parallelises beautifully; high-card merge is the Amdahl bottleneck.

---

## 1. The load-bearing lesson (why pipeline-level, not intra-operator)

Two intra-operator strategies were measured on the full ClickBench GROUP BY
battery and both **lost**:

| strategy | geomean | why it lost |
|----------|--------:|-------------|
| round-robin morsels → per-worker engine clone → `merge()` on EOS | **0.94×** | serial `merge()` is O(groups × partials); at the high cardinality that dominates real workloads it ≈ the aggregation it parallelises |
| hash-partition rows by key → disjoint groups → no merge | **0.62–0.72×** | the per-row scatter is Python-bound + GIL-heavy on the main thread (starves workers) and copies every row |

The common root cause: **splitting + rejoining *inside one operator* adds
recombination cost comparable to the work it parallelises**, and that operator's
upstream (scan/filter/expr-prep) still ran serially on the main thread.

The win is structurally different. Parallelise the **whole pipeline segment**
(scan → filter → project → aggregate-ingest) over data partitions, so *every*
GIL-released stage runs N-ways — not just the aggregate. The agg merge term does
not vanish, but it is now amortised against scan (65% released), filter (50%,
scales 7.9×), and decode (already C++-parallel) all running in parallel too.
Recombination happens **only at pipeline breakers**, once, instead of per
operator. That is the target.

This also means: **the central scheduler wins where the breaker is *not* the
dominant cost** (filter-heavy, scan-heavy, sort-heavy, probe-heavy pipelines,
and low/medium-card aggregates), and is neutral-or-gated-off where a single
high-card breaker dominates. Honesty about that envelope is built into the
staged plan below — every stage has a wide-bench gate and ships flag-off.

---

## 2. Unit of parallelism

**Pipeline segments over data partitions.**

The pipeline compiler already decomposes the plan into `chains =
[(scan_node, chain_head)]` ([`pipeline_compiler.py`](../opteryx/managers/execution/pipeline_compiler.py)).
A *segment* is the maximal run of operators from a scan (or a breaker's output)
up to and including the next **pipeline breaker** (`is_pipeline_breaking` in
[`catalog.py`](../opteryx/operators/catalog.py): aggregates, distinct, sort,
heap-sort, all joins, union). Cutting the operator DAG at breakers yields a DAG
of segments; segments are the scheduler's tasks.

- **Within a segment**, every operator except the terminal breaker is
  STATELESS (Filter, Projection, streaming Window) — safe to run as private
  per-worker copies over disjoint partitions.
- **A data partition** is the natural morsel/row-group unit a scan already
  emits, or a partition of an intermediate result handed across a breaker.
- **At a breaker**, the W partial results recombine using the
  breaker-specific strategy in §5.

This is *not* intra-operator parallelism: a worker runs an entire
scan→filter→project→agg-ingest chain, not a sliver of one operator.

---

## 3. Worker pool

**One shared, query-scoped pool.** Size `min(MAX_EXECUTION_WORKERS, cpu-2)`,
default cap 8 (the prototype's regression boundary past 8 threads). A single
central pool is the whole point — operator-internal threads caused
oversubscription (it.1 risk #1). Two parallel aggregates in one query share the
one pool instead of spawning 2× threads.

- **Primitive: the vendored `CppThreadPool`**
  ([`opteryx/compiled/thread_pool.pyx`](../opteryx/compiled/thread_pool.pyx)) —
  a typed Cython wrapper over BS::thread_pool (C++17,
  [`third_party/bshoshany`](../third_party/bshoshany/BS_thread_pool.hpp)). It
  submits Python callables and returns `concurrent.futures.Future`s; no `object`
  members, C++-owned worker threads. **Not stdlib `threading.Thread`** (architect
  decision). Each submitted task runs a segment copy over one partition; the GIL
  is released *inside* the draken kernels (WP-6 sweep), so workers achieve real
  parallelism on the kernel-bound fraction. The per-morsel orchestration floor
  (push dispatch, Morsel lifecycle) stays GIL-held and is the residual ceiling
  (WP-6 § methodology); lifting it is the Phase-9 nogil-Morsel initiative, out of
  scope here.
- For the bounded scan→worker handoff,
  [`third_party/moodycamel`](../third_party/moodycamel) provides a lock-free
  concurrent queue if the GIL-bound stdlib queue shows up in a bench; start with
  the simplest correct handoff and only reach for it if measured.
- The pool is created at query start and torn down at query end (or on
  cancellation, via `shutdown(wait=...)`). No global/process-wide pool — sizing
  and lifetime are query-scoped so cancellation is clean.

---

## 4. Partition strategy

Two partitioning modes, chosen per breaker by §5:

1. **Pass-through partitioning (default).** A scan emits a stream of morsels;
   the scheduler hands each morsel to the next free worker (work-stealing /
   round-robin). Order-independent. Used for stateless segments and
   mergeable/no-key breakers. Cheapest — no row movement.
2. **Key-hash exchange (shuffle).** Rows are repartitioned by
   `hash(key) % W` so each worker owns a disjoint key range (for high-card
   grouped agg/distinct, and for hash-join build/probe by key). This is the
   strategy whose *Python* implementation was falsified; under M4 it is
   admissible **only** as a native off-GIL scatter kernel (Stage 4), and only
   if it measures out.

Backpressure: a bounded in-flight budget (a counting semaphore on queued
morsels, ≈ `Q × W`) throttles the scan when workers fall behind — exactly the
role `drive_scan`'s sequential pull played, generalised. A queued morsel holds
a **strong** reference (contract rule 7); no borrowed `DrakenVector*` crosses a
thread boundary.

---

## 5. Per-breaker recombination

The breaker decides how W partials become one result. Each cites its catalog
`OperatorParallelism` class.

| breaker | class | recombination |
|---------|-------|---------------|
| **Ungrouped aggregate** | MERGEABLE | clone per worker; `merge()` partials (already built + tested, WP-7). Cheap (scalar state) — pure win. |
| **Grouped aggregate** | MERGEABLE | clone per worker; ingest partitions; `merge()`. **Low/med card: win** (cheap merge). **High card: merge is the Amdahl term** — the key-hash exchange (Stage 4, no merge) is the eventual fix; until then high-card simply stays flag-off. `is_mergeable()=False` cases (COUNT DISTINCT, median, decimal-grouped, string MIN/MAX) stay serial *or* go key-hash. |
| **Distinct** | MERGEABLE | clone per worker (`_hash_set`); `merge()`. In-place mutation of input is safe under exclusive ownership (contract rule 2). |
| **Sort** | (clone+merge) | each worker sorts its partition; **k-way merge** of the W sorted runs. |
| **Heap-sort / top-N** | (clone+merge) | per-worker bounded heap of size N; merge W heaps → top-N. Strongly parallel (each worker discards early). |
| **Joins** | (build/probe) | **build once, probe in parallel.** Build side parallelises scan+filter, concatenates filtered build morsels, builds the hash table once (build is small relative to probe). Probe side parallelises over probe partitions against the **shared read-only** build table (concurrent reads are safe — contract "already safe"). Outputs concatenate. **No merge.** Preserves the checked build-before-probe invariant (`_require_build_complete`). |
| **Union** | SINGLETON | cannot clone — it *is* the join point. Synchronise EOS close-counting (`_seen_input_closes`) at the singleton; legs may still be parallel above it. |
| **Exit** | SINGLETON | terminal sink; synchronise the pending-result append, or per-worker exits merged above. |

The recombination cost is exactly where intra-op parallelism died — so **every
breaker's recombination is measured explicitly** (its serial fraction), not
assumed cheap.

---

## 6. Thread-safety (working the M4 enforcement checklist)

From the contract's checklist — these are the shared-state obligations, fixed in
Stage 0 before any parallel path engages:

1. **`_FOOTER_CACHE`** ([`parquet_read.pyx:73`](../opteryx/operators/parquet_read.pyx)) — declares itself thread-safe (RLock on its pool + LRU_K); **stress-verify under concurrent scan**, or make thread-local, when the scan operator is parallelised.
2. **`PipelineContext._terminated`** — replace the bare `bint` with a real atomic/event primitive. The API (`is_terminated()`/`terminate()`) already hides the primitive, so call sites are untouched.
3. **`Union` / `ExitNode` (SINGLETON)** — make EOS close-counting / result append atomic at the join point once N workers feed them.
4. **Exclusive morsel ownership (rule 1)** — the scheduler *guarantees* it: each morsel goes to exactly one worker; in-place mutators (Distinct, contract rule 2) depend on it.
5. **Borrowed `DrakenVector*` (rule 7)** — queues hold strong morsel refs only.

Already-safe and not re-litigated (contract §"What is already safe"): vectors
immutable post-construction, atomic refcounts, selection/validity pools,
`logical_type_intern` (mutex'd), ops table / allocator / kernel registry. The
`BLOOM_FASTPATH_COUNTER` race is already fixed.

**Validation:** mirror `test_grouped_engine_concurrency.py` at the *pipeline*
level — N OS threads each drive a full segment copy over a disjoint partition,
barrier-synced, output asserted byte-identical to the serial reference. This is
the gate before any stage proposes default-on.

---

## 7. Cancellation & errors

- **LIMIT short-circuit** calls `ctx.terminate()`; all workers check
  `ctx.is_terminated()` between morsels and stop promptly (generalises the
  `drive_scan` loop check).
- **Per-worker cleanup** mirrors `drive_scan`'s `try/finally`: on normal
  exhaustion, termination, exception, or abandonment, each worker closes its
  source (rugo C++ pipeline `cancel()`, WP-8) and the pool drains. The original
  exception is re-raised on the main thread after join (it.1's record + drain +
  re-raise pattern).
- **A worker that throws** records its exception, drains its queue so the
  scheduler's `put()` never deadlocks on a dead worker, and the main thread
  re-raises after join.

---

## 8. The flag & engine selection

One knob: **`MAX_EXECUTION_WORKERS`** (env/config, in `opteryx/config.py`),
**default 1 = serial engine, byte-for-byte today's path.** Selected at the clean
dispatch point in
[`opteryx/managers/execution/__init__.py`](../opteryx/managers/execution/__init__.py)
`execute()` — `> 1` routes to `parallel_engine.execute()`, `== 1` keeps
`serial_execute`. The parallel engine reuses `compile_pipeline` unchanged (the
segment cut is computed from the same wired graph).

Default-on is **not** part of this milestone's acceptance. It is proposed only
per-stage, only after the wide bench shows neutral-or-better **everywhere**.

---

## 9. Staged build plan (lowest-risk highest-value first; wide-bench gate each)

Every stage: `make q` 182/182 and `make tpch` 22/22 **identical** across
`MAX_EXECUTION_WORKERS ∈ {1,4,8}`; the wide ClickBench battery (W=1 vs W=N,
geomean + worst-case) is the perf gate; ships flag-off.

- **Stage 0 — infra, zero perf change. ✅ BUILT (2026-06-15).**
  - `MAX_EXECUTION_WORKERS` config flag (default 1) + the engine-selection seam
    in [`managers/execution/__init__.py`](../opteryx/managers/execution/__init__.py)
    (`>1` → `parallel_engine`, else serial — byte-identical).
  - [`parallel_engine.py`](../opteryx/managers/execution/parallel_engine.py):
    `resolve_worker_count` (sizing + cap-8) and `identify_segments` (the
    pipeline-segment cut, validated on linear/agg/distinct/sort/join plans).
    `execute()` **fails fast** (no silent serial fallback) — the per-segment
    parallel drive is Stage 1.
  - Pipeline-level concurrency stress harness exercising the central
    `CppThreadPool` as a clone-per-worker substrate
    ([`tests/unit/test_execution/test_parallel_segments.py`](../tests/unit/test_execution/test_parallel_segments.py),
    12 tests).
  - **Scope refinement (surfaced):** the §6 enforcement fixes (PipelineContext
    atomicity, Union/Exit close-counting, `_FOOTER_CACHE` stress) are **deferred
    to Stage 1**, where the concurrent push path that needs them lands — changing
    them in Stage 0 (no concurrent consumer) would be the premature hardening the
    contract warns against. The stress harness already proves the pool +
    clone-per-worker + merge substrate.
  - **Gate met:** `make q` 182/182 unchanged at W=1; new harness green.

- **Stage 1 — parallel stateless/mergeable pipeline. ✅ BUILT, MEASURED
  (2026-06-15). Correct everywhere; default STAYS OFF — wins narrow.**
  - Implemented for the grouped-aggregate pipeline (single scan →
    {Filter,Projection}* → Grouped Aggregate). W cloned chains, one worker thread
    each (no shared operator state), bounded queues, adaptive row-floor, recombine
    via WP-7 `merge()` into worker 0's populated engine, original breaker drives
    the serial tail. `parallel_engaged` telemetry; everything else serial.
  - **Correctness:** `make q` 182/182 and `make tpch` 22/22 IDENTICAL at default
    (W=1) AND force-engaged (W=4, floor=0); 16 unit tests incl. serial-vs-parallel
    differential.
  - **Wide bench (ClickBench GROUP BY, 92M rows, W1 vs W4): geomean 0.980×**
    (worst q34 0.83×, best q36 1.37×). Wins on medium-card (q16 1.22×) and
    computed-key (q36 1.37×); REGRESSES the very-high-card battery (q17/18 ~0.86×,
    q34/35 ~0.84×) — the **same Amdahl wall as it.1** (0.94×): these GROUP BYs are
    breaker-dominated, so the serial `merge()` swamps the parallel filter/scan
    gain. Pipeline-level framing does not escape `merge()` when the breaker *is*
    the cost.
  - **Favorable bench (low/med-card, scan/filter-heavy GROUP BY, 92M rows):
    geomean W4 1.27× / W8 1.28×** — pipeline-level parallelism IS confirmed where
    the thesis holds: L1 `RegionID COUNT` **2.09×/2.39×** (matches the old it.1
    "RegionID 2.6×"), L3 `RegionID + string-filter` **1.89×**. Three ceilings cap
    the rest, each a distinct future lever:
      1. **Mergeability** — `AVG(ResolutionWidth)` (narrow int) reports
         `is_mergeable()==False`, so the gate *correctly* keeps it serial (L2
         1.00×). Extending the mergeable collectors widens coverage (WP-7 work).
      2. **Serial scan pull** — decode-heavy / wide-projection queries (L4, wide
         URL) are bottlenecked on the single-threaded `_next_morsel_py()` pull,
         not the parallel filter/agg → ~1.0×. Parallel scan (workers pull own
         partitions) is the lever — needs the `_FOOTER_CACHE` thread-safety
         (enforcement checklist).
      3. **High cardinality** — the `merge()` Amdahl wall (Stage 4 no-merge
         exchange).
  - **Verdict:** ship flag-off (done). Stage 1's value is real but narrow
    (medium/low-card + computed-key, and — to be shown — filter/sort-dominated
    pipelines where the breaker is cheap). Broad high-card wins need the no-merge
    key-hash exchange (Stage 4) or an NDV gate (deferred by the architect).
    Two pre-existing bugs surfaced (reported): (a) grouped SUM/AVG over
    `orbitalPeriod` is non-deterministic garbage even serial (a type/reconstruct
    bug); (b) `merge()` of an EMPTY partial engine corrupts the AVG finalizer
    (the scheduler now skips empty engines).
  - *(superseded plan below)* Scan →
  {Filter,Projection}* → [mergeable agg]. Workers run the whole segment;
  recombine via existing `merge()`. This is it.1 lifted to the pipeline (filter
  + scan now parallel, not just agg-ingest). **Honest expectation:** wins where
  filter/scan dominate, neutral/loss where a single high-card agg dominates.
  **Engagement gate: a row-floor only** (architect decision) — stay serial below
  a row/morsel-count threshold (kills it.1's tiny-input regressions q37/38/42);
  no NDV gate. Default-off means high-card agg simply isn't engaged until Stage 4
  removes its merge. **Gate:** ClickBench geomean ≥ 1.0× with no material
  regression on the engaged set.

- **Stage 2 — parallel join probe.** Build once (parallel pre-filter +
  single build), probe in parallel over probe partitions against the shared
  read-only build table; concatenate. High value (join 54% released, probe
  usually dominant), no merge. **Gate:** TPC-H join queries + ClickBench.

- **Stage 3 — parallel sort / top-N.** Per-worker partition sort + k-way merge;
  per-worker bounded heap + heap merge for top-N. Sort 53% released. **Gate:**
  sort/top-N ClickBench queries.

- **Stage 4 — key-hash exchange (high-card breakers).** Only if Stages 1–3
  leave high-card grouped-agg/distinct as the gap. Requires a **native off-GIL
  scatter kernel** (Cython/C++ hash + counting-partition + native take) — the
  Python scatter is already falsified (0.62×). Removes the merge Amdahl term;
  unlocks COUNT DISTINCT / string MIN-MAX (no merge requirement). **Gate:** the
  it.1/it.2 high-card regressors recover to ≥ neutral.

- **Stage 5 — default-on proposal.** Only if the cumulative wide bench is
  neutral-or-better everywhere, possibly behind a cost-model/NDV gate that
  picks W per query. Architect decision, not automatic.

---

## 10. Architect decisions (RESOLVED 2026-06-15)

1. **Flag name & default** — ✅ `MAX_EXECUTION_WORKERS`, default 1 (off), cap 8.
2. **First slice** — ✅ **Stage 0 then Stage 1** (mergeable agg pipeline); the
   smallest provable slice that reuses the proven clone+`merge()` path.
3. **Engagement gate** — ✅ **row-floor only**, no NDV gate. Simpler; high-card
   agg stays flag-off until Stage 4's key-hash exchange removes its merge.
4. **Worker primitive** — ✅ the vendored **`CppThreadPool`** (BS::thread_pool),
   not stdlib threads; `third_party/moodycamel` lock-free queue available for the
   handoff only if a bench shows the stdlib queue dominating.
5. **Join build parallelism scope (Stage 2)** — recommendation stands:
   "build once, probe parallel" over a read-only shared table; full key-hash
   shuffle join deferred until Stage 4 demand is proven. (Revisit at Stage 2.)
```

---

## 11. Shuffle aggregation — NDV-selected (AGREED 2026-06-16)

Decided in discussion with the architect. This is the **next build** (it pulls
the old "Stage 4 key-hash exchange" forward — the bench showed round-robin+merge
is geomean-neutral on high-card GROUP BY, so the no-merge shuffle is the lever).

**Two strategies, selected by NDV (group-key cardinality) — NOT one path.**
NDV, not row count, is the signal: the merge/hash-table cost scales with NDV,
which is exactly what sank round-robin at high cardinality.

| input | strategy |
|-------|----------|
| < 250k rows | **serial** (split overhead not worth it; no NDV needed) |
| ≥ 250k rows, **low NDV** | **round-robin + `merge()`** (Stage 1, already built) — all W workers ingest, merge of small tables is cheap; this is where Stage 1 *wins* (L1 `RegionID` 2.4×) |
| ≥ 250k rows, **high NDV** | **shuffle into B bins, NO merge** (the new path) — disjoint keys per bin, finalize + concatenate |

Crossover ≈ where `merge()` stops being cheap (NDV ~100k–1m band); tunable.

**Bin count B is driven by NDV alone — decoupled from worker count W.** Bins
exist to keep each bin's hash table a sensible size (cache-fit / "grace"), not to
match thread count. B may be < W (low NDV → round-robin instead) or > W (high
NDV → work-stealing). Starting table (boundaries tunable — "this is just a
discussion"):

| NDV up to | bins |
|-----------|-----:|
| 250k | 2 |
| 1m | 4 |
| 10m | 8 |
| 100m | 16 |
| 1b | 32 |
| … | ×2 per ~decade |

**NDV source:** not needed below the 250k-row floor. Above it, **extrapolate NDV
from the first ~250k rows we already buffer** (a cheap runtime KMV/HLL over the
group key on that buffer) — won't be perfect, doesn't need to be, only the decade
matters. Use the optimizer's NDV sidecar when present. Bias upward when the
sample is "all-distinct" (sample under-estimates very high NDV); **grace
recursion** is the safety net for an under-estimate (a fat bin re-partitions).

**Shuffle is an OPERATOR (`ExchangeNode`), not engine-inline scatter.** Partition
*logic* lives in the operator; *threads* stay in the central pool (Volcano-style
exchange). `ExchangeNode.partition(morsel, B) → [B sub-morsels]`:
- routing hash = **german-string header `hash32`** for a single string key
  (`vector.hash_shaped()` reads the slot — zero arena re-hash), else the existing
  `morsel.hash_keys(cols)` (multi-column / fixed-width);
- `hash & (B-1)` (power-of-two radix) → counting-partition → native `take` per
  bin (off-GIL). The only genuinely new native code is the counting-partition.
- correctness: each sub-morsel exclusive to one worker (rule 1); identical keys
  route identically (deterministic hash); NULL keys → one bin → one group.

**Build order:**
1. Native counting-partition kernel + `ExchangeNode` (B bins) — the no-merge core.
2. NDV→bins table + runtime sample-KMV (optimizer NDV when available) + the
   low/high-NDV strategy switch (keep Stage 1 for low NDV).
3. Work-stealing drive (W workers pull B bins; needed since B can exceed W).
4. Grace recursion (fat-bin safety net) — last.

**Validate** after step 3 against the round-robin regressors (q34 URL, q16
UserID, q13 SearchPhrase): they must move from ~0.85× to a real win — the result
neither prior iteration achieved.
