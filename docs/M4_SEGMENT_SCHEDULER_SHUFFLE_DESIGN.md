# M4 — Morsel-Driven Parallel Execution (DuckDB model)

> Status: **PAUSED — execution-parallelism lever found EXHAUSTED on real workloads
> (2026-06-24).** Prototypes (G-A/G-B) and the bounded-adaptive switch validated *in
> isolation*; Stage 0 skeleton + `make m4-sweep` landed. But the build-and-measure pass
> showed the thesis does **not** speed up real queries, because the parallelism it targets
> is **already spent**: the parquet **decode is already 16-wide** (saturated from a single
> puller — `PARQUET_LOCAL_IO_WORKERS=16`, in-flight 18), and the **existing row-routing
> already extracts the agg-keying parallelism** (the 2.73×). Route-on-abandon and lockless
> pull both measured **neutral** against that real baseline (the prototypes' wins were vs a
> strawman in a *decode-free* synthetic harness). See "Outcome & honest post-mortem" below.
>
> **Kept (real):** the parallel-DISTINCT crash fix + `is_concurrent_pull_safe` capability
> (Stage 1 of the decode workstream) — a correctness win, perf-neutral.
> **Parked (gated off, no real-workload win):** `_grouped_agg_route` / `M4_ROUTE_AGG`,
> the scheduler skeleton (`M4_USE_SCHEDULER`), the lockless-pull change (reverted).
> **Where speed actually is:** not threads — per-query overhead (planning, `_split_morsel`,
> output-queue sync) + §12 read-less + faster per-core kernels.
>
> The DuckDB-vs-Spark model choice below remains correct *if* execution parallelism is ever
> revived for a workload that is genuinely sink-bound (high-card, narrow-column, aggregation-
> heavy) — but that is not ClickBench/TPC-H, which this pass proved are decode/overhead-bound.

## Context

The 6-month engine rewrite's final leap. Today the data executor
(`opteryx/managers/execution/parallel_engine.py`) only parallelises **single-scan**
pipelines; anything multi-scan (every join, set-op) falls to `_serial_stream` and
runs **single-threaded**. `identify_segments` already cuts the plan into segments
at breakers, but nothing consumes that graph as a schedule.

Goal: **morsel-driven parallelism** — the plan is cut into pipelines at breakers;
a pipeline is run by N identical tasks racing through the *same* operator chain on
*disjoint* morsels; parallelism is a property of the pipeline, never of an
operator. There is **no exchange operator and no materialized cross-pipeline
shuffle** — partitioning is folded into each sink's thread-local state.

Runtime is free-threaded CPython 3.14t (`sys._is_gil_enabled()` is False) — real
threads, no GIL. Residual per-object refcount contention is avoided by keeping
morsels in C++ (below), not by locking.

## Why not Spark

Spark materializes a partitioned intermediate to a shuffle buffer, hard-barriers,
then the next stage reads it. That is the model we were drifting toward (explicit
Exchange operator, `$bin` matrix, blocking shuffle, shuffle-both-join-sides) and
it is **slow**: a full intermediate materialization and a stage barrier on every
breaker. DuckDB avoids all of it — partitioning lives inside the sink, pipelines
stream, ordering is an event dependency, and the probe side never materializes.
We take the DuckDB model.

## Prime constraint — the serial path is the asset (locked)

We are already **~2× DuckDB per core**. The entire gap to DuckDB is the serial
ceiling, not single-thread efficiency. So this rewrite is **not a speed recipe** —
it removes a self-imposed ceiling on an already-superior core. The prime directive:
**spend cores without spending the single-thread lead.** The model only needs to be
a coherent way to scale; it must not tax the thing we already won.

This makes the cost model **DOP-proportional, not fixed**:

- **Partition degree scales with DOP.** Radix fan-out, thread-local fan-out,
  Combine, and the k-way merge tail appear ONLY when there is parallelism to pay
  for them. At `DOP=1` there is **1 partition**.
- **`DOP=1` ≡ today's serial path.** The whole local/global apparatus collapses to
  the current lean single-instance operator when there is one worker — no radix
  overhead, no Combine, no merge. The model is *invisible* at one core.
- This is the **opposite of DuckDB**, which pays radix-partitioning even
  single-threaded. We do not — our single-thread path is the asset to protect, and
  we are not chasing DuckDB's per-core number (we already beat it).

**Gate: single-thread (`DOP=1`) must be within noise of the current serial engine.**
A single-thread regression means the rewrite is wrong — the model is meant to be
free at one core and multiplicative above it. Above `DOP=1`, scaling efficiency is
the only metric that matters.

## Model (locked)

- **Pipeline** = source → 0..N streaming operators → one sink. The unit of
  scheduling and parallelism. A **breaker** is a sink: it fully consumes its input
  before producing output, ending one pipeline and beginning the next.
- **Within a pipeline, morsels stream** end-to-end through the operator chain;
  there is no materialized shuffle between operators.
- **Ordering is an event dependency, not a barrier-lock.** Build-before-probe and
  aggregate-before-read-out are edges in an event DAG (two-counter events:
  runnable when dependencies finish, complete when tasks finish). Independent
  pipelines overlap.
- **No exchange operator.** Every partitioning consumer (agg, join-build,
  distinct) radix-partitions *its own input* inside its sink. A breaker that
  re-groups by a new key just partitions by that key internally — the upstream
  breaker's read-out streams into it. Nothing materializes a cross-pipeline
  shuffle.
- **Source parallelism** is row-group handout under a short cursor lock; the scan
  is the only "partitioning" that isn't key-based.

## Partitioning is internal to the sink (locked)

The lock-free substrate (DuckDB §12, §14), folded into each breaker:

- **`GlobalSinkState` once / `LocalSinkState` per task.** Hot-path mutation
  touches only local state.
- **`Sink(chunk, local)`** ingests into thread-local, **radix-partitioned** state
  (hash the key, partition by the high/low bits into `radix` partitions).
- **`Combine(local → global)`** at task end is a **partition-aligned pointer
  hand-off** — move each local partition's fragment into the global partition's
  list. **O(partitions), no rehash.** (Not the O(groups) per-group merge we have
  today — that is the bottleneck this design exists to avoid.)
- **Read-out is a parallel `Source`** — each task finalizes-and-scans one
  partition; no group spans two partitions, so there is no serial global merge.
- `radix` bits are fixed for v1; **raising them under memory pressure** is the
  spill hook (deferred).

## Per-breaker (locked)

- **Aggregate** — thread-local radix HT; **bounded-adaptive local pre-agg**: low
  cardinality stays local (shuffle ~NDV), and on overflow the local table flushes
  (DuckDB's `Abandon()`), degrading gracefully. Partition-aligned Combine,
  parallel read-out. Holistic aggs (MEDIAN/COUNT DISTINCT) skip the local table.
  *(This resolves the former OPEN reduce-model question.)*
- **Inner/equi join — stream the probe.** Build side: thread-local
  `JoinHashTable`s, merged into **one shared immutable table** (parallel
  pointer-table init + finalize). Probe side: a **streaming operator** in the
  probe pipeline that looks up the read-only table — **no probe partitioning, no
  probe materialization.** Probe parallelism is inherited from the probe scan's
  morsels. *(This resolves the former OPEN probe-shuffle question — DuckDB's way.)*
  Outer joins track unmatched build rows for emission. **Keyless joins (CROSS /
  non-equi): serial fallback.**
- **Sort** — parallel **local sort** per task + a **k-way merge** tail (a
  deliberate partial serial tail).
- **Distinct** — like aggregate (radix-partitioned dedup, parallel read-out).

## Degree of parallelism (locked — data-bounded)

DOP per pipeline = `min(source morsels, operator hints, scheduler threads)` — the
principled version of a row-floor gate. Tiny inputs never oversubscribe and pay no
recombination tax, with **no tuned floor constant**. Supersedes the fixed
`resolve_worker_count` + `PARALLEL_MIN_ROWS`.

**Partition degree is derived from DOP** (see Prime constraint): 1 partition at
`DOP=1`, scaling up only with worker count — so the partitioning apparatus is never
paid on the serial path. A breaker at `DOP=1` is the current single-instance
operator, unchanged.

## The operator contract — a C++-first rewrite (the real work)

The scheduler is the easy half (prototype-proven). The body of work is rewriting
breakers to the local/global contract, in C++:

| Axis | Today | Target |
|---|---|---|
| State | one instance owns everything; scheduler clones it | `GlobalSinkState` once + `LocalSinkState` per task |
| Merge | today's serial path does **no** merge (row-routes disjoint key slices); the reverted hash-partition experiment merged per-group — **O(groups)**, the wall this design exists to avoid | partition-aligned hand-off — **O(partitions)**. **At high cardinality the pointer hand-off alone does NOT scale** (G-A: 1.0–1.5×); the Sink must *route raw to partitions on overflow* (Abandon) so the single aggregation pass happens **once per partition in parallel read-out** (G-A: 4.8× @ DOP8). Abandon is the mechanism, not the pointer hand-off. |
| Drive | one synchronous `push() nogil` | `Sink`/`Combine`/`Finalize` phases |
| Read-out | finalize on the instance, ~serial | parallel `Source`, one partition per task |

Rule established by the prototype: **bind orchestration to Python, never the
morsel.** Morsels stay C++ (`CxxMorsel` / `shared_ptr[CxxMorsel]` — the carrier
flip already does this); the per-task push loop runs in C++ with the GIL released;
only the scheduler crosses into Python. This **dissolves the free-threaded
refcount question** (no PyObject per row) and the nanobind type clash.

**Hard prerequisite (not a parallel convergence): the GIL must be genuinely off
the operator-chain push *bodies* before Stage 1.** The carrier flip
(`docs/M4_CPP_MORSEL_DESIGN.md`) is structurally landed — the chain currency is
`shared_ptr[CxxMorsel]` and the push signatures are `noexcept nogil` — but the
bodies still run under a transitional `with gil:` (only grouped-agg ingest has been
released so far). Building local/global breakers on a chain that still takes the GIL
per body means every scaling measurement is taken against a GIL ceiling and model
wins are indistinguishable from lock contention. Finish the carrier-flip GIL
release first; then start Stage 1.

## Deferred from DuckDB (deliberate v1 cuts — not Spark-isms)

- **Cooperative `BLOCKED` / `InterruptState` async machine** — our tasks run to
  completion; no in-pipeline I/O overlap or backpressure yet. §19.4 says it is
  what makes a GIL-off carrier fully pay; revisit after v1.
- **Spill / memory-awareness** — v1 is in-memory. Cheap to add later: DuckDB
  spills by **raising radix bits inside the sink** (more, smaller partitions,
  processed/spilled one at a time) and by spilling probe data to the external
  probe path — both stay within this model, no new architecture.

## Prototype validation (`scratch/ddb_proto/`)

Isolated (reuses only the draken Morsel + headers). Python scheduler (event DAG,
dependencies, data-bounded DOP) + C++-native operators (`_cops.cpp`). Morsels never
cross into Python; GIL off; results checked vs oracles.

| Shape | Demo | Proven | DuckDB-aligned? |
|---|---|---|---|
| Stateless (scan/filter/project) | `demo_cpp.py` | fused C++ push loop, no per-morsel cast | ✓ |
| Sort breaker | `demo_sort.py` | local sort + k-way merge | ✓ |
| Aggregate breaker | `demo_agg.py` / `demo_agg_bench.py` | *correctness* of thread-local radix HT + O(partitions) Combine + parallel read-out; **G-A scaling** showed thread-local HTs flat at high card, route-on-abandon = 4.8× | ✓ shape; scaling needs Abandon (see G-A) |
| Inner join breaker | `demo_join.py` (old, Spark-style) → `demo_join_stream.py` (G-B) | **stream-the-probe**: shared immutable table + streaming probe + parallel outer-unmatched; correct, probe 6.19× @ DOP8 | ✓ (redone — G-B passed) |

The join demo has been rebuilt to match the model (shared-table build + streaming
probe) in `demo_join_stream.py` — see Gate G-B (passed).

## Substrate to reuse

`CppThreadPool`; `pull_one` (reentrant scan); `_clone_op`; `identify_segments` +
`OperatorParallelism`; `PipelineContext` (terminate / `_exc`; note
`set_expected_input_closes` does **not** exist yet — only the
`_expected_input_closes` attribute — the scheduler must add the setter); exported
C-ABI handles (`cxx_morsel_raw_ptr`,
`cxx_take_c`) for out-of-tree C++. The agg engine's `merge()` is the embryo of
`Combine` but must become **radix-aligned (O(partitions))**. **Supersedes**
`_ScatterCollectEngine`, `_DistinctCollector`, `_parallel_engines`, the four
`_find_parallel_*` finders. (No exchange operator / `$hash`/`$bin` columns / shuffle
matrix — those were the Spark design and are dropped.)

## Components to build

1. **Event-DAG scheduler** — pipelines from `identify_segments`, two-counter
   events, dependency edges (build-before-probe, agg-before-read-out),
   data-bounded DOP, producer/consumer pools. 🔶 *Skeleton landed (Stage 0): events +
   pipeline decomposition + CppThreadPool streaming hand-off behind `M4_USE_SCHEDULER`,
   serial-identical. Dependency edges / data-bounded DOP > 1 arrive with the breaker
   rewrites.*
2. **C++ breaker rewrites** to local/global + `Sink`/`Combine`/`Finalize`, each
   **radix-partitioning its own input internally**:
   - aggregate (bounded-adaptive, O(partitions) Combine, parallel read-out)
   - join (thread-local tables → shared table → **streaming probe**)
   - distinct (like aggregate)
   - sort (local sort + k-way merge)
3. k-way merge kernel for sort.
4. Parallel read-out `Source` role on breakers.
5. Retire the single-scan finders/seams.

## v1 parallel surface (explicit scope)

What this plan makes parallel: **grouped/ungrouped aggregate, distinct, inner/equi
join (streaming probe), sort.** What stays **serial in v1**, by deliberate cut:

- **CROSS / non-equi joins** (keyless — no partition key).
- **Window functions** (`WindowNode`).
- **Set-op `ALL`** (INTERSECT/EXCEPT ALL ride on ROW_NUMBER + semi/anti join; they
  inherit join parallelism only once Stage 4 lands, and the ROW_NUMBER prefix stays
  serial).
- **LIMIT-only / subquery / projection-only** segments.

This is stated so "the join parallelised but my windowed query is still
single-threaded" is a documented boundary, not a surprise. None of these are
architecturally blocked — they are out of v1 scope.

## De-risking gates (prototype-tier, before the matching engine stage)

The prototype proved the *shapes run correctly vs. an oracle*. It did **not** prove
the two things this project can actually fail on. Each gate below is cheap
(prototype / microbench, no engine rewrite) and **must pass before its engine stage
begins** — so a failure costs a microbench, not a breaker rewrite.

- **Gate G-A → blocks Stage 2. ✅ PASSED (conditionally) — `scratch/ddb_proto/demo_agg_bench.py`.**
  Microbenched the high-cardinality regime (580k groups, 9.6M rows) at DOP 1/2/4/8.
  Result, and the condition it imposes on Stage 2:
  - **Thread-local full hash tables do NOT scale** (1.0–1.5×; fat radix *worse* at
    0.51×). With round-robin morsels every thread's local HT covers most of the key
    space → total build work multiplies by ~DOP, `sink` wall-clock never shrinks.
    This faithfully reproduces the prior reverted result.
  - **Route-raw + aggregate-once (the design's `Abandon()`) scales: 2.33× / 3.14× /
    4.80× at DOP 2/4/8.** The Sink only hashes+routes raw keys to partition buffers;
    the single aggregation pass runs once per partition in the parallel read-out.
  - **Verdict:** Stage 2 is justified **only if built as route-on-abandon, not
    thread-local full HTs.** Bounded-adaptive Abandon is load-bearing, not optional.
  - **Wide-payload qualifier — CLOSED.** Re-ran with a multi-aggregate payload
    (COUNT+SUM+MIN+MAX, the Q23/Q31/Q33 shape): route-on-abandon scales **4.87×** at
    DOP8 (vs 4.80× on bare COUNT(*)) while the HT strawman gets *worse* (0.51×) — the
    bigger per-group state multiplies harder. HT and route cross-check **identical**
    (580k groups, 2.784M rows), so the win is not a corner cut.
  - **String keys:** at the aggregate breaker a draken string key reduces to a
    fixed-width hash slot (hash-only equality, hash computed at scan), so it behaves
    as the int-hash case already proven — not a separate risk for the breaker. (The
    real-data Q34/Q13 string-key flatness is end-to-end and likely scan-bound, a
    separate axis the agg breaker does not own.)
  *Why this gate existed:* intra-op agg parallelism was already tried and **reverted**
  — round-robin (0.94×) and hash-partition (0.62–0.72×) both **lost** on ClickBench
  because recombination cost ≈ aggregation cost at high NDV.
- **Gate G-B → blocks Stage 4. ✅ PASSED — `scratch/ddb_proto/demo_join_stream.py`.**
  Rebuilt the join as **stream-the-probe** (the old `demo_join.py` is the disowned
  Spark-style co-partitioned shuffle). Proven in isolation:
  - (a) thread-local build collection → ONE shared **immutable** table (finalize);
  - (b) **streaming probe, no probe materialization** — probe parallelism inherited
    from the probe scan's morsels;
  - (c) **parallel emission of unmatched build rows for outer joins** (per-build-row
    matched flag set during probe, then a parallel range-claimed pass).
  - **Correctness:** inner match count/sum AND outer unmatched-build count/sum match
    the oracle, including build keys that never probe.
  - **Probe scaling:** 2.14× / 4.21× / **6.19×** at DOP 2/4/8 on a fixed read-only
    table (12.8M probe rows) — near-linear, no recombination tail (there is nothing
    to recombine on the probe side; that is the model's advantage over the shuffle).
  - **Verdict:** the highest-risk stage now has a validated prototype. Stage 4 builds
    this shape (keyless/non-equi joins still fall back to serial per the design).

## Implementation plan (staged)

Sequencing falls out of two lessons — the scheduler is the easy half, and `DOP=1`
must never regress — plus the ClickBench read: start where headroom *and* our lead
concentrate (the heavy agg/scan band, Q17–Q35). Each stage ships green; the gate is
the same every time.

**Cross-cutting gate (every stage):** `make q` (190) + tpch (22) + clickbench (43)
green · result-identical to serial (`workers=1` vs `N`) · **`DOP=1` within noise of
today's serial path** · orchestration crosses to Python, never the morsel.

**Status (2026-06-24) — all landed work is gate-green (q190 · tpch22 · cb43):**
- ✅ **Both prototype gates passed.** G-A (route-on-abandon agg scales 4.80×, and
  **4.87× with a wide multi-aggregate payload** — the qualifier is closed; thread-local
  full HTs do NOT scale). G-B (stream-the-probe join: correct inner+outer, probe 6.19×
  @ DOP8, no probe materialization).
- ✅ **Bounded-adaptive switch prototype-validated** (`demo_agg_adaptive.py`) — the last
  untested Stage-2 piece. LOW card stays pre-agg (abandoned 0%, no route tax → prime
  constraint holds), HIGH card abandons → routes (abandoned 100%, scales 4.42×), both
  correct vs oracle. The Stage-2 route-raw engine design is now fully prototype-backed.
- ✅ **Stage 0 landed.** DOP-sweep harness `make m4-sweep` (`dev/m4_dop_sweep.py`) +
  event-DAG scheduler skeleton `opteryx/managers/execution/scheduler_engine.py` behind
  the `M4_USE_SCHEDULER` flag (default off; `parallel_engine` untouched so the two
  compare at DOP=1). Serial-identical no-op (effective DOP pinned to 1 — operators not
  yet breaker-split); `make q` 190/190 both flag states. Decision: kept the floor
  constants for now (data-bounded DOP computed but capped at 1 in the skeleton).
- ✅ **Real-data baseline captured** (`make m4-sweep M4_DATASET=scratch.hits`, current
  engine): parity OK at every DOP. **The GIL go/no-go is answered green** — plain int-key
  GROUP BY (Q16) scales **2.73× @ DOP8** through the already-nogil aggregate ingest, so
  releasing the GIL pays off. High-cardinality string-key band (Q13/Q34) is flat, but
  that is end-to-end / likely scan-bound, a separate axis the agg breaker does not own.
- 🔶 **Stage 1's "carrier-flip GIL-off bodies" prereq is partial** (tracked in
  `docs/M4_CPP_MORSEL_DESIGN.md` Part D, S-A/S-B). Done: the S-B.1 carrier flip
  (`shared_ptr[CxxMorsel]`), nogil grouped-agg ingest, and the expression VM is now
  c-native (no Python closure) across numeric, bitwise, string, interval, temporal,
  and decimal (same-kind, ×float, ×int64, DECIMAL128/cross-kind via int128 widening).
  The c-native compare fast-path for `col OP literal` was resurrected (was dead on a
  type-coercion gap), which also lit up a nogil filter `_dispatch_push`. Not done: full
  nogil operator bodies across the chain (S-B.3+) — the heavy loops already run nogil;
  what remains is lightweight glue with a small marginal dividend.

## Outcome & honest post-mortem (2026-06-24) — the lever was already spent

The build-and-measure pass (Stage 0 skeleton → route-raw increment 1 → parallel-decode
Stage 0/1/2) reached a **negative result** that supersedes the optimistic status above.
Recording it so the next person does not re-run the same loop.

**What was measured (free-threaded 3.14t, real parquet):**
1. **Route-on-abandon (`_grouped_agg_route`, increment 1) ran the FULL ClickBench suite:
   neutral.** It matches the existing row-routing, doesn't beat it.
2. **Lockless pull (the "parallel decode" lever): neutral.** Head-to-head 40.0ms (lockless)
   vs 41.1ms (locked) on a genuinely parallel query — within noise. **Reverted.**
3. **Decode/agg split** (workers=1, projection proxy vs GROUP BY): decode ~37–52%, agg the
   rest. Then: puller DOP 1→8 flat; `decode_workers` 1/2/4/16 flat; freeing cores flat.
   cProfile: a ~40ms ClickBench-scale query is **per-query-overhead-bound** (planning ~4ms,
   `_split_morsel` on the consumer thread, output-queue lock sync), with decode already
   16-wide and worker compute a minority.

**Why the prototypes lied.** G-A/G-B and the bounded-adaptive bench all scaled (4.8×/6.2×/
4.4×) — but against a **strawman** (per-thread full hash tables) in a **decode-free,
overhead-free synthetic harness** (`CScanSource` is an in-memory generator). The real
baseline is the shipped **disjoint row-routing**, which already extracts the agg-keying
parallelism, over a scan whose **decode is already parallel**. There was no idle
parallelism left for route-on-abandon or lockless pull to capture. The prototype gates
validated the *shape*, not the *win against the real competitor* — that was the gap.

**Root cause of "M4 can't speed up ClickBench/TPC-H":** the parallelism this design targets
(the aggregate sink, the scan pull) is **already parallel**. Decode is 16-wide and saturated
from one puller; agg-keying is already row-routed (the 2.73×). M4 execution-parallelism is
therefore **structurally unable to move these workloads** — it is not a missing optimization.

**What was actually kept (real, gate-green):**
- The **parallel-DISTINCT crash fix** + `is_concurrent_pull_safe()` capability + regression
  test (`tests/unit/test_execution/test_concurrent_pull_capability.py`). Correctness win,
  perf-neutral. `_distinct_stream` previously self-pulled lockless over non-reentrant
  sources → crashed at W≥2; now gated.

**Parked (correct, gated off, no real-workload win):** `_grouped_agg_route` (`M4_ROUTE_AGG`),
the scheduler skeleton (`M4_USE_SCHEDULER`), all prototypes (`scratch/ddb_proto/`). Revive
only for a workload proven **sink-bound** (high-cardinality, narrow-column, aggregation-heavy)
— measured, not assumed.

**Where speed actually is (the redirect):** not threads. (1) **Per-query overhead** — planning,
`_split_morsel`, output-queue sync are a large serial fraction at query scale. (2) **§12
read-less** — pushdown / pruning / bloom / indexes / materialized views. (3) **Faster per-core
kernels** — SIMD decode, cheaper expression/agg kernels (cores already saturated).

| Stage | Scope | Primary area | Risk |
|---|---|---|---|
| **0 · Scheduler skeleton + harness** ✅ **LANDED** | DOP-sweep `make m4-sweep` target built first (certifies later stages); event-DAG scheduler skeleton behind `M4_USE_SCHEDULER`, drives existing operators, serial-identical at DOP=1. Floor constants kept (data-bounded DOP computed but capped at 1 until breaker-split lands). `set_expected_input_closes` not needed — reused `compile_pipeline`+`drive_scan` wholesale. | `opteryx/managers/execution/scheduler_engine.py` (+ `dev/m4_dop_sweep.py`) | done |
| **1 · Aggregate → contract, `DOP=1`** ⬜ *(shrank — see plan correction below)* | DOP=1 ≡ today's incremental pre-aggregate path (bounded-adaptive never Abandons at low card/one worker), so Stage-1-distinct work is just the **`Abandon()` switch hook + `_dop`/`_partition_count` field**. The substance moved into Stage 2. NOTE: there is **no per-group Combine** to build (that was the reverted WP-7 model); route-on-abandon's Combine is a raw-fragment hand-off. | `operators/grouped_aggregate_hashed/` + draken `GroupHashEngine` | low — switch hook only |
| **G-A** | ✅ *Combine microbench gate — PASSED conditionally (above). Verdict: route-on-abandon scales (4.8×), thread-local full HTs do not.* | `scratch/ddb_proto/demo_agg_bench.py` | — gate |
| **2 · Aggregate parallelism on** ⛔ **PARKED** | Route-on-abandon built (increment 1, `_grouped_agg_route` / `M4_ROUTE_AGG`) → **NEUTRAL on full ClickBench** vs the shipped row-routing (post-mortem above). Correct + gated off; not pursued further (no real-workload win). | same | — parked |
| **3 · Distinct** ⛔ **PARKED** | Mirror of agg — same neutrality expected (agg-keying already parallel). Not built. | `operators/distinct/` | — parked |
| **G-B** | ✅ *Stream-the-probe prototype gate — PASSED in isolation; BUT see post-mortem: prototype wins were vs a strawman in a decode-free harness. Join not built.* | `scratch/ddb_proto/demo_join_stream.py` | — gate |
| **4 · Inner/equi join** ⛔ **PARKED** | Multi-scan join parallelism — the one stage with a *distinct* unbuilt shape (joins fall to `_serial_stream` today). Only revive if a join-heavy workload is proven sink/probe-bound, not decode/overhead-bound. | `operators/hashed_inner_join/` + join engines | — parked (highest residual value if revived) |
| **5 · Sort** ⛔ **PARKED** | Parallel local sort + k-way merge. Not built. | `operators/sort/` | — parked |
| **6 · Cleanup** ⬜ | If parked permanently: retire the single-scan finders/seams + the parked M4 scaffolding (`_grouped_agg_route`, `scheduler_engine.py`, `scratch/ddb_proto/`). | `parallel_engine.py` | low |

**Critical path:** ⛔ **HALTED at Stage 2.** Stages 0/G-A/G-B and route-raw increment 1 are
done, but Stage 2 measured **neutral on real workloads** (post-mortem above), so the
agg/distinct/sort line is **parked** — execution-parallelism is already spent on these
workloads. **The only stage with genuinely unbuilt parallelism is Stage 4 (multi-scan join)**,
since joins still fall to `_serial_stream`; revive it **only** if a join-heavy workload is
*measured* sink/probe-bound. **Next action is NOT a stage here** — it is the redirect: cut
**per-query overhead** (planning / `_split_morsel` / output-queue sync) and §12 **read-less**,
where ClickBench/TPC-H time actually lives (post-mortem). See `[[m4_delivery_ga_stage0]]`.

**Validation harness (Stage 0's first deliverable, not an afterthought):** extend
the prototype's `DOP=1` gate into a `make` target — a DOP sweep (1/2/4/8) proving
both serial parity at `DOP=1` and scaling above it, on the Q17–Q35 band first. No
stage can be certified without it; that is why it leads Stage 0.

**Deferred (named, not on the path):** radix-join-as-second-strategy (above
`DOP=1`); the cooperative `BLOCKED` async machine; spill (slots into the sink via
radix-bit raising). None blocks v1.

## Stage 1 — execution plan (grounded in the current engine)

Current state (`opteryx/operators/grouped_aggregate_hashed/`, surveyed 2026-06-24):
ONE `GroupHashEngine` instance per node (`_node.pxi:87`), a single flat
`CarcharIndex` + `KeyStore` + per-collector typed state. `ingest()` (`_engine.pxi:335`)
IS the Sink (keying → store → grow → accumulate; nogil span `_ingest_cxx_span`
`_engine.pxi:398`). `finalize_morsels()` (`_engine.pxi:769`) IS Finalize (serial
reconstruct + per-collector finalize + chunk). **No `merge()` / Combine exists** — the
parallel path (`parallel_engine._grouped_agg_stream`) row-routes disjoint key bins to
W cloned engines (`_clone_op`) and concats at finalize, so no group is ever shared.
No radix/partition dimension anywhere.

**The collectors are the risk.** ~20 collector subclasses (`_collectors_*.pxi`) each
hold a flat typed state vector indexed `[group_slot]`. A partition dimension must
re-index every one of them; a single off-by-one silently corrupts aggregates engine-
wide. So Stage 1 keeps DOP=1 byte-identical and defers the partition data-layout to
Stage 2 — Stage 1 establishes the **contract surface + the Combine seam only.**

⚠️ **PLAN CORRECTION (2026-06-24, verified against the G-A prototype
`scratch/ddb_proto/_cops.cpp` `CAggRoute`).** An earlier 1a/1b/1c decomposition
("name the contract → per-partition Combine → partition the collectors") modeled the
*rejected* pre-aggregate-then-merge approach and is WRONG for route-on-abandon. In the
proven winning shape: the Sink **routes raw key+value to per-partition buffers and does
NOT aggregate** (`local[part(k)].push_back(k)`); `Combine` is an O(partitions) pointer
hand-off of the **raw fragment lists** (`global_raw[p].push_back(std::move(local[p]))`),
NOT a per-group state merge; **read-out aggregates each partition exactly once**. So:
- There is **no per-group `Combine`/`merge_state`** to build — that was the WP-7
  pre-aggregate model (reverted, `[[m4_parallel_group_agg_built]]`). Don't re-add it.
- Partitioning today's *pre-aggregate collector state* (the old "1c") is also wrong —
  route-on-abandon bypasses incremental aggregation; the partition buffers hold **raw
  routed rows**, not accumulated state.

**Corrected decomposition (route-on-abandon model):**
- **DOP=1 / low-cardinality = today's path, unchanged.** Bounded-adaptive keeps the Sink
  in the current incremental pre-aggregate mode (one local HT) until it overflows; it
  never Abandons at low card / one worker. So there is little *distinct* "Stage 1 engine
  work" — today's `ingest`/`finalize` ARE the DOP=1 path. The only Stage-1-shaped piece
  is the **`Abandon()` switch hook** (overflow detector that flips the Sink from
  pre-aggregate to route-raw) + the `_dop`/`_partition_count` field that drives it.
- **The real engine work is Stage 2** (and it is one coherent build, not three sub-steps):
  (i) a **route-raw Sink** path — buffer (key-hash, value) into `radix` per-partition
  fragment lists instead of accumulating; (ii) **`Combine` = O(partitions) raw-fragment
  hand-off** (move local fragment lists into the global per-partition lists); (iii) a
  **parallel per-partition read-out** that aggregates each partition once (this is where
  the existing collectors finally run — over one partition's routed rows, no cross-
  partition merge since hash co-locates a group in exactly one partition).
- **Gate**: DOP=1 stays byte-identical (bounded-adaptive → pre-aggregate path);
  at DOP>1 the route-raw path must match a DuckDB oracle (SUM/MIN/MAX/AVG/COUNT/DISTINCT,
  NULL keys, multi-col keys) AND beat serial on the high-card band (the G-A 4.8× shape).

**Net:** Stage 1 shrinks to the Abandon-switch + DOP field; the substance moves into
Stage 2's route-raw rewrite. This is the careful, central-operator change — do it with
full attention, oracle-verified, DOP-sweep-gated. **A grouped-agg bug is silent +
engine-wide.**

## Stage 2 — route-raw engine design (grounded)

The substance of the whole agg rewrite. Grounded in the current engine
(`GroupHashEngine`, `_engine.pxi`), the Stage-0 scheduler (`scheduler_engine.py`), and
the proven prototype (`scratch/ddb_proto/_cops.cpp` `CAggRoute`, G-A: 4.80× / 4.87×).

**The real lever vs today's parallel path.** Today's row-routing
(`_grouped_agg_stream`) has a **serial scatter**: the main thread routes every morsel
into W bins, then workers key their bin. That serial O(N) scatter is the Amdahl
ceiling (why real-data int-key tops out ~2.73×). Route-on-abandon's win is a **parallel
sink**: each worker pulls its *own* morsels and routes them to its *own* thread-local
per-partition buffers — no serial scatter at all. That is the difference the prototype
measured, and it is what Stage 2 must realize.

**Four pieces (one coherent build):**

1. **Parallel route-raw Sink (per task, `LocalSinkState`).** A sink task pulls morsels
   off the shared scan (`pull_one` under the cursor lock, already reentrant), and for
   each row appends `(key-hash, key-column slices, value-column slices)` into one of
   `radix` thread-local **column fragment buffers** by `hash & (radix-1)`. **Columnar
   append, not row-by-row** — gather the routed row indices per partition, then
   `cxx_take`/typed-append the key+value columns into that partition's fragment (reuses
   the §11 take path; no PyObject per row). No hash table, no accumulation in the Sink.
2. **Bounded-adaptive (the `Abandon()` switch). ✅ PROTOTYPE-VALIDATED**
   (`scratch/ddb_proto/demo_agg_adaptive.py`, `CAggAdaptive`). Start each task in the
   **current incremental pre-aggregate mode** (one local `GroupHashEngine`-style HT).
   Track the local table's reduction (rows-in / distinct-groups). While reduction is
   high (low cardinality) **stay local — this IS today's path**. When the local table
   exceeds a size threshold *and* reduction is poor (high cardinality), **Abandon**:
   flush the local HT's current groups as a per-partition fragment and switch that task
   to pure route-raw for the rest of its input; read-out folds both the flushed partials
   and the routed raw. Measured: **LOW card → abandoned 0% (stays pre-agg, scales 3.63×,
   no route tax), HIGH card → abandoned 100% (routes, scales 4.42×), both correct vs the
   oracle** (the mid-stream switch is sound). → DOP=1 / low-card never Abandons ⇒
   **byte-identical to trunk**; only high-card / DOP>1 routes (where G-A proved it wins).
3. **`Combine` = O(partitions) hand-off (`GlobalSinkState`).** At task end, move each
   local partition's fragment list into the global partition's list under a short lock
   (`global_frags[p].push_back(move(local[p]))`) — O(radix), no rehash, no per-group
   work. (This replaces the rejected O(groups) merge.)
4. **Parallel per-partition read-out (`Source`).** Each read-out task claims a partition
   via an atomic counter and aggregates **that partition's fragments exactly once** — a
   group's hash lands in exactly one partition, so no cross-partition merge. **This is
   where the existing collectors run**: build the partition's hash table over its routed
   rows, drive `grow`/`accumulate` (the same nogil collector path as today, just
   deferred from ingest to read-out), reconstruct keys (`KeyStore`), emit chunks. Plus
   any fragments flushed by Abandon (already-aggregated partial groups) folded in.

**Scheduler integration.** This is exactly the event-DAG the Stage-0 skeleton stands up:
a **sink event** (N tasks: route-raw, end with Combine) → dependency edge → a **read-out
event** (≤radix tasks: aggregate one partition each, emit). `radix = next_pow2(DOP)`
(≥ DOP so read-out has parallelism); `DOP=1 ⇒ radix=1 ⇒` one partition, sink stays
pre-aggregate, read-out is the single serial finalize = today.

**DOP derivation.** `_dop = min(source morsels, scheduler threads)` (data-bounded, §DOP);
`radix` derived from `_dop`; pre-agg-vs-route decided per-task by bounded-adaptive. The
`M4_USE_SCHEDULER` flag gates the whole path; `parallel_engine` stays as the off-path.

**Open questions for the architect (decide before building):**
- **Fragment representation.** Per-partition fragments as appended `CxxMorsel` slices vs
  raw typed column buffers (key-hash + value columns)? The take-per-partition cost is
  the new per-row work the Sink adds — measure it against the serial-scatter it removes.
- **Abandon threshold.** Size + reduction cutoff that flips pre-agg→route. DuckDB uses a
  fixed local-table cap; we need one that keeps DOP=1 low-card on the pre-agg path
  (the byte-identical guarantee) — start conservative, tune on the Q17–Q35 band.
- **Holistic aggs (MEDIAN / COUNT DISTINCT / approx).** They cannot pre-aggregate; in
  route-raw they buffer raw values per partition and finalize at read-out (already their
  shape). Confirm the dual-interface collectors (`accumulate_gil`) work in the read-out
  task, not the sink.
- **Memory.** Route-raw buffers all surviving rows for a partition until read-out (vs
  today's incremental fold). Bounded by one partition's routed rows; the spill hook
  (raise radix) is the release valve (deferred). Confirm the high-card worst case fits.

**Build order (each DOP-sweep-gated, DOP=1 byte-identical):** (a) read-out aggregation
over a single pre-built fragment (no routing yet) == today's finalize; (b) route-raw
Sink + Combine at `radix=1` (still serial, still pre-agg via bounded-adaptive) == today;
(c) flip `radix>1` + parallel sink/read-out via the scheduler, oracle-verify + measure
the G-A 4.8× shape. **First measured win is (c).**

### ✅ Increment 1 LANDED (2026-06-24) — parallel route-raw sink (always-route)

`_grouped_agg_route` in `parallel_engine.py`, gated `config.M4_ROUTE_AGG` (default off).
Each worker SELF-PULLS its own morsels (concurrent `pull_one`), runs them through its
OWN cloned `scan→middle→breaker-prepare` chain (reusing `_stateless_stream`'s per-worker
clone wiring), and routes the prepared morsels into its OWN thread-local `radix`-bin
`_ScatterCollectEngine` — **no serial scatter** (the Amdahl ceiling `_grouped_agg_stream`
hits). Combine = O(partitions) hand-off of thread-local bins → global lists; per-partition
read-out aggregates each partition once via a fresh `GroupHashEngine.ingest`, then the
identical `breaker._parallel_engines` concat-finalize. **GroupHashEngine internals
UNTOUCHED** — the engine is reused per-partition, so no collector re-index risk. This is
build-order (c)'s *mechanism* minus the bounded-adaptive pre-agg (increment 2 = always-
route → adaptive, the validated `CAggAdaptive` switch). **Verified:** route path fires;
single- & multi-col GROUP BY with COUNT/MIN/MAX/SUM/AVG checksum-identical to serial;
`make q` 190/190 default (path untouched), and route-on adds **zero** new failures (the
4 fails at `PARALLEL_MIN_ROWS=0` are a PRE-EXISTING parallel-DISTINCT bug — present with
route OFF too, unrelated to grouped agg).

#### ⚠️ MEASURED: NEUTRAL on the full ClickBench suite — Stage 2 re-scoped (2026-06-24)

Route-on-abandon ran over the **entire ClickBench suite: neutral** (route ≈ existing
row-routing). Root-caused by measuring the serial decode vs parallel agg split
(workers=1, projection-only `SELECT col` decode proxy vs full GROUP BY): INT UserID
≈ 37% decode / 63% agg; STR keys ≈ 50/50 (and the proxy *over*-counts decode, so true
agg share is higher). So ClickBench agg is **not decode-bound** — but:

- The existing `_grouped_agg_stream` **already does disjoint-partition parallel agg**, so
  it already sits at the Amdahl ceiling: `0.37 + 0.63/8 ≈ 2.2–2.7×` == the recorded 2.73×
  on int-key Q16. **Decode (~40%) is serial in BOTH paths** (`pull_lock`, line 35 ceiling).
- Route-on-abandon parallelizes the same keying **plus** prepare+scatter — but decode
  stays serial and the prepare+scatter delta is negligible for plain-column GROUP BY.
  → route ≈ existing → **neutral**.
- The G-A prototype's 4.8×-vs-1.5× win was vs a **strawman** (full per-thread HT over the
  whole keyspace) in a **decode-free** synthetic harness. Against the real baseline (the
  shipped disjoint row-routing) route-on-abandon has the **same parallel ceiling**.

**Conclusion: Stage 2 (route-on-abandon) is largely REDUNDANT with the shipped row-routing**
for these workloads — it wins only on expression-heavy / scatter-heavy aggregates (a narrow
band). `_grouped_agg_route` (+ `M4_ROUTE_AGG`) is kept as a correct, gated, **parked**
capability — NOT the ClickBench win. **The real lever is PARALLEL DECODE** (gates EVERY parallel shape —
agg/distinct/stateless/join-build — and lifts the existing row-routing too, with no new
agg operator). That, not increment 2, is where the next work should go.

### Parallel-decode workstream — Stage 0 + Stage 1 ✅ LANDED (2026-06-24)

Deep trace (sub-agent) corrected the model: **the parquet decode is ALREADY parallel and
GIL-free** on rugo's own C++ pool (`io_pipeline.hpp:621`); `next_vectors()` blocks
GIL-released on a *completed* decode (`pool_reader.pyx:947-948`). The serial fraction is
the **GIL-held per-morsel assembly tail** (lazy wrap + bytecode predicate filter
`parquet_read.pyx:1339` + select + commit), serialised by `pull_lock` at the 3 parallel
sites (`parallel_engine.py:473/1077/1241`). The lever = lockless self-pull gated on source
reentrancy.

- **Stage 0 (baseline):** `dev/m4_pull_scaling.py` — the lock-bound stateless path is
  **perfectly FLAT 1.00× across DOP 1/2/4/8** (25.4→25.5ms, 1M rows). The entire parallel
  dividend on that path is currently discarded by the lock. Re-run after Stage 2 to quantify.
- **Stage 1 (capability + P1 crash fix):** `BasePlanNode.is_concurrent_pull_safe()` (default
  False) + `ParquetReadNode` override (True iff resolved `_SCAN_SINGLE`; LATMAT/FALLBACK/
  generators False). Fixed the P1 bug: `_distinct_stream` self-pulled lockless
  unconditionally → crashed on non-reentrant sources at W≥2 (this was the
  parallel-DISTINCT-at-floor=0 failure). Now gated: serialised pull when unsafe. Regression
  test `tests/unit/test_execution/test_concurrent_pull_capability.py` (4 tests). Gates: default
  q 190/190; floor=0/W4 q **190/190 (was 186/4 — fixed)**; route q 190/190.
- **NEXT — Stage 2 (the lever):** branch the 3 `pull_lock` sites to lockless self-pull when
  `is_concurrent_pull_safe()`, serialised otherwise. Re-run Stage 0 → expect it off 1.00×.
  Stage 3 = `decode_workers`/in-flight tuning vs W. **Stage 2 also re-opens the route-raw
  verdict** — `_grouped_agg_route` is throttled by the same lock; lockless pull may let it
  beat the serial-producer row-routing.

## Verification

- **Single-thread no-regression gate (primary).** `DOP=1` must be within noise of
  the current serial engine on `make q` / tpch / clickbench — the model must be free
  at one core. A serial regression fails the rewrite regardless of parallel gains.
- `make q` (190) + tpch (22) + shapes + clickbench (43) green; result-identical to
  serial; workers=1 vs N identical.
- DuckDB oracle on multi-join + GROUP BY + ORDER BY, incl. NULL keys, multi-col
  keys, outer joins, empty inputs.
- ClickBench timing: parallel agg/join must **win** (validates bounded-adaptive
  pre-agg and stream-the-probe).
- Skew probe: confirm parallel read-out balances; a mega-partition degrades
  gracefully (doesn't crash/corrupt).
