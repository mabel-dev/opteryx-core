# M4 — Morsel-Driven Parallel Execution (DuckDB model)

> Status: **DESIGN — prototype-validated.** Aligned to DuckDB's morsel-driven
> model (`docs/DUCKDB_PARALLELISM_REFERENCE.md`); the Spark-style materialized
> shuffle + stage barriers are **explicitly rejected** (slow: a full intermediate
> materialization and a hard barrier per breaker). Both prior open questions
> (reduce model, probe-side shuffle) are resolved by this alignment.

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
| Merge | per-group transplant — **O(groups)** | partition-aligned hand-off — **O(partitions)** |
| Drive | one synchronous `push() nogil` | `Sink`/`Combine`/`Finalize` phases |
| Read-out | finalize on the instance, ~serial | parallel `Source`, one partition per task |

Rule established by the prototype: **bind orchestration to Python, never the
morsel.** Morsels stay C++ (`CxxMorsel` / `shared_ptr[CxxMorsel]` — the carrier
flip already does this); the per-task push loop runs in C++ with the GIL released;
only the scheduler crosses into Python. This **dissolves the free-threaded
refcount question** (no PyObject per row) and the nanobind type clash. This rewrite
converges with the carrier-flip / C++-first morsel initiative
(`docs/M4_CPP_MORSEL_DESIGN.md`).

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
| Aggregate breaker | `demo_agg.py` | thread-local radix HT, O(partitions) Combine, parallel read-out | ✓ (this IS the DuckDB shape) |
| Inner join breaker | `demo_join.py` | co-partitioned shuffle + per-partition join | ✗ Spark-style — **redo as stream-the-probe** |

The join demo is the one piece to rebuild to match the model (shared-table build +
streaming probe).

## Substrate to reuse

`CppThreadPool`; `pull_one` (reentrant scan); `_clone_op`; `identify_segments` +
`OperatorParallelism`; `PipelineContext` (terminate / `_exc` /
`set_expected_input_closes`); exported C-ABI handles (`cxx_morsel_raw_ptr`,
`cxx_take_c`) for out-of-tree C++. The agg engine's `merge()` is the embryo of
`Combine` but must become **radix-aligned (O(partitions))**. **Supersedes**
`_ScatterCollectEngine`, `_DistinctCollector`, `_parallel_engines`, the four
`_find_parallel_*` finders. (No exchange operator / `$hash`/`$bin` columns / shuffle
matrix — those were the Spark design and are dropped.)

## Components to build

1. **Event-DAG scheduler** — pipelines from `identify_segments`, two-counter
   events, dependency edges (build-before-probe, agg-before-read-out),
   data-bounded DOP, producer/consumer pools.
2. **C++ breaker rewrites** to local/global + `Sink`/`Combine`/`Finalize`, each
   **radix-partitioning its own input internally**:
   - aggregate (bounded-adaptive, O(partitions) Combine, parallel read-out)
   - join (thread-local tables → shared table → **streaming probe**)
   - distinct (like aggregate)
   - sort (local sort + k-way merge)
3. k-way merge kernel for sort.
4. Parallel read-out `Source` role on breakers.
5. Retire the single-scan finders/seams.

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
