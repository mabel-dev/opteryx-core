# Execution Maturity Gaps — Opteryx vs DuckDB (Overview)

**Status:** Overview / opportunity map. Not a ratified plan.
**Audience:** Execution-engine contributors.
**Purpose:** Use DuckDB as a *lens* — not a target to clone — to locate where Opteryx
needs maturity. Each gap is grounded in current Opteryx source and in DuckDB's
mechanism (see [`DUCKDB_PARALLELISM_REFERENCE.md`](DUCKDB_PARALLELISM_REFERENCE.md)
and [`PARALLEL_ENGINE_DESIGN.md`](PARALLEL_ENGINE_DESIGN.md)).

The goal is not feature parity. Some divergence is deliberate and correct. This
document ranks where the engine's *multiples* (and one robustness cliff) actually
come from, so the next design pass targets leverage, not breadth.

---

## Framing: a contract vs a pattern set

The single structural difference everything else hangs off:

- **DuckDB parallelizes a contract.** Parallelism is a property of a *pipeline*. Every
  operator implements the same lifecycle — `GetGlobalSinkState` (once),
  `GetLocalSinkState` (per task), `Sink` → `Combine` → `Finalize`, plus a cooperative
  `BLOCKED` / `InterruptState` return. Because *every* operator honours it, *every*
  plan shape parallelizes for free.

- **Opteryx parallelizes patterns.** The Event-DAG scheduler
  ([`scheduler_engine.py`](../opteryx/managers/execution/scheduler_engine.py)) is
  structurally close to DuckDB's, but the operators do not carry a universal sink
  contract. Instead [`parallel_engine.py`](../opteryx/managers/execution/parallel_engine.py)
  contains 5–6 hand-written **shape matchers**; anything that does not match falls to
  `_serial_stream()`.

This is the lens for everything below. Most of the speed gaps are downstream of
"parallelism is enumerated, not universal."

---

## Opportunity register

| # | Opportunity | Class | Leverage | Bounded? | Parts already exist? |
|---|-------------|-------|----------|----------|----------------------|
| 1 | Universal parallel sink contract (retire shape matchers) | Speed | **Highest** | Large | Scheduler + sink contract + segment cutter all exist; resolution & per-segment drive are the gap (§1) |
| 2 | Runtime filters (build → probe sideways info passing) | Speed | High | **Small** | Build + scan-prune both exist |
| 3 | Fold decode into the one scheduler (thread budget) | Speed | High | Medium | Two uncoordinated CPU pools; decode is **per-scan** (N×16); reserved-unwired flag exists; fix is mostly native (§3) |
| 4 | Parallel partition-aligned join build | Speed | High | Medium | Serial build today |
| 5 | Spilling + memory budget | Robustness | Cliff-removal | Large | None |
| 6 | Normalized sort keys | Speed (const-factor) | Modest | Small | `std::sort` + memcmp today |
| 7 | Compressed execution (FSST/ALP) | Speed | Medium | Large | Dict-through-engine only |
| 8 | Auto / multi-column statistics | Plan quality | Second-order | Medium | DPccp + sidecars exist |

---

# 1. Universal parallel sink contract — *highest leverage* (DETAILED DESIGN)

The other sections stay overview-level. This one is worked through because it is the
gap everything else hangs off, and because Opteryx already has most of the parts — the
work is *connecting* them, not inventing them.

## 1.0 The gap, concretely

Parallelism is decided by a chain of **enumerated shape matchers** in
[`dispatch_data_pipeline`](../opteryx/managers/execution/parallel_engine.py) (the router
at `parallel_engine.py:1345`). It tries each matcher in turn and falls through to
`_serial_stream` (`parallel_engine.py:1325`):

| Matcher | `parallel_engine.py` | Plan shape it accepts |
|---|---|---|
| `_find_parallel_grouped_agg` | `:231` | `scan → stateless* → GroupedAggregateHashedNode` |
| `_find_parallel_ungrouped_agg` | `:197` | `scan → stateless* → UngroupedAggregateNode` |
| `_find_parallel_distinct` | `:267` | `scan → stateless* → DistinctNode` (W≥2) |
| `_find_parallel_join_agg` | `:775` | `[scan,scan] → INNER JOIN → stateless* → AGG → exit` |
| `_find_parallel_multi_join_agg` | `:856` | multi-join build subtree → fact probe → AGG |
| `_find_parallel_join` | `:411` | bare `scan → INNER JOIN → stateless* → exit` |
| `_find_parallel_stateless` | `:305` | `scan → {filter,projection}* → exit` |

Each matcher hard-codes structural gates: **single scan**, every middle op
`STATELESS`, the join is exactly `DrakenInnerJoinNode` with **empty
`_compiled_right_evals`**, every downstream op `STATELESS`. Anything outside the
enumerated set is serial:

> outer / semi / anti / non-equi / ASOF joins; multi-join **without** a downstream
> aggregate; **any** sort; window with `ORDER BY`; union; INTERSECT / EXCEPT; two
> sequential breakers (e.g. `GROUP BY … GROUP BY` or agg→distinct).

The cost of adding *one* new parallel shape today is: write a new `_find_parallel_*`
matcher, write a new sink adapter, register it by class name, and thread it through the
router. That is O(shapes) bespoke code — the opposite of DuckDB's O(1) contract.

## 1.1 What already exists (the embryo — be fair)

Opteryx is closer than the matcher sprawl suggests. Four pieces are already in place:

1. **The Event-DAG scheduler** ([`scheduler_engine.py`](../opteryx/managers/execution/scheduler_engine.py)).
   `Event` (`:79`) carries DuckDB's exact double-gate — upstream `total_deps/finished_deps`
   (runnable when deps finish) and downstream `total_tasks/finished_tasks` (complete when
   tasks finish), `scheduler_engine.py:96-133`. `Executor` (`:135`) runs the DAG on a
   `CppThreadPool`. This *is* DuckDB's `event.cpp` mechanism (reference doc §5–6).

2. **A segment cutter.** `identify_segments` (`parallel_engine.py:140`) already splits a
   plan into `Segment`s (`:119`) at breaker boundaries — exactly DuckDB's pipeline
   decomposition (reference §3). Each `Segment` already carries `tail_is_breaker` and the
   tail's `parallelism` class.

3. **A generic breaker skeleton.** `_run_breaker_segment` (`parallel_engine.py:1508`) is a
   single 7-part driver — compile → (build prelude) → row-floor serial fallback → W
   self-pull workers → errors barrier → `combine` → `finalize` read-out → EOS. It already
   serves SCALAR_MERGE, HASH_REPARTITION (agg + distinct) **and** join→agg through one
   body. This is the embryo of DuckDB's `PipelineExecutor`.

4. **A sink contract.** `PipelineSink` ([`pipeline_sink.py:62`](../opteryx/managers/execution/pipeline_sink.py))
   already names the three points that vary per breaker —
   `make_local_sink_state` / `combine` / `finalize_source` — i.e. DuckDB's
   `GetLocalSinkState` / `Combine` / `Finalize+Source` (reference §12, §14). Three adapters
   exist: `_ScalarMergeSink`, `_HashRepartitionSink`, `_DistinctSink`.

So the contract and the scheduler both exist. **Two things block universality**, and
they are the whole of this design:

- **(a) Resolution is by hard-coded class name, gated by bespoke matchers.** The sink is
  looked up in `_RECOMB_BY_CLASS` / `_ADAPTER_BY_CLASS` (`pipeline_sink.py:525,531`) —
  string dicts of three class names — and only *reached* if one of the seven matchers
  fires first. A breaker the dicts don't name, or a shape no matcher accepts, is invisible
  to the contract.
- **(b) The Event-DAG does not actually run per-segment.** `_build_segment_dag`
  (`scheduler_engine.py:187`) makes producer segments **no-op events** (`lambda: []`,
  `:210`) and routes the *entire* pipeline through the one terminal drive (`:212`). The
  dependency edges exist but express nothing at runtime — cross-segment composition is the
  EMIT-into-cloned-downstream trick inside the single terminal `dispatch_data_pipeline`
  call. So a two-breaker plan runs its first breaker in parallel and its tail **serially**;
  segments are not independently scheduled or parallelised.

## 1.2 DuckDB's contract (the target, cited)

From [`DUCKDB_PARALLELISM_REFERENCE.md`](DUCKDB_PARALLELISM_REFERENCE.md):

- **Every operator declares its roles + a `Parallel*` flag per role**, and a `MaxThreads`
  hint (§2, §8, §18-1). A single non-parallel role serialises *that pipeline only*.
- **Sink lifecycle** (§12): `GetGlobalSinkState` once, `GetLocalSinkState` per task,
  `Sink` (lock-free into local state), `Combine` (merge local→global — the *only* mutating
  contact), `Finalize` (one global wrap-up, may fan back out into parallel sub-events).
- **DOP = min over operator `MaxThreads`, clamped to scheduler threads, driven by source
  morsel count** (§8) — not a per-shape decision.
- **Ordering is event dependencies, never locks** (§5, §18-6): build-before-probe is "the
  probe pipeline's initialize event depends on the build pipeline's complete event."
- **Partition-aligned merge** (§14.2): `Combine` is an O(partitions) pointer hand-off, and
  read-out is itself parallel across partitions (§14.4).

The key property: **adding an operator never touches the scheduler.** The operator
implements the contract; the engine parallelises it for free.

## 1.3 The central design decision — RATIFIED: Option A

Opteryx has a heuristic stated at `parallel_engine.py:9`:

> *"The engine owns parallelism; the OPERATORS stay parallel-unaware."*

This is a heuristic, **not** a non-negotiable — true until it isn't. DuckDB's contract
puts `Sink/Combine/Finalize/MaxThreads` **on the operator**, which would contradict it.
Two faithful ways forward were weighed:

- **Option A — declared capability (keep operators parallel-unaware).** Keep the
  recombination logic *outside* the operator, but make it a **declared capability**
  (resolved from the catalog, not a hard-coded class-name dict), and drive dispatch off
  `identify_segments` instead of the seven matchers. The operator gains *one* piece of
  self-knowledge — "here is my parallel-sink factory, or None" — a capability declaration,
  not parallel logic.
- **Option B — contract on the operator (DuckDB-faithful).** Move
  `make_local_sink_state/combine/finalize` onto each breaker. Maximal fidelity; touches
  every breaker and folds parallel logic into the operators.

**Ratified: Option A.** It reaches the same universality (any breaker with a registered
factory parallelises through one path), keeps operators parallel-unaware, and reuses the
three adapters and the skeleton. The rest of this design assumes A.

> **Language is orthogonal to A-vs-B — and it is settled by CLAUDE.md §1/§2.** Option A
> is about *where the recombination contract lives* (declared capability vs operator
> method), not *what language it is written in*. The execution control layer — the
> scheduler, the dispatcher, the per-morsel drive loop — is **native (Cython/C++)** by
> contract. The current `scheduler_engine.py` / `parallel_engine.py` / `pipeline_sink.py`
> are **interim Python debt on the execution path** (the planning/execution boundary
> should be crossed once, into native). So Phases A–C below are described as edits to
> *today's* interim Python control layer, but the destination is native: new control-path
> code must not deepen the Python execution path, and the contract is shaped so it can be
> lowered into Cython/C++ without re-litigating the model.

## 1.4 The design — phased

### Phase A — Catalog-declared sink capability (kill the class-name registry)

Replace the two string dicts (`_RECOMB_BY_CLASS`, `_ADAPTER_BY_CLASS`,
`pipeline_sink.py:525-535`) with a field on the operator's catalog meta
([`catalog.py`](../opteryx/operators/catalog.py)). Today the meta already carries
`parallelism: OperatorParallelism` (`catalog.py:85`) and `is_pipeline_breaking` (`:86`).
Add a `parallel_sink_factory` (a callable building the `PipelineSink`, or `None`).

`recombination_class_for` / `make_sink` (`pipeline_sink.py:538,544`) then resolve off the
meta, not `node.__class__.__name__`. A breaker with no factory → `RecombClass.NONE` →
that segment runs serial *by declaration*, not by falling off the end of seven matchers.

This is the DuckDB "uniform role declaration" invariant (§18-1) expressed as catalog
data. `OperatorParallelism` (`catalog.py:52`, today `STATELESS / STATEFUL_MERGEABLE /
STATEFUL_SERIAL / SINGLETON`) is the coarse middle-op classification the segment walk
already reads — it stays as the *streaming-operator* parallel-safety flag (DuckDB's
`ParallelOperator`); the new factory field is the *sink* role (`ParallelSink`).

### Phase B — Segment-driven dispatch (kill the matchers)

`identify_segments` (`parallel_engine.py:140`) already produces, per segment, the source,
the middle ids, and the tail breaker with its `parallelism`. Rewrite
`dispatch_data_pipeline` (`:1345`) to consume *that* instead of re-deriving shapes:

```
for each segment:
    if segment.tail_is_breaker and meta(tail).parallel_sink_factory is not None
       and every middle op is ParallelOperator (STATELESS)
       and the source is splittable:
        drive it through _run_breaker_segment with the factory's sink
    else:
        drive it serially
```

The structural gates the matchers hand-coded (single-scan, all-middle-stateless) become
**per-operator checks along the segment** — exactly DuckDB's "min over operator hints"
(§8). The seven `_find_parallel_*` functions
(`parallel_engine.py:197,231,267,305,411,775,856`) and the precedence ordering they
require all **delete**. `_run_breaker_segment` is untouched — it already takes a sink and
a recomb class.

### Phase C — Per-segment Event-DAG drive (cross-segment parallelism)

Make `_build_segment_dag` (`scheduler_engine.py:187`) give each **producer** segment a
*real* drive task (today: `lambda: []` no-op, `:210`) that materialises its breaker's
output, with the consuming segment's Event depending on it (`add_dependency`, already
wired `:233`). This is DuckDB's pipeline DAG (§3.2, §5.1): build-before-probe and
agg-before-readout become genuine event edges, and each segment parallelises *on its own
DOP*.

Concretely this is what unlocks the shapes that are serial today even though their
*pieces* are parallelisable: `GROUP BY … ORDER BY` (agg segment ∥, then sort segment),
agg→distinct, and multi-breaker chains. The terminal-only drive (the EMIT-into-cloned-
downstream model) remains correct for single-segment plans and as the within-segment
composition; Phase C is additive.

### Phase D — Parallel join build as a sink (the hard, highest-value operator)

Today the join build is **serial-once**: `_join_probe_stream` (`parallel_engine.py:1990`)
and `_SharedSourceJoin` (`:949`) drive the build leg to completion on one thread, then
each probe worker *rebuilds a private hash* over the shared read-only `left_morsel`
(`build_side_carchar_morsel_map`, e.g. `:2127`). Two costs: the serial build is an Amdahl
anchor (the doc notes Q3 ≈ 29% of wall), and every worker pays a full hash rebuild.

Under the contract the join build becomes a fourth `PipelineSink` (`SHARED_SOURCE` is
already reserved in `RecombClass`, `pipeline_sink.py:50`):

- `make_local_sink_state` → a thread-local Carchar build over this worker's slice of the
  build scan;
- `combine` → a **partition-aligned hand-off** of the per-thread tables (DuckDB §15.1–15.2:
  move, don't rehash);
- `finalize` → assemble the global probe table once; the probe segment becomes a
  `ParallelOperator` over the read-only table (§15.3).

This removes the **inner-equi-only** and **empty-`_compiled_right_evals`** gates: outer /
semi / anti differ only in the probe-emit and a match-flag finalize (DuckDB §15.2 keeps
the flag in the tuple layout), so they fall out of the same sink with a different finalize
— not seven more matchers.

**This phase needs a Carchar/draken C++ change** (radix-partitioned build + partition
hand-off `combine`). Phases A–C edit the *existing interim* Python control layer (no new
C++), but per CLAUDE.md §2 that layer is itself debt to be lowered into native — A–C must
not deepen it. See §1.5.

### Phase E — Cooperative `BLOCKED` (deferred; prerequisite for #5 spilling)

Today workers are blocking: `pool.submit(...)` then `future.result()` barriers
(`parallel_engine.py:1780-1782`), and scans serialise their pull unless
`is_concurrent_pull_safe()` is True (default **False**, `_operators.pyx:546`). DuckDB's
operators instead *return a result type* and never block a worker (§10–11): any phase can
return `BLOCKED`, arm an `InterruptState`, and be rescheduled. That cooperative loop is
what later lets a sink **park on a memory/IO wait** instead of holding a thread — the
enabling substrate for #5 (spilling). Out of scope here; noted so Phase D's sink contract
is shaped to allow a `BLOCKED` return later rather than being rebuilt for it.

## 1.5 New native work vs editing the interim layer

The "interim layer" column flags whether a phase edits today's Python control layer
(`scheduler_engine.py` / `parallel_engine.py` / `pipeline_sink.py`) — itself debt to be
lowered into native per CLAUDE.md §2 — versus adding genuinely new native code.

| Phase | Change surface | New native code? |
|---|---|---|
| A — catalog sink capability | `catalog.py`, `pipeline_sink.py` (interim layer) | No |
| B — segment-driven dispatch | `parallel_engine.py` — delete matchers (interim layer) | No |
| C — per-segment events | `scheduler_engine.py` (interim layer) | No |
| D — parallel join build | `pipeline_sink.py` + **Carchar radix build / partition `combine`** | **Yes** |
| E — cooperative BLOCKED | operator base `_operators.pyx`, scheduler | Yes (later) |

Phases A–C *remove* code (the seven matchers) while widening coverage, but they touch the
interim Python control layer — so the standing direction is to lower that layer (segment
cut, Event-DAG, the per-morsel drive loop) into Cython/C++. The contract is model-stable
across that lowering: the capability declaration and the skeleton's shape do not change,
only the language. Phase D is where the real engineering — and the real join speed-up —
lives, and is native from the start.

## 1.6 Invariants preserved

- **DOP=1 byte-identity** (the "prime constraint", `parallel_engine.py:40` and
  `GENERIC_PIPELINE_PARALLELISM_DESIGN.md §3`): every segment still has the row-floor
  serial fallback that drives the *original* un-cloned breaker, so `W=1` / tiny inputs are
  bit-for-bit serial. Phases A–C change *which* segments parallelise, never the W=1 path.
- **Operators stay parallel-unaware** (Option A, a heuristic — §1.3): the only new
  operator-level knowledge is a catalog capability declaration.
- **Execution is native** (CLAUDE.md §1/§2): the control layer this design edits is interim
  Python debt; the destination is Cython/C++. New control-path code must not deepen the
  Python execution path.
- **§11 vector contract**: any partitioning introduced for a sink (`combine`) is a layout
  hand-off; it may never change the answer or the row set (CLAUDE.md §11; DuckDB §18-7).

## 1.7 Sequencing and first proof point

1. **Phase A + B together** — they are one refactor: declare the capability, drive off
   segments, delete the matchers. Net *less* code, same behaviour on today's shapes, and
   immediately parallelises any breaker that gets a factory (e.g. wiring a `SortNode` or
   `WindowNode` sink becomes a one-file change, not a new matcher).
2. **Phase C** — turn on per-segment events; unlocks multi-breaker plans
   (`GROUP BY … ORDER BY`, agg→distinct).
3. **Phase D** — parallel partition-aligned join build (this is item #4 in the register,
   folded in here as the proof that the contract scales to the hard operator).

**First proof point:** land A+B with the *existing* three sinks and confirm byte-identical
results + unchanged ClickBench on the shapes that parallelise today — proving the
contract-driven path subsumes the matchers with zero regression. Then add a `SortNode`
sink as the first *new* operator the contract carries that no matcher ever did.

---

## 2. Runtime filters (sideways information passing) — *best first proof point*

**Gap.** Filter propagation across joins happens only at *plan* time
([`correlated_filters.py`](../opteryx/planner/optimizer/strategies/correlated_filters.py)).
There is zero runtime adaptivity; [`adaptive_join_statistics.md`](adaptive_join_statistics.md)
is design-only.

**DuckDB mechanism.** During the hash-join build it tracks **min/max of the build-side
key** and pushes those bounds as a table filter into the probe-side scan, which skips
row groups via zone maps. DuckDB's own worked example: ~10×, ~40% of a 100M-row probe
eliminated.

**Why this is the smallest win.** Both ends already exist:
- the join already materializes the build side;
- the scan already does min/max and bloom row-group pruning
  ([`bloom_filter_read_pruning`], [`in_list_rowgroup_pruning`] in memory).

The work is the *wire between them*: emit a min/max (or small bloom) from the build,
hand it sideways into the probe scan's existing prune path. Bounded, high payoff,
classic on selective star-schema / TPC-H joins.

---

# 3. Fold decode into the one scheduler — the thread-budget problem (DETAILED DESIGN)

The measured #1 execution loss. Worked through because it is the lever that unlocks the
string-heavy ClickBench queries, and because — unlike #1 — the fix is mostly native
(rugo C++ + Cython), squarely on the right side of CLAUDE.md §2.

## 3.0 The gap, concretely — two uncoordinated CPU budgets

Opteryx runs **two thread pools that never coordinate**, both CPU-bound, both sized to
"about the core count":

| Pool | Where | Size |
|---|---|---|
| **Execution** | `CppThreadPool` per scheduler (`scheduler_engine.py:306`); DOP = `resolve_worker_count` (`parallel_engine.py:83`) | `max(1, min(cpu−2, 8))` — caps at 8, **reserves 2 cores** |
| **Decode** | per-scan C++ `BS::light_thread_pool` in `ParquetIOPipeline` (`rugo/src/parquet/io_pipeline.hpp:621,1117`) | local `min(16, max(8, cpu−2))`; GCS `128` (`config.py:156-167`) |

The decode pool is **per `IpcRowGroupSource`, i.e. per scan** — `open_ipc_source`
(`parquet_read.pyx:1256`) does `new ParquetIOPipeline(...)` (`pool_reader.pyx:255`). So a
plan with *N* parquet scans spins up *N* independent decode pools. Decode runs **ahead**
of execution: the pool fetches+decompresses+decodes row groups into a lock-free
`moodycamel` queue bounded to `decode_workers + 2` in flight (`pool_reader.pyx:1235,1326`);
the execution worker's `pull_one(scan)` (`_operators.pyx:908`) just dequeues an
*already-decoded* row group (cheap, GIL released on the wait).

`resolve_worker_count`'s "reserve 2 cores" (`parallel_engine.py:103`) is a **static guess**
that decode needs "the rest." But decode does not take 2 cores — it takes up to 16 *per
scan*. The two budgets are set independently and collide.

There is even a reserved-but-**unwired** feature flag for exactly this —
`FEATURE_PARQUET_THREAD_SCHEDULER` (`config.py:227`) has zero consumers in the tree. It is
a placeholder for this work, nothing more.

## 3.1 The arithmetic that produces the 1.8× ceiling

On an 8-vCPU Cloud Run box (the prod target, CLAUDE.md §6):

- Execution DOP = `min(8−2, 8)` = **6 workers**.
- Each parquet scan = `min(16, max(8, 6))` = **8 decode threads**.
- **Single-scan string-key agg:** 8 decode + 6 exec = **14 CPU-hungry threads on 8 cores**
  (~1.75× oversubscribed). Decode is the wide, expensive side (UTF-8 string decode), so it
  saturates first and execution DOP starves.
- **2-table join:** 2 decode pools = 16 decode + 6 exec = **22 threads on 8 cores**.

This is precisely §4.2 of [`PARALLEL_ENGINE_DESIGN.md`](PARALLEL_ENGINE_DESIGN.md): route-ON
string `URL` agg stays **1.78× / 7.35 cores** with decode pinned at 1.0×→1.0× (saturated),
versus int-key **4.51× / 8.19 cores** where decode is cheap and leaves cores for exec DOP.
The bottleneck is not the aggregation kernel — it is two pools fighting for one set of cores.

## 3.2 DuckDB's mechanism (cited)

From [`DUCKDB_PARALLELISM_REFERENCE.md`](DUCKDB_PARALLELISM_REFERENCE.md):

- **One `TaskScheduler`, `nproc − 1` threads** (§7.1). There is exactly one CPU-bound budget
  for the whole query.
- It has two *queue types* — a **regular compute** pool and an **async/IO** pool (§7.3) —
  but **regular workers steal from all pools** (§7.5). So I/O is segregated for *latency*,
  not given its own CPU army.
- **The parquet scan is the source phase.** Morsel handout is at row-group granularity
  under one short cursor lock (§13); the compute worker then **decodes its own row group**
  (decompress → vectors) inside `GetData()`. Decode is *not* a separate pool — it is work
  the execution worker does for the morsel it is about to process (vectors stay hot in that
  worker's cache).
- **I/O latency is hidden by `BLOCKED` + async** (§10–11), so a worker fetching bytes from
  disk/network never *holds* a CPU thread while waiting.

Net: **decode and execution share one fixed CPU budget; I/O latency is hidden by async, not
by a fat dedicated decode pool.** That is the exact inversion of Opteryx today.

## 3.3 The design — phased

### Phase 1 — One decode pool per query, not per scan (cheap, immediate)

Hoist the `BS::light_thread_pool` out of per-`IpcRowGroupSource` ownership into a
**query-scoped shared pool**, injected into every `open_ipc_source`. `decode_pool_` is
already a `std::shared_ptr` (`io_pipeline.hpp:621`) — the change is to *share one instance*
across all scans in a plan instead of `new`-ing one per scan (`pool_reader.pyx:255`). This
alone kills the *N×16* multiplication on every join / multi-scan plan. Pure plumbing, no
model change — the highest ratio of impact to risk in this whole item.

### Phase 2 — One CPU budget across decode + execution (the real fix)

Make decode draw from the **same** budget as execution so the total CPU-bound thread count
is bounded by cores, DuckDB-style. Two faithful options:

- **2a — decode-on-pull (DuckDB source phase, recommended).** The execution worker that
  calls `pull_one(scan)` does the decompress+convert itself, instead of blocking on a
  separate pool's queue. The only pool left is the execution pool; the row-group cursor
  (`_scan_mtx`, already present and already making `is_concurrent_pull_safe()` True for
  single-pass, `parquet_read.pyx:943`) is the single sync point — exactly DuckDB §13. I/O
  *fetch* of compressed bytes stays a small async prefetch ahead of the cursor (latency
  hiding); the CPU-heavy decode moves onto the consuming worker, which then processes the
  morsel it just decoded — best cache locality. Requires splitting `ParquetIOPipeline`'s
  conflated fetch+decompress+decode into (i) async fetch and (ii) a *synchronous* decode
  entrypoint callable on the calling thread.
- **2b — shared scheduler, decode as tasks (DuckDB two-queue model).** Keep the
  prefetch-ahead structure, but submit decode tasks to the **same** scheduler pool that runs
  execution (the `CppThreadPool`/`Executor` in `scheduler_engine.py`), sized once to cores,
  with decode and execution as two task classes the scheduler interleaves and work-steals
  (reference §7.3, §7.5). `FEATURE_PARQUET_THREAD_SCHEDULER` is the natural switch. Lower
  structural risk (keeps the pipeline), but keeps two task systems rather than one.

**Recommend 2a** for fidelity — one pool, decode amortised into the worker that consumes
it. 2b is the lower-risk staging step if 2a's rugo refactor is too big a single bite.
Architect's call.

### Phase 3 — Separate I/O concurrency from CPU decode (esp. GCS)

The GCS `128` "workers" (`config.py:166`) conflate **network-RTT hiding** (latency-bound,
wants high concurrency) with **CPU decode** (core-bound, ≤ cores). Under one CPU budget
these must split: an **async fetch ring** (high concurrency, ~no CPU) feeds a **bounded
decode** that draws from the one execution budget — DuckDB's async-I/O-queue + `BLOCKED`
return (§7.3, §10–11). The `IO_POOL_SLOT_*` machinery already present
(`config.py:208-211`) is the embryo of the fetch ring. This phase is also the concrete
consumer of #1 Phase E (cooperative `BLOCKED`): the scan fetch becomes a `BLOCKED`-able
source, so a worker waiting on GCS is rescheduled rather than parked.

### Phase 4 — Retire the static "reserve 2 cores" guess (consequence)

Once decode draws from the one budget, the `cpu − 2` reservation and the cap of 8 in
`resolve_worker_count` (`parallel_engine.py:103`) stop compensating for an uncoordinated
pool. DOP becomes DuckDB's **min over operator hints, clamped to scheduler threads** (§8),
where the scan's hint is "how many row groups I can hand out concurrently." The magic
constants fall away because the budget is now real instead of guessed.

## 3.4 Native vs orchestration

| Phase | Surface | Native? |
|---|---|---|
| 1 — shared decode pool | `io_pipeline.hpp`, `pool_reader.pyx` | **Yes** (C++) |
| 2a — decode-on-pull | `io_pipeline.hpp` (split fetch/decode), `parquet_read.pyx` | **Yes** (C++/Cython) |
| 2b — decode as scheduler tasks | scheduler + `pool_reader.pyx`, flag wiring | **Yes** |
| 3 — async fetch ring | rugo fetch ring + `BLOCKED` source | **Yes** (C++) |
| 4 — drop the reservation | `parallel_engine.py` (`resolve_worker_count`) | No |

Unlike #1, this item is almost entirely native — it lives in rugo and the scan operator,
exactly where CLAUDE.md §2 says execution belongs.

## 3.5 Pre-empting the "don't unify — different concerns" objection

The instinct (and the current design's rationale) is that I/O and decode are separate
concerns from execution, so they deserve their own pool. That is **true for I/O *latency***
— which Phase 3 keeps async, exactly as DuckDB does — but **false for CPU decode**, which
competes with execution for the same physical cores. Conflating the two is the entire bug:
a 16-thread *CPU* pool decoding strings will always starve a 6-thread execution pool on an
8-core box, no matter how the kernels are tuned. The design unifies the **CPU** budget and
keeps **I/O** async. Those are different axes; today's pool merges them.

## 3.6 Invariants

- **`is_concurrent_pull_safe` stays the correctness gate** (`parquet_read.pyx:943`): decode
  must not change which mode is concurrency-safe. Two-pass LATMAT stays serial-pull; only
  single-pass parallelises — unchanged.
- **§11 vector contract**: decode produces the same Draken vectors regardless of *which*
  thread runs it; decode-on-pull must yield byte-identical morsels to today's prefetch path.
- **DOP=1 byte-identity** (#1's prime constraint) is unaffected — this changes *where*
  decode runs, never the rows.

## 3.7 Sequencing and proof

1. **Phase 1** first — shared per-query pool. Smallest change, immediately removes the
   multi-scan oversubscription; measure cores-used on a 2-table join before/after.
2. **Phase 2a** (or 2b as a staging step) — the unification that lifts the single-scan
   ceiling.
3. **Phase 3** — only if GCS scans show I/O starvation after 2a; local-disk ClickBench does
   not need it.
4. **Phase 4** — drop the reservation once 2 lands.

**Proof metric (not wall-clock alone):** the §4.2 signature — string `URL` agg at **1.78× /
7.35 cores** with decode saturated — is the *before*. Success is cores-used climbing toward
the int-key **~8-core / 4.5×** envelope for *string* keys too, i.e. execution DOP no longer
starved by decode. Measure cores-used and agg DOP scaling, per
[`feedback_profile_and_ceiling_before_optimizing`].

---

## 4. Parallel, partition-aligned join build

**Gap.** `DrakenInnerJoinNode` builds serially at left-EOS; each probe worker then
*rebuilds a private engine* from the shared read-only left morsel
([`hashed_inner_join.pyx`](../opteryx/operators/hashed_inner_join/hashed_inner_join.pyx)).
Only bare inner-equi joins parallelize, and only when `_compiled_right_evals` is empty.

**DuckDB mechanism.** Parallel build into thread-local hash tables, radix-partitioned;
`Combine` is a **partition-aligned pointer hand-off** (O(partitions)); `Finalize` fans
back out into parallel sub-events (`HashJoinTableInitEvent` + `HashJoinFinalizeEvent`).

**Direction.** Make the build a parallel sink under the #1 contract with radix-aligned
partitions, so build cost stops being an Amdahl ceiling on every join-heavy query.

---

## 5. Spilling + memory budget — *robustness, not speed*

**Gap.** Nothing spills. There is no memory budget or admission control. Every sort,
hash aggregate, distinct, and join build is in-RAM. A query that exceeds memory does
not get slower — it **crashes**.

**DuckDB mechanism.** Page-level buffer manager (LRU eviction of individual pages);
Grace hash join (radix-partition + spill, since 0.6.1); external aggregation and
external sort; `memory_limit` defaults to 80% RAM, temp dir defaults to 90% free disk.
Pointer *recomputation* (symbolic block-id+offset, not raw pointers) makes spill a
re-swizzle, not a serialization step.

**Direction.** This is a large, separate workstream and a correctness/maturity gap,
not a benchmark number. It matters the moment a workload outgrows RAM. Sequencing it
after the speed items is reasonable, but it should be on the map explicitly.

---

## 6. Normalized sort keys — *modest, bounded*

**Gap.** Sort is full-materialize + `std::sort` with a `memcmp` tiebreak
([`sort.pyx`](../opteryx/operators/sort/sort.pyx)). Parallel sort was correctly
reverted as DRAM-bandwidth-bound (0.46× regression).

**DuckDB mechanism.** All `ORDER BY` keys encoded into **one fixed-size blob comparable
by a single `memcmp`** (DESC inverts bits; ints byte-swapped big-endian; sign bit
flipped; strings prefix-encoded with full-string tiebreak; NULL gets one extra byte).
Enables branchless / SIMD comparison.

**Direction.** A single-thread constant-factor win on the comparison path — *not*
parallelism. Worth it where sort is on the critical path; low risk, well-scoped.

---

## 7. Compressed execution (FSST / ALP) — attacks #3 at the root

**Gap.** We carry dictionary encoding into the engine
([`dict_aware_int_filter`] in memory) but decode strings eagerly — which is exactly
why string-key aggregation saturates the decode pool (#3).

**DuckDB mechanism.** FSST-compressed strings and ALP-compressed floats flow *through
execution without decompression*; dictionary stays a DICTIONARY vector. Fewer bytes for
the decode pool to touch.

**Direction.** FSST-style compressed string execution cuts the bytes decode has to
move, attacking the thread-budget bottleneck at the source rather than throwing more
threads at it. Storage/rugo-side, larger effort.

---

## 8. Auto / multi-column statistics — *second-order today*

**Gap.** DPccp join ordering is solid, but statistics are *optional sidecars* (manual
`ANALYZE`), and the estimator assumes column independence (no multi-column / column-group
stats). Un-analyzed tables get weaker join orders.

**DuckDB mechanism.** Automatic statistics collection at load; HyperLogLog-backed
cardinality over equivalence sets; statistics propagation across joins.

**Direction.** Real, but our current bottlenecks are execution-bound. Pipeline-time
sidecar generation and column-group stats are worth doing *after* the parallelism items
move the metric.

---

## Recommended sequencing

**Speed (all point the same direction the M4 work started):**

1. Universal parallel sink contract — retire the shape matchers.
2. Runtime filters (build → probe) — smallest bounded win, parts exist; **best first proof point**.
3. Fold decode into the one scheduler — lifts the measured string-agg ceiling.
4. Parallel partition-aligned join build — removes the join Amdahl ceiling.

**Robustness:**

5. Spilling + memory budget — turns OOM from a crash into graceful degradation.

**Later / lower-leverage:** 6 normalized sort keys, 7 compressed execution, 8 statistics.

---

## Out of scope / deliberate divergence

- Not cloning DuckDB's storage format or buffer manager wholesale.
- Not adopting pull-based anything — Opteryx is push-based and stays so.
- Compression schemes (FSST/ALP) are evaluated on merit for *our* decode bottleneck,
  not adopted for parity.
- The §11 vector contract (uniform `data[selection[i]]`) is non-negotiable: any
  partition/shape discriminant introduced for parallelism may change layout, never the
  answer or the row set.
