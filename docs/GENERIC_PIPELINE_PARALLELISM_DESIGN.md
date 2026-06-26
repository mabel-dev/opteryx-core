# Generic Pipeline Parallelism — Design

> Authored 2026-06-25. Designed via a 9-agent workflow: 5 code-anchored substrate maps,
> 3 independent design approaches, adversarial judge + verification against the real tree.
> This supersedes the bespoke-strategy direction in `PARALLEL_ENGINE_DESIGN.md` for
> *how* parallelism is structured. Every claim is anchored to a `file:line`.

## Status — DELIVERED (2026-06-25), with one measured negative

This design is **implemented and shipped as the default engine**, and the original §5 sequence
is resolved. What changed since authoring (knowledge + intent), recorded here so the rest of
the doc is read in light of it:

- **Steps 0–7 landed.** The five bespoke per-shape strategies are deleted; the one executor was
  folded into `scheduler_engine.py` as one Event per segment (Step 7) and is the **sole data
  path** — `M4_USE_SCHEDULER` defaults on, `parallel_engine.execute` is removed, `_pipeline_stream`
  is now `dispatch_data_pipeline`. The `PipelineSink` contract + one `_run_breaker_segment`
  skeleton cover stateless / ungrouped / grouped / distinct / inner-join→agg / multi-join,
  including a **parallel build prelude** for multi-joins.
- **It scales with data, as designed.** A multi-join→agg (TPC-H Q3) goes **1.30× at SF=1 →
  1.66× at SF=10** (cores 3.0→7.7), byte-identical. The SF=1 "ceiling" was a measurement
  artifact of small data, not a limit. The **tdoms cardinality estimator** (Ebergen) was wired
  into build-side selection so the fact table becomes the probe — a durable *serial* win that
  also unlocks the multi-join parallel path.
- **Step 8 (`ORDER_MERGE` parallel sort) is a measured NEGATIVE — built, then REVERTED.**
  Byte-identical but a **0.46× regression** on a large sort. Root cause is fundamental: the
  serial radix sort is **DRAM-bandwidth-bound** (runs at ~1.1 cores), so slicing it across W
  workers contends on memory bandwidth and *loses* — the W parallel local sorts alone (38.3s)
  already exceed the whole serial sort (25.6s), so even a free merge can't win, and the k-way
  merge adds a cache-missing serial tail. The §4.2 claim "sort is not a hard block" was **wrong
  for this kernel** — corrected inline below. Sort stays serial.
- **Unifying knowledge:** Opteryx's hot paths are increasingly **bandwidth-bound, not
  CPU-bound**. Pipeline parallelism wins where there is real CPU work to spread (joins,
  aggregation — validated scaling) and loses where the kernel just streams memory (sort — the
  same wall as ClickBench decode). The lever there is touching *less* memory, not more threads.
- **Out of scope by design (§6), surfaced for a separate plan:** outer joins and subquery
  shapes. At SF=10 only 4/22 TPC-H queries fire a parallel path; the expensive serial ones
  (Q5/Q7/Q9/Q13/Q18) need that coverage. Also open: a Q21 SF=10 binder regression (empty
  projection, stats-driven) — unrelated to this engine.

## 0. The problem this kills

`parallel_engine.py` today has **five bespoke per-shape strategies** —
`_grouped_agg_route`, `_join_probe_stream`, `_stateless_stream`, `_distinct_stream`,
`_ungrouped_agg_stream`. The maps confirmed every one of them **copy-pastes the same
7-part skeleton**: `compile_pipeline` → row-floor serial fallback → clone the worker chain
(`_clone_op` + `set_context`/`set_downstream`) → N self-pull tasks (`pull_one` gated on
`is_concurrent_pull_safe()`) → thread-local sink state → combine the W partials → drain.
The *only* thing that differs per strategy is **what the sink does** (scatter+readout for
grouped agg, scalar merge for ungrouped, per-worker engine+probe for join, dedup for
distinct, exit-clone for stateless).

So each new shape — join+agg, join+agg+sort, multi-join — needs *another* hand-rolled
function. That is inventing pipeline parallelism in parts: accretion, grown-not-designed,
combinatorially unbounded. **This design lifts the shared skeleton into ONE executor and
makes "what the sink does" a thin per-breaker contract** — so every shape and combination
falls out with no per-shape code.

## 1. The two pieces

### 1.1 `PipelineSink` — a thin contract every breaker exposes (a seam, NOT a rewrite)

The decisive map finding: the breakers' *existing internal seams* already are the sink
state. `_grouped_agg_route` doesn't rewrite the agg operator — it swaps a
`_ScatterCollectEngine` into a **clone's** `_engine` and leaves the operator body untouched
(`parallel_engine.py:1280`); finalize reuses the operator's own `_parallel_engines` concat
ABI (`grouped_aggregate_hashed/_node.pxi:293-301`). The contract formalizes exactly that,
no more:

```
class PipelineSink:                         # mixed into breaker operators
    recombination_class() -> RecombClass    # how W partials recombine; see §1.3
    make_global_sink_state(dop)             # once per query
    make_local_sink_state(global, task)     # per task — returns a CLONE whose existing
                                            #   seam IS the local state (_engine for agg,
                                            #   _scatter_engine for distinct, captured
                                            #   left_morsel/left_hash for join)
    combine(global, locals)                 # the ONE mutating contact with shared state
    finalize_source(global) -> iterator     # the parallel read-out; default = the existing
                                            #   breaker._parallel_engines concat + EOS-drain
```

**Default `recombination_class()` is `NONE` → serial.** An un-migrated breaker keeps
running today's `_finalize` verbatim. So the contract is additive; nothing breaks the day
it lands.

### 1.2 `_pipeline_stream(segment, sink, dop, ctx)` — the one executor

Extracts the verified 7-part skeleton the 6 bespoke functions duplicate. For a
breaker-delimited `Segment` (the existing unit — `identify_segments`, `parallel_engine.py:101`):

1. `compile_pipeline(plan)` → chains, exit, ctx (unchanged).
2. **Row-floor serial fallback** — below `PARALLEL_MIN_ROWS`, drive the **original
   un-cloned** operator with seams unset (this is what makes DOP=1 byte-identical, §3).
3. N self-pull tasks on `CppThreadPool`: each clones the chain, `make_local_sink_state`
   gives it a private sink, self-pulls disjoint morsels (`pull_one`, lockless only when
   `is_concurrent_pull_safe()`), drives the chain into its local sink.
4. Errors barrier; `combine(global, locals)`.
5. `finalize_source` drives the parallel read-out into the original breaker's EOS path.
6. `finally`: pool shutdown, ctx terminate, `close_source`.

Dispatch is on **`recombination_class()`**, not the coarse 4-value enum (§1.3). One
function, parameterised by the sink. The five `_find_parallel_*` finders and the `execute()`
cascade are deleted at the end of migration.

### 1.3 The recombination taxonomy (replacing the too-coarse enum)

The map flagged `OperatorParallelism` (`catalog.py:52-74`) as too coarse — joins carry *no*
class, and Distinct + GroupedAggregate are both `STATEFUL_MERGEABLE` yet recombine
differently (hash-repartition, not `merge()`). Replace it with a per-breaker
`recombination_class`:

| class | combine | finalize read-out | operators |
|---|---|---|---|
| `SCALAR_MERGE` | `engine.merge()` (exists) | the merged engine | ungrouped aggregate |
| `HASH_REPARTITION` | O(radix) bin hand-off | parallel per-partition | grouped agg, distinct |
| `SHARED_SOURCE` | no-op (built once) | streaming probe | inner-equi join |
| ~~`ORDER_MERGE`~~ | ~~register sorted run~~ | ~~k-way merge tail~~ | sort — **BUILT then REVERTED: bandwidth-bound negative (§4.2)** |
| `NONE` | — | today's serial `_finalize` | everything un-migrated |

## 2. How every operator adopts it (easy → hard, all verified)

| operator | role | adoption | status |
|---|---|---|---|
| scan | source | `is_concurrent_pull_safe()` already exists | done |
| filter / project | streaming | `STATELESS` — clone per task, no sink | trivial |
| **ungrouped agg** | sink | `SCALAR_MERGE` — `merge()`/`is_mergeable()` already in `ungrouped_agg*.pyx` | easy |
| **grouped agg** | sink | `HASH_REPARTITION` — **`_grouped_agg_route` (1181-1360) IS this contract already** | proven |
| **distinct** | sink | `HASH_REPARTITION` — identical shuffle+combine; only `finalize` differs | easy |
| **inner-equi join** | sink (build) + streaming (probe) | `SHARED_SOURCE` — v1 thread-local-full (Phase-1, 3.67×); build-before-probe is an Event edge | done (v1) |
| **sort** | sink | `ORDER_MERGE` — built then **reverted: measured 0.46× regression, DRAM-bandwidth-bound (§4.2)** | ✗ negative |

**Net: all 5 bespoke strategies retired** (stateless, ungrouped, grouped, distinct); the join
became the `SHARED_SOURCE` sink (thread-local-full). Sort was built as the `ORDER_MERGE` sink
but **reverted — a DRAM-bandwidth-bound negative (§4.2)** — so sort stays serial. The
verification confirmed each adopts on its *existing* seam with the operator body untouched —
this is the "seam-swap-into-clone" model, not relocating `_push_impl` onto a new contract
object.

## 3. Prime constraint — DOP=1 is byte-identical (verified achievable)

DOP=1 byte-identity holds **only if W=1 / below-floor drives the ORIGINAL un-cloned
operator with its seams unset** (verified at the row-floor branches `1090-1099`,
`1237-1246`, `1573-1582`; and `scheduler_engine`'s `_drive_whole_plan` *is* literally
`_serial_stream`). Therefore:

- The row-floor / W=1 path in `_pipeline_stream` MUST drive the original breaker, never a
  clone. Any refactor that routes DOP=1 through a clone breaks the prime constraint.
- **A `DOP=1 == serial-engine` golden differential test is a hard gate** on every migration
  step — not optional.

## 4. The real blockers (surfaced, not buried)

The verification found these; the design accommodates each rather than pretending they
don't exist:

1. **Read-out-as-Source is NOT free plumbing.** `compile_pipeline` (`pipeline_compiler.py:138-152`)
   builds chains *only* from `is_scan` nodes and wires one continuous `_downstream` chain;
   a breaker is **not** re-sourced — it EMITs into its downstream on EOS. So
   "agg-reads-join" and "sort-reads-agg" compose via the **EMIT-into-cloned-downstream
   model** (the next segment is a cloned tail appended to the upstream worker chain), *not*
   independent pullable pipelines. Multi-segment DAG composition is real; **zero-new-plumbing
   is a myth**. Read-out-as-pullable-Source is an explicit *later, measured* change — do not
   assume it in v1.
2. **Sort IS effectively a hard block — this was BUILT and REVERTED (corrected knowledge).**
   The original claim here ("not a hard block; just write the k-way merge kernel") was wrong.
   The shape *was* implemented exactly as described — each worker sorts its slice into a sorted
   run, a heap k-way merge tail (`merge_runs.h`) combines them — byte-identical including ties.
   But it **regressed 0.46×** on a large standalone sort (28.5s serial → 61.8s parallel) and
   was reverted. Root cause, profiled: the serial `morsel_sort` (radix) is **DRAM-bandwidth-
   bound**, running at ~1.1 cores — it is *already* saturating memory, not CPU. Slicing it
   across W workers makes the W threads contend on bandwidth: the parallel local sorts **alone**
   (38.3s) exceed the entire serial sort (25.6s), so even a zero-cost merge cannot win; the
   k-way merge then adds a ~22s cache-missing serial tail (random-access comparator over 43M
   rows). This is not fixable with a better merge — it needs a fundamentally different,
   bandwidth-aware parallel sort primitive (research-level, not pursued). Sort stays serial;
   the post-agg sort *tail* (small grouped output) was always serial and is unaffected. Same
   wall as ClickBench decode: a bandwidth-bound kernel does not parallelize by adding threads.
3. **No GIL ceiling — we are free-threaded.** An earlier draft (parroting standard-CPython
   logic) called DOP>1 "GIL-bound." That is WRONG for our runtime: we run 3.14t with
   `PYTHON_GIL=0`, `sys._is_gil_enabled()` is False, and `with gil:` does NOT mutually
   exclude on a free-threaded build (threads attach concurrently). Proof is in our own
   numbers — a GIL build could not produce the measured 4.5× agg / 3.67× join scaling. The
   residual cost of the transitional `with gil:` push bodies is **PyObject refcount /
   per-object-lock contention** on shared Python objects (a slowdown, not serialization),
   already mostly mitigated by keeping morsels in C++ (the carrier flip). Finishing the
   nogil bodies is an **optimization, not a prerequisite**, and DOP>1 numbers are trustworthy
   today.
4. **Join shared-state.** Inner-equi only; non-empty `_compiled_right_evals` mutates shared
   `right_columns` (race — finder rejects it, `357-363`); v1 thread-local-full rebuilds
   `left_hash` per worker (`1621-1623`), costly for large build sides. The
   shared-sealed-table optimization needs the `CarcharJoinIndex` 7-scratch-member hoist
   (deferred; thread-local-full ships first).
5. **The enum migration.** `Segment.parallelism` reads the 4-value enum (`:146`); dispatch
   must move to the per-breaker `recombination_class` property before the join (which has no
   enum class today) can be dispatched generically.

## 5. Migration — strangler-fig, every step gate-green + DOP=1 byte-identical

**Status: COMPLETE.** Steps 0–7 landed and shipped as the default (`M4_USE_SCHEDULER` now
defaults on; `scheduler_engine` is the sole data path; `parallel_engine.execute` removed).
Step 8 was built and **reverted** (negative, §4.2). Each step held the gate: `make q` 190 +
tpch 22 + cb 43, result-identical, **DOP=1 golden differential** green. The strangler-fig
`M4_USE_SCHEDULER` toggle existed only during migration and is now legacy.

- **Step 0** — Land the `PipelineSink` contract (default `NONE`=serial) + the
  `recombination_class` property + `_pipeline_stream` skeleton. No behaviour change (every
  breaker still `NONE`).
- **Step 1** — `STATELESS` streaming through `_pipeline_stream`; retire `_stateless_stream`.
- **Step 2** — `SCALAR_MERGE` ungrouped agg (`merge()` exists); retire `_ungrouped_agg_stream`.
- **Step 3** — `HASH_REPARTITION` grouped agg: lift `_grouped_agg_route`'s scatter/combine/
  readout into the contract verbatim; retire `_grouped_agg_route`.
- **Step 4** — `HASH_REPARTITION` distinct (same combine, different finalize); retire
  `_distinct_stream`.
- **Step 5** — `SHARED_SOURCE` inner-equi join: the Phase-1 thread-local-full becomes the
  sink; build-before-probe is an Event dependency; retire `_join_probe_stream`. **Now
  join→agg falls out** (the agg segment's cloned tail consumes the join segment's emitted
  output — the EMIT-into-cloned-downstream model, §4.1) with NO `_join_agg_stream`.
- **Step 6** — Multi-segment DAG composition: chain segments via the Event DAG's
  `add_dependency`; multi-join falls out (build legs are upstream segments).
- **Step 7** — Fold `_pipeline_stream` into `scheduler_engine.py` as one Event per segment;
  lift the `min(ceiling, 1)` DOP pin; flip `M4_USE_SCHEDULER` default on; **delete the five
  `_find_parallel_*` finders and the `execute()` cascade.**

- **Step 8** — ~~`ORDER_MERGE` sort~~ — **BUILT then REVERTED (negative, §4.2).** The k-way
  merge kernel + parallel-local-sort sink were implemented and byte-identical, but regressed
  0.46× because the radix sort is DRAM-bandwidth-bound. Removed; sort stays serial. `join→agg`
  parallelizes (the agg); the small post-agg sort tail runs serial, as it always did.

**Kept (the proven substrate):** `identify_segments`/`Segment`, `resolve_worker_count`,
`_clone_op`, `compile_pipeline`, `pull_one`/`push_one`, `CppThreadPool`, the
`Event`/`Executor` DAG, `is_concurrent_pull_safe`. **Deleted:** the five bespoke strategies +
their finders. **New work (named, planned — not blockers):** the sort k-way merge tail kernel
(Step 8, §4.2), read-out-as-Source plumbing (§4.1, enables cross-segment without
cloned-tails), the join shared-sealed-table draken hoist (§4.4, optimization over
thread-local-full), and finishing the carrier-flip nogil bodies (§4.3, contention
optimization — not a ceiling, we are GIL-free).

## 6. What this is — and isn't

It **is** the single mechanism that makes scan/filter/project/ungrouped/grouped/distinct/
inner-join and their combinations parallelize with no per-shape code, retiring 4-5 bespoke
strategies into one executor + a thin contract — designed, not grown.

It is **not** a magic 6× on TPC-H, and the measured outcome bears that out. The honest results:
`join→agg` and multi-join parallelize and **scale with data** (Q3 1.30×@SF1 → 1.66×@SF10) —
that's the win, and it's real (no GIL ceiling; we are free-threaded, DOP>1 numbers are genuine).
But **sort does NOT parallelize** — it was built and reverted as a bandwidth-bound negative
(§4.2). Cross-segment composition uses the **EMIT-into-cloned-downstream** model (read-out-as-
Source was never needed). The deeper lesson, learned by building it: parallelism pays where
there is CPU work to spread and **loses on bandwidth-bound kernels** (sort, decode), where the
lever is touching less memory, not more threads.

It is **also not** a parallelizer for every query. By design it covers
scan/filter/project/ungrouped/grouped/distinct/**inner**-join and their combinations. Outer
joins and subquery shapes are out of scope — at SF=10 only 4/22 TPC-H queries fire a parallel
path, and the expensive serial ones (Q5/Q7/Q9/Q13/Q18) need shape coverage that is a **separate
plan**, not this one. The deliverable here is the *architecture* (one executor + a thin
contract, designed not grown), measured on real data, with the prime constraint as the gate.
