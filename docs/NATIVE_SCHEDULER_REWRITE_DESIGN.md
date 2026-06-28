# Native Execution Scheduler — Design

Status: **DRAFT for architect ratification.** No code until sign-off.

## 0. Goal

Remove the last Python island on the execution path. Today the operator
pipeline is native (`cdef class` operators, a `nogil` `push(shared_ptr[CxxMorsel],
ErrCtx*)` drive surface), but the thing that *schedules and drives* it —
`opteryx/managers/execution/scheduler_engine.py` and the per-shape handlers in
`parallel_engine.py` — is Python: `queue.Queue` hand-off, `threading.Lock`/
`Event`, a Python `Event`/`Executor` DAG, and per-morsel `push_one`/`pull_one`
shim calls that bounce the GIL once per morsel.

This is a **ground-up rewrite as a Cython executable**: declared `cdef` types,
explicit C++ ownership and lifetimes, native synchronisation. Not a `.py`→`.pyx`
recompile.

Per CLAUDE.md §1/§2 the line is by PHASE. `identify_segments` (cutting the plan
into pipeline segments) stays on the **planning** side. Everything past it —
DAG, drive loop, hand-off, fan-out — becomes native.

## 1. Two sanctioned boundaries (the only Python crossings)

1. **Plan in.** Python planning hands a compiled segment list to the Cython
   `execute(plan, segments, dop, telemetry)`. One crossing, at query start.
2. **Result out.** The cursor materialises the final morsel to PyObject columns
   via `Morsel.materialize()` — the *existing* single sanctioned shim point
   (`_morsel_shim.pyx`). The engine-internal hand-off never materialises.

Nothing else crosses. No `cdef class Morsel` (the shim) enters the queue or the
drive loop; the carrier is `shared_ptr[CxxMorsel]` end to end.

## 2. What already exists (do not rebuild)

- `shared_ptr[CxxMorsel]` carrier; `CxxMorsel` columns are `DrakenVector` views,
  `nogil`-accessible (`_col_view`).
- `BasePlanNode.push / push_left / push_right (shared_ptr[CxxMorsel], ErrCtx*)
  noexcept nogil` and `next_morsel() -> shared_ptr[CxxMorsel]`.
- EOS as `cxx_morsel_new_eos()` / `MorselState.END_OF_STREAM`.
- `ErrCtx{code,msg}` + `_stash_exc`/`_take_exc` for surfacing a Python exception
  out of a `nogil` region at the boundary.
- `CppThreadPool` (BS::thread_pool).

The rewrite *consumes* this surface directly. `push_one`/`pull_one` (the
per-morsel GIL-bouncing Python shims) are **bypassed**, not reused.

## 3. Type model (all declared, no `object` on the hot path)

| Today (Python)              | Native                                                            |
|-----------------------------|-------------------------------------------------------------------|
| `Segment` (Python class)    | `cdef class Segment` — `cdef` typed fields (node ids, flags)       |
| `Event` (Python class)      | `cdef class Event` — `cdef int` counters, typed parent list        |
| `Executor` (Python class)   | `cdef class Executor` — owns events, native sync                   |
| `queue.Queue` hand-off      | `MorselQueue` — C++ bounded ring of `shared_ptr[CxxMorsel]`        |
| `threading.Lock`/`Event`    | `std::mutex` / `std::condition_variable` (cdef RAII guard)         |
| `_DONE = object()` sentinel | EOS `CxxMorsel` (`MorselState.END_OF_STREAM`)                      |

## 4. The hand-off queue — ownership (the decision #1 we settled)

`MorselQueue`: a bounded queue holding `shared_ptr[CxxMorsel]` by value, built on
**vendored moodycamel**. No Python object ever sits on a native queue — the
carrier is the C++ `shared_ptr[CxxMorsel]`, period.

- **Carrier ownership is C++ shared ownership.** Enqueue moves a `shared_ptr`
  in; dequeue moves it out. No `Py_INCREF`. No shim crossing the queue. When the
  last `shared_ptr` drops, the `CxxMorsel` frees deterministically.
- **Bounded depth** (`_QUEUE_DEPTH`, default 8) → backpressure. moodycamel's MPMC
  `BlockingConcurrentQueue` blocks the *consumer* when empty but is natively
  *unbounded* on enqueue; producer-side bounding is a `LightweightSemaphore`
  initialised to `_QUEUE_DEPTH` (acquire-before-enqueue, release-after-dequeue) —
  the standard moodycamel bounded idiom. Matches the serial streaming memory
  profile.
- **Drain-at-close.** On early consumer abandonment (LIMIT), `close()` drops all
  queued `shared_ptr`s (freeing their morsels) and wakes a blocked producer.
  Mirrors `MorselRef`'s drain-on-destruct discipline.
- `nogil` `put`/`get` — the carrier is C++, the queue is C++; no GIL needed to
  move a morsel across the boundary.

### 4.1 Queue shape — RESOLVED (A: MPMC, vendor `concurrentqueue.h`)

The per-shape fan-out (`out_q` in `_stateless_stream`, `_join_probe_stream`, …)
is **N workers → 1 consumer = MPSC**. moodycamel's vendored variants
(`readerwriterqueue.h`, `readerwritercircularbuffer.h`) are SPSC only, so we
**vendor `concurrentqueue.h`** (MPMC) — architect-approved (§4). One `MorselQueue`
type serves both the terminal hand-off and the fan-out. (Per-worker SPSC was the
no-new-dependency alternative; rejected — one MPMC queue is simpler than N SPSC
queues + round-robin drain.)

### 4.2 Dead code removed

`opteryx/compiled/pyobject_queue.{pyx,cpp,so}` and `src/cpp/pyobject_queue.hpp`
(a `ReaderWriterQueue<PyObject*>` doing INCREF/DECREF) had **zero consumers** —
built but never imported. It is exactly the Python-object-on-a-native-queue
pattern this rewrite forbids. **Deleted** (files + `setup.py` extension entry) as
part of slice 1.

## 5. The drive loop — `nogil`, no per-morsel GIL bounce

Per worker, once: build the cloned operator chain (typed `cdef`-class
construction), then:

```
while True:
    cdef shared_ptr[CxxMorsel] m = scan.next_morsel()   # nogil
    if m.get() == NULL: break
    head.push(m, &err)                                  # nogil, whole chain
    if err.code: break
# push EOS carrier, then drain pending to MorselQueue
```

The entire loop is `nogil`. Contrast today: `push_one` re-acquires the GIL and
runs `cxx_to_morsel` for **every** morsel. Even in free-threaded 3.14t the
GIL-state transition and the shim alloc are per-morsel cost this removes.

## 6. DAG ownership & lifetimes

- The Cython `execute()` generator owns one `Executor` for the query's lifetime.
- `Executor` is the **sole owner** of its `Event`s. `Event.parents` is a
  **borrowed** typed list (no back-owning reference) — breaks the current
  `event ↔ executor` cycle by construction.
- Deterministic teardown order on generator close: drain/close `MorselQueue` →
  join worker futures → `Executor` releases `Event`s → `pool.shutdown(wait=True)`.
- Dependency edges (build-before-probe, breaker ordering) stay as the proven
  two-counter model (DuckDB §6), now with `cdef int` counters under a `std::mutex`.

## 7. Synchronisation

Every `threading` primitive is replaced 1:1:

- `Executor.lock` → `std::mutex` (cdef RAII guard for the counter updates).
- `Executor._all_done` / completion signalling → `std::condition_variable`.
- per-worker `pull_lock` (shared scan pull) → `std::mutex`.

No `import threading`, no `import queue` survives in the execution package.

## 8. Errors & cancellation

- In-loop errors surface via `ErrCtx`; the worker stops, stashes via `_take_exc`,
  and the `Executor` keeps the first error (idempotent), re-raised on the consumer
  thread after the EOS carrier wakes it. Same semantics as today's `stash_error`.
- LIMIT / early close: consumer `close()` → `MorselQueue.close()` → workers see
  the closed queue on next `put` and unwind, running operator `finally` (ctx
  terminate, source close).

## 8.1 Slice 3 — native terminal hand-off (DRAFT, for ratification)

Traced the live single-segment path. Today: `scheduler_engine.execute` runs one
terminal `Event` on a `CppThreadPool`; the task is `_drive_pipeline`, a Python
generator (`dispatch_data_pipeline` → `drive_scan`) that `yield`s **Python `Morsel`**
(from `exit_node.pop_pending()`); morsels cross a Python `queue.Queue` to `_stream`;
`threading.Lock`/`Event` guard the `Executor`. So three Python things on the path:
the `queue.Queue` hand-off, the `threading` sync, and the Python drive generator.

**Foundation landed:** `MorselQueue.finish()` (graceful, in-band null sentinel →
`MQ_FINISHED`, drops nothing) vs `close()` (abandon, drop) — the end-of-data
semantics the hand-off needs. Using `close()` as "done" would lose unconsumed
morsels; `finish()` is the correct signal.

**The crux:** the terminal `Exit` emits Python `Morsel` via a `_pending` deque
drained by a Python generator. A native hand-off needs `Exit` to emit
`shared_ptr[CxxMorsel]` into a sink. Two ways:

- **(A) Queue-swap only** — keep the Python drive generator + `Executor`; just
  replace `queue.Queue` with `MorselQueue`, converting `Morsel`↔`cxm` at the
  boundary (`morsel_to_cxx`/`cxx_to_morsel`). Removes `queue.Queue` only. Adds a
  per-output-morsel round-trip (cheap for Cxx-backed, but a representation-change
  risk for PyObject-backed). Keeps `threading` and the Python drive. **Lateral —
  not recommended.**
- **(B) Native terminal sink + drive** — give `Exit` a `cdef` sink seam; the drive
  becomes a native loop (`scan.next_morsel` → `head.push` → `Exit` emits `cxm` into
  the `MorselQueue` sink), `finish()` on EOS, `close()` on early abandon. Removes
  `queue.Queue` AND the Python drive generator AND (for a single segment, which has
  no dependency edges) the `Executor`/`threading` entirely. The `cxm` round-trip
  collapses to one cheap `morsel_to_cxx` at `Exit.select`'s output. **Recommended.**

Scope for (B): single-segment plans first (no breaker chain, no build-before-probe).
Multi-segment keeps the Python `Executor` until slice 4. DOP=1 byte-identical.

**DECIDED: B.** First increment ✅ **DONE** — the DOP=1 single-scan path (the
byte-identical-serial prime constraint): `drive_scan_to_sink` (`_operators.pyx`) is
the push-loop twin of `drive_scan` — pushes the scan through the chain `nogil` and
emits the terminal Exit's output into a `MorselQueue` (`sink.put` releases the GIL
on the backpressured enqueue; False = consumer abandoned; `finish()` on EOS).
`scheduler_engine._native_serial_execute` runs it on a `CppThreadPool` with a
consumer generator draining the queue — **no `queue.Queue`, no `Executor`, no
`threading`, no Python drive generator** for this path. Verified: `make q` 190/190
at default DOP (dormant) AND at `MAX_EXECUTION_WORKERS=1` (every single-scan plan
through the native drive); LIMIT early-close returns exactly N with no hang;
backpressure over 2.4M rows; parallel-vs-serial sha-identical. Bonus: 0.74s vs 5.36s
on the suite (no pool/Executor spin-up for the serial path).

**Slice 4 — parallel handlers to native (the production bulk). STATELESS DONE.**
`_native_stateless_execute` (scheduler_engine) replaces `_stateless_stream` for the
`scan → stateless* → exit` CONCAT shape (the most common parallel shape — filter/
projection at scale). W workers each push their cloned chain's output into a shared
`MorselQueue` via `stateless_worker_drive` (`_operators.pyx`, native per-morsel push,
no EOS — matches the old worker). **Removed for this shape: the `queue.Queue` fan-out,
the per-morsel `push_one` shim, AND the outer `Executor`/`queue.Queue`.** Still Python:
the `next_input` pull coordination (one `threading.Lock` guarding a Python buffer
iterator — until the scan pull goes nogil) + the row-floor serial fallback (tiny
inputs). Verified: `make q` 190/190 at default DOP=8 (path live across the suite);
native-parallel(8)-vs-serial multiset sha-identical on 2.4M rows.

**JOIN PROBE DONE.** `_native_join_execute` replaces `_join_probe_stream` for the
parallel inner-equi-join shape: serial build prelude (builds `left_hash` on the
join), then W workers each probe a disjoint slice through a private join clone
(sharing the one read-only `left_hash`), streaming matches into a shared
`MorselQueue`. Reuses `stateless_worker_drive` + a final `push_one_to_sink(EOS)`
(the cloned join needs EOS to flush its path — the one difference from stateless).
Removed for this shape: `queue.Queue` fan-out, per-morsel `push_one`, outer
`Executor`/`queue.Queue`. Verified: `make q` 190/190 (serial-fallback at small data);
native-parallel(8)-vs-serial multiset sha-identical (30209 rows). The join clone
still uses the default reflection `make_worker` (inner-join `make_worker` deferred,
slice 2d) + the parallel engine's external build-state injection — unchanged.

**BREAKER ACCUMULATE DONE (per-morsel hot path).** The breaker (agg/distinct/GROUP
BY) is BARRIER-based — workers accumulate into per-worker engines → barrier →
`PipelineSink.combine` → finalize read-out — so the streaming-sink model doesn't fit
its accumulate phase. But the accumulate is the per-morsel hot path, and it's now
native: `accumulate_worker_drive` (`_operators.pyx`, native `nogil` push, no sink —
the breaker accumulates internally) replaces the worker's per-morsel `push_one` in
BOTH `_run_breaker_segment` and `_drive_segment` (the multi-breaker producer).
Verified: `make q` 190/190; big parallel GROUP BY (DOP=8) byte-identical to serial.

**EXECUTOR BYPASSED FOR ALL NON-CHAIN PLANS.** A breaker plan's drive is still a
single terminal task (the producer segment is a no-op dependency), so the `Executor`
DAG was pure overhead for it too. `execute()` now routes EVERY non-chain plan
through `_native_generic_execute`: the per-shape handler (`dispatch_data_pipeline` →
breaker / serial / …) runs on a `CppThreadPool` straight into a `MorselQueue`, the
consumer drains — no `Executor`, no `queue.Queue`. Verified by path-instrumentation:
GROUP BY and ungrouped agg now hit `native_generic`; the multi-breaker chain (GROUP
BY → DISTINCT) still hits the `Executor`. `make q` 190/190; big GROUP BY byte-
identical parallel-vs-serial.

**SLICE 5 DONE — the `Executor` is DELETED.** The multi-breaker chain (the last DAG
user) is now `_native_chain_execute`: a linear sequence (producers materialise →
terminal streams) driven sequentially into a `MorselQueue`, no DAG. With nothing left
using it, `Executor`/`Event`/`_build_segment_dag`/`_build_multibreaker_chain_dag`/
`_drive_pipeline`/`_stream`/`_DONE` and the `import queue`/`import threading` were
**deleted** from `scheduler_engine.py`. Verified: no dangling refs anywhere; module-
level `Executor`/`Event`/`queue.Queue`/`threading` grep = 0; `make q` 190/190; chain
+ GROUP BY byte-identical. **No Python `Executor`/`Event` DAG and no `queue.Queue`
hand-off remain — every plan runs a native drive into a `MorselQueue`.**

**Dead code exposed (follow-up cleanup):** the native gates handle stateless/join
before dispatch, so the OLD `_stateless_stream` / `_join_probe_stream` in
`parallel_engine.py` (and their dispatch branches) are now unreachable — prune them.

**Still Python below the Executor (the substrate, next layer down):** the per-shape
handler generators (`_run_breaker_segment`, `_serial_stream`) that `_native_generic_
execute` pumps; `PipelineSink.combine`/`finalize_source`; the pull-coordination
`threading.Lock` in each handler; and the morsels are still Python `Morsel` objects
converted to `cxm` at the sink boundary (cxm-end-to-end is the deeper item).

## 9. Staging (each slice independently compiles + passes `make q`)

1. **`MorselQueue`** — ✅ **DONE.** `src/cpp/morsel_queue.hpp` (moodycamel MPMC
   `BlockingConcurrentQueue` + `LightweightSemaphore` bound) carrying
   `shared_ptr[CxxMorsel]`, drain-on-close; `opteryx/compiled/morsel_queue.pyx`
   wrapper (`nogil` `_put_cxx`/`_get_cxx`, Python test edge). Isolation tests in
   `tests/unit/test_execution/test_morsel_queue.py`. `concurrentqueue.h`/
   `blockingconcurrentqueue.h`/`lightweightsemaphore.h` vendored (v1.0.4); dead
   `pyobject_queue.*` deleted. `make q` 190/190. No scheduler wiring yet.
2. **Worker plan-distribution** (§9.1) — replace `_clone_op`'s Python fork with the
   spec/worker-state split. Opens with an operator spec/state audit (join path is
   the model). Cross-operator change, parity-tested before anything depends on it.
3. **`cdef class Executor`/`Event`/`Segment`** + native sync, driving the
   **existing** single terminal pipeline through the `nogil` `push` surface
   (replaces `scheduler_engine.py`). Single-segment plans first.
4. **Per-shape handlers** (`_stateless_stream`, `_join_probe_stream`,
   `_run_breaker_segment`, multi-breaker chain) ported onto the native queue +
   `nogil` drive + the slice-2 distribution mechanism (replaces
   `parallel_engine.py`).
5. **Delete** `scheduler_engine.py`, `parallel_engine.py`, and the now-unused
   `push_one`/`pull_one`/`push_left_one`/`push_right_one` Python shims.

### 9.1 Worker plan-distribution — kill the Python fork (Q2, OPEN)

Two things are bundled in today's "forking" and must not be conflated:

- **The strategy (KEEP).** Partition data by `hash(key) % radix`; each worker
  holds *private* operator state over a disjoint partition; CONCAT / scalar-merge
  at the end. No shared mutable state. This is the strategy that **won** — shared-
  mutable intra-op parallelism was tried and lost; partition+private-state is the
  central-scheduler direction.
- **The mechanism (KILL).** How a worker gets its private state: today
  `type(op)(properties=…, **op.parameters)` — re-running the operator's Python
  `__init__` to deep-copy a fully-built operator. This Python fork is the hack.

A `cdef clone(self)` that deep-copies a built operator natively would just
**re-skin the fork** — wrong target. The intended replacement is a **spec/state
split**: each operator separates an *immutable spec* (built once from the physical-
plan node, shared read-only across all workers) from *worker-local mutable state*
(instantiated per worker from the spec). The scheduler hands every worker the one
shared spec and spins up only the per-worker state — no operator copy, no
`__init__` round-trip, no Python. The join path already half-does this (workers
"rebuild their OWN private join engine from the shared read-only `left_morsel`").

Stateless ops (filter/project) holding no per-morsel mutable state may share a
single instance across workers; only breakers (agg, sort, distinct, join build)
need per-worker state — an optimisation to confirm, not assume.

**DECIDED:** keep the strategy, kill the mechanism, replace with the spec/worker-
state split above. Slice 2 opens with an audit of how cleanly each operator family
already separates immutable spec from mutable state (the join path is the model);
that audit shapes the per-operator work, it does not reopen the decision.

### 9.2 Audit findings (2026-06-27) — the split is uneven; resequence slice 2

Audited every cloned data-pipeline operator. Verdicts:

- **CLEAN** (spec already on its own fields; only waste is `__init__` recomputing
  it): projection, exit, null_reader, function_dataset, distinct, sort,
  cross_join, non_equi_join.
- **MODERATE** (a specific refactor): filter, read, aggregate_node, heap_sort,
  hashed_inner_join, outer_join, nested_loop_join, asof_join, union.
- **ENTANGLED** (spec & state fused, or no per-worker model at all): parquet_read,
  ALL ungrouped aggregates, grouped_aggregate_hashed, filter_join, unnest_join,
  window/row_number.

Four cross-cutting patterns drive the work:

1. **Immutable compiled programs recompiled per worker** — `_compiled_evals`
   (sort, heap_sort, filter, projection, joins) and compiled predicates are built
   in `__init__` but are read-only at run time. **Hoisting these into shared spec
   is the highest-value, lowest-risk first move** and covers most of the CLEAN set.
2. **Lazy first-morsel resolution** — column indices, types, key-kinds, keep-buffers
   are resolved on the FIRST push against the data schema, not in `__init__` (filter
   `_flt_*`, parquet_read `_sp_*`, grouped-agg collector binding, union `schema`).
   So "spec" is NOT fully derivable from the plan node alone — it needs the input
   schema. **Lever: resolve these at BIND time** (the input schema is known then)
   into the spec, instead of per-worker first-push. Otherwise they stay per-worker.
3. **Aggregates fuse config with accumulator in one `cdef class`** — every
   `UngroupedAggregate` subclass welds `column_name`/`alias`/`result_type` to its
   running totals; grouped collectors weld type/column config to state buffers.
   This is the largest, genuinely-new refactor: split each into a spec object +
   an accumulator. `merge()`/`merge_from` precedent helps; COUNT(DISTINCT)/MEDIAN
   are non-mergeable. The frozen `AggregationSpec` dataclass is the seed.
4. **Global-semantics streaming state that CANNOT be data-partitioned** —
   window/row_number `_counts` (running per-partition sequence), unnest_join
   `hash_set` (global DISTINCT), union `schema`+leg-counter. These are not
   join-shaped; partitioning them per worker changes the *answer*. **They must be
   marked serial/merge-only, never driven through the partition-parallel worker
   model.** `_clone_op` already silently splits some of these — a latent bug the
   split must make explicit.

Joins (the exemplar) are the *best* case for the build/probe shape:
hashed_inner_join already runs a read-only shared `CarcharJoinEngine` with
stack-local `ProbeScratch` for concurrent probe; cross_join/non_equi are clean.
Caveats to fix: per-instance telemetry (`kernel_metrics`/`join_readings`) is not
per-worker (lost-update race if shared); outer_join shares a build only for LEFT
outer (RIGHT/FULL rebuild inside the probe); **filter_join mutates its shared
`right_hash_set` from inside the probe** (unguarded PerfectHash→Carchar fallback)
— that must move to build time before the set can be shared.

**Resequenced slice 2:**
- **2a** — ✅ **DONE.** Contract on `BasePlanNode` (§9.3): `resolve_schema`
  (bind-time hook, default no-op), `make_worker` (default = reflection fallback;
  overridden by projection/sort to share SPEC by reference, no recompile),
  `is_partition_parallel` (default True), `_copy_worker_base` helper; cpdef edges
  `spawn_worker`/`operator_is_partition_parallel`/`operator_resolve_schema`;
  `_clone_op` routed through `spawn_worker`. Proven: ProjectionNode via real
  scheduler fan-out (SPEC shared, STATE fresh, 53.9M rows correct), SortNode via
  direct plan capture. `make q` 190/190.
- **2b** — ✅ **DONE.** `make_worker` overrides on filter, exit, distinct, heap_sort
  (sharing compiled predicate / evals / spec by reference, fresh STATE). Filter
  gained a `__cinit__` NULLing its owned malloc buffers so `__new__`-built workers
  are dealloc-safe. exit/distinct/heap_sort verified by direct capture (SPEC shared,
  STATE fresh); filter spawns clean + parallel(4)-vs-serial(1) byte-identical on a
  60M-row fan-out (sha match, 2.4M rows). `make q` 190/190. (Pattern-2 bind-time
  `resolve_schema` for filter's first-push caches deferred — kept as per-worker
  STATE for now, byte-identical; bind-time migration is its own focused step.)
- **2c** — aggregates. **Ungrouped: ✅ DONE.** `UngroupedAggregateNode.make_worker`
  shares parsed aggregates + compiled evals + identifier caches by reference;
  STATE (engine accumulators + literal states) rebuilt fresh via an extracted
  `_build_accumulators` factory. Parallel(4)-vs-serial(1) byte-identical on a 60M-row
  SUM/COUNT/AVG/MIN/MAX query; `make q` 190/190. Scope note: did NOT do the full
  per-class config/accumulator teardown (~15 classes) — the expensive shared work
  is the eval compile + parse, already shared at node level; per-aggregate config
  rebuild is nanosecond-cheap, so the teardown is high-risk for ~0 metric gain.
  **Grouped: ✅ DONE.** Architect chose full consistency: the data engine has no
  Python operators. `GroupedAggregateHashedNode` CONVERTED from plain `class` to
  `cdef class` (12 fields declared; `_engine`/`_parallel_engines` are `object` —
  the parallel sink wraps `_engine` in a `_ScatterCollectEngine`). `make_worker`
  shares parsed SPEC by reference; STATE rebuilt via an extracted `_build_engine`
  factory. Serial-vs-parallel byte-identical (COUNT/SUM/AVG over 60M rows);
  `make q` 190/190. **Trap caught:** a `cdef class` overriding a `cpdef` base method
  with a bare `def` does NOT update the C vtable — `_push_impl` silently ran the
  base and produced empty output; fixed by declaring the override `cpdef`. (Plain
  classes don't hit this — their cpdef override falls back to Python lookup.)
  Boundary: special ops (EXPLAIN/SET/SHOW/INSERT/DDL) STAY Python — they run on the
  serial engine, outside the relational/data engine, so Python is correct there.
  Data-engine consistency: ✅ COMPLETE. `FunctionDatasetNode` + `NullReaderNode`
  converted to `cdef class` (relation sources; not cloned, so no make_worker — just
  native). Every relational operator is now a `cdef class`; the only Python operators
  left are the special ops on the serial engine (correct). `make q` 190/190;
  VALUES/GENERATE_SERIES/UNNEST/FILTER(FALSE) all correct.
- **2d** — joins. **Re-scoped after tracing the parallel path, mostly DEFERRED.**
  `_find_parallel_join` parallelizes ONLY `DrakenInnerJoinNode` — outer/cross/nlj/
  non_equi/asof/filter/semi/anti are rejected by class and run serial. Consequences:
  (1) the filter_join probe-time `right_hash_set` mutation is a LATENT race (filter_
  join is never fanned out) — documented as a constraint, NOT fixed (no live path);
  (2) only the inner join is cloned, and its `make_worker` is the worst risk/value
  in slice 2 — two `__init__` levels to replicate, 7 fields the parallel engine
  injects post-clone, build-mutated param-derived lists (byte-identical landmines),
  and for the parallel shape `_compiled_right_evals` is required-empty + left evals
  trivial, so the recompile saved ≈0. The default reflection clone already works.
  **Deferred to slices 3–5**, where the native scheduler makes the worker-construction
  requirement concrete (and may redesign the external-injection mechanism). Per-worker
  telemetry is a non-issue: the default clone already gives fresh `kernel_metrics`/
  `join_readings` via `__init__`.
- **2e** — ✅ **DONE.** `is_partition_parallel()` overridden to `False` on the three
  global-semantics operators — `WindowNode` (`_counts` running counter),
  `UnnestJoinNode` (global DISTINCT `hash_set`), `UnionNode` (first-morsel schema +
  leg counter). `spawn_worker` now asserts `is_partition_parallel()` and raises
  `InvalidInternalStateError` if the scheduler ever tries to fan one out — turning
  `_clone_op`'s old silent mis-split into a loud failure. `make q` 190/190; the three
  run serial (assert dormant), verified unbroken.

### 9.3 Slice 2a — the spec/state contract on `BasePlanNode` (DRAFT, for ratification)

Replace `_clone_op(op) = type(op)(**op.parameters)` with three `cdef` surfaces on
`BasePlanNode`, overridden per operator:

1. **Field discipline (declaration, not code).** Each operator's `cdef` fields are
   partitioned into SPEC (built once, immutable after bind, shared read-only across
   worker threads) and STATE (per-worker, mutable during push). Documented per
   class; the two methods below encode it.

2. **`cdef void resolve_schema(self, input_schema) except *`** — the bind-time hook
   (pattern 2). Called once during physical-plan build, when the input schema is
   known, BEFORE execution. Populates the SPEC's resolved fields (column indices,
   types, key-kinds, keep-buffer shapes) that today resolve on first push. After
   this, SPEC is frozen; no first-push resolution remains. Default: no-op.

3. **`cdef BasePlanNode make_worker(self)`** — replaces `_clone_op`. ALWAYS returns
   a **fresh-STATE** worker instance that **borrows the SPEC by reference** (shares
   the already-compiled programs / resolved caches, and read-only-after-build
   artifacts like the inner-join engine — no `__init__`, no recompile; that is the
   pattern-1 win). Cost is one `cdef` alloc + a few reference assignments. It does
   NOT `return self` — that would share mutable STATE (including the `readings`
   telemetry counters) across worker threads, a lost-update race in free-threaded
   3.14t. STATE — including `readings` — is per-worker; the scheduler sums each
   worker's `readings` at EOS. That fixed-size counter reduction is NOT the
   per-row/per-group data merge the partition strategy exists to avoid; it is O(
   workers × counters), off the data path. No shared mutable state anywhere.

4. **`cdef bint is_partition_parallel(self)`** — pattern 4 marker. Operators with
   global-semantics streaming state (window/row_number `_counts`, unnest_join
   global DISTINCT, union schema+leg-counter) return **False**: the scheduler runs
   them serial/merge-only and MUST NOT call `make_worker` for fan-out. `make_worker`
   on a non-partition-parallel operator **asserts** — turning `_clone_op`'s current
   silent mis-split into a loud failure. Default: True.

Note: SPEC is the only thing shared across worker threads, and it is read-only
after `resolve_schema`. STATE (buffers, accumulators, `readings`) is per-worker by
construction. There is no shared mutable state, hence no lock/atomic on the worker
fan-out — the only synchronisation is the `MorselQueue` hand-off.

Scheduler use: at plan build it calls `resolve_schema` on every operator (SPEC
frozen); at fan-out it calls `make_worker` per worker for partition-parallel
operators, and routes the rest to the serial/merge drive. No `type(op)(**dict)`
reflection, no per-worker recompile, no Python `__init__` on the execution path.

Proof-of-contract: land 2a + apply it to `projection` (CLEAN, compiled-evals SPEC,
STATE = only `readings`) and `sort` (CLEAN, compiled-evals SPEC + STATE `_morsels`
+ `readings`) before rolling to the rest — both prove the share-SPEC / fresh-STATE
split and the per-worker `readings` reduction. DOP=1 byte-identical throughout.

DOP=1 must stay byte-identical to the serial path at every slice (the prime
constraint). No flags, no shadow path, no fallback (§ contract).

## 10. Decisions

- **Q1 — RESOLVED.** `MorselQueue` on **vendored moodycamel `concurrentqueue.h`**
  (MPMC, architect-approved), carrying `shared_ptr[CxxMorsel]` only — no
  `PyObject*` on any native queue. The dead, unreferenced `pyobject_queue.*`
  (PyObject-on-queue hack) is **deleted** (§4.2).
- **Q2 — RESOLVED.** Strategy (partition + private state + concat) is kept; the
  Python fork mechanism (`_clone_op`) is killed and replaced by a **spec/worker-
  state split**, not a native deep-copy clone (§9.1). Slice 2 opens with the
  operator spec/state audit (shapes the work, not the decision).
- **Q3 — RESOLVED.** `identify_segments` stays planning-phase; may be pythonic
  Cython or pure Python (planning-phase latitude).
- **P2 (audit) — RESOLVED.** Lazy first-morsel resolution moves to BIND time via
  `resolve_schema` (§9.3), into shared SPEC — not per-worker state.
- **P4 (audit) — RESOLVED.** Global-semantics operators (window/unnest/union) stay
  serial/merge-only; `is_partition_parallel()=False` + `make_worker` asserts.
  Fixing `_clone_op`'s silent mis-split is in scope in 2e, not deferred.
- **2a contract (§9.3) — PENDING ratification** before base-class code.
