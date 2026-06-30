# Morsel-Driven Parallel Execution Engine — Design

Status: DESIGN (greenfield). This is the heart of the engine. It is NOT a port of the
existing Python scheduler (`scheduler_engine.py` / `parallel_engine.py` /
`pipeline_sink.py`) — those are discarded as this subsumes them.

## 1. Goal

Write a **morsel-driven, pipeline-parallel** execution engine. Direction (not a cage):
the DuckDB / Leis-et-al. model — pull a morsel at the source, push it through a chain of
operators into a sink, with N worker threads each driving the pipeline over disjoint
morsels and merging thread-local state at the end.

**The execution phase is 100% Python-free.** Python builds the physical plan and consumes
the result stream — those are the two edges. Once a pipeline starts executing, no Python
runs: the task loop, the operators, the state, the scheduling are all native.

**The engine controls the operator API, not the reverse.** The operator contract below is
dictated by what a clean morsel-driven engine needs. Existing push-based operators are
rewritten to it; legacy operator shape is not a constraint on the engine's design.

## 2. Model

A physical plan is an operator DAG. Execution is **morsel push**:

- A **morsel** is a unit of columnar data (`shared_ptr<CxxMorsel>`), the carrier end to end.
- A **pipeline** is a linear chain `SOURCE → OPERATOR* → SINK`. The source emits morsels;
  operators transform them in flight; the sink consumes them.
- A **pipeline breaker** (sink) ends a pipeline: it must see ALL input before producing
  output (aggregate, hash-join build, sort, distinct). Its finalized result becomes the
  SOURCE of a dependent pipeline.
- The plan therefore decomposes into a **pipeline graph** with dependency edges
  (build-before-probe, breaker-before-consumer). Pipelines with no unfinished dependency
  are runnable.

## 3. Operator API (dictated by the engine)

Every physical operator implements one or more of three roles. State is split into
**global** (shared across worker threads) and **local** (per-thread, lock-free on the hot
path) — this split is what makes morsel-driven parallelism scale.

### Source
Produces morsels; parallel by dynamic morsel assignment.
- `GlobalSourceState` — shared; owns the outstanding-morsel assignment (e.g. the scan's
  row-group / range queue). Thread-safe hand-out.
- `LocalSourceState` — per worker; its cursor into whatever the global handed it.
- `get_morsel(global, local, out) -> bool` (nogil): fill `out` with the next morsel for
  this worker; return False at exhaustion. Disjoint across workers; load-balanced (a
  worker that finishes its morsel asks the global for the next — work-stealing-ish).

### Operator (in-pipeline transform)
Stateless-per-morsel transform (filter, projection, expression eval).
- `execute(in, state, out) -> OpResult` (nogil): transform `in` → `out`. `OpResult` is
  `EMIT` (one output), `NEED_INPUT` (consumed, no output — e.g. fully filtered), or
  `HAVE_MORE` (more outputs available from this input — re-call). `state` per-thread.

### Sink (pipeline terminal / breaker)
Consumes the pipeline's morsels into accumulated state.
- `GlobalSinkState` — shared; the merged result region (e.g. the list of per-thread
  aggregate tables, or the shared hash table).
- `LocalSinkState` — per worker; the thread-local accumulator (its own hash table /
  buffer). Lock-free during `sink`.
- `sink(in, global, local) -> SinkResult` (nogil): accumulate `in` into `local`.
- `combine(global, local)` (nogil): merge this worker's `local` into `global`. Called once
  per worker after the source is exhausted.
- `finalize(global)` (nogil): produce the breaker's result; the result is exposed as a
  SOURCE for the dependent pipeline. Called once after all `combine`s.

The **output sink** of the terminal pipeline writes into the result `MorselQueue` the
cursor drains — the only Python-facing edge of execution.

## 4. Scheduler / pipeline execution

- **Build** the pipeline graph from the plan (split at sinks; add dependency edges) — the
  once-crossed planning→execution boundary. Done from the plan; produces native pipeline
  objects holding native operators + their global states.
- **Run** pipelines in dependency order. For a runnable pipeline at degree W:
  1. Create `GlobalSourceState` + `GlobalSinkState`.
  2. Spawn W native worker tasks on the `CppThreadPool` (`submit_native`). Each task:
     - make `LocalSourceState` + `LocalSinkState` + per-op `state`.
     - loop: `get_morsel`; for each operator `execute`; `sink`. Until source exhausted.
     - `combine(global, local)`.
  3. Barrier (`wait_native`); then `finalize(global)` once.
- **Serial is W=1.** No separate "row-floor / byte-identical-to-serial" code path — the
  same drive with one worker. DOP is a number, never a branch to a different driver.
- The drive loop, the operators, the states are native; tasks carry no Python closure and
  no `Future`. The carrier is `shared_ptr<CxxMorsel>` throughout.

## 5. Python boundary (the two edges)

- **In:** the planner produces the physical plan and the engine builds the pipeline graph
  (constructing native operator + state objects). This is setup, crossed once.
- **Out:** the terminal sink streams `CxxMorsel`s into a `MorselQueue`; the cursor (Python)
  drains it.
- Between those edges, during pipeline execution: **no Python.**

## 6. Reuse (substrate, not scaffolding)

Reused as the data/compute substrate (already native): `CxxMorsel` (carrier), the compute
kernels, the aggregate / group-hash / distinct / carchar engines (as Local/Global sink
state implementations), `MorselQueue` (output edge), `CppThreadPool` + `submit_native` /
`wait_native` (the task pool). Discarded (Python scaffolding): the `PipelineSink` class
tree, per-shape handlers, `dispatch_data_pipeline`, the generator/closure drivers.

## 7. Build sequence (verified vertical slices)

1. **Core**: the operator API (Source/Operator/Sink + Global/Local state) + `Pipeline` +
   the single-pipeline parallel executor. Prove `scan → filter → project → output` runs
   parallel, native, results to the queue, W=1 == W=N.
2. **Aggregate sink**: grouped + ungrouped agg as a Sink (local tables → combine → finalize).
3. **Pipeline graph + dependencies**: dependent pipeline sources from a finalized sink
   (GROUP BY → ORDER BY, GROUP BY → DISTINCT).
4. **Hash join**: build pipeline (Sink = hash build) feeding a probe-pipeline join Operator.
5. **Cutover**: repoint `execute()`; delete the old scheduler / parallel_engine /
   pipeline_sink and the push-based operator API.

## 8. Non-goals / explicitly out

- No preservation of the existing push-based `BasePlanNode.push` API or the `PipelineSink`
  hierarchy — the engine dictates the new API.
- No per-shape special-casing in the engine — operators declare their roles; one general
  drive runs them.
- No Python on the execution path. Planner and cursor are the only Python, at the edges.
