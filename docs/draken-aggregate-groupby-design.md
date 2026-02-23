# Draken-Native Aggregate + Group By Design

## Context

Current `AggregateAndGroupNode` and `SimpleAggregateAndGroupNode` are still primarily Arrow-based and rely on NumPy/Arrow operations in hot paths. This design proposes a full Draken-native aggregate/group-by execution path:

- Convert to Draken on operator entry.
- No NumPy in aggregation/grouping kernels.
- No Arrow in aggregation/grouping kernels.
- Optional spill-to-disk for high-cardinality / large state.
- Reuse existing shuffle/spill primitives and KV-backed spill layers rather than introducing a separate spill subsystem.

This is a design doc only.

---

## Goals

1. Build a single Draken-native grouped aggregation pipeline that supports:
- `COUNT`, `SUM`, `MIN`, `MAX`, `AVG`, `COUNT(DISTINCT)`, `ANY_VALUE`/`ONE`
2. Eliminate NumPy/Arrow from grouped aggregation execution.
3. Support large cardinality with deterministic spill-to-disk.
4. Preserve current SQL semantics.
5. Improve ClickBench heavy group-by stability and latency.

## Non-Goals (initial implementation)

1. `ARRAY_AGG` with `ORDER BY` / `LIMIT` in v1.
2. Approximate aggregates in v1.
3. Full vectorized expression engine rewrite in same PR.

---

## High-Level Architecture

Introduce a new operator path:

- `DrakenAggregateAndGroupNode` (new)
- Optional future: `DrakenAggregateNode` (non-grouped)

Execution model:

1. `execute(morsel)` calls `ensure_draken_morsel` immediately.
2. For each input morsel:
- Evaluate required pre-aggregate expressions (Draken path).
- Extract group key vectors + aggregate input vectors.
- Insert/merge rows into in-memory hash aggregate state.
3. On memory pressure:
- Partitioned spill of current hash state to disk.
- Clear in-memory state and continue.
4. On EOS:
- Finalize in-memory + spilled partitions.
- Emit output morsels in chunks.

No conversion to Arrow inside the operator.

---

## Core Components

## 1) Group State Store

New Draken Cython/C++ component:

- `GroupStateStore`
- Backed by hash table: `group_key_hash -> state_index list` + collision chain.
- State vectors are columnar (struct-of-arrays), one vector per aggregate state field.

Design notes:

- Must not trust 64‑bit hash alone (there is always a non‑zero collision probability).  implementors should include a collision‑safe equality check; for example store a small fingerprint plus the full key payload, or keep the unhashed key values alongside the hash bucket.
- Store group key fingerprint + row‑id/key payload for exact compare.
- Keep keys in Draken-friendly representations (fixed-width inline, var-width arena offsets).

## 2) Aggregate Function State Kernels

Each aggregate defines:

- `init(state_idx)`
- `update(state_idx, row_idx)` (vector-aware batch variant preferred)
- `merge(dst_state_idx, src_state_idx)` (for spill merge / partition merge)
- `finalize(state_idx) -> scalar`

Expected state layouts:

- `COUNT`: int64
- `SUM`: type-specific accumulator (int128 optional for overflow policy)
- `MIN/MAX`: value + seen flag
- `AVG`: `sum + count`
- `COUNT(DISTINCT)`: per-group hash set handle (see dedicated section)

## 3) Draken Expression Pre-Evaluation Bridge

Grouped aggregation depends on `evaluate_and_append`, which is Arrow/NumPy-oriented today. To keep this design constraint:

- Add `evaluate_and_append_draken(expressions, morsel)` path.
- Implement only the required expression subset first, e.g. bare column references,
  simple arithmetic, and the few functions used by ClickBench group keys and
  aggregate arguments.  Further expressions can be added later; the bridge
  will return an error if it encounters an unsupported expression in strict
  mode.
- Fallback policy:
  - v1 strict mode: fail if expression not supported in Draken path.
  - optional compatibility mode (feature flag): fallback to legacy node.

## 4) Spill Manager

New component:

- `GroupBySpillManager`
- Handles partitioned write/read/merge lifecycle by delegating storage to existing shuffle spill capabilities.

Responsibilities:

- Track memory usage of hash table + state vectors + per-group distinct structures.
- Trigger spill when budget exceeded.
- Write/read partition payloads through existing BinStore/KV scoped paths.
- Coordinate final merge passes.

Memory budget accounting (what is budgeted):

1. Group index structures:
- hash table buckets, occupancy metadata, collision chains/probe metadata.
2. Group key storage:
- fixed-width key vectors and var-width arenas/offset buffers.
3. Aggregate state vectors:
- per-aggregate state arrays (for example sum/count/min/max/seen flags).
4. Distinct state:
- per-group set handles and backing storage.
5. Spill staging buffers:
- serialized payload buffers pending write.
6. Runtime overhead:
- allocator fragmentation/headroom reserve.

---

## Data Flow (Detailed)

### In-Memory Path

For each morsel:

1. Compute group hash vector from key columns (`morsel.hash(columns=...)`).  In practice the planner will resolve and cache the column indices once; the hash call need only be passed a tuple of indices to avoid per‑morsel name lookups.
2. Probe/insert rows into group table:
- Probe by hash bucket.
- For collisions, compare full key values.
3. Update aggregate states for each matched/new group.
4. Periodically check memory budget.

### Spill Path

When memory threshold reached:

1. Partition all current groups by `hash % P` (power‑of‑two recommended).
   *In a pathological case a single partition may still exceed budget; the spill
   manager should detect that and recursively repartition the oversized
   partition (using a new pass id or increased bit‑shift) until all partitions
   fit.*
2. Serialize per‑partition partial state chunks using DRKM payloads.
3. Persist chunks using existing shuffle spill stack (BinStore -> scoped layered KV stores).
4. Flush and clear in-memory store.
5. Continue consuming input.

At EOS:

1. For each partition:
- Load partition stream chunk-wise.
- Rebuild in-memory `GroupStateStore`.
- Merge partial states.
- If partition still exceeds budget, recursively repartition.
2. Finalize merged states to output morsels.

---

## Spill File Format (Proposed)

Use existing DRKM morsel payload format and existing manifest/chunk conventions from shuffle spill.

GroupBy-specific requirement:

1. Partial aggregate state must be representable as Draken morsels for DRKM serde.
2. Chunk and manifest lifecycle must use scoped context (`query_id`, `operator_id`).
3. Any gap in existing spill interfaces should be raised as a bug against shared spill capability, not reimplemented locally.

Design choice:

- Do not introduce a second, group-by-specific file format in v1.

---

## COUNT(DISTINCT) Strategy

Exact distinct per group is memory-heavy. Proposed two-tier strategy:

1. In-memory:
- Per-group distinct `FlatHashSet` handle (or pooled segmented set).
2. Spill-aware:
- Serialize distinct state per group as hashed-value segments.
- On merge, union hashed segments.

Optimization hooks:

- Shared memory pool for small sets.
- Threshold-based promotion to externalized distinct segments.

Semantics:

- Existing hash-only distinct behavior is approximate when collisions occur.
- v1 must explicitly choose one mode:
  - preserve current hash-based approximate semantics, or
  - implement exact value-aware distinct state.

---

## Required Draken Changes

## Hash/Map Utilities

1. Add key iteration API for `FlatHashMap` wrapper (currently missing).
2. Add collision-safe multi-key equality helpers (`compare_rows_on_columns`).

## Morsel/Vector APIs

1. Efficient batch `take`/gather guarantees for String/Binary vectors.
2. Typed serialization helpers for key/state write/read (fixed + varwidth).
3. Stable memory accounting hooks:
- `estimated_bytes()` on vectors/store structures.

## Aggregation Kernels

1. New Cython/C++ aggregate state kernel module:
- `group_state_store.pyx`
- `aggregate_kernels.pyx`
2. Merge/finalize APIs usable by both in-memory and spill merge passes.

## Spill I/O

1. Integrate with existing shuffle spill storage stack (BinStore + layered KV stores).
2. Raise bugs for deficiencies in shared spill interfaces instead of introducing separate group-by file spill logic.

---

## Planner / Operator Integration

1. Physical planner selects `DrakenAggregateAndGroupNode` when feature flag enabled, following the existing physical-planner operator decisioning patterns.
2. Legacy nodes remain as fallback path.
3. Suggested feature flags:
- `FEATURE_DRAKEN_GROUPBY_V2`  (enables the new operator path)
- `FEATURE_DRAKEN_GROUPBY_SPILL`  (only has effect when V2 is enabled)
- `FEATURE_DRAKEN_GROUPBY_STRICT_EXPRESSIONS`  (treat unsupported expressions
   as hard errors)

---

## Telemetry and Diagnostics

Add operator readings:

- `groupby_rows_processed`  *(incremented in the probe/insert loop)*
- `groupby_groups_created`
- `groupby_hash_collisions`
- `groupby_spill_count`
- `groupby_spill_bytes_written`
- `groupby_spill_bytes_read`
- `groupby_repartition_passes`
- `groupby_distinct_groups_externalized`
- time buckets:
  - `time_groupby_probe_insert`
  - `time_groupby_agg_update`
  - `time_groupby_spill_write`
  - `time_groupby_spill_merge`

---

## Phased Implementation Plan

### Phase 1: Test Harness and Baselines (No Engine Wiring)

- Add golden correctness tests for grouped aggregates versus legacy behavior.
- Add targeted performance harness for ClickBench group-by shapes.
- Add stress tests for high-cardinality and null-heavy keys.
- Keep all usage in standalone/post-shuffle paths only.

### Phase 2: Core In-Memory GroupBy Kernel

- Implement `group_state_store.pyx` for probe/insert, collision handling, and state indexing.
- Implement `aggregate_kernels.pyx` for `COUNT/SUM/MIN/MAX/AVG/ANY_VALUE`.
- Add `ShuffleGroupByOperationV2` wrapper to call compiled kernels.
- Support strict Draken expression subset needed for ClickBench.
- Acceptance gate: correctness parity on supported functions with no Arrow/NumPy in hot loop.

### Phase 3: Spill and Merge of Partial Aggregate State

- Implement `GroupBySpillManager` using existing shuffle spill stack (BinStore + layered KV).
- Spill partial aggregate state chunks as DRKM payloads.
- Implement replay + merge and recursive repartition for oversized partitions.
- Enforce hard-fail behavior on spill errors.
- Acceptance gate: bounded-memory execution on large-cardinality workloads.

### Phase 4: Distinct Path and Distribution-Friendly State

- Implement per-group distinct hash state and merge behavior.
- Keep existing hash-based 64-bit DISTINCT semantics in v1.
- Ensure serialized distinct state can be fanned out/merged in future distributed steps.
- Acceptance gate: stable distinct behavior across spill/replay cycles.

### Phase 5: Hardening, Telemetry, and Tuning

- Complete telemetry counters and timing buckets listed in this document.
- Tune probe strategy, growth/load factors, and spill thresholds with benchmark feedback.
- Validate behavior on SQL battery + ClickBench + pathological synthetic datasets.
- Acceptance gate: measurable performance improvement over current shuffle group-by path.

### Phase 6: Main Engine Wiring (Last Phase)

- Wire `DrakenAggregateAndGroupNode` into physical planner decisioning.
- Gate with existing feature flags for A/B testing.
- Keep legacy and v2 dual-path only for validation window.
- Execute cut-over plan to make v2 default after acceptance gates pass.
- Acceptance gate: engine-integrated correctness/performance sign-off and hard cut-over readiness.

---

## Current Status

### Phase 1 Status: In Progress (Substantial Completion)

Implemented in repo:

1. Expanded unit-level correctness/stress coverage for shuffle group-by:
- `tests/unit/operators/test_shuffle_group_by_phase1.py`
- Coverage includes:
  - multi-key/multi-aggregate golden checks
  - chunking invariance checks
  - high-cardinality stress
  - null-heavy stress
  - legacy aggregate-spec compatibility path
2. Added standalone baseline benchmark harness (no planner wiring):
- `tests/performance/benchmarks/bench_clickbench_shuffle_groupby_phase1_baselines.py`
3. Added benchmark compare path for SQL vs shuffle group-by variants:
- `tests/performance/benchmarks/bench_clickbench_shuffle_groupby_compare.py`
- Tracks legacy SQL, shuffle V1, and shuffle V2 timings and stage breakdowns.
4. Added integration-level SQL-vs-shuffle golden pack:
- `tests/integration/test_shuffle_groupby_golden.py`
- Compares fixed query-shape outputs between SQL engine and shuffle post-group-by path.

Validated:

1. New and existing shuffle unit tests pass.
2. Integration golden pack passes, including nullable numeric aggregate semantics parity.

Phase 1 acceptance gate interpretation:

1. Test harness and baseline scaffolding is in place.
2. Correctness coverage is significantly expanded.
3. Remaining Phase 1 work is optional polish (more shapes/fixtures), not blocker-level for entering Phase 2.

### Phase 2 Status: Completed

Implemented in repo:

1. New compiled group-state kernel:
- `opteryx/compiled/aggregations/group_state_store.pyx`
2. New compiled aggregate kernel helpers:
- `opteryx/compiled/aggregations/aggregate_kernels.pyx`
3. New Python glue/operator surface for V2:
- `opteryx/operators/group_state_store.py` (`ShuffleGroupByOperationV2`)
4. Build wiring added:
- `setup.py` now includes `opteryx.compiled.aggregations.group_state_store`
- `setup.py` now includes `opteryx.compiled.aggregations.aggregate_kernels`

Current behavior:

1. `ShuffleGroupByOperationV2` requires compiled backend and fails fast if extension is unavailable.
2. Group-state updates/finalize dispatch through precomputed aggregate function codes in Cython.
3. Single-aggregate fast paths are implemented for common functions (`COUNT(*)`, `COUNT(col)`, `SUM`, `MIN`, `MAX`, `AVG`, `COUNT_DISTINCT`) to avoid generic list-state overhead.
4. Typed int64-key fast paths are implemented for `COUNT(*)`, `COUNT(col)`, `SUM`, `MIN`, `MAX`, `AVG`, and `HASH_ONE`.
5. Nullable float semantics are now preserved through shuffle partitioning (null bitmap preserved in `Float64Vector.take`), removing the previous nullable aggregate mismatch in post-shuffle group-by tests.
6. Existing Phase 1/2 tests exercise the V2 semantics path and are passing.

---

## Risks

1. Distinct state memory blow-up per group.
2. Hash collision overhead if key equality checks are expensive.
3. Expression support gap delaying strict Draken-only pipeline.
4. Spill merge recursion on pathological high-cardinality partitions.
5. String/Binary gather stability issues in existing vector kernels.

---

## Open Questions / Unknowns

1. Memory budget source:
- Should group-by use a fixed budget from config/env, or derive from query/session limits?
- This budget applies to the runtime aggregate state as it is built (group index + key storage + aggregate state + distinct state + spill staging buffers).

---

## Decisions Captured

---

## Next Steps / TODOs

The work will be tracked directly in this document rather than via
external issue trackers. Below are the immediate action items to move from
Phase 2 completion into Phase 3.

* **Baseline harness polish** – completed:
  unit/integration baseline coverage is in place and nullable semantics
  parity test now passes without `xfail`.
* **Prototype kernels** – completed for Phase 2 scope:
  `group_state_store.pyx` and `aggregate_kernels.pyx` are in active use for
  standalone shuffle group-by.
* **ShuffleGroupByOperationV2 wrapper** – completed:
  `ShuffleGroupByOperationV2` exists, requires compiled backend, and is used
  by current benchmarks/golden tests.
* **Expression bridge stub** – implement a minimal
  `evaluate_and_append_draken` that handles the small set of expressions
  used by ClickBench queries and returns an error for anything else.
* **Memory budget decision** – choose whether the budget is derived from
  configuration, session limits, or a hard-coded constant and document the
  chosen source here.
* **Spill manager sketch** – draft the skeleton of `GroupBySpillManager` in
  Python/Cython, reusing the shuffle spill interfaces; identify any
  missing APIs and log them in the doc.
* **Telemetry wiring** – map each of the listed readings to specific loops
  in the code so that implementers know where to increment them.

These items correspond to the Phase 1 and early Phase 2 tasks outlined
previously.

## Decisions Captured

1. Spill path reuse:
- Group-by spill must use existing shuffle spill/BinStore/KV infrastructure.
- Deficiencies should be raised as bugs in shared spill capability.

2. Spill failure policy:
- Hard fail on spill failure; do not fallback to legacy Arrow path.

3. Expression scope:
- v1 may support clickbench-relevant expression subset first.
- Planner/operator selection should follow existing physical planner decision patterns.

4. Distinct distribution model:
- Distinct state is per-group and must support multi-machine/distributed execution steps.

5. Compatibility window:
- Keep both legacy and Draken group-by paths only for A/B testing.
- Target hard cut-over after validation; no long-term dual-path commitment.

6. Output boundary:
- Keep morsels Draken-native through to `ExitNode`.
- Convert to Arrow only at API boundary as needed.

7. Distinct semantics mode:
- Keep existing hash-based (64-bit) distinct semantics in v1.
- This is approximate under collisions and is an accepted v1 tradeoff.

8. Distinct state structure:
- Use per-group distinct hash structures.
- This supports future fan-out/distributed work partitioning.

---
