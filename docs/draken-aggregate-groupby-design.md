# Draken-Native Aggregate + Group By Design

## Context

Current `AggregateAndGroupNode` and `SimpleAggregateAndGroupNode` are still primarily Arrow-based and rely on NumPy/Arrow operations in hot paths. This design proposes a full Draken-native aggregate/group-by execution path:

- Convert to Draken on operator entry.
- No NumPy in aggregation/grouping kernels.
- No Arrow in aggregation/grouping kernels.
- Optional spill-to-disk for high-cardinality / large state.

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

- Must not trust 64-bit hash alone (collision-safe equality check required).
- Store group key fingerprint + row-id/key payload for exact compare.
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
- Implement only required expression subset first (identifiers, literals, simple funcs used in clickbench group keys/agg args).
- Fallback policy:
  - v1 strict mode: fail if expression not supported in Draken path.
  - optional compatibility mode (feature flag): fallback to legacy node.

## 4) Spill Manager

New component:

- `GroupBySpillManager`
- Handles partitioned write/read/merge lifecycle.

Responsibilities:

- Track memory usage of hash table + state vectors + per-group distinct structures.
- Trigger spill when budget exceeded.
- Write partition files with deterministic format.
- Coordinate final merge passes.

---

## Data Flow (Detailed)

### In-Memory Path

For each morsel:

1. Compute group hash vector from key columns (`morsel.hash(columns=...)`).
2. Probe/insert rows into group table:
- Probe by hash bucket.
- For collisions, compare full key values.
3. Update aggregate states for each matched/new group.
4. Periodically check memory budget.

### Spill Path

When memory threshold reached:

1. Partition all current groups by `hash % P` (power-of-two recommended).
2. Serialize per-partition records:
- Group key payload (typed)
- Aggregate partial state payload
3. Flush and clear in-memory store.
4. Continue consuming input.

At EOS:

1. For each partition:
- Load partition stream chunk-wise.
- Rebuild in-memory `GroupStateStore`.
- Merge partial states.
- If partition still exceeds budget, recursively repartition.
2. Finalize merged states to output morsels.

---

## Spill File Format (Proposed)

Binary row-oriented records per partition, append-only:

- Header:
  - magic/version
  - query_id/operator_id
  - schema descriptors (group key types, aggregate state types)
- Repeated records:
  - key hash (uint64)
  - key fields (typed encoding; varlen prefixed)
  - aggregate state blob(s)
- Footer (optional):
  - counts/checksum

Design choice:

- Keep format simple and self-describing for first iteration.
- Compression optional (off by default in v1).

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

- Maintain exact semantics in v1 (no approximate mode by default).

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

1. New temp-file manager for query-scoped spill files.
2. Partition writer/reader with streaming iterator API.

---

## Planner / Operator Integration

1. Physical planner selects `DrakenAggregateAndGroupNode` when feature flag enabled.
2. Legacy nodes remain as fallback path.
3. Suggested feature flags:
- `FEATURE_DRAKEN_GROUPBY_V2`
- `FEATURE_DRAKEN_GROUPBY_SPILL`
- `FEATURE_DRAKEN_GROUPBY_STRICT_EXPRESSIONS`

---

## Telemetry and Diagnostics

Add operator readings:

- `groupby_rows_processed`
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

## Rollout Plan

### Phase 1: In-Memory Draken GroupBy (No Spill)

- Implement `COUNT/SUM/MIN/MAX/AVG` grouped path.
- Expression subset support required by clickbench.
- Strict correctness tests vs legacy node.

### Phase 2: Spill Infrastructure

- Partition spill files.
- Final merge pass.
- Recursive repartition for oversized partitions.

### Phase 3: DISTINCT and Advanced Aggregates

- Exact `COUNT(DISTINCT)` spill-aware merge.
- `ANY_VALUE`/`ONE`.
- Optional `ARRAY_AGG` follow-up.

### Phase 4: Default Enablement

- A/B compare on ClickBench + SQL battery.
- Enable by default after performance and correctness gates pass.

---

## Risks

1. Distinct state memory blow-up per group.
2. Hash collision overhead if key equality checks are expensive.
3. Expression support gap delaying strict Draken-only pipeline.
4. Spill merge recursion on pathological high-cardinality partitions.
5. String/Binary gather stability issues in existing vector kernels.

---

## Open Questions / Unknowns

1. Distinct semantics mode:
- Do you want exact-only in v1, or permit optional approximate mode for very large groups?

2. Memory budget source:
- Should group-by use a fixed budget from config/env, or derive from query/session limits?

3. Spill location and lifecycle:
- Preferred directory for spill files?
- Must files be encrypted at rest, or is plaintext temp acceptable?

4. Failure policy:
- On spill failure (disk full / permission), should query fail hard or fallback to legacy Arrow node?

5. Expression scope for v1:
- Is it acceptable to support only clickbench-relevant expression forms first, then expand?

6. Distinct data structure:
- Do you prefer per-group hash sets (simple) or pooled/global segmented set handles (more complex, lower overhead)?

7. Compatibility window:
- Should both legacy and Draken group-by paths coexist long-term behind flags, or do you want a hard cutover target?

8. Output boundary:
- Is keeping Draken morsels all the way to `ExitNode` acceptable, with Arrow conversion only at API edge?

---

## Suggested Next Step

After your answers to the open questions above, I can convert this into a concrete implementation plan with:

- file-by-file change list
- kernel interfaces
- phased PR breakdown
- test matrix (correctness + performance + spill stress)
