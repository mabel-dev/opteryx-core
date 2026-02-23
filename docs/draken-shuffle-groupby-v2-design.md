# Draken Shuffle GroupBy V2 Design

## Context

The current shuffle group-by prototype in `opteryx/operators/shuffle/group_by.py` is Python-heavy in the hot path (`to_pylist`, Python tuple keys, Python dict/set state updates). This design replaces it with a Draken-native, compiled implementation.

Scope:

1. Replace post-shuffle group-by + aggregate execution path.
2. Keep execution Draken-native end-to-end in hot loops.
3. Reuse existing compiled/hash/serde/KV infrastructure already in the repo.

Non-goals:

1. Planner wiring in this phase.
2. Arrow/Numpy fallback path in hot aggregation loops.

---

## Goals

1. No `to_pylist()` in group-by update path.
2. No Python per-row key/state mutation in update path.
3. Streaming ingestion of morsels (no full concat).
4. Memory-bounded operation with spill of partial aggregate state.
5. Correct SQL semantics for supported aggregates.

---

## Existing Components To Reuse

1. Row hashing: `third_party/mabel/draken/morsels/morsel.pyx` (`Morsel.hash`).
2. Fast index partitioning: `opteryx/compiled/structures/shuffle_partition.pyx` (`row_indexes_by_bin_flat`).
3. Hash sets/maps: `opteryx/third_party/abseil/containers.pyx` (`FlatHashSet` and map wrappers).
4. Index buffers: `opteryx/compiled/structures/buffers.pyx` (`Int32Buffer`, `IntBuffer`).
5. Spill serde: `third_party/mabel/draken/storage/morsel_io.pyx` (`write_morsel`, `read_morsel`).
6. Tiered spill routing: `opteryx/managers/kvstores/*` (layered + scoped stores).

---

## High-Level Architecture

New operation/kernel set:

1. `ShuffleGroupByOperationV2` (Python thin wrapper).
2. `groupby_state.pyx` (compiled hash table + key store + state arrays).
3. `groupby_kernels.pyx` (compiled update/merge/finalize kernels).
4. `groupby_distinct.pyx` (compiled distinct state handling).

Execution model:

1. Ensure input is Draken morsel on entry.
2. Resolve group and aggregate vectors once per morsel.
3. Compute row hashes once per morsel.
4. Probe/insert groups in compiled state table.
5. Update aggregate state with typed compiled kernels.
6. Spill partial state when memory budget is hit.
7. Merge spilled state at finalize and emit Draken morsel.

---

## State Model

### Group Index

1. Open-addressing hash table: `hash -> group_id`.
2. Collision-safe equality checks against stored typed keys.
3. Load factor based resize policy.

### Key Storage

1. One key row per group in typed storage.
2. Fixed-width keys inline.
3. Variable-width keys via arena/offsets.

### Aggregate State Storage (SoA)

1. `COUNT(*)`: int64 counter.
2. `COUNT(col)`: int64 counter with null checks.
3. `SUM`: typed accumulator.
4. `MIN/MAX`: value + seen flag.
5. `AVG`: `(sum, count)`.
6. `ANY_VALUE`/`HASH_ONE`: first non-null + seen flag.
7. `COUNT_DISTINCT`: per-group distinct handle.

---

## Update Path (Per Morsel)

1. Resolve required vectors once.
2. Compute hash vector once (`morsel.hash(columns=...)`).
3. Optional micro-bucket processing for cache locality using bit-mask partitioning.
4. For each row:
   1. Probe/insert group id.
   2. Collision-check keys in compiled comparator.
   3. Apply aggregate opcode updates.

Hot path constraints:

1. No Python row loops.
2. No Python tuple materialization for keys.
3. No Python sets for distinct.

---

## Merge/Finalize Path

1. Merge partial states using `merge_state(dst, src)` kernels.
2. Finalize with typed kernels (for example `AVG = sum/count`).
3. Build output vectors directly in Draken.
4. Return `Morsel.from_vectors(...)`.

No intermediate Arrow table construction in the aggregation kernel path.

---

## Spill Strategy

Spill unit: partial aggregate state chunks (not raw input rows).

1. Serialize state chunks via DRKM (`write_morsel`).
2. Store through layered/scoped KVStore (`query_id`, `operator_id` required).
3. Replay state chunks via `read_morsel`.
4. Merge replayed state with in-memory state using the same merge kernels.

Benefits:

1. Lower spill volume than row spill for high-reduction group-bys.
2. Fits current memory -> remote layered KV behavior.
3. Keeps replay path Draken-native.

---

## Aggregate Semantics (Supported v2)

1. `COUNT(*)`
2. `COUNT(col)`
3. `SUM`
4. `MIN`
5. `MAX`
6. `AVG`/`MEAN`
7. `ANY_VALUE`/`HASH_ONE`
8. `COUNT_DISTINCT` (exact)

Out of scope initially:

1. `ARRAY_AGG` with order/limit.
2. Approximate distinct modes.

---

## Performance Rationale

Main bottlenecks removed from current implementation:

1. `morsel.column(...).to_pylist()`.
2. Python tuple key creation per row.
3. Python dict/set mutations per row.
4. Python-side finalize list construction in hot paths.

Expected outcome:

1. Major CPU reduction in group/update loops.
2. Better cache locality from typed state arrays.
3. Improved scalability at high cardinality with state-spill merges.

---

## Open Design Decisions

1. `COUNT_DISTINCT` variable-width value representation (hash-only vs value-aware).
2. Integer `SUM` overflow policy (saturate/widen/error).
3. Hash table resize/load-factor defaults.
4. Output group order contract (hash/insertion/unspecified).
5. Initial micro-bucket sizing heuristic for cache-local updates.

---

## Suggested Delivery Plan

1. Build `groupby_state.pyx` for key probe/insert and typed state arrays.
2. Add core kernels for `COUNT/SUM/MIN/MAX/AVG/HASH_ONE`.
3. Replace current `ShuffleGroupByOperation` implementation with v2 wrapper.
4. Add exact `COUNT_DISTINCT` compiled path.
5. Add spill/merge path for partial states.
6. Benchmark against current shuffle group-by benchmark on `scratch.hits`.

