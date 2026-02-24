# Draken-Native Filter Operators Design

## Context

`FilterNode` is still Arrow/NumPy-centric in the hot path, even when upstream operators can produce Draken morsels.

Current behavior:

1. `FilterNode.execute` converts input to Arrow immediately via `ensure_arrow_table` and applies `Table.filter(...)`.
2. Expression evaluation (`opteryx/managers/expression/__init__.py`) returns NumPy/PyArrow arrays and uses Arrow compute for boolean logic.
3. Filter operator kernels in `opteryx/managers/expression/ops.py` rely heavily on `pyarrow.compute` and NumPy conversion.
4. Many `list_ops` kernels are Arrow-buffer or NumPy-object based, not Draken-vector based.

This creates conversion barriers in the filter step and prevents fully Draken-native execution.

## Goal

For the filter step only:

1. Accept Draken morsels directly.
2. Evaluate predicates against Draken vectors.
3. Apply row filtering on Draken morsels without Arrow/NumPy conversions.
4. Preserve SQL null/three-valued logic and current semantics.

Non-goal for this change: full Arrow/NumPy removal from unrelated operators.

## Target End State

1. Filter operators run on `Morsel` + `Vector` types only.
2. Filter mask operations are Draken-native (bitmask/vector), including `AND`/`OR`/`NOT`/`XOR`.
3. Row selection is performed with a Draken-native mask-to-indices path and `Morsel.take(...)` (or new `Morsel.filter_mask(...)`).
4. Legacy Arrow path remains available as fallback during rollout.

## Required Changes

## 1) Operator Module Changes

Primary files:

- `opteryx/operators/filter_node.py`
- `opteryx/planner/physical_planner.py` (if adding dedicated node selection)

Required updates:

1. Add a Draken execution path in `FilterNode.execute`:
- Do not call `ensure_arrow_table` for Draken inputs.
- Evaluate predicate with Draken evaluator.
- Apply mask to morsel directly.

2. Keep compatibility mode:
- If Draken evaluator does not support an expression shape, fallback to current Arrow path under a feature flag.

3. Optional structure:
- Either extend `FilterNode` with dual-path logic, or add `DrakenFilterNode` and choose it in the physical planner.

## 2) Expression Evaluation Engine Changes

Primary files:

- `opteryx/managers/expression/__init__.py`
- `opteryx/managers/expression/ops.py`
- `opteryx/managers/expression/unary_operations.py`
- `opteryx/managers/expression/binary_operators.py` (for filter-side dependencies)

Required updates:

1. Add Draken-native evaluator entry points:
- `evaluate_draken(expression, morsel)`
- `evaluate_and_append_draken(expressions, morsel)` (for filter function pre-evaluation)

2. Add vector-oriented dispatch:
- Resolve identifiers to `morsel.column(<bytes>)` vectors.
- Avoid `table[col].to_numpy(...)` and `pyarrow.array(...)`.

3. Replace Arrow boolean logic in filter path:
- Current `LOGICAL_OPERATIONS` uses `pyarrow.compute.and_/or_/xor`.
- Draken path should use mask kernels (or `BoolVector` methods) with SQL null semantics.

4. Remove NumPy-based short-circuiting in Draken path:
- Current `short_cut_and`/`short_cut_or` uses NumPy masks + Arrow `take`.
- Draken path should short-circuit using Draken masks and morsel slicing/take.

5. Unary operators:
- Replace NumPy/Arrow null checks and boolean checks with Draken vector methods/null bitmaps.

## 3) Filter Operator Kernel Rewrite

Primary file:

- `opteryx/managers/expression/ops.py`

This module currently mixes:

1. `pyarrow.compute` comparisons.
2. NumPy mask materialization and inversion.
3. Arrow array conversions for list/string operators.

Required rewrite:

1. Add Draken filter kernel dispatcher keyed by operator + vector type.
2. Use existing Draken vector comparisons where available:
- `Int64Vector`, `Float64Vector`, `Date32Vector`, `TimeVector`, `TimestampVector`, `BoolVector`, `IntervalVector`.
3. Add missing Draken kernels where absent (notably string/list/regex cases).
4. Return a Draken mask type instead of NumPy/PyArrow arrays.

## 4) Additional `list_ops` Work

Primary files:

- `opteryx/compiled/list_ops/*.pyx`
- `opteryx/compiled/list_ops/list_ops.pyx`

Current gap:

1. Several kernels are Arrow-dependent (`pyarrow.types`, Arrow buffer reads) or NumPy-object-array dependent.
2. Filter path uses these operators heavily: `InList`, `NotInList`, `InStr`, `Like`, `RLike`, `AnyOp*`, `AllOp*`, `AtArrow`, `ArrayContainsAll`.

Needed Draken-native kernels:

1. String containment/match kernels over `StringVector` (case-sensitive and case-insensitive).
2. LIKE/ILIKE/regex kernels over `StringVector`.
3. Array/list membership kernels over `ArrayVector` child buffers:
- `ANY`/`ALL` comparisons.
- `@>` / `@>>` style contains-any/contains-all behavior.
4. JSON-related kernels (`AtQuestion` / `AtArrow`) that do not require Arrow conversion.
5. Mask post-processing helpers:
- invert/not
- optional mask-to-indices helper for fast row extraction.

## 5) Draken Mask + Morsel Selection Primitives

Primary files:

- `third_party/mabel/draken/vectors/bool_vector.pyx`
- `third_party/mabel/draken/morsels/morsel.pyx`
- optional: `third_party/mabel/draken/compiled/maskops.pyx`

Needed capabilities:

1. Canonical filter mask representation for Draken path.
2. Clear null semantics representation (validity bitmap plus value bits, or equivalent).
3. Fast row selection API from mask:
- either `Morsel.filter_mask(mask)`
- or `mask_to_indices(mask)` + `Morsel.take(indices)`.

Notes:

1. `BoolVector` logical methods currently treat nulls as false in places; filter semantics must match SQL 3-valued logic.
2. `Morsel.take(...)` exists, but filter path needs a fast and safe mask-to-index bridge.

## 6) Type Coercion + Null Semantics Parity

Parity-critical behaviors from current path must be retained:

1. Decimal/integer coercions before comparison.
2. Date/timestamp integer coercion behavior.
3. Interval comparison behavior.
4. Null compression/expansion behavior in comparisons.
5. SQL Kleene logic for boolean composition and filter null handling.

This is the highest-risk area for silent behavioral regressions.

## 7) Planner + Feature Flags

Recommended rollout guard:

1. Add `FEATURE_USE_DRAKEN_FILTER` in `opteryx/config.py`.
2. Enable Draken filter path only when flag is on.
3. Keep hard fallback to legacy Arrow path until parity test gates are met.

## Implementation Phases

## Phase 1: Plumbing and Safe Dual Path

1. Add Draken filter execution path and flag.
2. Add evaluator entry points (`evaluate_draken`, `evaluate_and_append_draken`) with minimal operator subset.
3. Fallback unsupported expressions/operators to legacy path.

## Phase 2: Operator Coverage Expansion

1. Port all comparison/unary/filter operators used in WHERE/HAVING.
2. Add missing `list_ops` Draken kernels for array/string operators.
3. Add strict null/typing parity tests.

## Phase 3: Performance and Cleanup

1. Optimize mask operations and mask-to-indices conversion.
2. Remove avoidable Arrow/NumPy imports from Draken filter path.
3. Add telemetry counters for conversion fallbacks and ensure they trend to zero.

## Test Plan

1. Unit tests:
- `Eq`, `NotEq`, `Lt`, `Gt`, `LtEq`, `GtEq`
- `IN`/`NOT IN`
- `LIKE`/`ILIKE`/regex
- `ANY`/`ALL` array operators
- unary `IS NULL`, `IS TRUE`, etc.
- null and mixed-type coercion edge cases

2. Integration tests:
- SQL battery operator/expression suites
- row visibility filters (`AnyOp*`, `InList`) parity

3. Performance tests:
- update/extend `tests/performance/benchmarks/bench_filter_optimization.py`
- add Draken morsel benchmark to validate conversion removal

## Acceptance Criteria

1. Filter step processes Draken morsels without Arrow/NumPy conversions in the enabled path.
2. SQL result parity against legacy path for operator-expression battery.
3. No regression in null semantics.
4. Performance improves or is neutral on representative filter-heavy workloads.

## Important Existing Gaps to Resolve

1. `opteryx/draken/__init__.py` references `opteryx.draken.evaluators.evaluator`, but that module is not present in this tree.
2. Several tests/benchmarks in `tests/draken/*` assume evaluator APIs that are currently incomplete.

Decide whether to complete and reuse that evaluator path, or implement Draken evaluator logic directly under `opteryx/managers/expression`.
