# Correctness Action List

## Status Overview
- **Goal**: Return to correctness baseline before performance tuning
- **Minimum Bar**: `make t` and `make clickbench` must pass
- **Secondary**: `make test` (full suite)
- **Last Updated**: 2026-03-14 (session 4)

> [!Note]
> The goal is not fix at the cost of architectural principles - we do not fix through poor programming practices or changes which violate the design goals of the system.

---

## Test Results Summary

### Current Test Status (2026-03-14 session 4)
```
make t (SQL Battery Tests):
- test_shapes_basic.py:                  ✅ PASSING
- test_shapes_data_sources.py:           ⚠️  SKIP (missing opteryx_catalog module — env issue)
- test_shapes_operators_expressions.py:  ❌ FAILED
- test_shapes_aliases_distinct.py:       ❌ FAILED
- test_shapes_functions_aggregates.py:   ❌ FAILED
- test_shapes_joins_subqueries.py:       ❌ FAILED
- test_shapes_edge_cases.py:             ❌ FAILED

Total: 12 failures across 5 files (down from 168)

Remaining failures:
  10  UnsupportedSyntaxError: ARRAY_AGG not supported (issues 1a/1d/1f)
   2  UnsupportedSyntaxError: APPROX_PERCENTILE not supported (issue 1c)

make clickbench:
- 42/42 queries passing ✅
```

---

## ✅ Completed Work

### Segmentation Faults — GROUP BY without Aggregates
**Files Modified**: `opteryx/operators/draken_aggregate_and_group_node.py`
- Added implicit COUNT(*) when GROUP BY has no explicit aggregates
- Track with `self._implicit_count_added`, remove column before output
- Fixes 3 previously-segfaulting test files

### NULL Comparison Operators
**Files Modified**: `opteryx/expression/evaluator/__init__.py`
- All comparison dispatchers (`_string_compare`, `_int64_compare`, `_float64_compare`, `_timestamp_compare`, `_date32_compare`, `_interval_compare`, `_dict_compare`, `_constant_compare`) return empty BoolVector when right is None
- SQL three-valued logic now correct

### numpy.datetime64 in `_coerce_timestamp`
**Files Modified**: `opteryx/expression/evaluator/__init__.py`
- Added `isinstance(value, numpy.datetime64)` case before the bare `int(value)` fallthrough

### BoolVector in `draken_compare`
**Files Modified**: `opteryx/expression/evaluator/__init__.py`
- Added `BoolVector` dispatch case using `left.equals(bool(right))`

### CIDR `right[0]` bug in `_ip_containment`
**Files Modified**: `opteryx/expression/binary_operators.py`
- `cidr_str = right if isinstance(right, str) else str(right[0])`

### NULLIF with DictionaryVector
**Files Modified**: `opteryx/expression/functions/implementations/logical.py`
- Added `to_arrow()` conversion for draken vectors; `numpy.isscalar(col2)` shortcut

### Constant bitwise / non-boolean WHERE predicates
**Files Modified**:
- `tests/integration/sql_battery/test_shapes_aliases_distinct.py` — 5 tests → `UnsupportedSyntaxError`
- `tests/integration/sql_battery/test_shapes_operators_expressions.py` — `id ^ 1`, `id & 1`, `id | 1` → `UnsupportedSyntaxError`
- `opteryx/planner/binder/binder_visitor.py` — `visit_filter` validates predicate is BOOLEAN

### ChunkedArray not combined before Morsel.from_arrow
**Files Modified**: `opteryx/operators/filter_node.py`
- `morsel = Morsel.from_arrow(morsel.combine_chunks())` — combines chunks before conversion; zero-cost when already flat
- Fixes LEFT JOIN output producing chunked columns that `vector_from_arrow()` rejects

### NORMAL() 0-arg overload
**Files Modified**: `opteryx/functions/function_signatures.json`, `opteryx/expression/functions/native_function_registrar.py`
- Added `NORMAL_0` overload with `arity {minimum:0, maximum:0}` and `kernel_id="zero_arg"`
- Converted NORMAL entry from `_make()` to full `FunctionDefinition` to hold two overloads
- Matches existing `RANDOM_0` pattern

### POSITION rewritten as Cython vector_ops kernel
**Files Modified**: `opteryx/compiled/vector_ops/vector_position.pyx` (new), `opteryx/compiled/vector_ops/vector_ops.pyx`, `opteryx/expression/functions/native_function_registrar.py`
- New BMH-based kernel operating directly on `StringVector` / `bytes` needle
- Single-byte fast path uses `memchr`; multi-byte uses Boyer-Moore-Horspool skip table
- Returns 1-based `Int64Vector` positions (0 = not found), consistent with SQL POSITION semantics
- Also fixed pre-existing missing `uint16_t`/`uint32_t` import in `vector_match_against.pyx`

### CONCAT / CONCAT_WS rewritten as Cython vector_ops kernel
**Files Modified**: `opteryx/compiled/vector_ops/vector_concat.pyx` (new), `opteryx/compiled/vector_ops/vector_ops.pyx`, `opteryx/expression/functions/native_function_registrar.py`, `opteryx/expression/functions/implementations/text.py`
- `vector_concat_array(ArrayVector)` and `vector_concat_ws_array(bytes, ArrayVector)` operate directly on draken vectors
- Root cause of segfault: `DrakenArrayBuffer.ptr.values` is never populated for arrow-backed `ArrayVector`s — child string data lives in Python-level `_child` attribute (`StringVector`). Fixed by casting `arr._child` to `StringVector` and using its `ptr` directly
- Both SELECT path (numpy.ndarray input → Python wrapper) and WHERE path (pyarrow.ListArray → ArrayVector → Cython kernel) confirmed working
- Removed old Python `concat()` / `concat_ws()` from `text.py`

### ONE alias removed
**Decision**: `ONE` was an opteryx-specific alias for `ANY_VALUE`. Removed in favour of the SQL-standard name.
**Files Modified**:
- `opteryx/operators/draken_aggregate_and_group_node.py` — removed from both frozensets; dispatch collapsed to `value == "ANY_VALUE"`
- `opteryx/operators/aggregate_node.py` — removed `"ONE": "hash_one"` mapping
- `tests/integration/sql_battery/test_shapes_aliases_distinct.py` — `SELECT ONE(name)` test row removed
- `docs/draken-aggregate-groupby-design.md` — removed `/ONE` from function list

### DISTINCT aggregate alias removed
**Decision**: `DISTINCT` as a standalone aggregate name was incorrect — `DISTINCT` is a query modifier, not a function. It was inconsistently mapped (`count_distinct` in Draken path, `distinct` row-dedup kernel in legacy path). Removed; `COUNT_DISTINCT` is the correct name.
**Files Modified**:
- `opteryx/operators/draken_aggregate_and_group_node.py` — removed from both frozensets; dispatch `value in ("DISTINCT", "COUNT_DISTINCT")` → `value == "COUNT_DISTINCT"`
- `opteryx/operators/aggregate_node.py` — removed `"DISTINCT": "distinct"` entry (was marked `# fated`)

### STARTS_WITH / ENDS_WITH removed
**Decision**: Not SQL-92 standard; removed entirely rather than maintaining broken rewrite path.
**Files Modified**:
- `opteryx/planner/optimizer/strategies/predicate_rewriter.py` — removed docstring + 2 rewrite blocks
- `opteryx/planner/logical_planner/logical_planner_builders.py` — removed early-rewrite block
- `opteryx/functions/signatures.py` — removed from string functions UI list
- `opteryx/expression/functions/implementations/text.py` — removed `starts_w()` / `ends_w()`
- `tests/integration/sql_battery/test_shapes_aliases_distinct.py` — 4 tests → `UnsupportedSyntaxError`
- `tests/unit/planner/test_optimizations_invoked.py` — 2 rows removed

> **Note on session 3 error inventory**: The session 3 breakdown listed ~18 distinct error categories (IIF arity, IFNULL scalar, TRIM on binary, TimestampVector in pyarrow compute, etc.). All of those disappeared when `ANY_VALUE`/`ARRAY_AGG` were fixed — without touching any of that code. They were not independent bugs; they were secondary errors thrown by queries whose *primary* failure was an unsupported aggregate. pytest reported the error at the point it was thrown, not the root cause. The session 3 issue list overcounted distinct problems by ~16 categories.

---

## 🔴 Open Issues

### 1a. ARRAY_AGG not in supported aggregates — ~20 failures
**Error**: `UnsupportedSyntaxError: Draken aggregator does not support this query shape`
**Gate**: `aggregate.value not in SUPPORTED_AGGREGATES` — `ARRAY_AGG` is absent from the frozenset in `DrakenAggregateAndGroupNode`
**Location**: `opteryx/operators/draken_aggregate_and_group_node.py` — `SUPPORTED_AGGREGATES` + `supports()`
**Affected queries** (examples):
```sql
SELECT ARRAY_AGG(id) FROM testdata.satellites GROUP BY planetId
SELECT ARRAY_AGG(name), planetId FROM testdata.satellites GROUP BY planetId
SELECT ARRAY_AGG(DISTINCT name) FROM testdata.satellites GROUP BY planetId
SELECT ARRAY_AGG(name ORDER BY name DESC LIMIT 2) FROM testdata.satellites GROUP BY planetId
SELECT ARRAY_AGG(DISTINCT LEFT(name, 1)) FROM testdata.satellites GROUP BY planetId
```
**Fix needed**: Implement `ARRAY_AGG` in the Draken group-state engine and add to `SUPPORTED_AGGREGATES` + `FAST_PATH_AGGREGATES`.

---

### 1b. ANY_VALUE — ✅ COMPLETED (session 4)
**Error was**: `UnsupportedSyntaxError: Carchar group-state engine does not support runtime fallback`
**Root cause chain**:
1. `ANY_VALUE` not in `FAST_PATH_AGGREGATES` → added
2. Carchar dispatch in `_maybe_init_carchar_mode` had no `elif fn == "hash_one":` branch → hit `else: self._init_legacy_backend()` → raises
3. `AGG_HASH_ONE` not defined locally in carchar file → added `cdef int AGG_HASH_ONE = 8`
4. All ingest dispatch gates (`VALUE_OBJECT and self._agg_mode in (AGG_MIN, AGG_MAX)`) needed `AGG_HASH_ONE` added — 5 sites
5. `_agg_output_is_object`, `_build_chunk_morsel` nulls check, finalize paths — 3 more sites
6. `_ingest_object_minmax_for_states` pick-first semantics (store first non-null, skip subsequent) — 1 site
**Files Modified**:
- `opteryx/operators/draken_aggregate_and_group_node.py` — `ANY_VALUE` to `FAST_PATH_AGGREGATES`
- `opteryx/compiled/aggregations/carchar_group_state_engine.pyx` — `AGG_HASH_ONE` constant + ~10 dispatch/ingest/finalize sites
**Result**: 168 → 12 failures (156 failures resolved by this one fix, mostly cascades)

> **Lesson**: "It's just an elif branch" is never just an elif branch in a pipeline processor. The constant must propagate through every dispatch gate, ingest path, and finalize path that already handles the analogous modes. Count the grep hits for the surrounding pattern *before* committing to a scope estimate.

---

### 1c. APPROX_PERCENTILE not supported — ~2 failures
**Error**: `UnsupportedSyntaxError: Draken aggregator does not support this query shape`
**Gate**: `APPROX_PERCENTILE` not in `SUPPORTED_AGGREGATES`
**Affected queries**:
```sql
SELECT APPROX_PERCENTILE(radius, 0.5) AS AM FROM testdata.satellites GROUP BY planetId HAVING AM > 5
SELECT APPROX_PERCENTILE(radius, 0.5) AS AM FROM testdata.satellites GROUP BY planetId HAVING APPROX_PERCENTILE(radius, 0.5) > 5
```
**Fix needed**: Implement `APPROX_PERCENTILE` in the Draken engine, or route to legacy when this function is present.

---

### 1d. Subquery / derived table containing unsupported GROUP BY — ~20 failures
**Error**: `UnsupportedSyntaxError: Draken aggregator does not support this query shape`
**Root Cause**: Inner subquery contains `ARRAY_AGG` GROUP BY (blocked by 1a) — physical planner rejects before the outer query runs. Cascades from 1a.
**Affected queries** (examples):
```sql
SELECT * FROM (SELECT ARRAY_AGG(id) AS pids, planetId FROM testdata.satellites GROUP BY planetId) AS sats
SELECT * FROM $planets INNER JOIN (SELECT ARRAY_AGG(id) AS pids, planetId FROM testdata.satellites GROUP BY planetId) AS sats ON ...
SELECT * FROM $planets LEFT JOIN (SELECT ARRAY_AGG(id) AS pids, planetId FROM testdata.satellites GROUP BY planetId) AS sats ON ...
SELECT * FROM (SELECT ARRAY_AGG(name) AS n FROM testdata.astronauts GROUP BY group) AS alma CROSS JOIN UNNEST(n) AS nn
SELECT * FROM (SELECT ARRAY_AGG(CASE WHEN LENGTH(alma_mater) > 10 THEN name ELSE NULL END) AS arr FROM ...) CROSS JOIN UNNEST(arr)
SELECT * FROM (SELECT LENGTH(ARRAY_AGG(DISTINCT planetId)) AS L FROM testdata.satellites GROUP BY planetId) AS I WHERE L = 1
```
**Fix needed**: Resolves automatically once 1a is fixed.

---

### 1e. Aggregate without parameters — ~3 failures
**Error**: `UnsupportedSyntaxError: Draken aggregator does not support this query shape`
**Gate**: `not aggregate.parameters` — `supports()` rejects aggregates with an empty `parameters` list
**Affected queries** (examples):
```sql
SELECT COUNT(*) FROM testdata.satellites GROUP BY TIMESTAMP('2022-01-0' || VARCHAR(planetId))
SELECT * FROM (SELECT COUNT(planetId) AS moons, planetId FROM testdata.satellites GROUP BY planetId) AS SQ WHERE moons > 10
```
**Fix needed**: Investigate why `parameters` is empty for these plan shapes; likely `COUNT(*)` producing a node with `parameters = []` instead of `[WILDCARD]` in some paths.

---

### 1f. Aggregate-only node rejects unsupported function — ~8 failures
**Error**: `UnsupportedSyntaxError: Draken aggregator does not support this query shape`
**Gate**: `DrakenAggregateNode.supports()` at `opteryx/operators/draken_aggregate_node.py:257` — no-GROUP-BY aggregate path (`LogicalPlanStepType.Aggregate`)
**Affected queries** (examples):
```sql
SELECT CONCAT(ARRAY_AGG(name)) FROM $planets GROUP BY gravity
SELECT CONCAT_WS(', ', ARRAY_AGG(mass)) AS MASSES FROM $planets GROUP BY gravity
SELECT GREATEST(ARRAY_AGG(name)) AS NAMES FROM testdata.satellites GROUP BY planetId
SELECT LEAST(ARRAY_AGG(name)) AS NAMES FROM testdata.satellites GROUP BY planetId
```
**Fix needed**: Resolves naturally once `ARRAY_AGG` is implemented (1a); `CONCAT(ARRAY_AGG(...))` and similar wrapped forms will then plan correctly.

---

## Priority Order

| # | Issue | Count | Effort |
|---|-------|-------|--------|
| 1a | ARRAY_AGG not in supported aggregates | ~10 | Large (engine impl) |
| 1d | Subquery with unsupported GROUP BY (cascades from 1a) | free when 1a done | Free |
| 1f | Aggregate-only node (CONCAT/GREATEST/LEAST of ARRAY_AGG) | free when 1a done | Free |
| 1c | APPROX_PERCENTILE not supported | 2 | Medium |
| 1e | Aggregate without parameters | investigate | Small |
| 1b | ANY_VALUE | ✅ done (session 4) | — |

---

## Testing Commands

```bash
# Run all shape battery files
make t

# Get failure summary by error type
python -m pytest tests/integration/sql_battery/test_shapes_operators_expressions.py tests/integration/sql_battery/test_shapes_aliases_distinct.py tests/integration/sql_battery/test_shapes_functions_aggregates.py tests/integration/sql_battery/test_shapes_joins_subqueries.py tests/integration/sql_battery/test_shapes_edge_cases.py --tb=line -q 2>&1 | grep "^E " | sed 's/^E   //' | sort | uniq -c | sort -rn | head -40

# Performance baseline
make clickbench
```

---

## Key Files
- `opteryx/expression/evaluator/__init__.py` — comparison dispatchers, `_coerce_timestamp`, `draken_compare`
- `opteryx/planner/physical_planner.py` — Draken query shape routing (line ~75)
- `opteryx/operators/draken_aggregate_and_group_node.py` — GROUP BY / aggregate node
- `opteryx/expression/functions/implementations/` — function implementations
- `opteryx/expression/casts.py` — CAST / TRY_CAST logic
- `opteryx/draken/interop/arrow.pyx` — Arrow↔Draken conversions
