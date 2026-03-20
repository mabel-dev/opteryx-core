# Correctness Action List

## Status Overview
- **Goal**: Return to correctness baseline before performance tuning
- **Minimum Bar**: `make t` and `make clickbench` must pass
- **Secondary**: `make test` (full suite)
- **Last Updated**: 2026-03-20 (live verification)

### Live execution status (2026-03-20)
- `make t`: running output shows 1686 ✅, 90 ❌ (approx 88 failing SQL-cases, plus some unsupported features intentionally exposed)
- `make clickbench`: 42/42 queries pass (100%)
- Root currently failing buckets:
  - TRIM(LEADING/TRAILING/"pattern") `FunctionExecutionError`
  - JSON access `->` on casted VARCHAR (NotImplementedError)
  - MATCH() AGAINST() full-text mismatches (row count mismatch)
  - GENERATE_SERIES path yields UnsupportedSyntaxError in many forms
  - TIME_BUCKET with date intervals yields IncompatibleTypesError
  - EXTRACT(ISOYEAR|WEEK|ISOWEEK|millisecond|nanosecond|DECADE|CENTURY|EPOCH|JULIAN|DOW) entry error
  - CONCAT/CONCAT_WS over arrays and string concat path pyarrow errors
  - ANY_VALUE grouped semantic row count mismatch
  - date/COALESCE/interval predicate row count discrepancies

> [!Note]
> This update is based on the latest local gate executions and replaces the previous 2026-03-15 snapshot.

> [!Note]
> The goal is not fix at the cost of architectural principles - we do not fix through poor programming practices or changes which violate the design goals of the system.

---

## Test Results Summary

### Current Test Status (verified locally on 2026-03-15)
```
make t (file-level gate):
- test_shapes_basic.py:                  ✅ PASSING
- test_shapes_data_sources.py:           ✅ PASSING
- test_shapes_operators_expressions.py:  ❌ FAILED
- test_shapes_aliases_distinct.py:       ❌ FAILED
- test_shapes_functions_aggregates.py:   ❌ FAILED
- test_shapes_joins_subqueries.py:       ❌ FAILED
- test_shapes_edge_cases.py:             ❌ FAILED

Statement-level battery inventory (same SQL battery, executed per statement):
- test_shapes_basic.py:                  89 passed / 0 failed
- test_shapes_data_sources.py:           pytest collection error only (`opteryx_catalog` import)
- test_shapes_operators_expressions.py:  536 passed / 14 failed
- test_shapes_aliases_distinct.py:       526 passed / 41 failed
- test_shapes_functions_aggregates.py:   180 passed / 4 failed
- test_shapes_joins_subqueries.py:       175 passed / 5 failed
- test_shapes_edge_cases.py:             289 passed / 24 failed

Total executable statement cases: 1795 passed / 88 failed
Additional pytest-only collection issue: 1 (`test_shapes_data_sources.py`)

Current top root causes:
- wrong row counts / wrong semantics still dominate, especially `HAVING`, joins, and grouped filters
- Draken grouped routing and grouped fallback selection remain a major bucket
- IFNULL / COALESCE scalar handling still breaks several filter paths
- grouped Draken routing still rejects or misroutes several aggregate shapes
- scalar grouped-function parameters still hit `invalid index to scalar variable` (`ROUND`)
- a smaller array/list bucket remains, now mostly `SORT(ARRAY_AGG(...))` schema/cast typing
- DictionaryVector decode/coercion issues still affect nested CAST/compare paths
- binary/string kernel mismatches remain in concat/trim-style operations

clickbench:
- 42/42 queries passing ✅
```

> [!Note]
> Session 8 added planner-level rejection for non-`INTERVAL` typed prefix literals and refreshed the affected SQL battery fixtures. The broad battery counts above have not been rerun since that deprecation-only change; only targeted verification was performed for that work.

> [!Note]
> Session 9 removed physical-planner fallback to the legacy aggregate/group operators. Aggregate planning is now Draken-only: unsupported aggregate shapes fail fast with `UnsupportedSyntaxError` instead of silently routing through Python/Arrow implementations. The broad battery counts above predate that architectural cutover; a quick statement-level battery rerun confirmed newly exposed grouped failures in `HAVING`, grouped `ROUND`/`CASE`, and unsupported grouped aggregate shapes.

> [!Note]
> Session 9 also removed physical-planner fallback to the legacy inner join operator. Inner join planning is now Draken-only: unsupported join shapes fail fast with `UnsupportedSyntaxError` instead of routing through the Arrow join implementation.

> [!Note]
> There are two different numbers in play:
> - `make t` is only a file-level gate and currently reports 5 failing battery files.
> - the detailed inventory below is based on executing every SQL battery statement directly, which is where the current `88` failing cases come from.

---

## ✅ Completed Work

### Legacy aggregate planner path removed
**Files Modified**:
- `opteryx/planner/physical_planner.py`
- `opteryx/operators/aggregate_helpers.py`
- `opteryx/operators/__init__.py`
- `opteryx/operators/draken_aggregate_node.py`
- `opteryx/operators/draken_aggregate_and_group_node.py`
- `opteryx/config.py`
- `tests/unit/planner/test_physical_planner_draken_agg_flag.py`
- `tests/unit/operators/test_count_star_filtered_projection.py`
- deleted:
  - `opteryx/operators/aggregate_node.py`
  - `opteryx/operators/aggregate_and_group_node.py`
  - `opteryx/operators/simple_aggregate_node.py`
  - `opteryx/operators/simple_aggregate_and_group_node.py`
  - `tests/unit/operators/test_groupby_partial.py`

- Physical planning for `Aggregate` and `AggregateAndGroup` is now single-path: Draken support is required.
- Removed planner fallback to `SimpleAggregateNode`, `SimpleAggregateAndGroupNode`, `AggregateNode`, and `AggregateAndGroupNode`.
- Deleted the legacy aggregate operator implementations entirely; the shared aggregate name map and expression pre-evaluation helper now live in `aggregate_helpers.py`.
- Unsupported aggregate/grouped shapes now raise `UnsupportedSyntaxError("Draken aggregator does not support this query shape")` during planning.
- targeted verification completed:
  - `pytest tests/unit/planner/test_physical_planner_draken_agg_flag.py`
  - `pytest tests/unit/operators/test_count_star_filtered_projection.py`
  - `pytest tests/unit/operators/test_draken_aggregate_and_group_node.py`
  - `python tests/integration/sql_battery/run_shapes_battery.py` (used to surface newly exposed failures; broad inventory above not yet recomputed from a clean rerun)

### Legacy inner join planner path removed
**Files Modified**:
- `opteryx/planner/physical_planner.py`
- `opteryx/operators/__init__.py`
- `opteryx/operators/draken_inner_join_node.py`
- `opteryx/config.py`
- `tests/unit/planner/test_physical_planner_draken_agg_flag.py`
- deleted:
  - `opteryx/operators/inner_join_node.py`

- Physical planning for `inner` joins is now single-path: Draken support is required.
- Removed planner fallback to `InnerJoinNode`.
- Unsupported inner join shapes now raise `UnsupportedSyntaxError("Draken inner join does not support this query shape")` during planning.
- targeted verification completed:
  - `pytest tests/unit/planner/test_physical_planner_draken_agg_flag.py`

### Non-`INTERVAL` typed prefix literals removed
**Files Modified**:
- `opteryx/planner/logical_planner/logical_planner_builders.py`
- `tests/unit/core/test_interval_types.py`
- `tests/integration/sql_battery/test_shapes_aliases_distinct.py`
- `tests/integration/sql_battery/test_shapes_edge_cases.py`
- `tests/integration/sql_battery/test_shapes_operators_expressions.py`
- `tests/integration/sql_battery/test_battery_sql92.py`
- `tests/integration/sql_battery/test_data/tests/documentation.run_tests`
- `tests/integration/sql_battery/test_data/tests/feature_tests.run_tests`
- `tests/integration/sql_battery/test_data/tests/regression.run_tests`
- `tests/integration/sql_battery/test_data/tests/tpch_data.run_tests`
- `tests/integration/sql_battery/test_data/tests/types.run_tests`

- `DATE '...'`, `TIMESTAMP '...'`, `INTEGER '...'`, `DOUBLE '...'`, `DECIMAL '...'`, and `BOOLEAN '...'` now raise `UnsupportedSyntaxError` at planning time.
- `INTERVAL '...'` remains supported.
- SQL battery fixtures were migrated away from prefix literal syntax using existing supported forms (`CAST(...)`, `DATE(...)`, `TIMESTAMP(...)`, or `EXTRACT(...)`) depending on which path currently executes correctly.
- targeted verification completed:
  - `pytest tests/unit/core/test_interval_types.py`
  - representative session queries covering the rewritten battery patterns
  - full battery totals above were not recomputed after this change

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

### ARRAY_AGG in Draken grouped aggregation
**Files Modified**:
- `opteryx/operators/draken_aggregate_and_group_node.py`
- `opteryx/operators/group_state_store.py`
- `opteryx/operators/shuffle/group_by.py`
- `opteryx/compiled/aggregations/array_agg.pyx`
- `opteryx/compiled/aggregations/aggregate_kernels.pyx`
- `opteryx/compiled/aggregations/group_state_store.pyx`
- `setup.py`
- `tests/unit/operators/test_array_agg.py`
- `tests/unit/operators/test_draken_aggregate_and_group_node.py`

- Added `ARRAY_AGG` to the Draken grouped planner support set.
- Implemented grouped `ARRAY_AGG` state with `DISTINCT`, `LIMIT`, and same-expression `ORDER BY`.
- Routed `ARRAY_AGG` queries through the compiled grouped state store backend used by Draken for non-scalar aggregate shapes.
- Verified with targeted unit coverage and the existing `ARRAY_AGG` SQL battery slice.

### Array / list typing fixes
**Files Modified**:
- `opteryx/expression/functions/implementations/text.py`
- `opteryx/expression/functions/implementations/utility.py`
- `opteryx/expression/functions/native_function_registrar.py`
- `opteryx/expression/casts.py`
- `opteryx/expression/evaluator/__init__.py`
- `opteryx/planner/binder/binder.py`
- `opteryx/planner/logical_planner/logical_planner_builders.py`

- `LENGTH(...)` now handles list-like vectors, including `missions` and `ARRAY_AGG(DISTINCT ...)`.
- `ARRAY_CONTAINS`, `ARRAY_CONTAINS_ANY`, and `ARRAY_CONTAINS_ALL` now normalize Arrow and Draken list inputs correctly.
- grouped `name[1]` now routes through the generic `MapAccess` path rather than the list-only fast path.
- `CAST(... AS ARRAY<T>)` now preserves `T` from planning/binding and casts scalar values to singleton arrays instead of attempting JSON parsing.
- Verified improvements:
  - `test_shapes_operators_expressions.py`: `527/23` → `539/11`
  - `test_shapes_aliases_distinct.py`: `515/52` → `519/48`
  - `test_shapes_functions_aggregates.py`: `176/8` → `180/4`
  - `test_shapes_joins_subqueries.py`: `175/5` → `177/3`

### Timestamp / date coercion and comparison fixes
**Files Modified**:
- `opteryx/expression/casts.py`
- `opteryx/expression/__init__.py`
- `opteryx/expression/evaluator/__init__.py`
- `opteryx/expression/ops.py`
- `opteryx/expression/functions/implementations/temporal.py`
- `opteryx/planner/logical_planner/logical_planner_builders.py`

- `CAST(... AS DATE)` and `CAST(... AS TIMESTAMP)` literals now materialize correctly in projection and comparison paths.
- `CAST('2023-01-01' AS TIMESTAMP) = CAST('2023-01-01' AS DATE)` now evaluates correctly.
- bare ISO string bounds now coerce correctly against timestamp/date columns in filters and `BETWEEN`.
- `UNIXTIME(...)` now accepts Arrow temporal arrays, including `date64`.
- fixed representative failures:
  - `SELECT CAST('2023-01-01' AS DATE) AS d FROM $planets`
  - `SELECT * FROM $planets WHERE CAST('2023-01-01' AS TIMESTAMP) = CAST('2023-01-01' AS DATE)`
  - `SELECT Location FROM testdata.missions WHERE Lauched_at BETWEEN '1950-01-01' AND '1975-01-01'`
  - `SELECT name FROM testdata.astronauts WHERE UNIXTIME(birth_date) = UNIXTIME('1961-11-05'::DATE)`
- verified improvements:
  - `test_shapes_edge_cases.py`: `283/30` → `289/24`
  - the old timestamp/date bucket is no longer a top-level blocker; remaining temporal stragglers are now folded into other buckets such as grouped semantics, joins, and date/interval expression rewrites

### STARTS_WITH / ENDS_WITH removed
**Decision**: Not SQL-92 standard; removed entirely rather than maintaining broken rewrite path.
**Files Modified**:
- `opteryx/planner/optimizer/strategies/predicate_rewriter.py` — removed docstring + 2 rewrite blocks
- `opteryx/planner/logical_planner/logical_planner_builders.py` — removed early-rewrite block
- `opteryx/functions/signatures.py` — removed from string functions UI list
- `opteryx/expression/functions/implementations/text.py` — removed `starts_w()` / `ends_w()`
- `tests/integration/sql_battery/test_shapes_aliases_distinct.py` — 4 tests → `UnsupportedSyntaxError`
- `tests/unit/planner/test_optimizations_invoked.py` — 2 rows removed

> **Note on session 3 error inventory**: The session 3 breakdown listed ~18 distinct error categories. These were initially (incorrectly) dismissed as cascades from unsupported aggregates. They are real independent bugs — confirmed by the later statement-level battery inventory after `ARRAY_AGG` and `ANY_VALUE` were fixed.

---

## 🔴 Open Issues

### 1. Wrong row counts / wrong semantics — largest remaining bucket
**Error**: `AssertionError`
**Scope**: still the largest bucket by far. The remaining failures are concentrated in:
- `HAVING` and grouped filtering
- join semantics / null-side filtering
- date and interval predicates
- expression rewrites that change null behavior
- now-visible grouped Draken gaps after legacy planner removal (unsupported grouped shapes, grouped scalar expressions, and grouped alias/HAVING handling)

Representative failures:
```sql
SELECT * FROM testdata.satellites WHERE planetId = id
SELECT planetId, MIN(magnitude) FROM testdata.satellites GROUP BY planetId HAVING MIN(magnitude) > 5
SELECT COUNT(*), VARCHAR(year) FROM testdata.astronauts GROUP BY VARCHAR(year)
SELECT * FROM testdata.missions WHERE Lauched_at < CURRENT_TIMESTAMP + INTERVAL '7' DAY
```

---

### 2. Draken grouped routing / support gaps — ~6 confirmed failures
**Current sub-buckets**:
- `UnsupportedSyntaxError: Carchar group-state engine does not support runtime fallback`
- grouped HAVING / aggregate routing that currently returns partial results
- aggregate shapes that should stay on the legacy backend but currently do not

This is no longer the old `ARRAY_AGG` blocker. The remaining unsupported shapes are current, separate failures.

Representative failures:
```sql
SELECT SUM(id), planetId FROM testdata.satellites GROUP BY planetId
SELECT COUNT(planetId) AS moons, planetId FROM testdata.satellites GROUP BY planetId
SELECT APPROX_PERCENTILE(radius, 0.5) AS am FROM testdata.satellites GROUP BY planetId HAVING am > 5
```

Likely touch points:
- `opteryx/operators/draken_aggregate_and_group_node.py`
- `opteryx/operators/draken_aggregate_node.py`
- carchar planner routing for dict/object aggregates

---

### 3. Array / list typing bugs — 2 confirmed remaining failures
**Status**: this bucket has mostly been closed. The remaining confirmed failures are both schema/cast issues around sorted grouped arrays.

**Current sub-buckets**:
- `2` — `SORT(ARRAY_AGG(...))` materializes `list<item: binary>` but the derived schema still carries a null element type, causing Arrow cast failure

Representative failures:
```sql
SELECT SORT(ARRAY_AGG(name)) AS names FROM testdata.satellites GROUP BY planetId
SELECT SORT(ARRAY_AGG(name LIMIT 5)) AS names FROM testdata.satellites GROUP BY planetId
```

---

### 4. IFNULL / IFNOTNULL scalar binding bugs — 4 confirmed failures
**Error**:
- `FunctionExecutionError: 'str' object has no attribute 'dtype'`
- `FunctionExecutionError: Function 'IFNOTNULL' was not bound`

Representative failures:
```sql
SELECT * FROM (SELECT p.Price AS pri, s.escapeVelocity FROM testdata.missions AS p INNER JOIN $planets AS s ON p.Price = s.escapeVelocity) AS sv WHERE IFNULL(NULL, pri) = pri
SELECT * FROM testdata.astronauts WHERE IFNULL(birth_place['state'], 'home') == 'CA'
```

---

### 5. Scalar indexing bugs in grouped expressions — 2 confirmed failures
**Error**: `IndexError: invalid index to scalar variable`
**Affected functions**:
- `ROUND`

Representative failures:
```sql
SELECT ROUND(magnitude, 1) FROM testdata.satellites GROUP BY ROUND(magnitude, 1)
SELECT COUNT(*), LENGTH(name), ROUND(density, 2) FROM $planets GROUP BY LENGTH(name), ROUND(density, 2)
```

---

### 6. DictionaryVector decode/coercion bugs — 4 confirmed failures
**Error**:
- `int() argument ... not 'DictionaryVector'`
- `must be real number, not DictionaryVector`

Representative failures:
```sql
SELECT TRY_CAST(planetId AS DECIMAL) AS value FROM testdata.satellites
SELECT * FROM testdata.satellites WHERE CAST(CAST(id AS BLOB) AS INTEGER) == id
```

---

### 7. Binary/string kernel mismatch — ~5 confirmed failures
**Current sub-buckets**:
- `3` — `CONCAT` / `||` on binary-typed columns
- `1` — `utf8_ltrim(binary)`
- `1` — `utf8_trim(binary)`

Representative failures:
```sql
SELECT name || ' ' || name FROM $planets
SELECT TRIM(LEADING 'E' FROM name) FROM $planets
SELECT * FROM $planets WHERE TRIM(TRAILING 'arth' FROM name) = 'E'
```

---

### 8. IIF arity bug — 1 confirmed failure
**Error**: `select_values() takes 2 positional arguments but 3 were given`
**Representative failure**:
```sql
WITH categorised AS (
  SELECT id, name, CASE WHEN mass > 1 THEN 'large' ELSE 'small' END AS size FROM $planets
)
SELECT size, COUNT(*) FROM categorised GROUP BY size
```

---

### 9. Long-tail singleton issues
These are current but low-count enough that they are best handled after the large buckets above:
- `ModuleNotFoundError: opteryx_catalog` during direct `pytest` collection of `test_shapes_data_sources.py`
- `RANDOM_STRING()` zero-arg arity
- `StringVector` column-column comparisons not yet supported
- `pyarrow.lib.StringScalar` / `ChunkedArray` type handling gaps
- a few remaining null/dtype edge cases (`NoneType`, scalar-vs-vector function args)

---

## Priority Order

| # | Issue | Count | Effort |
|---|-------|-------|--------|
| 1 | Wrong row counts / wrong semantics | largest bucket | Investigate |
| 2 | Draken grouped routing / support gaps | ~6 | Medium |
| 3 | IFNULL / IFNOTNULL scalar binding | 4 | Small |
| 4 | DictionaryVector decode / coercion | 4 | Small |
| 5 | Array / list typing bugs | 2 | Small |
| 6 | Scalar indexing in grouped ROUND | 2 | Small |
| 7 | Binary/string kernel mismatch | ~5 | Small |
| 8 | IIF arity | 1 | Small |
| 9 | Long-tail singleton issues | ~20 | Mixed |

---

## Testing Commands

```bash
# File-level SQL battery gate
make t

# Statement-level inventory across the currently failing battery files
python -m pytest \
  tests/integration/sql_battery/test_shapes_operators_expressions.py \
  tests/integration/sql_battery/test_shapes_aliases_distinct.py \
  tests/integration/sql_battery/test_shapes_functions_aggregates.py \
  tests/integration/sql_battery/test_shapes_joins_subqueries.py \
  tests/integration/sql_battery/test_shapes_edge_cases.py \
  --tb=line -q

# Clickbench regression check
python tests/performance/clickbench/clickbench.py
```

---

## Key Files
- `opteryx/expression/evaluator/__init__.py` — comparison dispatchers, timestamp coercion, draken compare paths
- `opteryx/expression/ops.py` — constant-folding and filter comparison kernels
- `opteryx/expression/casts.py` — CAST / TRY_CAST timestamp and decimal paths
- `opteryx/expression/__init__.py` — literal materialization and Arrow/Draken append paths
- `opteryx/operators/draken_aggregate_and_group_node.py` — grouped aggregate support and routing
- `opteryx/operators/draken_aggregate_node.py` — aggregate-only support checks
- `opteryx/expression/functions/implementations/` — IFNULL, IFNOTNULL, LENGTH, ARRAY_* and text functions
- `opteryx/expression/functions/implementations/temporal.py` — `UNIXTIME`, temporal scalar handling
- `opteryx/draken/interop/arrow.pyx` — Arrow↔Draken conversions for list/date/timestamp values
