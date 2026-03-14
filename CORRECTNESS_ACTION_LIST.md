# Correctness Action List

## Status Overview
- **Goal**: Return to correctness baseline before performance tuning
- **Minimum Bar**: `make t` and `make clickbench` must pass
- **Secondary**: `make test` (full suite)
- **Last Updated**: 2026-03-14 (session 5)

> [!Note]
> The goal is not fix at the cost of architectural principles - we do not fix through poor programming practices or changes which violate the design goals of the system.

---

## Test Results Summary

### Current Test Status (2026-03-14 session 5)
```
make t (SQL Battery Tests):
- test_shapes_basic.py:                  ✅ PASSING
- test_shapes_data_sources.py:           ⚠️  SKIP (missing opteryx_catalog module — env issue)
- test_shapes_operators_expressions.py:  ❌ FAILED
- test_shapes_aliases_distinct.py:       ❌ FAILED
- test_shapes_functions_aggregates.py:   ❌ FAILED
- test_shapes_joins_subqueries.py:       ❌ FAILED
- test_shapes_edge_cases.py:             ❌ FAILED

Total: 119 failures across 5 files

Breakdown by error type:
  38  AssertionError (wrong row counts)
   7  ValueError: Invalid timestamp
   7  TypeError: int() arg ... not 'ArrowVector' (_coerce_timestamp)
   6  TypeError: TimestampVector passed to pyarrow compute
   4  UnsupportedSyntaxError: Draken aggregator (ARRAY_AGG remnants)
   4  ValueError: Buffer has wrong number of dimensions
   4  IndexError: invalid index to scalar variable
   3  ArrowNotImplementedError: binary_join_element_wise (binary+string)
   3  UnsupportedSyntaxError: Carchar engine runtime fallback
   3  FunctionExecutionError: LENGTH got ArrayVector not StringVector
   3  FunctionExecutionError: IFNULL 'str' has no dtype
   3  TypeError: LENGTH got ArrayVector not StringVector
   3  TypeError: ArrayVector got pyarrow.lib.ListArray
   3  AttributeError: 'str' has no dtype
   2  (and ~20 more low-count error types)

make clickbench:
- 42/42 queries passing ✅
```

> [!Note]
> Session 5 update: `ARRAY_AGG` is now implemented in the Draken grouped aggregate path. The failure counts in the session 4 snapshot above are historical and should not be read as current `ARRAY_AGG` status.

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

### STARTS_WITH / ENDS_WITH removed
**Decision**: Not SQL-92 standard; removed entirely rather than maintaining broken rewrite path.
**Files Modified**:
- `opteryx/planner/optimizer/strategies/predicate_rewriter.py` — removed docstring + 2 rewrite blocks
- `opteryx/planner/logical_planner/logical_planner_builders.py` — removed early-rewrite block
- `opteryx/functions/signatures.py` — removed from string functions UI list
- `opteryx/expression/functions/implementations/text.py` — removed `starts_w()` / `ends_w()`
- `tests/integration/sql_battery/test_shapes_aliases_distinct.py` — 4 tests → `UnsupportedSyntaxError`
- `tests/unit/planner/test_optimizations_invoked.py` — 2 rows removed

> **Note on session 3 error inventory**: The session 3 breakdown listed ~18 distinct error categories. These were initially (incorrectly) dismissed as cascades from unsupported aggregates. They are real independent bugs — confirmed by running `make t` with 119 failures after ARRAY_AGG and ANY_VALUE were fixed.

---

## 🔴 Open Issues

### 1a. ARRAY_AGG not in supported aggregates — ✅ COMPLETED (session 5)
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
**Resolution**: Implemented in session 5. This note is retained as historical root cause context.

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

### 2. AssertionError — wrong row counts — 38 failures
**Error**: `AssertionError` (query returns wrong number of rows)
**Root Cause**: Mixed — GROUP BY / HAVING / JOIN evaluation correctness bugs.

---

### 3. ArrowVector in `_coerce_timestamp` — 7 failures
**Error**: `TypeError: int() argument must be a string, a bytes-like object or a real number, not 'ArrowVector'`
**Root Cause**: Date arithmetic produces an `ArrowVector`; `_coerce_timestamp` falls through to bare `int(value)`.
**Fix**: Add `isinstance(value, ArrowVector)` case in `_coerce_timestamp` in `opteryx/expression/evaluator/__init__.py`.

---

### 4. Invalid timestamp (microsecond epoch integer) — 7 failures
**Error**: `ValueError: Invalid timestamp` / `SqlError: Error casting '1577836800000000' to TIMESTAMP`
**Root Cause**: orso's `parse_timestamp` rejects raw microsecond-epoch integers.
**Fix**: In `opteryx/expression/casts.py` — detect µs-epoch integers (value > ~1e12) and divide by 1e6.

---

### 5. TimestampVector passed to pyarrow compute — 6 failures
**Error**: `TypeError: Got unexpected argument type <class 'TimestampVector'> for compute function`
**Root Cause**: `_arrow_vector_compare()` passes a draken `TimestampVector` directly to `pyarrow.compute.*`.
**Fix**: Call `.to_arrow()` before the compute call in `opteryx/expression/evaluator/__init__.py`.

---

### 6. `Buffer has wrong number of dimensions (expected 1, got 0)` — 4 failures
**Error**: `ValueError: Buffer has wrong number of dimensions (expected 1, got 0)`
**Root Cause**: A scalar (0-dimensional array) reaches a place expecting a 1D buffer.

---

### 7. `IndexError: invalid index to scalar variable` — 4 failures
**Error**: `FunctionExecutionError: invalid index to scalar variable` in ROUND / TIME_BUCKET
**Root Cause**: Function receives a 0-d numpy scalar from the aggregation pipeline and tries to index it.

---

### 8. CONCAT with binary-typed column — 3 failures
**Error**: `ArrowNotImplementedError: Function 'binary_join_element_wise' has no kernel matching input types (binary, string, string)`
**Root Cause**: `CONCAT()` receives a binary-typed column alongside strings.
**Fix**: Cast binary inputs to utf8 before calling the kernel.

---

### 9. Carchar group-state engine — 3 failures
**Error**: `UnsupportedSyntaxError: Carchar group-state engine does not support runtime fallback`
**Root Cause**: `DictionaryVector` inputs hitting Carchar (SUM on a dict-encoded column). Planner should route to legacy.

---

### 10. IFNULL / IFNOTNULL with scalar string — 3 + 2 failures
**Error**: `FunctionExecutionError: 'str' object has no attribute 'dtype'` in IFNULL; `function_ref is None` in IFNOTNULL
**Root Cause**: IFNULL receives a raw Python `str` scalar; IFNOTNULL not properly bound at evaluation time.

---

### 11. ArrayVector / ListArray type mismatch — 3 + 2 failures
**Error**: `TypeError: Argument 'vec' has incorrect type (expected ArrayVector, got pyarrow.lib.ListArray)` in ARRAY_CONTAINS_ALL, LENGTH
**Root Cause**: Arrow→Draken conversion not happening for list-typed columns in some code paths.

---

### 12. `AttributeError: 'str' has no dtype` — 3 failures
Same class as issue 10 — a Python `str` scalar reaches code that calls `.dtype` on it.

---

### 13. IIF arity — 2 failures
**Error**: `FunctionExecutionError: select_values() takes 2 positional arguments but 3 were given`
**Root Cause**: IIF dispatches to `select_values()` with 3 arguments; function signature expects 2.

---

### 14. DictionaryVector in arithmetic / timestamp coerce — 4 failures
**Error**: `TypeError: must be real number, not DictionaryVector` / `TypeError: int() argument ... not 'DictionaryVector'`
**Root Cause**: Dict-encoded columns reach arithmetic operators and `_coerce_timestamp` without being decoded first.

---

### 15. TRIM / LTRIM on binary-typed columns — 2 failures
**Error**: `ArrowNotImplementedError: Function 'utf8_trim' / 'utf8_ltrim' has no kernel matching input types (binary)`
**Root Cause**: TRIM/LTRIM call pyarrow utf8 kernels on binary (large_binary) typed columns.

---

### 16. Date / timestamp vs integer comparison — 4 failures
**Error**: `ArrowNotImplementedError: less/greater (date64[ms], int64)`
**Root Cause**: Direct comparison of date/timestamp columns against raw integer values — missing coercion step.

---

### 17. LENGTH got ArrayVector not StringVector — 3 failures
**Error**: `FunctionExecutionError: Argument 'vec' has incorrect type (expected StringVector, got ArrayVector)`
**Root Cause**: LENGTH called on a list-typed column, which produces an ArrayVector, not a StringVector.

---

## Priority Order

| # | Issue | Count | Effort |
|---|-------|-------|--------|
| 2 | AssertionError / wrong row counts | 38 | Investigate |
| 3 | ArrowVector in `_coerce_timestamp` | 7 | Small |
| 4 | Invalid timestamp (µs epoch integer) | 7 | Small |
| 5 | TimestampVector to pyarrow compute | 6 | Small |
| 8 | CONCAT with binary column | 3 | Small |
| 13 | IIF arity | 2 | Small |
| 14 | DictionaryVector in arithmetic | 4 | Small |
| 6 | Buffer 0-dimensional | 4 | Investigate |
| 7 | IndexError in ROUND/TIME_BUCKET | 4 | Small |
| 9 | Carchar + DictionaryVector routing | 3 | Small |
| 10/12 | IFNULL/IFNOTNULL scalar str | 5 | Small |
| 11 | ArrayVector/ListArray mismatch | 5 | Small |
| 15 | TRIM/LTRIM on binary | 2 | Small |
| 16 | Date vs integer comparison | 4 | Small |
| 17 | LENGTH got ArrayVector | 3 | Small |
| 1a | ARRAY_AGG | ✅ done (session 5) | — |
| 1b | ANY_VALUE | ✅ done (session 4) | — |
| 1c | APPROX_PERCENTILE not supported | 2 | Medium |
| 1d | Subquery cascades from 1a | free | Free |
| 1e | Aggregate without parameters | investigate | Small |
| 1f | Aggregate-only node | free | Free |

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
