# Correctness Action List

## Status Overview
- **Goal**: Return to correctness baseline before performance tuning
- **Minimum Bar**: `make t` and `make clickbench` must pass
- **Secondary**: `make test` (full suite)
- **Last Updated**: 2026-03-14 (session 5 refresh)

> [!Note]
> The goal is not fix at the cost of architectural principles - we do not fix through poor programming practices or changes which violate the design goals of the system.

---

## Test Results Summary

### Current Test Status (verified locally on 2026-03-14)
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
- test_shapes_operators_expressions.py:  527 passed / 23 failed
- test_shapes_aliases_distinct.py:       515 passed / 52 failed
- test_shapes_functions_aggregates.py:   176 passed / 8 failed
- test_shapes_joins_subqueries.py:       175 passed / 5 failed
- test_shapes_edge_cases.py:             283 passed / 30 failed

Total executable statement cases: 1937 passed / 118 failed
Additional pytest-only collection issue: 1 (`test_shapes_data_sources.py`)

Current top root causes:
  44  AssertionError (wrong row counts / wrong semantics)
   6  TypeError: TimestampVector passed to pyarrow compute
   5  ValueError: Invalid timestamp
   4  UnsupportedSyntaxError: Draken aggregator does not support this query shape
   4  ValueError: Buffer has wrong number of dimensions
   4  IndexError: invalid index to scalar variable
   3  UnsupportedSyntaxError: Carchar runtime fallback
   3  TypeError: LENGTH got ArrayVector not StringVector
   3  TypeError: ArrayVector expected, got pyarrow ListArray
   3  ArrowNotImplementedError: binary_join_element_wise (binary + string)
   3  AttributeError: 'str' has no dtype
   2  TypeError: select_values() takes 2 positional arguments but 3 were given
   2  TypeError: DictionaryVector reaches numeric/timestamp coercion
   2  FunctionExecutionError: IFNOTNULL was not bound
   2  SqlError: microsecond-epoch TIMESTAMP cast still invalid

clickbench:
- 42/42 queries passing ✅
```

> [!Note]
> There are two different numbers in play:
> - `make t` is only a file-level gate and currently reports 5 failing battery files.
> - the detailed inventory below is based on executing every SQL battery statement directly, which is where the current `118` failing cases come from.

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

> **Note on session 3 error inventory**: The session 3 breakdown listed ~18 distinct error categories. These were initially (incorrectly) dismissed as cascades from unsupported aggregates. They are real independent bugs — confirmed by the later statement-level battery inventory after `ARRAY_AGG` and `ANY_VALUE` were fixed.

---

## 🔴 Open Issues

### 1. Wrong row counts / wrong semantics — 44 failures
**Error**: `AssertionError`
**Scope**: still the largest bucket by far. The remaining failures are concentrated in:
- `HAVING` and grouped filtering
- join semantics / null-side filtering
- date and interval predicates
- expression rewrites that change null behavior

Representative failures:
```sql
SELECT * FROM testdata.satellites WHERE planetId = id
SELECT planetId, MIN(magnitude) FROM testdata.satellites GROUP BY planetId HAVING MIN(magnitude) > 5
SELECT COUNT(*), VARCHAR(year) FROM testdata.astronauts GROUP BY VARCHAR(year)
SELECT * FROM testdata.missions WHERE Lauched_at < CURRENT_TIMESTAMP + INTERVAL '7' DAY
```

---

### 2. Timestamp / date coercion and comparison bugs — ~17 failures
**Current sub-buckets**:
- `6` — `TimestampVector` passed directly to `pyarrow.compute`
- `5` — plain `Invalid timestamp`
- `2` — microsecond-epoch TIMESTAMP cast still rejected
- `3` — date/timestamp vs integer comparison kernel mismatch
- `1` — interval arithmetic path using `numpy.datetime64` incorrectly

Representative failures:
```sql
SELECT TIMESTAMP(1700000000000000)
SELECT CAST('2022-01-0' || VARCHAR(planetId) AS TIMESTAMP) FROM testdata.satellites
SELECT * FROM $planets WHERE TIMESTAMP '2023-01-01' = DATE '2023-01-01'
SELECT * FROM testdata.missions WHERE INTERVAL '7' DAY < CURRENT_TIMESTAMP - Lauched_at
```

Likely touch points:
- `opteryx/expression/evaluator/__init__.py`
- `opteryx/expression/casts.py`
- `opteryx/expression/ops.py`

---

### 3. Draken grouped routing / support gaps — 7 failures
**Current sub-buckets**:
- `4` — `UnsupportedSyntaxError: Draken aggregator does not support this query shape`
- `3` — `UnsupportedSyntaxError: Carchar group-state engine does not support runtime fallback`

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

### 4. Array / list typing bugs — ~11 failures
**Current sub-buckets**:
- `3` — `LENGTH` gets `ArrayVector` instead of `StringVector`
- `3` — `ArrayVector` expected, got `pyarrow.lib.ListArray`
- `2` — array casts fail with `Unsupported cast from list<item: binary> to null`
- `1` — `LENGTH(ARRAY_AGG(...))` gets `VectorVector`
- `1` — grouped `name[1]` goes down an `ArrayVector` path on a `StringVector`
- `1` — `ARRAY_CONTAINS` hits `string index out of range`

Representative failures:
```sql
SELECT LENGTH(missions) FROM testdata.astronauts
SELECT * FROM (SELECT LENGTH(ARRAY_AGG(DISTINCT planetId)) AS L FROM testdata.satellites GROUP BY planetId) AS I WHERE L = 1
SELECT missions FROM testdata.astronauts WHERE ARRAY_CONTAINS_ALL(missions, ('Gemini 7', 'Apollo 8'))
SELECT CAST(p.id AS ARRAY<VARCHAR>) FROM testdata.satellites AS s LEFT JOIN $planets AS p ON s.id = p.id WHERE s.id > 10
```

---

### 5. IFNULL / IFNOTNULL scalar binding bugs — 5 failures
**Error**:
- `FunctionExecutionError: 'str' object has no attribute 'dtype'`
- `FunctionExecutionError: Function 'IFNOTNULL' was not bound`

Representative failures:
```sql
SELECT * FROM (SELECT p.Price AS pri, s.escapeVelocity FROM testdata.missions AS p INNER JOIN $planets AS s ON p.Price = s.escapeVelocity) AS sv WHERE IFNULL(NULL, pri) = pri
SELECT * FROM testdata.astronauts WHERE IFNULL(birth_place['state'], 'home') == 'CA'
```

---

### 6. Scalar indexing bugs in grouped expressions — 4 failures
**Error**: `IndexError: invalid index to scalar variable`
**Affected functions**:
- `ROUND`
- `TIME_BUCKET`

Representative failures:
```sql
SELECT ROUND(magnitude, 1) FROM testdata.satellites GROUP BY ROUND(magnitude, 1)
SELECT TIME_BUCKET(birth_date, 10, 'year') AS decade, COUNT(*) FROM testdata.astronauts GROUP BY TIME_BUCKET(birth_date, 10, 'year')
```

---

### 7. DictionaryVector decode/coercion bugs — 4 failures
**Error**:
- `int() argument ... not 'DictionaryVector'`
- `must be real number, not DictionaryVector`

Representative failures:
```sql
SELECT TRY_CAST(planetId AS DECIMAL) AS value FROM testdata.satellites
SELECT * FROM testdata.satellites WHERE CAST(CAST(id AS BLOB) AS INTEGER) == id
```

---

### 8. Binary/string kernel mismatch — 5 failures
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

### 9. IIF arity bug — 2 failures
**Error**: `select_values() takes 2 positional arguments but 3 were given`
**Representative failure**:
```sql
WITH categorised AS (
  SELECT id, name, CASE WHEN mass > 1 THEN 'large' ELSE 'small' END AS size FROM $planets
)
SELECT size, COUNT(*) FROM categorised GROUP BY size
```

---

### 10. Long-tail singleton issues
These are current but low-count enough that they are best handled after the large buckets above:
- `ModuleNotFoundError: opteryx_catalog` during direct `pytest` collection of `test_shapes_data_sources.py`
- `RANDOM_STRING()` zero-arg arity
- `StringVector` column-column comparisons not yet supported
- `TIMESTAMP = DATE` constant-folding path produces Arrow kernel mismatch
- `pyarrow.lib.StringScalar` / `ChunkedArray` type handling gaps
- a few remaining null/dtype edge cases (`NoneType` / `Date64Array`)

---

## Priority Order

| # | Issue | Count | Effort |
|---|-------|-------|--------|
| 1 | Wrong row counts / wrong semantics | 44 | Investigate |
| 2 | Timestamp / date coercion and comparison | ~17 | Small-Medium |
| 4 | Array / list typing bugs | ~11 | Small-Medium |
| 3 | Draken grouped routing / support gaps | 7 | Medium |
| 5 | IFNULL / IFNOTNULL scalar binding | 5 | Small |
| 8 | Binary/string kernel mismatch | 5 | Small |
| 6 | Scalar indexing in ROUND / TIME_BUCKET | 4 | Small |
| 7 | DictionaryVector decode / coercion | 4 | Small |
| 9 | IIF arity | 2 | Small |
| 10 | Long-tail singleton issues | ~20 | Mixed |

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
- `opteryx/operators/draken_aggregate_and_group_node.py` — grouped aggregate support and routing
- `opteryx/operators/draken_aggregate_node.py` — aggregate-only support checks
- `opteryx/expression/functions/implementations/` — IFNULL, IFNOTNULL, LENGTH, ARRAY_* and text functions
- `opteryx/draken/interop/arrow.pyx` — Arrow↔Draken conversions for list/date/timestamp values
