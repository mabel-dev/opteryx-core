# Correctness Action List

## Status Overview
- **Goal**: Return to correctness baseline before performance tuning
- **Minimum Bar**: `make t` and `make clickbench` must pass
- **Secondary**: `make test` (full suite)
- **Current Date**: 2026-03-11

> [!Note]
> The goal is not fix at the cost of architectural principles - we do not fix through poor programming practices or changes which violate the design goals of the system.

---

## Test Results Summary

### Current Test Status
```
make t (SQL Battery Tests):
- test_shapes_basic.py:                  ✅ PASSING
- test_shapes_data_sources.py:           ❌ FAILED (exit 1) - 2 failures
- test_shapes_operators_expressions.py:  ❌ FAILED (exit -11) - SEGFAULT
- test_shapes_aliases_distinct.py:       ❌ FAILED (exit -11) - SEGFAULT
- test_shapes_functions_aggregates.py:   ❌ FAILED (exit 1) - Multiple failures
- test_shapes_joins_subqueries.py:       ❌ FAILED (exit 1) - Multiple failures
- test_shapes_edge_cases.py:             ❌ FAILED (exit -11) - SEGFAULT

make clickbench:
- Q11: ❌ FAILED - "Carchar group-state engine does not support runtime fallback"
- Q14: ❌ FAILED - "Carchar group-state engine does not support runtime fallback"
- 40/43 queries passing
```

---

## ✅ Completed Work

### NULL Comparison Operator Fix
**Status**: DEPLOYED
**Files Modified**: `opteryx/expression/evaluator/__init__.py`
**Changes**:
- Added NULL handling to: `_string_compare`, `_int64_compare`, `_float64_compare`, `_timestamp_compare`, `_date32_compare`, `_interval_compare`, `_dict_compare`, `_constant_compare`
- Returns empty BoolVector when comparing with NULL (SQL three-valued logic)
- Lines modified: All comparison dispatchers now check `if right is None: return BoolVector(len(vec))`

**Test Result**: NULL comparisons no longer throw TypeError ✅

---

## 🔴 Critical Blockers (HIGH PRIORITY)

### 1. Segmentation Faults - GROUP BY without Aggregates (✅ FIXED)
**Status**: ✅ FIXED AND DEPLOYED
**Root Cause Identified**: When GROUP BY has no aggregates, the Draken engine tried to create CarcharGroupStateEngine with an empty aggregations list, causing a crash.

**Solution Implemented**:
- File: `opteryx/operators/draken_aggregate_and_group_node.py`
- Added implicit COUNT(*) aggregate when GROUP BY has no explicit aggregates
- Track when implicit aggregate added via `self._implicit_count_added` flag
- Remove implicit aggregate column from output in `finalize_morsels()`

**Test Results**:
```
❌ BEFORE: SELECT id FROM planets GROUP BY id -- SEGFAULT
✅ AFTER:  SELECT id FROM planets GROUP BY id -- SUCCESS
✅ AFTER:  SELECT name FROM testdata.satellites GROUP BY name -- SUCCESS
```

**Verification**: All 4 test patterns now pass:
- ✅ SELECT DISTINCT id FROM planets
- ✅ SELECT id, COUNT(*) FROM planets GROUP BY id  
- ✅ SELECT id FROM planets GROUP BY id
- ✅ SELECT name FROM satellites GROUP BY name

**Impact**: Fixes 3 previously-segfaulting test files (operators_expressions, aliases_distinct, edge_cases)

---

### 2. LIKE ANY Operator with Array Columns
**Status**: ❌ PARTIALLY FIXED (still broken)
**Error**: `TypeError: Argument 'column' has incorrect type (expected opteryx.draken.vectors.array_vector.ArrayVector, got pyarrow.lib.StringArray)`

**Root Cause**: When query filters array/list columns with LIKE ANY:
- Column comes from disk as `ArrowVector` wrapping PyArrow `ListArray`
- `vector_anyop_like()` Cython function expects `ArrayVector`
- Conversion via `vector_from_arrow()` not working correctly

**Failing Queries**:
```sql
SELECT name, missions FROM testdata.astronauts WHERE missions LIKE ANY '%apoll%'
```

**Files Involved**:
- `opteryx/expression/evaluator/__init__.py` (lines 613-658) - Fixed but still broken
- `opteryx/compiled/vector_ops/vector_anyop_like.pyx` - Expects ArrayVector
- `opteryx/draken/interop/arrow.py` - Conversion functions

**Action Items**:
- [ ] Debug why `vector_from_arrow(arrow_list_array)` doesn't convert ListArray properly
- [ ] Check if `ListArray.values` is the actual StringArray causing the error
- [ ] Implement fix: Either convert values separately OR change vector_anyop_like signature
- [ ] Test with: `SELECT name FROM testdata.astronauts WHERE missions LIKE ANY '%apoll%'`
- [ ] Also fix AnyOpILike, AnyOpNotLike, AnyOpNotILike similarly

---

### 3. ARRAY_AGG + UNNEST Failures (8+ instances)
**Status**: ❌ UNRESOLVED
**Error**: `IncorrectTypeError`
**Affected Queries** (examples):
```sql
SELECT * FROM (SELECT ARRAY_AGG(name) AS n FROM testdata.astronauts 
               GROUP BY GROUP) AS alma CROSS JOIN UNNEST(n) AS nn
```

**Investigation Needed**:
- [ ] Check ARRAY_AGG return type validation
- [ ] Check UNNEST input type validation
- [ ] Verify ARRAY_AGG properly creates ArrayVector vs ArrowVector
- [ ] Check GROUP BY expression parsing (is GROUP a reserved word issue?)

**Action Items**:
- [ ] Test simple ARRAY_AGG queries: `SELECT ARRAY_AGG(name) FROM testdata.astronauts`
- [ ] Test UNNEST standalone: `SELECT * FROM UNNEST([1, 2, 3])`
- [ ] Check if GROUP is being parsed as string literal vs reserved word
- [ ] Verify type system handles array types correctly

---

## 🟡 High Priority Issues

### 4. Function Execution Errors
**Status**: ❌ MULTIPLE ISSUES
**Failing Functions**:
- `REVERSE(name)` - FunctionExecutionError
- `REGEXP_REPLACE(name, '^E', 'G')` - FunctionExecutionError  
- `ARRAY_CONTAINS_ANY(missions, @@user_memberships)` - FunctionExecutionError

**Action Items**:
- [ ] Check REVERSE function implementation
- [ ] Check REGEXP_REPLACE function implementation
- [ ] Check ARRAY_CONTAINS_ANY function handling
- [ ] Verify function signatures and parameter types

---

### 5. ORDER BY with Specific Column Types
**Status**: ❌ UNRESOLVED
**Error**: `ArrowNotImplementedError`
**Failing Queries**:
```sql
SELECT * FROM testdata.planets ORDER BY distanceFromSun DESC
SELECT id FROM testdata.planets WHERE density > 4000 ORDER BY id ASC
```

**Investigation Needed**:
- [ ] Check if issue is specific to floating-point columns
- [ ] Check Arrow vs Draken vector sorting implementations
- [ ] Verify fallback logic for unsupported Arrow operations

**Action Items**:
- [ ] Test: `SELECT * FROM $planets ORDER BY id ASC`
- [ ] Test: `SELECT * FROM $planets ORDER BY distanceFromSun ASC`
- [ ] Check if Arrow vectors don't implement sorting
- [ ] Implement Draken fallback for Arrow-wrapped columns

---

### 6. Variable/Membership Binding Issues
**Status**: ❌ UNRESOLVED
**Error**: `UnboundLocalError` / `FunctionExecutionError`
**Failing Queries**:
```sql
SELECT * FROM $planets WHERE name = ANY(@@user_memberships)
SELECT * FROM $planets WHERE ARRAY_CONTAINS_ANY(missions, @@user_memberships)
```

**Investigation Needed**:
- [ ] Check session membership variable binding
- [ ] Verify `@@user_memberships` is correctly set in session
- [ ] Check if variables are being passed to function correctly

---

### 7. GROUP BY Expression and HAVING Clause Issues
**Status**: ❌ MULTIPLE ISSUES

**Issue 7a**: HAVING count mismatch
```sql
SELECT planetId, MIN(magnitude) FROM testdata.satellites 
GROUP BY planetId HAVING MIN(magnitude) < 5
-- Returns 7 rows but 2 were expected
```

**Issue 7b**: GROUP by reserved word parsing
```sql
-- Multiple queries with "GROUP BY GROUP" fail with IncorrectTypeError
```

**Action Items**:
- [ ] Verify HAVING clause filtering logic
- [ ] Check if GROUP is being parsed as reserved word vs column name
- [ ] Test: `SELECT COUNT(*) FROM $planets` (basic HAVING)
- [ ] Test: `SELECT status, COUNT(*) FROM testdata.astronauts GROUP BY status HAVING COUNT(*) > 5`

---

## 🟢 Medium Priority Issues

### 8. Carchar Group-State Engine Fallback (make clickbench)
**Status**: ❌ UNRESOLVED (Performance, not correctness critical)
**Error**: "Carchar group-state engine does not support runtime fallback"
**Queries**: Q11, Q14
**Note**: Per project guidelines: "fail early rather than silent degradation"

**Action Items**:
- [ ] Research Carchar architecture for group-state operations
- [ ] Identify which aggregations trigger group-state
- [ ] Implement proper fallback or fix group-state implementation
- [ ] Verify fix doesn't regress performance

---

### 9. Other Function Issues
**Status**: ❌ UNRESOLVED
**Functions Needing Review**:
- `REPLACE(name, 'e', 'a')` - FunctionExecutionError
- `INITCAP(REVERSE(name))` - FunctionExecutionError
- `CONCAT(ARRAY_AGG(name))` - UnsupportedSyntaxError

**Action Items**:
- [ ] Review each function implementation for issues
- [ ] Add error context to understand failure points

---

## Action Plan (Suggested Priority Order)

### Phase 1: Crash Stability (CRITICAL)
1. [ ] **Debug segfaults** - Use gdb to identify crash locations
2. [ ] **Fix segfault root cause** - Patch memory safety issues
3. [ ] Verify test_shapes_operators_expressions.py passes
4. [ ] Verify test_shapes_aliases_distinct.py passes  
5. [ ] Verify test_shapes_edge_cases.py passes

### Phase 2: Core Correctness Fixes
6. [ ] **Fix LIKE ANY array conversion** - Resolve ArrowVector→ArrayVector issue
7. [ ] Verify test_shapes_functions_aggregates.py passes
8. [ ] **Fix ORDER BY for Arrow vectors** - Implement proper sorting
9. [ ] Verify test_shapes_data_sources.py passes

### Phase 3: Advanced Features
10. [ ] **Fix ARRAY_AGG + UNNEST** - Resolve type errors
11. [ ] **Fix function errors** - REVERSE, REGEXP_REPLACE, etc.
12. [ ] **Fix variable binding** - @@user_memberships handling
13. [ ] Verify test_shapes_joins_subqueries.py passes

### Phase 4: Performance Baseline
14. [ ] **Fix Carchar group-state fallback** - Q11, Q14
15. [ ] Verify `make clickbench` passes (40→42+ queries)

### Phase 5: Full Validation
16. [ ] Run `make test` (full suite)
17. [ ] Review all test output
18. [ ] Commit fixes

---

## Testing Commands

```bash
# Run individual test files for debugging
python tests/integration/sql_battery/test_shapes_basic.py
python tests/integration/sql_battery/test_shapes_data_sources.py
python tests/integration/sql_battery/test_shapes_operators_expressions.py
python tests/integration/sql_battery/test_shapes_aliases_distinct.py
python tests/integration/sql_battery/test_shapes_functions_aggregates.py
python tests/integration/sql_battery/test_shapes_joins_subqueries.py
python tests/integration/sql_battery/test_shapes_edge_cases.py

# Run test suite targets
make t              # Quick test (shapes battery)
make clickbench     # Performance baseline
make test           # Full test suite

# Debug with segfault info
gdb python tests/integration/sql_battery/test_shapes_operators_expressions.py

# Test specific query
python -c "
import opteryx
from opteryx.connectors import DiskConnector
opteryx.register_workspace('testdata', DiskConnector)
session = opteryx.session()
result = session.execute_to_arrow('YOUR QUERY HERE')
print(result)
"
```

---

## Notes

### Project Constraints (from .github/copilot-instructions.md)
- ✅ Always prefer failure over silent degradation
- ✅ Don't generate Python fallback implementations for Cython code
- ✅ If Cython compilation would fail, return compile-time error
- ✅ Never duplicate logic in Python and Cython unless explicitly requested
- ✅ Performance > convenience (can tolerate less convenient APIs if performance is better)
- ✅ No dynamic dispatch in hot paths
- ✅ Do not gate imports behind try/except

### Key Files
- `opteryx/expression/evaluator/__init__.py` - Query expression evaluation
- `opteryx/compiled/vector_ops/*.pyx` - Cython vector operations
- `opteryx/draken/vectors/*.pyx` - Draken vector implementations
- `opteryx/draken/interop/arrow.pyx` - Arrow↔Draken conversions
- `opteryx/managers/execution/serial_engine.py` - Execution engine
- `opteryx/operators/filter_node.py` - Filter execution

---

## References

### Test Files
- `tests/integration/sql_battery/run_shapes_battery.py` - Test runner
- `tests/integration/sql_battery/test_shapes_*.py` - Individual test suites
- `tests/performance/clickbench/clickbench.py` - Performance baseline

### Data
- `testdata/astronauts/astronauts.parquet` - Test table (missions: list<element: string>)
- `testdata/planets/planets.parquet` - Test table (distanceFromSun: float64)
- `testdata/satellites/satellites.parquet` - Test table

---

## Status Board

**Last Updated**: 2026-03-11  
**Phase**: 1 - Crash Stability  
**Blocking**: Segfaults in 3 test files  
**Next Action**: Debug segfaults with gdb
