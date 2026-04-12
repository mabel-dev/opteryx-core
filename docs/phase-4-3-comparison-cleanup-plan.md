# Phase 4.3 Comparison Dispatch Cleanup & Refactor

## Objective
Improve comparison function robustness, eliminate remaining hasattr() and __class__.__name__ anti-patterns, consolidate repeated logic, and add comprehensive test coverage.

## Executive Summary
Despite Phase 4.1 introducing VectorType discrimination, the comparison code still uses `__class__.__name__` comparisons in 4 locations and duplicated logic patterns. Phase 4.3 eliminates these, unifies dispatch patterns, and improves code quality.

### Scope & Effort Estimate
- **Duration:** 6-8 hours
- **Files Modified:** 2-3 files
- **Tests Added:** 1 comprehensive test suite (~500-700 lines)
- **Risk Level:** Medium (touches hot path, must validate thoroughly)

---

## Current Issues Identified

### Issue 1: __class__.__name__ Comparisons (4 locations)

**Location 1: _int64_compare() @ L67**
```python
if right.__class__.__name__ in ("Int64Vector", "IntegerVector"):
```

**Location 2: _int64_compare() @ L81**
```python
if right.__class__.__name__ == "Float64Vector":
```

**Location 3: _float64_compare() @ L116**
```python
if right.__class__.__name__ == "Float64Vector":
```

**Location 4: _dict_compare() @ L188-194**
```python
cls = vec.__class__.__name__
if cls == "Date32Vector":
    ...
elif cls == "TimestampVector":
```

**Problem:** All should use VectorType dispatch instead (O(1), explicit, maintainable)

### Issue 2: Duplicated "ops" Dictionaries

**Pattern Identified in _int64_compare and _float64_compare:**
```python
ops = {
    "Eq": vec.equals_vector,
    "Lt": vec.less_than_vector,
    "Gt": vec.greater_than_vector,
    "LtEq": vec.less_than_or_equals_vector,
    "GtEq": vec.greater_than_or_equals_vector,
}
fn = ops.get(op)
```

**Problem:** Same dictionary defined twice; should be extracted to shared mapping

### Issue 3: Scalar Detection Pattern

**Current (draken_compare @ L418):**
```python
if isinstance(
    left,
    (str, int, float, bytes, bool, tuple, list, type(None), datetime.date, datetime.datetime),
) and hasattr(right, "null_count"):
```

**Problems:**
- Uses hasattr() which is slow
- Incomplete list (missing decimal.Decimal, datetime.time, etc.)
- Logic unclear (why hasattr instead of is_draken_vector?)

**Better:** Use is_scalar() and is_draken_vector() from vector_types module

### Issue 4: Negate/Flip Logic

**Current Structure:**
```python
negate = op in _NEGATED_OPS
if negate:
    op = _NEGATED_OPS[op]

# ... dispatch logic ...

return result.not_vector() if negate else result
```

**Issues:**
- Negate handling at top and bottom makes it easy to miss
- Flip logic for scalar-vector operations could be more explicit
- No dedicated tests for negate behavior

---

## Refactoring Plan

### Step 1: Create Unified Vector-Vector Comparison Dispatch

**Goal:** Extract the repeated "ops" dictionary pattern into a reusable dispatch table.

**New Code:**
```python
# Unified dispatch table for vector-vector comparisons
_VECTOR_VECTOR_OPS = {
    "Eq": lambda vec, other: vec.equals_vector(other),
    "Lt": lambda vec, other: vec.less_than_vector(other),
    "Gt": lambda vec, other: vec.greater_than_vector(other),
    "LtEq": lambda vec, other: vec.less_than_or_equals_vector(other),
    "GtEq": lambda vec, other: vec.greater_than_or_equals_vector(other),
}

def _call_vector_vector_op(op: str, left_vec, right_vec):
    """Call vector-vector operation with consistent error handling."""
    fn = _VECTOR_VECTOR_OPS.get(op)
    if fn is None:
        raise NotImplementedError(f"Vector-vector operation {op!r} not supported")
    return fn(left_vec, right_vec)
```

**Benefit:** Eliminates duplicate ops dictionaries; single source of truth

### Step 2: Replace __class__.__name__ with VectorType Checks

**Location 1: _int64_compare() @ L67**

**Before:**
```python
if right.__class__.__name__ in ("Int64Vector", "IntegerVector"):
    ops = { "Eq": vec.equals_vector, ... }
    fn = ops.get(op)
    if fn is None:
        raise NotImplementedError(...)
    return fn(right)
```

**After:**
```python
from opteryx.utils.vector_types import get_vector_type, VectorType

right_type = get_vector_type(right)
if right_type in (VectorType.INT64, VectorType.INTEGER):
    return _call_vector_vector_op(op, vec, right)
```

**Apply similar changes to all 4 locations.**

### Step 3: Replace Scalar Detection Pattern

**Before:**
```python
if isinstance(
    left,
    (str, int, float, bytes, bool, tuple, list, type(None), datetime.date, datetime.datetime),
) and hasattr(right, "null_count"):
    flip_ops = {"Gt": "Lt", "Lt": "Gt", "GtEq": "LtEq", "LtEq": "GtEq"}
    op = flip_ops.get(op, op)
    left, right = right, left
```

**After:**
```python
from opteryx.utils.vector_types import is_scalar, is_draken_vector

if is_scalar(left) and is_draken_vector(right):
    flip_ops = {"Gt": "Lt", "Lt": "Gt", "GtEq": "LtEq", "LtEq": "GtEq"}
    op = flip_ops.get(op, op)
    left, right = right, left
```

**Benefits:**
- Cleaner, more maintainable
- is_scalar() handles all scalar types
- is_draken_vector() is explicit

### Step 4: Improve Negate/Flip Logic Documentation

**Add clear comments:**
```python
# Negation handling:
# 1. Extract negate flag from op (e.g., "NotEq" -> negate=True, op="Eq")
# 2. Apply negate at end: result.not_vector() if negate else result
negate = op in _NEGATED_OPS
if negate:
    op = _NEGATED_OPS[op]

# Flip handling (scalar vs vector):
# 1. Scalar left, vector right: flip operands and directional operators
# 2. Example: 5 > [1, 2, 3] becomes [1, 2, 3] < 5
# 3. Ops that flip: Gt <-> Lt, GtEq <-> LtEq
if is_scalar(left) and is_draken_vector(right):
    flip_ops = {"Gt": "Lt", "Lt": "Gt", "GtEq": "LtEq", "LtEq": "GtEq"}
    op = flip_ops.get(op, op)
    left, right = right, left
```

### Step 5: Add Comprehensive Test Suite

**File:** `tests/test_draken_comparisons.py` (NEW)

**Test Coverage:**
```
1. Vector-Vector Comparisons
   - Int64Vector vs Int64Vector: all ops (Eq, Lt, Gt, LtEq, GtEq)
   - IntegerVector vs IntegerVector: all ops
   - Int64Vector vs IntegerVector: all ops
   - Float64Vector vs Float64Vector: all ops
   - StringVector vs StringVector: all ops
   - Mixed types (int vs float, string vs int, etc.)

2. Vector-Scalar Comparisons
   - Int64Vector vs scalar int: all ops
   - Float64Vector vs scalar float: all ops
   - StringVector vs scalar string: all ops
   - Null handling: null vs non-null, edge cases

3. Scalar-Vector Comparisons (flip logic)
   - scalar int vs Int64Vector: verify flip happens
   - scalar float vs Float64Vector: verify flip happens
   - 5 > [1, 2, 3] should produce [3]

4. Negate Operations
   - NotEq works correctly
   - NotLike works correctly
   - NotInList works correctly

5. Edge Cases
   - None / NULL handling
   - Empty vectors
   - All-null vectors
   - Mixed null and non-null vectors
   - Very large values (int64 overflow, float precision)

6. Set Operations
   - InList with various types
   - InList with null values
```

---

## Implementation Tasks (In Order)

### Task 1: Analyze Current Test Coverage
- Run existing tests to establish baseline
- Identify which comparisons are already tested
- Document gaps

**Effort:** 0.5 hours

### Task 2: Create VectorType-Based Comparison Helpers
- Add _call_vector_vector_op() dispatcher
- Add helper for type checking
- Add helper for flip logic

**Effort:** 1 hour

### Task 3: Refactor _int64_compare()
- Replace __class__.__name__ checks with VectorType
- Use new helper functions
- Verify tests pass

**Effort:** 1 hour

### Task 4: Refactor _float64_compare()
- Replace __class__.__name__ checks with VectorType
- Use new helper functions
- Verify tests pass

**Effort:** 1 hour

### Task 5: Refactor _dict_compare()
- Replace __class__.__name__ checks with VectorType
- Improve date/timestamp handling
- Verify tests pass

**Effort:** 1 hour

### Task 6: Refactor draken_compare()
- Replace isinstance chain with is_scalar() and is_draken_vector()
- Improve documentation
- Verify tests pass

**Effort:** 1 hour

### Task 7: Add Comprehensive Test Suite
- Create test_draken_comparisons.py
- Add vector-vector tests
- Add vector-scalar tests
- Add scalar-vector tests (flip logic)
- Add negate tests
- Add edge case tests

**Effort:** 2-3 hours

### Task 8: Performance Validation
- Run `make q` to verify no regressions
- Check performance hasn't degraded
- Document any performance changes

**Effort:** 0.5 hours

**Total: 7-8 hours**

---

## Success Criteria

### Code Quality
- [ ] Zero __class__.__name__ comparisons remain
- [ ] Zero hasattr() checks in comparison code
- [ ] No duplicate ops dictionaries
- [ ] Scalar detection uses is_scalar() and is_draken_vector()
- [ ] All code is documented with examples

### Test Coverage
- [ ] All vector-vector comparisons tested
- [ ] All vector-scalar comparisons tested
- [ ] All scalar-vector comparisons tested (flip logic)
- [ ] All negate operations tested
- [ ] All edge cases tested
- [ ] `make q` passes 82/88 (no regressions)

### Performance
- [ ] No performance regression (VectorType is O(1))
- [ ] All comparison operations remain fast
- [ ] Dispatch logic is clear and efficient

### Documentation
- [ ] All new functions have docstrings
- [ ] Negate/flip logic clearly documented
- [ ] Test suite documents expected behavior

---

## Risks & Mitigation

### Risk 1: Performance Regression
**Concern:** Changing dispatch logic could slow hot path
**Mitigation:** VectorType dispatch is O(1) (identical to class name comparison); validate with measurements
**Validation:** Run `make q` and compare execution times

### Risk 2: Broken Comparisons
**Concern:** Refactoring could introduce subtle bugs
**Mitigation:** Comprehensive test suite (100+ test cases); validate each change with `make q`
**Validation:** All 82/88 tests must pass; no new failures

### Risk 3: Incomplete Type Coverage
**Concern:** VectorType might not cover all types
**Mitigation:** VectorType covers all 13 vector types + UNKNOWN fallback
**Validation:** Refactor each location carefully; add tests for each type

---

## Files to Modify

### Primary
- `opteryx/expression/evaluator/comparisons.py` (150-200 lines changed)

### New
- `tests/test_draken_comparisons.py` (~500-700 lines)

### Reference (Read-Only)
- `opteryx/utils/vector_types.py` (already correct)
- `opteryx/expression/evaluator/evaluation.py` (already using VectorType)

---

## Before & After Examples

### Example 1: Vector-Vector Comparison

**Before:**
```python
def _int64_compare(op: str, vec, right):
    if right.__class__.__name__ in ("Int64Vector", "IntegerVector"):
        ops = {
            "Eq": vec.equals_vector,
            "Lt": vec.less_than_vector,
            "Gt": vec.greater_than_vector,
            "LtEq": vec.less_than_or_equals_vector,
            "GtEq": vec.greater_than_or_equals_vector,
        }
        fn = ops.get(op)
        if fn is None:
            raise NotImplementedError(f"Int64Vector vector-vector: unsupported op {op!r}")
        return fn(right)
```

**After (3 lines):**
```python
right_type = get_vector_type(right)
if right_type in (VectorType.INT64, VectorType.INTEGER):
    return _call_vector_vector_op(op, vec, right)
```

**Benefits:** 60% reduction, no duplication, clear dispatch

### Example 2: Type Conversion Check

**Before:**
```python
if right.__class__.__name__ == "Float64Vector":
    import pyarrow as pa
    from opteryx.compiled.draken.interop.arrow import vector_from_arrow
    float_vec = vector_from_arrow(vec.to_arrow().cast(pa.float64()))
    return _float64_compare(op, float_vec, right)
```

**After:**
```python
if get_vector_type(right) == VectorType.FLOAT64:
    import pyarrow as pa
    from opteryx.compiled.draken.interop.arrow import vector_from_arrow
    float_vec = vector_from_arrow(vec.to_arrow().cast(pa.float64()))
    return _float64_compare(op, float_vec, right)
```

**Benefits:** Explicit type check, single source of truth

### Example 3: Scalar Detection

**Before (4 lines):**
```python
if isinstance(
    left,
    (str, int, float, bytes, bool, tuple, list, type(None), datetime.date, datetime.datetime),
) and hasattr(right, "null_count"):
```

**After (1 line):**
```python
if is_scalar(left) and is_draken_vector(right):
```

**Benefits:** 75% reduction, complete type coverage, no hasattr()

---

## Status

**Created:** Phase 4.2 cleanup completion
**Status:** Ready for implementation
**Next Step:** Execute Task 1 (analyze current test coverage)
**Estimated Completion:** 6-8 hours from task start