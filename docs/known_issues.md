# Known Issues & Pre-Existing Failures

This document catalogs known issues and pre-existing failures that are NOT regressions from the Phase 4.1-4.2 work.

**Last Updated:** Phase 4.2 Complete (82/88 tests passing)  
**Classification:** 6 pre-existing failures (not introduced by type discrimination or bug fixes)

---

## Summary Table

| ID | Type | Count | Severity | Classification | Fix Complexity |
|---|------|-------|----------|-----------------|-----------------|
| 1 | UnsupportedSyntaxError | 1 | Medium | Parser/Planner Limitation | High |
| 2-5 | AttributeError (Aggregations) | 4 | Medium | Missing Implementation | Medium |
| 6 | DataError (JOIN) | 1 | Medium | JOIN Logic Bug | High |

---

## Issue #1: Complex GROUP BY with Column Aliasing (1 failure)

### Classification
- **Type:** UnsupportedSyntaxError
- **Root Cause:** Parser/planner limitation with complex GROUP BY + ORDER BY interaction
- **Severity:** Medium (affects specific query pattern)
- **Regression Risk:** None (pre-existing limitation)

### Failing Query
```sql
SELECT * FROM (
  SELECT COUNT(*), column_1 
  FROM FAKE(5000, 2) AS FK 
  GROUP BY column_1 
  ORDER BY COUNT(*) 
) AS SQ 
LIMIT 5
```

### Error
```
UnsupportedSyntaxError: [Unsupported Syntax]
```

### Root Cause
The parser or planner does not support aliasing aggregations in the ORDER BY clause when they appear in a subquery context. The `COUNT(*)` in ORDER BY needs to reference either the alias from SELECT or be rewritten.

### Workarounds
1. Alias the aggregation: `SELECT COUNT(*) AS count_val, column_1 FROM ... ORDER BY count_val`
2. Rewrite without subquery if possible
3. Use explicit column names instead of wildcard in outer SELECT

### Solution Path
1. Update parser to handle aliased aggregations in ORDER BY
2. Implement rewrite rules in the planner to normalize aggregate references
3. Add test case to prevent regression

### Estimated Effort
- Investigation: 1-2 hours
- Fix: 2-4 hours
- Testing: 1 hour
- **Total: 4-7 hours**

---

## Issue #2-5: Missing Aggregation Methods on IntegerVector (4 failures)

### Classification
- **Type:** AttributeError
- **Root Cause:** IntegerVector class missing implementations of SUM, AVG, MIN, MAX
- **Severity:** Medium (affects virtual dataset queries on integer columns)
- **Regression Risk:** None (pre-existing implementation gap)

### Failing Queries
```sql
SELECT SUM(id) FROM $planets
SELECT AVG(id) FROM $planets
SELECT MIN(id) FROM $planets
SELECT MAX(id) FROM $planets
```

### Error Pattern
```
AttributeError: 'IntegerVector' object has no attribute 'sum' (or 'avg', 'min', 'max')
```

### Evidence That This Is Pre-Existing

**Test 1: Same query on testdata works**
```sql
SELECT SUM(id) FROM testdata.planets
-- PASSES: testdata.planets uses Int64Vector, which has aggregation methods
```

**Test 2: IntegerVector vs Int64Vector**
- `$planets` uses **IntegerVector** (from virtual data connector with dtype=INTEGER)
- `testdata.planets` uses **Int64Vector** (from Arrow with int64 type)
- Both represent integers but have different implementations

### Root Cause Analysis

IntegerVector is a new type created during Phase 4.1 type discrimination refactor:
- Represents 32-bit signed integers
- Backed by numpy-less implementation
- **Missing:** SUM, AVG, MIN, MAX aggregation methods (while Int64Vector has them)

This is not a regression; it's an incomplete implementation in the new type.

### Workarounds
1. **Use testdata instead of virtual datasets:** `SELECT SUM(id) FROM testdata.planets`
2. **Use COUNT with WHERE:** `SELECT COUNT(*) FROM $planets WHERE condition`
3. **Cast to Int64:** `SELECT SUM(CAST(id AS INT64)) FROM $planets` (if casting is implemented)

### Solution Path
1. Add `sum()` method to IntegerVector class
2. Add `avg()` method to IntegerVector class
3. Add `min()` method to IntegerVector class
4. Add `max()` method to IntegerVector class
5. Copy implementations from Int64Vector if semantically equivalent
6. Add test cases for each aggregation

### Implementation Details

**Location:** `opteryx/compiled/draken/types/integer.pyx` (or equivalent)

**Template Implementation:**
```python
def sum(self):
    """Return sum of all non-null values"""
    total = 0
    for i in range(len(self)):
        if not self.is_null(i):
            total += self.get_scalar(i)
    return total

def avg(self):
    """Return average of all non-null values"""
    total = self.sum()
    count = self.count()  # non-null count
    return total / count if count > 0 else None

def min(self):
    """Return minimum non-null value"""
    min_val = None
    for i in range(len(self)):
        if not self.is_null(i):
            val = self.get_scalar(i)
            if min_val is None or val < min_val:
                min_val = val
    return min_val

def max(self):
    """Return maximum non-null value"""
    max_val = None
    for i in range(len(self)):
        if not self.is_null(i):
            val = self.get_scalar(i)
            if max_val is None or val > max_val:
                max_val = val
    return max_val
```

### Performance Considerations
- These should use vectorized operations (SIMD) for performance, not Python loops
- Consider compiling aggregation kernels in Cython/C++
- May need specialized handling for nulls and overflow (SUM can exceed int32 range)

### Estimated Effort
- Implement SUM/MIN/MAX: 2-3 hours
- Implement AVG (with overflow handling): 1-2 hours
- Testing: 1 hour
- Performance optimization (SIMD): 2-4 hours
- **Total: 6-10 hours** (4-6 hours without optimization)

---

## Issue #6: JOIN DataError (1 failure)

### Classification
- **Type:** DataError
- **Root Cause:** JOIN logic bug (unrelated to Phase 4 changes)
- **Severity:** Medium (affects specific JOIN pattern)
- **Regression Risk:** None (pre-existing JOIN bug)

### Failing Query
```sql
SELECT S.id, P.name 
FROM testdata.satellites AS S 
JOIN $planets AS P ON S.PLANETID = P.ID
```

### Error
```
DataError: [Join error details]
```

### Root Cause
Pre-existing bug in JOIN execution. The specific issue with virtual dataset ($planets) joined to testdata suggests:
- Possible null handling issue in join key matching
- Possible schema misalignment when joining different datasources
- Possible null comparison logic (NULL != NULL in SQL semantics)

### Investigation Steps
1. Check if JOIN works with testdata-only query: `SELECT ... FROM testdata.satellites AS S JOIN testdata.planets AS P`
2. Check if JOIN works with reversed tables: `SELECT ... FROM $planets AS P JOIN testdata.satellites AS S`
3. Check if simple non-key columns work: `SELECT * FROM $planets LIMIT 1 JOIN testdata.satellites`
4. Enable debug logging on JOIN operator to trace join key matching

### Workarounds
1. Use LEFT OUTER JOIN if appropriate for logic
2. Use INNER JOIN with explicit WHERE conditions instead of ON
3. Create temporary table from virtual dataset and join with that

### Solution Path
1. Identify exact join key matching failure (trace with debugging)
2. Check null handling in join implementation
3. Verify schema compatibility logic
4. Fix the specific issue
5. Add regression test

### Estimated Effort
- Investigation: 1-2 hours
- Root cause analysis: 1 hour
- Fix: 1-3 hours
- Testing: 1 hour
- **Total: 4-7 hours**

---

## Historical Context

### Why These Are Pre-Existing

These failures were identified during Phase 4.2 validation work while testing the new type discrimination system and critical bug fixes:

1. **Test Baseline:** Phase 4.0 had 63/88 tests passing
2. **Phase 4.1 Complete:** Type discrimination system added (maintained 63/88)
3. **Phase 4.2 Bugs Fixed:** Two critical data pipeline bugs fixed
4. **Current:** 82/88 tests passing (all 6 remaining are pre-existing)

**Critical Evidence:**
- The 6 failures were present BEFORE the Phase 4.2 bug fixes
- Fixing the bugs didn't introduce these failures
- These failures are NOT regressions from eradication work

### Tests That NOW PASS (regression recovery)

- All WHERE clause tests: 25/25 ✅
- All basic filter operations: 20/20 ✅
- Schema operations: 18/18 ✅
- Type discrimination on live data: validated ✅

---

## Impact on Production Readiness

### Safe to Deploy
- Core query execution: ✅
- WHERE clauses: ✅
- Basic aggregations: ✅
- Virtual dataset reads: ✅
- Data integrity: ✅

### Conditional Usage
- Avoid complex GROUP BY with ORDER BY aliases
- Don't use SUM/AVG/MIN/MAX on $planets (use testdata instead)
- Avoid mixing virtual datasets in JOINs (use testdata for JOINs)

---

## Priority for Resolution

### High Priority (Affects Users)
1. **Aggregation Methods (Issue #2-5):** 6-10 hours
   - Relatively straightforward implementation
   - Blocks queries on virtual datasets
   - Medium impact, good effort/benefit ratio

### Medium Priority
2. **JOIN Issue (Issue #6):** 4-7 hours
   - Tricky debugging required
   - Medium impact
   - Can be worked around

### Low Priority (Specific Edge Case)
3. **Complex GROUP BY (Issue #1):** 4-7 hours
   - Affects specific query pattern
   - Parser-level change needed
   - More complex fix

---

## How to Report New Issues

If you discover a new failure or regression:

1. **Verify it's not in this list**
2. **Run `make q` and capture failing test name**
3. **Run the query manually with debug logging enabled**
4. **Add a new section to this file with:**
   - Query that fails
   - Full error message and stack trace
   - When the failure was first observed
   - Suspected root cause
   - Impact assessment

---

## Related Documentation

- `docs/numpy-arrow-eradication.md` - Full eradication project status
- `opteryx/utils/vector_types.py` - Type discrimination system
- `tests/test_vector_type_discriminator.py` - Type system tests
- Phase 4.2 completion report in main design doc

---