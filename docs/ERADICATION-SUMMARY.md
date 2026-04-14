# NumPy/PyArrow Eradication: Executive Summary

**Status:** 86/88 tests passing (97%) | Analysis Complete | Ready for implementation

---

## The Challenge

Opteryx's expression evaluator (query execution hot path) is deeply dependent on PyArrow and NumPy for:
- Filter operations and comparisons
- Logical operations (AND/OR/XOR)
- Type coercion and temporal conversions
- String matching and list operations
- Null handling

These dependencies create:
- 🐢 Performance bottlenecks (repeated array allocations, conversions)
- 🔒 API lock-in (can't optimize below PyArrow's abstractions)
- 📦 Unnecessary external dependencies (goals: remove numpy, pyarrow, abseil, minio)

---

## The Opportunity

Opteryx owns **Draken** — a high-performance columnar vector library with:
- ✅ Native kernels for all comparison operations
- ✅ Draken vectors (Int64Vector, Float64Vector, BoolVector, StringVector, etc.)
- ✅ Compiled C++/Cython implementation
- ✅ Direct memory access without conversions
- ✅ Type-specialized fast paths

**Key insight:** The compilation infrastructure exists. We just need to wire it into the expression evaluator.

---

## What We Found

### By the Numbers
- **56 files** import numpy or pyarrow
- **16 files** are in HOT paths (query execution loop) — 🔴 CRITICAL
- **21 files** are in WARM paths (boundaries, column ops) — 🟡 MEDIUM  
- **19 files** are in COLD paths (planning, initialization) — 🟢 ACCEPTABLE

### Files in HOT Path (Must Eradicate)
1. `opteryx/expression/operations/__init__.py` — Filter dispatch (HIGHEST IMPACT)
2. `opteryx/expression/__init__.py` — Expression evaluator core
3. `opteryx/expression/operations/comparisons.py` — Comparison operators
4. `opteryx/expression/operations/string_matching.py` — LIKE/RLIKE
5. `opteryx/expression/operations/list_ops.py` — IN operations
6. `opteryx/expression/unary_operations.py` — NOT / IS NULL
7. + 10 more in evaluator, arithmetic, type coercion, binary operators

### Opportunity for Consolidation
Many hot-path files are **thin wrappers** around PyArrow compute:
- They already dispatch based on type
- Direct mapping to Draken kernels possible
- One-to-one replacement in most cases

---

## Implementation Strategy

### Phase 1: CRITICAL (2-3 weeks)
**Target: Core filter and logical operations**

1. `opteryx/expression/operations/__init__.py`
   - Replace `numpy.logical_or()` → Draken BoolVector ops
   - Replace `numpy.place()` → Draken masking
   - Replace `pyarrow.nulls()` → Draken BoolVector.from_nulls()
   - **Impact:** Every WHERE clause

2. `opteryx/expression/operations/comparisons.py`
   - Migrate all comparisons to Draken kernels
   - Replace `pyarrow.compute.equal()` → `vector_ops.vector_equal_*()`
   - **Impact:** Every filter operation

3. `opteryx/expression/__init__.py` - LOGICAL_OPERATIONS
   - Replace `pyarrow.compute.and_/or_/xor` with Draken vector ops
   - **Impact:** Logical expressions in WHERE clauses

**Expected Outcome:** All basic filter operations run on Draken, 15-30% performance improvement

### Phase 2: HIGH (2-3 weeks)
**Target: String, list, and advanced operations**

4. String matching, list operations, temporal coercion
5. Type coercion (create Cython layer for safe conversions)
6. Arithmetic and evaluator dispatch

**Expected Outcome:** All expression evaluation on Draken, 30-50% performance improvement

### Phase 3: MEDIUM (1-2 weeks)
**Target: Function implementations**

7. Function kernels and registrars
8. Fastpath optimizations (dictionary/constant encoding)

**Expected Outcome:** Full migration complete, 50%+ performance improvement

### Phase 4: POLISH (0-1 week)
- Consolidate utilities
- Clean up remaining imports
- Performance benchmarking

---

## Key Patterns

### Pattern 1: Replace numpy.logical_or()
```python
# BEFORE
null_positions = numpy.logical_or(left_nulls, right_nulls)

# AFTER
null_positions = left_nulls_vec.or_(right_nulls_vec)  # Draken BoolVector
```

### Pattern 2: Replace pyarrow.compute comparisons
```python
# BEFORE
result = pyarrow.compute.equal(arr, value)

# AFTER
result = vector_ops.vector_equal_int64(vector_from_arrow(arr), value)
```

### Pattern 3: Replace type casting
```python
# BEFORE
value = pa.array([x], type=pa.int64()).cast(pa.float64())[0].as_py()

# AFTER
# Cython: cdef inline double int_to_float(int64_t val): return <double>val
value = int_to_float(x)
```

### Pattern 4: Replace null array creation
```python
# BEFORE
result = pyarrow.nulls(size, type=pyarrow.bool_())

# AFTER
result = BoolVector.from_nulls(size)
```

---

## Success Criteria

✅ All 88 tests passing
✅ No numpy/pyarrow imports in:
  - opteryx/expression/__init__.py
  - opteryx/expression/operations/__init__.py
  - opteryx/expression/operations/comparisons.py
  - opteryx/expression/operations/string_matching.py
  - opteryx/expression/operations/list_ops.py
  - opteryx/expression/unary_operations.py

✅ Performance improvement (target: 30-50% on query benchmarks)
✅ No PyArrow in execution loop (only at boundaries)

---

## Risk Mitigation

| Risk | Mitigation |
|------|-----------|
| Breaking existing tests | Keep broad API compatibility, test thoroughly |
| Performance regression | Benchmark each phase, use `make clickbench` |
| Type coercion edge cases | Comprehensive unit tests for all type pairs |
| Temporal precision loss | Use direct bit arithmetic (days→microseconds) |

---

## Dependencies & Prerequisites

✅ **All infrastructure is ready:**
- Draken vectors implemented and working
- Vector kernels compiled (vector_ops.*)
- Interop layer exists (vector_from_arrow)
- 86/88 tests passing
- Recent CROSS JOIN work proved Draken viability

✅ **No new dependencies needed**
✅ **No architectural changes required**
✅ **Purely mechanical replacement of library calls**

---

## Recommended Next Steps

1. **Start with Phase 1, PR #1:** `opteryx/expression/operations/__init__.py`
   - Highest impact, smallest scope
   - Filters are most common operation
   - All test infrastructure already in place

2. **Benchmark before/after** with:
   - `make clickbench` for overall performance
   - Query logs showing filter operation counts
   - Memory usage comparison

3. **Keep Phase 2 ready** while Phase 1 is in review
   - Comparisons, string matching, list ops
   - Can proceed in parallel with minimal conflicts

4. **Consolidate** Phase 3 & 4 after core is done

---

## Resources

📄 **Detailed Analysis:**
- `docs/numpy-pyarrow-eradication-analysis.md` — Full breakdown by file

📊 **Tracking:**
- `docs/eradication-tracking-matrix.md` — Priority matrix and progress

🔧 **Implementation Guide:**
- `docs/eradication-patterns-and-examples.md` — Code patterns and examples

---

## Conclusion

**This is a high-impact, low-risk effort.** The infrastructure is complete, the patterns are clear, and the performance gains are significant. The main work is mechanical: replacing PyArrow/NumPy calls with Draken equivalents.

**Recommended timeline:** 4-6 weeks for full completion (8-10 PRs)

**Expected outcome:** 30-50% query performance improvement, full eradication of numpy/pyarrow from hot paths, cleaner, faster codebase aligned with Opteryx's architecture.

**Status:** Ready to begin Phase 1 implementation.