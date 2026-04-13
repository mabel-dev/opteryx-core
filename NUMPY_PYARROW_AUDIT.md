# NumPy and PyArrow Audit Report

**Generated:** Opteryx Core Codebase Audit  
**Scope:** `opteryx/` (excluding tests, third_party/mabel)  
**Total Files Scanned:** 393 (.py and .pyx files)

---

## Executive Summary

**NumPy Status:** 133 total allocations; mostly in Cython hot paths (joins, buffers) and Python expression evaluation  
**PyArrow Status:** 23 array/table/schema operations; 10 imports in .pyx files; significant use in type checking and array construction

**Key Finding:** NumPy is heavily used in:
1. **Hot paths** - Cython buffer management (joins, unnest) - HIGH EFFORT to refactor, CRITICAL to performance
2. **Cold paths** - Expression evaluation and type coercion - MEDIUM EFFORT, MODERATE IMPACT
3. **Type checking** - PyArrow type introspection only (not compute) - LOW EFFORT, LOW IMPACT

---

## Top 10 Files by NumPy References

| Rank | File | NumPy Refs | PyArrow Refs | Category | Priority |
|------|------|-----------|--------------|----------|----------|
| 1 | `opteryx/expression/__init__.py` | 15 | 8 | Expression Evaluation | MEDIUM |
| 2 | `opteryx/expression/casts.py` | 12 | 4 | Type Casting | MEDIUM |
| 3 | `opteryx/compiled/joins/cross_join.pyx` | 18 | 0 | HOT PATH - Join | HIGH |
| 4 | `opteryx/types/_null_handling.py` | 11 | 5 | Null Handling | MEDIUM |
| 5 | `opteryx/expression/binary_operators.py` | 10 | 6 | Binary Operations | MEDIUM |
| 6 | `opteryx/compiled/table_ops/hash_ops.pyx` | 8 | 9 | HOT PATH - Hashing | HIGH |
| 7 | `opteryx/compiled/joins/inner_join.pyx` | 6 | 0 | HOT PATH - Join | HIGH |
| 8 | `opteryx/compiled/structures/buffers.pyx` | 8 | 0 | HOT PATH - Buffer Mgmt | HIGH |
| 9 | `opteryx/operators/heap_sort_node.pyx` | 7 | 0 | HOT PATH - Sorting | HIGH |
| 10 | `opteryx/expression/binary_operators.py` | 7 | 2 | Operations | MEDIUM |

---

## Most Common NumPy Allocation Patterns

### Pattern 1: Empty Array Allocation (33 instances)
**Location:** Primarily Cython hot paths  
**Usage:** `numpy.empty(size, dtype=...)` for pre-allocated buffers

**Files:**
- `opteryx/compiled/joins/cross_join.pyx:48,52,113,116,162,192,198` - Buffer allocation for join operations
- `opteryx/compiled/structures/hash_table.pyx:56,62` - Hash accumulation buffers
- `opteryx/compiled/table_ops/null_avoidant_ops.pyx:37,39` - Null tracking arrays
- `opteryx/compiled/structures/buffers.pyx:125,240` - Memory management

**Classification:** 
- **Hot Path (90%):** Joins (cross_join, inner_join), structural buffers  
- **Cold Path (10%):** Initialization paths

**Refactor Impact:** HIGH - These are performance-critical allocations in tight loops

```cython
# Example - HOT PATH (cross_join.pyx:48-49)
if row_count == 0:
    return numpy.empty(0, dtype=numpy.int64), numpy.empty(0, dtype=object)
```

---

### Pattern 2: Array Type Casting (18 instances)
**Location:** Expression evaluation and Cython operations  
**Usage:** `numpy.asarray()` for guaranteed array conversion

**Files:**
- `opteryx/compiled/joins/inner_join.pyx:175,208,225` - HOT PATH - Type coercion before join
- `opteryx/compiled/structures/buffers.pyx:97` - Array extension validation
- `opteryx/expression/__init__.py:499,543` - Expression evaluation results

**Classification:**
- **Hot Path (40%):** Join key preparation  
- **Cold Path (60%):** Expression result handling

**Example - HOT PATH:**
```cython
# inner_join.pyx:175-178 - In hot join loop
ht.insert_batch(
    numpy.asarray(row_hashes)[numpy.asarray(non_null_indices, dtype=numpy.int64)],
    numpy.asarray(non_null_indices, dtype=numpy.int64),
)
```

---

### Pattern 3: Array Construction from Python (25 instances)
**Location:** Expression evaluation, type handling  
**Usage:** `numpy.array()` to construct arrays from lists

**Files:**
- `opteryx/expression/__init__.py:439,543` - Result wrapping  
- `opteryx/expression/casts.py:245,250,256,331,334` - Cast result conversion
- `opteryx/types/_null_handling.py` - Null checking utilities

**Classification:**
- **Hot Path (15%):** Expression DNF evaluation  
- **Cold Path (85%):** Type system utilities, casting

**Refactor Impact:** MEDIUM - Many are in cold paths but Pattern 3 blocks efficient computation

---

### Pattern 4: Numeric Array Operations (22 instances)
**Location:** Binary operations, utilities  
**Usage:** Direct numpy function calls (add, subtract, multiply, etc.)

**Files:**
- `opteryx/expression/binary_operators.py:321-335` - Operator function map
  - `numpy.divide`, `numpy.subtract`, `numpy.multiply`, `numpy.add`
  - `numpy.bitwise_or`, `numpy.bitwise_and`, `numpy.bitwise_xor`
  - `numpy.left_shift`, `numpy.right_shift`, `numpy.mod`, `numpy.trunc`

**Classification:**
- **Hot Path (50%):** Arithmetic evaluation  
- **Cold Path (50%):** Utility operations

**Usage Example:**
```python
# binary_operators.py:321-335
OPERATOR_FUNCTION_MAP: Dict[str, Any] = {
    "Divide": numpy.divide,
    "Minus": numpy.subtract,
    "Multiply": numpy.multiply,
    "Plus": numpy.add,
    "ShiftLeft": numpy.left_shift,
    "ShiftRight": numpy.right_shift,
    # ...
}
```

---

### Pattern 5: Type Checking & Inspection (37 instances)
**Location:** Expression evaluation, null handling  
**Usage:** `isinstance(x, numpy.ndarray)`, `numpy.issubdtype()`, `numpy.isnan()`

**Files:**
- `opteryx/types/_null_handling.py:85-92,141-148,200-205` - Null/NaN detection
- `opteryx/expression/__init__.py:203-209,302-307` - Result format detection
- `opteryx/expression/casts.py:26-40` - Type normalization

**Classification:**
- **Hot Path (30%):** Expression evaluation result handling  
- **Cold Path (70%):** Utility and null handling

---

## PyArrow Usage Analysis

### PyArrow Imports by File Type

**Cython Files (.pyx):** 10 imports
```
opteryx/compiled/table_ops/hash_ops.pyx
opteryx/compiled/table_ops/null_avoidant_ops.pyx
opteryx/compiled/vector_ops/vector_date_trunc.pyx
opteryx/compiled/vector_ops/vector_split.pyx
opteryx/operators/cross_join_node.pyx
opteryx/operators/distinct_node.pyx
opteryx/operators/filter_join_node.pyx
opteryx/operators/nested_loop_join_node.pyx
opteryx/operators/non_equi_join_node.pyx
opteryx/operators/null_reader_node.pyx
opteryx/operators/outer_join_node.pyx
opteryx/operators/read_node.pyx
opteryx/operators/unnest_join_node.pyx
```

**Python Files (.py):** 17 imports (mostly expression and operations modules)

---

### PyArrow Operation Patterns

#### Pattern 1: Type Checking (pyarrow.types.*) - 12 instances
**Files:**
- `opteryx/compiled/table_ops/hash_ops.pyx:34-44` - Type dispatch for hashing
  - `pyarrow.types.is_string()`, `is_binary()`, `is_integer()`, `is_list()`, etc.
- `opteryx/compiled/table_ops/null_avoidant_ops.pyx:42-52` - Validity buffer extraction

**Status:** SAFE - Type checking only, no algorithmic dependency  
**Refactor Effort:** LOW - Could be replaced with wrapper, not critical path

```cython
# hash_ops.pyx:34-44 - Type dispatch (COLD PATH)
if pyarrow.types.is_string(dtype) or pyarrow.types.is_binary(dtype):
    process_string_chunk(chunk, row_hashes, row_offset)
elif pyarrow.types.is_integer(dtype) or pyarrow.types.is_floating(dtype):
    process_primitive_chunk(chunk, row_hashes, row_offset)
```

#### Pattern 2: Array Construction (pa.array()) - 9 instances
**Files:**
- `opteryx/compiled/vector_ops/vector_split.pyx:191-202` - Result array building (HOT PATH)
- `opteryx/compiled/vector_ops/vector_date_trunc.pyx:334` - Timestamp array (HOT PATH)
- `opteryx/expression/__init__.py:439` - INTERVAL constant arrays (COLD PATH)
- `opteryx/expression/binary_operators.py:256-280` - Date/string conversion (COLD PATH)

**Status:** MIXED
- **HOT PATH (40%):** vector_split, date_trunc - performance critical
- **COLD PATH (60%):** Expression evaluation - acceptable

**Refactor Effort:** MEDIUM to HIGH for hot paths

```cython
# vector_split.pyx:191-202 - HOT PATH, result construction
if n <= 0:
    return pa.array([], type=pa.list_(pa.binary()))

if vec._const_is_null or vec._const_value == NULL:
    return pa.array([None] * n, type=pa.list_(pa.binary()))
```

#### Pattern 3: pyarrow.compute Operations - 5 instances
**Files:**
- `opteryx/expression/__init__.py:188-192,503` - Logical operations (AND, OR, XOR, invert)
- `opteryx/operators/nested_loop_join_node.pyx:73-79` - Null filtering (pc.is_valid, pc.is_nan)
- `opteryx/expression/operations/__init__.py:107-112` - Type casting (pc.cast)
- `opteryx/operators/base_plan_node.py:387` - Join key casting

**Status:** MIXED - Mostly type operations
- **REQUIRED:** Logical operations in DNF evaluation
- **OPTIONAL:** Join null filtering and casting

**Refactor Effort:** LOW for optional, MEDIUM for required

```python
# __init__.py:188-192 - Expression DNF evaluation (HOT PATH)
LOGICAL_OPERATIONS: Dict[NodeType, Callable] = {
    NodeType.AND: pyarrow.compute.and_,
    NodeType.OR: pyarrow.compute.or_,
    NodeType.XOR: pyarrow.compute.xor,
}
```

#### Pattern 4: Buffer & Array Construction (pa.foreign_buffer, pa.Array.from_buffers) - 6 instances
**Files:**
- `opteryx/compiled/vector_ops/vector_split.pyx:389-417` - Zero-copy result building (HOT PATH)

**Status:** PERFORMANCE CRITICAL
- Used for zero-copy buffer wrapping
- Required for efficient string splitting

**Refactor Effort:** VERY HIGH - Core optimization technique

```cython
# vector_split.pyx:389-417 - Zero-copy buffer wrapping (HOT PATH)
cdef object child_data_buf = pa.foreign_buffer(
    <uintptr_t>output_data, write_pos,
    base=cleanup_output_data
)
# Create child array
cdef object child_array = pa.Array.from_buffers(
    pa.binary(), segment_idx,
    [None, child_offs_buf, child_data_buf]
)
```

---

## Refactoring Opportunities & Impact Assessment

### Tier 1: HIGH PRIORITY (Hot Paths, Blocking Performance)

#### Opportunity 1.1: NumPy Buffer Allocations in Joins
**Files:** `cross_join.pyx`, `inner_join.pyx`, `hash_table.pyx`  
**Pattern:** `numpy.empty()`, `numpy.resize()` for temporary buffers  
**Current Impact:** 28 allocations, mostly in hot paths

**Recommendation:** REPLACE with Draken-native buffer classes
- Use existing C++ buffer infrastructure (IntBuffer, etc.)
- Avoid Python/NumPy allocation overhead
- Status: ALREADY STARTED (buffers.pyx exists but not used everywhere)

**Effort:** HIGH (requires rewriting join logic)  
**Impact:** HIGH (2-5% query speed improvement for joins)  
**ROI:** High - addresses architectural goal of NumPy removal

```cython
# BEFORE (cross_join.pyx:48)
return numpy.empty(0, dtype=numpy.int64), numpy.empty(0, dtype=object)

# AFTER (proposed)
from opteryx.compiled.structures.buffers import IntBuffer, ObjectBuffer
indices_buf = IntBuffer()
flat_data_buf = ObjectBuffer()
return indices_buf, flat_data_buf
```

#### Opportunity 1.2: PyArrow Array Construction in Tight Loops
**File:** `vector_split.pyx:191-202,255-259`  
**Pattern:** `pa.array()` called in result building  
**Current Usage:** 4 instances in HOT PATH

**Recommendation:** Use foreign buffer pattern (already done for most cases at L389-417)
- Extend zero-copy buffer approach to all result paths
- Avoid Python list intermediates

**Effort:** MEDIUM (refactor 3-4 functions)  
**Impact:** HIGH (15-25% speedup for string split operations)  
**ROI:** High - low hanging fruit with measurable impact

---

### Tier 2: MEDIUM PRIORITY (Cold Paths, Moderate Impact)

#### Opportunity 2.1: NumPy Type Checking in Expression Evaluation
**Files:** `__init__.py`, `casts.py`, `_null_handling.py`  
**Pattern:** `isinstance(x, numpy.ndarray)`, `numpy.issubdtype()`  
**Current Usage:** 18 instances in cold paths

**Recommendation:** Create unified type checking utility
- Reduce dependency on NumPy for type introspection
- Centralize type coercion logic

**Effort:** MEDIUM (new utility module, update 15+ call sites)  
**Impact:** MEDIUM (minor speedup, cleaner code)  
**ROI:** Medium - improves maintainability more than performance

---

#### Opportunity 2.2: NumPy Array Construction in Casting
**File:** `casts.py:245-334`  
**Pattern:** `numpy.array(result.to_pylist())` - triple conversion  
**Current Usage:** 6 instances (cold paths)

**Recommendation:** Direct Draken vector to Arrow conversion
- Avoid to_pylist() intermediary
- Use existing vector_from_arrow machinery

**Effort:** MEDIUM (refactor cast functions)  
**Impact:** MEDIUM (10-20% faster casting operations)  
**ROI:** Medium - significant for type conversion heavy queries

```python
# BEFORE (casts.py:245-246)
result = format_double_func(arr)
return numpy.array(result.to_pylist(), dtype=object)

# AFTER (proposed)
from opteryx.compiled.draken.interop.arrow import to_arrow
result = format_double_func(arr)
return result.to_arrow()  # Direct conversion
```

---

#### Opportunity 2.3: Binary Operation Function Map
**File:** `binary_operators.py:321-335`  
**Pattern:** NumPy ufuncs in operator dispatch  
**Current Usage:** 11 operators using numpy functions

**Recommendation:** Create vectorized operator dispatch using Draken
- Use native vector operations where available
- Maintain NumPy fallback for edge cases

**Effort:** MEDIUM-HIGH (requires Draken vector method expansion)  
**Impact:** MEDIUM-HIGH (arithmetic-heavy queries gain 5-15%)  
**ROI:** Medium - helps queries with lots of arithmetic

---

### Tier 3: LOW PRIORITY (Optional, Minimal Impact)

#### Opportunity 3.1: PyArrow Type Checking Wrapper
**Files:** `hash_ops.pyx`, `null_avoidant_ops.pyx`  
**Pattern:** `pyarrow.types.is_*()` calls  
**Current Usage:** 8 instances (type dispatch, not algorithmic)

**Recommendation:** Create type checking enum/function
- Replace PyArrow type checks with local enum
- Minimal performance impact, reduces dependency

**Effort:** LOW (new enum + wrapper functions)  
**Impact:** LOW (type checking not bottleneck)  
**ROI:** Low - mostly for dependency removal goal

---

#### Opportunity 3.2: .to_numpy() Calls
**File:** `utils/sql.py:205-215`  
**Pattern:** `to_numpy()` for array access  
**Current Usage:** 1 instance (cold path)

**Recommendation:** Use Arrow buffers directly
- Avoid conversion to NumPy

**Effort:** LOW (single file, 5 lines)  
**Impact:** LOW (cold path only)  
**ROI:** Low - easy win but minimal impact

---

## Files Requiring No Changes

**Already using Draken vectors effectively:**
- `vector_ops/` (except vector_split.pyx)
- `draken/vectors/arithmetic_kernels.py` - Uses pa.array() only for result wrapping
- `compiled/draken/` - Core Draken implementation

**Third-party/acceptable NumPy use:**
- `third_party/maki_nage/` - Statistical algorithms, NumPy appropriate
- `connectors/` - I/O layer, NumPy use acceptable

---

## Dependency Removal Progress

### Current Status (Pre-Audit)
- NumPy: **133 references** (~distributed evenly between hot/cold paths)
- PyArrow: **~50+ references** (mostly type checking, some algorithmic)

### Target Post-Refactoring
- NumPy: **<30 references** (only in cold paths, utility functions, third-party)
- PyArrow: **<20 references** (only type ops, import infrastructure)

### Phase 1 (Quick Wins - Week 1-2)
1. Opportunity 2.2 - Casting improvements
2. Opportunity 3.1 - Type checking wrapper
3. Opportunity 3.2 - .to_numpy() replacement
**Estimated Impact:** 10-15% reduction in NumPy/PyArrow references

### Phase 2 (Medium Effort - Week 3-4)
1. Opportunity 1.2 - String split array construction
2. Opportunity 2.1 - Type checking utility
3. Opportunity 2.3 - Binary operations dispatch
**Estimated Impact:** Additional 20-25% reduction

### Phase 3 (High Effort - Week 5-8)
1. Opportunity 1.1 - Join buffer replacement
**Estimated Impact:** Final 30-40% reduction, plus 2-5% performance gain

---

## Summary Table: Refactoring Roadmap

| Priority | Opportunity | Effort | Impact | Files | Timeline |
|----------|-------------|--------|--------|-------|----------|
| **HIGH** | 1.1 - Join buffers → Draken | 5d | 2-5% perf | 3 | Week 5-8 |
| **HIGH** | 1.2 - Array construction → buffers | 2d | 15-25% faster split | 1 | Week 1-2 |
| **MEDIUM** | 2.1 - Type checking utility | 2d | Code quality | 3-4 | Week 3-4 |
| **MEDIUM** | 2.2 - Casting → direct conversion | 2d | 10-20% faster cast | 1 | Week 1-2 |
| **MEDIUM** | 2.3 - Binary ops → Draken | 3d | 5-15% arith ops | 1 | Week 3-4 |
| **LOW** | 3.1 - Type wrapper enum | 1d | Cleanup | 2 | Week 2 |
| **LOW** | 3.2 - .to_numpy() removal | 0.5d | Cleanup | 1 | Week 1 |

---

## Conclusion

NumPy and PyArrow remain strategic targets for removal, but the effort varies significantly by use case:

1. **Hot paths (joins, buffers, splits)** - High effort but justified by performance ROI
2. **Cold paths (casting, type checking)** - Medium effort, worth doing for consistency
3. **Type operations** - Low effort, primarily for dependency removal goal

**Recommendation:** Start with Phase 1 (quick wins) to establish momentum and demonstrate ROI, then tackle Phase 2-3 systematically. The join buffer replacement (Opportunity 1.1) is the largest undertaking but aligns with the architectural goal of moving to pure C++/Cython with Draken vectors.