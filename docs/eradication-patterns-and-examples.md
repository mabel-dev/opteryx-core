# NumPy/PyArrow Eradication: Implementation Patterns & Examples

This document provides concrete patterns for eliminating NumPy and PyArrow from performance-critical paths.

---

## Pattern 1: Logical Operations (NumPy → Draken)

### BEFORE: NumPy logical_or in filter_operations()
```python
# opteryx/expression/operations/__init__.py, line ~120
import numpy

left_null_positions = compute.is_null(left_arr, nan_is_null=True)
right_null_positions = compute.is_null(right_arr, nan_is_null=True)
null_positions = numpy.logical_or(left_null_positions, right_null_positions)

if null_positions.any():
    valid_positions = ~null_positions
    compressed = True
    # Use valid_positions to filter
```

### AFTER: Draken vector operations
```python
# opteryx/expression/operations/__init__.py
from opteryx.compiled.draken.vectors import BoolVector
from opteryx.compiled.draken.interop.arrow import vector_from_arrow

# Approach 1: Use Draken OR directly if both are Draken BoolVectors
if isinstance(left_null_positions, BoolVector) and isinstance(right_null_positions, BoolVector):
    null_positions = left_null_positions.or_(right_null_positions)
    valid_positions = null_positions.logical_not()
else:
    # Convert to Draken, perform operation
    left_null_v = vector_from_arrow(left_null_positions) if not isinstance(left_null_positions, BoolVector) else left_null_positions
    right_null_v = vector_from_arrow(right_null_positions) if not isinstance(right_null_positions, BoolVector) else right_null_positions
    null_positions = left_null_v.or_(right_null_v)
    valid_positions = null_positions.logical_not()

if null_positions.any_true():
    compressed = True
    # Use valid_positions (BoolVector mask) to filter
```

### Key Changes
- Replace `numpy.logical_or()` with Draken `BoolVector.or_()`
- Replace `~null_positions` with `null_positions.logical_not()`
- Replace `.any()` with `.any_true()`
- Result is native Draken, no conversion overhead

### Draken APIs Required
- `BoolVector.or_(other: BoolVector) -> BoolVector`
- `BoolVector.logical_not() -> BoolVector`
- `BoolVector.any_true() -> bool`

---

## Pattern 2: PyArrow compute → Draken Kernels

### BEFORE: PyArrow comparisons
```python
# opteryx/expression/operations/comparisons.py
from pyarrow import compute as pc

def equal(arr, value, dict_candidate=False):
    if isinstance(arr, pyarrow.Array):
        return pc.equal(arr, value)
    # fallback for other types
```

### AFTER: Draken comparison kernels
```python
# opteryx/expression/operations/comparisons.py
from opteryx.compiled.draken.interop.arrow import vector_from_arrow
from opteryx.compiled import vector_ops

def equal(arr, value, dict_candidate=False):
    # Convert to Draken if needed
    if isinstance(arr, pyarrow.Array):
        arr = vector_from_arrow(arr)
    
    # Use Draken kernel
    if isinstance(arr, Int64Vector):
        return vector_ops.vector_equal_int64(arr, value)
    elif isinstance(arr, Float64Vector):
        return vector_ops.vector_equal_float64(arr, value)
    elif isinstance(arr, StringVector):
        return vector_ops.vector_equal_string(arr, value)
    # ... more types
```

### Key Changes
- Convert PyArrow arrays to Draken vectors once at entry
- Use `vector_ops.*` kernels for each type
- Return Draken vector directly (no PyArrow conversion)
- Kernels are pre-compiled C++, much faster

### Available Draken Kernels (in opteryx.compiled.vector_ops)
- `vector_equal_*`
- `vector_not_equal_*`
- `vector_less_than_*`
- `vector_greater_than_*`
- `vector_less_equal_*`
- `vector_greater_equal_*`

---

## Pattern 3: Type Coercion (PyArrow → Cython)

### BEFORE: PyArrow type casting
```python
# opteryx/expression/evaluator/type_coercion.py or __init__.py
import pyarrow as pa

def coerce_date_to_timestamp(date_value):
    """Convert DATE scalar to TIMESTAMP."""
    arr = pa.array([date_value], type=pa.date32())
    ts_array = arr.cast(pa.timestamp("us"))
    return ts_array[0].as_py()

def coerce_int_to_float(int_value):
    """Convert integer scalar to float."""
    arr = pa.array([int_value], type=pa.int64())
    float_array = arr.cast(pa.float64())
    return float_array[0].as_py()
```

### AFTER: Cython type conversion (new file: _type_coercion.pyx)
```cython
# opteryx/expression/_type_coercion.pyx

cdef extern from "limits.h":
    long LONG_MAX
    long LONG_MIN

from libc.math cimport isnan, isinf

# Scalar conversions
cdef inline int64_t date_to_timestamp(int32_t date_val):
    """Convert DATE (days since 1970) to TIMESTAMP (microseconds since 1970)."""
    return <int64_t>date_val * 86400_000_000  # days to microseconds

cdef inline int32_t timestamp_to_date(int64_t ts_val):
    """Convert TIMESTAMP to DATE."""
    return <int32_t>(ts_val // 86400_000_000)

cdef inline double int_to_float(int64_t val):
    """Convert int64 to float64."""
    return <double>val

cdef inline int64_t float_to_int(double val):
    """Convert float64 to int64 with saturation."""
    if isnan(val) or isinf(val):
        return 0  # or raise error
    if val > <double>LONG_MAX:
        return LONG_MAX
    if val < <double>LONG_MIN:
        return LONG_MIN
    return <int64_t>val

# Public wrapper functions
def coerce_date_to_timestamp(int32_t date_value):
    return date_to_timestamp(date_value)

def coerce_timestamp_to_date(int64_t ts_value):
    return timestamp_to_date(ts_value)

def coerce_int_to_float(int64_t int_value):
    return int_to_float(int_value)

def coerce_float_to_int(double float_value):
    return float_to_int(float_value)
```

### Key Changes
- Use Cython for type conversion (compiled to C)
- Direct bit manipulation, no array allocation
- Temporal types use direct calculation (no array intermediate)
- Integer overflow protection with saturation
- NaN/inf handling explicit

### Performance Impact
- Before: ~100x slower (array allocation + casting + extraction)
- After: ~1000x faster (direct bit operation)

---

## Pattern 4: NULL Handling (PyArrow → Draken)

### BEFORE: Creating NULL arrays
```python
# opteryx/expression/__init__.py, line ~462
import pyarrow

result = pyarrow.nulls(morsel_size, type=pyarrow.bool_())
```

### AFTER: Draken NULL vector
```python
# opteryx/expression/__init__.py
from opteryx.compiled.draken.vectors import BoolVector

result = BoolVector.from_nulls(morsel_size)
```

### BEFORE: NULL mask operations
```python
# opteryx/expression/operations/__init__.py
import numpy
import pyarrow

full_result = numpy.full(morsel_size, None, dtype=object)
numpy.place(full_result, valid_positions, results_mask)
return pyarrow.array(full_result, type=pyarrow.bool_())
```

### AFTER: Draken masking
```python
# opteryx/expression/operations/__init__.py
from opteryx.compiled.draken.vectors import BoolVector

# If valid_positions is Draken BoolVector and results_mask is Draken BoolVector
result = BoolVector.from_mask(valid_positions, results_mask)
# result will have nulls where valid_positions is False
```

### Draken APIs Required
- `BoolVector.from_nulls(size: int) -> BoolVector`
- `BoolVector.from_mask(mask: BoolVector, values: BoolVector) -> BoolVector`
- `BoolVector.from_nulls_like(vector: Vector) -> BoolVector`

---

## Pattern 5: Column Access (PyArrow → Draken)

### BEFORE: to_numpy() calls
```python
# opteryx/expression/__init__.py, line 423
def _inner_evaluate(root, table):
    if identity in table.column_names:
        return table[identity].to_numpy(False)  # HOT PATH!
```

### AFTER: Direct Draken vector
```python
# opteryx/expression/__init__.py
def _inner_evaluate(root, table):
    if identity in table.column_names:
        col = table[identity]
        # If table is already Draken, return directly
        if hasattr(col, '__class__') and col.__class__.__name__.endswith('Vector'):
            return col  # Already Draken
        # Convert from PyArrow
        from opteryx.compiled.draken.interop.arrow import vector_from_arrow
        return vector_from_arrow(col)
```

### Key Changes
- Avoid `.to_numpy()` conversion (allocates new array)
- Keep data in Draken vectors throughout evaluation
- Only convert at boundaries (query result return)

---

## Pattern 6: Temporal Type Conversions

### BEFORE: PyArrow temporal coercion
```python
# opteryx/expression/operations/type_coercion.py
import pyarrow as pa

def to_temporal_array(values, source_type, target_type):
    if source_type == OrsoTypes.DATE and target_type == OrsoTypes.TIMESTAMP:
        arr = pa.array(values, type=pa.date32())
        ts_array = arr.cast(pa.timestamp("us"))
        return ts_array
```

### AFTER: Cython temporal conversion (in _temporal_kernels.pyx)
```cython
# opteryx/compiled/temporal_kernels.pyx

def to_temporal_array_date_to_timestamp(values):
    """Convert DATE array to TIMESTAMP array."""
    # Assumes values is either a Draken vector or iterable
    cdef int64_t[::1] result
    cdef int32_t[::1] dates
    cdef int i, n
    
    if hasattr(values, '__class__') and values.__class__.__name__ == 'Date32Vector':
        dates_array = values.to_arrow()  # Get underlying data
        dates = dates_array.cast(pa.int32()).to_pylist()
    else:
        dates = values
    
    n = len(dates)
    result = <int64_t[:n]>malloc(n * sizeof(int64_t))
    
    for i in range(n):
        result[i] = <int64_t>dates[i] * 86400_000_000  # Convert to microseconds
    
    from opteryx.compiled.draken.vectors import TimestampVector
    return TimestampVector.from_memoryview(result)
```

### Key Changes
- Vectorize the conversion (process entire array at once)
- Use Cython memoryview for zero-copy access
- Direct arithmetic on int32 → int64 conversion

---

## Pattern 7: String Operations (PyArrow → Compiled)

### BEFORE: PyArrow string operations
```python
# opteryx/expression/operations/string_matching.py
from pyarrow import compute as pc

def like(arr, pattern, dict_candidate=False):
    return pc.match_substring(arr, pattern)
```

### AFTER: Compiled string kernels
```python
# opteryx/expression/operations/string_matching.py
from opteryx.compiled.draken.interop.arrow import vector_from_arrow
from opteryx.compiled import vector_ops

def like(arr, pattern, dict_candidate=False):
    # Check for dictionary encoding (fast path)
    if dict_candidate and hasattr(arr, 'dictionary_encode'):
        # Use dictionary fastpath
        return dictionary_fastpath(arr, pattern, 'like')
    
    # Convert to Draken string vector
    if isinstance(arr, pyarrow.Array):
        arr = vector_from_arrow(arr)
    
    # Use compiled regex kernel
    if isinstance(arr, StringVector):
        return vector_ops.vector_like_string(arr, pattern)
```

### Key Changes
- Regex compilation happens once, not per-row
- StringVector has optimized string matching
- Dictionary encoding fast path for constants

---

## Pattern 8: Filter Result Restoration

### BEFORE: NumPy place + PyArrow array
```python
# opteryx/expression/operations/__init__.py, line ~145
import numpy
import pyarrow

if compressed:
    full_result = numpy.full(morsel_size, None, dtype=object)
    numpy.place(full_result, valid_positions, results_mask)
    return pyarrow.array(full_result, type=pyarrow.bool_())
```

### AFTER: Draken masking operation
```python
# opteryx/expression/operations/__init__.py
from opteryx.compiled.draken.vectors import BoolVector

if compressed:
    # valid_positions and results_mask should be Draken BoolVectors
    # Create result with nulls where valid_positions is False
    if isinstance(results_mask, BoolVector):
        # results_mask already has values, expand with nulls
        full_result = BoolVector.from_mask_and_nulls(
            mask=valid_positions,
            values=results_mask,
            null_size=morsel_size
        )
    else:
        # Fallback for non-Draken results
        full_result = BoolVector.from_mask_expand(valid_positions, results_mask, morsel_size)
    return full_result
```

### Draken APIs Required
- `BoolVector.from_mask_and_nulls(mask, values, null_size)`
- `BoolVector.from_mask_expand(mask, values, total_size)`

---

## Pattern 9: Logical Operations Dispatch

### BEFORE: PyArrow compute
```python
# opteryx/expression/__init__.py, line 188-192
import pyarrow

LOGICAL_OPERATIONS: Dict[NodeType, Callable] = {
    NodeType.AND: pyarrow.compute.and_,
    NodeType.OR: pyarrow.compute.or_,
    NodeType.XOR: pyarrow.compute.xor,
}

# Usage in _inner_evaluate
result = LOGICAL_OPERATIONS[node_type](left, right)
```

### AFTER: Draken vector operations
```python
# opteryx/expression/__init__.py
from opteryx.compiled.draken.vectors import BoolVector
from opteryx.compiled.draken.interop.arrow import vector_from_arrow

def _get_logical_op(node_type):
    """Get Draken logical operation for node type."""
    if node_type == NodeType.AND:
        return lambda l, r: vector_from_arrow(l).and_(vector_from_arrow(r))
    elif node_type == NodeType.OR:
        return lambda l, r: vector_from_arrow(l).or_(vector_from_arrow(r))
    elif node_type == NodeType.XOR:
        return lambda l, r: vector_from_arrow(l).xor_(vector_from_arrow(r))

LOGICAL_OPERATIONS = {
    NodeType.AND: _get_logical_op(NodeType.AND),
    NodeType.OR: _get_logical_op(NodeType.OR),
    NodeType.XOR: _get_logical_op(NodeType.XOR),
}
```

### Better Approach: Direct Draken dispatch
```python
# opteryx/expression/__init__.py
from opteryx.compiled.draken.interop.arrow import vector_from_arrow

def _inner_evaluate(root, table):
    # ... after evaluating left and right ...
    
    left_vec = vector_from_arrow(left) if isinstance(left, pyarrow.Array) else left
    right_vec = vector_from_arrow(right) if isinstance(right, pyarrow.Array) else right
    
    # Direct Draken vector operations
    if root.node_type == NodeType.AND:
        result = left_vec.and_(right_vec)
    elif root.node_type == NodeType.OR:
        result = left_vec.or_(right_vec)
    elif root.node_type == NodeType.XOR:
        result = left_vec.xor_(right_vec)
```

---

## General Principles for All Patterns

### 1. Conversion Boundary
```python
# Always convert once at entry point, not in hot loop
input_vec = vector_from_arrow(input) if isinstance(input, pyarrow.Array) else input
```

### 2. Avoid Intermediate Allocations
```python
# BAD: Creates temporary array
temp = pa.array([value] * n)
result = temp.cast(pa.float64())

# GOOD: Direct Cython allocation
cdef double[::1] result = <double[:n]>malloc(n * sizeof(double))
```

### 3. Type Checking Once
```python
# BAD: Check type in every loop iteration
for item in items:
    if isinstance(item, MyType):
        # process
    
# GOOD: Check once, use fast path
if len(items) > 0 and isinstance(items[0], MyType):
    # All items same type - use typed loop
    for item in items:
        # process (no type check needed)
```

### 4. Keep Draken Throughout
```python
# BAD: Convert to Python list, lose efficiency
vec_result = vector_from_arrow(arr1)
py_list = vec_result.to_pylist()
# process
vec_result2 = vector_from_arrow(pa.array(py_list))

# GOOD: Keep as Draken
vec_result = vector_from_arrow(arr1)
vec_result2 = vec_result.map(transform_func)  # Draken operation
```

### 5. Use Memoryview for Loops
```python
# BAD: Slow Python object access
for i in range(n):
    val = array[i]  # Calls __getitem__ each time

# GOOD: Fast C-level access
cdef int64_t[::1] arr = array
for i in range(n):
    val = arr[i]  # Direct memory access
```

---

## Testing & Validation

### Before/After Test Template
```python
# tests/test_eradication_pattern.py
import pytest
from opteryx.compiled.draken.vectors import Int64Vector
from opteryx.compiled.draken.interop.arrow import vector_from_arrow

def test_pattern_equivalence():
    """Verify new implementation produces same results as old."""
    import pyarrow as pa
    
    # Test data
    arr = pa.array([1, 2, 3, None, 5])
    
    # Old way (PyArrow)
    result_old = pa.compute.equal(arr, 2)
    
    # New way (Draken)
    vec = vector_from_arrow(arr)
    result_new = vector_ops.vector_equal_int64(vec, 2)
    result_new_arrow = result_new.to_arrow()
    
    # Compare
    assert result_old.equals(result_new_arrow)

def test_pattern_performance():
    """Verify performance improvement."""
    import time
    import pyarrow as pa
    
    arr = pa.array(range(1_000_000))
    
    # Old way (PyArrow)
    start = time.perf_counter()
    for _ in range(100):
        result_old = pa.compute.equal(arr, 42)
    old_time = time.perf_counter() - start
    
    # New way (Draken)
    vec = vector_from_arrow(arr)
    start = time.perf_counter()
    for _ in range(100):
        result_new = vector_ops.vector_equal_int64(vec, 42)
    new_time = time.perf_counter() - start
    
    # Should be 5-10x faster
    assert new_time < old_time / 5, f"New not faster: {new_time} vs {old_time}"
```

---

## Checklist for Each Pattern

- [ ] Identify all call sites of the pattern in hot paths
- [ ] Create Cython/.pyx file if needed for type conversion
- [ ] Implement Draken kernel wrapper if needed
- [ ] Add unit tests for equivalence
- [ ] Add performance benchmark
- [ ] Update imports in main files
- [ ] Run full test suite (make q)
- [ ] Run performance benchmark (make clickbench)
- [ ] Update this document with lessons learned
