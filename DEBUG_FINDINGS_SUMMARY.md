# WHERE Clause Filter Failure - Root Cause Analysis

## Summary

The WHERE clause is failing with `AttributeError: IntegerVector has no equals method` because:

1. **IntegerVector is being returned instead of Int64Vector** from `morsel.column()` during query execution
2. **IntegerVector lacks comparison methods** (equals, less_than, greater_than, etc.)
3. **Vectors contain all None values** instead of actual data
4. This occurs AFTER vectors leave the VirtualDataTable source but BEFORE they reach the FilterNode

## Key Findings

### 1. Vector Type Differentiation

**Int64Vector** (CORRECT for OrsoTypes.INTEGER):
- Created by `Vector.from_arrow(int64_arrow_array)`
- Has comparison methods: `equals()`, `less_than()`, `greater_than()`, etc.
- Used for 64-bit integers
- Created successfully by `vector_from_sequence([1,2,3], dtype=OrsoTypes.INTEGER)`

**IntegerVector** (INCORRECT for OrsoTypes.INTEGER):
- Created by `Vector.from_arrow(int32_arrow_array)` or `Vector.from_arrow(int8/int16_arrow_array)`
- NO comparison methods - only `min()`, `max()`, `sum()`, `take()`, `to_pylist()`
- Used for 8/16/32-bit integers
- Returns all None values when vectors have data pointers that are misaligned

### 2. The Execution Pipeline Problem

```
VirtualDataTable.read_dataset()
  ↓ yields Morsel with Int64Vector (CORRECT ✓)
  ↓
[OPERATOR BETWEEN SOURCE AND FILTER - VECTOR TYPE CHANGES HERE]
  ↓
FilterNode receives Morsel with IntegerVector (WRONG ✗)
  ↓
FilterNode calls evaluate_draken()
  ↓
draken_compare() calls _int64_compare()
  ↓
_int64_compare() calls vec.greater_than(5)
  ↓
AttributeError: 'IntegerVector' object has no attribute 'greater_than'
```

### 3. Data Integrity Issue

When IntegerVector is returned, the data shows as all None:
```python
id_vec = morsel.column(b"id")
print(id_vec.to_pylist())  # [None, None, None, None, None]
```

But the same morsel directly from planet_data.read() shows correct data:
```python
morsel = planet_data.read()
id_vec = morsel.column(b"id")
print(id_vec.to_pylist())  # [1, 2, 3, 4, 5, 6, 7, 8, 9]
```

This indicates a pointer/buffer alignment issue when IntegerVector is created.

### 4. Arrow Type Detection

The Vector.from_arrow() method routes based on Arrow type:
- `pa.int64()` → Int64Vector ✓
- `pa.int32()` → IntegerVector ✗
- `pa.int16()` → IntegerVector ✗
- `pa.int8()` → IntegerVector ✗

This is correct for **physical** data types but incorrect for **logical** types where OrsoTypes.INTEGER should always map to Int64Vector for consistency.

### 5. Schema Type Mismatch

The schema defines:
```python
FlatColumn(name="id", type=OrsoTypes.INTEGER)
```

But somewhere in the pipeline:
- OrsoTypes.INTEGER (logical type) is being converted to int32 (physical type)
- This causes Vector.from_arrow() to create IntegerVector instead of Int64Vector
- The physical int32 representation doesn't match the int64 data layout, causing None values

## Hypothesis on Root Cause

There is likely an operator (possibly a projection, schema validation, or type coercion step) that:

1. Converts the Morsel to Arrow table via `to_arrow()`
2. Uses the schema type `OrsoTypes.INTEGER` 
3. Casts or reinterprets the Arrow int64 columns to int32
4. Converts back via `Morsel.from_arrow()`
5. `Vector.from_arrow(int32_array)` creates IntegerVector
6. IntegerVector tries to read int64 buffer as int32, resulting in None values and pointer misalignment

## Files Involved

- `opteryx/expression/evaluator/comparisons.py` - Lines 49-94 (`_int64_compare`)
- `opteryx/expression/evaluator/evaluation.py` - Lines 260-295 (`_eval_value`)
- `opteryx/compiled/draken/vectors/integer_vector.pyx` - Lacks comparison methods
- `opteryx/compiled/draken/vectors/int64_vector.pyx` - Has all comparison methods
- `third_party/mabel/draken/interop/arrow.pyx` - `vector_from_arrow()` routing
- `opteryx/managers/virtual_datasets/planet_data.py` - Creates vectors with `OrsoTypes.INTEGER`

## Recommended Solutions

### Option A: Fix IntegerVector to have comparison methods ✓ BEST
Add `equals()`, `less_than()`, `greater_than()`, etc. methods to IntegerVector that handle 8/16/32-bit integer comparison.

**Pros:**
- Fixes the AttributeError immediately
- IntegerVector could legitimately be used for OrsoTypes.INTEGER columns
- Consistent with Int64Vector interface

**Cons:**
- Requires implementation of comparison logic in IntegerVector
- May not fix the None value issue

### Option B: Ensure OrsoTypes.INTEGER always uses Int64Vector
Modify `vector_from_arrow()` to always create Int64Vector for OrsoTypes.INTEGER, never IntegerVector.

**Pros:**
- Consistent type handling
- Fixes both AttributeError and None value issues
- Aligns logical type with physical representation

**Cons:**
- May waste memory for small integers
- Requires finding and fixing the type coercion happening upstream

### Option C: Fix the type coercion bug upstream
Find the operator that's converting int64→int32 and fix it at the source.

**Pros:**
- Fixes the root cause
- Most architecturally clean

**Cons:**
- Requires identifying exactly which operator is doing the conversion
- May be complex to fix

### Option D: Prevent IntegerVector from being used in filter expressions
Add validation to reject IntegerVector in `draken_compare()` and force conversion to Int64Vector.

**Pros:**
- Defensive programming
- Prevents crashes

**Cons:**
- Doesn't fix the underlying issue
- Creates runtime overhead

## Next Steps

1. **Identify the operator** that's converting vectors between VirtualDataTable and FilterNode
2. **Trace the Arrow conversion** to see where int32 is being introduced
3. **Check schema type handling** in projection and type coercion code
4. **Implement Option A or B** based on architectural preferences
5. **Run test suite** to verify fix doesn't break other queries
6. **Add test case** specifically for WHERE on $planets integer columns