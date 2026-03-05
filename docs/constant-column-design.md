# Constant Column Native Encoding Design

## Context

Constant columns—where every value (except possibly nulls) is identical—occur frequently in real workloads:

1. **Synthetic keys**: `SELECT 1 AS batch_id, col1, col2 FROM table`
2. **Materialized constants**: Subqueries projecting literals
3. **GROUP BY results**: Post-aggregation columns with repeated constants
4. **CASE expressions**: Branches that resolve to the same value

Current behavior materializes constant columns as full-width data (e.g., 1M copies of the value `42`). This wastes memory and decode bandwidth.

## Goals

1. Store constant columns with minimal memory footprint (single value + null map).
2. Decode constant columns in O(1) time (no iteration or decompression).
3. Fast-path constant columns in expressions, filters, and group-by kernels.
4. Enable planner optimizations that recognize and produce constant columns.

## Non-Goals (v1)

1. Constant-column-aware sort or distinct operations (v1 accepts materialization for these).
2. Mixed-type constant columns (all constants are of the declared column type).
3. Parquet native constant encoding (Parquet does not support this; will be created in-memory by planner/operators).

## Engine-Principle Constraints (Mandatory)

These constraints align this design with `docs/engine-principles.md`:

1. **Static dispatch, not dynamic**: Constant kernels use `switch (column.type)` at the top level; no runtime (is_constant ? constant_path : slow_path) checks in hot loops.
2. **Fail visibly**: If a constant buffer is malformed (null value pointer, length mismatch, incorrect null bitmap), reject it explicitly before execution.
3. **No Python in kernels**: Constant vector implementations use direct C++, not Python/Cython fallbacks.
4. **Arrow at boundaries only**: `to_arrow()` expands the constant into a full Arrow array (not a constant-encoded Arrow format); `vector_from_arrow()` recognizes trivial-predicate Arrow arrays and can opt into constant representation.

## Key Design

### 1) Constant Type Specification

#### Enum Value

Add to `third_party/mabel/draken/core/buffers.h`:

```c
enum DrakenType {
    // ... existing types ...
    DRAKEN_STRING     = 60,
    DRAKEN_DICTIONARY = 61,
    DRAKEN_CONSTANT   = 62,  // NEW
    // ... rest ...
};
```

#### Buffer Structure

```c
struct DrakenConstantBuffer {
    DrakenType type;              // DRAKEN_CONSTANT
    DrakenType value_type;        // type of the constant value (e.g., DRAKEN_INT32)
    void* value;                  // pointer to single value in heap
                                  // - For fixed-width types: raw bytes (int32*, float64*, etc.)
                                  // - For STRING: pointer to DrakenVarBuffer
    uint32_t length;              // number of logical rows
    uint8_t* null_bitmap;         // nullable: per-row bit map; nullptr if no nulls
};
```

#### Ownership & Lifetime

1. **Value ownership**: `DrakenConstantBuffer` owns the heap allocation for `value`:
   - For STRING: owns the child `DrakenVarBuffer*` (freed in destructor).
   - For fixed types: owns the heap-allocated scalar (freed in destructor).
2. **Null bitmap ownership**: Owned by the buffer; freed in destructor.
3. **Thread safety**: Read-only after construction, safe to share across threads without locking.

#### Null Semantics

1. `null_bitmap` is optional; `nullptr` means no nulls, all `length` rows are valid.
2. When `null_bitmap` is present, it follows the same bit-ordering as `DrakenFixedBuffer` (little-endian, bit 0 = first row).
3. If a row is marked null, the constant `value` is ignored; reading that row returns null.
4. **Example**: A constant column of `[42, 42, NULL, 42]` has `value = 42` and `null_bitmap` with bit 2 set.

### 2) Cython Vector Wrapper

Create `third_party/mabel/draken/vectors/constant_vector.pyx` (+ `.pxd`):

```cython
# constant_vector.pyx

cdef class ConstantVector(DrakenVector):
    """Read-only vector wrapping a constant value."""
    
    cdef DrakenConstantBuffer* _buffer
    cdef DrakenType _value_type
    
    def __init__(self, DrakenConstantBuffer* buffer):
        self._buffer = buffer
        self._value_type = buffer.value_type
        
    @property
    def length(self) -> uint32_t:
        return self._buffer.length
    
    @property
    def type(self) -> DrakenType:
        return DRAKEN_CONSTANT
    
    @property
    def value_type(self) -> DrakenType:
        return self._value_type
    
    def __getitem__(self, int64_t index):
        """Return the value at index, or None if null."""
        if self._is_null(index):
            return None
        return self._get_value()
    
    def to_pylist(self) -> list:
        """Expand to Python list."""
        value = self._get_value()
        null_bitmap = self._buffer.null_bitmap
        if null_bitmap == NULL:
            return [value] * self._buffer.length
        else:
            return [None if self._is_null(i) else value 
                    for i in range(self._buffer.length)]
    
    def to_arrow(self):
        """Convert to pyarrow array (expands to full width)."""
        cdef uint32_t length = self._buffer.length
        cdef list py_list = self.to_pylist()
        return pyarrow.array(py_list, type=arrow_type_from_draken(self._value_type))
    
    def take(self, indices: DrakenFixedBuffer or ConstantVector or ...) -> DrakenVector:
        """Return a new vector with rows selected by indices."""
        # If indices is also constant, result is constant.
        # Otherwise, expand to fixed buffer and take.
        # (v1: always materialize as fixed buffer; v2 optimize constant indices)
        ...
    
    def hash_into(self, output_buffer: DrakenFixedBuffer):
        """Hash the constant value `length` times into output buffer."""
        ...
    
    def compress_into(self, output_buffer: DrakenFixedBuffer):
        """Compress the constant into output buffer (e.g., for dictionary compression)."""
        ...
    
    # Predicates (these are instant)
    def equals(self, other):
        """Fast equality check."""
        if isinstance(other, ConstantVector):
            return self._buffer.value == other._buffer.value and \
                   self._buffers_equal(self._buffer.null_bitmap, other._buffer.null_bitmap)
        else:
            # Materialize self and compare
            return to_fixed_buffer().equals(other)
    
    cdef bint _is_null(self, uint32_t index):
        """Check if row is null."""
        if self._buffer.null_bitmap == NULL:
            return False
        return is_bit_set(self._buffer.null_bitmap, index)
    
    cdef object _get_value(self):
        """Extract the constant value as a Python object."""
        # Dispatch on self._value_type
        if self._value_type == DRAKEN_INT32:
            return (<int32_t*>self._buffer.value)[0]
        elif self._value_type == DRAKEN_STRING:
            return extract_draken_string(<DrakenVarBuffer*>self._buffer.value)
        # ... etc for all types ...
```

**Key Methods** (as per dictionary design):
- `__getitem__`, `to_pylist`, `to_arrow`, `take`, `hash_into`, `compress_into`
- `equals`, `not_equals`, `in_list` (predicates)
- Optional v2: `like`, `ilike` for STRING constants

### 3) Column Creation from Planner/Operators

#### In the Planner

Recognize constant expressions and tag them. Example:

```python
# In the planner's projection builder:
if expression_is_constant(expr):
    column = create_constant_column(
        value=evaluate_constant(expr),
        length=num_rows,
        nulls=None  # or a bitmap if expr can be null
    )
```

#### Operator Support

Operators that produce constants (or can detect them):

1. **ProjectionOperator**: Detects constant projections; creates `ConstantVector`.
2. **LimitOperator**: Can create a constant length column if needed.
3. **JoinOperator** (future): Recognize constant join keys.

### 4) Expression & Kernel Dispatch

#### In Expression Evaluator

When a column is constant, short-circuit evaluation:

```c
switch (left_column.type) {
    case DRAKEN_CONSTANT:
        // Fastest path: apply operation to single value, broadcast result
        return apply_predicate_constant(operation, 
                                        ((DrakenConstantBuffer*)left_column)->value,
                                        right_value);
    case DRAKEN_FIXED:
        // Standard path for fixed columns
        return apply_predicate_fixed(operation, left_column, right_value);
    // ... etc ...
}
```

#### Example: Filter with `col == 42` (col is constant 42)

1. Evaluate constant: `42 == 42 → true`
2. Result: Either all-true vector or all-false vector (depending on null bitmap).
3. Cost: O(1) instead of O(n) scan.

#### Example: GROUP BY a Constant

1. Planner recognizes constant column.
2. All rows belong to a single group.
3. GROUP BY reduces to a single aggregate row (no need for hash table).

### 5) Arrow Interop

#### `vector_from_arrow(arrow_array) -> DrakenVector`

When receiving an Arrow array:
- Check if it's trivial (constant value, possibly with nulls).
- If yes, create `ConstantVector`.
- Otherwise, create `FixedVector` or `DictionaryVector` as today.

**Heuristic**: An array is constant if all non-null values are identical.

```cython
def vector_from_arrow(arrow_array):
    if array_is_constant(arrow_array):
        return ConstantVector.from_arrow(arrow_array)
    else:
        # Fall through to existing logic
        ...
```

#### `ConstantVector.to_arrow() -> pyarrow.Array`

Expand the constant into a full `pyarrow.Array` for export.

```cython
def to_arrow(self):
    return pyarrow.array([self._get_value() if not self._is_null(i) 
                          else None for i in range(self.length)],
                         type=...)
```

---

## Implementation Plan

### Phase 1: Foundation (2–3 days)

**Milestone 1.1: C++ Buffer & Enum**
- [ ] Add `DRAKEN_CONSTANT` to `DrakenType` enum in `buffers.h`
- [ ] Define `DrakenConstantBuffer` struct
- [ ] Implement basic accessors (value getter, null bitmap check)
- [ ] Write unit tests for buffer lifecycle and null bitmap bit operations

**Milestone 1.2: Cython Vector Wrapper**
- [ ] Create `constant_vector.pyx` and `constant_vector.pxd`
- [ ] Implement `__init__`, `length`, `type`, `value_type` properties
- [ ] Implement `__getitem__` and `to_pylist`
- [ ] Implement `to_arrow`
- [ ] Unit tests: basic access, null handling, type correctness

### Phase 2: Operator Integration (2–3 days)

**Milestone 2.1: Planner Recognition**
- [ ] Add `is_constant_expression()` predicate to planner
- [ ] Add `create_constant_column()` helper in planner
- [ ] Update `ProjectionOperator` to emit `ConstantVector` for constant projections
- [ ] End-to-end test: `SELECT 42, 100, NULL LIMIT 1000` produces constant columns

**Milestone 2.2: Kernel Dispatch**
- [ ] Update expression evaluator to dispatch on `DRAKEN_CONSTANT` type
- [ ] Implement fast-path for constant predicates (`equals`, `in_list`, numeric comparisons)
- [ ] Unit tests: constant == constant, constant == fixed, constant IN (...)

### Phase 3: Expression & GROUP BY (2–3 days)

**Milestone 3.1: Predicate Kernels**
- [ ] Implement `equals`, `not_equals`, `in_list` fast-path methods
- [ ] Implement basic arithmetic kernels for constant numeric columns
- [ ] Tests: filter, arithmetic, null handling

**Milestone 3.2: GROUP BY Optimization**
- [ ] Detect constant group keys in planner
- [ ] If all group keys are constant, reduce to single-row aggregation
- [ ] Tests: `SELECT const_col, SUM(x) FROM table GROUP BY const_col`

### Phase 4: Arrow Interop & Optimization (1–2 days)

**Milestone 4.1: Arrow Import**
- [ ] Implement `array_is_constant(pyarrow.Array) -> bool`
- [ ] Implement `ConstantVector.from_arrow(pyarrow.Array)`
- [ ] Update `vector_from_arrow()` to recognize and wrap constant arrays
- [ ] Tests: import Arrow constant arrays, verify memory footprint

**Milestone 4.2: Take & Compression (optional v1)**
- [ ] Implement `take()` (v1: materialize; v2: optimize constant indices)
- [ ] Implement `hash_into()` for hash-based operations
- [ ] Tests: take operations on constant columns

---

## Test Approach

### Unit Tests

#### File: `tests/draken/test_constant_vector.py`

1. **Buffer Lifecycle**
   - Create with null bitmap, release, verify no leaks
   - Null bitmap bit operations correctness

2. **Vector Access**
   - `__getitem__` on valid rows
   - `__getitem__` on null rows returns None
   - `to_pylist()` correctness for constant + nulls
   - `length` property

3. **Type Dispatch**
   - INT32, INT64, FLOAT64, STRING constants
   - Mixed nulls and non-nulls

### Integration Tests

#### File: `tests/operators/test_constant_projection.py`

1. **Projection Operator**
   - `SELECT 42` produces constant column
   - `SELECT 42, 100, 'hello'` produces three constant columns
   - `SELECT col1, 42 FROM table` mixes regular and constant
   - Null constants: `SELECT NULL`

2. **Filter on Constants**
   - `WHERE 1 = 1` (always true)
   - `WHERE 1 = 0` (always false)
   - `WHERE NULL` (all nulls, no rows pass)
   - Mixed: `WHERE const_col = value`

3. **GROUP BY Constants**
   - `SELECT const_col, COUNT(*) FROM table GROUP BY const_col`
   - Single aggregation row produced
   - Multiple aggregate functions

### End-to-End Tests

#### File: `tests/e2e/test_constant_columns_e2e.py`

1. **Synthetic Workloads**
   - Batch ID column: `SELECT batch_id, col1 FROM (SELECT '2026-03' AS batch_id, * FROM data)`
   - Constant join key: `SELECT *, 'USA' AS country FROM orders`

2. **Memory Footprint**
   - Constant column with 1M rows: verify < 1KB overhead (vs. full column)
   - Null bitmap: 1M rows with 50% nulls = 62.5KB (vs. 4MB for int32 full)

3. **Query Performance**
   - Filter on constant: O(1) planning, O(1) predicate evaluation
   - GROUP BY constant: Single aggregation row

### Benchmark Tests

#### File: `tests/benchmarks/bench_constant_columns.py`

1. **Access Speed**
   - `__getitem__` on 1M-element constant vs. fixed vector
   - `to_pylist()` expansion cost

2. **Filter Speed**
   - `WHERE const_col = X` vs. `WHERE fixed_col = X` (should be instant)
   - `WHERE NULL` (should be instant false)

3. **GROUP BY Speed**
   - `GROUP BY constant` vs. `GROUP BY fixed` (should be 10–100x faster for single key)

---

## Rollout & Gating

### Phase 1–2 (MVP)

- Constant columns work end-to-end.
- Planner detects and creates them.
- Filters and basic expressions fast-path them.
- Arrow import recognizes trivial constant arrays.

### Phase 3+ (Optimization)

- GROUP BY constant automatic single-row reduction.
- Advanced kernel fast-paths (STRING LIKE, arithmetic).
- Compression and take optimizations.

### Feature Flag (Optional)

```python
# In query planner:
if config.ENABLE_CONSTANT_COLUMNS:
    detect_and_create_constant_columns()
```

---

## Risk & Mitigations

| Risk | Mitigation |
|------|-----------|
| Constant buffer lifetime bugs | Strict ownership; unit tests for leak detection; ASAN in CI |
| Null bitmap off-by-one errors | Bit-level tests; consistent implementation with `DrakenFixedBuffer` |
| Expression kernel dispatch complexity | Single `switch (type)` at top level; no nested if-else in hot loops |
| Arrow interop misses edge cases | Comprehensive import/export tests; fall back to non-constant on uncertainty |

---

## Success Criteria

1. ✅ Constant columns created and accessed with 100% correctness.
2. ✅ Memory footprint < 1KB for 1M-row constant (vs. 4–8MB for full column).
3. ✅ Filter on constant column instant (< 1µs for row-group decision).
4. ✅ GROUP BY constant column fast-pathed to single aggregation.
5. ✅ Arrow import recognizes trivial constant arrays.
6. ✅ No performance regression on non-constant columns.
