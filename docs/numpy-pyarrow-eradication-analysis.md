# NumPy & PyArrow Eradication Analysis

**Status Date:** Current (86/88 tests passing)  
**Objective:** Identify and prioritize remaining NumPy/PyArrow usage in performance-critical code paths

---

## Executive Summary

| Category | Count | Priority |
|----------|-------|----------|
| **HOT path** (query execution loop) | ~16 files | 🔴 CRITICAL |
| **WARM path** (boundaries, column ops) | ~21 files | 🟡 MEDIUM |
| **COLD path** (init, schema, planning) | ~20 files | 🟢 LOW |
| **Dev/Third-party** | ~11 files | ✅ ACCEPTABLE |

**Key Finding:** PyArrow is deeply embedded in the expression evaluator (HOT path), while NumPy is primarily in utilities and functions (WARM/COLD).

---

## CRITICAL - HOT PATH TARGETS

These files contain code that runs in the query execution loop and should be **eradicated first**:

### 1. **opteryx/expression/operations/__init__.py** 🔴 CRITICAL
- **Severity:** HIGHEST - Core filter operation dispatch
- **Imports:** `numpy`, `pyarrow`
- **Hot Usage:**
  ```python
  # Line 100-130: filter_operations() dispatch
  null_positions = numpy.logical_or(left_null_positions, right_null_positions)
  numpy.place(full_result, valid_positions, results_mask)
  pyarrow.nulls(morsel_size, type=pyarrow.bool_())
  ```
- **Impact:** Called for every comparison/filter operation in execution loop
- **Eradication Path:** Replace `numpy.logical_or()` with Draken vector ops, use Cython for null handling

### 2. **opteryx/expression/__init__.py** 🔴 CRITICAL
- **Severity:** HIGHEST - Main expression evaluator
- **Imports:** `numpy`, `pyarrow`, `compute`
- **Hot Usage:**
  ```python
  # Line 188-192: LOGICAL_OPERATIONS dispatch (AND/OR/XOR)
  LOGICAL_OPERATIONS: Dict[NodeType, Callable] = {
      NodeType.AND: pyarrow.compute.and_,
      NodeType.OR: pyarrow.compute.or_,
      NodeType.XOR: pyarrow.compute.xor,
  }
  # Line 423: Column access
  return table[identity].to_numpy(False)
  # Line 204-209: Result conversion
  elif isinstance(result, numpy.ndarray):
      result_bool = result.tolist()
  ```
- **Impact:** Runs for every expression evaluation
- **Eradication Path:** Use Draken vector ops for logical operations, avoid `.to_numpy()` calls

### 3. **opteryx/expression/operations/type_coercion.py** 🔴 CRITICAL
- **Severity:** HIGH - Hot path for type conversions
- **Imports:** `numpy`, `pyarrow`
- **Hot Usage:** Temporal type coercion in filter operations
- **Impact:** Called during every DATE/TIMESTAMP comparison
- **Eradication Path:** Implement type coercion directly in Cython

### 4. **opteryx/expression/ops.py** 🔴 CRITICAL
- **Severity:** HIGH - Legacy filter operations
- **Imports:** `numpy`, `pyarrow`, `compute`
- **Hot Usage:** Backup path for comparison operations
- **Impact:** Fallback for all comparison operations
- **Note:** 892 lines - may contain significant legacy code
- **Eradication Path:** Consolidate with comparisons.py, migrate to Draken

### 5. **opteryx/expression/evaluator/type_coercion.py** 🔴 CRITICAL
- **Severity:** HIGH - Type coercion in evaluator
- **Imports:** `numpy`, `pyarrow`
- **Hot Usage:** Scalar/vector type normalization during evaluation
- **Impact:** Called for parameter coercion in bounded functions
- **Eradication Path:** Use Draken's type system directly

### 6. **opteryx/expression/evaluator/arithmetic.py** 🟠 HIGH
- **Severity:** HIGH - Arithmetic expression evaluation
- **Imports:** `pyarrow`
- **Hot Usage:** Arithmetic operator dispatch
- **Impact:** Every arithmetic operation passes through here
- **Eradication Path:** Use Draken arithmetic kernels directly

### 7. **opteryx/expression/operations/comparisons.py** 🟠 HIGH
- **Severity:** HIGH - Core comparison operations
- **Imports:** `pyarrow`
- **Hot Usage:** Equality/inequality/ordering operations
- **Impact:** Every filter operation uses this
- **Eradication Path:** Consolidate with Draken comparison operators

### 8. **opteryx/expression/operations/string_matching.py** 🟠 HIGH
- **Severity:** HIGH - LIKE/RLIKE operations
- **Imports:** `pyarrow`
- **Hot Usage:** Pattern matching in filters
- **Impact:** Used in string predicates
- **Eradication Path:** Use compiled regex in Draken

### 9. **opteryx/expression/operations/list_ops.py** 🟠 HIGH
- **Severity:** HIGH - IN list operations
- **Imports:** `pyarrow`
- **Hot Usage:** IN/NOT IN filter operations
- **Impact:** Every IN clause passes through here
- **Eradication Path:** Use vector_in_list from compiled

### 10. **opteryx/expression/operations/fastpath_*.py** (3 files) 🟠 HIGH
- **Severity:** HIGH - Dictionary/constant encoding optimization
- **Imports:** `pyarrow`
- **Files:**
  - `fastpath_constant.py` - Constant vector encoding
  - `fastpath_dictionary.py` - Dictionary vector encoding
  - Both in hot path for encoded vectors
- **Impact:** These ARE Draken optimizations but still reference PyArrow
- **Eradication Path:** Wrap PyArrow interactions in thin layer, move to Cython

### 11. **opteryx/expression/binary_operators.py** 🟠 HIGH
- **Severity:** HIGH - Binary operator dispatch
- **Imports:** `numpy`, `pyarrow`
- **Hot Usage:** ArrowOp (JSON selector), other binary operations
- **Impact:** Called for every binary operation in expression tree
- **Eradication Path:** Consolidate JSON path handling, use Draken vectors

### 12. **opteryx/expression/unary_operations.py** 🟠 HIGH
- **Severity:** HIGH - Unary operations (NOT, IS NULL, etc.)
- **Imports:** `numpy`, `pyarrow`
- **Hot Usage:** NULL checking, logical NOT
- **Impact:** IS NULL is extremely common in WHERE clauses
- **Eradication Path:** Use Draken null handling directly

### 13. **opteryx/managers/execution/serial_engine.py** 🟠 MEDIUM-HIGH
- **Severity:** MEDIUM-HIGH - Result type handling
- **Imports:** `pyarrow`
- **Hot Usage:** Returns PyArrow tables to user
- **Impact:** Every query result passes through here
- **Note:** PyArrow here is at boundary (warm), not hot
- **Eradication Path:** Keep PyArrow (boundary), but minimize internal conversions

---

## HIGH-IMPACT WARM PATHS

These are boundaries or column-level operations - acceptable but should be reviewed:

### Function/Expression Implementations (7 files)
- `opteryx/expression/functions/implementations/arithmetic.py` - 🟡 MEDIUM
- `opteryx/expression/functions/implementations/logical.py` - 🟡 MEDIUM
- `opteryx/expression/functions/implementations/temporal.py` - 🟡 MEDIUM
- `opteryx/expression/functions/implementations/text.py` - 🟡 MEDIUM
- `opteryx/expression/functions/implementations/utility.py` - 🟡 MEDIUM
- `opteryx/expression/evaluator/function_execution.py` - 🟡 MEDIUM
- `opteryx/expression/evaluator/arithmetic_dispatch.py` - 🟡 MEDIUM

**Status:** Function implementations use PyArrow compute for now (acceptable warm path), but should migrate to Draken kernels when available.

### Function Registrars (4 files)
- `opteryx/expression/functions/registrar/arithmetic.py` - 🟡 MEDIUM
- `opteryx/expression/functions/registrar/constant.py` - 🟡 MEDIUM
- `opteryx/expression/functions/registrar/logical.py` - 🟡 MEDIUM
- `opteryx/expression/functions/registrar/utility.py` - 🟡 MEDIUM

**Status:** Registration/metadata layer, minimal runtime impact. `numpy.nanmax()` used in GREATEST function - acceptable.

### Model/Schema Layer (4 files)
- `opteryx/models/dataframe.py` - 🟡 WARM (minimal PyArrow)
- `opteryx/models/execution_context.py` - 🟡 WARM (schema only)
- `opteryx/types/schema.py` - 🟡 WARM (schema conversion)
- `opteryx/operators/base_plan_node.py` - 🟡 WARM (telemetry, type checking)

**Status:** Acceptable - used for schema definition and context, not in execution loop.

### Type Utilities (3 files)
- `opteryx/types/_null_handling.py` - 🟡 WARM (defensive imports)
- `opteryx/types/_scalar_to_vector.py` - 🟡 WARM (conversion boundary)
- `opteryx/vectors/embeddings.py` - 🟡 WARM (ML feature code)

**Status:** Acceptable - boundary/utility code.

### Special Operations (2 files)
- `opteryx/expression/operations/special_ops.py` - 🟡 WARM (JSON path)
- `opteryx/expression/operations/array_ops.py` - 🟡 WARM (array containment)

**Status:** Special case operations, acceptable warm path.

### Interval Handling
- `opteryx/expression/intervals.py` - 🟡 WARM (interval type operations)

**Status:** Temporal interval handling, acceptable.

### Utility Functions (6 files)
- `opteryx/utils/arrow.py` - 🟡 WARM (arrow helper functions)
- `opteryx/utils/arrow_interop.py` - 🟡 WARM (conversion utilities)
- `opteryx/utils/dates.py` - 🟡 WARM (date utilities)
- `opteryx/utils/parquet_decoder.py` - 🟡 WARM (parquet metadata)
- `opteryx/utils/sql.py` - 🟡 WARM (SQL parsing)
- `opteryx/utils/firestore_utils.py` - 🟡 WARM (Firestore serialization)

**Status:** Utility/initialization code, acceptable.

---

## COLD PATHS (Acceptable)

These are initialization, planning, or schema-related code - acceptable to keep PyArrow/NumPy:

### Planning Phase (5 files)
- `opteryx/planner/__init__.py`
- `opteryx/planner/ast_rewriter.py`
- `opteryx/planner/logical_planner/logical_planner_builders.py`
- `opteryx/planner/optimizer/strategies/statistics_only_response.py`
- `opteryx/query_session.py`

### Connectors (1 file)
- `opteryx/connectors/catalogs/local_catalog.py`

### Dev & Third-Party (11 files - ALL ACCEPTABLE)
- `dev/` - All utility scripts for development
- `opteryx/third_party/maki_nage/` - Distribution/histogram calculations (not performance path)
- `opteryx/compiled/draken/vectors/arithmetic_kernels.py` - Kernel initialization

---

## PRIORITIZED ERADICATION ROADMAP

### **Phase 1: CRITICAL (Blocks performance optimization)**
**Target: 2-3 PRs, high impact**

1. **opteryx/expression/operations/__init__.py** (HIGHEST PRIORITY)
   - Replace `numpy.logical_or()` with Draken vector ops
   - Replace `numpy.place()` with Draken masking
   - Replace `pyarrow.nulls()` with Draken null creation
   - **Impact:** Improves filter operation performance immediately

2. **opteryx/expression/operations/comparisons.py**
   - Migrate all comparison ops to Draken
   - Remove PyArrow compute dependency
   - **Impact:** Every WHERE clause benefits

3. **opteryx/expression/__init__.py** (LOGICAL_OPERATIONS)
   - Replace logical operation dispatch with Draken
   - Remove `.to_numpy()` calls
   - **Impact:** AND/OR/XOR in WHERE clauses

### **Phase 2: HIGH (Major hot paths)**
**Target: 3-4 PRs, significant impact**

4. **opteryx/expression/operations/string_matching.py**
   - LIKE/RLIKE to Draken regex
   - **Impact:** String predicates

5. **opteryx/expression/operations/list_ops.py**
   - IN/NOT IN using Draken vector_in_list
   - **Impact:** IN clauses

6. **opteryx/expression/unary_operations.py**
   - IS NULL to Draken native
   - NOT operations to Draken
   - **Impact:** Null checks are ubiquitous

7. **opteryx/expression/evaluator/type_coercion.py**
   - Type coercion to Cython
   - Parameter normalization without PyArrow
   - **Impact:** Function parameter handling

### **Phase 3: MEDIUM (Secondary hot paths)**
**Target: 2-3 PRs, moderate impact**

8. **opteryx/expression/binary_operators.py**
   - JSON path handling consolidation
   - Other binary ops to Draken

9. **opteryx/expression/ops.py** (if still needed)
   - Consolidate into primary comparison path
   - Or migrate remaining ops to Draken

10. **opteryx/expression/evaluator/arithmetic.py**
    - Arithmetic dispatch to Draken kernels

### **Phase 4: NICE-TO-HAVE (Warm paths, minimal impact)**
**Target: 1-2 PRs, polish**

11. Consolidate fastpath dictionaries/constants
12. Migrate function implementations to full Draken kernels
13. Clean up utility imports

---

## USAGE PATTERNS TO ERADICATE

### Pattern 1: NumPy logical operations in filters
```python
# CURRENT (BAD)
null_positions = compute.is_null(left_arr, nan_is_null=True)
right_null_positions = compute.is_null(right_arr, nan_is_null=True)
null_positions = numpy.logical_or(left_null_positions, right_null_positions)

# TARGET
# Use Draken OR operation directly
```

### Pattern 2: PyArrow compute for comparisons
```python
# CURRENT (BAD)
from pyarrow import compute
result = compute.equal(left, right)

# TARGET
# Use Draken vectors + comparison kernel
from opteryx.compiled.draken.vectors import ... 
result = draken_equal(left, right)
```

### Pattern 3: Type coercion via PyArrow
```python
# CURRENT (BAD)
import pyarrow as pa
value = pa.array([value], type=pa.int64()).cast(pa.float64())[0].as_py()

# TARGET
# Use Cython type conversion directly
# cdef double cast_to_float(int64_t value):
#     return <double>value
```

### Pattern 4: NULL creation
```python
# CURRENT (BAD)
return pyarrow.nulls(morsel_size, type=pyarrow.bool_())

# TARGET
# Use Draken BoolVector.from_nulls(size)
```

---

## KEY METRICS FOR SUCCESS

- ✅ All 88 tests passing
- ✅ No imports of `numpy` or `pyarrow` in hot-path files
- ✅ Expression evaluator operates entirely on Draken vectors
- ✅ Filter operations use compiled kernels
- ✅ Warm/cold paths isolated from execution loop

---

## RISKS & MITIGATION

| Risk | Mitigation |
|------|-----------|
| Breaking existing function tests | Migrate functions incrementally, keep broad API compat |
| Performance regression | Benchmark each phase, compare before/after |
| Increased Cython complexity | Use established patterns from Draken |
| Type coercion edge cases | Comprehensive unit tests for each type pair |

---

## NOTES FOR IMPLEMENTATION

1. **Draken Vectors are Ready:** Int64Vector, Float64Vector, BoolVector, StringVector all available
2. **Compiled Kernels Available:** vector_ops.* contains most needed operations
3. **Keep Boundaries:** PyArrow acceptable at query result boundary (serial_engine.py)
4. **Isolate Connectors:** Parquet/catalog reading can keep PyArrow (cold path)
5. **Testing:** Existing test suite validates compatibility

---

## FILES RECOMMENDED FOR IMMEDIATE ACTION

**Start Here (in order):**
1. `opteryx/expression/operations/__init__.py` - Filter operation dispatch
2. `opteryx/expression/operations/comparisons.py` - Core comparisons
3. `opteryx/expression/__init__.py` - Logical operations
4. `opteryx/expression/operations/string_matching.py` - String predicates
5. `opteryx/expression/unary_operations.py` - NULL handling

**Then:**
6. `opteryx/expression/operations/list_ops.py` - IN clauses
7. `opteryx/expression/evaluator/type_coercion.py` - Type conversion
8. `opteryx/expression/operations/fastpath_*.py` - Dictionary/constant optimizations

---

**Generated:** Current analysis reflects commit 59cb3637 status (86/88 tests, post-CROSS JOIN work)