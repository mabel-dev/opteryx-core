# NumPy/PyArrow Eradication: File Reference

Quick lookup for all files involved in the eradication effort.

---

## PHASE 1: CRITICAL (Start Here)

### 1. opteryx/expression/operations/__init__.py
- **Severity:** 🔴 CRITICAL - Filter operation dispatch
- **Imports:** `numpy`, `pyarrow`
- **Key Functions:** `filter_operations()`, `_inner_filter_operations()`
- **NumPy Usage:** `numpy.logical_or()`, `numpy.place()`, `numpy.full()`, `numpy.empty()`
- **PyArrow Usage:** `pyarrow.nulls()`, `pyarrow.compute.cast()`, `pyarrow.compute.filter()`
- **Lines to Change:** ~100-150 (null handling), ~120-145 (filter restoration)
- **Replacement:** Use Draken BoolVector.or_(), from_mask(), from_nulls()
- **Test Coverage:** All filter operation tests

### 2. opteryx/expression/operations/comparisons.py
- **Severity:** 🔴 CRITICAL - Core comparisons
- **Imports:** `pyarrow`
- **Key Functions:** `equal()`, `not_equal()`, `less_than()`, `greater_than()`, etc.
- **PyArrow Usage:** `pyarrow.compute.equal()`, `compute.is_dictionary_encoded()`, etc.
- **Replacement:** Use vector_ops.vector_equal_*(), vector_not_equal_*(), etc.
- **Test Coverage:** All comparison operator tests

### 3. opteryx/expression/__init__.py
- **Severity:** 🔴 CRITICAL - Expression evaluator core
- **Imports:** `numpy`, `pyarrow`, `compute`
- **Key Functions:** `evaluate()`, `_inner_evaluate()`, `evaluate_and_append()`
- **NumPy Usage:** `isinstance(result, numpy.ndarray)`, `result.tolist()`
- **PyArrow Usage:** 
  - Line 188-192: `LOGICAL_OPERATIONS` dict with `pyarrow.compute.and_/or_/xor`
  - Line 423: `table[identity].to_numpy(False)` 
  - Line 437-441: Various `pyarrow.array()`, `pyarrow.timestamp()` calls
  - Line 462-479: `pyarrow.nulls()`, `pyarrow.Array` type checks
- **Lines to Change:** ~188-192 (logical ops), ~204-209 (result conversion), ~423 (column access), ~462-479 (null handling)
- **Replacement:** Draken vector operations throughout

---

## PHASE 2: HIGH

### 4. opteryx/expression/operations/string_matching.py
- **Severity:** 🟠 HIGH - LIKE/RLIKE operations
- **Imports:** `pyarrow`
- **Key Functions:** `like()`, `rlike()`, `ilike()`, `irlike()`
- **PyArrow Usage:** `pyarrow.compute.match_substring()`, type checking
- **Replacement:** Use vector_ops string matching kernels

### 5. opteryx/expression/operations/list_ops.py
- **Severity:** 🟠 HIGH - IN/NOT IN operations
- **Imports:** `pyarrow`
- **Key Functions:** `in_list()`, `not_in_list()`
- **PyArrow Usage:** `pyarrow.compute.*` operations
- **Replacement:** Use vector_ops.vector_in_list()

### 6. opteryx/expression/binary_operators.py
- **Severity:** 🟠 HIGH - Binary operator dispatch
- **Imports:** `numpy`, `pyarrow`
- **Key Functions:** `ArrowOp()` (JSON selector), other binary operations
- **NumPy Usage:** `numpy.ndarray` type checking, `.to_numpy()` calls
- **PyArrow Usage:** `pyarrow.array()` creation
- **Replacement:** Draken JSON path handling, vector creation

### 7. opteryx/expression/unary_operations.py
- **Severity:** 🟠 HIGH - NOT, IS NULL
- **Imports:** `numpy`, `pyarrow`
- **Key Functions:** `_is_null()`, `_is_not_null()`, `_logical_not()`
- **NumPy Usage:** `numpy.ndarray` type checking
- **PyArrow Usage:** `pyarrow.Array` checks, `pyarrow.compute.is_null()`
- **Replacement:** Draken native null handling

### 8. opteryx/expression/evaluator/type_coercion.py
- **Severity:** 🟠 HIGH - Type coercion in evaluator
- **Imports:** `numpy`, `pyarrow`
- **Key Functions:** Multiple coercion helper functions
- **Usage:** Parameter type normalization during function execution
- **Replacement:** Create Cython layer (_type_coercion.pyx)

### 9. opteryx/expression/evaluator/arithmetic.py
- **Severity:** 🟠 HIGH - Arithmetic evaluation
- **Imports:** `pyarrow`
- **Key Functions:** Arithmetic operator implementations
- **Replacement:** Draken arithmetic kernels

### 10. opteryx/expression/operations/fastpath_constant.py
- **Severity:** 🟠 HIGH - Constant encoding optimization
- **Imports:** `pyarrow`
- **Usage:** Hot path for constant-encoded vectors
- **Replacement:** Wrap PyArrow in thin Cython layer

### 11. opteryx/expression/operations/fastpath_dictionary.py
- **Severity:** 🟠 HIGH - Dictionary encoding optimization
- **Imports:** `pyarrow`
- **Usage:** Hot path for dictionary-encoded vectors
- **Replacement:** Wrap PyArrow in thin Cython layer

---

## PHASE 3: MEDIUM

### 12. opteryx/expression/evaluator/arithmetic_dispatch.py
- **Severity:** 🟡 MEDIUM - Arithmetic dispatch
- **Imports:** `pyarrow`
- **Usage:** Arithmetic type coercion

### 13. opteryx/expression/evaluator/function_execution.py
- **Severity:** 🟡 MEDIUM - Function execution
- **Imports:** `pyarrow`
- **Usage:** Parameter coercion for bounded functions

### 14. opteryx/expression/evaluator/comparisons.py
- **Severity:** 🟡 MEDIUM - Temporal comparison
- **Imports:** `pyarrow`
- **Usage:** Temporal type coercion in comparisons

### 15. opteryx/expression/evaluator/temporal_ops.py
- **Severity:** 🟡 MEDIUM - Temporal operations
- **Imports:** `pyarrow`
- **Usage:** Temporal type casting

### 16. opteryx/expression/operations/type_coercion.py
- **Severity:** 🟡 MEDIUM - Type coercion
- **Imports:** `numpy`, `pyarrow`
- **Key Function:** `to_temporal_array()`
- **Usage:** Temporal array coercion in filters

### 17. opteryx/expression/operations/array_ops.py
- **Severity:** 🟡 MEDIUM - Array operations
- **Imports:** `numpy`
- **Usage:** Array containment operations

### 18. opteryx/expression/operations/special_ops.py
- **Severity:** 🟡 MEDIUM - Special operations
- **Imports:** `pyarrow`
- **Usage:** JSON path operations

### 19. opteryx/expression/intervals.py
- **Severity:** 🟡 MEDIUM - Interval operations
- **Imports:** `pyarrow`
- **Usage:** Temporal interval handling

### 20. opteryx/expression/ops.py
- **Severity:** 🟡 MEDIUM - Legacy filter operations
- **Imports:** `numpy`, `pyarrow`
- **Size:** 892 lines - may consolidate into comparisons.py
- **Usage:** Backup comparison operations

---

## PHASE 4: WARM PATHS (Lower Priority)

### Function Implementations (5 files)
- `opteryx/expression/functions/implementations/arithmetic.py` - NumPy, PyArrow, compute
- `opteryx/expression/functions/implementations/logical.py` - NumPy, PyArrow
- `opteryx/expression/functions/implementations/temporal.py` - NumPy, PyArrow, compute
- `opteryx/expression/functions/implementations/text.py` - NumPy, PyArrow
- `opteryx/expression/functions/implementations/utility.py` - NumPy, PyArrow

### Function Registrars (4 files)
- `opteryx/expression/functions/registrar/arithmetic.py` - PyArrow compute
- `opteryx/expression/functions/registrar/constant.py` - NumPy (numpy.nanmax used)
- `opteryx/expression/functions/registrar/logical.py` - NumPy
- `opteryx/expression/functions/registrar/utility.py` - NumPy

---

## PHASE 5: COLD PATHS (ACCEPTABLE - No Action)

### Models & Schema
- `opteryx/models/dataframe.py` - PyArrow (schema)
- `opteryx/models/execution_context.py` - PyArrow (schema)
- `opteryx/types/schema.py` - PyArrow (schema conversion)
- `opteryx/operators/base_plan_node.py` - PyArrow (type checking)

### Query Execution (Boundary - Keep PyArrow)
- `opteryx/managers/execution/serial_engine.py` - PyArrow (ACCEPTABLE at boundary)
- `opteryx/__main__.py` - PyArrow (CLI output)

### Type Utilities
- `opteryx/types/_null_handling.py` - NumPy, PyArrow (defensive imports)
- `opteryx/types/_scalar_to_vector.py` - PyArrow (conversion boundary)
- `opteryx/vectors/embeddings.py` - NumPy (ML features)
- `opteryx/vectors/vector_types.py` - NumPy (type defs)

### Utilities
- `opteryx/utils/arrow.py` - PyArrow (helpers)
- `opteryx/utils/arrow_interop.py` - PyArrow (conversion)
- `opteryx/utils/dates.py` - NumPy, PyArrow, compute (date utilities)
- `opteryx/utils/parquet_decoder.py` - PyArrow (parquet metadata)
- `opteryx/utils/sql.py` - NumPy, PyArrow (SQL parsing)
- `opteryx/utils/firestore_utils.py` - NumPy (Firestore serialization)

### Planning & Connectors
- `opteryx/planner/__init__.py` - NumPy, PyArrow (planning)
- `opteryx/planner/ast_rewriter.py` - NumPy (AST rewriting)
- `opteryx/planner/logical_planner/logical_planner_builders.py` - NumPy
- `opteryx/planner/optimizer/strategies/statistics_only_response.py` - PyArrow
- `opteryx/query_session.py` - PyArrow (session init)
- `opteryx/connectors/catalogs/local_catalog.py` - PyArrow (parquet)
- `opteryx/compiled/draken/vectors/arithmetic_kernels.py` - PyArrow (kernel init)

---

## Third Party & Dev (ACCEPTABLE)

### Third Party
- `opteryx/third_party/maki_nage/distogram.py` - NumPy
- `opteryx/third_party/maki_nage/tests/_test_histogram.py` - NumPy
- `opteryx/third_party/maki_nage/tests/test_quantile.py` - NumPy
- `opteryx/third_party/maki_nage/tests/test_stats.py` - NumPy

### Dev Scripts (Not in production)
- `dev/analyze_function_costs.py`
- `dev/build-wheels.sh`
- `dev/compare_function_costs.py`
- `dev/data_generators.py`
- `dev/estimate_function_costs.py`
- `dev/estimate_operator_costs.py`
- `dev/generate_security_parquet_files.py`
- `dev/generate_test_parquet.py`
- `dev/load_gh_messages.py`
- `dev/load_gharchive.py`

---

## File Dependency Graph

```
PHASE 1 (Foundation)
├─ operations/__init__.py (filter_operations)
├─ operations/comparisons.py (comparison operators)
└─ __init__.py (LOGICAL_OPERATIONS)

PHASE 2 (Hot Paths)
├─ operations/string_matching.py
├─ operations/list_ops.py
├─ binary_operators.py
├─ unary_operations.py
├─ operations/fastpath_*.py (2 files)
├─ evaluator/type_coercion.py
├─ evaluator/arithmetic.py
└─ operations/type_coercion.py

PHASE 3 (Secondary)
├─ evaluator/arithmetic_dispatch.py
├─ evaluator/function_execution.py
├─ evaluator/comparisons.py
├─ evaluator/temporal_ops.py
├─ operations/array_ops.py
├─ operations/special_ops.py
├─ intervals.py
└─ ops.py

PHASE 4 (Functions)
├─ functions/implementations/* (5 files)
└─ functions/registrar/* (4 files)

PHASE 5 (Cold Paths)
└─ [27 files - no action needed]
```

---

## Key Statistics

| Metric | Count |
|--------|-------|
| Total files with numpy/pyarrow | 56 |
| HOT path files (CRITICAL) | 3 |
| HOT path files (HIGH) | 8 |
| WARM path files | 21 |
| COLD path files | 19 |
| Dev/Third-party files | 5 |
| Total PRs needed (estimated) | 8-10 |
| Total weeks (estimated) | 4-6 |

---

## Files to Create

### New Cython Files Needed
- `opteryx/expression/_type_coercion.pyx` - Type coercion kernels
- `opteryx/expression/_temporal_kernels.pyx` - Temporal conversions (optional)

### New Python Files Needed
- None (all changes are modifications)

---

## Implementation Checklist Template

For each file phase:

```
[ ] File: xxxxxxxxxx
  [ ] Identify all numpy/pyarrow usage
  [ ] Create replacement using Draken
  [ ] Write unit tests for equivalence
  [ ] Run performance benchmark
  [ ] Update imports
  [ ] Run `make q` (minimum regression suite)
  [ ] Create PR with detailed explanation
  [ ] Code review and merge
```

---

## Next Steps

1. Print this document
2. Start with Phase 1, File 1: `opteryx/expression/operations/__init__.py`
3. Use `eradication-patterns-and-examples.md` for implementation guidance
4. Update `eradication-tracking-matrix.md` as you progress
5. Run tests: `make q` (minimum) or `make test` (full)
6. Run benchmarks: `make clickbench`
