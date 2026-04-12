# NumPy Usage Diagnostic Audit Report
## Opteryx-Core Project

**Date Generated:** 2024
**Status:** Phase 6 Planning - NumPy Eradication Strategy
**Current Test Status:** 86/88 passing (PyArrow compute fully eliminated)

---

## Executive Summary

- **Total NumPy Usage Lines:** 968 lines across 99 files
- **Critical Files:** 54 files (production code)
- **Test Files:** 45 files (validation, lower priority)
- **Cython Files (.pyx):** 26 files with NumPy dependency
- **Python Files (.py):** 73 files with NumPy dependency

**Key Finding:** NumPy usage is **heavily concentrated in hot paths** (Cython compiled code and expression evaluation). This represents a **HIGH-IMPACT** removal target for Phase 6.

---

## 1. NumPy Usage by Category

### 1.1 HOT-PATH USAGE (Query Execution Critical Path)

**Files:** 26 Cython files + 12 Python expression files  
**Lines:** ~550+ (57% of total)  
**Impact:** CRITICAL - directly affects query performance

#### A. Cython Compiled Files (In Hot Path)

| Module | Files | Usage Type | Priority |
|--------|-------|-----------|----------|
| **Joins** | 6 files | Array allocation, dtype handling, index management | **CRITICAL** |
| **Vector Operations** | 10 files | Array operations, dtype inference, masking | **CRITICAL** |
| **Table Operations** | 2 files | Index arrays, null handling, data layout | **CRITICAL** |
| **Hash Tables** | 2 files | Index buffers, dtype management | **CRITICAL** |
| **Heap Sort** | 1 file | Sorting with index buffers | **HIGH** |
| **Joins (Operators)** | 5 files | Join computation, index management | **CRITICAL** |

**Breakdown:**

```
opteryx/compiled/joins/cross_join.pyx: 36 lines
  - numpy.empty(), numpy.ndarray, numpy.dtype, numpy.resize()
  - Array allocation for indices and data flattening
  - IMPACT: HOT PATH - array construction in join logic

opteryx/compiled/joins/inner_join.pyx: 19 lines
  - numpy.asarray(), numpy.empty(), numpy.ndarray
  - Index array conversions, row hashing
  - IMPACT: HOT PATH - probe-side filtering

opteryx/compiled/vector_ops/vector_match_against.pyx: 15 lines
  - numpy.linalg.norm(), numpy.dot(), numpy.asarray()
  - Vector normalization and cosine similarity
  - IMPACT: LINEAR SCAN (query execution)

opteryx/compiled/vector_ops/vector_levenshtein.pyx: 6 lines
  - numpy.zeros() - DP table allocation
  - IMPACT: LINEAR SCAN (string distance computation)

opteryx/compiled/table_ops/null_avoidant_ops.pyx: 8 lines
  - numpy.empty(), numpy.ones(), numpy.array()
  - Null mask handling
  - IMPACT: HOT PATH - null filtering

opteryx/compiled/structures/buffers.pyx: 13 lines
  - numpy.ndarray, numpy.empty(), numpy.ascontiguousarray()
  - Index buffer management
  - IMPACT: HOT PATH - buffer lifecycle

opteryx/compiled/structures/hash_table.pyx: 7 lines
  - numpy.empty(), numpy.ndarray
  - Hash table index management
  - IMPACT: HOT PATH - hash lookups

opteryx/operators/*.pyx: 24 lines (heap_sort, cross_join_node, unnest_join, etc.)
  - Array allocation and dtype inference
  - IMPACT: HOT PATH - sort and join operations
```

#### B. Expression Evaluation (Python, In Hot Path)

| File | Lines | Usage | Priority |
|------|-------|-------|----------|
| `opteryx/expression/__init__.py` | 51 | evaluate_dnf, short_cut_and/or, mask operations | **CRITICAL** |
| `opteryx/expression/ops.py` | 32 | Binary/unary operations, type coercion | **CRITICAL** |
| `opteryx/expression/binary_operators.py` | 23 | Arithmetic ops, comparisons | **CRITICAL** |
| `opteryx/expression/unary_operations.py` | 15 | Type checking, negation, NOT | **CRITICAL** |
| `opteryx/expression/casts.py` | 23 | Type casting, datetime64 handling | **CRITICAL** |
| `opteryx/expression/operations/*.py` | 35 | String matching, comparisons, array ops | **CRITICAL** |
| `opteryx/expression/functions/implementations/*.py` | 180 | Function evaluation (temporal, logical, arithmetic, text) | **CRITICAL** |

**Key Finding:** Expression evaluation uses numpy for:
- Boolean mask creation and manipulation (`numpy.zeros()`, `numpy.full()`, `numpy.arange()`)
- Array indexing and slicing
- Type checking (`isinstance()` checks with `numpy.integer`, `numpy.generic`, etc.)
- `numpy.datetime64` handling
- `numpy.array()`, `numpy.asarray()` for type coercion

**IMPACT:** This is the PRIMARY filter/projection hot path. Every query execution touches this code.

---

### 1.2 MEDIUM-PATH USAGE (Frequently Called, Not Always Hot)

**Files:** 5 Python files  
**Lines:** ~180 (19% of total)  
**Impact:** HIGH - setup and frequent operations

| File | Lines | Usage | Priority |
|------|-------|-------|----------|
| `opteryx/vectors/embeddings.py` | 64 | Vector normalization, array ops for embeddings | **MEDIUM** |
| `opteryx/types/_orso_types.py` | 20 | Type system mappings (numpy_dtype property) | **MEDIUM** |
| `opteryx/expression/evaluator/type_coercion.py` | 11 | Type coercion logic | **MEDIUM** |
| `opteryx/planner/logical_planner/logical_planner_builders.py` | 17 | Logical plan construction | **MEDIUM** |
| `opteryx/utils/series.py` | 9 | Series manipulation utility | **LOW** |

**Usage Pattern:**
- `numpy.ndarray`: Type checking and conversions
- `numpy.linalg.norm()`: Vector normalization
- `numpy.float32`, `numpy.int64`: Dtype construction
- `numpy.asarray()`: Array coercion

---

### 1.3 LOW-PRIORITY USAGE (Setup, Testing, Non-Critical)

**Files:** 44+ test files  
**Lines:** ~238 (25% of total)  
**Impact:** LOW - not in query critical path

Test files use NumPy for:
- Result validation
- Test data setup
- Benchmark comparisons
- Mock implementations

**Examples:**
- `tests/unit/functions/test_function_registry_runtime_types.py`: 28 lines
- `tests/unit/functions/test_vector_similarity.py`: 25 lines
- `tests/unit/core/test_vector_search_cpp.py`: 23 lines
- Various performance benchmarks

---

## 2. NumPy Operations Frequency Analysis

### Top 20 NumPy Functions Used

```
170  numpy.ndarray              - Type declarations and checks
168  numpy.array               - Array construction
126  numpy.int* (int32, int64) - Dtype specification
108  numpy.float* (float32, float64) - Dtype specification
 69  numpy.empty               - Array allocation
 52  numpy.datetime64          - Temporal values
 50  numpy.asarray             - Array coercion
 38  numpy.bool_               - Boolean dtype
 20  numpy.full                - Array fill
 19  numpy.zeros               - Zero array
 16  numpy.linalg.norm         - Vector normalization
 16  numpy.import_array()      - Cython initialization
 15  numpy.uint*               - Unsigned int dtype
 15  numpy.integer             - Type checking
 13  numpy.object_             - Object dtype
 13  numpy.array_equal         - Comparison
 13  numpy.arange              - Index generation
 12  numpy.issubdtype          - Type checking
 11  numpy.generic             - Type checking base
  9  numpy.nan                 - NaN constant
```

### Usage Patterns

**Pattern 1: Array Allocation (69 uses of `numpy.empty`)**
- Used in Cython for buffer pre-allocation
- REPLACEABLE: Use Orso memory management or malloc

**Pattern 2: Type Specification (234+ dtype uses)**
- `numpy.int64`, `numpy.float32`, `numpy.bool_`, etc.
- REPLACEABLE: Define custom dtype constants or use C++ equivalents

**Pattern 3: Type Checking (38 uses)**
- `isinstance(x, numpy.integer)`, `isinstance(x, numpy.generic)`
- REPLACEABLE: Custom type checks in Python layer

**Pattern 4: Array Operations (168 uses of `numpy.array`)**
- Construction and coercion
- REPLACEABLE: Orso arrays or custom Python lists

**Pattern 5: Datetime Handling (52 uses of `numpy.datetime64`)**
- Temporal value representation
- REPLACEABLE: Use int64 microseconds + custom wrapper

**Pattern 6: Vector Math (16 uses of `numpy.linalg.norm`)**
- Cosine similarity, normalization
- REPLACEABLE: Custom C++ implementation or native Python

---

## 3. Module-by-Module Analysis

### 3.1 Compiled/Joins (36 lines)

**Files:** cross_join.pyx, inner_join.pyx, filter_join.pyx, outer_join.pyx, nested_loop_join_equals.pyx

**Usage:**
- `numpy.ndarray`: Type declarations for Cython memoryviews
- `numpy.empty()`, `numpy.asarray()`: Buffer allocation
- `numpy.dtype`: Type inspection
- `numpy.issubdtype()`: Element type detection

**Effort Estimate:** 
- **MEDIUM-HIGH** (30-40 hours)
- Replace numpy dtype checks with C++ type inspection
- Use direct Orso memory allocation
- May require Cython template specialization

**Recommendation:** This should be **Phase 6a** (first priority for joins)

---

### 3.2 Compiled/Vector Operations (60+ lines)

**Files:** 10 vector_*.pyx files

**Key Operations:**
- `vector_match_against.pyx`: `numpy.linalg.norm()`, `numpy.dot()` - Vector similarity
- `vector_cast_string_to_int.pyx`: `numpy.zeros()` - Result allocation
- `vector_date_diff.pyx`, `vector_length.pyx`: `numpy.zeros()` - Integer arrays
- `vector_levenshtein.pyx`: `numpy.zeros()` - DP table

**Effort Estimate:**
- **LOW-MEDIUM** (15-25 hours)
- Most are simple `numpy.zeros()` → direct allocation
- `numpy.linalg.norm()` needs custom C++ implementation
- Others are straightforward replacements

**Recommendation:** This should be **Phase 6b** (second priority)

---

### 3.3 Expression Evaluation (180+ lines)

**Files:** `__init__.py`, `ops.py`, `binary_operators.py`, `casts.py`, `unary_operations.py`, `operations/*.py`, `functions/implementations/*.py`

**Usage Patterns:**

| Pattern | Lines | Replaceable |
|---------|-------|------------|
| `numpy.array()` | 40 | YES - use list or Orso |
| `numpy.asarray()` | 30 | YES - type guard only |
| `numpy.bool_` type checks | 25 | YES - custom checks |
| `numpy.datetime64` | 40 | YES - wrap int64 |
| `numpy.zeros()`, `numpy.full()` | 25 | YES - direct allocation |
| `numpy.arange()` | 13 | YES - Python range |
| `numpy.integer` type checks | 15 | YES - custom checks |
| Other dtype operations | 20 | YES - mapping table |

**Hot Path Impact:**
- `evaluate_dnf()`: Creates boolean masks with `numpy.zeros()`, `numpy.arange()`
- `short_cut_and/or()`: Array slicing and indexing
- Binary operators: Type coercion with `numpy.asarray()`

**Effort Estimate:**
- **MEDIUM** (40-50 hours)
- Most conversions are straightforward
- Requires careful testing of mask operations
- May need to add Orso array support to expression layer

**Recommendation:** This should be **Phase 6c** (parallel with vector ops)

---

### 3.4 Types System (20 lines)

**Files:** `_orso_types.py`, `_scalar_types.py`, `_null_handling.py`

**Usage:**
- `_orso_types.py`: Maps OrsoTypes to numpy dtypes (numpy.int32, numpy.float64, etc.)
- Type mapping table: ~18 lines
- Property accessor: `.numpy_dtype` → returns numpy dtype

**Effort Estimate:**
- **LOW** (2-3 hours)
- Replace mapping with C++ type equivalents
- Remove `.numpy_dtype` property entirely or wrap to custom type system

**Recommendation:** This should be **Phase 6 preparatory** (do first, unblocks other work)

---

### 3.5 Embeddings (64 lines)

**Files:** `vectors/embeddings.py`

**Usage:**
- `numpy.float32`: Dtype specification
- `numpy.zeros()`: Vector allocation
- `numpy.vstack()`: Stacking vectors
- `numpy.linalg.norm()`: Vector normalization
- `numpy.dot()`: Dot product
- `numpy.asarray()`: Type coercion

**Context:** Used for:
- Text embedding with hash-based static provider
- Lexical scoring with BM25
- Hybrid embedding provider

**Execution Context:** NOT in query critical path - embeddings are computed during function setup, not per-row in queries.

**Effort Estimate:**
- **MEDIUM** (15-20 hours)
- Replace `numpy.linalg.norm()` with custom implementation
- Replace `numpy.dot()` with manual loop
- Replace `numpy.vstack()` with Python list + array allocation
- Replace dtype specifications with int/float

**Recommendation:** This should be **Phase 6d** (later, lower priority)

---

## 4. Effort Estimation Summary

### By Difficulty & Impact

#### Phase 6a: Types System Refactor (PREREQUISITE)
- **Files:** 3
- **Effort:** 2-3 hours
- **Impact:** Unblocks other work
- **Tasks:**
  1. Remove `.numpy_dtype` property
  2. Create internal type mapping (C++ enums or constants)
  3. Update callers

#### Phase 6b: Expression Evaluation (HIGHEST IMPACT)
- **Files:** 15+ Python files
- **Effort:** 40-50 hours
- **Impact:** Query execution hot path, 180+ lines
- **Tasks:**
  1. Replace mask operations (numpy.zeros/full/arange)
  2. Replace dtype checks (numpy.integer, numpy.generic)
  3. Replace datetime64 with int64 wrapper
  4. Replace array coercion (numpy.asarray)
  5. Extensive testing (mask operations critical)

#### Phase 6c: Compiled Joins (CRITICAL HOT PATH)
- **Files:** 5 Cython files
- **Effort:** 30-40 hours
- **Impact:** Join computation, 36+ lines in hot path
- **Tasks:**
  1. Replace numpy.dtype checks with C++ type inspection
  2. Replace numpy.empty() with malloc or Orso allocation
  3. Replace numpy.ndarray type hints
  4. May require Cython specialization templates

#### Phase 6d: Vector Operations (MEDIUM IMPACT)
- **Files:** 10 Cython files
- **Effort:** 15-25 hours
- **Impact:** Vector search hot path, 60+ lines
- **Tasks:**
  1. Replace numpy.zeros() with direct allocation (3-5 hours)
  2. Replace numpy.linalg.norm() with custom C++ implementation (5-10 hours)
  3. Replace other dtype operations (5-10 hours)

#### Phase 6e: Embeddings & Utilities (LOWER PRIORITY)
- **Files:** 8 Python files
- **Effort:** 20-30 hours
- **Impact:** Text search, setup code
- **Tasks:**
  1. Replace vector operations (linalg, dot)
  2. Replace array allocation patterns

#### Phase 6f: Test Suite Cleanup (OPTIONAL)
- **Files:** 44+ test files
- **Effort:** 10-15 hours
- **Impact:** None on production
- **Tasks:**
  1. Replace numpy validation in tests
  2. Update mock implementations

---

## 5. Strategic Recommendations

### 5.1 Phase 6 Roadmap

**Week 1: Foundation (2-3 hours)**
- [ ] Phase 6a: Types system refactor
  - Remove numpy dtype mappings
  - Create internal type system

**Week 2-3: Expression Engine (40-50 hours, can parallelize with 6d)**
- [ ] Phase 6b: Expression evaluation refactor
  - Start with mask operations
  - Test each change incrementally
  - Use `make q` frequently

**Week 3-4: Joins & Core Execution (30-40 hours, parallel with 6b)**
- [ ] Phase 6c: Compiled joins refactor
  - Replace array allocation
  - Replace type inspection
  - Update memoryview declarations

**Week 4-5: Vector Operations (15-25 hours, parallel with 6b/c)**
- [ ] Phase 6d: Vector operations refactor
  - Replace numpy.zeros() (quick wins)
  - Implement C++ norm function
  - Test vector search extensively

**Week 5-6: Polish & Testing (10-15 hours)**
- [ ] Phase 6e: Embeddings & utilities
- [ ] Run full test suite
- [ ] Performance benchmarking

**Optional - Week 6-7:**
- [ ] Phase 6f: Test suite cleanup

### 5.2 Risk Mitigation

**High Risk Areas:**
1. **Expression Evaluation Masks** - Used in every query
   - Mitigation: Create comprehensive mask operation tests before refactoring
   - Keep old code available for comparison testing

2. **Join Index Arrays** - Critical for correctness
   - Mitigation: Use unit tests from compiled/joins tests
   - Benchmark join performance after changes

3. **Vector Similarity** - Uses numpy.linalg
   - Mitigation: Verify cosine similarity algorithm correctness
   - Create C++ test for norm calculation

**Testing Strategy:**
- Run `make q` after each phase (minimum regression)
- Run `make test` at phase boundaries
- Create targeted unit tests for each replaced operation

### 5.3 Performance Considerations

**Expected Improvements:**
- Reduce import time (no NumPy initialization)
- Reduce memory overhead (no NumPy dtype objects)
- Faster mask operations (direct int arrays instead of bool arrays)
- Potential improvement in vector operations (native C++ vs NumPy layer)

**Potential Regressions:**
- Expression evaluation if not optimized carefully
- Vector normalization if custom C++ implementation has bugs
- Join index handling if memory layout changes

**Mitigation:**
- Use ClickBench for performance testing
- Profile hot paths before and after
- Keep performance logs

---

## 6. Detailed File Inventory

### Cython Files (.pyx) - 26 files

**Joins (6 files, ~120 lines):**
- cross_join.pyx: 36 lines
- inner_join.pyx: 19 lines
- filter_join.pyx: 8 lines
- outer_join.pyx: 4 lines
- nested_loop_join_equals.pyx: 6 lines
- (Operators subfolder): 5 files with joins

**Vector Operations (10 files, ~60 lines):**
- vector_match_against.pyx: 15 lines (linalg.norm, dot product)
- vector_levenshtein.pyx: 6 lines (zeros)
- vector_cast_string_to_int.pyx: 4 lines (zeros)
- vector_date_diff.pyx: 4 lines (zeros)
- vector_length.pyx: 4 lines (zeros)
- vector_position.pyx: 4 lines (zeros)
- vector_string_slice.pyx: 1 line (zeros)
- vector_arrow_op.pyx: 6 lines (empty)
- vector_long_arrow_op.pyx: 6 lines (empty)
- vector_iif.pyx: 1 line (module name check)

**Data Structures (3 files, ~30 lines):**
- buffers.pyx: 13 lines (ndarray, empty, ascontiguousarray)
- hash_table.pyx: 7 lines (empty, ndarray)
- (Others): null_avoidant_ops.pyx: 8 lines

**Other Compiled (7+ files, ~30 lines):**
- heap_sort_node.pyx, cross_join_node.pyx, unnest_join_node.pyx, etc.

### Python Files (.py) - 73 files

**Expression Layer (28 files, ~240 lines):**
- `__init__.py`: 51 lines (evaluate_dnf, masks)
- `ops.py`: 32 lines (binary/unary)
- `binary_operators.py`: 23 lines
- `casts.py`: 23 lines
- `unary_operations.py`: 15 lines
- `functions/implementations/utility.py`: 51 lines
- `functions/implementations/arithmetic.py`: 14 lines
- `functions/implementations/temporal.py`: 16 lines
- `functions/implementations/logical.py`: 18 lines
- `functions/implementations/text.py`: 6 lines
- `functions/registrar/*.py`: 20+ lines
- `operations/*.py`: 35 lines (comparisons, string_matching, array_ops, etc.)
- `evaluator/type_coercion.py`: 11 lines

**Type System (4 files, ~50 lines):**
- `_orso_types.py`: 20 lines (dtype mapping)
- `_scalar_types.py`: 8 lines
- `_null_handling.py`: 6 lines
- `_scalar_to_vector.py`: 1 line

**Vectors/Embeddings (3 files, ~75 lines):**
- `vectors/embeddings.py`: 64 lines (normalize, dot, vstack)
- `vectors/vector_types.py`: 3 lines

**Utilities & Planning (6 files, ~40 lines):**
- `planner/__init__.py`: 7 lines
- `planner/logical_planner/logical_planner_builders.py`: 17 lines
- `planner/ast_rewriter.py`: 2 lines
- `utils/series.py`: 9 lines
- `utils/dates.py`: 5 lines
- `utils/sql.py`: 6 lines

### Test Files (45 files, ~240 lines)
- Predominantly validation and setup
- Lower priority for Phase 6
- Can be addressed after core refactoring

---

## 7. Alternative Implementation Strategies

### Strategy A: Direct Replacement (Recommended)
- Replace NumPy with direct Python/C++ implementations
- Use int arrays instead of numpy dtypes
- Pros: Clean break, maximum performance
- Cons: More effort, higher risk

### Strategy B: Wrapper Layer
- Create internal array/dtype wrapper classes
- Gradual replacement of NumPy underneath
- Pros: Lower risk, can be staged
- Cons: Additional abstraction layer

### Strategy C: Hybrid Approach (Recommended)
- Replace hot paths first (expression, joins)
- Keep NumPy in utilities/setup until ready
- Remove NumPy gradually
- Pros: Balance risk/benefit, can validate improvements
- Cons: Longer timeline

---

## 8. Success Metrics

**Definition of Done for Phase 6:**

1. ✓ All 88 tests passing (`make test`)
2. ✓ No NumPy imports in:
   - `opteryx/compiled/*.pyx`
   - `opteryx/expression/` (except utility functions)
   - `opteryx/operators/*.pyx`
3. ✓ Performance maintained or improved:
   - Query latency: no regression > 5%
   - Memory usage: reduced by removing NumPy overhead
4. ✓ ClickBench runs successfully
5. ✓ Code review for correctness of dtype/array replacements

---

## 9. Open Questions for Architect Review

1. **Datetime Representation:** Should we use int64 microseconds with wrapper class, or Orso temporal types?
2. **Type System:** Should dtype mapping remain in Python or move to C++?
3. **Vector Operations:** Should we implement numpy.linalg.norm() in C++ or use existing algorithms?
4. **Array Allocation:** Should we use Orso memory manager or direct malloc for Cython buffers?
5. **Expression Layer:** Should we create a custom "mask" type or continue using bool arrays?
6. **Testing:** Should we keep test NumPy usage for validation, or replace entirely?

---

## Appendix: Full File List by Category

### Critical Hot Path Files
- opteryx/expression/__init__.py
- opteryx/expression/ops.py
- opteryx/expression/binary_operators.py
- opteryx/compiled/joins/cross_join.pyx
- opteryx/compiled/joins/inner_join.pyx
- opteryx/compiled/vector_ops/vector_match_against.pyx
- opteryx/compiled/structures/buffers.pyx

### High Priority Files
- opteryx/expression/casts.py
- opteryx/expression/unary_operations.py
- opteryx/compiled/joins/filter_join.pyx
- opteryx/compiled/vector_ops/vector_levenshtein.pyx
- opteryx/vectors/embeddings.py

### Medium Priority Files
- (All other expression/functions/implementations/*.py)
- (All other compiled/vector_ops/*.pyx)
- opteryx/types/_orso_types.py

### Low Priority Files
- All test files (tests/)
- opteryx/planner/
- opteryx/utils/
- opteryx/third_party/

---

**End of Report**