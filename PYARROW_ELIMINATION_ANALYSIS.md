# PyArrow Elimination Analysis & Prioritization

**Status**: Phase 5 - PyArrow Removal Initiative  
**Generated**: 2024  
**Scope**: opteryx-core/opteryx (excluding third_party, tests)  
**Total Files Analyzed**: 64  
**Total Import Statements**: 105

---

## Executive Summary

PyArrow is used across **64 files** in the opteryx codebase, with clustering in:
- **Expression evaluation** (binary ops, type coercion, function implementations)
- **Vector operations** (Cython kernels for temporal, string, list operations)
- **Operators** (join nodes, read/write operations)
- **Utilities** (arrow interop, schema conversion, date handling)

**Key Metrics**:
- Highest usage file: `expression/ops.py` (84 usage points)
- Lowest hanging fruit: 9 files with 0-1 usage (dead imports)
- Anti-pattern violations: 5 imports in `types/_null_handling.py` with try/except

---

## Elimination Priority Map

### 🟢 QUICK WINS (Dead/Near-Dead Imports)
These can be eliminated immediately - they import PyArrow but don't use it.

| File | Imports | Usage | Status | Action |
|------|---------|-------|--------|--------|
| `compiled/table_ops/null_avoidant_ops.pyx` | 1 | 0 | **DEAD** | Remove import |
| `connectors/catalogs/local_catalog.py` | 2 | 0 | **DEAD** | Remove `pq` import, keep parquet decoder |
| `expression/functions/implementations/arithmetic.py` | 1 | 0 | **DEAD** | Remove import |
| `expression/functions/registrar/arithmetic.py` | 1 | 0 | **DEAD** | Remove import (only uses `from pyarrow import compute`) |
| `expression/functions/registrar/arithmetic_extended.py` | 1 | 0 | **DEAD** | Remove import |
| `models/execution_context.py` | 1 | 0 | **DEAD** | Remove import |
| `operators/distinct_node.pyx` | 1 | 0 | **DEAD** | Only used in isinstance check; use marker/type instead |
| `operators/non_equi_join_node.pyx` | 2 | 0 | **DEAD** | Remove both imports |
| `planner/optimizer/strategies/statistics_only_response.py` | 1 | 0 | **DEAD** | Remove import |

**Effort**: ~30 minutes  
**Impact**: Eliminates 11 dead imports (10% of total)  
**Blocker**: None

---

### 🟡 MEDIUM-TERM TARGETS (Low-Moderate Usage, Replaceable)
These files have concrete, replaceable PyArrow dependencies.

#### A. Try/Except Anti-Pattern Violations

**File**: `types/_null_handling.py`  
**Imports**: 5 (all wrapped in try/except ImportError)

```python
# ANTI-PATTERN: Violates rule 9 ("Do not gate imports behind try/except")
try:
    import pyarrow as pa
    if isinstance(value, pa.Scalar):
        return not value.is_valid
except ImportError:
    pass
```

**Current Usage**:
- `is_null()` - checks for `pa.Scalar` types
- `is_nan()` - checks for `pa.Scalar` types  
- `is_inf()` - checks for `pa.Scalar` types
- `is_null_vector()` - checks for `pa.Array`, `pa.ChunkedArray`
- `null_count_vector()` - queries null_count on Arrow arrays

**Replacement Strategy**:
1. These functions are designed for multi-type support (numpy, pyarrow, draken)
2. Since we're eliminating PyArrow, remove PyArrow branch entirely
3. Ensure all callers use Draken vectors (preferred) or numpy
4. Verify no production codepath hits these functions at runtime

**Effort**: 2-4 hours (includes audit of call sites)  
**Impact**: Cleans up anti-pattern, clarifies null handling contract  
**Blocker**: Need to audit call sites to confirm no runtime PyArrow scalars pass through

---

#### B. Type Checking & Schema Utilities (Type-Safe Elimination)

**File**: `utils/arrow.py`  
**Imports**: 3 (Table, nulls, pyarrow)  
**Usage**: 20

**Current Usage**:
- Creates `pyarrow.nulls()` arrays for null columns
- Imports `Table` type for isinstance checks
- Used by morsel/dataframe code for Arrow interop

**Replacement Strategy**:
1. Replace `pa.nulls()` with Draken null vector factory
2. Replace `Table` isinstance checks with internal type marker
3. Arrow interop should move entirely to Draken path

**Effort**: 4-6 hours  
**Impact**: Decouples schema/utilities from Arrow  
**Blocker**: Verify all callers are internal (no external Arrow inputs)

---

**File**: `types/schema.py`  
**Imports**: 1 (pyarrow)  
**Usage**: 21

**Current Usage**:
- Type mapping between Orso types and Arrow types
- Schema conversion for interop

**Replacement Strategy**:
1. If schema conversion is only for incoming Arrow, move to Arrow interop module
2. Replace Arrow type constants with internal type enum
3. Consider if this is entry point for external Arrow data (if so, isolate to connector layer)

**Effort**: 3-5 hours  
**Impact**: Removes schema conversion dependency  
**Blocker**: Need to understand if external systems still send Arrow schemas

---

**File**: `utils/arrow_interop.py`  
**Imports**: 2 (pyarrow, arrow_types)  
**Usage**: 2 (very low!)

**Assessment**: This file is meant to be an Arrow ↔ Opteryx converter. Light usage suggests Arrow input is minimal. Consider if this entire module can be archived.

**Effort**: 2-3 hours  
**Impact**: Archive entire module if Arrow inputs are not critical  
**Blocker**: None (low usage already)

---

#### C. Parquet & IO Layer

**File**: `utils/parquet_decoder.py`  
**Imports**: 2 (pyarrow, parquet)  
**Usage**: 2

**Assessment**: Uses `pyarrow.parquet` for reading. Parquet reading is a fundamental connector capability. This should be kept in the **Connector/IO phase**, not eliminated now.

**Effort**: N/A (out of scope for Phase 5)  
**Impact**: Keep for now; review in connector refactor  
**Blocker**: Strategic - parquet is a primary data source

---

#### D. Utility Functions (Low Complexity)

**File**: `utils/sql.py`  
**Imports**: 1 (pyarrow)  
**Usage**: 3

**Assessment**: Minimal usage. If just type checks, replace with internal type marker.

**Effort**: 1-2 hours  
**Impact**: Minor  
**Blocker**: None

---

**File**: `utils/dates.py`  
**Imports**: 2 (pyarrow, compute)  
**Usage**: 12

**Current Usage**: Date/time arithmetic using `compute.*` functions

**Replacement Strategy**:
1. Replace PyArrow compute calls with equivalent Draken operations or Python stdlib
2. Temporal operations should use dedicated temporal vector kernels

**Effort**: 3-4 hours  
**Impact**: Decouples date utilities  
**Blocker**: Need to verify Draken has equivalent temporal functions

---

### 🔴 LONG-TERM TARGETS (Heavy Usage, Architectural Impact)

#### Tier 1: Expression Evaluator (Core Critical Path)

**File**: `expression/ops.py`  
**Imports**: 2  
**Usage**: 84 (HIGHEST)

**Current Usage**:
- `pyarrow.compute.*` for filter operations (is_null, cast, filter)
- `pyarrow.*` type checks and array construction
- Direct Arrow array manipulation in fastpath operations

**Why It's Hard**:
- This is the **hot path** for filter execution
- Uses PyArrow compute kernels for performance
- Handles type coercion, null masking, compression

**Replacement Strategy**:
- Not elimination, but **migration path**:
  1. `compute.is_null()` → Draken vector kernel
  2. `compute.filter()` → Draken mask application
  3. `compute.cast()` → Draken type coercion
  4. Type checks → Internal type system
- This is a **medium-term refactor**, not Phase 5

**Effort**: 20-30 hours (requires new Draken kernels)  
**Impact**: High - improves performance by removing intermediates  
**Blocker**: Draken vector kernels must be feature-complete first

---

**File**: `expression/__init__.py`  
**Imports**: 2 (pyarrow, compute)  
**Usage**: 47

**Current Usage**:
- Import statement only: `from pyarrow import Table, compute`
- `Table` used for type checks
- `compute` passed through to evaluators

**Replacement Strategy**:
1. Stop importing `Table` (use internal marker)
2. Replace `compute` usage at call sites, not here
3. This is a gateway module; don't add dependencies here

**Effort**: 4-6 hours (depends on call site changes)  
**Impact**: Decouples expression module from Arrow  
**Blocker**: Depends on expression/ops.py refactor

---

**File**: `expression/binary_operators.py`  
**Imports**: 3  
**Usage**: 45

**Current Usage**:
- Type checking and array construction
- Special case for IP CIDR operations (uses Arrow)

**Replacement Strategy**:
1. Move IP CIDR handling to Draken vector kernel
2. Replace Arrow array construction with Draken constructors
3. Type checks → internal system

**Effort**: 6-8 hours  
**Impact**: Medium - reduces operator module interdependency  
**Blocker**: IP CIDR kernel implementation needed

---

#### Tier 2: Function Implementations (Replaceable but Scattered)

**File**: `expression/functions/implementations/text.py`  
**Imports**: 3  
**Usage**: 27

**Current Usage**: String functions using `compute.*` kernels

**Replacement Strategy**:
- Replace `compute.ascii_upper`, `compute.utf8_length`, etc. with Draken string kernels
- Most string operations have Draken equivalents

**Effort**: 8-12 hours (many string functions)  
**Impact**: Medium-high - string ops are common  
**Blocker**: Draken string kernel coverage

---

**File**: `expression/functions/implementations/temporal.py`  
**Imports**: 2  
**Usage**: 17

**Current Usage**: Temporal functions using `compute.*` kernels

**Replacement Strategy**:
- Replace `compute.*` temporal operations with Draken kernels
- Similar to date utilities - consolidate into temporal module

**Effort**: 8-10 hours  
**Impact**: Medium  
**Blocker**: Temporal kernel coverage

---

#### Tier 3: Operators (High Impact, Cython-Heavy)

**File**: `operators/unnest_join_node.pyx`  
**Imports**: 1  
**Usage**: 27

**Assessment**: Core join operator. Arrow usage is likely structural (input/output format).

**Effort**: 10-15 hours  
**Impact**: High (joins are fundamental)  
**Blocker**: Requires understanding current join architecture

---

**File**: `operators/read_node.pyx`  
**Imports**: 1  
**Usage**: 25

**Assessment**: Input/output operator. Arrow usage expected for connector compatibility.

**Effort**: Variable (depends on connector architecture)  
**Impact**: High (data entry point)  
**Blocker**: Strategic - may need Arrow for external data sources

---

**File**: `compiled/vector_ops/vector_split.pyx`  
**Imports**: 1  
**Usage**: 22

**Assessment**: String split operation. Uses `pa.list_()` and `pa.binary()` types.

**Replacement Strategy**:
- Return Draken vector directly instead of Arrow
- Or replace with Draken list vector constructor

**Effort**: 4-6 hours  
**Impact**: Medium  
**Blocker**: Draken list vector support

---

#### Tier 4: Type Coercion (Scattered Throughout)

Multiple files handle type coercion with PyArrow conversions:
- `expression/evaluator/type_coercion.py` (24 usage)
- `expression/operations/type_coercion.py` (24 usage)
- `types/_scalar_to_vector.py` (19 usage)

**Combined Effort**: 15-20 hours  
**Impact**: Medium - consolidates type handling  
**Blocker**: Unified type system must be clear first

---

## Anti-Pattern Violations

### Rule 9: "Do not gate imports behind try/except"

**File**: `types/_null_handling.py`

```python
# VIOLATION: 5 instances
try:
    import pyarrow as pa
    if isinstance(value, pa.Scalar):
        return not value.is_valid
except ImportError:
    pass
```

**Required Action**:
- Per architectural rules, this violates fail-fast principle
- Either commit to PyArrow support OR remove it entirely
- Current approach silently degrades behavior

**Fix**: Remove PyArrow branches; require callers to use Draken vectors

---

## Summary Table: All 64 Files

| File | Imports | Usage | Category | Difficulty | Action |
|------|---------|-------|----------|------------|--------|
| **QUICK WINS** |
| compiled/table_ops/null_avoidant_ops.pyx | 1 | 0 | Dead | Trivial | Remove import |
| connectors/catalogs/local_catalog.py | 2 | 0 | Dead | Trivial | Remove imports |
| expression/functions/implementations/arithmetic.py | 1 | 0 | Dead | Trivial | Remove import |
| expression/functions/registrar/arithmetic.py | 1 | 0 | Dead | Trivial | Remove import |
| expression/functions/registrar/arithmetic_extended.py | 1 | 0 | Dead | Trivial | Remove import |
| models/execution_context.py | 1 | 0 | Dead | Trivial | Remove import |
| operators/distinct_node.pyx | 1 | 0 | Dead | Trivial | Use type marker |
| operators/non_equi_join_node.pyx | 2 | 0 | Dead | Trivial | Remove imports |
| planner/optimizer/strategies/statistics_only_response.py | 1 | 0 | Dead | Trivial | Remove import |
| **ANTI-PATTERN** |
| types/_null_handling.py | 5 | 2 | Anti-pattern | Medium | Audit & remove try/except |
| **SHORT-TERM (Next Sprint)** |
| utils/sql.py | 1 | 3 | Utility | Low | 1-2h |
| compiled/vector_ops/vector_date_trunc.pyx | 1 | 1 | Vector Op | Low | 2-3h |
| managers/execution/serial_engine.py | 1 | 4 | Manager | Low | 2-3h |
| models/dataframe.py | 1 | 3 | Model | Low | 2-3h |
| expression/functions/implementations/logical.py | 1 | 3 | Function | Low | 2-3h |
| expression/evaluator/function_execution.py | 2 | 4 | Evaluator | Low | 2-3h |
| **MEDIUM-TERM (2-4 Weeks)** |
| utils/arrow_interop.py | 2 | 2 | Utility | Medium | 2-3h (consider archiving) |
| utils/dates.py | 2 | 12 | Utility | Medium | 3-4h |
| expression/casts.py | 1 | 8 | Type System | Medium | 3-4h |
| expression/operations/fastpath_constant.py | 1 | 5 | Operation | Medium | 3-4h |
| expression/operations/fastpath_dictionary.py | 1 | 16 | Operation | Medium | 4-5h |
| expression/operations/list_ops.py | 1 | 9 | Operation | Medium | 3-4h |
| expression/operations/special_ops.py | 1 | 3 | Operation | Medium | 2-3h |
| expression/operations/type_coercion.py | 1 | 24 | Type System | Medium | 5-6h |
| expression/evaluator/arithmetic.py | 1 | 8 | Evaluator | Medium | 3-4h |
| expression/evaluator/arithmetic_dispatch.py | 1 | 7 | Evaluator | Medium | 3-4h |
| expression/evaluator/comparisons.py | 1 | 3 | Evaluator | Medium | 2-3h |
| expression/evaluator/temporal_ops.py | 2 | -2 | Evaluator | Low | 1-2h (check usage) |
| expression/functions/implementations/utility.py | 1 | 10 | Function | Medium | 3-4h |
| expression/functions/registrar/__init__.py | 1 | 3 | Registrar | Low | 2h |
| expression/intervals.py | 1 | 11 | Expression | Medium | 3-4h |
| expression/operations/__init__.py | 1 | 19 | Operation | Medium | 4-5h |
| expression/operations/comparisons.py | 2 | -1 | Operation | Low | 1-2h (check usage) |
| expression/operations/string_matching.py | 2 | -1 | Operation | Low | 1-2h (check usage) |
| expression/unary_operations.py | 1 | 6 | Operation | Medium | 2-3h |
| managers/execution/__init__.py | 1 | 1 | Manager | Low | 1-2h |
| operators/base_plan_node.py | 3 | 6 | Operator | Medium | 3-4h |
| operators/cross_join_node.pyx | 1 | 8 | Operator | Medium | 4-5h |
| operators/filter_join_node.pyx | 1 | -1 | Operator | Low | 1-2h (check usage) |
| operators/null_reader_node.pyx | 1 | 6 | Operator | Medium | 3-4h |
| operators/outer_join_node.pyx | 1 | 9 | Operator | Medium | 4-5h |
| planner/__init__.py | 1 | 13 | Planner | Medium | 4-5h |
| query_session.py | 1 | 17 | Session | Medium | 4-5h |
| types/schema.py | 1 | 21 | Type System | Medium | 3-5h |
| utils/arrow.py | 3 | 20 | Utility | Medium | 4-6h |
| **LONG-TERM (Strategic, 1-2 Months)** |
| compiled/draken/vectors/arithmetic_kernels.py | 1 | 3 | Draken Kernel | Medium | 2-3h |
| compiled/table_ops/hash_ops.pyx | 1 | 12 | Cython Kernel | High | 6-8h |
| types/_scalar_to_vector.py | 4 | 19 | Type System | High | 6-8h |
| expression/functions/implementations/text.py | 3 | 27 | Functions | High | 8-12h |
| expression/functions/implementations/temporal.py | 2 | 17 | Functions | High | 8-10h |
| expression/evaluator/type_coercion.py | 1 | 6 | Evaluator | High | 6-8h |
| expression/__init__.py | 2 | 47 | Expression | High | 4-6h (depends on ops.py) |
| expression/binary_operators.py | 3 | 45 | Expression | High | 6-8h |
| expression/ops.py | 2 | 84 | Expression | Critical | 20-30h (HOTPATH) |
| operators/nested_loop_join_node.pyx | 3 | 4 | Operator | High | 6-8h |
| operators/read_node.pyx | 1 | 25 | Operator | Critical | 10-15h (data entry) |
| operators/unnest_join_node.pyx | 1 | 27 | Operator | Critical | 10-15h |
| compiled/vector_ops/vector_split.pyx | 1 | 22 | Vector Op | High | 4-6h |
| **OUT OF SCOPE (Phase 6+)** |
| utils/parquet_decoder.py | 2 | 2 | IO Layer | Strategic | Keep for connector phase |

---

## Recommended Phasing

### Phase 5.1 (This Sprint - 1 week)
**Goal**: Eliminate dead imports, fix anti-patterns

- [ ] Remove 9 dead imports (~30 min)
- [ ] Audit `types/_null_handling.py` call sites (~2-3h)
- [ ] Remove try/except anti-pattern (~1h fix, after audit)
- [ ] Clean up 5 low-hanging utility files (~8-10h)

**Expected Impact**: -11 imports, -5 try/except violations, improved code health

---

### Phase 5.2 (Weeks 2-3 - Moderate Effort)
**Goal**: Decouple utilities and type system

- [ ] `types/_scalar_to_vector.py` - centralize type conversions
- [ ] `utils/arrow.py` - replace Arrow nulls with Draken
- [ ] `utils/dates.py` - temporal operations
- [ ] `expression/operations/*` - filter fastpath operations
- [ ] `expression/evaluator/*` - type coercion

**Expected Impact**: -30-40 imports, cleaner separation of concerns

---

### Phase 5.3 (Weeks 4-6 - Major Refactor)
**Goal**: Replace compute kernels with Draken equivalents

- [ ] String functions: `text.py`, `string_matching.py`
- [ ] Temporal functions: `temporal.py`
- [ ] Type coercion: consolidate
- [ ] Expression operators

**Expected Impact**: -40-50 imports, but requires Draken kernel expansion

---

### Phase 6 (Future - Strategic)
**Goal**: Connector/IO layer refactor

- [ ] Keep `parquet_decoder.py` for now
- [ ] Review `operators/read_node.pyx` as part of connector overhaul
- [ ] Evaluate external Arrow data source requirements

---

## Key Dependencies & Blockers

### Must Have (Before Phase 5.2):
- [ ] Draken null vector factory (replaces `pa.nulls()`)
- [ ] Internal type marker (replaces `isinstance(x, pa.Table)` checks)
- [ ] Verified call sites for `types/_null_handling.py`

### Should Have (Before Phase 5.3):
- [ ] Draken string kernel expansion
- [ ] Draken temporal kernel expansion
- [ ] Consolidated type coercion system

### Nice to Have (For performance):
- [ ] Direct SIMD string comparison (avoid compute.*)
- [ ] Direct temporal arithmetic (avoid compute.*)

---

## Risk Mitigation

**High Risk**: `expression/ops.py` (84 usage points in hot path)
- **Mitigation**: Don't rush. Use perf benchmarks (clickbench) to validate equivalence
- **Testing**: Full regression suite required before commit

**High Risk**: Operators (join nodes, read node)
- **Mitigation**: Start with less critical operators (nested_loop before outer)
- **Testing**: Query execution tests + correctness validation

**Anti-Pattern Risk**: `types/_null_handling.py`
- **Mitigation**: Audit all 50+ call sites before removal
- **Testing**: Unit tests for each code path

---

## Success Criteria

- [ ] All 64 files either PyArrow-free OR justified (connectors/parquet)
- [ ] No try/except ImportError blocks (fail-fast rule)
- [ ] No unused imports (cleaner audits)
- [ ] Performance >= baseline (clickbench)
- [ ] Full regression suite passes

---

## Related: NumPy Elimination Lessons

- Dead imports are easy wins; batch them together
- Type checking is a recurring pattern; solve once
- Compute kernels require Draken equivalents; plan ahead
- Anti-pattern violations (try/except) should be fixed immediately
- Measure performance regression early; not at the end

---

## Next Steps

1. **Immediate** (today): Remove 9 dead imports
2. **This week**: Audit `_null_handling.py`, fix anti-pattern
3. **Sprint planning**: Prioritize Phase 5.1 & 5.2 work
4. **Blocker check**: Confirm Draken kernel roadmap
5. **Benchmark**: Run clickbench baseline before Phase 5.3
