# NumPy Eradication - Executive Summary
## Phase 6 Planning Document

**Status:** Diagnostic Complete | Ready for Architecture Review  
**Current Test Status:** 86/88 passing (PyArrow eliminated, NumPy remains)  
**Audit Date:** 2025  

---

## Key Metrics

| Metric | Value |
|--------|-------|
| **Total NumPy Usage** | 968 references across 129 files |
| **Hot-Path Files** | 54 files (557 refs, 57% of total) |
| **Critical Files** | 5 Python + 5 Cython (237 refs) |
| **Estimated Effort** | 130-140 hours (~3-4 weeks) |
| **Test Files** | 50+ files (can use NumPy for validation) |

---

## Critical Findings

### 1. **Expression Evaluation is the Keystone** ⚠️ HIGH RISK
- **File:** `opteryx/expression/__init__.py` (51 refs)
- **Impact:** Every query execution touches this code
- **Blocker:** Boolean array indexing pattern `true_indices = true_indices[result_bool]`
- **Status:** NOT trivially replaceable without algorithm redesign
- **Phase:** 6b (45 hours, MEDIUM RISK, HIGH IMPACT)

**Why This Matters:**
The DNF short-circuit evaluation and AND/OR operations use NumPy's boolean masking. Replacing this requires implementing equivalent index compression logic with custom data structures.

---

### 2. **Cython Type System is Architecturally Embedded** ⚠️ CRITICAL
- **Files:** 26 Cython files using `cdef numpy.ndarray[dtype_t, ndim=1]`
- **Impact:** Type safety and memory views core to compiled code
- **Blocker:** Cython type declarations can't be trivially replaced
- **Status:** Requires buffer protocol or custom ABC design
- **Phase:** 6c (40 hours, HIGH RISK, CRITICAL)

**Why This Matters:**
The Cython type system uses NumPy's ndarray for compile-time type checking and memoryview safety. Removing this requires introducing a new buffer abstraction across all join/vector operators.

---

### 3. **Vector Operations Need Linear Algebra** ⚠️ MEDIUM RISK
- **File:** `opteryx/compiled/vector_ops/vector_match_against.pyx` (15 refs)
- **Usage:** `numpy.linalg.norm()`, `numpy.dot()` for cosine similarity
- **Blocker:** No internal BLAS library; `numpy.linalg` is external dependency
- **Status:** Requires custom C++ implementation or external BLAS wrapper
- **Phase:** 6d (20 hours, MEDIUM RISK)

**Why This Matters:**
Vector search relies on norm() and dot product. These are non-trivial to implement correctly. Options: (a) custom C++ code, (b) OpenBLAS wrapper, (c) inline Cython math.

---

### 4. **Type System is a Quick Win** ✓ LOW RISK
- **File:** `opteryx/types/_orso_types.py` (20 refs)
- **Usage:** Simple dtype mapping (int32→numpy.int32, float64→numpy.float64)
- **Status:** Can be removed with 2-3 hour refactor
- **Phase:** 6a (3 hours, LOW RISK, PREREQUISITE)

**Why This Matters:**
Removing type system dtype mappings unblocks all downstream work. This should be Phase 6a to validate replacement patterns.

---

## NumPy Operations Breakdown

| Operation | Count | Replaceability | Priority |
|-----------|-------|-----------------|----------|
| `numpy.ndarray` | 170 | Hard (type system) | CRITICAL |
| `numpy.array()` | 168 | Medium (buffer factory) | HIGH |
| `numpy.int64/float32` | 234 | Easy (enum mapping) | LOW |
| `numpy.empty()` | 69 | Medium (memory mgmt) | HIGH |
| `numpy.asarray()` | 50 | Hard (type coercion) | CRITICAL |
| `numpy.datetime64` | 52 | Medium (int64 wrapper) | HIGH |
| `numpy.linalg.*` | 16 | Hard (math impl) | MEDIUM |
| `numpy.zeros/full` | 39 | Easy (direct alloc) | MEDIUM |
| `numpy.arange()` | 13 | Easy (range()) | LOW |

---

## Recommended Phase 6 Roadmap

### Phase 6a: Type System Foundation (Week 1, 3 hours)
**Files:** `opteryx/types/_orso_types.py`  
**Goal:** Remove dtype mappings, create native type registry  
**Risk:** LOW  
**Test Impact:** 86/88 → 86/88 (no change)  
**Unblocks:** 6b, 6c, 6d

**Deliverables:**
- [ ] Remove `.numpy_dtype` property from OrsoTypes
- [ ] Create internal type constants/enums
- [ ] Update all callers
- [ ] `make test` passes

---

### Phase 6b: Expression Evaluation (Weeks 2-3, 45 hours)
**Files:** `expression/__init__.py`, `ops.py`, `binary_operators.py`, `casts.py`, functions/*  
**Goal:** Replace mask operations, datetime64, asarray patterns  
**Risk:** MEDIUM (algorithm changes need validation)  
**Test Impact:** 86/88 → 86/88 (with careful testing)  
**Unblocks:** Cleaner expression engine

**Key Tasks:**
- [ ] Design index compression primitives (replace boolean masking)
- [ ] Create int64 datetime64 wrapper
- [ ] Replace `numpy.asarray()` with type guards
- [ ] Replace dtype checks with custom type system
- [ ] Extensive mask operation testing
- [ ] `make test` passes

**Critical Prototype Needed:**
Create POC for `short_cut_and/or` with native index lists before committing to this phase.

---

### Phase 6c: Compiled Join Operators (Weeks 4-5, 40 hours)
**Files:** Join operators, structures/buffers.pyx  
**Goal:** Replace ndarray with custom buffer types  
**Risk:** HIGH (interface changes across subsystems)  
**Test Impact:** 86/88 → 84/88 initially, iterate to 88/88  
**Unblocks:** Full hot-path NumPy elimination

**Key Tasks:**
- [ ] Design buffer protocol/ABC
- [ ] Create `Int64Buffer`, `ObjectBuffer` Cython types
- [ ] Update join operators to use new buffers
- [ ] Update downstream operators (sort, group-by)
- [ ] Extensive join correctness testing
- [ ] Performance benchmarking
- [ ] `make test` passes

**Critical Design Decision:**
Should buffers be Cython classes (fast) or Python ABC (flexible)?

---

### Phase 6d: Vector Operations (Weeks 5-6, 20 hours)
**Files:** `vector_match_against.pyx`, other vector_*.pyx  
**Goal:** Replace linalg.norm, array allocation  
**Risk:** MEDIUM (numerical correctness)  
**Test Impact:** No change (isolated code path)

**Key Tasks:**
- [ ] Implement custom C++ norm() function
- [ ] Replace `numpy.zeros()` with direct allocation
- [ ] Replace `numpy.dot()` with manual loop
- [ ] Validate vector search correctness
- [ ] Benchmark performance
- [ ] `make test` passes

---

### Phase 6e: Embeddings & Utilities (Weeks 6-7, 25 hours)
**Files:** `vectors/embeddings.py`, utility functions  
**Goal:** Replace vector math, clean up remaining NumPy  
**Risk:** LOW (not in query hot path)  
**Test Impact:** No change

**Key Tasks:**
- [ ] Replace `numpy.linalg.norm()` with custom C++
- [ ] Replace `numpy.dot()` in BM25 scoring
- [ ] Replace `numpy.vstack()` with Python list
- [ ] Clean up remaining utility NumPy usage
- [ ] `make test` passes

---

## Effort Summary

| Phase | Focus | Hours | Risk | Impact |
|-------|-------|-------|------|--------|
| **6a** | Type System | 3 | LOW | Prerequisite |
| **6b** | Expression Eval | 45 | MEDIUM | HIGH (query hot path) |
| **6c** | Join Operators | 40 | HIGH | CRITICAL (correctness) |
| **6d** | Vector Ops | 20 | MEDIUM | MEDIUM (features) |
| **6e** | Utilities | 25 | LOW | LOW (setup code) |
| **TOTAL** | | **133** | | |

**Timeline:** 3-4 weeks for experienced engineer  
**Parallelization:** 6b and 6d can run in parallel with 6c (separate code paths)

---

## Risk Assessment

### High-Risk Areas

| Risk | Mitigation | Validation |
|------|-----------|-----------|
| **Boolean indexing in expressions** | POC implementation, side-by-side testing | Unit tests for mask ops |
| **Join index correctness** | Property-based testing, ClickBench | 100% test coverage |
| **Vector similarity math** | Reference against NumPy, correctness proof | Vector search tests |
| **Cross-subsystem interface change** | Gradual migration, adapter layer | Full regression suite |

### Testing Strategy

- **`make q`** after each major component (catch regressions early)
- **`make test`** at phase boundaries (full validation)
- **ClickBench** for performance regression detection
- **Targeted unit tests** for each replaced operation
- **Property-based tests** for correctness-critical operations

---

## Success Criteria

**Phase 6 Complete when:**

✓ All 88 tests passing (`make test`)  
✓ Zero NumPy imports in `opteryx/compiled/`  
✓ Zero NumPy imports in `opteryx/expression/` (except test adapters)  
✓ Performance maintained or improved (< 5% regression on benchmarks)  
✓ ClickBench runs successfully  
✓ Code review approved for architectural soundness  
✓ All decision points resolved (buffer protocol, BLAS strategy, etc.)

---

## Decision Points Requiring Architect Input

### 1. Buffer Abstraction Design
**Question:** Should we use Cython custom types or Python buffer protocol?
- **Option A:** Cython classes (faster, but more complex)
- **Option B:** Python ABC (simpler, but potential overhead)
- **Decision:** Needed before Phase 6c

### 2. Linear Algebra Strategy
**Question:** How should we handle `numpy.linalg.norm()` and `numpy.dot()`?
- **Option A:** Custom C++ implementation
- **Option B:** External BLAS wrapper (OpenBLAS, MKL)
- **Option C:** Inline Cython math
- **Decision:** Needed before Phase 6d

### 3. Temporal Type Representation
**Question:** Should datetime64 become int64 microseconds or custom Timestamp class?
- **Option A:** int64 (simpler, matches Draken approach)
- **Option B:** Wrapper class (type safety)
- **Decision:** Needed before Phase 6b

### 4. Test NumPy Usage
**Question:** Should tests continue using NumPy or replace entirely?
- **Option A:** Keep NumPy in tests with adapters (practical)
- **Option B:** Replace tests with native implementation (thorough)
- **Decision:** Needed before Phase 6 completion

---

## Estimated Impact

### Benefits
- Eliminate external NumPy dependency from hot path
- Reduce startup time (no NumPy initialization)
- Reduce memory overhead (no NumPy dtype objects)
- Potential performance improvement in hot paths
- Cleaner dependency graph

### Risks
- Expression evaluation algorithm changes
- Join operator interface changes (cross-system)
- Vector search correctness validation
- Performance regression if not carefully optimized

### No-Change Areas
- Tests can continue using NumPy
- Third-party libraries (maki_nage, fastfloat)
- Lower-priority utilities

---

## Next Steps

1. **Architect Review** of this summary (1-2 days)
   - Approve or revise roadmap
   - Resolve decision points (buffer strategy, BLAS, etc.)
   - Identify any blocking architectural concerns

2. **Phase 6a Design** (3-5 days)
   - Create type system refactor spec
   - Design internal type registry
   - Estimate completion

3. **Phase 6b POC** (3-5 days)
   - Prototype expression layer without NumPy
   - Validate algorithm correctness
   - Benchmark performance

4. **Go/No-Go Decision** (5 days)
   - Review POC results
   - Decide to proceed with full Phase 6b
   - Or iterate on design

---

## Appendix: File Categories

**Critical Hot-Path (must remove):**
- `opteryx/expression/__init__.py` (51 refs)
- `opteryx/compiled/joins/cross_join.pyx` (36 refs)
- `opteryx/expression/ops.py` (32 refs)

**High-Priority (should remove):**
- `opteryx/expression/binary_operators.py` (23 refs)
- `opteryx/expression/casts.py` (23 refs)
- `opteryx/compiled/joins/inner_join.pyx` (19 refs)

**Medium-Priority (can remove):**
- `opteryx/vectors/embeddings.py` (64 refs, but not query hot path)
- `opteryx/compiled/vector_ops/vector_match_against.pyx` (15 refs)
- All function implementations

**Low-Priority (optional):**
- Test files (validation use only)
- Utilities and planning code
- Third-party libraries

---

**Prepared for:** Opteryx Core Architecture Review  
**Prepared by:** NumPy Diagnostic Audit System  
**Status:** Ready for Discussion and Decision-Making