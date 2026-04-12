# Phase 6 Planning Checklist: NumPy Eradication

**Status:** Pre-Planning  
**Estimated Duration:** 3-4 weeks (133 hours)  
**Current Test Status:** 86/88 passing  
**Target Test Status:** 88/88 passing, Zero NumPy in hot paths  

---

## PRE-PHASE DECISIONS (Required Before Starting)

- [ ] **Decision 1: Buffer Abstraction Strategy**
  - [ ] Option A chosen: Cython custom types (fast)
  - [ ] Option B chosen: Python ABC (flexible)
  - [ ] Design document created
  - [ ] Architect sign-off obtained

- [ ] **Decision 2: Linear Algebra Strategy**
  - [ ] Option A chosen: Custom C++ norm/dot
  - [ ] Option B chosen: External BLAS wrapper
  - [ ] Option C chosen: Inline Cython
  - [ ] Implementation proof-of-concept created
  - [ ] Performance baseline established

- [ ] **Decision 3: Datetime Representation**
  - [ ] Option A chosen: int64 microseconds
  - [ ] Option B chosen: Wrapper class
  - [ ] Migration strategy documented
  - [ ] All temporal tests identified

- [ ] **Decision 4: Test NumPy Usage**
  - [ ] Option A chosen: Keep NumPy in tests
  - [ ] Option B chosen: Replace entirely
  - [ ] Adapter strategy defined (if needed)

---

## PHASE 6a: Type System Foundation (Week 1)

**Goal:** Remove NumPy dtype mappings, create native type registry  
**Effort:** 3 hours  
**Risk:** LOW  
**Expected Result:** 86/88 tests passing, no regressions

### Task Checklist

- [ ] **Code Review & Planning**
  - [ ] Analyze `opteryx/types/_orso_types.py` (20 refs)
  - [ ] Identify all callers of `.numpy_dtype` property
  - [ ] Create migration strategy document
  - [ ] Estimate effort for each caller

- [ ] **Implementation: Type Registry**
  - [ ] Create `opteryx/types/_type_registry.py`
  - [ ] Define type constants (replace numpy.int32, etc.)
  - [ ] Map OrsoTypes → native types
  - [ ] Handle temporal types (datetime64)

- [ ] **Implementation: Remove NumPy**
  - [ ] Remove `import numpy` from `_orso_types.py`
  - [ ] Remove `numpy_dtype` property
  - [ ] Update all calling code (grep for references)
  - [ ] Test each change incrementally

- [ ] **Implementation: Update Callers**
  - [ ] File: `opteryx/expression/casts.py` (23 refs)
  - [ ] File: `opteryx/expression/__init__.py` (datetime64 refs)
  - [ ] File: `opteryx/compiled/joins/*.pyx` (dtype checks)
  - [ ] File: All other files using OrsoTypes

- [ ] **Testing**
  - [ ] `make q` - minimum regression suite
  - [ ] `make test` - full test suite
  - [ ] Confirm 86/88 passing
  - [ ] No performance regression

- [ ] **Code Review & Merge**
  - [ ] Architecture review
  - [ ] Code quality review
  - [ ] Merge to main branch

### Success Criteria
- ✓ All 86/88 tests passing
- ✓ Zero `import numpy` in `opteryx/types/`
- ✓ Type system tests green
- ✓ No performance regression
- ✓ Code review approved

---

## PHASE 6b: Expression Evaluation (Weeks 2-3)

**Goal:** Replace mask operations, datetime64, asarray patterns  
**Effort:** 45 hours  
**Risk:** MEDIUM  
**Expected Result:** 86/88 tests passing (with careful validation)

### Pre-Phase Validation

- [ ] **Proof-of-Concept Required**
  - [ ] Create POC branch for `short_cut_and()` without NumPy
  - [ ] Implement index compression logic
  - [ ] Run expression tests against POC
  - [ ] Validate correctness vs NumPy version
  - [ ] Benchmark performance impact
  - [ ] Architect approval for approach

### Task Breakdown

#### Task 6b.1: Mask Operation Replacement (12 hours)

Files: `opteryx/expression/__init__.py` (51 refs)

- [ ] **Analyze Current Implementation**
  - [ ] Study `evaluate_dnf()` function (line 194-217)
  - [ ] Study `short_cut_and()` function (line 220-251)
  - [ ] Study `short_cut_or()` function (line 254-285)
  - [ ] Understand boolean masking pattern
  - [ ] Document algorithm

- [ ] **Design New Index Handling**
  - [ ] Create index compression primitives
  - [ ] Design true_indices list representation
  - [ ] Design false_indices list representation
  - [ ] Document algorithm equivalence

- [ ] **Implement evaluate_dnf()**
  - [ ] Replace `numpy.arange(num_rows)` with list(range(...))
  - [ ] Replace array masking with index list filtering
  - [ ] Replace `numpy.zeros()` return with bool array
  - [ ] Test: `make test` for expression tests

- [ ] **Implement short_cut_and()**
  - [ ] Replace mask operations
  - [ ] Handle null values correctly
  - [ ] Test: Unit tests for all paths

- [ ] **Implement short_cut_or()**
  - [ ] Replace mask operations
  - [ ] Handle null values correctly
  - [ ] Test: Unit tests for all paths

- [ ] **Testing**
  - [ ] Unit tests for mask operations
  - [ ] `make test` for expression module
  - [ ] Confirm 86/88 passing

#### Task 6b.2: Datetime64 Replacement (15 hours)

Files: `opteryx/expression/casts.py` (23 refs), `__init__.py` (datetime refs)

- [ ] **Design Datetime Wrapper**
  - [ ] Create `opteryx/types/_datetime_wrapper.py`
  - [ ] Implement microsecond representation
  - [ ] Handle arithmetic operations
  - [ ] Test: Verify behavior matches numpy.datetime64

- [ ] **Replace datetime64 Usage**
  - [ ] Find all `numpy.datetime64()` calls
  - [ ] Replace with int64 microseconds or wrapper
  - [ ] Update type checking
  - [ ] Update arithmetic operations

- [ ] **Update Temporal Functions**
  - [ ] File: `expression/functions/implementations/temporal.py`
  - [ ] Replace datetime64 references
  - [ ] Test: Temporal function tests

- [ ] **Testing**
  - [ ] Unit tests for datetime operations
  - [ ] `make test` for temporal functions
  - [ ] ClickBench temporal queries
  - [ ] Confirm 86/88 passing

#### Task 6b.3: Type Coercion Replacement (10 hours)

Files: `expression/ops.py` (32 refs), `binary_operators.py` (23 refs)

- [ ] **Replace numpy.asarray()**
  - [ ] Study current coercion patterns
  - [ ] Create native type guards
  - [ ] Replace `numpy.asarray(x, dtype=bool)` calls
  - [ ] Test: Type coercion tests

- [ ] **Replace Type Checks**
  - [ ] Replace `isinstance(x, numpy.integer)`
  - [ ] Replace `isinstance(x, numpy.generic)`
  - [ ] Replace `numpy.issubdtype()` calls
  - [ ] Create custom type checking functions

- [ ] **Update Function Implementations**
  - [ ] File: `expression/functions/implementations/arithmetic.py`
  - [ ] File: `expression/functions/implementations/logical.py`
  - [ ] Replace type checks
  - [ ] Test each file incrementally

- [ ] **Testing**
  - [ ] Unit tests for type coercion
  - [ ] `make test` for functions
  - [ ] Confirm 86/88 passing

#### Task 6b.4: Cleanup & Validation (8 hours)

- [ ] **Comprehensive Testing**
  - [ ] `make test` full suite
  - [ ] All 86/88 tests passing
  - [ ] No regressions
  - [ ] Performance baseline established

- [ ] **Code Review**
  - [ ] Architecture review
  - [ ] Algorithm correctness review
  - [ ] Performance review
  - [ ] Merge approval

- [ ] **Documentation**
  - [ ] Update code comments
  - [ ] Document new patterns
  - [ ] Create migration guide for other engineers

### Success Criteria for Phase 6b
- ✓ All 86/88 tests passing
- ✓ Zero `numpy.ndarray` in __init__.py
- ✓ Zero `numpy.asarray()` in expression layer
- ✓ Zero `numpy.datetime64` (if using wrapper)
- ✓ Expression evaluation correctness verified
- ✓ Performance maintained or improved
- ✓ Code review approved

---

## PHASE 6c: Compiled Join Operators (Weeks 4-5)

**Goal:** Replace numpy.ndarray with custom buffer types  
**Effort:** 40 hours  
**Risk:** HIGH  
**Expected Result:** 84/88 initially → 88/88 after iteration

### Pre-Phase Requirements

- [ ] **Buffer Abstraction Design** (5 hours)
  - [ ] Decision from Phase pre-planning
  - [ ] Design buffer ABC/Cython class
  - [ ] Define type annotations
  - [ ] Create reference implementation
  - [ ] Architect sign-off

- [ ] **Join Tests Analysis** (3 hours)
  - [ ] Identify all join operation tests
  - [ ] Understand correctness requirements
  - [ ] Prepare for extensive testing

### Task Breakdown

#### Task 6c.1: Buffer Abstraction Implementation (12 hours)

- [ ] **Create Buffer Base Class**
  - [ ] File: `opteryx/compiled/structures/_buffer.pyx` (or .py)
  - [ ] Define interface for Int64Buffer
  - [ ] Define interface for ObjectBuffer
  - [ ] Define interface for UInt64Buffer
  - [ ] Implement memory management
  - [ ] Test: Buffer interface tests

- [ ] **Implement Specific Buffer Types**
  - [ ] `Int64Buffer` - for index arrays
  - [ ] `ObjectBuffer` - for string/object data
  - [ ] `UInt64Buffer` - for hashes
  - [ ] Memory allocation strategy
  - [ ] Memory deallocation/cleanup

- [ ] **Integration Points**
  - [ ] Update join operator returns
  - [ ] Update downstream operator inputs
  - [ ] Handle conversions for compatibility

#### Task 6c.2: Join Operator Refactoring (20 hours)

- [ ] **Cross Join Refactor** (5 hours)
  - [ ] File: `opteryx/compiled/joins/cross_join.pyx` (36 refs)
  - [ ] Replace `numpy.empty()` with buffer allocation
  - [ ] Replace `numpy.ndarray` type hints
  - [ ] Replace `numpy.dtype` checks with C++ type inspection
  - [ ] Test: Join correctness tests

- [ ] **Inner Join Refactor** (5 hours)
  - [ ] File: `opteryx/compiled/joins/inner_join.pyx` (19 refs)
  - [ ] Replace array operations with buffers
  - [ ] Update index handling
  - [ ] Test: Inner join tests

- [ ] **Filter/Outer/Nested Loop Joins** (5 hours)
  - [ ] Files: filter_join.pyx, outer_join.pyx, nested_loop_join_equals.pyx
  - [ ] Replace NumPy patterns
  - [ ] Consolidate buffer usage
  - [ ] Test: All join tests

- [ ] **Operator Integration** (5 hours)
  - [ ] File: `opteryx/operators/cross_join_node.pyx`
  - [ ] File: `opteryx/operators/nested_loop_join_node.pyx`
  - [ ] File: `opteryx/operators/non_equi_join_node.pyx`
  - [ ] File: `opteryx/operators/unnest_join_node.pyx`
  - [ ] Update to work with new buffers
  - [ ] Test: Operator tests

#### Task 6c.3: Data Structure Updates (5 hours)

- [ ] **Buffer Management**
  - [ ] File: `opteryx/compiled/structures/buffers.pyx` (13 refs)
  - [ ] File: `opteryx/compiled/structures/hash_table.pyx` (7 refs)
  - [ ] Replace numpy array handling
  - [ ] Update to_numpy() methods (for tests)
  - [ ] Test: Structure tests

- [ ] **Null Handling**
  - [ ] File: `opteryx/compiled/table_ops/null_avoidant_ops.pyx` (8 refs)
  - [ ] Update null mask operations
  - [ ] Replace numpy array patterns
  - [ ] Test: Null handling tests

#### Task 6c.4: Comprehensive Testing & Debugging (3 hours)

- [ ] **Join Correctness Testing**
  - [ ] `make test` for compiled/joins/
  - [ ] `make test` for operators/
  - [ ] Expected: Some failures initially
  - [ ] Debug and fix failures
  - [ ] Iterate until 88/88 passing

- [ ] **Performance Validation**
  - [ ] Benchmark join performance
  - [ ] Compare to NumPy baseline
  - [ ] Document any regressions
  - [ ] Optimize if necessary

- [ ] **Cross-Subsystem Testing**
  - [ ] Test with GROUP BY (uses join results)
  - [ ] Test with filters (uses index arrays)
  - [ ] Test with sorts (uses index buffers)
  - [ ] Confirm 88/88 passing

### Success Criteria for Phase 6c
- ✓ All 88 tests passing
- ✓ Zero `numpy.ndarray` in compiled/joins/
- ✓ Zero `numpy.ndarray` in compiled/structures/
- ✓ Zero `numpy.empty()` in hot path
- ✓ Join correctness verified
- ✓ Performance maintained
- ✓ Code review approved
- ✓ Buffer abstraction documented

---

## PHASE 6d: Vector Operations (Weeks 5-6)

**Goal:** Replace numpy.linalg functions, array allocation  
**Effort:** 20 hours  
**Risk:** MEDIUM  
**Expected Result:** No regressions, maintained correctness

### Pre-Phase Requirements

- [ ] **Linear Algebra Proof-of-Concept** (3 hours)
  - [ ] Implement C++ norm() function
  - [ ] Implement C++ dot product
  - [ ] Validate against NumPy reference
  - [ ] Benchmark performance

### Task Breakdown

#### Task 6d.1: Implement Linear Algebra Functions (8 hours)

- [ ] **Custom norm() Implementation**
  - [ ] Create `opteryx/compiled/math/_linalg.pyx`
  - [ ] Implement L2 norm (Euclidean)
  - [ ] Handle edge cases (zero vector)
  - [ ] Test: Verify against numpy.linalg.norm()
  - [ ] Performance: Benchmark

- [ ] **Custom dot() Implementation**
  - [ ] Implement dot product
  - [ ] Test: Verify against numpy.dot()
  - [ ] Performance: Benchmark

#### Task 6d.2: Vector Operations Refactoring (8 hours)

- [ ] **vector_match_against.pyx** (15 refs)
  - [ ] Replace `numpy.linalg.norm()` calls
  - [ ] Replace `numpy.dot()` calls
  - [ ] Replace `numpy.asarray()` coercions
  - [ ] Test: Vector search tests
  - [ ] Performance: Benchmark vector similarity

- [ ] **Other vector_*.pyx Files**
  - [ ] Replace `numpy.zeros()` with direct allocation
  - [ ] Replace `numpy.asarray()` calls
  - [ ] Update dtype specifications
  - [ ] Test: All vector operation tests

#### Task 6d.3: Testing & Validation (4 hours)

- [ ] **Correctness Validation**
  - [ ] `make test` for compiled/vector_ops/
  - [ ] Verify all tests passing
  - [ ] Compare results with NumPy reference

- [ ] **Performance Validation**
  - [ ] Benchmark vector search performance
  - [ ] Compare to baseline
  - [ ] Document any improvements/regressions

- [ ] **Full Test Suite**
  - [ ] `make test` complete suite
  - [ ] Confirm 88/88 passing

### Success Criteria for Phase 6d
- ✓ All 88 tests passing
- ✓ Zero `numpy.linalg.*` calls
- ✓ Zero `numpy.zeros()` in vector ops
- ✓ Vector similarity correctness verified
- ✓ Performance maintained or improved
- ✓ Code review approved

---

## PHASE 6e: Embeddings & Utilities (Weeks 6-7)

**Goal:** Replace vector math in embeddings, clean up remaining NumPy  
**Effort:** 25 hours  
**Risk:** LOW  
**Expected Result:** No regressions

### Task Breakdown

#### Task 6e.1: Embeddings Refactoring (15 hours)

- [ ] **vectors/embeddings.py** (64 refs)
  - [ ] Replace `numpy.linalg.norm()` with custom norm
  - [ ] Replace `numpy.dot()` with custom dot
  - [ ] Replace `numpy.vstack()` with Python list
  - [ ] Replace `numpy.asarray()` calls
  - [ ] Replace `numpy.float32` specifications
  - [ ] Test: Embedding tests

#### Task 6e.2: Function Implementations (6 hours)

- [ ] **Replace Remaining Function NumPy**
  - [ ] File: `expression/functions/implementations/text.py` (6 refs)
  - [ ] File: `expression/functions/implementations/utility.py` (51 refs)
  - [ ] Replace dtype checks
  - [ ] Replace array coercions
  - [ ] Test: Function tests

#### Task 6e.3: Utility Functions (4 hours)

- [ ] **Remaining Utility Code**
  - [ ] File: `utils/series.py` (9 refs)
  - [ ] File: `utils/dates.py` (5 refs)
  - [ ] File: `planner/` files (7+ refs)
  - [ ] Replace NumPy usage
  - [ ] Test: Utility tests

#### Task 6e.4: Final Validation (3 hours)

- [ ] **Comprehensive Testing**
  - [ ] `make test` full suite
  - [ ] All 88 tests passing
  - [ ] No regressions

- [ ] **Zero NumPy Verification**
  - [ ] Grep for remaining `import numpy` in production code
  - [ ] Confirm zero imports in critical paths
  - [ ] Document any remaining NumPy in utilities/tests

### Success Criteria for Phase 6e
- ✓ All 88 tests passing
- ✓ Zero NumPy in `opteryx/vectors/embeddings.py`
- ✓ Zero NumPy in critical utilities
- ✓ Embedding functionality verified
- ✓ Performance maintained
- ✓ Code review approved

---

## FINAL VALIDATION (Post-Phase 6e)

- [ ] **Code Quality**
  - [ ] All tests passing: `make test` (88/88)
  - [ ] Minimum regression: `make q` (pass)
  - [ ] No warnings/errors in build
  - [ ] Code style consistent

- [ ] **Performance Validation**
  - [ ] ClickBench runs successfully
  - [ ] Query latency: no regression > 5%
  - [ ] Memory usage: ideally reduced
  - [ ] Startup time: ideally reduced

- [ ] **NumPy Elimination Verification**
  - [ ] Zero `import numpy` in `opteryx/compiled/`
  - [ ] Zero `import numpy` in `opteryx/expression/`
  - [ ] Zero `import numpy` in `opteryx/operators/`
  - [ ] Zero `import numpy` in `opteryx/types/`
  - [ ] Zero `import numpy` in `opteryx/vectors/embeddings.py`
  - [ ] NumPy may remain in: tests/, third_party/, utilities

- [ ] **Documentation**
  - [ ] Update README if needed
  - [ ] Document new buffer abstraction
  - [ ] Document migration patterns
  - [ ] Create post-mortem summary

- [ ] **Final Review**
  - [ ] Architecture review
  - [ ] Performance review
  - [ ] Correctness review
  - [ ] Merge to main

---

## Monitoring & Metrics

### Weekly Metrics (Track Throughout Phase 6)

| Week | Phase | Tests Passing | Effort Spent | Blockers | Notes |
|------|-------|--------------|-------------|----------|-------|
| 1 | 6a | 86/88 | ~3h | None | Type system foundation |
| 2-3 | 6b | 86/88 | ~45h | ? | Expression eval |
| 4-5 | 6c | 84-88/88 | ~40h | ? | Join operators |
| 5-6 | 6d | 88/88 | ~20h | None | Vector ops |
| 6-7 | 6e | 88/88 | ~25h | None | Utilities |

### Key Metrics to Track

- **Test Pass Rate:** Should remain 86+/88 (or 88/88 for phases 6d-6e)
- **Build Time:** Monitor for regressions due to buffer changes
- **Query Latency:** Benchmark with ClickBench
- **Memory Usage:** Monitor for leaks with buffer changes
- **Code Coverage:** Maintain or improve

---

## Risk Mitigation Strategies

### Expression Evaluation Risks
- [ ] Create comprehensive mask operation tests
- [ ] Side-by-side comparison testing (old vs new)
- [ ] Property-based testing for algorithm equivalence

### Join Operator Risks
- [ ] Create join correctness test suite first
- [ ] Gradual migration (one join at a time)
- [ ] Keep old code for reference during transition
- [ ] Property-based testing for index correctness

### Vector Operation Risks
- [ ] Extensive numerical validation against NumPy
- [ ] Property-based testing for vector math
- [ ] Performance regression testing
- [ ] Special case handling (zero vectors, etc.)

---

## Decision Log

| Date | Decision | Chosen Option | Rationale | Status |
|------|----------|---------------|-----------|--------|
| | Buffer abstraction | | | PENDING |
| | Linear algebra | | | PENDING |
| | Datetime representation | | | PENDING |
| | Test NumPy usage | | | PENDING |

---

## Blockers & Assumptions

### Known Blockers
- None yet (pending decisions)

### Assumptions
1. Expression evaluation algorithm can be replicated with Python lists/integers
2. Custom buffer types will have acceptable performance vs NumPy
3. Linear algebra functions can be implemented or wrapped
4. All tests will pass after migration

### Unknowns to Validate
- Performance impact of new buffer abstraction
- Cython compilation time with new types
- Vector search numerical stability
- Memory overhead of wrapper objects

---

## Communication Plan

- [ ] Weekly standup with architect
- [ ] Blockers escalation within 24 hours
- [ ] Major decision points require architect approval
- [ ] Code review required before each phase merge
- [ ] Post-mortem after Phase 6 completion

---

## Success Celebration Criteria

When Phase 6 is complete AND all 88 tests pass AND no NumPy in hot paths:

🎉 NumPy eradication complete!  
✨ Opteryx-core is now dependency-clean for execution engine  
🚀 Ready for next optimization phases (SIMD, vectorization, etc.)

---

**Last Updated:** 2025  
**Status:** Ready for Architect Review  
**Next Step:** Architect approves decisions → Begin Phase 6a