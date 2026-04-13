# NumPy & PyArrow Removal Action Plan

**Document Version:** 1.0  
**Status:** Ready for Implementation  
**Last Updated:** 2024  
**Architect Sign-Off Required:** YES

---

## Executive Summary

This document outlines the strategic plan to remove NumPy and PyArrow dependencies from opteryx-core's hot execution paths, replacing them with native Draken/C++ implementations. The effort is structured into 4 phases with clear success criteria and risk mitigation strategies.

**Business Justification:**
- NumPy/PyArrow allocations currently occupy ~50 LOC in critical execution paths
- Estimated 2-5% query latency improvement by eliminating GC pressure
- Alignment with architectural goal: "Performance > convenience"
- Foundation for future optimizations (SIMD, better memory layout)

**High-Level Timeline:** 8-10 weeks (full execution)
**Estimated Team Effort:** 320 person-hours
**Risk Level:** MEDIUM (well-scoped, testable components)

---

## Phase 1: Foundation & Quick Wins (Week 1-2)

### Objective
Build infrastructure for buffer pooling and establish proof-of-concept for Draken replacement patterns. Collect baseline performance metrics.

### Tasks

#### 1.1: Baseline Performance Instrumentation
**Owner:** Performance Engineering  
**Effort:** 1.5 days  
**Risk:** LOW

**Deliverables:**
- [ ] Query execution timer with allocation tracking
- [ ] Memory profiling hooks in hot paths (joins, expression eval)
- [ ] ClickBench baseline with NumPy/PyArrow metrics

**Success Criteria:**
- Can measure allocation count per query
- Can compare memory usage before/after refactoring
- Baseline established (run `make clickbench` and record allocations)

**Dependencies:** None

---

#### 1.2: Draken Buffer Pool Infrastructure
**Owner:** Core Infrastructure  
**Effort:** 2 days  
**Risk:** LOW

**Deliverables:**
- [ ] Create `opteryx/compiled/structures/buffer_pool.pyx`
- [ ] Implement thread-safe pool for IntBuffer, ObjectBuffer
- [ ] Add allocation/release tracking telemetry

**Code Structure:**
```cython
# buffer_pool.pyx (new)
cdef class BufferPool:
    cdef object int_buffers
    cdef object obj_buffers
    
    cpdef IntBuffer acquire_int_buffer(self, size_t initial_size)
    cpdef void release_int_buffer(self, IntBuffer buf)
    cpdef void clear(self)  # thread-safe reset
```

**Success Criteria:**
- IntBuffer pool working in single-threaded context
- Memory reuse validated (no allocation on acquire)
- Telemetry shows pool hit rate >= 90%

**Dependencies:** buffers.pyx (existing)

---

#### 1.3: Type Checking Utility Wrapper
**Owner:** Expression Engine  
**Effort:** 1 day  
**Risk:** LOW

**Deliverables:**
- [ ] Create `opteryx/expression/type_checker.py`
- [ ] Unified type checking functions replacing try/except guards
- [ ] Update imports in _null_handling.py

**Code Example:**
```python
# type_checker.py (new)
def is_numpy_type(value, dtype):
    """Safe type check without try/except guard."""
    # Avoid circular imports; check module name
    return value.__class__.__module__.startswith('numpy')

def is_pyarrow_type(value):
    """Safe PyArrow type check."""
    return value.__class__.__module__.startswith('pyarrow')
```

**Success Criteria:**
- All try/except guards removed from _null_handling.py
- Tests pass; no behavior change
- Code is cleaner (fewer lines)

**Dependencies:** None

---

#### 1.4: Logical Operation Inline Refactor (Quick Win)
**Owner:** Expression Engine  
**Effort:** 0.5 days  
**Risk:** LOW

**Deliverables:**
- [ ] Replace `pyarrow.compute.and_()`, `or_()`, `xor()` with inline bitwise ops
- [ ] Update LOGICAL_OPERATIONS in expression/__init__.py

**Before:**
```python
LOGICAL_OPERATIONS: Dict[NodeType, Callable] = {
    NodeType.AND: pyarrow.compute.and_,
    NodeType.OR: pyarrow.compute.or_,
    NodeType.XOR: pyarrow.compute.xor,
}
```

**After:**
```python
def _bitwise_and(left, right):
    """Inline AND operation."""
    if hasattr(left, '__and__'):
        return left & right
    return [l and r for l, r in zip(left, right)]  # fallback

LOGICAL_OPERATIONS: Dict[NodeType, Callable] = {
    NodeType.AND: _bitwise_and,
    NodeType.OR: lambda l, r: l | r if hasattr(l, '__or__') else [x or y for x, y in zip(l, r)],
    NodeType.XOR: lambda l, r: l ^ r if hasattr(l, '__xor__') else [x ^ y for x, y in zip(l, r)],
}
```

**Success Criteria:**
- Logical operations pass all expression tests
- No performance regression (should improve slightly)
- PyArrow compute import optional

**Dependencies:** None

---

#### 1.5: Regex Utility .to_numpy() Removal (Quick Win)
**Owner:** SQL Utilities  
**Effort:** 0.5 days  
**Risk:** LOW

**Deliverables:**
- [ ] Replace `.to_numpy()` with Arrow buffer access in sql.py:205-215
- [ ] Use ArrowBuffer abstraction or direct buffer pointer

**Before:**
```python
offsets = chunk.offsets.to_numpy()
validity = chunk.is_valid().to_numpy(False)
```

**After:**
```python
# Use Arrow buffer directly
offsets_buffer = chunk.offsets.buffers()[1]  # offset buffer
validity_buffer = chunk.is_valid().buffers()[0]  # validity buffer
# Work with memory views instead
```

**Success Criteria:**
- Regex matching tests pass
- No performance regression
- Cleaner code (fewer conversions)

**Dependencies:** None

---

**Phase 1 Completion Criteria:**
- All 5 tasks complete
- `make test` passes (full regression suite)
- Baseline metrics collected and documented
- Team alignment on Phase 2 approach

---

## Phase 2: Medium-Effort Optimizations (Week 3-4)

### Objective
Refactor cast operations and string/array functions to use buffer pooling and Draken vectors. No breaking changes to public APIs.

### Tasks

#### 2.1: Cast Operation Buffer Pooling
**Owner:** Type System  
**Effort:** 2 days  
**Risk:** MEDIUM

**Scope:** `opteryx/expression/casts.py` - Lines 240-257, 316-335

**Changes:**
- [ ] Replace `numpy.array()` wraps with Draken vector constructors
- [ ] Pre-allocate buffers for known-size conversions
- [ ] Use vector_from_arrow() for Arrow→Draken transitions

**Before (Line 245-246):**
```python
result = format_double_func(arr)
return numpy.array(result.to_pylist(), dtype=object)
```

**After:**
```python
from opteryx.compiled.draken.interop.arrow import to_arrow
result = format_double_func(arr)  # Returns StringVector (already Draken)
return result.to_arrow()  # Direct conversion, no intermediate array
```

**Affected Functions:**
- `_cast_to_binary_representation()` - 3 instances
- `cast_to_double()` - 2 instances

**Success Criteria:**
- Cast operations 10-20% faster
- All cast tests pass
- No behavioral changes (values identical)
- Memory allocation reduced by 50%

**Testing:**
```bash
# Run cast-specific tests
python -m pytest tests/test_casts.py -v
# Benchmark casting-heavy queries
make clickbench | grep -i cast
```

**Dependencies:** 
- Phase 1 completion
- Draken vector type system understanding

**Risk Mitigation:**
- Keep fallback to numpy.array() for edge cases
- Run comprehensive type casting regression suite
- Compare output values byte-for-byte

---

#### 2.2: Expression Literal Array Pool
**Owner:** Expression Engine  
**Effort:** 2 days  
**Risk:** MEDIUM

**Scope:** `opteryx/expression/__init__.py` - Lines 429-445

**Changes:**
- [ ] Create constant buffer pool for literal replication
- [ ] Reuse buffers for repeated literal broadcasts
- [ ] Track pool stats (hits, misses, reallocation)

**New Infrastructure:**
```python
# expression/__init__.py (add to top-level)
class LiteralBufferPool:
    def __init__(self, pool_size=100):
        self.pools = {}  # type -> buffer list
        self.pool_size = pool_size
    
    def get_constant_buffer(self, value, length, dtype):
        """Get or create buffer for constant replication."""
        key = (value, length, dtype)
        if key in self.pools:
            return self.pools[key]
        # Create new buffer
        buffer = create_constant_array(value, length, dtype)
        if len(self.pools) < self.pool_size:
            self.pools[key] = buffer  # cache
        return buffer

_literal_pool = LiteralBufferPool()
```

**Usage Sites:**
- Line 429: INTERVAL constant broadcasting
- Line 439: Generic literal replication
- Line 441+: Timestamp/DATE literals

**Success Criteria:**
- Constant replication 15-30% faster
- Pool hit rate >= 60% on realistic workloads
- All expression evaluation tests pass
- Memory usage for constants reduced

**Testing:**
```bash
# Test constant folding
python -m pytest tests/test_expressions.py::test_constant_* -v
# Profile pool behavior
python -c "from opteryx import session; s = session(); \
    for _ in range(100): s.execute('SELECT 1 WHERE 1=1')"
```

**Dependencies:**
- Phase 1 buffer infrastructure
- Draken vector factory functions

**Risk Mitigation:**
- Conservative pool sizing (start small)
- Metrics collection for cache effectiveness
- Can disable pooling via config for debugging

---

#### 2.3: Vector Split Array Construction (Zero-Copy)
**Owner:** Vector Operations  
**Effort:** 2 days  
**Risk:** MEDIUM-HIGH

**Scope:** `opteryx/compiled/vector_ops/vector_split.pyx` - Lines 191-259

**Current State:** Mixes `pa.array()` calls with foreign_buffer pattern

**Changes:**
- [ ] Consolidate all result paths to use foreign_buffer
- [ ] Eliminate intermediate Python lists for small results
- [ ] Ensure zero-copy for all cases

**Current (Lines 191-202):**
```cython
if n <= 0:
    return pa.array([], type=pa.list_(pa.binary()))
if vec._const_is_null or vec._const_value == NULL:
    return pa.array([None] * n, type=pa.list_(pa.binary()))
```

**Target (Lines 191-202):**
```cython
if n <= 0:
    # Zero-copy empty list array
    return _create_empty_list_array(pa.binary())
if vec._const_is_null or vec._const_value == NULL:
    # Use constant buffer pattern
    return _create_constant_list_array(None, n)
```

**New Helper Functions (add to module):**
```cython
cdef object _create_empty_list_array(object child_type):
    """Create empty list array without pa.array() overhead."""
    cdef object empty_buf = pa.foreign_buffer(b'', 0, base=None)
    return pa.Array.from_buffers(
        pa.list_(child_type), 0,
        [None, empty_buf]
    )

cdef object _create_constant_list_array(object value, int64_t n):
    """Create list array of constant values."""
    # Implementation uses buffer pool
    pass
```

**Success Criteria:**
- String split operations 15-25% faster
- No intermediate Python lists for common cases
- All vector_split tests pass
- Memory peak usage during split reduced

**Testing:**
```bash
# Benchmark string operations
python -c "from opteryx import session; s = session(); \
    result = s.execute('SELECT SPLIT(col, \",\") FROM data')"
# Memory profiling
python -m memory_profiler test_vector_split.py
```

**Dependencies:**
- Phase 1 buffer infrastructure
- Understanding of PyArrow foreign buffer API

**Risk Mitigation:**
- Keep fallback to pa.array() for complex cases
- Extensive benchmarking (compare with baseline)
- Validate results match exactly (bit-for-bit)

---

#### 2.4: Null Handling Utility Refactor
**Owner:** Type System  
**Effort:** 1 day  
**Risk:** LOW

**Scope:** `opteryx/types/_null_handling.py` - Remove try/except guards

**Changes:**
- [ ] Replace try/except-guarded numpy checks with pyarrow-only
- [ ] Simplify is_nan(), is_inf(), is_null() functions
- [ ] Update docstrings

**Before (Lines 85-92):**
```python
def is_null(value: Any) -> bool:
    # ... native float check ...
    try:
        import numpy as np
        if isinstance(value, np.floating):
            return np.isnan(value)
        if value is np.nan:
            return True
    except ImportError:
        pass
    # ... rest of function ...
```

**After:**
```python
def is_null(value: Any) -> bool:
    # ... native float check ...
    # NumPy checks removed; rely on pyarrow.Scalar checks
    try:
        import pyarrow as pa
        if isinstance(value, pa.Scalar):
            return not value.is_valid
    except ImportError:
        pass
    return False
```

**Success Criteria:**
- All null-checking tests pass
- Code is simpler (fewer lines, fewer imports)
- No performance regression
- Dependency on NumPy removed from this module

**Testing:**
```bash
python -m pytest tests/test_null_handling.py -v
```

**Dependencies:** Phase 1 type checker utility

**Risk Mitigation:**
- Test with various Python numeric types
- Ensure backward compatibility

---

**Phase 2 Completion Criteria:**
- All 4 tasks complete
- `make test` passes
- Performance benchmarks show improvements (5-10% aggregate)
- Memory allocation reduced by 20-30%

---

## Phase 3: Hot Path Refactoring (Week 5-7)

### Objective
Refactor critical join operations to eliminate NumPy allocations. This is the highest-impact, highest-complexity work.

### Tasks

#### 3.1: UNNEST Cross-Join Buffer Refactor
**Owner:** Query Execution / Joins  
**Effort:** 4 days  
**Risk:** HIGH

**Scope:** `opteryx/compiled/joins/cross_join.pyx` - ~250 lines

**Current State:**
- Lines 48-58: `numpy.empty()` for row indices and data
- Lines 113-118: Length/offset arrays for UNNEST calculation
- Lines 162-199: Filtered cross-join with dynamic reallocation

**Problem:** Multiple buffer allocations per UNNEST operation; reallocation in tight loops

**Solution:** Pre-allocate Draken buffers; use growth strategy instead of numpy.resize()

**Changes:**
1. Import buffer pool infrastructure
2. Acquire buffers from pool instead of numpy.empty()
3. Use exponential growth instead of numpy.resize()
4. Return buffers to pool on scope exit

**New Code Structure:**
```cython
# cross_join.pyx modifications
from opteryx.compiled.structures.buffer_pool cimport BufferPool

cdef BufferPool buffer_pool = BufferPool()

cpdef tuple build_rows_indices_and_column(object column):
    """Refactored UNNEST - uses buffer pool."""
    cdef IntBuffer indices_buf = buffer_pool.acquire_int_buffer(100)
    cdef ObjectBuffer flat_data_buf = buffer_pool.acquire_object_buffer(100)
    
    try:
        # Process column, resizing as needed
        # ... populate buffers ...
        return indices_buf.to_numpy(), flat_data_buf.to_numpy()
    finally:
        buffer_pool.release_int_buffer(indices_buf)
        buffer_pool.release_object_buffer(flat_data_buf)
```

**Sub-Tasks:**
- [ ] 3.1.1: Refactor `build_rows_indices_and_column()` (1 day)
- [ ] 3.1.2: Refactor `numpy_build_rows_indices_and_column()` (1 day)
- [ ] 3.1.3: Refactor `numpy_build_filtered_rows_indices_and_column()` (1.5 days)
- [ ] 3.1.4: Refactor `build_filtered_rows_indices_and_column()` (0.5 days)

**Testing Strategy:**
```bash
# UNNEST-specific tests
python -m pytest tests/test_unnest.py -v
# Query with UNNEST
make b  # Run current test query (brace.py)
# Benchmark UNNEST-heavy workload
python benchmark_unnest.py
```

**Success Criteria:**
- All UNNEST tests pass (output identical to baseline)
- UNNEST operations 2-5% faster
- Memory allocations reduced by 80% (from 5+ per UNNEST to 1)
- GC pressure reduced (fewer objects for GC to scan)
- Peak memory during UNNEST lower

**Risk & Mitigation:**
| Risk | Probability | Mitigation |
|------|-------------|-----------|
| Buffer lifetime issues | MEDIUM | Comprehensive buffer lifecycle tests |
| Index calculation errors | MEDIUM | Unit tests for offset/length calculation |
| Memory leak from unreleased buffers | LOW | Finally-block cleanup + telemetry |

**Code Review Checklist:**
- [ ] All numpy.empty() calls replaced
- [ ] All numpy.resize() calls replaced
- [ ] Buffer release in all code paths (including exceptions)
- [ ] No dangling pointers to freed buffers
- [ ] Allocation count < 2 per UNNEST (was 5+)

**Dependencies:**
- Phase 1 buffer pool (CRITICAL)
- Phase 2 completion
- Draken buffer API understanding

---

#### 3.2: Inner Join Hash Table Refactor
**Owner:** Query Execution / Joins  
**Effort:** 2 days  
**Risk:** MEDIUM

**Scope:** `opteryx/compiled/joins/inner_join.pyx` - Lines 175-230

**Current State:**
- Line 175-178: `numpy.asarray()` calls for hash table row batch insertion
- Line 208-215: Probe hash array construction with asarray()
- Line 225-230: Result array type casting with asarray()

**Changes:**
- [ ] Use direct C++ array pointers instead of numpy.asarray()
- [ ] Verify types at construction time, not conversion time
- [ ] Inline type coercion where possible

**Before (Line 175-178):**
```cython
ht.insert_batch(
    numpy.asarray(row_hashes)[numpy.asarray(non_null_indices, dtype=numpy.int64)],
    numpy.asarray(non_null_indices, dtype=numpy.int64),
)
```

**After:**
```cython
# Verify types upfront
assert isinstance(row_hashes, (list, tuple, object))
hash_array = _as_int64_array(row_hashes)  # Custom coercion
idx_array = _as_int64_array(non_null_indices)
ht.insert_batch(hash_array, idx_array)
```

**New Helper (add to inner_join.pyx):**
```cython
cdef inline int64_t[::1] _as_int64_array(object arr):
    """Convert to int64 memoryview without numpy.asarray() overhead."""
    if isinstance(arr, list):
        return numpy.array(arr, dtype=numpy.int64)  # One allocation
    elif hasattr(arr, 'dtype'):
        if arr.dtype != numpy.int64:
            return arr.astype(numpy.int64)
        return arr
    else:
        raise TypeError(f"Cannot convert {type(arr)} to int64")
```

**Success Criteria:**
- Inner join tests pass
- Join operations 2-4% faster (fewer allocations)
- Hash table insertion unchanged (functional equivalence)
- Code cleaner (fewer redundant asarray calls)

**Testing:**
```bash
python -m pytest tests/test_joins.py::test_inner_join -v
make clickbench | grep -i join
```

**Dependencies:**
- Phase 1 completion
- Phase 2 completion

---

#### 3.3: Hash Operations Type Dispatch (Optional Optimization)
**Owner:** Query Execution / Joins  
**Effort:** 1 day  
**Risk:** LOW

**Scope:** `opteryx/compiled/table_ops/hash_ops.pyx` - Lines 27-44

**Current State:** Uses `pyarrow.types.is_*()` calls for type dispatch (already efficient, but can add wrapper)

**Changes:**
- [ ] Keep existing pyarrow.types calls (they're compiled efficiently)
- [ ] Add telemetry for type dispatch hot spots
- [ ] Document that these are NOT removal targets (they're fast)

**Note:** This is a documentation/telemetry task, not a refactoring task. Already well-optimized.

**Deliverable:**
- [ ] Add comment explaining why pyarrow.types calls are retained
- [ ] Add optional telemetry hook for type dispatch counting

**Success Criteria:**
- Code is well-documented
- Team understands this is intentionally kept
- Telemetry helps identify any unexpected hot spots

**Dependencies:** None (independent task)

---

**Phase 3 Completion Criteria:**
- All join refactoring complete
- `make test` passes with 100% match to baseline
- Join operations 2-5% faster (measured via ClickBench)
- Memory allocations in hot path reduced by 80%
- No regressions in correctness

---

## Phase 4: Validation & Cleanup (Week 8-10)

### Objective
Comprehensive testing, performance validation, and documentation of changes.

### Tasks

#### 4.1: End-to-End Performance Benchmark
**Owner:** Performance Engineering  
**Effort:** 2 days  
**Risk:** LOW

**Deliverables:**
- [ ] Run ClickBench with full instrumentation (before/after)
- [ ] Compile performance report (latency, throughput, memory)
- [ ] Identify any regressions or unexpected behavior
- [ ] Document performance improvements by query type

**Benchmark Queries to Track:**
- Joins (inner, outer, cross, unnest)
- Aggregations with GROUP BY
- String operations (SPLIT, LIKE, regex)
- Type casting operations
- Expression evaluation

**Success Criteria:**
- Overall query latency improved by 2-5%
- Join latency improved by 3-8%
- String operation latency improved by 10-25%
- No regressions in any query category
- Memory usage reduced by 15-30%

**Testing:**
```bash
make clickbench 2>&1 | tee benchmark_post_refactor.txt
diff benchmark_baseline.txt benchmark_post_refactor.txt
```

---

#### 4.2: Memory & Allocation Profiling
**Owner:** Core Infrastructure  
**Effort:** 1.5 days  
**Risk:** LOW

**Deliverables:**
- [ ] Profile query execution with memory_profiler
- [ ] Verify NumPy/PyArrow allocations are minimal
- [ ] Check for memory leaks (buffer pool exhaustion)
- [ ] Document allocation patterns

**Profiling Tools:**
```bash
# Line-by-line memory profiling
python -m memory_profiler query_test.py

# Allocation tracking
python -c "
import tracemalloc
tracemalloc.start()
# ... run query ...
current, peak = tracemalloc.get_traced_memory()
print(f'Current: {current / 1024 / 1024}MB; Peak: {peak / 1024 / 1024}MB')
"
```

**Success Criteria:**
- Peak memory usage reduced by 15-30%
- Allocation count reduced by 80% in hot paths
- No memory leaks detected
- Buffer pool efficiency >= 80%

---

#### 4.3: Regression Test Suite Expansion
**Owner:** QA  
**Effort:** 2 days  
**Risk:** MEDIUM

**Deliverables:**
- [ ] Add 50+ new test cases for buffer pool operations
- [ ] Add edge case tests (empty results, null values, large batches)
- [ ] Stress test buffer lifecycle (reuse, exhaustion, cleanup)
- [ ] Fuzz test cast operations with random inputs

**New Test Files:**
- `tests/test_buffer_pool.py` - Pool lifecycle and reuse
- `tests/test_unnest_refactored.py` - UNNEST-specific edge cases
- `tests/test_cast_operations.py` - Cast operation correctness
- `tests/test_stress_allocations.py` - High-volume buffer usage

**Success Criteria:**
- All new tests pass
- 100% code coverage of buffer pool
- Edge cases well-tested
- No crashes or memory errors under stress

**Testing:**
```bash
make test  # Full regression suite
python -m pytest tests/test_buffer_pool.py -v --cov
python -m pytest tests/test_stress_allocations.py -v
```

---

#### 4.4: Dependency Impact Analysis
**Owner:** Architecture  
**Effort:** 1 day  
**Risk:** LOW

**Deliverables:**
- [ ] Document remaining NumPy/PyArrow usage
- [ ] Identify next removal candidates (if any)
- [ ] Update dependency documentation
- [ ] Create removal roadmap for future phases

**Analysis Scope:**
- Third-party modules (acceptable usage)
- Remaining expression evaluation NumPy calls
- I/O layer PyArrow usage (intentional)
- Future candidates for removal

**Success Criteria:**
- Clear documentation of what remains and why
- Roadmap for Phase 5+ (if warranted)
- Team alignment on remaining dependencies

---

#### 4.5: Documentation & Handoff
**Owner:** Tech Writer / Architect  
**Effort:** 1.5 days  
**Risk:** LOW

**Deliverables:**
- [ ] Update ARCHITECTURE.md with Draken buffer pool info
- [ ] Document buffer lifecycle and pool API
- [ ] Create migration guide for future NumPy removals
- [ ] Update performance tuning guide

**Documents to Update:**
- `docs/architecture/execution_engine.md`
- `docs/performance_tuning.md`
- `DEVELOPMENT.md` (buffer management section)
- Code comments (explain why certain calls were removed)

**Success Criteria:**
- Clear documentation of changes
- Future engineers can understand buffer pool usage
- Performance recommendations documented

---

**Phase 4 Completion Criteria:**
- All benchmarks run and documented
- Zero regressions
- New test suite passes
- Documentation updated
- Handoff to team complete
- Ready for production deployment

---

## Success Metrics & KPIs

### Primary Metrics (Must Achieve)
| Metric | Target | Measurement |
|--------|--------|-------------|
| Query Latency Improvement | 2-5% | ClickBench aggregate |
| Join Latency Improvement | 3-8% | ClickBench join-heavy queries |
| Memory Allocation Reduction | 80% in hot paths | Instrumentation |
| Test Pass Rate | 100% | `make test` |
| Performance Regression | 0% | Query-by-query comparison |

### Secondary Metrics (Nice to Have)
| Metric | Target | Measurement |
|--------|--------|-------------|
| GC Pause Time | -30% | Tracing GC behavior |
| Memory Peak Usage | -20% | Peak resident set size |
| Code Maintainability | Improved | Reduced LOC, clearer logic |
| NumPy/PyArrow Refs | <30 | Grepped from codebase |

### Telemetry & Monitoring
**Instrumentation Points:**
- Buffer pool allocation/deallocation
- Cache hit rate (literal pools)
- Query latency by category
- Memory usage over time

**Dashboard / Reporting:**
```python
# Post-refactor validation script
import opteryx
from opteryx.utils import telemetry

session = opteryx.session()
for query in BENCHMARK_QUERIES:
    metrics = session.execute_with_metrics(query)
    print(f"{query}: {metrics.latency_ms}ms, {metrics.allocations} allocs")
```

---

## Risk Assessment & Mitigation

### Risk 1: Correctness Regression (HIGH PROBABILITY, HIGH IMPACT)
**Risk:** Buffer management changes introduce subtle bugs (off-by-one errors, null pointer derefs)

**Mitigation:**
- Comprehensive unit tests for each buffer operation
- Byte-for-byte output comparison vs baseline
- Memory sanitizer (ASAN) enabled in CI
- Staged rollout (feature flag for buffer pool)

**Contingency:** Rollback to baseline (Phase 1-2 work is reversible)

---

### Risk 2: Performance Not Meeting Targets (MEDIUM PROBABILITY, HIGH IMPACT)
**Risk:** Refactoring doesn't yield expected 2-5% improvement

**Mitigation:**
- Continuous benchmarking during each phase
- Profiling data to identify bottlenecks
- Alternative optimization paths (SIMD, better algorithms)
- Prioritize highest-ROI changes first

**Contingency:** Stop after Phase 2; report findings; plan Phase 5

---

### Risk 3: Buffer Pool Exhaustion (LOW PROBABILITY, MEDIUM IMPACT)
**Risk:** Concurrent queries exhaust buffer pool; fallback to numpy allocation

**Mitigation:**
- Generous pool sizing (start with 10x expected concurrent buffers)
- Metrics collection for pool usage
- Telemetry alerts if pool exhaustion occurs
- Graceful fallback to numpy if pool exhausted

**Contingency:** Increase pool size dynamically; investigate contention

---

### Risk 4: Integration Issues (MEDIUM PROBABILITY, MEDIUM IMPACT)
**Risk:** Changes to core execution paths break downstream code

**Mitigation:**
- Careful API boundaries (buffer interface unchanged)
- Thorough integration tests
- Staged rollout (feature flag)
- Revert plan if needed

**Contingency:** Disable buffer pool; revert to numpy

---

## Rollout Strategy

### Feature Flags
```python
# config.py (new)
ENABLE_BUFFER_POOL = env('ENABLE_BUFFER_POOL', default=True)
ENABLE_LITERAL_POOL = env('ENABLE_LITERAL_POOL', default=True)
ENABLE_ZERO_COPY_SPLIT = env('ENABLE_ZERO_COPY_SPLIT', default=True)
ENABLE_JOIN_REFACTOR = env('ENABLE_JOIN_REFACTOR', default=True)
```

### Deployment Plan
1. **Week 1-2 (Phase 1):** Deploy quick wins + infrastructure
2. **Week 3-4 (Phase 2):** Deploy cast & expression improvements
3. **Week 5-7 (Phase 3):** Deploy join refactoring (behind feature flag)
4. **Week 8-10 (Phase 4):** Validate, enable by default, monitor

### Monitoring
- Query latency (by category)
- Memory usage (peak, average)
- Error rate (should stay at 0)
- Feature flag usage

---

## Team Assignments (Proposed)

| Phase | Tasks | Team | Lead | Effort |
|-------|-------|------|------|--------|
| 1 | 1.1-1.5 | Infrastructure | Alice | 8 days |
| 2 | 2.1-2.4 | Expression/Types | Bob | 8 days |
| 3 | 3.1-3.3 | Query Execution | Carol | 10 days |
| 4 | 4.1-4.5 | QA/Perf/Docs | Dave | 8 days |

**Cross-Functional:**
- Architect oversight (weekly sync)
- Performance engineering (continuous benchmarking)
- QA (test case development)

---

## Communication Plan

### Status Updates
- **Weekly standup:** Monday 10am (15 min)
- **Bi-weekly demo:** Thursday 2pm (30 min)
- **Performance review:** After each phase (1 hour)

### Documentation
- **Slack channel:** #numpy-pyarrow-refactor
- **Issue tracker:** Opteryx-Core/Features/NumPy-Removal
- **Design docs:** Shared in Confluence/Google Drive

### Stakeholder Communication
- **Week 1:** Kick-off meeting + roadmap review
- **Week 4:** Mid-project status + Phase 2 review
- **Week 10:** Final review + production deployment plan

---

## Exit Criteria & Sign-Off

### All Phases Complete When:
- [ ] All tasks completed and code reviewed
- [ ] `make test` passes (100% green)
- [ ] `make clickbench` shows 2-5% improvement
- [ ] No performance regressions
- [ ] Memory usage reduced by 15-30%
- [ ] Documentation updated
- [ ] Team trained on new infrastructure
- [ ] Architect sign-off obtained

### Production Readiness Checklist
- [ ] All feature flags enabled by default
- [ ] Monitoring/telemetry in place
- [ ] Rollback plan documented
- [ ] On-call team trained
- [ ] Incident response plan ready

---

## Appendix: Reference Materials

### Key Files to Study
- `opteryx/compiled/structures/buffers.pyx` - Existing buffer classes
- `opteryx/compiled/structures/buffer_pool.pyx` - To be created
- `opteryx/compiled/joins/cross_join.pyx` - Main refactoring target
- `opteryx/expression/__init__.py` - Expression evaluation changes

### External References
- Draken Documentation: `/docs/draken/vectors.md`
- PyArrow Foreign Buffers: https://arrow.apache.org/docs/python/memory.html
- Cython Memory Management: https://cython.readthedocs.io/en/latest/src/tutorial/memory_allocation.html

### Tools & Resources
- ClickBench: `make clickbench`
- Memory Profiler: `pip install memory-profiler`
- Valgrind: For leak detection
- Flamegraph: For performance profiling

---

**Document Owner:** Architecture Team  
**Last Review Date:** [Today]  
**Next Review:** After Phase 1 completion  
**Version History:**
- v1.0 (Initial) - All phases outlined, ready for implementation
