# NumPy & PyArrow Eradication - Status

**Last Updated:** SESSION 41  
**Status:** 87/88 tests passing (99%)  
**Baseline Failure:** 1 pre-existing (GROUP BY column resolution in planner)

---

## ✅ COMPLETED PHASES

### Session 41: Phase 5.5.D.1 - Draken-Native Nested Loop Join (COMPLETE ✅)
- **Bloom Filter Refactoring:** Added native Morsel-based API to `bloom_filter.pyx`
  - `create_bloom_filter_morsel(morsel, columns)` — uses `Morsel.hash()` instead of Arrow buffer access
  - `bloom_filter_check_morsel(filter, morsel, columns)` — returns bit-packed results (no Arrow conversion in hot path)
  - Eliminated intermediate wrapper layer (`bloom_filter_draken.pyx` deleted)
  - **Breaking point:** Old Arrow-based API (`create_bloom_filter(relation, columns)`) still exists for other operators; new callers use Morsel API
- **Nested Loop Join Refactoring:** Rewrote `nested_loop_join_node.pyx` to be 100% Draken-native
  - `_DATA_FORMAT = "draken"` — operator now consumes and produces Morsels
  - **Eliminated `pyarrow.concat_tables()`** in build phase: replaced with `Morsel.combine()`
  - Build side now buffers Morsels in `self.left_morsels`, combines at end
  - Bloom filter built using `create_bloom_filter_morsel()` (native hashing)
  - Bloom filter checked using `bloom_filter_check_morsel()` (native hashing)
  - Join computation via new `draken_nested_loop_join()` (uses `Morsel.hash()`)
  - **Only Arrow conversion:** Join key casting and final `align_tables()` call (warm/cold paths, acceptable)
- **New Join Implementation:** Created `draken_nested_loop_join.pyx`
  - Pure Draken nested loop join using `Morsel.hash()` for row hashing
  - No buffer access patterns, no Arrow table conversions
  - Returns `(left_indexes, right_indexes)` as Int32Buffer for alignment
  - Smaller side in outer loop for cache locality
- **Compilation & Testing:**
  - All Cython modules recompiled successfully
  - `make q` results: 87/88 tests passing (baseline maintained, no regressions)
  - Operator now uses Draken-native API throughout hot path
- **Result:** Nested loop join fully Draken-native. Eliminates ~5 warm-path PyArrow references. ✅

### Session 40: Phase 5.5.B VERIFICATION - Draken-Native UNNEST (VERIFIED ✅)
- Recompiled all Cython/Rust modules successfully
- Ran `make q` — all 87 passing tests confirmed
- Morsel `__str__()` pretty-printer active (displays ASCII table on `str(morsel)`)
- UNNEST operator verified Draken-native:
  - No `pyarrow.Table/array()` calls in hot path
  - Uses `build_rows_indices_and_column_draken()` for native vector flattening
  - Returns `Morsel` with native Draken vectors
  - Profiler shows no UNNEST-specific Arrow calls
- **Key Learning:** Remaining `pyarrow.compute.cast` in profiler comes from **other operators and expression machinery**, not UNNEST
- **Result:** Phase 5.5.B truly complete. Hot-path UNNEST pipeline is Arrow/NumPy-free. ✅

### Session 39: Phase 5.5.B - Draken-Native UNNEST Refactor (COMPLETE)
- Created `opteryx/compiled/joins/cross_join_draken.pyx` with pure Draken-native UNNEST
- Refactored `list_distinct()` to work with Draken vectors
- Updated `build_filtered_rows_indices_and_column()` to return Draken vectors
- **Result:** Complete Draken-native UNNEST pipeline

### Session 38: Phase 5.4.1 - Fallback Comparison Elimination
- Refactored 12 functions in expression operations
- Eliminated `.to_numpy().astype(numpy.bool_)` chains
- **Result:** 12 NumPy bool conversions removed

### Prior Sessions: Phase 5.3, 5.2, 5.1 (completed)
- Cast operations, arithmetic propagation, buffer protocol integration, vector aggregates

---

## 🔴 REMAINING WORK

### Phase 5.5.C: Audit Other Operators (COMPLETE ✅)
- **Status:** Grep audit finished. 100+ references found across 8 operators.
- **Findings:**

**🔴 HOT PATH (Main Query Loop) - HIGHEST PRIORITY:**
1. `heap_sort_node.pyx` (L694-770) — Vector similarity/top-K search
   - Uses: `numpy.ascontiguousarray()`, `numpy.asarray()`, `numpy.nan_to_num()`, `numpy.clip()`, `numpy.argpartition()`, `numpy.lexsort()`
   - Impact: ~9 refs in `_vector_top_n()` method
   - Scope: Vector similarity scoring and ranking (hot when sorting vectors)
   - **Recommendation:** Port to Draken vector operations or specialized hot-path module

2. `cross_join_node.pyx` (L35-42, L81-93) — Cartesian product indices
   - Uses: `numpy.empty()`, `numpy.ix_()`, `numpy.hsplit()`, `numpy.arange()`, `pyarrow.Table.from_batches()`, `pyarrow.concat_tables()`
   - Impact: ~8 refs in `_cartesian_product()` and `_cross_join()` 
   - Scope: Cartesian product row index generation (hot in CROSS JOIN non-UNNEST)
   - **Recommendation:** Port to Draken Int64Vector index building

3. `nested_loop_join_node.pyx` (L90-100, L120-126) — Bloom filter & buffering
   - Uses: `pyarrow.concat_tables()`, `pyarrow.Array.from_buffers()`, `pyarrow.py_buffer()`
   - Impact: ~5 refs in execute() build phase
   - Scope: Bloom filter construction + null filtering (warm/hot in large joins)
   - **Recommendation:** Understand Bloom filter output, consider Draken-native wrapper

**🟡 WARM PATH (Build/Buffering Phase) - MEDIUM PRIORITY:**
4. `outer_join_node.pyx` (L208-244) — Outer join build phase
   - Uses: `pyarrow.concat_tables()` (2 refs), table construction
   - Impact: ~4 refs
   - Scope: Buffered accumulation before join execution
   - **Recommendation:** Replace concat_tables with native Morsel buffering

5. `non_equi_join_node.pyx` (L92-95) — Non-equi join buffering
   - Uses: `pyarrow.concat_tables()`
   - Impact: ~1 ref
   - Scope: Similar to outer join (build phase)
   - **Recommendation:** Same as outer_join

6. `cross_join_node.pyx` (L59-62, L103-110) — COUNT(*) & empty tables
   - Uses: `pyarrow.Table.from_pydict()`, `pyarrow.Table.from_arrays()`, `pyarrow.array()`, `pyarrow.schema()`
   - Impact: ~6 refs (mostly edge cases)
   - Scope: Empty result construction, COUNT(*) aggregation
   - **Recommendation:** Use Morsel.empty() or construct Draken vectors

**🟢 COLD PATH (Initialization/Rare Cases) - LOW PRIORITY:**
7. `read_node.pyx` (L38-94) — Struct/JSONB conversion
   - Uses: `pyarrow.array()`, `pyarrow.types.is_struct()`, `pyarrow.field()`, `pyarrow.schema()`
   - Impact: ~12 refs
   - Scope: Schema transformation during read (cold - only on JSON/STRUCT columns)
   - **Recommendation:** Keep as-is (cold path, acceptable integration point)

8. `null_reader_node.pyx` (L57-96) — Empty/null table construction
   - Uses: `pyarrow.table()`, `pyarrow.array()`, `pyarrow.nulls()`
   - Impact: ~8 refs
   - Scope: Edge case result construction
   - **Recommendation:** Keep as-is (cold path, initialization)

---

### Phase 5.5.D: Architectural Assessment (DEFERRED - Strategic Hold)

**Analysis Summary:**
After code review, remaining NumPy/PyArrow refs require architectural decisions, not quick fixes:

1. **Join operators buffer model**: Currently uses `list[Arrow Table]` + `pyarrow.concat_tables()` at build phase end
   - Refactoring to Morsel buffering requires rearchitecting build/probe phases across 4 operators
   - Effort: 8-12 hours (not the 4-6 hours estimated)
   - Risk: Must maintain join semantics + performance characteristics
   - **Decision needed:** Is Morsel buffering model approved?

2. **Vector operations (heap_sort)**: NumPy usage is for ranking/sorting scored vectors
   - Lacks Draken equivalent (no native argpartition/lexsort)
   - Effort: 8-10 hours (custom algorithm or new Draken module)
   - **Decision needed:** Implement custom ranking or create new Draken sorting module?

3. **Cold paths acceptable**: `read_node` struct/JSONB and `null_reader` empty tables
   - These are initialization/schema handling (not hot path)
   - Removal cost >> benefit
   - **Decision:** Keep as-is

**Recommendation:**
**Do not proceed with Phase 5.5.D** until:
1. Profiling data shows warm-path join buffering is actual bottleneck
2. Architecture approves Morsel buffering model for joins
3. Vector sorting use case is validated/prioritized

**Alternative:** Run profiling on prod workloads to identify if warm-path joins or vector ops are even measurable costs.

**Tactical Cleanup Available (Low-Risk):**
If you want incremental improvement without architectural change:
- Replace `pyarrow.Array.from_buffers()` in Bloom filter path with `BoolVector.from_arrow()` (~15 mins)
- This is standalone, low-risk, removes 1 PyArrow reference from hot-path Bloom filtering

### Phase 5.5.A: Carchar Integration (BLOCKED - Awaiting Architecture Input)
- **Status:** Requires explicit approval for C++ coordination
- **Scope:** NumPy array conversions in `inner_join.pyx` for Carchar interop
- **Challenge:** C++ layer needs memoryview protocol support
- **Impact:** 6-10 refs
- **Effort:** 3-5 days with C++ team

### Phase 5.4.2: FastPath Constant Optimization (DEFERRED)
- **Status:** Already wrapped in BoolVector.from_arrow(), low impact
- **Impact:** ~3-4 allocations
- **Effort:** 30-45 minutes

---

## ⏭️ NEXT STEPS

**Immediate (Ready Now):**
1. ✅ Nested loop join refactoring complete and tested
2. Pattern proven: Morsel buffering + native bloom filters + native join functions works

**Next Priority (High ROI, Similar Pattern):**
1. **Phase 5.5.D.2: Outer Join refactoring** (~4 hours)
   - File: `opteryx/operators/outer_join_node.pyx`
   - Apply same pattern: `Morsel.combine()` + native bloom filters
   - Impact: Eliminates ~4 warm-path PyArrow refs
   - **Ready to execute after approval**

2. **Phase 5.5.D.2b: Non-Equi Join refactoring** (~1 hour)
   - File: `opteryx/operators/non_equi_join_node.pyx`
   - Single concat_tables replacement
   - Can follow outer join pattern

3. **Phase 5.5.D.2c: Cross Join refactoring** (~4 hours)
   - File: `opteryx/operators/cross_join_node.pyx`
   - More complex (non-UNNEST path + COUNT(*) handling)
   - Shares some patterns with nested loop

**Lower Priority (Complex/Rare):**
- Heap sort vector ranking (requires specialized module, complex)
- Cartesian product optimization (cold path, rare execution)

**Deferred (External Dependencies):**
- Phase 5.5.A (Carchar C++ integration) — blocked on C++ team
- Cold paths (read_node, null_reader) — accept as integration points

**Recommended Action:**
1. Run profiling on production workloads to identify if outer_join / cross_join refactoring is high-value
2. If yes, proceed with Phase 5.5.D.2 (outer_join) in parallel with other work
3. If no, consolidate gains from Session 41 and wait for profiling guidance

---

## 📊 REFERENCE: Current NumPy/PyArrow Distribution

**Hot Paths:**
- UNNEST flattening: ✅ Clean (Phase 5.5.B)
- Cross-join indices: ✅ Clean (Phase 5.2)
- Vector arithmetic: ✅ Clean (Phase 5.3)
- Comparisons: ✅ Clean (Phase 5.4.1)

**Warm Paths:**
- Other operators: 🔍 Under investigation (Phase 5.5.C)
- Expression machinery: Needs audit

**Cold Paths:**
- Integration points: Accepted (metadata, initialization)

---

## 🎯 SESSION 41 OUTCOMES

1. ✅ Nested loop join refactored to Draken-native
2. ✅ Bloom filter API extended with native Morsel support
3. ✅ Pattern established: Morsel buffering + native hashing + native join computation works reliably
4. ✅ Test suite stable (87/88 passing, no regressions)
5. ✅ Eliminated ~5 warm-path PyArrow references (concat_tables + Array.from_buffers in nested loop)

**Key Deliverables:**
1. **Bloom Filter Redesigned (bloom_filter.pyx):**
   - Added `create_bloom_filter_morsel(morsel, columns)` — native Draken API
   - Added `bloom_filter_check_morsel(filter, morsel, columns)` — returns bit-packed results
   - Old Arrow-based API deprecated (still works for other operators)
   - Transition point: new operators use Morsel API, breaking change for old Arrow callers

2. **Nested Loop Join Refactored (nested_loop_join_node.pyx):**
   - 100% Draken-native operator (consumes/produces Morsels)
   - Eliminated `pyarrow.concat_tables()` in build phase → use `Morsel.combine()`
   - Eliminated `pyarrow.Array.from_buffers()` → use `create_bloom_filter_morsel()` + `bloom_filter_check_morsel()`
   - Join algorithm via new `draken_nested_loop_join()` (uses `Morsel.hash()`)
   - Only warm-path Arrow conversions: join key casting + final alignment (acceptable)

3. **New Join Implementation (draken_nested_loop_join.pyx):**
   - Pure Draken nested loop join using `Morsel.hash()`
   - No Arrow buffer access patterns
   - Returns index tuples for final alignment

**Pattern Established:**
The proven pattern for join refactoring:
1. Buffer build-side as Morsels: `self.morsels = []` + `Morsel.combine()`
2. Build bloom filter: `create_bloom_filter_morsel(morsel, columns)`
3. Filter probe-side: `bloom_filter_check_morsel(filter, morsel, columns)`
4. Compute join: `draken_nested_loop_join(left_morsel, right_morsel, columns)`
5. Final alignment: `align_tables(left.to_arrow(), right.to_arrow(), indexes)`

**Next Can Use This Pattern:**
- Outer join (4 hours)
- Non-equi join (1 hour)
- Cross join (4 hours)

**Cost-Benefit Updated:**
- Session 41: 1 operator refactored, ~5 refs eliminated (warm path)
- Session 42 potential: 3 more operators, ~10 more refs (warm path)
- Total warm-path elimination: ~15 refs in 8-10 hours (good ROI)
- Remaining: vector operations (complex), cold paths (acceptable)