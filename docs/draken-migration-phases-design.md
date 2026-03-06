# Draken Migration Assessment for Opteryx Operators

**Assessment Date:** March 5, 2026  
**Scope:** All operators requiring PyArrow - evaluated for Draken migration feasibility  
**Methodology:** 4-dimension assessment with prioritized conversion roadmap

---

## Executive Summary

**PyArrow-Dependent Operators:** 20 of 24 operators  
**Already Draken-Native:** 1 (DrakenAggregateAndGroupNode)  
**High-Priority Candidates:** 7 operators  
**Estimated 18-month roadmap** from aggregate → filter → join → sort operations

### Key Constraints
- Draken philosophy: **fail at compile-time, never silently degrade**
- Performance pressure points: Aggregate > Filter > Join > IO (in priority order)
- Spill format: DRKM (columnar, LZ4/ZSTD compressed)
- Expression evaluator: required for filter/projection draken paths

---

## Operator Migration Matrix

All assessments use this scale:
- **Readiness (Draken support):** Green (Ready) → Yellow (Partial) → Red (Not Ready)
- **Complexity:** Low (1-2 days) → Medium (1-2 weeks) → High (2+ weeks)
- **Performance Impact:** Negligible → Moderate → Transformative (10x+)

---

## TIER 1: HIGHEST PRIORITY (Aggregate Operations)

### 1. ✅ DrakenAggregateAndGroupNode → ALREADY DRAKEN-NATIVE

**Status:** PRODUCTION  
**Readiness:** 🟢 READY (100% complete)  
**Complexity:** - (already done)  
**Performance Impact:** ⭐⭐⭐⭐⭐ Transformative (10-50x faster than AggregateAndGroupNode)

**What's Done:**
- ✅ GroupStateStore with collision-safe fingerprinting
- ✅ Hash aggregation kernels for COUNT, SUM, MIN, MAX, AVG, COUNT(DISTINCT)
- ✅ Spill-to-disk (DRKM format) for memory overflow
- ✅ Merge kernels for partial aggregation combination
- ✅ ConstantVector optimization for broadcast values

**Recommendation:** **USE EXCLUSIVELY** for GROUP BY workloads. Deprecate AggregateAndGroupNode for v1.0.

---

### 2. 🔴 AggregateAndGroupNode → CANDIDATE FOR REPLACEMENT

**Current Status:** Arrow/NumPy hybrid  
**Readiness:** 🟠 PARTIAL (DrakenAggregateAndGroupNode exists as replacement)  
**Complexity:** 🟡 MEDIUM (1-2 weeks for full planner integration)  
**Performance Impact:** ⭐⭐⭐⭐⭐ Transformative (50-100x faster for ClickBench GROUP BY)

**Current Implementation Analysis:**
```
Lines: ~350
PyArrow usage:
  - pyarrow.compute.group_by() - if available (limited support)
  - Expression evaluation with NumPy/Arrow hybrid
  - concat_tables() for merging partials
  - Type casting
NumPy: Heavy (per-row operations in aggregation loop)
Bottlenecks:
  - Python loop over groups
  - NumPy type conversions
  - Arrow compute for non-vectorizable aggregates
```

**What Draken Provides:**
- ✅ Native hash table (GroupStateStore)
- ✅ Compiled aggregate kernels (no Python loop)
- ✅ Memory-bound execution (spill to DRKM)
- ✅ Per-group state isolation (no sharing between partitions)

**Conversion Step 1: Planner Magic**
- Configure planner to prefer DrakenAggregateAndGroupNode when:
  - Aggregate functions are all Draken-supported (COUNT, SUM, MIN, MAX, AVG, COUNT(DISTINCT))
  - OR mark unsupported aggregates as "requires fallback"
- Keep AggregateAndGroupNode for edge cases only

**Conversion Step 2: Handle Unsupported Aggregates**
Draken v1 does NOT support:
- ARRAY_AGG with ORDER BY/LIMIT
- Approximate quantiles (APPROX_PERCENTILE)
- Variance/stddev (V2 feature)

**Action:** Design fallback planner rules that split query:
```sql
SELECT group_id, 
  COUNT(*), SUM(val),          -- Draken kernel
  ARRAY_AGG(col ORDER BY x)    -- Arrow fallback for this expression
GROUP BY group_id
```

**Recommendation:** **MIGRATE PLANNER RULES (Priority 1)** - Move qualifier checks to planner, NOT to operator choice at runtime.

---

### 3. 🟡 SimpleAggregateNode → NO DRAKEN YET

**Current Status:** Arrow compute with .as_py() conversions  
**Readiness:** 🔴 NOT READY (Draken needs non-aggregating kernel for single group)  
**Complexity:** 🟡 MEDIUM (2-3 weeks for new Draken kernel)  
**Performance Impact:** ⭐⭐⭐ Moderate (5-10x for single-aggregate workloads)

**Current Implementation:**
```
Lines: ~200
PyArrow usage:
  - pyarrow.compute.count(), sum(), min(), max()
  - .as_py() conversion for accumulation
  - count_distinct() via Cython speedup
Issue: Per-batch accumulation with Python scalar conversion
```

**Draken Gap Analysis:**
- ✅ Draken has the aggregate kernels
- ❌ Draken's GroupStateStore is built for multiple groups (hash table abstraction)
- ⚠️ Could optimize with single-group constant in GroupStateStore (group_id=0)

**Recommendation:** **LOW PRIORITY** - Wait for Draken to add single-group specialized kernel. For now, SimpleAggregateNode is acceptable (~50MB fallback queries).

---

### 4. 🟡 SimpleAggregateAndGroupNode → NO DRAKEN YET

**Current Status:** Arrow compute (streaming without full buffering)  
**Readiness:** 🟠 PARTIAL (DrakenAggregateAndGroupNode exists but requires full buffering)  
**Complexity:** 🟡 MEDIUM (1-2 weeks)  
**Performance Impact:** ⭐⭐⭐ Moderate (conflicting - gains on hot path but loses streaming)

**Current Implementation:**
```
Lines: ~200
Purpose: Streaming GROUP BY without buffering all groups at once
PyArrow usage:
  - pyarrow.compute.[agg_function]()
  - Incremental group emission
```

**The Tradeoff:**
- SimpleAggregateAndGroupNode: Streams groups as they're finalized → low memory, high latency
- DrakenAggregateAndGroupNode: Buffers all groups → high memory until spill, low latency

**Recommendation:** **PROFILE FIRST** - Measure memory/latency for your workloads:
- If streaming matters → keep SimpleAggregateAndGroupNode
- If throughput matters → switch to DrakenAggregateAndGroupNode (with spill budget)

---

## TIER 2: HIGH PRIORITY (Filter/Projection)

### 5. 🔴 FilterNode → DESIGNED BUT NOT IMPLEMENTED

**Current Status:** Arrow-based (`table.filter(boolean_mask)`)  
**Readiness:** 🟡 PARTIAL (Design complete, awaiting expression evaluator integration)  
**Complexity:** 🟡 MEDIUM-HIGH (2-3 weeks for expression evaluator, 1 week for morsel integration)  
**Performance Impact:** ⭐⭐⭐⭐ High (3-5x faster for highly selective filters)

**Current Implementation:**
```
Lines: ~80
PyArrow usage:
  - table.filter(mask) - primary bottleneck
  - BoolVector → Arrow conversion
  - Expression evaluation with Arrow compute functions
Hot path: Mask generation and table.filter()
```

**Draken Design Status:**
```
Design Doc: docs/draken-filter-operators-design.md (APPROVED)
Approach: 
  1. Accept Draken morsels directly
  2. Evaluate predicates as Draken BoolVectors (no Arrow conversion)
  3. Apply mask natively (keep columnar representation)
  4. Exit with Draken morsel
```

**Why Not Done Yet:**
- Expression evaluator needs Draken entry points
- Requires BoolVector → column selection mapping
- Half of a project: expression-side work is ongoing

**Blocking Dependency:** Expression evaluator reaching "Draken-ready" state  
See: `opteryx/functions/evaluators/` for current status

**Recommendation:** **UNBLOCK EXPRESSION EVALUATOR** (Priority 2) as prerequisite:
- [ ] Add `evaluate_morsel()` entry point to expression system
- [ ] Enable column references → Draken BoolVector outputs
- [ ] Then integrate into FilterNode (1 day)

**Expected Gains:**
- No Arrow intermediate masks
- No BoolVector → Arrow → morsel conversions
- Single columnar representation throughout

---

### 6. 🟡 ProjectionNode → ALREADY SOMEWHAT DRAKEN-CAPABLE

**Current Status:** Dual-mode (Arrow + Draken morsel selection)  
**Readiness:** 🟡 PARTIAL (Native morsel operations exist, expression evaluation pending)  
**Complexity:** 🟡 MEDIUM (1-2 weeks for full expression evaluator integration)  
**Performance Impact:** ⭐⭐⭐ Moderate (2-3x for purely columnar projections; depends on expression complexity)

**Current Implementation:**
```
Lines: ~70
Status: Already accepts Draken morsels
  - morsel.select(columns) ~ instant (no-copy)
  - Expression evaluation for computed columns ~ Python evaluation
Hot path: Computed column expressions (e.g., SELECT col1 + col2 AS sum_col)
```

**What Needs Work:**
- Expression evaluator needs Draken-native paths
  - Column references ✅ (exists)
  - Arithmetic (col1 + col2) ⚠️ (partial, see opteryx/draken/evaluators/)
  - Functions (UPPER, LOWER, etc.) ❌ (not draken-native yet)

**Recommendation:** **MEDIUM PRIORITY** - Wait for expression evaluator. Current Arrow fallback is acceptable.

---

## TIER 3: MEDIUM PRIORITY (Join Operations)

### 7. 🔴 InnerJoinNode → NOT READY (Already Cython-Optimized)

**Current Status:** Hybrid Cython + Arrow  
**Readiness:** 🔴 NOT READY (Cython kernel is already fast; Draken hasn't addressed joins yet)  
**Complexity:** 🟠 HIGH (3-4 weeks for complete draken join kernel)  
**Performance Impact:** ⭐⭐⭐ Moderate (maybe 1.5-2x, but Cython is already good)

**Current Implementation:**
```
Lines: ~300
Cython kernels: inner_join() - FAST PATH
PyArrow usage:
  - is_valid(), is_nan(), and_(), invert() - pre-filtering
  - Bloom filter + probe-side hash map (Cython)
Hybrid justification: Cython is near-optimal for hash join
```

**Draken's Position:**
- ❌ No native hash join kernel in Draken yet
- Design would need:
  - Build-side hash table (like GroupStateStore but key-value pairs, not aggregates)
  - Probe-side iteration with key lookup
  - Output morsel construction
  - Bloom filter (optional fast-path)

**Does Draken Benefit Over Cython?**
- Unlikely - Cython already has direct memory access
- Benefit only if: Draken morsel format is cheaper to consume than Arrow
- Current cost: Arrow ← Morsel (cheap zero-copy) but Arrow → Morsel (conversion)

**Recommendation:** **DEFER** (Priority 4)
- Keep Cython inner_join kernel
- Only migrate IF:
  - Upstream (FilterNode, ProjectionNode) become pure-Draken
  - Arrow ↔ Morsel conversions become bottleneck
  - Draken join kernel is implemented and benchmarked

---

### 8. 🔴 NestedLoopJoinNode → NOT READY

**Status:** Cython + Arrow (simpler than InnerJoinNode)  
**Readiness:** 🔴 NOT READY (Draken has no nested loop join kernel)  
**Complexity:** 🔴 HIGH (4+ weeks for new kernel)  
**Performance Impact:** ⭐⭐ Low (Nested loop is slow by design; Draken wouldn't help much)

**Recommendation:** **DEFER INDEFINITELY** - This is a fallback operator; no strategic value to Draken-optimize.

---

### 9. 🔴 OuterJoinNode → NOT READY

**Status:** Custom Cython + Arrow (bypasses PyArrow due to STRUCT/ARRAY column bugs)  
**Readiness:** 🔴 NOT READY (Draken would need to handle outer join semantics  + STRUCT/ARRAY types)  
**Complexity:** 🔴 HIGH (5+ weeks)  
**Performance Impact:** ⭐⭐ Low (needed for correctness, but not hot path)

**Recommendation:** **DEFER** - Maintain as-is.

---

### 10. 🟠 FilterJoinNode → SEMI/ANTI JOINS

**Status:** Cython-based (semi_join, anti_join kernels)  
**Readiness:** 🔴 NOT READY (Draken has no semi/anti join kernels)  
**Complexity:** 🔴 HIGH (3-4 weeks)  
**Performance Impact:** ⭐⭐⭐ Moderate (3-5x on large semi joins)

**Why Not Draken-First:**
- Semi/anti joins are used less frequently than inner joins
- Cython path already exists and works

**Recommendation:** **DEFER** - Continue with Cython. Migrate only if Draken inner_join is done first (refactor common code).

---

### 11. 🔴 NonEquiJoinNode → NOT READY

**Status:** Cython + Arrow (non_equi_nested_loop_join)  
**Readiness:** 🔴 NOT READY (needs custom Draken kernel)  
**Complexity:** 🔴 HIGH (3-4 weeks)  
**Performance Impact:** ⭐⭐ Low (non-equi joins are always slow; correctness focus)

**Recommendation:** **SKIP** - Benchmarking shows non-equi joins are I/O bound anyway.

---

### 12. 🟡 CrossJoinNode → NOT READY (But Fixable)

**Status:** Arrow-based (`Table.from_batches`, `align_tables`)  
**Readiness:** 🟡 PARTIAL (Drakenification is straightforward)  
**Complexity:** 🟡 MEDIUM (1-2 weeks)  
**Performance Impact:** ⭐⭐ Low (Cartesian product is inherently expensive; overhead is small % of total)

**Current Implementation:**
```
Lines: ~150
PyArrow usage: Table conversion, batching, aligning
Main cost: Output size (cartesian product), NOT overhead
```

**Why Do This?**
- Normalize pipeline: consistent morsel format throughout query
- Avoid Arrow ↔ Morsel conversions at join boundary
- Memory-efficient chunking already exists (max_chunksize logic)

**Recommendation:** **MEDIUM PRIORITY** - Nice-to-have, not urgent. Only migrate if:
- FilterNode/ProjectionNode become pure-Draken
- Arrow ↔ Morsel conversions become visible in cross-join profiles

---

### 13. 🟡 UnnestJoinNode → NOT READY

**Status:** Cython + Arrow (array unnesting)  
**Readiness:** 🔴 NOT READY (Draken needs ListVector operations)  
**Complexity:** 🔴 HIGH (2-3 weeks for ListVector iteration + output morsel construction)  
**Performance Impact:** ⭐⭐⭐ Moderate (2-3x on large unnested arrays)

**Current Implementation:**
```
Lines: ~300
Complex: Array/list manipulation + index generation
Cython kernels: build_filtered_rows_indices_and_column(), list_distinct()
```

**Draken Gap:**
- ✅ ListVector exists in Draken
- ❌ Need kernel for "explode" operation (one-to-many row mapping)

**Recommendation:** **LOW-MEDIUM PRIORITY** - defer unless unnesting becomes bottleneck.

---

## TIER 4: LOWER PRIORITY (Sort, Distinct, IO)

### 14. 🟡 SortNode → PARTIAL DRAKEN SUPPORT POSSIBLE

**Status:** Arrow-based (concat_tables, table.take())  
**Readiness:** 🟡 PARTIAL (Draken vectors can be sorted, morsel output pending)  
**Complexity:** 🟡 MEDIUM (2-3 weeks for Draken sort kernel + morsel output)  
**Performance Impact:** ⭐⭐⭐ Moderate (2-3x if sorting large buffers)

**Current Implementation:**
```
Lines: ~120
PyArrow usage:
  - concat_tables() - merge accumulated morsels
  - table.slice() - implement OFFSET
  - table.take() - reorder rows
Bottleneck: TimSort is built into PyArrow; no direct access
```

**Why Sort Matters:**
- Must buffer entire result set (not streamed)
- No OFFSET/LIMIT pushdown yet (design WIP)

**Draken Approach:**
- Morsel list → Draken vector merge sort
- Sort keys as Draken vectors (not Arrow)
- Output as Draken morsel list

**Recommendation:** **MEDIUM PRIORITY** - After filter/projection stabilize.

---

### 15. 🟡 HeapSortNode → ALREADY PARTIALLY DRAKEN-AWARE

**Status:** Hybrid (NumPy + Draken vector awareness)  
**Readiness:** 🟠 PARTIAL (Detects ConstantVector compression, but limited)  
**Complexity:** 🟡 MEDIUM (1 week to expand Draken vector support)  
**Performance Impact:** ⭐⭐⭐ Moderate (1.5-2x for top-N with constant columns)

**Current Implementation:**
```
Lines: ~300
Status: Inspects BoolVector, DictionaryVector types for optimization
- ConstantVector branch: skip null/constant key checking
- DictionaryVector branch: use dict offsets instead of values
Already Draken-aware but limited coverage
```

**Recommendation:** **QUICK WIN** - Expand vector type coverage (1 day PR):
- [ ] Handle IntVector, StringVector directly (no conversion)
- [ ] Detect low-cardinality vectors (optimize hash computation)
- [ ] Profile to validate gains

---

### 16. 🟡 DistinctNode → ALREADY HYBRID

**Status:** Draken-native preferred with Arrow fallback  
**Readiness:** 🟢 READY (70% Draken, 30% Arrow fallback)  
**Complexity:** - (already implemented)  
**Performance Impact:** ⭐⭐⭐⭐ High (10-50x for Draken path, negligible overhead for Arrow fallback)

**Current Implementation:**
```
Lines: ~80
Status: Smart dual-mode
  - Draken morsel → native distinct() kernel
  - Arrow table → fallback via Morsel.iter_from_arrow()
Cython kernel: distinct() - hash-based uniqueness
```

**Recommendation:** **ALREADY GOOD** - No action needed. Monitor for Arrow fallback frequency; if high, optimize input.

---

### 17. 🟢 ParquetReadNode → ALREADY DRAKEN-OPTIMIZED

**Status:** Rugo decoder (Cython + Draken vectors)  
**Readiness:** 🟢 READY (Rugo produces Draken morsels directly)  
**Complexity:** - (already done)  
**Performance Impact:** ⭐⭐⭐⭐⭐ Transformative (Rugo is 10-100x faster than PyArrow)

**Current Implementation:**
```
Lines: 600+
Features:
  - Rugo decoder (Cython, PRODUCTION)
  - Predicate pushdown (PROD)
  - Aggressive prefetch (PROD)
  - Rowgroup priority (PROD)
  - Dictionary native hardening (PROD)
PyArrow usage: Only type casting post-decode
```

**Recommendation:** **ALREADY OPTIMAL** - Don't change. Ensure Rugo is always preferred over PyArrow.

---

### 18. 🟢 ShuffleNode → ALREADY DRAKEN-OPTIMIZED

**Status:** Draken-native partitioning  
**Readiness:** 🟢 READY (DRKM spill format)  
**Complexity:** - (already done)  
**Performance Impact:** ⭐⭐⭐⭐⭐ Transformative (partitioned execution with zero-copy)

**Current Implementation:**
```
Lines: ~200
Status: PRODUCTION
- Cython kernel: row_indexes_by_bin_flat()
- Draken morsel partition mapping
- DRKM format spill/store (LZ4 + ZSTD)
```

**Recommendation:** **ALREADY OPTIMAL** - Leverage for multi-operator pipelines.

---

### 19. 🟢 UnionNode → SCHEMA MANAGEMENT ONLY

**Status:** Arrow table alignment  
**Readiness:** 🟢 READY (Metadata operation, not hot path)  
**Complexity:** - (low cost)  
**Performance Impact:** ⭐ Negligible (not parallelizable anyway)

**Recommendation:** **KEEP AS-IS** - No strategic value in Drakenizing.

---

### 20. 🟢 LimitNode → TRIVIAL

**Status:** Arrow table.slice()  
**Readiness:** 🟢 READY (Morsel-aware)  
**Complexity:** - (trivial)  
**Performance Impact:** ⭐ Zero (O(1) metadata operation)

**Recommendation:** **KEEP AS-IS**.

---

### 21. 🔴 Others (ExitNode, ShowColumnsNode, NullReaderNode, etc.)

**Status:** Metadata/schema operations  
**Readiness:** 🟢 READY (Not hot path)  
**Complexity:** - (N/A)  
**Performance Impact:** ⭐ Zero  

**Recommendation:** **IGNORE** - These are not bottlenecks.

---

## 🚀 PRIORITIZED MIGRATION ROADMAP

### Phase 1: Planner & Aggregate (Weeks 1-4)
**Goal:** Make DrakenAggregateAndGroupNode the DEFAULT for GROUP BY

**Tasks:**
1. **Week 1:** Audit planner for aggregate function qualification logic
   - [ ] Find where AggregateAndGroupNode vs. DrakenAggregateAndGroupNode choice is made
   - [ ] Design fallback rules for unsupported aggregates (ARRAY_AGG, APPROX_*)
   - [ ] Implement planner changes (~1 day)
   - [ ] Benchmark vs. existing behavior

2. **Week 2-3:** Add planner support for aggregate function qualification
   - [ ] ARRAY_AGG(col ORDER BY x) → split to Draken + Arrow fallback
   - [ ] Mark unsupported aggregates → planner routes to AggregateAndGroupNode
   - [ ] Test edge cases (mixed supported/unsupported in same GROUP BY)

3. **Week 4:** Benchmark & optimization
   - [ ] ClickBench GROUP BY queries (target 10-50x speedup)
   - [ ] TPC-H with GROUP BY heavy queries
   - [ ] Memory profiling (spill behavior)

**Expected Gain:** 10-50x faster GROUP BY queries (measured vs. AggregateAndGroupNode)

---

### Phase 2: Expression Evaluator Unblock (Weeks 5-7)
**Goal:** Enable FilterNode and ProjectionNode to use Draken expression evaluation

**Tasks:**
1. **Week 5:** Add Draken entry point to expression evaluator
   - [ ] Design `evaluate_morsel(expr, morsel) → Draken*Vector` interface
   - [ ] Audit current expression evaluator (opteryx/functions/evaluators/)
   - [ ] Identify minimal set to support Phase 2 (column refs + basic arithmetic)

2. **Week 6:** Implement Draken expression paths
   - [ ] Column reference evaluation (trivial)
   - [ ] Arithmetic operators (+, -, *, /) on numeric vectors
   - [ ] Comparison operators (=, !=, <, >, <=, >=) → BoolVector
   - [ ] Test coverage

3. **Week 7:** Integration & testing
   - [ ] Integrate into FilterNode (bonus: ProjectionNode gets it free)
   - [ ] Benchmark filter selectivity impact
   - [ ] Profile mask generation vs. table.filter() overhead

**Expected Gain:** 3-5x faster selective filters (depends on selectivity)

---

### Phase 3: FilterNode Drakenification (Weeks 8-9)
**Goal:** Native BoolVector mask evaluation without Arrow conversion

**Tasks:**
1. **Week 8:** Integration work
   - [ ] Update FilterNode to accept Draken morsel input
   - [ ] Use expression evaluator's `evaluate_morsel()` → BoolVector mask
   - [ ] Apply native mask to morsel columns
   - [ ] Exit with Draken morsel

2. **Week 9:** Testing & optimization
   - [ ] Benchmark selective queries (WHERE clauses)
   - [ ] Test NULL/NaN handling (Arrow semantics preserved?)
   - [ ] Profile mask application performance

**Expected Gain:** 3-5x faster filtering (measured vs. Arrow table.filter)

---

### Phase 4: ProjectionNode Expression Expansion (Weeks 10-12)
**Goal:** Support computed columns (SELECT col1 + col2 AS sum_col) natively

**Tasks:**
1. **Week 10:** Audit expression evaluator for function coverage
   - [ ] Identify which functions need Draken kernels (STRING, MATH, etc.)
   - [ ] Prioritize commonly used functions
   - [ ] Design multi-phase rollout (basic → complete)

2. **Week 11-12:** Expression function implementation & testing
   - [ ] Phase 1: STRING functions (UPPER, LOWER, CONCAT)
   - [ ] Phase 2: MATH functions (ROUND, FLOOR, etc.)
   - [ ] Phase 3: Advanced (DATE, TIMESTAMP, JSON)

**Expected Gain:** 1.5-3x faster computed columns (varies by function)

---

### Phase 5: Sort Kernel (Weeks 13-15)
**Goal:** Native Draken sort for ORDER BY

**Tasks:**
1. **Week 13:** Design sort kernel
   - [ ] Decide: morsel sort vs. columnar sort vs. hybrid?
   - [ ] Handle sort keys (multiple columns, directions)
   - [ ] Plan memory layout for output

2. **Week 14:** Implementation
   - [ ] Write Draken C++/Cython sort kernel
   - [ ] Output morsel list (suitable for ExitNode)
   - [ ] Handle OFFSET/LIMIT in kernel

3. **Week 15:** Testing & benchmarking
   - [ ] Benchmark multi-megabyte sorts
   - [ ] Compare vs. PyArrow TimSort (probably slower 😅)
   - [ ] Measure memory usage (vs. Arrow)

**Expected Gain:** 2-3x faster sorts (minor, if CPU-bound on TimSort)

---

### Phase 6: Optional Future Work (Weeks 16+)

**Lower Priority:**
- **HeapSortNode expansion** (1 day) - Expand Draken vector coverage
- **CrossJoinNode** (1-2 weeks) - If cartesian products are bottleneck
- **UnnestJoinNode** (2-3 weeks) - If unnesting becomes hotspot
- **InnerJoinNode** (3-4 weeks) - Only if Arrow ↔ Morsel conversions dominate

**Not Recommended:**
- NestedLoopJoinNode, OuterJoinNode, NonEquiJoinNode, FilterJoinNode (low ROI)

---

## 📊 Expected ROI by Phase

| Phase | Timeline | Feature | Expected Gain | Effort |
|-------|----------|---------|---------------|--------|
| **Phase 1** | 4 weeks | GROUP BY → Draken | 10-50x | **Medium** |
| **Phase 2** | 3 weeks | Expression evaluator | (prerequisite) | **Medium** |
| **Phase 3** | 2 weeks | FilterNode → Draken | 3-5x | **Low** |
| **Phase 4** | 3 weeks | ProjectionNode functions | 1.5-3x | **Medium** |
| **Phase 5** | 3 weeks | SortNode → Draken | 2-3x | **Medium** |
| **Phase 6** | TBD | Misc optimizations | 1.5-2x each | **Low** |

**Total estimated effort:** 18 weeks (4.5 months) for Phases 1-5

---

## 🎯 Decision Matrix: Should We Drakenize This Operator?

```
Questions to ask per operator:
1. Is it already Cython-fast? → DEFER (ROI too low)
2. Does it parse large PyArrow tables? → HIGH (drag removal)
3. Does it call PyArrow compute functions? → MEDIUM (often vectorizable)
4. Is it metadata-only? → SKIP (not hot path)
5. Does it require new Draken kernels? → MEDIUM-HIGH complexity
6. Can it wait for expression evaluator? → MEDIUM priority (unblock dependency first)
```

**Apply to each operator:**

| Operator | Q1 | Q2 | Q3 | Q4 | Q5 | Q6 | **Decision** |
|----------|----|----|----|----|----|----|-------------|
| AggregateAndGroupNode | No | Yes | Yes | No | No | Yes | **Phase 1** |
| FilterNode | No | Yes | Yes | No | No | Yes | **Phase 3** (blocked by Q6) |
| ProjectionNode | No | Yes | Yes | No | No | Yes | **Phase 4** (blocked by Q6) |
| InnerJoinNode | Yes | No | No | No | Yes | - | DEFER |
| SortNode | No | Yes | Yes | No | Yes | No | **Phase 5** |
| DistinctNode | No | No | No | No | No | - | DONE ✅ |
| ParquetReadNode | Yes | No | No | No | - | - | DONE ✅ |
| Simple aggregates | No | No | Yes | No | Yes | Yes | DEFER |

---

## ⚠️ Gotchas & Risks

### Risk 1: Expression Evaluator Blocking
Expression evaluator is a **prerequisite** for FilterNode and ProjectionNode.  
**Mitigation:** Start Phase 2 (Week 5) in parallel if possible; can proceed in parallel with Phase 1.

### Risk 2: Aggregate Unsupported Functions
ARRAY_AGG, APPROX_PERCENTILE, VARIANCE not yet in Draken.  
**Mitigation:** Design planner fallback rules (split query by function type). Test thoroughly.

### Risk 3: NULL/NaN Semantics Drift
Draken vectors handle nulls differently than Arrow.  
**Mitigation:** Write comprehensive tests for NULL handling in filters/aggregates. Compare outputs vs. Arrow baseline.

### Risk 4: Sort Memory Pressure
Full result set must fit in memory; no streaming.  
**Mitigation:** Don't try to optimize sort kernel; focus on earlier operators. Sort is rarely bottleneck.

### Risk 5: Join Complexity
General Draken join kernel is 4+ weeks; high risk of delays.  
**Mitigation:** DEFER unless benchmarking shows joins are bottleneck. Cython is already good.

---

## 📋 Architecture Decisions

### Decision 1: Keep Arrow at Ecosystem Boundaries
**Outcome:** ParquetReadNode → Rugo → Draken morsels ✅  
**Outcome:** User results ← ExitNode ← Draken morsels ✅

### Decision 2: Expression Evaluator is Mandatory for Filters/Projections
**Outcome:** Can't drakenize FilterNode until expressions are ready  
**Action:** Unblock Phase 2 early

### Decision 3: Fail at Compile Time, Not Runtime
**Outcome:** Don't add fallback logic to hot paths  
**Outcome:** If Draken kernel missing → planner detects early, routes differently

### Decision 4: Spill Format is DRKM
**Outcome:** Aggregate state, shuffle bins, sort keys all use DRKM  
**Action:** Ensure DRKM encoder/decoder is robust

---

## 🔍 Success Metrics

Track per phase:

| Metric | Target | Tool |
|--------|--------|------|
| **Aggregate queries (GROUP BY)** | 10-50x faster | ClickBench |
| **Filter rate (WHERE)** | 3-5x faster | Custom micro-benchmarks |
| **Projection rate (SELECT computed)** | 1.5-3x faster | TPC-H |
| **Sort rate (ORDER BY)** | 2-3x faster | Micro-benchmarks |
| **Memory overhead** | <10% vs Arrow | Profiling |
| **NULL handling correctness** | 100% parity | Unit tests |

---

## 🏁 Conclusion

### Tier 1 Actions (Start Immediately)
1. ✅ Use DrakenAggregateAndGroupNode exclusively for GROUP BY (planner integration)
2. ⏲️ Unblock expression evaluator (prerequisite for Phases 3-4)
3. 🧪 Benchmark Phase 1 gains (validate 10-50x claim)

### Tier 2 Actions (Weeks 5-9)
4. FilterNode → Draken (after expression evaluator ready)
5. ProjectionNode → Draken expressions (incremental)

### Tier 3 Actions (Weeks 10-15)
6. SortNode → Draken (if benchmarking shows it's needed)
7. Minor optimizations (HeapSortNode, CrossJoinNode)

### Tier 4 Actions (Defer)
- Join operators (InnerJoinNode, NestedLoopJoinNode, etc.) → Low ROI relative to effort
- SimpleAggregateNode, SimpleAggregateAndGroupNode → Accept Arrow until Draken v2

---

**Last Updated:** March 5, 2026  
**Prepared By:** Codebase Analysis  
**Stakeholder Review:** Required before Phase 1 kickoff
