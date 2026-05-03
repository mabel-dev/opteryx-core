# Vectorized Engine Optimizations from Compiled vs. Vectorized Query Processing Research

**Status:** Proposal  
**Author:** Design review prompted by Kersten, Leis, Kemper, Neumann, Pavlo, Boncz (2018)  
**Reference:** "Everything You Always Wanted to Know About Compiled and Vectorized Queries But Were
Afraid to Ask" — T. Kersten, V. Leis, A. Kemper, T. Neumann, A. Pavlo, P. Boncz.  
PVLDB, 11(13): 2209–2222, 2018. DOI: https://doi.org/10.14778/3275366.3275370

---

## 1. Purpose

This document evaluates the actionable findings of the Kersten et al. paper against Opteryx's
current execution engine. The paper compares two state-of-the-art query processing paradigms —
vectorized interpretation (Tectorwise) and data-centric code generation (Typer) — using an
apples-to-apples prototype that holds algorithms, data structures, and parallelization fixed.
Because Opteryx is a vectorized engine, the paper's vectorization-specific findings are directly
relevant.

Three proposals emerge from this review. A fourth area — hash join Bloom filtering — is already
well-implemented and is documented here for completeness.

---

## 2. What Already Exists (No Action Required)

### 2.1 Hash Function Selection

The paper (Section 4.1) finds that vectorized engines benefit from a throughput-oriented hash
function, specifically Murmur2, because hash computation is decoupled from table probing: the
engine first hashes a full vector of keys, then probes. This separation tolerates higher per-hash
instruction cost in exchange for better pipeline throughput.

Opteryx uses **XXHash3-64**, which is strictly superior to Murmur2: it is SIMD-accelerated (AVX2
on x86, NEON on ARM), produces better distribution, and has lower latency at the same or higher
throughput. The hash/probe separation that makes Murmur2 work well in Tectorwise is also present
in Opteryx — `Morsel.hash()` is called once per morsel before the aggregation or join lookup loop.
No change is warranted here.

### 2.2 Bloom Filter on Join Probe

The paper (Section 3.2) notes that the test system encodes a small Bloom filter into unused pointer
bits on each hash table entry to short-circuit non-matching probes before traversing the collision
chain. This is described as a "dictionary" that improves performance on selective joins.

Opteryx's `CarcharJoinEngine` already builds a Bloom filter from all build-side hashes after
sealing the build phase, and checks it per probe-side row before touching the main hash table. The
filter eliminates rows whose hash does not appear in the build set at all. This is equivalent to —
and structurally cleaner than — the pointer-tag trick in the paper.

### 2.3 Two-Tier Aggregation Hash Table (Parvi + Carchar)

The paper (Section 2.2, Figure 2b) notes that aggregation in a vectorized engine must partition
the lookup work across primitives, which creates extra materialization cost versus generated code.
The paper validates that for low-cardinality aggregation (TPC-H Q1, 4 groups) this materializaton
overhead is significant.

Opteryx addresses this differently and more aggressively: it uses a two-tier hash table. Parvi
handles GROUP BY with ≤16 groups using a fixed 16-entry inline array, a single SIMD 128-bit
control-group probe per lookup, and an 8-entry direct-mapped cache within the hot row loop.
Groups beyond 16 automatically promote to Carchar, a Swiss-table style open-addressed map with
SIMD tag matching. Both tiers operate with zero Python object allocation in the hot path.

---

## 3. Proposed Changes

### Proposal 1 — Adaptive Ordered Aggregation for Low-Cardinality GROUP BY

**Relevant paper section:** Section 8.4 (Adaptivity), Table 1 (Q1 results)

#### What the paper found

For TPC-H Q1 (4 groups, arithmetic-heavy), Typer (compiled) is faster than Tectorwise
(vectorized) by 74% (34 cycles/tuple vs 59 cycles/tuple). The paper traces this to
materialization overhead: Tectorwise must write each intermediate arithmetic result to a vector
between primitives, whereas Typer keeps intermediates in CPU registers within a fused loop.

The paper identifies the mechanism by which VectorWise recovers this gap (Table 2: VectorWise runs
Q1 in 71 ms, close to HyPer's 53 ms, despite being a vectorized engine). VectorWise uses an
adaptive optimization (Section 8.4, referencing [39] Raducanu et al., SIGMOD 2013) that activates
when GROUP BY cardinality within the current vector is small:

> "During aggregation, the system partitions the input tuples into multiple selection vectors —
> one for each group-by key. This task only succeeds if there are few groups in the current
> vector; if it fails the system exponentially backs off from trying this optimization. If it
> succeeds, by iterating over all elements in a selection vector — i.e., all tuples of one group
> in the vector — hash-based aggregation is turned into **ordered aggregation**. Ordered
> aggregation performs partial aggregate calculation, keeping e.g., the sum in a register, which
> strongly reduces memory traffic, since updating aggregate values in a hash table for each tuple
> is no longer required." (Section 8.4)

The net effect: for a 4-group aggregation over millions of rows, rather than doing 1 hash table
write per row (millions of writes into potentially separate cache lines), the engine makes one
pass per group over all rows in the selection vector, accumulating into a local register variable.
The register is written back to the result buffer only once at the end of the pass.

#### Current state in Opteryx

Parvi handles ≤16 groups with an inline 16-entry table and an 8-entry direct-mapped cache. This
eliminates heap allocation and reduces probe cost, but it still performs a hash table write per
row: each row looks up its group index and updates the aggregate in the hash table entry. For a
2-group query (`GROUP BY status` where status ∈ {active, inactive}) on a 500K-row morsel, this
is 500K conditional stores to hash table memory, which — even if the entire Parvi table is in L1
— creates per-row branch-and-store pressure.

The paper's approach eliminates that pattern entirely. Instead of "for each row, find its group
and update the aggregate", it does "for each group, iterate over all rows of that group and
accumulate in a register". This inverts the loop structure.

#### Proposed design

Add an **ordered aggregation fast path** that activates within the existing Parvi tier when two
conditions are met: (1) the morsel has been fully hashed and group assignments are known, and (2)
the number of distinct groups in this morsel is ≤ a threshold (proposed: ≤ 16, matching Parvi's
capacity).

After the group-assignment pass (which is already done to populate Parvi), build one
position-sorted index vector per group: a contiguous int32 array of row indices belonging to that
group. Then, for each aggregate column, execute a tight inner loop over the index vector for each
group, accumulating into a stack-allocated register variable (a `cdef double accumulator` in
Cython, or a local in a `nogil` Cython loop). Write the final accumulated value back to the group
buffer once per group.

```
# Conceptual hot loop (Cython, nogil)
for group_idx in range(n_groups):
    acc = 0.0
    indices = group_index_vectors[group_idx]
    col_data = column.data_ptr
    for i in range(indices.size):
        acc += col_data[indices[i]]
    result_buffer[group_idx] = acc
```

The index vectors can be built during the existing Parvi lookup pass at negligible extra cost
(each row appends its row number to its group's index vector).

**Failure path:** If the morsel has more distinct groups than the threshold, fall through to the
existing Carchar path unchanged. The threshold attempt cost is bounded: one pass over the morsel
to populate group assignments (already done) plus the index-vector append. The exponential
back-off that VectorWise uses (halving the attempt probability after each failure) is recommended
to avoid paying the index-vector cost on high-cardinality GROUP BY queries.

#### Expected benefit

The paper shows this is the mechanism by which VectorWise closes 90% of the gap with compiled
execution on Q1-style queries. For Opteryx, the gain materializes whenever:
- GROUP BY cardinality is low (status, region, category columns — extremely common in analytical
  workloads)
- The aggregate is arithmetic (SUM, AVG, COUNT — not string concatenation)
- Morsel size is large relative to group count (i.e., each group sees many rows per morsel)

Under these conditions the current path does N hash-table writes (one per row); the proposed path
does G aggregation-pass sweeps where G ≪ N, with each sweep accumulating into a register. Memory
traffic drops from O(N) random writes to O(G) sequential reads + O(G) result writes.

---

### Proposal 2 — SIMD First-Predicate Selection

**Relevant paper section:** Section 5.1 (Data-Parallel Selection), Figure 6

#### What the paper found

The paper benchmarks scalar branchless x86 selection against AVX-512 COMPRESSSTORE selection for
a 40% selectivity predicate on an in-cache integer array. The SIMD variant achieves **8.4×
speedup** on a dense (no input selection vector) predicate (Figure 6a). For a secondary selection
operating on a selection vector (sparse input, Figure 6b), the gain falls to 2.7×. For TPC-H Q6
end-to-end — which has five conjunctive predicates — the realized speedup is **1.4×** (Figure 6c).

The paper explains the hierarchy: the first predicate in a filter cascade always operates on a
dense column and achieves the maximum benefit. Each subsequent predicate receives a selection
vector as input and must gather non-contiguous elements before comparing, which reduces the SIMD
advantage because gather throughput is limited by memory port count regardless of SIMD width.

A key caveat: when the working set does not fit in cache, memory latency dominates and SIMD gains
nearly vanish (Figure 7). The benefit is real and consistent only when the column fits in L2 or
smaller.

#### Current state in Opteryx

Opteryx has SIMD for bitwise mask operations (`simd_bitops.cpp`: AND, OR, NOT, XOR, POPCOUNT)
using AVX2 on x86 and NEON on ARM64. These operate on pre-computed boolean byte masks — they
combine two masks but do not compute them.

The initial mask computation — comparing a column's values against a literal constant — runs
through the expression evaluator, which dispatches to type-specialized Draken vector comparison
methods. These comparisons produce a boolean byte mask. The comparison primitives themselves are
not explicitly SIMD-vectorized for the common `column OP constant` case.

The gap is precisely where the paper's largest gain lives: dense column against scalar constant,
first predicate, no input selection vector.

#### Proposed design

Add SIMD comparison primitives for the most common filter shapes: `int64 column == constant`,
`int64 column < constant`, `float64 column < constant`, and their negations. The output is the
same boolean byte mask that the existing path produces, so no downstream change is needed.

For AVX2 (x86 production target):

```cpp
// int64 column < constant, dense input, produces byte mask
void simd_lt_i64_scalar(const int64_t* col, int64_t val,
                        uint8_t* mask, size_t n) {
    __m256i vval = _mm256_set1_epi64x(val);
    size_t i = 0;
    for (; i + 4 <= n; i += 4) {
        __m256i vcol = _mm256_loadu_si256((__m256i*)(col + i));
        __m256i cmp  = _mm256_cmpgt_epi64(vval, vcol); // val > col[i]
        // extract 4 comparison bits, store as bytes
        mask[i]   = (cmp >> 0) & 1; // per-lane extraction
        ...
    }
    // scalar tail
}
```

AVX-512 COMPRESSSTORE (not available on all x86 targets) produces a packed index vector
directly; AVX2 requires extracting individual comparison bits and packing manually, which is
still substantially faster than scalar for dense input.

For NEON (ARM dev target), the equivalent uses `vcltq_s64` / `vcltq_f64` and `vst1q_u8` to
produce the mask at 2 int64 lanes per instruction.

These primitives slot into the Draken vector comparison dispatch table as specializations for
`(TypedVector, Literal)` operand pairs. The expression evaluator already dispatches to
type-specialized comparison methods; no architectural change is required, only new implementations
for the common scalar-constant case.

**Activation condition:** Only the dense path (no input selection vector) uses the SIMD primitive.
Compound expressions — `col_a < 10 AND col_b > 5` — will call the dense SIMD path for the first
operand, then combine masks with the existing `simd_and_mask()`. Subsequent predicates that
operate on a gathered subset still use the existing scalar path. This matches exactly the split
the paper recommends.

#### Expected benefit

For filter-heavy queries (selective scans, WHERE clauses with range predicates), the first
predicate evaluation is currently the bottleneck. The paper's Q6 result (five date-range
predicates, 1.4× end-to-end) is a realistic lower bound on the gain, because Opteryx evaluates
`col OP constant` predicates identically to Tectorwise's first-predicate case. For queries with
a single highly selective predicate on a numeric column (e.g., `WHERE event_date = '2024-01-01'`
on an int32 date column), the gain is closer to the 2–4× micro-benchmark range.

The gain is absent on string columns (gather cost dominates even for dense input) and on columns
that do not fit in L2 cache (~1 MB on production x86 targets). It is most pronounced on narrow
numeric columns that fit in cache — exactly the case for date, integer, and float filter columns
in typical analytical workloads.

---

### Proposal 3 — Morsel Sub-Batching for Aggregation Cache Efficiency

**Relevant paper section:** Section 4.3 (Vector Size), Figure 5

#### What the paper found

Figure 5 shows query runtime versus vector size for Tectorwise on TPC-H queries at scale factor 1.
The pattern is consistent across all five queries: small vectors (<64 tuples) are slow because
per-call interpreter overhead dominates. Large vectors (>64K tuples) are slow because
intermediate data — comparison results, hashes, candidate lists — no longer fits in L1/L2 cache
and causes cache misses.

The sweet spot is 1K–64K tuples:

> "Generally, a vector size of 1,000 seems to be a good setting for all queries. The only
> exception is Q3, which executes 15% faster using a vector size of 64K." (Section 4.3)

The paper quantifies the degradation at >64K: for Q1, runtime roughly doubles between 64K and
full-table processing. For Q6 it roughly triples. The mechanism is that intermediate vectors
(hashes, selection vectors, candidate match arrays) computed during the primitive cascade spill
from L1 to L2 or L2 to LLC.

#### Current state in Opteryx

Opteryx's unit of processing is the morsel, defaulting to 64 MB. For a column of `int64` values,
64 MB = 8 million rows. Even for a column of `float32`, it is 16 million rows. Both are orders of
magnitude beyond the paper's 64K upper boundary.

Within the aggregation hot loop, a 64 MB morsel is processed in a single pass: all group-hash
lookups and accumulate calls are issued against all rows before any flush. The intermediate
structures — the row-level group index assignments, the scratch vectors for new-group detection —
are proportional to morsel row count and will not fit in L1 or L2 cache for typical column widths.

For joins the impact is partially mitigated because Carchar itself is the bottleneck and its size
depends on build-side cardinality, not probe-side morsel size. But for aggregation the hash table
size is bounded by group count while the lookup volume is proportional to morsel row count — and
it is the lookup volume that determines cache pressure on the group-assignment arrays.

#### Proposed design

Introduce an internal **sub-batch size** for the aggregation hot loop, independent of the I/O
morsel size. The sub-batch size governs how many rows are processed per iteration of the outer
aggregation loop; the hash table and group buffers are shared across sub-batches for the same
morsel.

Proposed default: **4,096 rows** (16 KB for int32 keys, 32 KB for int64 — fits in L1 on both
target platforms). This is smaller than the paper's 1K recommendation because Opteryx processes
multiple columns and stores intermediate group-index arrays alongside the column data, so the
effective working set per row is larger than a single integer.

The outer morsel remains intact as the I/O and parallelism unit. Only the inner aggregation
loop is sub-batched. The change is localized to `_engine.pxi` and does not affect morsel
construction, connector reads, or the join path.

```
# Outer loop: iterate over morsel in sub-batches
for batch_start in range(0, morsel_row_count, SUB_BATCH_SIZE):
    batch_end = min(batch_start + SUB_BATCH_SIZE, morsel_row_count)
    # existing hash + lookup + accumulate logic on rows [batch_start, batch_end)
    ...
# finalize: merge sub-batch accumulators into final result
```

Because the Parvi/Carchar tables are shared across sub-batches within a morsel, group promotion
(Parvi→Carchar) still fires at the correct threshold. The only structural addition is that the
group-assignment scratch vector is allocated at sub-batch granularity rather than morsel
granularity.

**Interaction with Proposal 1:** Sub-batching makes the ordered aggregation path (Proposal 1)
more effective. The per-group index vectors built during a sub-batch fit entirely in L1, so the
ordered aggregation sweep is fully in-cache. At morsel granularity these vectors would spill.

**Tuning:** The optimal sub-batch size depends on group count and column width and should be
benchmarked against the ClickBench suite, which exercises both low-cardinality and
high-cardinality GROUP BY. The paper's recommendation of 1K–64K provides a well-validated search
range.

#### Expected benefit

The paper's data suggests that moving from full-table processing to a 1K–64K vector size reduces
runtime by roughly 50% on aggregation-heavy queries (Figure 5, Q1 at 4K vs. full). Opteryx's
64 MB morsels are effectively operating in the "full materialization" regime of the paper's
experiment. The gain from sub-batching is therefore expected to be significant for
aggregation-heavy queries (high-cardinality GROUP BY, multi-column aggregates) and negligible
for join-dominated queries where the bottleneck is hash table probing rather than intermediate
vector pressure.

A secondary benefit is reduced peak memory per thread. At 64 MB morsel granularity, each worker
thread allocates group-assignment scratch proportional to the full morsel row count. Sub-batching
caps this allocation at `SUB_BATCH_SIZE * sizeof(int32)` regardless of morsel size.

---

## 4. Implementation Priority

| Proposal | Expected gain | Implementation risk | Scope |
|---|---|---|---|
| 1 — Adaptive ordered aggregation | High for low-cardinality GROUP BY | Medium (new loop structure in hot path) | `_engine.pxi`, Parvi tier |
| 2 — SIMD first-predicate selection | Moderate (1.4–3× on filter-heavy) | Low (new primitives, no structural change) | `simd_bitops.cpp` + Draken comparison dispatch |
| 3 — Aggregation sub-batching | High for high-cardinality GROUP BY | Low (bounded change in `_engine.pxi`) | `_engine.pxi` only |

Proposal 3 is the lowest-risk starting point and is a prerequisite for Proposal 1 to be
maximally effective. Proposal 2 is entirely independent and can be developed in parallel.

---

## 5. What the Paper Validates That Opteryx Already Does Correctly

The paper provides an empirical basis for several architectural choices already present in Opteryx:

- **Vectorized (pull-model, primitive-based) execution** is competitive with data-centric
  compilation for OLAP workloads despite executing more instructions, because it better hides
  cache miss latency in join-heavy queries (Table 1, Section 4.1). Opteryx's vectorized model
  is architecturally sound for its target workloads.

- **Morsel-driven parallelism** scales as well as exchange-operator parallelism and significantly
  better on NUMA hardware (Section 6.1). Opteryx's morsel model is the right choice.

- **SIMD for bitwise mask operations** (AND/OR/NOT on selection vectors) is exactly where
  the paper shows reliable, memory-independent gains (Section 5.1). Opteryx's `simd_bitops.cpp`
  covers this correctly.

- **Bloom pre-filtering on joins** eliminates probe-side rows before hash table access, which
  is the right optimization for selective joins (Section 3.2). Opteryx's CarcharJoinEngine
  implements this.

- **Two-tier hash table for aggregation** (Parvi + Carchar) directly addresses the
  low-cardinality aggregation penalty that the paper identifies as a weakness of vectorized
  execution relative to compiled execution (Section 8.4). This is an architectural strength.

---

## 6. What the Paper Rules Out

- **SIMD for hash join probing** achieves at most 1.1× real-world speedup (Figure 8) because
  the bottleneck is memory latency, not computation. This is not worth the implementation cost
  in Opteryx.

- **AVX-512** (wider SIMD registers) provides minimal advantage over AVX2 for OLAP workloads
  dominated by memory-bound operations (Section 5, Figure 8). Opteryx's AVX2 target is correct.

- **Compiled query execution** is faster for computation-heavy queries but requires a JIT
  infrastructure (LLVM or similar), has high compile-time overhead, and significantly complicates
  adaptivity and profiling (Section 8). The paper does not recommend migrating a vectorized
  engine to compilation — only that hybrid approaches can capture some gains. The Parvi/Carchar
  adaptive tiers are a vectorized-native approach to the same problem.
