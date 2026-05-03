# Adaptive Join Statistics and Factorized Aggregation

**Source:** "Adaptive Factorization Using Linear-Chained Hash Tables", Groß, ten Wolde, Boncz — CIDR 2025  
**Status:** Design / Pre-implementation

---

## 1. Problem Statement

Opteryx's cost-based optimizer makes join ordering and strategy decisions at planning time using column-range histograms stored in `statistics.py`. These estimates are fragile: they do not account for operators between the scan and the join (filters, subqueries, projections), they are unavailable for Parquet files that haven't been profiled, and they say nothing about the *duplicate structure* of join keys — which is the variable that determines whether an n:m join will explode the intermediate result.

The paper identifies the same problem in DuckDB: "even information about foreign and primary keys is lacking, and it is even hard to identify an n:m join in the first place" when querying Parquet files directly. Their solution is to collect accurate statistics *during the hash join build phase*, at near-zero cost, because the key hash is already being computed anyway. These runtime statistics then drive adaptive decisions about how to execute subsequent operators.

This document describes three graduated improvements for Opteryx that draw from that work.

---

## 2. Relationship to the Paper

The paper makes three distinct contributions. The table below maps them to Opteryx:

| Paper Contribution | Applicable to Opteryx | Why |
|---|---|---|
| Linear-Chained Hash Table | **Partially already implemented** | `CarcharJoinIndex::RowListEntry` already chains duplicate-key rows via `inline0 / inline1 / overflow`. The paper's design and Opteryx's independently converged on the same structure. |
| Runtime HLL + AMS sketches during hash build | **Yes — missing today** | `insert_batch()` in `CarcharJoinEngine` computes key hashes but discards them. The bloom filter path already demonstrates the pattern of "capture the hash while you have it". |
| Factorized aggregation for n:m joins | **Yes — high value** | `GroupHashEngine` and `Hashed​InnerJoinNode` are independent operators today. When the GROUP BY key matches the join key the join can be deferred and aggregate partial results can be cached per chain head. |
| Worst-case optimal joins (triangle queries) | **Lower priority** | Requires cyclic join patterns. Worth revisiting if graph-style workloads become a target. |

---

## 3. Current Architecture

### 3.1 Hash Join Build

`build_side_carchar_morsel_map()` in [`draken_inner_join.pyx`](../opteryx/compiled/joins/draken_inner_join.pyx):

1. Calls `relation.hash(join_columns)` to produce a `uint64[N]` array of row hashes.
2. Strips NULL hashes via `_append_valid_rows_and_hashes()`.
3. Calls `CarcharJoinEngine::insert_batch()` to build the hash index.
4. Optionally builds a bloom filter from the same hashes (only when `N ≤ 16M`).
5. Returns a `DrakenCarcharJoinMap` containing the engine and the bloom filter.

The bloom filter construction is the precedent that matters: it already proves that reusing the computed hashes to build a secondary structure is both correct and cheap. The pattern here is deliberate — it is not an accident of the code.

### 3.2 Chain Structure

`CarcharJoinIndex::RowListEntry` in [`carchar_join_index.hpp`](../third_party/mabel/carchar/carchar_join_index.hpp) stores:

```cpp
struct RowListEntry {
    int64_t inline0 = -1;   // first row ID for this key (always used)
    int64_t inline1 = -1;   // second row ID (used when count ≥ 2)
    vector<int64_t> overflow; // rows 3+ (heap-allocated on demand)
};
```

`row_counts_[payload_ref]` gives the length of the chain for any key. This is exactly the "key-unique chain" structure from the paper. The paper calls this a *d-representation*: a pointer to the chain is a compact representation of all row bindings for that key, without materialising them. Opteryx uses it already for efficient duplicate-key joins, but does not yet exploit it beyond that.

### 3.3 Grouped Aggregation

`GroupHashEngine` in [`_engine.pxi`](../opteryx/operators/grouped_aggregate_hashed/_engine.pxi) is a self-contained operator. It:

- Computes `morsel.hash(group_columns)` independently per morsel.
- Looks up or inserts each hash into `ParviMap` (16-entry inline fast path) or `CarcharIndex`.
- Calls `collector.accumulate(morsel, state_indices, n_rows)` for each aggregate.

There is no communication between the join operator and the aggregate operator. If the query is `SELECT a.x, COUNT(b.y) FROM a JOIN b ON a.k = b.k GROUP BY a.x`, the join expands every (a, b) pair and the aggregate processes the full expanded set. When `b.k` has high duplication this is wasteful: for every group in `a.x`, the same set of `b.y` values is counted repeatedly.

---

## 4. Proposed Changes

### Phase 1 — Export Chain Statistics After Build

**What:** After `seal()` completes, expose the chain length distribution from `CarcharJoinEngine` back through the Cython layer as part of the join metrics already collected by `last_draken_inner_join_*`.

**Specifically:** Add `average_chain_length()` and `unique_key_count()` methods to `CarcharJoinEngine`. Surface them alongside the existing metrics in `get_last_draken_inner_join_metrics()`.

**Why this matters:** The paper demonstrates (Table 1) that *average chain length* is the single strongest predictor of whether factorized execution will outperform a flat binary join (Pearson r = 0.58 for 1st-join chain length, 0.64 for min-chain-length across both joins). Collecting this is free: `row_counts_` already exists and its average can be computed in O(1) after build (total rows inserted / unique key count, both already tracked as `size_` in `CarcharJoinEngine`).

**Changes:**

| File | Change |
|---|---|
| `third_party/mabel/carchar/carchar_join_index.hpp` | Add `average_chain_length()` → `(double)total_rows / unique_keys` using existing `row_counts_` sum |
| `third_party/mabel/carchar/carchar_join_engine.hpp` | Aggregate per-partition chain stats; expose `average_chain_length()` and `unique_key_count()` |
| `opteryx/compiled/joins/draken_inner_join.pyx` | Capture chain stats post-seal; include in `get_last_draken_inner_join_metrics()` |
| `opteryx/operators/hashed_inner_join/hashed_inner_join.pyx` | Read new metrics; log or store on the node for planner feedback |

**Expected overhead:** Zero at runtime — the data already exists. The average is one division.

---

### Phase 2 — HLL Sketch During Hash Join Build

**What:** Construct a HyperLogLog sketch from key hashes during `insert_batch()`, immediately after computing the key hash that is already being used for the join index. Expose the resulting distinct-key count estimate alongside the Phase 1 chain statistics.

**Why this matters:** The paper uses HLL to estimate the number of distinct keys on the *second* join's build side, which — combined with the exact chain length from the first join (Phase 1) — gives the "Min Average Chain Length" feature. This feature has the highest correlation with factorization benefit (r = 0.64). The H2 heuristic in the paper (`MinChainLength > 5.5`) uses exactly this. Average chain length is `total_rows / HLL_distinct_count` — precise for the first join, estimated via HLL for the second.

**Crucially:** The paper notes (Section 3) that this sketch costs approximately 0.35% of total query runtime on TPC-H. Opteryx already demonstrates the "free hash reuse" pattern for the bloom filter. The HLL is built from the same hash values, so the hashing cost has already been paid.

**Changes:**

| File | Change |
|---|---|
| `third_party/mabel/carchar/carchar_join_engine.hpp` | Add `hllpp.h` (already vendored for `_collectors_approx.pxi`); maintain one HLL sketch per `CarcharJoinEngine`; feed each key hash into it during `insert_batch()` |
| `opteryx/compiled/joins/draken_inner_join.pyx` | Expose `hll_distinct_count()` from the sealed engine; include in metrics |
| `opteryx/planner/optimizer/statistics.py` | Accept runtime-measured chain statistics as an alternative to histogram-derived selectivity; use when histogram estimates are absent (Parquet) or stale |

**Notes on HLL precision:** The existing `HllppSketch` (precision=14, used in `_collectors_approx.pxi`) gives ~0.8% relative error. A lower precision (p=10, ~3.25% error) would be sufficient for the join/factorization decision and use 16× less memory. Two separate precisions may be warranted — or the existing implementation reused if memory is not a concern at this scale.

**Design boundary:** The HLL sketch built here is a *planning aid*, not a query result. It does not replace `APPROX_COUNT_DISTINCT`. Its cardinality estimate feeds the optimizer and is discarded after the query.

---

### Phase 3 — Factorized Aggregation for Co-Located GROUP BY + Join

**What:** When the query plan has the shape `GROUP BY <key>` immediately above a hash join where the GROUP BY key is the join key, defer join expansion and compute partial aggregates per chain head rather than per expanded row.

**Why this matters:** This is the paper's core result. In their benchmark (500K-row Orders, 1M-row Parts, average chain length 50), factorized aggregation with chain-head caching delivers a **17.58× speedup** over flat execution (Figure 5). The gain scales with average chain length: longer chains mean more redundant work in the flat plan.

The pattern in Opteryx SQL is common:

```sql
SELECT customer_id, COUNT(order_id)
FROM orders
JOIN customers ON orders.customer_id = customers.id
GROUP BY customer_id
```

Here `customers.id` is likely unique (chain length = 1, no benefit) but `orders.customer_id` is not. The mechanism is symmetric: it applies when the *build* side has high duplication on the join key that is also the GROUP BY key.

**Mechanism:** The paper's approach, adapted to Opteryx's morsel-driven architecture:

1. During hash join build, if the query plan is flagged as factorization-eligible, allocate a *chain head aggregate cache* alongside the chain index: one partial aggregate slot per unique key (indexed by `payload_ref`).
2. During probe, instead of emitting `(left_row, right_row)` pairs for every chain member, emit one `(payload_ref, probe_row)` pair per probe hit.
3. The aggregate collector traverses the chain once per unique key: for `COUNT(expr)` multiply chain length by the probe occurrence count; for `SUM(expr)` sum the build-side `expr` once and multiply; for `MIN/MAX` traverse once and cache.
4. Results are correct because the aggregate is computed over the same set of values — just without materialising the Cartesian product.

**When to apply (from the paper's H4 heuristic):**

```
use_factorized = (min_chain_length > 5.5 OR min_key_skew > 5)
                 AND join_size_ratio > 0.05
```

`min_chain_length` is available from Phase 1. `join_size_ratio` is `build_side_1_cardinality / build_side_2_cardinality`, available from morsel counts during planning or at runtime. `min_key_skew` requires the AMS sketch (a further extension, not proposed here). H2 alone (`min_chain_length > 5.5`) already achieves 1.27–1.37× speedup across all workloads in Table 2.

**Changes required:**

| Component | Change |
|---|---|
| `opteryx/planner/` | Detect factorization-eligible plan shape during binder/optimizer pass; annotate the join node with `factorize=True` |
| `opteryx/operators/hashed_inner_join/hashed_inner_join.pyx` | When `factorize=True` and Phase 1 chain stats meet H2 threshold at runtime, switch to chain-pointer probe mode |
| `third_party/mabel/carchar/carchar_join_index.hpp` | Add `probe_chain_heads()` path that emits `(payload_ref, probe_row)` pairs instead of expanding |
| `opteryx/operators/grouped_aggregate_hashed/_engine.pxi` | Add `ingest_factorized()` path that accepts `(payload_ref, probe_row)` pairs and uses chain head caching |
| `opteryx/operators/grouped_aggregate_hashed/_collectors_*.pxi` | Add `accumulate_factorized(chain_head, chain_length, probe_count)` to numeric collectors |

**Phase 3 is the largest change and has the most cross-operator coupling.** It should not be started without Phase 1 in place, since the chain-length threshold check that controls whether factorization activates depends on Phase 1 statistics.

---

## 5. Interaction with Existing Systems

### Bloom Filter

The bloom filter built in `build_side_carchar_morsel_map()` is constructed from the same `valid_hashes` vector as the join index. The HLL sketch (Phase 2) should be built from the same vector, before the bloom filter step, so all three structures share one pass over the data. The existing guard (`valid_hashes.size() <= 16_000_000`) for bloom filter construction does not apply to HLL — an HLL sketch is constant-memory regardless of input size and should always be built.

### Parquet Statistics Gap

The paper specifically calls out querying Parquet files as the hardest case for static statistics: "even information about foreign and primary keys is lacking". Opteryx will face the same condition. Runtime HLL and chain statistics are the only accurate source of join cardinality in that case. Phase 2 is therefore particularly valuable for Parquet-heavy workloads, where planning-time statistics are absent or stale.

### Morsel-Driven Parallelism

`CarcharJoinEngine` is partitioned. Chain statistics must be aggregated across partitions (total rows / total unique keys). The HLL sketch is mergeable — HLL sketches from independent partitions can be unioned without loss of accuracy, which suits the partitioned build perfectly.

### Adaptive Fallback

The decision to use factorized execution (Phase 3) is made *after the build phase completes*, using runtime chain statistics. This is the paper's central architectural point: defer the decision from the optimizer to runtime. The optimizer marks the plan as *eligible* but does not commit. If the chain statistics fall below the threshold (e.g., the join turns out to be 1:1), flat execution proceeds unchanged. There is no regression path for plans that fall through — they execute exactly as they do today.

---

## 6. Implementation Order and Risk

| Phase | Effort | Risk | Benefit |
|---|---|---|---|
| 1 — Chain stats export | Small (C++ + Cython) | Negligible | Enables Phase 3 gate; visible in telemetry |
| 2 — HLL during build | Small–Medium (C++) | Low (additive only) | Feeds planner; fixes Parquet cardinality gap |
| 3 — Factorized aggregation | Large (cross-operator) | Medium (new code path through probe + aggregate) | 5–17× on high-duplication n:m joins |

Phases 1 and 2 are purely additive — they add output but change no existing behaviour. Phase 3 adds a new code path gated by a runtime condition; the existing path is always available as fallback. All three phases are consistent with the zero-dependency and no-PyArrow constraints.

---

## 7. What Is Not Proposed

**Worst-case optimal joins (WCOJ) for cyclic patterns** (triangle queries). The paper devotes significant space to this. It requires detecting cyclic join graphs in the planner and implementing list-intersection probing. This is a worthwhile future direction if graph-pattern workloads become a target, but the benefit in typical SQL analytics is more limited and the implementation complexity is substantially higher than the three phases above.

**AMS sketch for key skew estimation.** The paper uses AMS sketches to estimate cross-join cardinality between the two build sides. This enables the `MinKeySkew` feature in the H4 heuristic. H2 (`MinChainLength > 5.5`) alone achieves 92% of H4's speedup and requires only Phase 1 data. AMS adds marginal benefit over H2 for the non-graph workloads Opteryx primarily handles.

---

## 8. References

Groß, P., ten Wolde, D., and Boncz, P. (2025). *Adaptive Factorization Using Linear-Chained Hash Tables*. CIDR 2025, Amsterdam. CC-BY 4.0.

Specific sections cited in this document:
- §2 (Linear-Chained HT design and chain structure)
- §3 (AMS and HLL sketch construction during hash build; runtime adaptivity mechanism)
- §4 Table 1 (feature correlations with WCOJ speedup)
- §4 Table 2 (H1–H4 heuristic speedups across workloads W1–W4)
- §4 Figure 5 (17.58× factorized aggregation speedup with chain-head caching)
