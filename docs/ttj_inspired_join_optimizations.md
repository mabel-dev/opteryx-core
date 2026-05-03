# TTJ-Inspired Join Optimizations

**Status:** Proposal  
**Author:** Design review prompted by Hu, Wang, Miranker (2025)  
**Reference:** "TreeTracker Join: Simple, Optimal, Fast" — Zeyuan Hu, Remy Wang, Daniel P. Miranker.  
arXiv:2403.01631v4, May 2025. Available at https://arxiv.org/abs/2403.01631

---

## 1. Background

The TreeTracker Join (TTJ) paper introduces a linear-time acyclic join algorithm built as a minimal
modification to binary hash join. When a hash probe fails, TTJ backjumps to the tuple that bound
the failing key and deletes it from its relation. This prevents the same doomed key from being
probed again, achieving O(|IN| + |OUT|) time on acyclic queries.

The paper proves TTJ makes no more hash probes than standard hash join on the same plan, for any
query — acyclic or cyclic. Empirically, TTJ achieves an average 1.11× speedup over hash join on
the Join Order Benchmark, 1.09× on TPC-H, and 1.15× on the Star Schema Benchmark, with
maximum per-query speedups of 12.6× (JOB query 16b), 1.9× (TPC-H Q9), and 1.7× (SSB Q2.2).

The paper also analyses Yannakakis's algorithm (YA), which runs a semijoin reduction pass before
joining. YA's worst-case overhead is that the semijoin pass always runs even when it removes
nothing. Its best case (JOB query 6a) reduces the largest relation (cast_info) from 36,000,000
rows to 486 rows before any join begins, making hash table construction trivially fast. YA achieves
average speedups of 1.60× (JOB), 1.40× (TPC-H), and 3.16× (SSB) over hash join.

### What does not transfer to Opteryx

TTJ's core execution mechanism — per-row backjumping and hash table tuple deletion — assumes
tuple-at-a-time execution. The paper explicitly notes: *"future work shall explore how to
incorporate various system-level optimizations like query compilation, vectorization, and
parallelization into TTJ."* Opteryx operates on Draken morsels (columnar batches), so the
row-level deletion model does not map cleanly to the execution engine without invasive changes
to the Carchar C++ hash table. That work is deferred.

What does transfer is the **insight behind** TTJ: dangling tuples (tuples that can never produce
output because no matching key exists in a downstream relation) are wasted work, and the earlier
they are removed from the pipeline, the larger the saving. Three adaptations to Opteryx's
existing optimizer and join infrastructure can capture a significant share of this benefit.

---

## 2. Current State in Opteryx

### Join execution

Opteryx's primary equi-join path is `DrakenInnerJoinNode`
(`opteryx/operators/hashed_inner_join/hashed_inner_join.pyx`), backed by the Carchar C++ hash
table engine (`third_party/mabel/carchar/carchar_join_engine.hpp`). Execution is morsel-based:
the left (probe) side is fully buffered and hashed, then right (build) morsels are probed. A
bloom filter is optionally created for the build side when cardinality is ≤ 16M rows, which
eliminates probes for keys not present in the build relation. This filter operates in the
**probe → build** direction.

Semi-join and anti-join are handled by `FilterJoinNode`
(`opteryx/operators/filter_join/filter_join.pyx`), which builds a hash set rather than a hash
table and applies it as a row filter over the left side. These are currently only emitted by the
planner when the SQL query explicitly uses `IN (subquery)` or `NOT IN (subquery)` syntax.

### Join planning

`JoinOrderingStrategy` (`opteryx/planner/optimizer/strategies/join_ordering.py`) arranges
two-relation join pairs so the smaller side is always on the left (probe) and the larger side on
the right (build). The decision uses: relation byte size, column cardinality from manifest
statistics, and null fraction adjustments. For sufficiently small relations it falls back to
nested loop join.

`JoinRewriteStrategy` (`opteryx/planner/optimizer/strategies/join_rewriter.py`) has stubs for
rewriting LEFT OUTER JOINs to INNER JOINs and semi/anti joins based on surrounding filter
predicates. These rewrites are currently unimplemented (they emit `warnings.warn`).

### What is missing

- No mechanism to prune dangling tuples from either side of a join **before** hash table
  construction begins. The bloom filter does late-stage filtering (during probing) but the build
  side is still fully hashed, including rows that can never produce output.
- No probe-side filter accumulating keys known to fail, which would let the outer (probe)
  relation skip rows on subsequent morsels.
- Join ordering decisions are made per binary pair in isolation. For multi-way joins the plan is
  constructed left-deep, but there is no global analysis of the join graph structure to detect
  acyclicity or find a GYO reduction order.

---

## 3. Proposed Optimizations

### 3.1 Semi-join injection (optimizer strategy)

**What it is.** For acyclic equi-join queries, inject a `FilterJoinNode` (semi-join) immediately
before each large hash join build. The semi-join filters the build side using the probe side's
join key set, removing rows that cannot produce output before the full hash table is constructed.
This is equivalent to one pass of Yannakakis's semijoin reduction, applied selectively via
cost gating.

**How it connects to the paper.** Section 3.3 of the TTJ paper describes YA's single-pass semijoin
reduction: for each atom R_i in GYO reduction order, R_i is reduced by a semijoin against its
parent before the join is computed. The paper's most dramatic case study (YA on JOB query 6a,
Section 6.1) shows cast_info shrinking from 36 million to 486 rows before any join begins; hash
table build time drops from 13,398 ms to 499 ms. The TTJ paper's Table 1 shows YA averages
1.60× faster than hash join on JOB and 3.16× on SSB. These workloads are representative of
Opteryx's analytical query patterns.

**Where the code lives.**

- New optimizer strategy: `opteryx/planner/optimizer/strategies/semijoin_injection.py`
- Hooks into the same registration point as `JoinOrderingStrategy` and `JoinRewriteStrategy` in
  `opteryx/planner/optimizer/strategies/__init__.py`
- Emits `FilterJoinNode` plan nodes, reusing the existing semi-join execution path in
  `opteryx/operators/filter_join/filter_join.pyx`

**Algorithm.**

```
for each inner-join node J in the logical plan (leaf-first, bottom-up):
    if J.right_size > SEMIJOIN_SIZE_THRESHOLD:         # e.g. 50_000 rows
        selectivity = estimate_semijoin_selectivity(J) # via manifest stats
        if selectivity < SEMIJOIN_SELECTIVITY_GATE:    # e.g. 0.5
            inject FilterJoinNode(left=probe_columns, right=build_columns)
                immediately before J's build side
```

Selectivity is estimated from manifest cardinality statistics already collected for
`JoinOrderingStrategy`. The semi-join is only injected when there is statistical evidence it
will reduce the build side materially; otherwise it adds only overhead (the TTJ paper Section 6.1
notes the no-good list optimization regresses on queries where each filter element only reduces
intermediate results by a factor of 182, versus 318 for the cases where it helps).

**Expected benefit.** The semi-join adds one hash set build and one probe pass over the build
relation before hash table construction. For star schema queries this cost is O(|fact_keys|) and
the saving is proportional to how many dimension table rows have no matching fact key. In the
SSB benchmark the paper reports YA averages 3.16× speedup over hash join; a selective semi-join
injection will capture a fraction of this on queries where dimension tables have significant
dangling tuples.

**Cost gating is critical.** The paper (Section 4.2, Example 4.11) shows: *"when every tuple
successfully joins, TTJ behaves identically to binary join. However, YA spends additional time
futilely computing semijoins."* The gate condition must prevent injection when the join is likely
selective (few dangling tuples), such as joins between a primary key and a referencing foreign
key on a well-filtered dataset.

---

### 3.2 Probe-side no-good bloom filter

**What it is.** During a multi-morsel join execution, maintain a bloom filter on the probe side
tracking hash keys that produced zero matches. On subsequent probe morsels, rows whose join key
hashes into the no-good filter are skipped before probing the hash table. The filter is reset
between queries.

**How it connects to the paper.** Section 5 of the TTJ paper describes the no-good list
optimisation: key values from the leftmost (probe-driver) relation that caused a lookup failure
are added to a blacklist. The paper (Section 6.1) explains: *"the no-good list acts as a filter
that prevents any processing of a fact tuple whose join key values have already been determined
to be fruitless."* For SSB queries with high-selectivity dimension joins, each no-good entry
reduced intermediate results by an average of 318 for the fast queries. The optimisation is most
effective when the leftmost relation is a large fact table joined to selective dimension tables —
exactly the pattern Opteryx serves for analytical workloads.

The existing bloom filter in `DrakenInnerJoinNode` operates in the **build → probe** direction:
it is built from the build side and used to filter probe-side lookups. The no-good filter
operates in the **probe → probe** direction: it is accumulated as probe-side failures are
observed and applied to future probe morsels.

**Where the code lives.**

- `opteryx/operators/hashed_inner_join/hashed_inner_join.pyx`: add `no_good_filter` state
  (reuse `BloomFilter` already available in Draken or a simple hash set)
- `opteryx/compiled/joins/draken_inner_join.pyx`: add a pre-probe filter check before
  `inner_join_carchar_morsel_aligned` iterates rows

**Algorithm (morsel-level).**

```
on probe morsel M:
    1. compute join key hashes for M
    2. mask = bloom_filter.query_batch(hashes)     # rows not in no-good set
    3. filtered_M = M.filter(mask)
    4. probe filtered_M into carchar hash table
    5. collect zero-match hashes from this probe
    6. no_good_filter.insert_batch(zero_match_hashes)
```

Step 5 requires `inner_join_carchar_morsel_aligned` to report which probe hashes produced no
matches — this is already tracked internally (it is how the bloom filter counts eliminations)
but not currently surfaced to the operator.

**Expected benefit.** Probe rows that have already been determined fruitless on an earlier morsel
are eliminated at step 2 with no hash table lookup. The benefit compounds: once a key is added
to the no-good filter it is eliminated for all remaining morsels. For a query where 60% of fact
table keys have no matching dimension key, the last morsels to be processed spend up to 60% of
their probe work on already-known failures. The no-good filter eliminates this progressively as
the query executes.

**Cost.** A bloom filter probe per row per morsel, plus minor accumulation overhead. Bloom filter
probes are ~1–2 ns at L1-cached sizes; this is negligible compared to a hash table lookup
(~4–10 ns). The filter should be capped at a size proportional to the probe relation (e.g.
1 bit per expected probe row, up to 64 MB) to prevent memory pressure on very large relations.

---

### 3.3 GYO-informed join ordering for multi-way joins

**What it is.** For queries joining three or more relations, detect whether the join graph is
acyclic (α-acyclic in the sense of Definition 3.1 in the paper), and if so, compute a GYO
reduction order to guide the join sequence. This replaces or supplements the current per-pair
heuristic in `JoinOrderingStrategy`, which optimises each two-relation join in isolation and
may produce a suboptimal global ordering for the multi-join.

**How it connects to the paper.** Section 3.1 defines the GYO algorithm: iteratively identify
ears (atoms whose key schema is fully covered by another atom) and attach them to their parent.
Theorem 4.7 proves that executing TTJ in reverse GYO order is what achieves the linear time
guarantee. While Opteryx is not implementing TTJ's row-level execution, the GYO order is also
the order that minimises intermediate result sizes in a standard hash join pipeline — each ear
is joined last to its parent, so intermediate results are bounded by the parent relation size
rather than the Cartesian product of unordered joins.

The paper (Figure 2a) gives GYO as eight lines of pseudocode. The parent function (Figure 2b)
is five lines. Both are O(|atoms|²) in the number of relations — negligible for query planning.

**Where the code lives.**

- New function `gyo_reduction_order(join_graph)` in
  `opteryx/planner/optimizer/strategies/join_ordering.py` or a new utility module
- Called from `JoinOrderingStrategy.complete()` when three or more join nodes are present in
  the plan

**Algorithm.**

```
1. Build a join hypergraph from all Join nodes in the plan:
   nodes = relations; edges = join conditions (shared columns)
2. Run GYO: repeatedly find an ear R (a relation whose key schema is
   a subset of some other relation's schema), record R → parent(R),
   remove R from the active graph
3. If GYO reduces the entire graph to a single node → query is acyclic;
   use reverse of GYO reduction order as the join sequence
4. If GYO stalls (cyclic subgraph remains) → fall back to current
   per-pair heuristic for the cyclic portion; apply acyclic ordering
   to the remainder
```

**Expected benefit.** For acyclic queries (which dominate analytical SQL), GYO ordering
guarantees that intermediate results are minimised globally rather than per-pair. The current
heuristic can produce suboptimal orderings when three or more relations are joined because it
does not consider how the join of A and B affects the size of the input to a later join with C.

This optimisation also enables the semi-join injection in Section 3.1 to fire in the right
order: the GYO reduction order is exactly the order in which semijoins should be applied for
maximum pruning.

---

## 4. Implementation Priority

| Optimisation | Effort | Expected Gain | Risk |
|---|---|---|---|
| Probe-side no-good bloom filter | Low — extends existing bloom filter infrastructure in `hashed_inner_join.pyx` | Moderate — benefit compounds over morsel iterations; strongest on large fact tables | Low — purely additive, guarded by a size cap; no plan changes |
| Semi-join injection | Medium — new optimizer strategy, uses existing `FilterJoinNode` execution | High — can produce order-of-magnitude savings when first join is highly selective; mirrors YA's best-case results | Medium — cost gate must be tuned; over-injection is worse than doing nothing |
| GYO-informed join ordering | Medium — requires join graph construction and GYO traversal at plan time | Moderate — corrects the ordering of multi-way joins; prerequisite for semi-join injection to fire optimally | Low for acyclic detection; Medium for multi-join plan restructuring |

The recommended sequence is: no-good bloom filter first (self-contained, no plan changes), then
GYO ordering (enables better semi-join targeting), then semi-join injection (highest upside,
needs GYO ordering to be reliable).

---

## 5. What This Does Not Include

**TTJ row-level backjumping and tuple deletion.** As discussed in Section 1, this requires
restructuring join execution to operate at row granularity inside the probe loop — incompatible
with morsel-based vectorized execution. The paper acknowledges this as future work. It is not
proposed here.

**Worst-case optimal join (Leapfrog Triejoin).** Referenced in the paper (Section 2) as the
standard approach for cyclic queries. Opteryx does not have cyclic join workloads currently; if
this changes, it warrants a separate design.

**Cost-based TTJ plan optimisation.** The paper (Section 8) notes that estimating *hash probe
failures* rather than intermediate result sizes would produce better plans for TTJ. This is
deferred until the core TTJ execution model is adapted to vectorized execution.

---

## 6. Acceptance Criteria

A change implementing any of the above is complete when:

- `make q` passes (88/88 tests) with the optimisation enabled
- The optimisation is gated by a `features` flag so it can be disabled if regressions emerge
- Telemetry counters are added (consistent with the existing pattern:
  `rows_eliminated_by_no_good_filter`, `semijoin_injections`, `gyo_acyclic_queries`)
- ClickBench does not regress (41/42 or better)
- The cost gate for semi-join injection is validated against at least one query where
  injection would be net-negative (no filtering) to confirm the gate prevents regression
