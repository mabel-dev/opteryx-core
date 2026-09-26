# GROUP BY → ORDER BY <aggregate> LIMIT k fusion

Status: **v1 IMPLEMENTED 2026-09-26** (architect ruled GO the same day). See
"v1 as built" at the end for the shipped shape and its measured A/B.
Date: 2026-09-26.

## TL;DR

Keeping only each hash partition's top-k groups inside `GroupBySink::finalize`, and
gathering key values for those k groups only, cut the 12 high-cardinality ClickBench
target queries by:

- **−23.7%** on parquet (`scratch.hits_rugo_262k`): 6.51 s → 4.97 s.
- **−28.1%** on skene (`scratch.hits_skene`): 5.76 s → 4.14 s.

The biggest wins were Q19 −45/−48%, Q33 −28/−31%, Q34/Q35 −24/−30% and Q17 −19/−21%.
Results were identical apart from legitimate ties at the LIMIT boundary.

The original hypothesis was half right:

- **Wrong:** the cost is not the TopNSink's repeated compaction. The whole TopN
  pipeline is 3–42 ms of wall.
- **Right:** the cost is `finalize` emitting every group's key values, which is
  56M string/int rows for Q19 and 99M for Q33, only for 10 to be kept.

## Posture

- Measured on this checkout after a fresh `make compile` (Python 3.14.5, Apple
  Silicon, DOP 16, no LTO/PGO, no preload).
- Stored ClickBench results and the comparison files were **not** used.
- Harness (kept in scratch, not the repo): each query is timed through
  `session.execute_to_morsels`, reading `native_op_stats` (GroupBy `finalize_time`,
  HeapSort thread time) and `native_pipeline_stats` (per-pipeline wall).
- The finalize CPU split comes from the existing `OPTERYX_GB_FINALIZE_PROF=1` probe.
- A/B used the **same binary** with a temporary env-armed probe:
  - ABBA order per query, one fresh process per arm, 3 runs per process.
  - The reported figure is the min over 6 runs per arm.
  - Arm spreads were within ~2–5%, so every delta above is outside noise.

## Where the time goes today (baseline, parquet)

Per-finalize CPU, summed across the 16 merge threads:

| Q | groups | wall ms | GB finalize wall ms | merge CPU | **keys CPU** | lanes CPU | TopN pipe wall ms |
|---|---|---|---|---|---|---|---|
| 19 | 55.9M | 1262 | 727 | 1976 | **9394** | 15 | 23 |
| 33 | 99.0M | 1107 | 717 | 5180 | **5630** | 430 | 42 |
| 34 | 18.2M | 1036 | 313 | 789 | **3936** | 9 | 8 |
| 35 | 18.2M | 962 | 247 | 725 | **3080** | 9 | 11 |
| 17 | 23.9M | 483 | 141 | 728 | **1434** | 9 | 10 |
| 15 | 6.4M | 242 | 42 | 225 | 381 | 3 | 3 |
| 13 | 6.0M | 208 | 34 | 222 | 267 | 3 | 3 |
| 16 | 17.5M | 195 | 53 | 583 | 212 | 6 | 7 |

`keys` is the `keyref` → `append_from` gather plus `jpc_emit_range`
(`finish_and_emit`). It is the single largest finalize cost wherever the key is a
string or wide composite.

Queries 9–12, 22, 23, 28 and 29 have ≤ 9k groups after their filters, so their
ceiling is ≈ 0. They gain nothing, and v1 does not need to target them.

## Prototype result (ABBA, min of 6 per arm)

| Q | parquet off → on (ms) | Δ | skene off → on (ms) | Δ |
|---|---|---|---|---|
| 13 | 226.9 → 217.5 | −4.1% | 173.6 → 152.7 | −12.0% |
| 14 | 279.7 → 263.5 | −5.8% | 238.9 → 216.0 | −9.6% |
| 15 | 245.4 → 220.4 | −10.2% | 198.5 → 171.1 | −13.8% |
| 16 | 205.8 → 193.6 | −5.9% | 206.2 → 191.0 | −7.4% |
| 17 | 506.0 → 410.2 | −18.9% | 450.1 → 356.1 | −20.9% |
| 19 | 1296.5 → 711.3 | **−45.1%** | 1223.8 → 635.1 | **−48.1%** |
| 31 | 259.0 → 253.2 | −2.2% | 174.4 → 170.4 | −2.3% |
| 32 | 263.9 → 246.7 | −6.5% | 192.4 → 170.2 | −11.5% |
| 33 | 1133.7 → 815.1 | **−28.1%** | 1082.1 → 751.1 | **−30.6%** |
| 34 | 944.3 → 721.1 | **−23.6%** | 820.3 → 569.9 | **−30.5%** |
| 35 | 947.7 → 719.7 | **−24.1%** | 808.1 → 575.6 | **−28.8%** |
| 36 | 202.7 → 194.2 | −4.2% | 190.3 → 183.5 | −3.6% |
| **Σ** | 6511.6 → 4966.5 | **−23.7%** | 5758.7 → 4142.7 | **−28.1%** |

**Correctness (parquet, all 12):**

- The ORDER BY value column was identical with the probe off and on.
- The full row set was identical for 10 of the 12.
- Q32 and Q33 differ only among rows tied at the boundary value (c = 1), where any
  choice is correct.

**What is left in finalize with the probe on (CPU):**

- `merge` dominates: Q33 5.3 s, Q19 2.1 s.
- Keys are ≤ 23 ms.
- The prototype's selection pass costs 80–250 ms. It converted every value to
  `double` and ran `nth_element` over a full index array; the real design reads
  the lane directly.
- The prototype still emitted every lane full-range before gathering k rows: 415 ms
  CPU on Q33. The real design should compact the lanes first.

Both leftovers are prototype shortcuts, so the production version should do at
least as well as the prototype.

## Design

### Where the information comes from

- `HeapSortNode` compiles in `compiler.py` (the `kind == "HeapSortNode"` branch).
  It calls `_compile_only_child`, and the child compiles the
  `GroupedAggregateHashedNode`.
- The GroupBy branch creates the sink on pipeline `p`, then returns the
  buffer-source pipeline `p2` with its layout. HAVING is added to `p2` by
  `_apply_having`.
- **The compiler records** `p2 → (p, has_having, set_masks, out_layout,
  spec identities)` when it builds a GroupBy sink.
- In the HeapSort branch, when the child pipeline is such a `p2`:
  - Resolve each `order_by` identity against the GroupBy output layout.
  - If the shape qualifies (below), call a new `nplan.set_groupby_topk(p, order
    keys, k)`.
  - Each order key is (spec index or emitted-key index, ascending, nulls policy).
  - k = `node.step.limit`.
- The HeapSort/TopNSink is still compiled **unchanged**. It remains the exact final
  ordering and LIMIT, so the GroupBy side only ever *reduces* its input, and every
  armed plan is correct by construction.
- This is a plan-shape decision in the compiler, not a Python runtime eligibility
  gate. Nothing is decided per morsel.

### Qualifying shapes (v1)

| Condition | Why |
|---|---|
| No HAVING | Pruning before HAVING could drop groups HAVING would keep while keeping ones it removes. v2 option: evaluate HAVING inside finalize before selection. Q28 and Q29, the only HAVING targets, have ≤ 100 groups, so nothing is lost. |
| Every ORDER BY key is a GroupBy output (aggregate spec or emitted key), not an expression over them | Selection happens before `p2`'s operators run. |
| Nothing between the GroupBy buffer and the TopNSink filters rows | Only select/project may sit in `p2`. |
| No ROLLUP/CUBE/GROUPING SETS | `grouping_id` expansion; keep them out of v1. |
| LIMIT present | OFFSET never reaches HeapSort today (see findings). |

DISTINCT-operand aggregates qualify: `gb_fold_distinct` runs before emit.

### Per-partition selection (native)

In `finish_and_emit`, when armed and `merged.size() > k`:

1. **Selection** reads the sort-key lane(s) directly (`grows`, `i64`, `f64`
   plus `valid`), with no emit.
   - It uses **draken's sort-key normalisation and comparator** (`build_sort_keys`
     / `SortKeyCmp`, `draken/morsels/sort.hpp`), so NULL/NaN placement is
     bit-identical to TopNSink (see the null-ordering note).
   - An ad-hoc compare is not acceptable: a fast path whose ordering differs from
     the uniform path is a bug.
2. **Ties.**
   - Single ORDER BY key: any k of the tied rows is exact.
   - Multi-key ORDER BY: keep the top k by the full comparator. That is equally
     exact, because the comparator *is* the final order.
   - No tie-widening is needed if the full key tuple is compared here, and v1
     should do that.
3. **Compaction.** Compact the k winners' lanes into k-row lane arrays, then emit
   them. Gather key values for the k winners only, through `keyref` (radix path)
   or `keycols` (leaf path).
4. **Output.** Emit one ≤ k-row morsel per merged partition or radix bucket. Across
   64 partitions × ≤ 64 buckets × k, that is at most ~41k rows for k = 10, which
   the TopNSink handles trivially.

**Why this is exact.**

- Partitions (and radix buckets inside a partition) hold disjoint group sets,
  because they are split by the group hash.
- After merge, a group's aggregate is final.
- So the global top k ⊆ the union of per-bucket top k.
- The TopNSink then picks the exact global top k from that union.

**Optional v1.1.** A per-worker running threshold, i.e. the k-th best value seen so
far across the partitions a thread has finished, can skip compaction for buckets
whose best value cannot beat it. Measure it before adding.

### Group identity (for a ruling, not part of this change)

- Groups are identified by their 64-bit draken hash alone. Fusion does not change
  this and cannot fix a collision, because two keys that collided already share
  merged lanes.
- Fusion does make verifying the k winners cheap. The cheapest useful form would
  re-hash each winner's gathered key and assert it matches, which catches only a
  corrupted keyref, not a collision.
- A real collision check needs key equality at probe time. That is out of scope
  here; it is flagged so the architect can decide whether it is wanted.

### Tests

- SQL battery, with each case also run un-armed and compared:
  - High-cardinality `GROUP BY k ORDER BY COUNT(*) DESC LIMIT n`.
  - ASC and DESC.
  - SUM/AVG/MIN/MAX/COUNT(DISTINCT) as the order key.
  - An order key over a nullable aggregate, for NULL placement.
  - An emitted grouping key as the order key.
- Edge cases:
  - k ≥ group count, so the partition is emitted whole.
  - k = 0.
  - Multi-key ORDER BY with heavy ties at the boundary; assert the ordering
    values, not the tied row identities.
  - Partitions large enough to take the radix path (> `kGBMergeLeaf`) and small
    ones taking the leaf path.
- Not armed (assert through telemetry, e.g. an `optimization_groupby_topk`
  counter): HAVING, ROLLUP, ORDER BY an expression of aggregates, and a filter
  between the aggregate and the sort.
- `make q` must pass.

## Findings outside this change (reported, not acted on)

1. **`ORDER BY … LIMIT … OFFSET` never becomes a HeapSort.**
   `operator_fusion.py` requires `not next_node.offset`, so Q39–42 run a full
   SortSink over every group. Fusing with k = limit + offset would let those
   queries use this path too.
2. **Q09/Q10 spend ~104 ms of wall in GroupBy finalize merge for 9,009 groups**
   (COUNT DISTINCT pair merge). This is unrelated to top-k.
3. Group identity is hash-only (see above).

## Recommendation

**GO.** The saving is large and reproducible on both formats: −24% and −28% on the
target set, with Q19 nearly halved. The design keeps the existing TopNSink as the
exact final step, so the GroupBy side only reduces input and correctness does not
depend on selection details beyond comparator parity.

v1 scope:

- The compiler arms `set_groupby_topk` for the no-HAVING, direct-aggregate-or-key
  ORDER BY shape.
- Native per-bucket selection uses draken's sort comparator, lanes are compacted
  before emit, and keys are gathered for winners only.

Expected result: at least the prototype's numbers, since the prototype's two known
leftovers (full-range lane emit, double-converting selection) would be gone.

## v1 as built (2026-09-26)

- **Native.** Top-k selection is in `GroupBySink::topk_select`
  (`src/cpp/engine/native_group_sinks.hpp`), called from `finish_and_emit` once
  per merged partition or radix bucket. It runs only when the sink is armed and
  the partition holds more than k groups.
  - **Ranking.**
    - Each ORDER BY aggregate column is emitted with `emit_lane_column`, then
      ranked with draken's `build_sort_keys` + `sort_perm` (partial sort).
    - This is the comparator the HeapSort uses, so NULL, NaN and -0.0 place
      exactly as they do there.
  - **Winners.**
    - Winners' lanes are moved into `[0, n)` with `gb_lanes_swap`, never
      recomputed. Every aggregate kind therefore emits bit-identically,
      including digests, bitmaps and lists.
    - Winners' keys are gathered once, only for them, through `keyref` on the
      radix path or `keycols` otherwise.
    - The unchanged emit loop then emits those n groups.
  - **Refactor.** The emit loop now reads its lanes through a shared
    `lane_view` helper.
- **Engine and binding.**
  - `Engine::set_groupby_topk(p, keys, k, ties)` arms a sink through a
    plan-time `pipeline → GroupBySink*` registry. It throws when `p` is not a
    GROUP BY sink, `k` is 0, or a key isn't an aggregate spec.
  - Python calls it through `NativePlan.set_groupby_topk`.
- **Compiler.** The HeapSort branch calls `_Compiler._arm_groupby_topk`.
  - **Arms only when** all of these hold:
    - The GROUP BY is rows-preserving: no HAVING, no ROLLUP.
    - Every ProjectionNode in between only passes through GROUP BY outputs or
      literals, so nothing is evaluated per row.
    - The ORDER BY leads with rankable aggregates, matched by identity, so
      `ORDER BY COUNT(*)` qualifies.
  - `ties` is set when the ORDER BY continues past those aggregates.
  - MEDIAN, APPROX_*, ARRAY_AGG and CIDR_AGG as ORDER BY keys leave it unarmed.
- **Telemetry.** A new `topk_pruned` counter (partitions or buckets cut)
  appears in `get_groupby_telemetry()`.
- **Tests.** `tests/integration/sql_battery/test_groupby_topk_fusion.py`, 23
  cases. Every case compares the armed plan against the same plan with arming
  disabled.
  - Covered: ASC and DESC; COUNT, SUM, AVG, MAX, string MIN, COUNT(DISTINCT)
    and SUM(DISTINCT); NULL aggregates both ways.
  - Also: tie-heavy multi-key ORDER BY, both merge paths (the radix path is
    asserted through `merge_bucketed`), and k larger than the group count.
  - Six shapes are asserted not to arm (`topk_pruned == 0`): HAVING, an ORDER BY
    expression, a group key as the leading key, a computed projection, a MEDIAN
    key, and no ORDER BY.
  - `make q` passes. The ClickBench and TPC-H golden batteries pass.

### Which ClickBench queries arm

| Arms | Doesn't arm, and why |
|---|---|
| 9–17, 19, 22, 23, 31–35 | **28, 29**: HAVING. **36**: see below. |

**Q36** isn't armed, correctly. The optimizer reduces
`GROUP BY ClientIP, ClientIP - 1, ClientIP - 2, ClientIP - 3` to `GROUP BY ClientIP`,
and the projection then *computes* `ClientIP - n` for every group. Pruning first would
evaluate that arithmetic on fewer rows and could hide an error the unfused plan
raises. The fix belongs to the optimizer: move a projection of functionally
dependent keys above the HeapSort. That's a separate ruling, worth about 4% on Q36.

### v1 A/B (ABBA, one fresh process per arm, 3 runs each, min of 6; DOP 16; same binary with arming disabled in-process for arm A)

| Q | parquet off → on (ms) | Δ | skene off → on (ms) | Δ |
|---|---|---|---|---|
| 13 | 219.9 → 196.8 | −10.5% | 174.3 → 153.4 | −12.0% |
| 14 | 293.6 → 268.8 | −8.4% | 239.3 → 217.8 | −9.0% |
| 15 | 248.1 → 222.3 | −10.4% | 193.4 → 169.0 | −12.6% |
| 16 | 213.4 → 198.7 | −6.9% | 207.6 → 190.6 | −8.2% |
| 17 | 499.4 → 409.1 | −18.1% | 450.3 → 354.6 | −21.3% |
| 19 | 1296.5 → 720.5 | **−44.4%** | 1207.7 → 623.7 | **−48.4%** |
| 31 | 265.5 → 258.5 | −2.6% | 171.1 → 164.3 | −4.0% |
| 32 | 267.7 → 251.3 | −6.1% | 192.7 → 166.3 | −13.7% |
| 33 | 1120.5 → 770.7 | **−31.2%** | 1086.9 → 731.9 | **−32.7%** |
| 34 | 928.1 → 705.9 | **−23.9%** | 806.6 → 582.4 | **−27.8%** |
| 35 | 925.2 → 702.1 | **−24.1%** | 805.9 → 581.1 | **−27.9%** |
| 9–12, 22, 23, 28, 29, 36 | within ±2% (noise) | | within ±1% | |
| **Σ, all 20 target queries** | 9548 → 8000 | **−16.2%** | 7868 → 6275 | **−20.2%** |

**Q29 recheck.** Parquet Q29 first read +3.4%, but it doesn't arm. A second ABBA
read +1.4%, inside arm A's own 3.6% spread (827–857 ms). It is noise.

**Selection cost.** Selection costs nothing measurable. The low-cardinality armed
queries (9–12, 22, 23) moved −1.8% to +0.7%, all inside their own run-to-run
spread. No armed query got slower outside noise.

## OFFSET fusion (2026-09-26, follow-on)

**Before.** `OperatorFusionStrategy` refused any LIMIT carrying an OFFSET, so
`ORDER BY … LIMIT l OFFSET o` ran a full SortSink over every input row. For a
GROUP BY below it, that meant every group.

**Now.** `Order → Limit(l, o)` becomes `HeapSort(l + o) → Limit(l, o)`.
- The HeapSort replaces the Order, and the Limit stays above it to skip the offset.
- HeapSortStep and TopNSink are unchanged (no offset field).
- The LIMIT's native offset runs on the HeapSort's ordered, DOP-1 output
  pipeline.
- OFFSET with no LIMIT is not fused, since nothing bounds the rows to keep.
- LIMIT with no OFFSET behaves exactly as before: the Limit is absorbed into the
  HeapSort.
- Telemetry: `optimization_fuse_operators_heap_sort_offset`.

**What else picks up k = l + o.** Everything that reads a HeapSort's limit:
- the GROUP BY top-k fusion (Q40 now arms with k = 1,010);
- the scan top-N pushdown;
- manifest top-N pruning.

All three stay correct, because the rows the offset skips are inside the kept l + o.

**Passes checked for interaction.**
- `LimitEliminationStrategy` skips limits that carry an offset.
- `LimitPushdownStrategy` ignores them.
- `_limit_stats` applies the offset on the Limit node.

**Correctness.** Measured on `hits_rugo_262k`, comparing Q39–43 fused against unfused:
- The ordering values are identical.
- Q39–42 differ only among groups tied at their boundary values (PageViews 2, 15,
  24–27 and 1). Every returned row, from both plans, is a real group of the full
  result.
- **Tests.** `tests/integration/sql_battery/test_heapsort_offset_fusion.py`, 11
  cases, comparing the fused plan against the unfused one.
  - GROUP BY and plain streams, both directions, multi-key ORDER BY.
  - Offset past the end, and an offset reaching into the tail.
  - Scan top-N with an offset on parquet.
  - Ties.
  - Offset-only (not fused), and no-offset (unchanged).

**A/B.** ABBA, one fresh process per arm, 7 runs each, min of 14; baseline arm has
the offset fusion disabled in-process.

| Q | parquet off → on (ms) | Δ | skene off → on (ms) | Δ |
|---|---|---|---|---|
| 39 | 38.9 → 38.3 | −1.5% | 11.3 → 11.4 | +0.9% |
| 40 | 84.2 → 59.8 | **−29.0%** | 59.6 → 36.0 | **−39.6%** |
| 41 | 36.2 → 37.5 | +3.6% | 10.3 → 10.5 | +1.9% |
| 42 | 38.8 → 38.9 | +0.3% | 8.5 → 8.7 | +2.4% |
| 43 | 36.1 → 35.4 | −1.9% | 8.2 → 8.1 | −1.2% |

**Q41 is a real cost, open for a ruling.** It has 41k groups, about 640 per
partition, and k = 110.
- GroupBy finalize went from 1.3 ms to 2.5 ms, consistently, on both formats.
  Finalize runs on one thread here (under 65,536 entries), so the fixed cost of
  selecting in 64 small partitions (~17 µs each) exceeds the saved emit of cheap
  integer keys.
- The net is about +1 ms on a 36 ms query.
- The options are a proven size threshold for selection, or trimming the
  per-partition setup. Neither was added: a gate needs the architect's say and
  its own measurement.

A first 3-run pass showed ±40–110% swings on these 10–90 ms queries. At 7 runs
those collapsed to the figures above. 3 runs is too few for queries this short.
