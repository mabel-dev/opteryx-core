# Groupjoin — design (2026-10-15)

Status: **design only. Nothing built, nothing benchmarked.** For the architect to rule on (§12).

Source: `scratch/papers/REVIEW.md` §1 item 3. Papers read in full:
`scratch/papers/txt/moerkotte_neumann_2011_groupjoin_vldb.txt` (M&N) and
`scratch/papers/txt/fent_neumann_2021_groupjoin_nested_aggregates_vldb.txt` (Fent).

**Line numbers.** Every cited file is byte-identical between this branch's `HEAD` and the main
checkout's working tree (2026-10-15), with one exception: `src/cpp/engine/native_group_sinks.hpp`.
The main checkout holds uncommitted GROUP BY redesign work, so past about line 3050 its numbers
differ. Those citations give both forms: "main :X (HEAD :Y)".

**Plans.** Plan shapes marked "EXPLAIN" were read with `EXPLAIN` (planning only, no timing) on
`testdata/tpch_001` (SF0.01) with the main-checkout build. That shows the plan's *shape*. The row
counts in those plans are SF0.01 estimates, not SF10 facts.

---

## 0. Verdict

1. Opteryx has **no groupjoin and no eager aggregation for organic join+GROUP BY**. Scalar
   decorrelation already produces the *eager-right* shape (`GROUP BY` on the subquery side, then
   join, `decorrelate_subquery.py:6-25`), and the semi-join reducer turns it into Fent's
   *separate* shape (`semi_join_reducer.py:6-48`).
2. The engine already has almost all of the operator. The RIGHT SEMI/ANTI exchange is a
   three-pipeline shape: build the small leg, stream the large one into a sink that **mutates
   per-build-row state**, then emit the build rows from a source
   (`native_join2.hpp:2638-2667`, `Join2MarkSink` `:2750-2882`, `SemiAntiBuildSource`
   `:2680-2748`). A groupjoin is that same shape with **aggregate lanes in place of the
   `matched[]` byte**. The build sink, CSR, radix table and probe lookups are reused unchanged.
3. The design here is better than REVIEW's "small GROUP BY on top, dropped when uniqueness is
   proven". With an equi-join and no residual, **every build row with the same key sees exactly
   the same match set**. So accumulators are kept **per distinct build key**, not per build row,
   and one probe match costs one update whatever the build-side duplication. When the GROUP BY key
   *is* the join key, the per-key partial is multiplied by the key's build multiplicity `d` at emit
   (Yan–Larson `⊗c`, M&N §2.2). That gives the exact result with **no GROUP BY on top and no
   uniqueness proof**. That matters, because Opteryx has no uniqueness proof for base tables
   (`logical_planner.py:4919-4922` refuses PRIMARY KEY; `join_elimination.py:106-137` trusts only
   a `Distinct`).
4. The cost decision (eager / memoizing / none) needs Fent's `σ_S`. The inputs exist and are fed:
   `match_fraction` (`join_estimator.hpp:189-213`) reads `KeyStats.live_ndv`, which the native
   statistics refresh sets from each side's live distinct count
   (`statistics_refresh.hpp:1152-1157`). The Python `plan_adapter.py` builder that leaves
   `live_ndv` unset only feeds the DPccp join-order enumerator, which uses inner estimates.
   *(Corrected 2026-10-15: an earlier version of this doc claimed `live_ndv` was never fed.)*
5. Expected value, honestly: **Q13 is the real target** (organic LEFT groupjoin; today about 15M
   pair rows are gathered and then GROUP BY'd at SF10). Q20 has a moderate case only via the
   larger decorrelation rewrite (D-2, §5.3). Q17 is small. Q18 is likely neutral (its build side is
   tiny in reality). All of these are hypotheses until §10 is run.

---

## 1. What the papers say (and what transfers)

**M&N 2011.** Groupjoin `e1 Z e2`: each `e1` tuple is extended by aggregates over its `e2` join
partners (M&N §2.5). Two equivalences:
- **Eq. 11** `Γ_G;F(e1 ⟕ e2) ≡ Π(e1 Z e2)` and
- **Eq. 12** `Γ_G;F(e1 ⋈ e2) ≡ Π(σ_{c>0}(e1 Z_{F∘c:count(*)} e2))`.

Both hold under functional dependencies that amount to "no two `e1` tuples share a group, and the
group determines the join key". In practice that means the join key is a key of `e1` and
`G ⊆ A(e1)` (M&N §3.5). Eq. 11 additionally needs `F(∅) = F({⊥})`, which **fails for
`COUNT(*)`**: over a LEFT JOIN, an unmatched row is one NULL-padded tuple, so `COUNT(*)` must be 1,
not 0 (M&N §3.2). M&N measured single-threaded (System T, SF1): Q13 278→84 ms, Q3 104→70, Q21
500→127, total 1932→1295 ms (M&N Fig. 5). They claimed it is "always beneficial".

**Fent 2021.** That claim is wrong under parallel execution. Three strategies (Fent §3):
- **eager right**: aggregate all of S, then probe with R. Cost `C_eager = |S| + |R⋉S|`.
- **memoizing**: build R with aggregate space and accumulate during the probe. Thread-local
  aggregation for heavy hitters uses a CAS ownership tag, and the probe result pointer is memoized
  so the merge needs no second lookup. Cost `C_memo = 2|R| + 3|S⋉R|`.
- **separate**: join, then GROUP BY. Cost `C_sep = |R| + 3.3|S⋉R| + |S⋉R|`.

The deciding input is `σ_S`, the fraction of S rows with a partner in R. Measured (Fent Table 3,
§5.1):
- **eager wins Q13 (σ_S = 100%) by over 2×** and is **over 10× slower on Q17 (σ_S = 0.1%)**;
- memoizing and separate are close everywhere;
- the cost model's choice is within 1.7% MAPE of the best (Fent Fig. 12).

Fent also defines the **static groupjoin** used for decorrelation. There, `COUNT(*)` over an empty
partner set is **0** (the COUNT-bug semantics), the opposite of M&N's LEFT-join `COUNT(*) = 1`
(Fent Table 1). §3 keeps the two apart.

**What does not transfer as-is.** Fent's constants (the 2×, 3×, 3.3× factors and `c = 30%`) were
calibrated on Umbra (compiled, cheap dynamic allocation). In our design, accumulation is an array
add at an index the probe already produced, and the merge is per-key, not per-match (§6.4). So the
cost formulas in §5.4 are Opteryx's own, with constants to calibrate by measurement (§10.3).

---

## 2. What exists today (code facts)

### 2.1 Join (`src/cpp/engine/native_join2.hpp`)

- Build: `Join2BuildSink` (`:919`) retains payload column views and records per build row a
  `(morsel, row)` address plus the 64-bit key hash (`:1031-1145`). Build row ids are **dense
  `uint32` in `[0, total_rows)`** (`Join2BuildGlobal::row_m/row_r` `:343-345`, `total_rows`
  `:387`).
- NULL keys: NULL-keyed build rows are dropped from the keyed space, except under
  `track_matches`, which retains them outside it (`:1097-1120`). `finalize` appends them after
  the sealed keyed space (`:1338-1353`).
- Tables:
  - `JoinCsr` (`:239-271`): bucket offsets plus separate `rows` and `hashes` arrays.
    `append_probe_matches` scans one bucket comparing the stored hash (`:248-259`).
  - `RadixJoinTable` (`:302-335`): taken when `total_rows >= kRadixMinBuildRows` (2^20,
    `:274`), with buckets hash-sorted so **every key's entries are one contiguous run**.
    `find` returns `[start, start+count)` (`:318-334`).
  - The choice is fixed once in `finalize` (`:1322-1336`), and probes route per morsel, never per
    row (`DeferredJoin2Probe::active` `:2985-2987`; `Join2MarkSink` `:2868-2873`).
- Equality is 64-bit hash identity (`:221-223`, `:292-294`). **RULED accepted; not revisited here.**
- Match marking: `matched[]` is `std::atomic<uint8_t>` per keyed build row with relaxed idempotent
  stores (`:411`). It is allocated in `finalize` when `track_matches` (`:1338-1353`) and written by
  both probe forms (`:1659-1663`, `:2428-2432`).
- RightOuter (`JoinMode` `:59-83`) is "LEFT OUTER with legs exchanged" and was motivated by
  exactly TPC-H SF10 `customer LEFT JOIN orders`. It probes like INNER, marks build rows, and the
  `UnmatchedBuildSource` tail (`:2517`) emits the unmarked ones.
- RIGHT SEMI/ANTI (`:2638-2667`): build → `Join2MarkSink` (emits nothing; workers write global
  atomics; `combine`/`finalize` empty `:2879-2880`) → `SemiAntiBuildSource` (64K-row chunks,
  `kChunk` `:2683`, gathers the build half with `gather_rows`).
- Finalize width is the query's `exec_dop`, never re-derived from hardware (`join_finalize_width`
  `:472-476`). The "queue, don't insert" rule for per-worker state is at `:495-530`.

### 2.2 GROUP BY sink (`src/cpp/engine/native_group_sinks.hpp`)

- Aggregate vocabulary: `AggFn` (`:330-385`), `AggSpec2` (`:391-420`), per-(fn, operand type)
  collector kind `GBKind` (`:1066-1099`) chosen by `gb_kind_of` (`:1347-1384`).
- Columnar lanes `GBLanes` (`:2791-2812`), sized per kind by `gb_lanes_resize` (`:2828-2895`).
  The flat-pointer view is `GBFlatLanes` (`:2929-2945`).
- The row-kind update arms (COUNT(col), SUM int/float, AVG, MIN/MAX numeric) are
  `gb_apply_row_kind_rd` (`:2964-3040`), dispatched once per column pass. The row's group comes
  from a caller-supplied resolver `at(i)`. SUM overflow fails loud (`:2987-2994`).
- Emit: `emit_lane_column` (`:1713`) over a `GBLaneView` (`:1428-1446`).
- Merge folds the same arms over pairs of lanes (`combine_into`, main :4854 (HEAD :5167); SUM
  overflow at merge, main :4919-4924 (HEAD :5232-5237)).
- Partial-state ingestion already exists: the manifest statistics seed (`GroupSeed`
  `:2219-2225`, `agg2_seed_cell` `:2227`).
- The in-flight redesign is `docs/GROUPBY_REDESIGN_2026_10_12.md`: R1 segmented store and R3
  flush-clear built and uncommitted (§9); R4 estimate ladder and R9 ticket table ruled in and
  sequenced later (§8).

### 2.3 Planner

- Strategy order: `opteryx/planner/optimizer/__init__.py:239-378`. Decorrelation is first
  (`:243`), `JoinAlgorithmStrategy` (build side and exchanges) is at `:316`. Ordering contracts
  are `provides`/`requires` tokens checked by `_validate_strategy_order` (`:184-220`).
  `JoinAlgorithmStrategy` requires `joins-planned` and provides nothing (`join_algorithm.py:478`).
- Scalar decorrelation (`decorrelate_subquery.py:6-25`):
  `INNER JOIN (SELECT k, AGG(x) … GROUP BY k)` when the predicate rejects the empty-set value
  (`_unmatched_row_is_dropped` `:1503`). Otherwise `LEFT OUTER` plus a `CASE` on the inner key
  (`_empty_value_case` `:1553`). The empty-set value is computed through the subquery's
  expression and HAVING (`_empty_set_value` `:1457`). Grouping is added by `_expose_key` (`:953`).
- Reducers:
  - decorrelation's own `_reduce_aggregate_input` (`:932`, called `:3477`) grafts a `left semi`
    against a **copy of the outer leg** (`_graft_key_reducer` `:821`), only when that leg is
    structurally restricted (`_is_restricted` `:788`);
  - `SemiJoinReducerStrategy` (`semi_join_reducer.py:230`) does the same after pushdown, for INNER
    or LEFT OUTER over a grouped aggregate keyed on the join key (`:74`), with cost ratio 0.5
    (`:79`). Its docstring gives the motivation: Q17 "2,000,000 groups to serve 2,044", Q20
    "5,441,669 groups to serve 58,782".
- LEFT OUTER exchange: `join_algorithm.py:541-618` (ratio `_LEFT_OUTER_SWAP_RATIO = 4.0` `:396`)
  sets `swap_build_side`, which the compiler lowers to `JoinMode::RightOuter` (mode 8) with the
  build-preserved tail (`compiler.py:5564-5594`). The swapped semi/anti compile is
  `compiler.py:5596-5686`.
- Uniqueness: the only proof trusted today is a `Distinct` reached through row-preserving nodes
  (`join_elimination.py:106-137`). PRIMARY KEY / UNIQUE declarations are refused because "Opteryx
  enforces no constraints" (`logical_planner.py:4919-4922`).
- Estimator: `match_fraction` (`join_estimator.hpp:189-213`) computes
  `live_ndv(right) / max(ndv(left), ndv(right))` per key. It returns false when
  `right.has_live_ndv` is unset. The native statistics refresh, which produces the estimates
  EXPLAIN shows, sets both from each side's domain and live distinct counts
  (`statistics_refresh.hpp:1125-1159`). `plan_adapter.py:155-179` builds `KeyStats` without
  `live_ndv`, but only for the DPccp enumerator, which reads inner estimates only.

### 2.4 Today's plans (EXPLAIN, SF0.01)

| Q | shape today | groupjoin candidate |
|---|---|---|
| 13 | `GROUP BY c_count ← GROUP BY c_custkey [COUNT(o_orderkey)] ← left_outer(customer, orders σ)`, **exchanged** ("build leg 9.00x clears the 4x margin") → RightOuter | organic **LEFT** groupjoin, G = join key |
| 17 | `Filter ← (part ⋈ lineitem) ⋈ Π(GROUP BY l_partkey [AVG] ← lineitem ⋉ part-copy)`; "semi join reducer applied" | the `GROUP BY ← ⋉` subtree is a **semi** groupjoin (D-1) |
| 18 | `HeapSort ← GROUP BY (c_name,c_custkey,o_orderkey,o_orderdate,o_totalprice) [SUM(l_quantity)] ← (customer ⋈ (orders ⋉ GROUP BY l_orderkey HAVING)) ⋈ lineitem`. **No reducer chain**: the IN subquery is uncorrelated | outer organic **INNER** groupjoin, G ⊆ build side, G ≠ join key |
| 20 | `… ⋉ Π(nested-loop(partsupp ⋉ part, Π(GROUP BY (l_partkey,l_suppkey) [SUM] ← lineitem ⋉ (partsupp-copy ⋉ part-copy))))`; "decorrelate aggregate reduced" | semi groupjoin (D-1), or the join-back removed (D-2) |

"Nested loop" in Q20 is the engine's equi-join with a residual (`compiler.py:5149-5150`), not a
cartesian product. REVIEW §1.3 lists Q18 with the reducer chain; the EXPLAIN shows otherwise.
Q18's groupjoin is the outer organic one, which is also the one Fent uses (Fent §5.1).

---

## 3. Semantics

Notation: R is the **build** relation (holds the accumulators), S is the **streamed** relation,
`k` is the equi-join key (possibly composite), and `F` are the aggregates, with operands from S
(see P5 for R-side operands).

### 3.1 The core identity

For an equi-join with **no residual**, two R rows with the same key `k` have exactly the same S
match set `M(k)` (hash identity, ruled). Therefore, for every aggregate `f`:

> per-build-row partial `f(M(r.k))` = per-key partial `f(M(k))`.

So accumulators live **per distinct build key**, and a probe match updates exactly one slot. Three
emit modes cover every qualifying shape:

| mode | emits | used when | top GROUP BY |
|---|---|---|---|
| **E-ROW** | one row per build row, carrying its key's partial `F¹` | organic, `G ⊆ A(R)`, G ≠ join key | **yes**, `Γ_G;F²` (re-aggregate partials: M&N Eq. 3, grouping by TID(R) is finer than G) |
| **E-KEY⊗d** | one row per distinct build key, partial multiplied by the key's build multiplicity `d` (M&N's `F⊗c`) | organic, G ≡ the R-side join-key columns | **no** |
| **E-KEY** | one row per distinct build key, partial as is | semi groupjoin and decorrelation (static: one value per key, R multiplicity irrelevant) | **no** |

Why E-KEY⊗d is exact: `Γ_k;F(R ⋈ S)` puts in group `k` exactly `d` copies of `M(k)`.
Duplicate-sensitive aggregates scale by `d` and duplicate-agnostic ones are unchanged (M&N §2.2:
`sum→sum·d`, `count(*)→count·d`, `count(a)→countNN·d`, `min/max` unchanged, `avg` unchanged as
`(sum·d)/(cnt·d)`). This needs no uniqueness of R, which Opteryx cannot prove (§2.3).

### 3.2 Join forms

| form | source | emitted keys | empty-partner value |
|---|---|---|---|
| **GJ-INNER** | `Γ(R ⋈ S)` | keys with ≥1 match (M&N Eq. 12's `σ_{c>0}`) | — |
| **GJ-SEMI** | `Γ_k;F(S ⋉ R)`: the reducer subtree | keys with ≥1 match | — |
| **GJ-LEFT** | `Γ(R ⟕ S)`, R preserved: the RightOuter case | every R key, plus the NULL-key group | **organic**: `COUNT(*)` = `d` (each preserved row is one padded tuple: M&N §3.2), `COUNT(s.c)` = 0, every other aggregate NULL |
| **GJ-STATIC** | decorrelation D-2 (§5.3), R = outer leg | INNER or LEFT exactly as decorrelation chose (`_unmatched_row_is_dropped`) | **static**: `COUNT(*)` = 0, `COUNT(c)` = 0, others NULL (Fent Table 1). Must equal `_empty_set_value` (`decorrelate_subquery.py:1457`) |

GJ-LEFT's `COUNT(*) = d` and GJ-STATIC's `COUNT(*) = 0` are **different answers to the same
operator state** (a key with no matches). The form is a compile-time constant on the operator, and
§9 tests both.

### 3.3 NULL keys

| where | GJ-INNER / GJ-SEMI | GJ-LEFT (R preserved) | GJ-STATIC |
|---|---|---|---|
| S row, any key column NULL | never matches, contributes nothing (same rule as the probe, `native_join2.hpp:1638-1647`) | same | same |
| R row, any key column NULL | never matches, not emitted | **emitted**. All NULL-keyed R rows form **one** group (GROUP BY treats NULL as one key): `COUNT(*)` = their count, `COUNT(c)` = 0, others NULL. Needs the `track_matches` NULL-row retention (`:1097-1120`, `:1338-1353`) | E-ROW per outer row: the subquery value for a NULL correlation key is the empty-set value (`s.k = NULL` is never true) |
| `IS NOT DISTINCT FROM` keys (`null_equal`) | **not eligible** (P7) | not eligible | not eligible |

---

## 4. Qualifying shapes: exact preconditions

A plan qualifies only if **all** of P1–P9 hold. Anything else stays on today's plan. That is a
planner eligibility decision, never a runtime fallback (§6.7).

- **P1 Join.** One equi-join of type INNER, LEFT OUTER (R = preserved leg) or LEFT SEMI (R = the
  semi's build leg, S = its preserved leg). Excluded: FULL, ANTI/NOT IN, ASOF, BAND,
  nested-loop/residual joins, CROSS, and any join with a residual `on` conjunct. The match-set
  identity in §3.1 needs a pure key equality.
- **P2 Aggregate directly above.** A `AggregateAndGroup` (or ungrouped `Aggregate`, which is
  E-KEY over one group, and in practice not useful) consumes the join output through
  row-preserving nodes only (`Project`, `Subquery`, the `_UNIQUENESS_PRESERVING` set,
  `join_elimination.py:76-84`, minus `Limit`/`Order`). A `Filter` between them disqualifies the
  shape (it can drop rows after the join).
- **P3 Group key side.** Every group key resolves to an R column. For INNER and SEMI, an S-side
  *join-key* column may be substituted by its R-side equal (`l_orderkey → o_orderkey` in Q3). For
  LEFT this substitution is **forbidden**: unmatched rows carry S-key NULL, not R-key. Any other
  S column in G disqualifies the shape (M&N's `J2 → G2+`).
- **P4 Emit mode.** G ≡ the full R-side join-key column set gives E-KEY⊗d. G a strict superset,
  subset or other R columns gives E-ROW plus `Γ_G;F²`. GJ-SEMI always gives E-KEY.
- **P5 Aggregates.** v1 admits `COUNT(*)`, `COUNT(c)`, `SUM`, `AVG`, `MIN`, `MAX`, `ANY_VALUE`
  over S operands. Their `GBKind`s are `Rows`, `Valid`, `SumI/SumF/SumD128`, `AvgI/AvgF/AvgD128`
  and `MinMaxNum/MinMaxD128/MinMaxStr` (`native_group_sinks.hpp:1066-1099`). Decomposition, i.e.
  the F¹ partials and F² re-aggregation for E-ROW, is M&N Fig. 1. AVG travels as `(sum, count)`
  and is divided after F².
  - **Excluded** in v1: DISTINCT operands (not decomposable across build rows), MEDIAN,
    APPROX_*, ARRAY_AGG, CIDR_AGG (per-slot object lanes: memory scales with |R|), and
    STDDEV/VAR/CORR (decomposable via Σx, Σx², n; candidate for v2).
  - Aggregates whose operand is an **R** column are excluded in v1. They are computable at emit
    from the pair count via `⊗c` (M&N §2.2) (later scope).
- **P6 Grouping sets.** ROLLUP/CUBE/GROUPING SETS are not eligible.
- **P7 Key rule.** Plain equality keys only (`null_equal = false`). Join-key coercion casts
  (`compiler.py` `_join_key_coercions`) are allowed: they are just key columns.
- **P8 Decorrelation specifics (D-1/D-2).** The subquery has no HAVING (it changes the
  empty-group answer: a static groupjoin would have to emit NULL rather than drop) and no
  `ORDER BY … LIMIT 1` rewrite. The correlation is equi-only (already required,
  `decorrelate_subquery.py` header).
- **P9 Estimates exist.** `|R|`, `|S|`, `σ_S` and `σ_R` resolve (§5.4). Missing statistics mean a
  **decline with a recorded reason**, never a fabricated number. The same posture as
  `est_output_rows = -1` (`native_join2.hpp:967-972`).

---

## 5. Planner

### 5.1 New logical node

`GroupJoin` (new `LogicalPlanStepType`) carries:
- R and S legs;
- key pairs;
- form (INNER / SEMI / LEFT / STATIC_INNER / STATIC_LEFT);
- emit mode (E-ROW / E-KEY⊗d / E-KEY);
- the aggregate list (F¹ for E-ROW, F for E-KEY modes);
- R payload columns (G's R columns, plus for D-2 the outer columns the plan above reads).

For E-ROW an ordinary `AggregateAndGroup(G; F²)` node sits above it. It is planner-visible, gets a
`group_count_estimate` (`execution_estimates.py:76`) and is subject to everything the GROUP BY
programme does. EXPLAIN shows "Group Join (Hashed)" with form and mode in `details`.

### 5.2 Rule placement: `GroupJoinStrategy`

Position: **immediately after `JoinAlgorithmStrategy`** (`optimizer/__init__.py:316`). Reasons:
- It needs the final join order and build side. Groupjoin pins R as build, so it must see whether
  JoinAlgorithm chose R (Q13: it did, by exchange; Q18: it did) and cost the alternative when it
  did not.
- It needs predicates pushed and projections pruned. That gives the right `|R|`/`|S|`, and the
  reducers (decorrelation `:3477` and `SemiJoinReducerStrategy`, `__init__.py:306`) have already
  produced the GJ-SEMI shape.
- It must come before `RedundantOperationsStrategy`/`OperatorFusionStrategy`/
  `AggregateScanPushdownStrategy`, which assume nodes it removes.

Ordering contract: `JoinAlgorithmStrategy` gains `provides = ("join-algorithm-chosen",)`, and
`GroupJoinStrategy` declares `requires = ("join-algorithm-chosen", "projection-pushed",
"predicates-pushed")` so `_validate_strategy_order` (`__init__.py:184-220`) pins it.

One rule covers GJ-INNER, GJ-LEFT and GJ-SEMI: they are all "Aggregate over Join". D-2 (§5.3) is
a different rewrite and lives in decorrelation.

### 5.3 Decorrelation forms

- **D-1 (semi groupjoin; drop-in).** The reducer chain's subtree `Γ_k;F(S ⋉_k R')` (R' = the
  reducer's copy of the outer leg) becomes `GroupJoin(SEMI, E-KEY)`. Nothing above changes: the
  join-back, the LEFT/CASE empty handling (`_empty_value_case`) and the filter stay. Duplicate keys
  in R' are harmless because E-KEY emits one row per distinct key. This replaces "semi join +
  GROUP BY" with one operator over **the same build table the semi join already builds**. It saves
  the semi's emit of matched S rows and the GROUP BY's hash insert of them. It does **not** remove
  the reducer copy's scan or the join-back. Applies to Q17 and Q20.
- **D-2 (outer leg as build; join-back removed).** Instead of reducer copy + GROUP BY + join-back,
  the outer leg itself is R. GJ-STATIC, E-ROW (one value per outer row, no top GROUP BY: a
  correlated scalar is defined per outer row). The predicate above reads the emitted value
  directly. Thanks to §3.1, duplicate outer keys cost nothing extra on the probe side; they only
  enlarge R.
  - Saves: the reducer copy (Q20: a second `partsupp ⋉ part` scan and semi join), the GROUP BY and
    the join-back.
  - Costs: R = the full outer leg rather than a key set. Q17's outer leg is `part ⋈ lineitem`
    (~600K rows at SF10 for ~20K keys), so D-2 builds about 30× the rows D-1 does. Q20's outer leg
    is `partsupp ⋉ part`, whose keys are distinct in the data.
  - Placement: inside `DecorrelateSubqueryStrategy`'s scalar path is wrong. At position 1 the
    outer leg is an unrestricted cross-join chain (`decorrelate_subquery.py:788-808`;
    `semi_join_reducer.py:24-33`). So D-2 would be a rewrite in `GroupJoinStrategy` that
    recognises `Filter(pred(value)) ← Join(outer, Π Γ_k(S ⋉ copy(outer)))` and folds it. That
    needs a structural test that the reducer copy is a copy of *that* outer leg (the copy is made
    by `copy_sub_plan` + `rename_relations`, `decorrelate_subquery.py:865-872`, so node identity is lost). **This is the
    riskiest piece of the design** (ruled: try it, §12).

### 5.4 Cost decision: eager vs memoizing vs none

Inputs per candidate:
- `|R|`, `|S|` (row estimates at the legs: the same `_side_statistics` JoinAlgorithm uses,
  `join_algorithm.py:835`);
- `K_R` (distinct R keys = R key live NDV);
- `σ_S = |S⋉R|/|S|` and `σ_R = |R⋉S|/|R|` via `match_fraction` (`join_estimator.hpp:189-213`)
  with `left = S, right = R` and the reverse;
- `W` = `exec_dop`;
- `B` = bytes per accumulator slot (sum of the kinds' lane widths, `gb_lanes_resize`).

Opteryx-specific costs (units are row-operations; `c_*` constants are **unknown and must be
calibrated**, §10.3):

```
C_none  = build(B_side) + probe(P_side) + M·(c_emit + c_gb) + G_out·c_merge      (today)
C_memo  = build(R) + |S|·c_probe + |S⋉R|·c_acc + W·K_R·c_merge + emit_rows·c_emit
C_eager = |S|·c_gb + G_S·c_merge + build(min) + probe + |R|·c_emit                (Γ on S, then join)
```

Here `M` is the join's pair count, `G_out` the GROUP BY's output groups, `G_S` S's distinct keys,
and `emit_rows` = `|R|` (E-ROW) or `K_R` (E-KEY*).

Structure of the decision (it mirrors Fent §3.4, which found eager-or-not to be "the most impactful
decision"):
1. Eager only competes when `σ_S` is high. Otherwise it aggregates rows the join would discard:
   Fent's 10× loss on Q17. Fent's memo pays for the probe of every S row plus thread-local tables,
   but **our memo pays neither the per-match merge nor a group-table insert**: the accumulator
   index is the build entry the probe already found. So eager's region should be smaller here than
   in Umbra. Whether it is empty is a measurement (§10.3), and eager is built only if that sweep
   shows a region where it wins.
2. Memo vs none: memo removes `M·(c_emit + c_gb)` and adds `|S⋉R|·c_acc + W·K_R·c_merge`. It
   loses when R must become the build although JoinAlgorithm chose S (R much larger than S) and
   `M` is small.
3. A tie or an unresolvable input means none, recorded with the reason (`record_decision`,
   `optimization_strategy.py:214`), in the same three-way decline style as the exchange decisions
   (`join_algorithm.py:600-618`).

**Inputs.** `σ_S`/`σ_R` come from the same `KeyStats` the native statistics refresh already
builds per equi-key, with `live_ndv` set (`statistics_refresh.hpp:1152-1157`); no planner change
is needed to obtain them.

---

## 6. Operator design

### 6.1 Pipelines (mirrors the RIGHT SEMI exchange, `compiler.py:5596-5686`)

```
build   R leg -> Join2BuildSink(track_matches = (form needs NULL-keyed R rows))   [unchanged]
stream  S leg -> GroupJoinAccumSink                                        (emits nothing)
emit           GroupJoinSource(form, emit mode)  -> [AggregateAndGroup F² if E-ROW] -> …
```

Pipelines run in creation order, so the build is sealed before accumulation starts and
accumulation is merged before emit. This is the mechanism the RIGHT SEMI path already relies on
(`compiler.py:5600-5603`). The build sink, `finalize`'s CSR/radix choice, consolidation and the
runtime min/max bound are untouched. The bound is **sound** on the streamed leg for every form: a
streamed row outside the build key range can update nothing, which is the same argument as
`compiler.py:5660-5668`. `est_output_rows` stays -1 because consolidation is a fan-out trade and
the groupjoin has no fan-out.

### 6.2 Accumulator slot = the key's first table entry

The probe already locates the key's entries, so the slot needs no new lookup:
- **CSR**: slot = the first `i ∈ [off[b], off[b+1])` with `hashes[i] == h`. This is the same scan
  `append_probe_matches` does (`native_join2.hpp:248-259`), stopping at the first hit.
- **Radix**: slot = the run `start` from `find` (`:318-334`). The per-morsel lookup pass already
  computes it (`RadixMorselRuns::start` `:2360-2386`).

Slots live in entry space `[0, total_rows)`. That is sized by build rows, not keys, so no dense
key-id remap pass is needed, at the cost of lanes sized `|R|` rather than `K_R`. Both table
types give the slot from work the probe does anyway. **No new table, no second hash lookup.** This
is Fent's "memoized payload pointer" for free, because the CSR entry index *is* the pointer.

Per probe morsel: hash the keys (`compute_row_hashes`), then for each live row find the slot
(or none). Then each aggregate runs one column pass:

```
for k in pairs: lane[slot[k]] ⊕= operand[probe_row[k]]
```

`probe_row[k]` is read uniformly through `data[selection[i]]` (§11 contract; the existing arms
already do `RD::at(vdata, vsel[i])`, `native_group_sinks.hpp:2987`). The kernel is
`gb_apply_row_kind_rd` with the row index split into "operand row" and "slot" (§7.1). There is no
per-row dispatch on table type: the table is chosen per morsel, as in `Join2MarkSink`
(`:2868-2873`).

### 6.3 Parallel accumulation

| option | how | for | against |
|---|---|---|---|
| **A. shared lanes + atomics** | one lane set, relaxed `fetch_add` / CAS loops | `W×` less memory, no merge | heavy-hitter contention, which is Fent's motivating problem (§3); no atomic `__int128` or double add (CAS loops); MIN/MAX need CAS; the house record is that removing atomics is what made the radix build pay (`native_join2.hpp:281-285`; REVIEW §2) |
| **B. per-worker dense lanes + merge** (recommended v1) | each worker owns full lanes over `[0, total_rows)`; `combine` queues them O(1) under the mutex (house rule `:495-530`); `finalize` sums lanes per slot range in parallel at `exec_dop` width | no atomics, no contention, the update loop is the GROUP BY arm; the merge is a sequential, prefetch-free sweep | memory `W·|R|·B`, e.g. Q13 SF10: 1.5M × 16 B = 24 MB per worker → 96 MB at W=4 (Cloud Run), 384 MB at W=16 (Mac). The merge sweep is `W·|R|` regardless of how many slots were touched |
| **C. Fent memoizing** | shared lanes with an atomic owner-tag CAS on first touch; non-owners aggregate into a thread-local table keyed by slot id; merge follows the slot id | memory ≈ shared + heavy hitters only | a CAS per first touch; a thread-local hash table, which is a second structure to build and test |

**Recommendation:** B for v1. Its worst-case memory `W·|R|·B` is part of the plan-time cost
comparison against the separate plan (§12). Allocate lanes lazily in fixed pages on first touch,
so a selective probe (D-1, small `σ_R`) touches few pages and the merge skips untouched pages via a
per-page touched bit. C is the scale-out answer if B's memory or merge sweep measures badly at high
W. It is a v2 mode chosen at plan time, never switched at runtime.

Float SUM order across workers is nondeterministic, exactly as in GROUP BY today. SUM overflow is
checked in the merge as `combine_into` does (main :4919-4924 (HEAD :5232-5237)), and in the `⊗d`
multiply at emit (§6.4).

### 6.4 Emit (`GroupJoinSource`)

Chunks of 64K entries, claimed atomically (`SemiAntiBuildSource` pattern, `:2694-2746`):
- **E-KEY / E-KEY⊗d**: walk entries in table order. For each **representative** entry (radix:
  each run start; CSR: within the bucket, the first entry of its hash), with `d` = run length or
  the bucket count of that hash (`row_count_for` `:261-268`):
  - GJ-INNER / GJ-SEMI emit iff the pair count > 0. The pair-count lane is always allocated; it is
    the `Rows` lane `COUNT(*)` needs anyway.
  - GJ-LEFT emits every representative, then **one** NULL group from the `track_matches` tail rows
    (`tail_null_rows`, `:1338-1353`).
  - Key columns are gathered from the representative build row (`gather_rows`, as
    `SemiAntiBuildSource` does). Lane values are gathered into chunk-local arrays, `⊗d` is applied
    (int and DECIMAL multiplies overflow-checked, fail loud with the SUM message), and the column
    is emitted with `emit_lane_column` (`native_group_sinks.hpp:1713`).
- **E-ROW**: walk build rows. Each row's slot is its key's representative entry, so a
  build-row→slot map is needed. Computing it at `finalize` is one pass over entries (radix: run
  start per entry; CSR: per-bucket first-of-hash). It costs `4·|R|` bytes, only when E-ROW.
  Payload = G's R columns.
- Empty-partner values per form are as in §3.2. They are emitted from the pair count and `valid`
  lanes. Nothing is special-cased per type: the existing `valid_ok` NULL rule in
  `emit_lane_column` already gives NULL for `valid == 0`.

### 6.5 Memory

Resident while streaming: the build table (unchanged) plus `W·|R|·B` (B) or `|R|·B` (A/C), plus
`4·|R|` for E-ROW. Freed after emit. Compare today's Q13: the GROUP BY over about 15M pair rows
holds per-worker partitions that each see most of the 1.5M customers, so it is of the same order
as B (`docs/GROUPBY_REDESIGN_2026_10_12.md` §1). All lane memory goes through the draken allocator
and accounting, the same as the GROUP BY lanes.

### 6.6 Native boundary

Python plans: the `GroupJoin` node, the specs, the emit mode and the form. The compiler wires three
pipelines through `NativePlan` setters (new `set_groupjoin_accum_sink`, `set_groupjoin_source`,
alongside `set_join2_mark_sink` `engine.hpp:851` and `set_semi_anti_build_source` `:875`).
Everything from build to emit is C++ with no GIL. No Python on the execution path.

### 6.7 No runtime fallback

The operator never decides to "do a separate join + GROUP BY instead". The strategy is fixed at
plan time and recorded. If lane allocation fails, the query fails loud. An actual `|R|` far beyond
the estimate is a telemetry fact (§8) for the estimator, not a trigger to switch.

---

## 7. Interaction with the GROUP BY redesign

### 7.1 Shared pieces (stable, consumed read-only)

`AggFn`/`AggSpec2`, `GBKind`/`gb_kind_of`, the row-kind arms, `emit_lane_column`, and the overflow
rules. The groupjoin needs one change to the arms: split "operand row index" from "group index".
Today `at(i)` resolves the group and the operand is read at `i` (`:2964-3040`); for pairs, the
operand index is `probe_row[k]`. The cleanest form is a template parameter for the operand-index
functor, so the GROUP BY instantiation is the identity and compiles to today's loop. **That edit
touches `native_group_sinks.hpp`, which has uncommitted redesign work in the main checkout.** It
should land after R1/R3 are committed, or the arms should first be moved into a small shared header
(`native_agg_lanes.hpp`) as a pure move (coordination item, §12). Until then the groupjoin must not
fork a copy of the arms (§3 duplication rule).

### 7.2 Lanes

Groupjoin lanes are fixed-size and their size is known before the first update (`total_rows` at
build `finalize`). That fits the R1 segmented store (`GBSegVec` with `hint(total_rows)` gives one
segment) or plain draken arrays. Reusing `GBSegVec` pulls in the R1 block pool and its retention
policy. A plain array keeps the groupjoin independent of R1 tuning.

### 7.3 The GROUP BY above (E-ROW only)

It sees `|R|` rows instead of `M`. R4's estimate ladder gets a `group_count_estimate` bounded by
`|R|`. E-KEY modes remove that GROUP BY entirely, so groupjoin adds no load to R4–R9.

### 7.4 Ordering

The groupjoin does not depend on R4, R5 or R9, and R9 (ticket table) does not affect it. The only
coupling is the arm signature change in §7.1.

---

## 8. Telemetry

**Plan-time** (`record_decision`, shown in EXPLAIN like the exchange decisions):
- `groupjoin`: `applied memo (form, mode)`, with `|R|`, `|S|`, `σ_S`, `σ_R`, `K_R`, and the
  `C_memo`/`C_none`/`C_eager` figures;
- or one of `declined: <precondition Pn>`, `declined: no σ estimate`,
  `declined: cost (time or memory, …)`.

Each decline reason is distinct, for the same reason the exchange records keep theirs apart
(`join_algorithm.py:590-594`): they point at different work.

**Runtime** (harvested after the run like `collect_join2_build_stats`, `engine.hpp:425`,
`compiler.py:7462`):
- build rows; streamed rows; streamed rows with a match (**actual σ_S**);
- slots touched / representatives emitted (**actual σ_R**, actual `K_R`);
- pairs accumulated; lane bytes allocated (per worker, pages touched); merge ns; emitted rows;
- NULL-group rows (LEFT).

The actual σ values next to the estimated ones are what make the cost model falsifiable.

---

## 9. Test plan

### 9.1 Making the rule fire in tests

The rule declines without statistics (P9), and VALUES relations have none. So correctness tests
need either parquet fixtures with manifests in `testdata/`, or a test-only planner override that
forces the decision through the ENV > CONFIG > DEFAULT chain. **Every correctness case is paired with a plan assertion** (EXPLAIN shows
"Group Join") **and an equivalence run with the rule disabled.** A groupjoin test that silently ran
the old plan is fake green.

### 9.2 Fixture (SLT under `tests/tools/sqllogictest/tests/results/`)

```
R(k, g) = (1,'a'), (1,'b'), (2,'a'), (3,'c'), (NULL,'d'), (NULL,'e')
S(k, x) = (1,10), (1,NULL), (2,5), (4,7), (NULL,3)
```

Expected values below are hand-derived from SQL semantics. Generate the golden with the DuckDB
oracle as the TPC-H SLTs do (`dev/tpch/generate_slt_golden.py`).

| # | SQL (abridged) | expected | covers |
|---|---|---|---|
| T1 | `SELECT r.k, COUNT(*), COUNT(s.x), COUNT(s.k), SUM(s.x), MIN(s.x), AVG(s.x) FROM R r LEFT JOIN S s ON r.k=s.k GROUP BY r.k` | `1: 4,2,4,20,10,10.0` · `2: 1,1,1,5,5,5.0` · `3: 1,0,0,NULL,NULL,NULL` · `NULL: 2,0,0,NULL,NULL,NULL` | GJ-LEFT, E-KEY⊗d (d=2 on k=1), **COUNT(*)=1 vs COUNT(col)=0 on unmatched**, one NULL group from two NULL-keyed R rows, S NULL key ignored |
| T2 | same, `INNER JOIN` | `1: 4,2,4,20,10,10.0` · `2: 1,1,1,5,5,5.0` | GJ-INNER, `σ_{c>0}` |
| T3 | `SELECT r.g, COUNT(*), SUM(s.x) FROM R r JOIN S s ON r.k=s.k GROUP BY r.g` | `a: 3,15` · `b: 2,10` | E-ROW + F² |
| T4 | T3 with `LEFT JOIN` | `a: 3,15` · `b: 2,10` · `c: 1,NULL` · `d: 1,NULL` · `e: 1,NULL` | E-ROW, LEFT empties per build row incl. NULL keys |
| T5 | `SELECT r.k, r.g FROM R r WHERE (SELECT COUNT(*) FROM S s WHERE s.k=r.k) = 0` | `(3,c)`, `(NULL,d)`, `(NULL,e)` | **static COUNT(*)=0**, no ⊗d (k=1 count is 2, not 4) |
| T6 | `SELECT r.k, r.g FROM R r WHERE 5 < (SELECT SUM(s.x) FROM S s WHERE s.k=r.k)` | `(1,a)`, `(1,b)` | static INNER, duplicate outer keys (D-2) / semi groupjoin (D-1) |
| T7 | `… GROUP BY r.k` with `SUM(s.big)` where `s.big` near INT64 max and `d=2` | error: SUM overflow (same message as GROUP BY) | `⊗d` overflow fails loud |
| T8 | empty S (`S WHERE FALSE`), LEFT | every R key with `COUNT(*)=d`, others 0/NULL | empty stream |
| T9 | empty R, LEFT and INNER | no rows | empty build (`total_rows == 0`) |
| T10 | composite key `ON r.k1=s.k1 AND r.k2=s.k2` | (golden) | Q20 shape |
| T11 | DECIMAL SUM/AVG, FLOAT SUM, DATE/VARCHAR MIN/MAX, UUID MIN/MAX | (golden) | each admitted `GBKind` |
| T12 | `COUNT(DISTINCT s.x)`, `MEDIAN`, HAVING in subquery, `IS NOT DISTINCT FROM`, residual `ON` | rule declines (EXPLAIN reason) + correct result | P1/P5/P7/P8 exclusions |
| T13 | `ON r.k = s.k` with INT32 vs INT64 / FLOAT vs DOUBLE | (golden) | key coercion |
| T14 | `$planets p LEFT JOIN testdata.satellites s ON p.id=s.planetId GROUP BY p.id` | per-planet counts, Mercury/Venus `COUNT(*)=1`, `COUNT(s.id)=0` | real fixture, skewed fan-out (Jupiter 67) |

### 9.3 C++ / engine

- A unit test under ASan/UBSan (alongside `make gb-block-store-test`):
  - CSR path and radix path (build ≥ 2^20 rows to cross `kRadixMinBuildRows`);
  - DOP 1 and DOP > 1 with a heavy-hitter key;
  - representative selection on CSR buckets holding more than one distinct hash;
  - page-lazy lanes and the touched-page merge.
- An integration test at SF1: `customer LEFT JOIN orders GROUP BY c_custkey` results identical
  with the rule on and off.
- A planner test per precondition: the rule fires, or declines with the expected reason. Also a
  strategy-order test that `_validate_strategy_order` rejects a misplaced `GroupJoinStrategy`.
- Sign-off: `make q`, then `make slt`; `make dt st rt` because a new native module is added
  (memory: DTSTRT).

---

## 10. Benchmark plan

### 10.1 Baseline first

Before any edit, record the current tree (memory: base1st):
- `dev/ab_bench.py --suite tpch --queries 3,13,17,18,20 --b . --iterations 7`, at **SF10**, on
  **both arches**: M5 (local) and the x86 box (`.60`, memory: x86). Plus SF1 for planning share.
- Per query, from telemetry: actual `|R|`, `|S|`, pair count `M`, GROUP BY input and output rows
  (confirms or refutes REVIEW's "15M → 1.5M" for Q13), and peak RSS.
- Controls: Q1, Q6 (no join/GB interaction) and Q9, Q21 (big joins, should be untouched).

### 10.2 Arms (`.so`/env-swapped from one tree, ≥ 7 interleaved rounds, order alternated)

1. Organic groupjoin (GJ-INNER/GJ-LEFT, E-KEY⊗d/E-ROW): targets Q13, Q18, Q3.
2. D-1 semi groupjoin: Q17, Q20.
3. D-2: Q20 (and Q17, to check the cost model declines it there).
4. Accumulation variant A (atomics) vs B (per-worker), on Q13 at W = 2, 4, 8, 16. Cloud Run is
   ~4 vCPU, so low W is the one that matters. Apple P/E-core ratios above about 6 workers are
   artefacts (memory: PEcores).

Per the build-all-then-ablate rule (memory: ABLATE), build 1–3, then run leave-one-out. Anything
that measures slower is deleted, not gated (memory: SLOWDITCH).

### 10.3 Cost-constant calibration

This is the Fent Fig. 12 experiment on our engine. An orders–lineitem groupjoin at SF10, with
both legs pre-filtered by primary key in 10% steps (σ_R × σ_S grid), each cell run with the
strategy forced to none, memo and eager (eager hand-written as SQL: GROUP BY below the join). This
fits `c_acc`, `c_emit + c_gb` and `c_merge` per arch. It also decides whether eager has a region at
all. Run on both arches and compare **ratios**, never raw times across machines (ab_bench
header).

### 10.4 Reporting

Per-query B/A median ratio with spread, RSS ratio, telemetry σ estimate vs actual, and the decision
each query took. Expectations to falsify:
- Q13 improves most (the RightOuter pair gather plus the 15M-row GROUP BY disappear);
- Q20 improves with D-2 only;
- Q17 and Q3 are small;
- Q18 is neutral (its R is a few hundred rows at SF10).

---

## 11. Not proposed / out of scope

- **Aggregate estimates** (Fent §4, skew-normal over HAVING): REVIEW item #14, a separate design.
  The groupjoin only consumes join σ.
- **Single-pass Q17** (one lineitem pass computing AVG per part and then filtering the same rows):
  that needs retaining matched probe rows (window-over-join), which a groupjoin is not.
- **Groupjoin for Q18's IN subquery** (`orders ⋉ Γ_l_orderkey HAVING`): σ_S ≈ 1 makes today's
  eager form the right one (Fent's rule).
- Shape-specialised accumulate paths (§11 of CLAUDE.md): none proposed. The update loop is the
  uniform `data[selection[i]]` arm.
- Spill: none, as for GROUP BY today.

---

## 12. Rulings and open items

Rulings (architect, 2026-10-15):

- Performance questions are settled by argument or measurement, not escalated. Defaults below.
- **Overrides** use the existing ENV > CONFIG > DEFAULT policy; test forcing of the groupjoin
  decision is a config setting under it.
- **Outer-leg-as-build rewrite (D-2): try it and report.** It needs the groupjoin operator first.
- **Accumulator memory: allocate in pages on first touch** (§6.3), with memory also weighed in
  the plan-time cost comparison against the separate plan; allocation failure fails loud.

Defaults being built (each overturned only by measurement):

| question | default | what would overturn it |
|---|---|---|
| accumulator form | per key, E-KEY⊗d when G ≡ join key (§3.1) | a query where the emit-time representative walk costs more than the saved updates |
| parallel accumulation | per-worker paged lanes + parallel merge (option B) | merge sweep or RSS at W = 16 / x86 worse than option C |
| slot space | table entry space `[0, total_rows)` | lane memory at high build duplication |
| eager for organic joins | not built unless the σ grid (§10.3) shows a region where it wins | that sweep |
| v1 aggregates | COUNT, COUNT(*), SUM, AVG, MIN, MAX, ANY_VALUE over S operands | — (scope, extend later) |

Coordination item (yours): the groupjoin needs a one-parameter change to the GROUP BY row-kind
arms in `native_group_sinks.hpp` (§7.1), which has uncommitted redesign work in the main checkout.
The change is made on this branch; merging it touches that file.

---

## 13. As built (2026-10-15, uncommitted)

Status line: Design §3–§6 built for the two shapes below, `make q` and `make slt` green on
M5, `make q` green on x86, measured on both arches (§13.3, §13.4).

### 13.1 What exists

| piece | where |
|---|---|
| Accumulate sink, emit source, per-worker lanes (option B), page-touched merge | `src/cpp/engine/native_groupjoin.hpp` |
| Engine setters + telemetry reading | `src/cpp/engine/engine.hpp` (`new_groupjoin_state`, `set_groupjoin_accum_sink`, `set_groupjoin_source`, `collect_groupjoin_stats`) |
| Cython bindings | `opteryx/operators/_operators.pyx` |
| Planner rule (annotation only) | `opteryx/planner/optimizer/strategies/groupjoin.py`, registered after `ProjectFusionStrategy` |
| Kill switch (ENV > CONFIG > DEFAULT) | `FEATURE_DISABLE_GROUPJOIN` (`opteryx/config.py`, `_STRATEGY_DISABLE_FLAGS`) |
| Compiler lowering | `opteryx/managers/execution/compiler.py` (`_wire_groupjoin`, `_compile_groupjoin_organic`, `_compile_groupjoin_outer`) |
| Plan-step fields | `AggregateAndGroupStep.groupjoin`, `JoinStep.groupjoin`, `JoinStep.key_reducer` (`plan_steps.pyx`) |
| Reducer stamp | both reducer graft sites set `key_reducer = True` (`decorrelate_subquery.py`, `semi_join_reducer.py`) |
| Runtime telemetry | `groupjoin_diagnostics` in query telemetry: streamed / matched / emitted / build rows, workers |
| Tests | `tests/tools/sqllogictest/tests/results/groupjoin_01.slt` (passes with the rule on AND off), `tests/planner/test_groupjoin.py` (9 plan assertions) |

Shapes fused:
- **Organic, per key**: `Γ_k;F(R ⋈ S)` and `Γ_k;F(R ⟕ S)` with k exactly R's join key, emit
  per key ⊗ d (§3.1). LEFT only for a single key column (one NULL group).
- **Organic, per row**: `Γ_G;F(R ⋈ S)` / `Γ_G;F(R ⟕ S)` with G any build-side columns (for
  INNER, a streamed join-key column stands for its build-side equal). One partial per build
  row, re-aggregated by an ordinary GROUP BY above (COUNT/SUM add, MIN/MAX/ANY_VALUE fold).
  AVG is declined here (it needs a sum/count pair carried to the GROUP BY).
- **Expression operands** (e.g. `SUM(l_extendedprice * (1 - l_discount))`) are evaluated
  only on matched rows: the compiler puts a SEMI probe against the same build table ahead of
  the operand projection, and the accumulate step probes those rows again for their slot.
- **DECIMAL128 SUM/AVG** accumulate in exact int128 lanes (MIN/MAX over DECIMAL128 declined).
- **Outer leg as build** (D-2): `O ⋈_k Π*(Γ_k;F(S [⋉ key reducer]))`, INNER or the engine's
  "nested loop" (INNER + residual, which becomes a filter on the emitted rows). It also fires
  on window aggregates, which `window_to_join` rewrites into exactly this shape. LEFT OUTER
  is not taken: decorrelation tests the inner key above the join.

The operand is re-viewed as a dict over its own buffer (codes = matched rows' physical
indices), so the GROUP BY row-kind arms (`gb_apply_row_kind`) are called unchanged. No edit
to `native_group_sinks.hpp` was needed, which removes the coordination item in §12.

### 13.2 Not built (vs the design)

- **Cost gate: measured structural rules, not the §5.4 model.** The planner's join
  estimates are too far off to decide on at this point (SF10: Q3 join est 3.2M vs 302K
  actual, Q10 5.1M vs 1.15M, Q10 build 3.8M vs 574K). A rule on their ratio picked
  exactly the wrong queries, so the gate reads statistics only where they are reliable:
  1. Forced build leg estimated > 4× the streamed leg: declined (no TPC-H query trips it).
  2. Per-row emit over **fixed-width group columns only**: declined. The pair gather and
     the GROUP BY are already cheap. Q3 measured 1.070× at 16 workers, 1.012× at 4 on M5,
     0.993× at 4 on x86.
  3. Per-row emit with a **unique streamed join key** (key NDV ≥ 0.9 × streamed rows):
     declined. Each build row matches at most one streamed row, so the GROUP BY input
     cannot shrink. Q12 measured 1.118× at 16 workers, 1.038× at 4 on M5, 1.016× on x86.

  The per-row losses are INHERENT to the shape, not the per-worker lane merge. Profiled on
  Q3 (pipeline stats): probe + accumulate costs the same wall as the join (~75 ms). The
  extra is the emit, ~3 ms walking all 1.47M build entries for ~140K matched rows, plus the
  GROUP BY above them, against a 0.1 ms GROUP BY over 302K pairs.

  A touched-slot merge (fold only Σ touched slots instead of W × pages) was built and
  measured: Q3 1.099×, Q12 1.106× (unchanged), Q13 0.804× (worse, from the first-touch
  bookkeeping). It was removed. The Fent owner-tag option C targets the same merge cost,
  so it is **not built**: the data says it would not move these losses. The σ-based eager
  choice is also unbuilt; no measured query favours eager.
- **Per-row AVG** (needs a sum/count pair re-aggregated above) is not built.
- **Outer-leg payload is full width** (no `pre_update_columns` pruning). A first attempt at
  pruning read the wrong layout and broke 10 `make q` window queries; it was removed.
- The runtime min/max join bound is not wired onto the groupjoin's stream leg.
- R-side aggregate operands, STDDEV/VAR/CORR and DECIMAL128 are excluded (declined).

### 13.3 Measured: M5, TPC-H SF10 skene, 7 interleaved rounds, order alternated

Arms: the same build, `FEATURE_DISABLE_GROUPJOIN=1` (OFF) vs default (ON). Medians, ms.
Final version (run `ab-tpch-Justins-MacBook-Pro-20261015-202057`). The rule fires on six
queries; the other 16 are controls.

| query | shape | OFF | ON | ON/OFF | rounds won |
|---|---|---|---|---|---|
| Q2 | outer leg as build (per-key form declined: build-side rule) | 53.13 | 45.73 | 0.861 | 7/7 |
| Q10 | organic INNER, per row + GROUP BY, DECIMAL128 expression | 188.20 | 152.51 | 0.810 | 7/7 |
| Q13 | organic LEFT, per key | 156.83 | 114.68 | 0.731 | 7/7 |
| Q17 | outer leg as build | 112.54 | 91.33 | 0.812 | 7/7 |
| Q18 | organic INNER, per row + GROUP BY | 171.24 | 166.29 | 0.971 | 7/7 |
| Q20 | outer leg as build (nested loop) | 84.22 | 69.09 | 0.820 | 7/7 |
| 16 controls | — | — | — | 0.974–1.025 | — |
| 22-query total | | 2648.7 | 2525.1 | 0.953 | |

Correctness: all 22 TPC-H queries give identical results ON vs OFF at SF1 (and at SF10 for
the firing queries). `make q`, `make slt` (64/64) and `tests/planner/test_groupjoin.py`
(10 tests) are green on M5.

### 13.3b TPC-DS SF1 skene (second workload, rules not tuned on it at first)

Correctness: all 99 queries run ON vs OFF. 97 are identical. Q51 is refused identically by
both arms (window over joined tables). Q75 differs between ANY two runs, OFF vs OFF
included: float SUM order changes the last digits and its `ORDER BY … LIMIT` cut. The rule
does not fire on Q75.

What TPC-DS found, in order:
1. **A quadratic bug.** The CSR emit found each entry's representative by rescanning its
   bucket, O(n²) for a bucket holding one key's many duplicates. Q1 was 17.6× slower and
   Q6 4.3×. Fixed to one pass per bucket (`GJBucketKeys`). TPC-H never had heavy
   duplicate build keys on this path.
2. **Rules added after the bug fix:**
   - *Organic*: the groupjoin's build side must agree with the join planner's choice.
     INNER builds left; LEFT OUTER builds the preserved leg only if the join was exchanged.
     This catches TPC-H Q2's per-key form, which built all of partsupp: 1.270× when it ran
     alone, its leg estimates 12× off.
   - *Outer leg*: declined when the outer leg is estimated > 4× the aggregate's input, or
     when there is no reducer/window copy to remove and the aggregate folds > 100 input
     rows per group (TPC-DS Q6).
   - *Expression pre-filter*: skipped when the join is estimated to keep ≥ 50% of the
     streamed rows (TPC-DS Q23).

Timing, M5, 7 rounds, same harness discipline (`ds_ab.py`: fresh subprocess per arm-round,
order alternated):

| result | queries |
|---|---|
| wins (0.93–0.98, ≥ 5/7) | Q12 .961, Q20 .969, Q30 .982, Q32 .955, Q36 .960, Q43 .935, Q58 .976, Q63 .974, Q70 .982, Q81 .971, Q83 .961, Q89 .955, Q92 .968, Q98 .931 |
| neutral | Q5, Q41, Q53, Q57, Q77, Q80 |
| **losses** | **Q1 1.162 (0/7)**, Q23 1.042 (1/7), Q47 1.031 (2/7) |
| controls | 0.983–1.011 |
| firing + controls total | 1.002 |

Residual losses, honestly:
- **Q1:** outer leg as build over ~49K rows with about a dozen keys. Its outer and group
  estimates are 14×+ off, so no rule built on them can see it. It is a 2 ms difference on
  a 12 ms query.
- **Q23** (per key, ~100 ms) and **Q47** (window) are small and unexplained. Q47 ranged
  0.989–1.031 across runs.

The SF1 queries run 7–170 ms, where fixed costs dominate.

### 13.3c M5 at 4 workers (production-like; x86 box unavailable for the final version)

`MAX_EXECUTION_WORKERS=4`, same arms, 7 rounds. My notes flag Mac ratios above ~6 workers
as partly a P/E-core artefact; production is ~4 vCPU.

TPC-H SF10 (run `ab-tpch-Justins-MacBook-Pro-20261015-220428`):

| query | ON/OFF | rounds won |
|---|---|---|
| Q2 | 0.872 | 7/7 |
| Q10 | 0.872 | 7/7 |
| Q13 | 0.781 | 7/7 |
| Q17 | 0.760 | 7/7 |
| Q18 | 0.951 | 7/7 |
| Q20 | 0.824 | 7/7 |
| controls | 0.994–1.008 (Q12 read 1.060 once; re-measured 1.000 over 9 rounds, and it is declined) | — |
| 22-query total | 0.959 | — |

TPC-DS SF1 (firing + controls) at 4 workers:
- Total 0.999.
- Wins: Q36 0.884, Q43 0.934, Q32 0.944, Q92/Q41/Q58 ~0.97.
- The window wins seen at 16 workers (Q12/Q20/Q98) are ~1.00 here.
- Losses: **Q1 1.117 (0/7)**; Q47, Q57, Q70, Q81 at 1.012–1.014 (near noise).
- Controls: 0.998–1.016.

### 13.4 x86: i5-8500 (`.60`), TPC-H SF10 skene, 7 interleaved rounds, same arms

*(Measured on the version BEFORE the TPC-DS round's fixes and rules. The final version was
not re-run on x86: the box was in use by another session all evening.)*

Final version (run `ab-tpch-opteryx-perf-20261015-182741`, `make q` green). x86 runs 4
workers (cpu − 2).

| query | OFF | ON | ON/OFF | rounds won |
|---|---|---|---|---|
| Q2 | 361.23 | 316.42 | 0.876 | 7/7 |
| Q10 | 1194.04 | 1130.69 | 0.947 | 7/7 |
| Q13 | 1449.29 | 1114.89 | 0.769 | 7/7 |
| Q17 | 966.46 | 793.20 | 0.821 | 7/7 |
| Q18 | 1388.00 | 1342.96 | 0.968 | 7/7 |
| Q20 | 681.28 | 540.66 | 0.794 | 7/7 |
| 16 controls | — | — | 0.986–1.011 | — |
| 22-query total | 22013.0 | 21207.5 | 0.963 | |

Before the gate, at x86's 4 workers: Q3 0.993× (neutral), Q12 1.016× (0/7), both now declined.

Raw results: `~/gj-20261015/tree/dev/bench_results/ab-tpch-opteryx-perf-20261015-182741.csv` on
the box; M5 `dev/bench_results/ab-tpch-Justins-MacBook-Pro-20261015-182715.csv`. The i5 is
JCC-erratum affected (memory: ARCHPERF), so ±3–5% x86 deltas need an unaffected box; these
are 5–23%, outside that except Q18's 3–4%, which repeats on both arches.

---

## Appendix A — adjacent findings (reported, not acted on)

- **REVIEW §1.3 lists Q18 under the reducer chain.** Opteryx's Q18 has no reducer: its IN subquery
  is uncorrelated (EXPLAIN, §2.4).
- `native_group_sinks.hpp` in the main checkout differs from `HEAD` (uncommitted GROUP BY
  redesign). Citations past about line 3050 give both line numbers.
