# Design: Ranking Window Functions + INTERSECT ALL / EXCEPT ALL

Status: **APPROVED (2026-06-14).** Architect decisions:
- **Scope**: full Phase 1 → 3 (ROW_NUMBER, RANK, DENSE_RANK, with ORDER BY).
- **User-facing ranking functions REQUIRE ORDER BY.** The no-ORDER-BY ROW_NUMBER variant
  is internal-only, used solely by the INTERSECT/EXCEPT ALL rewrite. A user query using
  any ranking function without `ORDER BY` is a clean `UnsupportedSyntaxError`.
- **Dedicated Cython `opteryx/operators/window/` operator**, added to `_DISPATCH`,
  modeled on `DistinctNode`.

## Goal

1. Implement `ROW_NUMBER()` (and, scope permitting, `RANK()` / `DENSE_RANK()`) as
   window functions.
2. Rewrite `INTERSECT ALL` / `EXCEPT ALL` to `ROW_NUMBER` + semi/anti join, replacing
   today's fail-fast `InvalidInternalStateError`.

## Why the existing window machinery cannot be reused

Aggregate windows (`SUM(x) OVER (PARTITION BY c)`) are lowered by
[`window_to_join.py`](../opteryx/planner/plan_rewriter/strategies/window_to_join.py)
to `GROUP BY c` (one row per partition) + broadcast inner join. That collapses the
partition to a single value.

`ROW_NUMBER` must assign a **distinct** `1..n` to **each row** of a partition. For the
set-op-ALL use case the partition key is *every projected column*, so the rows in a
partition are byte-identical — only a stateful per-row counter can hand them distinct
numbers. No `GROUP BY`/join rewrite, correlated-subquery count, or theta-join can do
this (identical rows tie). **A dedicated physical window operator is required.** This is
the central decision below.

## Multiset semantics the rewrite must produce

- `INTERSECT ALL`: emit `min(count_left(r), count_right(r))` copies of row `r`.
- `EXCEPT ALL`: emit `max(count_left(r) − count_right(r), 0)` copies of row `r`.

Standard `ROW_NUMBER` rewrite (order within partition is irrelevant — we only need
distinct occurrence indices):

```
-- INTERSECT ALL
L' = SELECT cols, ROW_NUMBER() OVER (PARTITION BY cols) AS rn FROM left
R' = SELECT cols, ROW_NUMBER() OVER (PARTITION BY cols) AS rn FROM right
SELECT cols FROM L' SEMI JOIN R' ON L'.cols = R'.cols AND L'.rn = R'.rn

-- EXCEPT ALL  (same, ANTI JOIN)
SELECT cols FROM L' ANTI JOIN R' ON L'.cols = R'.cols AND L'.rn = R'.rn
```

The `rn`-th left copy matches only if the right side has ≥ `rn` copies → `min` for SEMI,
`max(left−right,0)` for ANTI. Correct, and reuses the semi/anti hash-join + the
`live_relations` chaining fix already landed.

## Proposed architecture

### Phase 1 — `ROW_NUMBER()` with PARTITION BY only (no ORDER BY) — streaming operator

This is the **minimum that unlocks INTERSECT/EXCEPT ALL** and is the simplest operator
(no buffering/sort).

- **New physical operator** `opteryx/operators/window/` (Cython), modeled on
  [`DistinctNode`](../opteryx/operators/distinct/distinct.pyx): stateful cross-morsel
  hashing. Maintains a hash map `partition_key_hash -> counter`; for each row emits
  `++counter`. Streaming (no blocking), GIL released over the row loop, draken vectors,
  full-row key hashing reusing the same hashing the distinct/hash-join path uses.
- **Logical node**: reuse `LogicalPlanStepType.Window`, extended with a `functions`
  kind tag so ranking funcs are distinguishable from aggregate funcs; carry
  `partition_by`. Add `Window` to the physical `_DISPATCH`
  ([`physical_planner/__init__.py`](../opteryx/planner/physical_planner/__init__.py)) →
  the new operator. (Aggregate windows continue to lower via the join rewrite;
  `window_to_join.should_i_run` must ignore ranking-window nodes — same fixed-point
  discipline as the set-op fix.)
- **Registration**: `ROW_NUMBER` recognized as a window-only function (errors if used
  without `OVER`). Likely a new small `RANKING` set rather than overloading
  `AGGREGATORS` in [`operators/aggregate/helpers.py`](../opteryx/operators/aggregate/helpers.py),
  since ranking funcs don't go through the aggregate operator.

### Phase 2 — set-op-ALL rewrite

New plan-rewriter strategy `intersect_except_all_to_window_join.py`:
- Fires on `Intersect`/`Except` nodes with `modifier == "All"` and resolvable columns.
- Builds the `ROW_NUMBER` Window node over each leg's subplan **directly in the plan**
  (bypassing the SQL-level window detection, so the current "single base scan / no
  joins / no GROUP BY" window constraints do not apply to legs).
- Reuses `_build_on_condition` + `live_relations` (already written) for the
  `cols = cols AND rn = rn` semi/anti join, then projects `rn` away.
- Remove the `modifier == "All"` fail-fast; flip the 3 pinned shape tests from
  `InvalidInternalStateError` to row-count assertions.

### STATUS: ✅ Phase 1, 2, 3 ALL LANDED.
### - Phase 1+2: INTERSECT/EXCEPT ALL, multiset min/max verified.
### - Phase 3: user-facing ROW_NUMBER/RANK/DENSE_RANK with ORDER BY (blocking sort),
###   tie semantics verified (RANK skips, DENSE_RANK doesn't), required-ORDER-BY +
###   required-OVER enforced. make q 182 + tpch 22 + shapes 183.

### Phase 3 (LANDED) — ORDER BY in windows + RANK / DENSE_RANK

- Allow `ORDER BY` in the window spec for ranking functions (currently rejected at
  [`logical_planner.py:481`](../opteryx/planner/logical_planner/logical_planner.py)).
- Window operator gains a **blocking** path: buffer partition rows, sort by the order
  key, then number (`ROW_NUMBER` = 1..n; `RANK`/`DENSE_RANK` = tie-aware).
- Not needed for INTERSECT/EXCEPT ALL; pure feature expansion.

## Constraints / risks to confirm

- Window operator state is per-partition-key across morsels — memory is O(distinct
  partition keys). For set-op-ALL the key is the full row; worst case O(distinct rows),
  same order as the downstream hash join already pays. Acceptable.
- Phase 1 ROW_NUMBER has arbitrary intra-partition order (no ORDER BY) — correct for
  set-ops, but if exposed as a user-facing `ROW_NUMBER() OVER (PARTITION BY c)` with no
  ORDER BY, the numbering is nondeterministic (standard SQL allows this). Confirm we’re
  OK surfacing that to users, or gate user-facing ROW_NUMBER on Phase 3 (ORDER BY) and
  keep Phase 1 internal-only for the rewrite.

## Open decisions for the architect

1. **Scope now**: Phase 1+2 only (ROW_NUMBER no-ORDER-BY → unlocks set-op ALL), or push
   straight through Phase 3 (full ranking + ORDER BY)?
2. **User-facing ROW_NUMBER without ORDER BY**: allow (nondeterministic, SQL-legal) or
   restrict ROW_NUMBER to internal rewrite use until Phase 3?
3. **Operator confirmation**: dedicated Cython `window/` operator added to `_DISPATCH`,
   modeled on DistinctNode — agreed?
