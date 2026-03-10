# Parquet Expanded Predicate Pushdown and Late Materialization Design

## Objective

Increase the set of predicates pushed into `ParquetReadNode` and implement
Phase 2 late materialization to reduce bytes decoded for selective queries.

The two improvements are directly synergistic: more pushed predicates means
more rows eliminated before wide projection columns are ever read.

---

## Background: Two Predicate Evaluation Tiers

Parquet scan predicate evaluation has two distinct levels with different
capabilities and costs.

### Tier 1 — Row-group pruning (before any column I/O)

Uses Parquet footer min/max column statistics to skip entire row groups.
Zero column bytes are read for pruned row groups.

Supported predicate shape: `identifier op literal` where `op ∈ {Eq, NotEq,
Gt, GtEq, Lt, LtEq}`.

Implemented in `parquet_io/predicates.py` via `extract_predicate_stats()`.
This is intentionally narrow — the correctness contract requires that no
matching row can exist in a pruned row group, which only holds for monotonic
range comparisons against scalar bounds.

### Tier 2 — Row-level filtering (after filter-column decode)

Evaluates an expression tree against decoded row data to mask individual rows.
Any predicate the Draken evaluator supports can be applied here.

Implemented in `ParquetReadNode._apply_predicates_to_morsel()` via
`evaluate_draken()`.

Supported predicate forms (as of current evaluator):

| NodeType             | Values / operators                                              |
|----------------------|-----------------------------------------------------------------|
| COMPARISON_OPERATOR  | Eq, NotEq, Gt, GtEq, Lt, LtEq, Like, NotLike, ILike, NotILike, RLike, NotRLike, InList, NotInList |
| UNARY_OPERATOR       | IsNull, IsNotNull, IsTrue, IsFalse, IsNotTrue, IsNotFalse       |
| NOT                  | wrapping any evaluable sub-expression                           |
| AND / OR / XOR / DNF | trees of evaluable sub-expressions; AND short-circuits          |
| NESTED               | identity wrapper — transparent                                  |
| FUNCTION (PASSTHRU)  | optimizer-generated PASSTHRU wrappers for rewritten predicates  |

---

## Current State

### What is pushed today

`PredicatePushdownStrategy` pushes a Filter node into `ParquetReadNode.predicates`
only when:

```
node.condition.node_type == COMPARISON_OPERATOR
AND len(identifiers) >= 1
AND no AGGREGATOR nodes in tree
AND len(node.relations) > 0
```

Because the gate requires the root to be `COMPARISON_OPERATOR`, these predicate
forms currently remain as Filter nodes sitting above the scan:

| Predicate form                    | Root node type   | Why not pushed today      |
|-----------------------------------|------------------|---------------------------|
| `col IS NULL`                     | UNARY_OPERATOR   | not COMPARISON_OPERATOR   |
| `col IS NOT NULL`                 | UNARY_OPERATOR   | not COMPARISON_OPERATOR   |
| `col IS TRUE` / `col IS FALSE`    | UNARY_OPERATOR   | not COMPARISON_OPERATOR   |
| `NOT col LIKE 'pat'`              | NOT              | not COMPARISON_OPERATOR   |
| `NOT (col IN (...))`              | NOT              | not COMPARISON_OPERATOR   |
| `col1 = 5 AND col2 LIKE '%x%'`   | AND              | not COMPARISON_OPERATOR   |
| Function-bool: `REGEXP_LIKE(...)` | FUNCTION         | not COMPARISON_OPERATOR   |

Note: `LIKE`, `ILIKE`, `RLIKE`, `IN`, `NOT LIKE`, `NOT RLIKE`, `NOT IN` are
already `COMPARISON_OPERATOR` nodes by value. They ARE already pushed when
the predicate root is `COMPARISON_OPERATOR`. The principal gap is IS NULL /
IS NOT NULL / NOT wrappers / AND-OR roots / FUNCTION boolean predicates.

### What is done with pushed predicates

1. `extract_predicate_stats()` extracts `(col, op, value)` triples for
   **Tier 1 row-group pruning** (narrow, fails silently for unsupported forms).
2. `_apply_predicates_to_morsel()` evaluates all pushed predicates for
   **Tier 2 row-level filtering** (wide, uses the full Draken evaluator).

Expanding the pushed set only affects step 2 — `extract_predicate_stats()`
silently ignores expressions it cannot map to min/max statistics, so Tier 1
correctness is unaffected.

---

## Proposed Design

Two coupled changes:

**A. Expand pushdown eligibility** — allow any predicate the Draken row-level
evaluator can handle to be pushed into `ParquetReadNode.predicates`. This is
a planner change in `PredicatePushdownStrategy`.

**B. Late materialization** — read filter-only columns first, evaluate the
pushed predicate tree to get a row mask, then conditionally read remaining
projection columns only for surviving rows. This is an execution change in
`ParquetReadNode`. The existing Phase 2 design in
`parquet-predicate-late-materialization-design.md` describes the mechanics;
this document provides the predicate eligibility rules that make it worthwhile.

---

## Part A — Expanded Pushdown Eligibility

### Eligibility predicate (replaces `is_simple_comparison`)

A Filter node condition is row-level evaluable and may be pushed if:

1. No `AGGREGATOR` node anywhere in the expression tree.
2. No `SUBQUERY` node anywhere in the expression tree (correlated subqueries
   require the outer scope; scalar subqueries are already constant-folded
   before this pass, so the concern is correlated forms).
3. At least one `IDENTIFIER` node referencing the scan relation.
4. All `IDENTIFIER` nodes reference the same single relation (no cross-relation
   joins masquerading as filter nodes).
5. The **condition root** is one of:
   - `COMPARISON_OPERATOR` (existing; any op value)
   - `UNARY_OPERATOR` (IsNull, IsNotNull, IsTrue, IsFalse, IsNotTrue, IsNotFalse)
   - `NOT` wrapping an eligible sub-expression
   - `AND`, `OR`, `XOR`, `DNF` where both operands are eligible sub-expressions
   - `NESTED` wrapping an eligible sub-expression
   - `FUNCTION` with value `PASSTHRU` (optimizer-generated wrappers only;
     general FUNCTION roots are excluded in Phase A; see open questions)

Check 5 is recursive: an `AND`-rooted condition is eligible only when both
branches independently satisfy all constraints.

### Why restrict FUNCTION roots

Determining at planning time whether an arbitrary FUNCTION returns a BoolVector
requires type propagation that does not yet exist. The evaluator raises
`TypeError` at runtime if the result is not BoolVector, which would corrupt a
pushed morsel batch. `PASSTHRU` is the only safe exception because the
optimizer only wraps boolean sub-expressions in it.

General FUNCTION-rooted predicates (e.g., `REGEXP_LIKE(col, pat)`) remain as
Filter nodes above the scan for now.

### Planner change location

`opteryx/planner/optimizer/strategies/predicate_pushdown.py`

Replace the `is_simple_comparison` boolean with a call to a new helper
`_is_row_level_evaluable(condition)` that recursively applies rule 5.

```python
def _is_row_level_evaluable(condition) -> bool:
    if condition is None:
        return False
    nt = condition.node_type
    if nt == NodeType.COMPARISON_OPERATOR:
        return True
    if nt == NodeType.UNARY_OPERATOR:
        return True
    if nt == NodeType.NOT:
        return _is_row_level_evaluable(condition.centre)
    if nt in (NodeType.AND, NodeType.OR, NodeType.XOR):
        return (
            _is_row_level_evaluable(condition.left)
            and _is_row_level_evaluable(condition.right)
        )
    if nt == NodeType.DNF:
        return all(_is_row_level_evaluable(p) for p in condition.parameters)
    if nt == NodeType.NESTED:
        return _is_row_level_evaluable(condition.centre)
    if nt == NodeType.FUNCTION and condition.value == "PASSTHRU":
        return all(_is_row_level_evaluable(p) for p in condition.parameters)
    return False
```

The gate becomes:

```python
if len(node.relations) > 0 and not has_agg and not has_subquery and _is_row_level_evaluable(node.condition):
    # push predicate
```

### No changes to `extract_predicate_stats`

Row-group pruning stays narrow. `extract_predicate_stats()` already silently
drops any predicate shape it cannot map to min/max statistics. Newly pushed
IS NULL / NOT / AND-root predicates will simply not contribute to row-group
pruning, but they will be evaluated at row level — which is correct.

### Guard for AND-rooted compound Filter nodes

If the optimizer preserves compound AND conditions as single Filter nodes (not
split into separate Filter nodes per leaf), pushing an AND-root condition
means both sub-predicates are evaluated together in the pushed morsel pass.
This is strictly correct: AND semantics are preserved by `evaluate_draken`.

The benefit is that the compound predicate evaluates inside the columnar scan
loop rather than in a separate Filter node above it, reducing morsel
allocations and operator overhead.

---

## Part B — Late Materialization

### Motivation

Today `ParquetReadNode` reads `projection_columns ∪ filter_columns` in a
single pass. For selective queries against wide tables, the majority of decoded
bytes correspond to projection columns on rows that the predicate immediately
discards.

Late materialization addresses this by:

1. Reading only `filter_columns` first.
2. Evaluating the predicate tree → row mask.
3. If no rows survive, skipping the projection read entirely.
4. If rows survive, reading only `projection_columns \ filter_columns` for
   surviving row indices, then assembling the final morsel.

With expanded pushdown eligibility (Part A), Step 2 eliminates more rows
before the wide projection read occurs.

### Heuristic gate

Late materialization is only beneficial when:

- The estimated selectivity (rows surviving / rows read) is below a threshold
  (default `PARQUET_LATE_MATERIALIZATION_SELECTIVITY_THRESHOLD`, default 0.4).
- There are projected columns outside the filter-column set
  (`len(projection_columns - filter_columns) > 0`).
- The projected non-filter columns materially exceed the filter columns in
  estimated byte footprint (heuristic: `projected_non_filter_columns > 0` is
  sufficient for the first implementation; byte-level estimation is a future
  enhancement).

When the gate does not trigger, fall back to the existing single-pass read
(Phase 1 behaviour).

### Selectivity estimation

Initial selectivity is not known before the first row group is read, and
within a single file the selectivity of consecutive row groups is usually
similar — the same predicate that misses one row group will likely miss
the next. This is the same principle that drives the bloom filter prefilter
on JOINs: default-enable the optimisation and abandon it when continued
use is demonstrably unprofitable.

The late-materialization pass is also an all-or-nothing operation per row
group: there is no mechanism to partially materialise rows within a group.
The unit of decision is always the whole row group.

**Approach — rolling abandonment**: Late materialization is enabled by default
when there are non-filter projected columns. After each row group, maintain a
rolling count of consecutive row groups where Pass 1 eliminated zero rows
(i.e., every row survived the predicate). When that count reaches
`PARQUET_LATE_MATERIALIZATION_ABANDON_AFTER` (default 5) consecutive
no-benefit row groups, disable the two-pass path for all remaining row groups
in the same file and fall back to single-pass reads. Reset the counter at the
start of each new file.

This mirrors the JOIN bloom-filter discard heuristic: pay the two-pass
overhead when it is earning its keep, and stop when empirical evidence from
the current file shows it is not.

The rolling window is reset per file rather than per query because selectivity
variation across partitions is common: a predicate that cuts nothing on one
file may cut heavily on another. Per-file tracking avoids the cost of a
permanently disabled optimisation when only some partitions are dense.

### Execution shape change in `ParquetReadNode.execute()`

**Current shape (single pass):**

```
fetch(filter_columns ∪ projection_columns)
│
evaluate_predicate → row_mask
│
drop filter-only columns
│
emit projected morsel
```

**Late materialization shape (two pass):**

```
Pass 1: fetch(filter_columns)
│
evaluate_predicate → row_mask, selectivity
│
┌────── selectivity > threshold ──────┐
│                                     │
single-pass fallback                 Pass 2: fetch(projection_columns \ filter_columns)
(re-read filter_cols + proj_cols)    │           for surviving row indices
                                     assemble final morsel
                                     │
                                     emit
```

The "re-read" fallback on the left branch exists because Pass 1 only decoded
filter columns. When selectivity is too high to justify a separate second
pass, re-reading everything in one shot is simpler than storing the decoded
filter columns and appending projection columns. At the selectivity threshold
(e.g., 40%), the cost of re-reading filter columns is small relative to the
saved projection bytes.

Alternative: cache the decoded filter columns from Pass 1 so the combined
morsel can be assembled without re-reading. This avoids re-issuing the filter
column byte ranges but increases memory pressure. Defer this optimisation to
a follow-up.

### Row index passing for second pass

The Parquet column-chunk reader must accept an optional row selection mask to
restrict which rows it decodes from the second-pass columns. This is already
possible in the `iter_row_groups` / `rugo` path via the `selection` argument
in `parquet_decoder`. Confirm that `selection` (row indices or boolean mask)
is threaded through the two-pass call correctly.

### Scheduler interaction

The existing parallel scheduler in `ParquetReadNode` dispatches row-group
work units to a thread pool. For late materialization:

- Pass 1 (filter columns) is issued as a normal scheduled work unit.
- Pass 2 (projection columns) is issued as a follow-up work unit for the same
  row group, gated on Pass 1 completing and the selectivity heuristic.
- Cancellation on LIMIT: if `records_to_read` reaches zero after Pass 1
  results are applied, discard the Pass 2 work unit before issuing I/O.

No structural scheduler changes are required for Phase A. Phase B requires
tracking per-row-group state to conditionally dispatch Pass 2 — this is new
but bounded in complexity.

### Feature flag

Gate behind `FEATURE_PARQUET_LATE_MATERIALIZATION` (already named in the
`parquet-predicate-late-materialization-design.md` doc), default off until
A/B benchmarks confirm no regression on non-selective workloads.

Predicate eligibility expansion (Part A) does NOT need a flag — it is a
correctness improvement (more predicates evaluated earlier) with no
performance downside. The pushed predicate set only grows; the Tier 1
row-group pruning path is unaffected.

---

## Predicate Tiers Summary

| Predicate category              | Row-group prunable? | Row-level pushable today? | Row-level pushable after Part A? |
|---------------------------------|---------------------|--------------------------|----------------------------------|
| `col op literal` (Eq/.../LtEq) | Yes                 | Yes                      | Yes (unchanged)                  |
| `col LIKE 'pat'`                | No                  | Yes                      | Yes (unchanged)                  |
| `col IN (...)`                  | No                  | Yes                      | Yes (unchanged)                  |
| `col NOT LIKE 'pat'`            | No                  | Yes                      | Yes (unchanged)                  |
| `col IS NULL`                   | No                  | No — Filter above scan   | **Yes — new**                    |
| `col IS NOT NULL`               | No                  | No — Filter above scan   | **Yes — new**                    |
| `NOT col_pred`                  | No                  | No — Filter above scan   | **Yes — new**                    |
| `pred1 AND pred2` (AND root)    | No                  | No — Filter above scan   | **Yes — new**                    |
| `pred1 OR pred2` (OR root)      | No                  | No — Filter above scan   | **Yes — new**                    |
| `REGEXP_LIKE(col, pat)` (FUNCTION) | No              | No — Filter above scan   | No — future work                 |
| Aggregates in predicate         | No                  | No                       | No (never pushable)              |
| Correlated subquery             | No                  | No                       | No (never pushable)              |

---

## Architectural Boundary

No change to existing boundary:

- `parquet_io/*` owns byte-range planning, fetch scheduling, decode, and
  footer statistics pruning. It does not own SQL predicate semantics.
- `operators/parquet_read_node.py` owns predicate evaluation, row masking,
  and projection assembly.

Part A changes touch `planner/optimizer/strategies/predicate_pushdown.py`
only.  
Part B changes touch `operators/parquet_read_node.py` and potentially add
a selection parameter threading change in `parquet_io`.

---

## Interactions with Existing Features

### `LIMIT` pushdown

`records_to_read` limits rows emitted per morsel batch. In two-pass mode,
after Pass 1 the surviving row count determines whether to issue Pass 2. If
`records_to_read` drops to zero after a batch, cancel any pending Pass 2
work units for subsequent row groups.

### Dictionary encoding

`rugo` preserves dictionary encoding. Predicate evaluation in
`_apply_predicates_to_morsel` must handle DictionaryVector → comparison
correctly. This is already handled by the Draken evaluator's type dispatch.

### Repeated / list columns

Repeated / list column row groups use the full-file fallback path. Late
materialization does not apply to these row groups. Row-level predicate
evaluation already applies via `_apply_predicates_to_morsel` in the fallback.

---

## Observability

Part A (no new metrics needed — existing rows_before/after_filter captures the gain).

Part B adds:

| Metric                                | Description                                              |
|---------------------------------------|----------------------------------------------------------|
| `parquet_late_materialization_used`   | Count of row groups where two-pass was engaged           |
| `parquet_late_materialization_skipped`| Count of row groups where all rows were filtered in Pass 1 (zero-scan saving) |
| `parquet_late_materialization_abandoned`| Count of files where rolling abandonment threshold was reached |
| `parquet_pass1_bytes`                 | Bytes decoded for filter-only columns                    |
| `parquet_pass2_bytes`                 | Bytes decoded for projection-only columns in Pass 2      |

---

## Open Questions

1. **AND-root split vs. push**: Does the binder/planner already split AND
   conditions in a WHERE clause into independent Filter nodes? If so, the
   AND-root eligibility extension in Part A covers unusual multi-column
   predicates arriving as compound Filter nodes only. Investigation required.

2. **FUNCTION-root boolean predicates**: `REGEXP_LIKE(col, pat)` and similar
   user-facing functions produce a FUNCTION-rooted predicate tree. If row
   counts after the evaluator confirm correctness, extend Part A eligibility
   to include FUNCTION roots that have identifier arguments but no aggregators.
   Requires guard against functions that raise TypeError at runtime.

3. **Abandonment threshold calibration**: The default window of 5 consecutive
   no-benefit row groups is a starting point. Profile against a mix of
   selective and non-selective workloads to confirm it is neither too eager
   (abandons before filtering materialises) nor too conservative (absorbs
   overhead for many unprofitable groups before stopping).

4. **Second-pass row index encoding**: Does `selection` in `parquet_decoder`
   accept a boolean mask, integer indices, or both? Confirm and document the
   contract before implementing Part B.

5. **Byte-footprint heuristic**: Using column count as a proxy for projected
   bytes is inaccurate for compressed narrow strings vs. wide numeric arrays.
   A future refinement is to use the footer's `total_compressed_size` values
   to compute a proper ratio.

---

## Task List

### Part A — Expand Pushdown Eligibility

- [ ] Audit: determine whether the binder splits AND-root WHERE clauses into
  separate Filter nodes or preserves compound conditions (affects scope of AND
  root expansion).
- [ ] Add `_is_row_level_evaluable(condition)` helper to `predicate_pushdown.py`.
- [ ] Replace `is_simple_comparison` gate with `_is_row_level_evaluable` call.
- [ ] Add `not has_subquery` guard using `get_all_nodes_of_type(condition, (NodeType.SUBQUERY,))`.
- [ ] Unit test: IS NULL predicate pushed correctly for Parquet scan.
- [ ] Unit test: IS NOT NULL predicate pushed correctly.
- [ ] Unit test: NOT LIKE predicate pushed (NOT-root with LIKE child).
- [ ] Unit test: compound AND-root predicate pushed if AND is present as root.
- [ ] Regression test: pushed predicates do not regress ClickBench battery.
- [ ] Confirm `extract_predicate_stats` correctly ignores newly pushed
  non-prunable forms (already by design, but add a targeted test).

### Part B — Late Materialization

- [ ] Add `FEATURE_PARQUET_LATE_MATERIALIZATION` config flag (default on).
- [ ] Add `PARQUET_LATE_MATERIALIZATION_ABANDON_AFTER` config parameter
  (default 5): consecutive no-benefit row groups before falling back to
  single-pass for the remainder of the file.
- [ ] Implement filter-only first pass in `ParquetReadNode`.
- [ ] Implement conditional second pass for projection-only columns.
- [ ] Implement rolling abandonment counter: reset per file; increment when
  Pass 1 eliminates zero rows; disable two-pass for file when counter reaches
  threshold.
- [ ] Implement fast-path: zero surviving rows → skip projection read entirely.
- [ ] Wire `selection` mask through `parquet_decoder` for second pass.
- [ ] Respect `records_to_read` limit: cancel pending Pass 2 work units when
  limit is reached after Pass 1 results.
- [ ] Add Phase B telemetry metrics listed above.
- [ ] Benchmark: selective scan (< 5% selectivity) on wide table — measure
  byte reduction and latency change.
- [ ] Benchmark: non-selective scan (> 80% selectivity) — confirm no
  material regression.
- [ ] A/B benchmark: ClickBench Q24 (SELECT * from large partition) with and
  without late materialization enabled.
- [ ] Calibrate `PARQUET_LATE_MATERIALIZATION_ABANDON_AFTER` default against
  benchmark results.

### Exit Criteria

- [ ] Part A: ClickBench 42/42 pass. No new predicate-pushdown regressions.
- [ ] Part A: IS NULL / IS NOT NULL / NOT-root predicates confirmed pushed and
  evaluated inside `ParquetReadNode` rather than as upstream Filter nodes.
- [ ] Part B: Demonstrated ≥ 30% byte reduction for selective (< 10%) wide
  scan queries.
- [ ] Part B: Non-selective (> 50% selectivity) throughput within 5% of
  pre-change baseline.
