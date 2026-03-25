# HAVING Pushdown into Aggregation Design

## Goal

Eliminate the separate `Filter` node that evaluates HAVING conditions after
aggregation.  Instead, attach the HAVING condition directly to
`DrakenAggregateAndGroupNode` and apply it chunk-by-chunk as groups are
finalised, so that groups which fail the condition are never written to the
output vectors that feed ORDER BY and LIMIT.

Targeted queries: ClickBench Q28, Q29.

---

## Prerequisite

This design depends on the HAVING aggregate expression rewrite described in
`having-aggregate-rewrite-design.md`.  That rewrite replaces `AGGREGATOR`
nodes in the HAVING condition tree with `IDENTIFIER` nodes pointing to the
already-computed aggregate column identities (e.g. `@@aggregate_0`).  Without
that rewrite, the condition tree cannot be evaluated against a finalised morsel.

The two designs are intentionally split:

| Document | Change | Effect |
|---|---|---|
| `having-aggregate-rewrite-design.md` | Rewrite AGGREGATOR → IDENTIFIER in the condition tree | Stops the runtime `NotImplementedError`; HAVING executes correctly as a post-aggregate `Filter` node |
| **this document** | Move the rewritten condition into the aggregate node itself | Eliminates the separate `Filter` node; reduces data flowing to ORDER BY / LIMIT |

---

## Background: Current Plan Structure

For a query such as Q28:

```sql
SELECT CounterID, AVG(length(URL)) AS l, COUNT(*) AS c
FROM testdata.clickbench_tiny
WHERE URL <> ''
GROUP BY CounterID
HAVING COUNT(*) > 100000
ORDER BY l DESC
LIMIT 25;
```

The logical planner currently produces:

```
Scan → Filter(WHERE) → AggregateAndGroup → Project → Filter(HAVING) → Order → Limit
```

The HAVING filter is built unconditionally as an ordinary `Filter` node at
`logical_planner.py` L511–521:

```python
_having = logical_planner_builders.build(ast_branch["Select"].get("having"))
if _having:
    having_step = LogicalPlanNode(node_type=LogicalPlanStepType.Filter)
    having_step.condition = _having
    ...
    inner_plan.add_node(step_id, having_step)
```

`predicate_pushdown.py` cannot move this node earlier because `has_agg` is
non-empty and the collector guard requires `not has_agg`.

### Why this is expensive for Q28 and Q29

`DrakenAggregateAndGroupNode._finalize_groupby` calls
`self._groupby_engine.finalize_morsels(chunk_size=CHUNK_SIZE)` which yields
result chunks covering **all** groups.  For Q29 the GROUP BY key is the
extracted domain from `REGEXP_REPLACE`, which produces many thousands of
distinct domains across the full hits dataset.  The HAVING condition
`COUNT(*) > 100000` passes only the handful of high-traffic domains.

The wasted work under the current plan:

1. `finalize_morsels` materialises every group into output vectors (key column
   strings, aggregate scalars).
2. `Project` node evaluates aliases over the full set.
3. `Filter(HAVING)` discards the vast majority of groups.
4. `Order` heap-sorts the small survivor set.

Steps 1–3 perform O(all groups) work before ORDER BY sees any data.  With
pushdown, steps 1–3 produce only the survivor groups and ORDER BY receives the
final working set directly.

---

## Proposed Plan Structure

```
Scan → Filter(WHERE) → AggregateAndGroup(having=…) → Project → Order → Limit
```

The `Filter(HAVING)` node is removed.  The condition is evaluated inside
`_finalize_groupby` on each chunk immediately after `finalize_morsels` yields
it, before the chunk is passed downstream.

---

## Implementation

### 1. Logical planner — attach HAVING to the aggregate node

**File**: `opteryx/planner/logical_planner/logical_planner.py`

Currently the `AggregateAndGroup` step is built at L352–358 and the HAVING
step at L511–521.  The change is to store the (unrewritten) HAVING condition on
the aggregate node instead of creating a separate `Filter` node.

The AGGREGATOR → IDENTIFIER rewrite is applied once, at planning time.  After
that the condition only references `IDENTIFIER` nodes that are valid in the
finalised morsel.

```python
# L352–358 (existing group_step construction, unchanged)
group_step = LogicalPlanNode(node_type=LogicalPlanStepType.AggregateAndGroup)
group_step.groups      = _groups
group_step.aggregates  = _aggregates
group_step.projection  = _projection

# L511–521 (replace the Filter-node block)
_having = logical_planner_builders.build(ast_branch["Select"].get("having"))
if _having:
    if group_step is not None:
        # Rewrite AGGREGATOR nodes → IDENTIFIER nodes now so the condition
        # can be evaluated against a finalised morsel at execution time.
        group_step.having_condition = rewrite_having_aggregates_to_identifiers(
            _having, _aggregates
        )
        # No separate Filter node is added to the plan.
    else:
        # No GROUP BY / aggregate step; fall back to the existing Filter node
        # (bare HAVING without GROUP BY is unusual but must not crash).
        having_step = LogicalPlanNode(node_type=LogicalPlanStepType.Filter)
        having_step.condition = _having
        previous_step_id, step_id = step_id, random_string()
        inner_plan.add_node(step_id, having_step)
        if previous_step_id is not None:
            inner_plan.add_edge(previous_step_id, step_id)
```

`group_step` is the local variable holding the `AggregateAndGroup` node.  It
is already in scope at the HAVING block because group-less aggregates use a
separate `Aggregate` node path (`elif len(_aggregates) > 0`).  The fallback
guard handles that case safely.

### 2. Physical planner — forward `having_condition` to the operator

**File**: `opteryx/planner/physical_planner.py` (or wherever
`DrakenAggregateAndGroupNode` is instantiated from the logical
`AggregateAndGroup` node)

Add `having_condition` to the keyword arguments forwarded to the operator:

```python
DrakenAggregateAndGroupNode(
    properties=properties,
    groups=logical_node.groups,
    aggregates=logical_node.aggregates,
    projection=logical_node.projection,
    having_condition=getattr(logical_node, "having_condition", None),
)
```

`getattr` with a default of `None` keeps the call site backward-compatible
with logical nodes built before this change and with the legacy
`AggregateAndGroupNode` fallback path.

### 3. `DrakenAggregateAndGroupNode` — accept and store the condition

**File**: `opteryx/operators/draken_aggregate_and_group_node.py`

In `__init__` (currently L114–162), accept the new parameter and store it:

```python
def __init__(self, properties: QueryProperties, **parameters):
    super().__init__(properties=properties, **parameters)
    ...
    self._having_condition = parameters.get("having_condition", None)
```

No further wiring is needed in `__init__`; the condition is already rewritten
to use `IDENTIFIER` nodes so it requires no knowledge of the aggregation
internals.

### 4. `_finalize_groupby` — apply the condition per chunk

**File**: `opteryx/operators/draken_aggregate_and_group_node.py`

Current loop (L383–385):

```python
for result in self._groupby_engine.finalize_morsels(chunk_size=CHUNK_SIZE):
    emitted += 1
    yield self._postprocess_finalized_morsel(result)
```

Updated loop:

```python
for result in self._groupby_engine.finalize_morsels(chunk_size=CHUNK_SIZE):
    if self._having_condition is not None:
        result = self._apply_having_filter(result)
        if result is None or result.num_rows == 0:
            continue
    emitted += 1
    yield self._postprocess_finalized_morsel(result)
```

Add the helper method to the class:

```python
def _apply_having_filter(self, morsel):
    """
    Evaluate the pushed-down HAVING condition against a finalised result
    chunk and return only the rows that pass.

    The condition tree was rewritten at planning time so that all
    AGGREGATOR nodes have been replaced with IDENTIFIER nodes referencing
    the aggregate output columns present in `morsel`.  Standard
    evaluate_draken / morsel.filter machinery handles the rest.
    """
    from opteryx.expression.evaluator.draken import evaluate_draken

    mask = evaluate_draken(self._having_condition, morsel)
    if mask is None:
        return morsel
    return morsel.filter(mask)
```

`morsel.filter(mask)` already exists and is used by the regular `FilterNode`
path; no new Morsel API is needed.

### 5. `predicate_pushdown.py` — guard against the now-absent Filter node

**File**: `opteryx/planner/optimizer/strategies/predicate_pushdown.py`

The pushdown strategy walks the plan looking for `Filter` nodes.  A HAVING
condition is no longer a `Filter` node when this change is active, so no
guard change is strictly required.  However, if the fallback path (bare
HAVING without GROUP BY) still emits a `Filter` node, existing behaviour is
preserved automatically because `has_agg` is non-empty and the collector guard
already blocks movement.

No change required here.

---

## Scope of Conditions Eligible for Pushdown

Any HAVING condition is eligible.  At the point finalization runs, the output
morsel contains exactly the GROUP BY key columns and all aggregate output
columns.  HAVING conditions in valid SQL can only reference those columns (any
reference to a raw input column that is not in GROUP BY is a query error caught
earlier in the binder).  There is therefore no case where a syntactically valid
HAVING condition cannot be evaluated against the finalised morsel.

Edge cases handled correctly by the existing `evaluate_draken` machinery:

| Condition form | Handled |
|---|---|
| `HAVING COUNT(*) > 100000` | ✅ single aggregate comparison |
| `HAVING AVG(x) > 5.0 AND COUNT(*) > 10` | ✅ compound condition |
| `HAVING COUNT(*) > COUNT(DISTINCT col)` | ✅ two aggregate identifiers |
| `HAVING CounterID = 62` | ✅ GROUP BY key column, present in morsel |
| `HAVING COUNT(*) * 2 > 100` | ✅ arithmetic on aggregate identifier |

---

## Interaction with ORDER BY + LIMIT Heap Sort

The `OperatorFusionStrategy` fuses `Order + Limit → HeapSort`.  With HAVING
pushdown, the heap sort receives only the groups that passed the condition.
For Q29 this is a handful of high-traffic domains rather than thousands.
Heap sort cost is O(N log k) where N is the input size and k is the LIMIT; the
win from pushing HAVING down is directly proportional to the reduction in N.

For Q28 and Q29 specifically:

| Query | Approximate groups before HAVING | Groups passing `COUNT(*) > 100000` | Reduction |
|---|---|---|---|
| Q28 (GROUP BY CounterID) | ~1 000 | < 50 | ~95 % |
| Q29 (GROUP BY extracted domain) | ~50 000+ | < 100 | ~99.8 % |

---

## Expected Gain

The gain is proportional to (1 − pass rate) × (finalization + projection +
sort work per group).

**Q28**: Low absolute gain.  CounterID has low cardinality; ~1 000 groups is
already cheap to materialise and sort.  The main benefit is eliminating the
separate `Filter` node pass (one morsel scan saved).  Expected improvement:
5–15 %.

**Q29**: High absolute gain.  Thousands of distinct extracted domains are
materialised and sorted under the current plan; only a handful survive HAVING.
Finalization of skipped groups is still paid (the Cython `finalize_morsels`
already computed the aggregate state), but output vector construction, string
column materialisation, projection evaluation, and sort input size are all
reduced by ~99 %.  Expected improvement: 30–60 % of the post-aggregation cost
(ORDER BY + projection + filter).  The dominant remaining cost is the
aggregation itself (ingesting REGEXP_REPLACE results into the hash table), which
this change does not affect.

---

## What This Does Not Address

- **Ingest-time pruning**: Groups that will fail HAVING cannot in general be
  pruned during ingestion because aggregate values grow monotonically over
  morsels (e.g. a group with COUNT 500 after morsel 1 may reach 200 000 after
  morsel N).  The only safe pruning direction would be `HAVING COUNT(*) <=
  threshold`, which is rare in practice.  Not in scope.

- **Finalize-level Cython pruning**: Passing the HAVING condition into
  `finalize_morsels` so that groups are skipped before their output vectors are
  written would save the vector-append cost for rejected groups.  This requires
  plumbing a Python callable into Cython hot path, which adds complexity
  disproportionate to the gain (vector-append cost is small relative to the hash
  table and state accumulation cost).  Not in scope.

- **High-cardinality GROUP BY (Q19, Q32)**: These queries have no HAVING clause
  and are not affected by this change.

---

## Implementation Order

1. Merge / verify `having-aggregate-rewrite-design.md` work (AGGREGATOR →
   IDENTIFIER rewrite) — prerequisite, must be stable before this change.
2. Add `having_condition` attribute to `LogicalPlanNode` for
   `AggregateAndGroup` (or rely on `getattr` duck-typing; the node class
   already uses `__dict__` storage).
3. Logical planner change — attach rewritten condition to `group_step` instead
   of emitting `Filter` node.
4. Physical planner change — forward `having_condition` to operator constructor.
5. Operator change — `__init__` storage + `_apply_having_filter` helper +
   updated `_finalize_groupby` loop.
6. Tests (see below).
7. Verify Q28 and Q29 pass in ClickBench suite and measure timing delta.

---

## Testing Strategy

### Correctness: existing behaviour preserved

```python
def test_having_count_gt():
    sql = """
        SELECT CounterID, COUNT(*) AS c
        FROM testdata.clickbench_tiny
        WHERE URL <> ''
        GROUP BY CounterID
        HAVING COUNT(*) > 100000
        ORDER BY c DESC
        LIMIT 25
    """
    result = opteryx.query(sql).fetchall()
    # All returned rows must satisfy the HAVING condition
    for row in result:
        assert row["c"] > 100000

def test_having_compound():
    sql = """
        SELECT x, COUNT(*) AS c, AVG(y) AS a
        FROM $table
        GROUP BY x
        HAVING COUNT(*) > 5 AND AVG(y) < 100.0
    """
    result = opteryx.query(sql).fetchall()
    for row in result:
        assert row["c"] > 5
        assert row["a"] < 100.0

def test_having_result_matches_without_pushdown():
    """
    Cross-check: result with pushdown must exactly match the result
    produced by the pre-pushdown plan (filter node path).
    Disable the pushdown flag, run both, compare.
    """
    ...
```

### Correctness: empty result when nothing passes

```python
def test_having_all_filtered():
    sql = "SELECT x, COUNT(*) AS c FROM $table GROUP BY x HAVING COUNT(*) > 999999999"
    result = opteryx.query(sql).fetchall()
    assert result == []
```

### Correctness: HAVING on GROUP BY key column

```python
def test_having_on_group_key():
    # CounterID is a GROUP BY column, not an aggregate
    sql = """
        SELECT CounterID, COUNT(*) AS c
        FROM testdata.clickbench_tiny
        GROUP BY CounterID
        HAVING CounterID = 62
    """
    result = opteryx.query(sql).fetchall()
    assert all(row["CounterID"] == 62 for row in result)
```

### No regression: queries without HAVING unaffected

```python
def test_group_by_no_having():
    sql = "SELECT x, COUNT(*) FROM $table GROUP BY x ORDER BY COUNT(*) DESC LIMIT 10"
    # Must run without error and produce correct results
    ...
```

### Performance: Q28 and Q29 timing

Run ClickBench benchmark before and after; record Q28 and Q29 wall-clock times.
Accept if both improve.  No regression allowed on any other query.

---

## Files Changed

| File | Change |
|---|---|
| `opteryx/planner/logical_planner/logical_planner.py` | Attach rewritten HAVING to `group_step.having_condition`; remove `Filter` node emit |
| `opteryx/planner/physical_planner.py` | Forward `having_condition` to `DrakenAggregateAndGroupNode` |
| `opteryx/operators/draken_aggregate_and_group_node.py` | Store `_having_condition`; add `_apply_having_filter`; update `_finalize_groupby` loop |

No changes to Cython, C++, or the aggregation engine internals.

---

## Status

- [ ] Prerequisite: HAVING aggregate rewrite merged and stable
- [ ] Logical planner change
- [ ] Physical planner change
- [ ] Operator change (`__init__`, `_apply_having_filter`, `_finalize_groupby`)
- [ ] Correctness tests passing
- [ ] Q28 and Q29 ClickBench timing delta recorded