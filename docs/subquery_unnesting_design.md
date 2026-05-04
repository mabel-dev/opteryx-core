# Subquery Unnesting Design

Based on: "Unnesting Arbitrary Queries" — Neumann & Kemper (TU München)

---

## Pipeline Context

The query planning pipeline runs in this order:

1. SQL Rewriter → Parser → AST Rewriter
2. **Logical Planner** — converts AST to unbound logical plan
3. **Plan Rewriter** — structural rewrites on unbound plan (before schema resolution)
4. **Binder** — resolves column references against schemas
5. **Optimizer** — cost-based optimizations on bound plan
6. Physical Planner

The decision of where a rewrite belongs turns on two questions:

- **Needs bound info?** If the rewrite requires resolved column-to-relation mappings, types, or statistics, it must go in the Optimizer. The Optimizer handles both cost-based decisions and any structural rewrite that requires a bound plan.
- **Purely structural?** If the rewrite operates only on names and tree shape — no type resolution needed — it belongs in the Plan Rewriter, which runs before the Binder.

The fixed-point loop in `rewriter.py` re-runs all strategies until no strategy modifies the plan. This means multi-level correlated subqueries are handled naturally — each pass peels one layer. The Optimizer applies strategies sequentially (no loop), so strategies there must not depend on each other's output within a single pass.

---

## Change 1: EXISTS / NOT EXISTS → Semi/Anti Join

**Classification:** Plan Rewriter — new file `plan_rewriter/strategies/exists_to_join.py`

### What changes in the logical planner

`logical_planner_builders.py:724` currently raises `UnsupportedSyntaxError` inside `exists()`. Remove the raise. Let it return the `NodeType.UNARY_OPERATOR / "EXISTS"` node with the subquery embedded. The Filter node above it carries this as its condition.

### Tree transformation

```
Input:
  Filter(condition: EXISTS(Subquery(inner_plan)))
    └─ outer_relation

Output (EXISTS, equi-correlated):
  Join(type: "left semi", on: outer.k = inner.k)
    ├─ outer_relation
    └─ Subquery(alias, inner_plan_without_correlated_predicate)

Output (NOT EXISTS):
  Join(type: "left anti", on: outer.k = inner.k)
    ├─ outer_relation
    └─ Subquery(alias, inner_plan_without_correlated_predicate)
```

### Algorithm

1. Scan Filter nodes for EXISTS/NOT EXISTS expressions in `visit()`.
2. Walk the inner subquery's Filter predicates to find **correlated predicates** — predicates that reference names not defined inside the subquery. These become the join `ON` condition.
3. If no correlated predicates exist: uncorrelated EXISTS — rewrite to a cross semi/anti join with a limit-1 subquery.
4. Strip the correlated predicates from the inner plan's Filter; wire the residual inner plan as the join's right side.
5. Any remaining outer predicates stay as a Filter above the join (same pattern as `in_subquery_to_join.py`).

### Constraints

- Non-equi correlated predicate (e.g. `e.grade > s.threshold`): raise `UnsupportedSyntaxError("EXISTS with non-equi correlation requires general decorrelation")`. No silent fallback.
- OR-joined correlated predicate: raise same error.
- Multi-column equi-correlation is fine; produces a composite join key.

---

## Change 2: NOT IN → Null-Aware Anti Join

**Classification:** Plan Rewriter — modify `plan_rewriter/strategies/in_subquery_to_join.py`

### Current state

An early guard in `in_subquery_to_join.py` (line 25) rejects NOT IN. The join type `"left anti null-aware"` already exists in the physical planner and filter-join operator.

### What changes

Remove the guard. The existing transformation path already emits `"left anti null-aware"` — it was gated off, not absent.

One correctness addition is required: inject a `Filter(y IS NOT NULL)` **inside** the subquery before it reaches the anti-join.

### NULL semantics

`x NOT IN (SELECT y FROM T WHERE ...)`:
- If the subquery returns **any NULL**, the entire NOT IN evaluates to UNKNOWN → no rows pass.
- Adding `IS NOT NULL` inside the subquery ensures nulls are excluded before the anti-join sees them.
- The null-aware anti-join then correctly handles the case where the outer join key is NULL (those rows are excluded too).

### Tree transformation

```
Input:
  Filter(x NOT IN (SELECT y FROM T WHERE ...))
    └─ outer

Output:
  Join(type: "left anti null-aware", on: outer.x = sub.y)
    ├─ outer
    └─ Subquery
        └─ Filter(y IS NOT NULL)     ← injected
            └─ original inner plan
```

### Remaining rejections (keep)

- Multi-column NOT IN: remains rejected. SQL semantics for multi-column null-aware anti-join are complex and the physical operator is untested for that case.
- `NOT IN (literal_list)`: handled by expression evaluation, not joins.

---

## Change 3: General Correlated Subquery Decorrelation

**Classification:** Optimizer — new file `optimizer/strategies/decorrelate_subquery.py` + new logical node type

This implements Neumann & Kemper §3.1 (simple unnesting) and §3.2 (general unnesting via dependent join push-down).

Free variable detection — identifying which column references inside a subquery refer to outer scope — is the core operation that drives all decorrelation. After binding, every column reference is tagged with its source relation. Detecting free variables in a bound plan is trivial: any column reference whose source relation is not in the inner plan's relation set is a free variable. Doing this pre-binding requires an explicit scope stack in the logical planner and is fragile. Therefore Change 3 lives in the Optimizer.

### New logical node: DependentJoin

Add to `LogicalPlanStepType`:

```python
DependentJoin = 57
```

Node carries:
- Left side: outer relation (the domain supplier)
- Right side: inner subquery plan (contains free variable references)
- `free_vars: set[str]` — column names from outer scope that appear in the inner plan

The `DependentJoin` is created by the logical planner and must survive through the Binder. The Binder treats the inner plan as a separate scope: it resolves inner-scope columns against the inner relations, and leaves outer-scope references unresolved (they are already tagged as free variables). The Optimizer then decorrelates the node.

### Logical planner change

When planning a correlated subquery, detect that the subquery's WHERE clause references names not available in the inner FROM clause. Emit a `DependentJoin` node instead of raising `UnsupportedSyntaxError`. The detection at this stage is name-based (not type-based): if a column name in the subquery predicate does not appear in any inner-scope relation alias, it is treated as a free variable candidate. The Binder later confirms and tags these precisely.

### Rewriter strategy: DecorrelateSubqueryStrategy

Two phases, applied in order. The fixed-point loop handles doubly-nested correlated subqueries — each pass peels one layer.

#### Phase 1 — Simple unnesting (predicate pull-up)

For each `DependentJoin(outer, inner)`:

1. Find all Filter nodes in `inner` whose predicates reference only outer free vars and inner columns in an equi-join pattern.
2. Pull those predicates up: remove them from the inner Filter, add them as the join condition on the `DependentJoin`.
3. If after pull-up `inner` has no remaining free variable references: the `DependentJoin` becomes a regular `Join`. Done for this node.

This handles the vast majority of real-world cases (TPC-H Q4, Q17, Q20, Q21, most EXISTS/IN patterns).

#### Phase 2 — General unnesting (D-based transformation)

Applied only when Phase 1 leaves a `DependentJoin` with remaining free variable references.

Implements the paper's core equivalence:

```
outer ⋈_dep inner
  ≡  outer ⋈_{outer=D} D (D ⋈_dep inner)
     where D = Π_{free_vars(inner) ∩ attrs(outer)}(outer)   [deduplicated set]
```

Then push `D ⋈_dep` down through the inner plan using these operator-specific rules:

| Inner operator | Push-down rule |
|---|---|
| `Filter(p)` | `D ⋈ Filter(p, inner')` → `Filter(p, D ⋈ inner')` |
| `Join(T1, T2)` — only T1 depends on D | `D ⋈ Join(T1, T2)` → `Join(D ⋈ T1, T2)` |
| `Join(T1, T2)` — both depend on D | `D ⋈ Join(T1, T2)` → `Join(D ⋈ T1, D ⋈ T2)` with natural join on D added to predicate |
| `LeftOuterJoin(T1, T2)` — T2 depends on D | Must replicate: `(D ⋈ T1) ⟕ (D ⋈ T2)` with natural join on D |
| `SemiJoin(T1, T2)` — T2 independent | `D ⋈ Semi(T1, T2)` → `Semi(D ⋈ T1, T2)` |
| `SemiJoin(T1, T2)` — T2 depends | `D ⋈ Semi(T1, T2)` → `Semi(D ⋈ T1, D ⋈ T2)` with natural join on D |
| `AntiJoin` | Same rules as SemiJoin |
| `GroupBy(A, f, T)` | `D ⋈ GroupBy(A, f, T)` → `GroupBy(A ∪ A(D), f, D ⋈ T)` |
| `Projection(A, T)` | `D ⋈ Projection(A, T)` → `Projection(A ∪ A(D), D ⋈ T)` |
| `Union(T1, T2)` | `D ⋈ Union(T1, T2)` → `Union(D ⋈ T1, D ⋈ T2)` |
| `Intersect / Except` | Same distribute rule as Union |
| Base table scan | `D ⋈_dep scan` → `Join(D, scan)` — dependent join eliminated, becomes regular join |

When `D ⋈_dep` reaches a base table, it becomes a regular join. All `DependentJoin` nodes are eliminated from the plan before the Physical Planner runs.

#### Optional: D-elimination (§4)

After unnesting, if the join with D is an equi-join and the joined attributes are already present elsewhere in the subtree (equivalence class analysis), replace the join with D with a `Map` operator that derives D's attributes from existing ones. This eliminates a join at the cost of potentially larger intermediates.

This optimisation is deferred: it requires equivalence class analysis and cost comparison, making it a candidate for the Optimizer phase rather than the Plan Rewriter.

### Constraints / failure modes

- Outer joins in the inner plan with the inner side depending on D: must always replicate D to both sides. More expensive but correct.
- Cyclic free variable references: cannot arise from valid SQL, but detect and raise if encountered.
- If Phase 1 cannot fully decorrelate and Phase 2 is not yet implemented: raise `UnsupportedSyntaxError("Correlated subquery requires general decorrelation — not yet supported")`. No silent fallback to nested loops.

### Implementation order

Implement Phase 1 first with explicit failure for Phase 1 misses. Phase 2 follows as a separate change. This gives partial coverage immediately without any silent degradation.

---

## Implementation Status

### Changes 1 & 2 — COMPLETE ✓

Both plan rewriter strategies have been fully implemented (commit `cf66549d`, May 3 2026):

**Change 1: EXISTS/NOT EXISTS → semi/anti joins**
- **File:** `plan_rewriter/strategies/exists_subquery_to_join.py` (318 lines)
- **What:** Rewrites `EXISTS(...)` → `LEFT SEMI JOIN`, `NOT EXISTS(...)` → `LEFT ANTI JOIN`
- **How:** Extracts equi-correlation predicates from subquery WHERE clause → join ON condition; replaces subquery projection with correlation key columns (for hash set)
- **Constraints:** Requires explicit equi-correlations; raises `UnsupportedSyntaxError` on non-equi or OR-branched correlations
- **NULL semantics:** Handled by filter_join kernels (semi excludes left nulls when right has nulls; anti is plain)

**Change 2: NOT IN → null-aware anti-join**
- **File:** `plan_rewriter/strategies/in_subquery_to_join.py` (216 lines, expanded from IN-only)
- **What:** Extends IN subquery rewrite to support NOT IN via `LEFT ANTI NULL-AWARE` join
- **How:** Line 193 sets join_type to `"left anti null-aware"` when subquery is negated
- **NULL semantics:** Correctly handled by `_anti_join_null_aware_filter()` in `filter_join.pyx:168-204`:
  - If right side contains NULL → return empty (all left rows excluded, per `NOT IN (NULL, ...) = UNKNOWN`)
  - Otherwise → return left rows not in set, excluding left nulls (`NULL NOT IN (...) = UNKNOWN`)
- **No IS NOT NULL injection needed** — the filter_join kernel proactively checks for `_NULL_HASH` in the right set (line 180)

**Supporting changes:**
- **Binder:** `planner/binder/join.py` + `_bind_on_condition_split()` resolves ambiguous identifiers when outer/subquery overlap column names
- **Logical planner:** `logical_planner_builders.py` removed the `UnsupportedSyntaxError` raise for EXISTS
- **Execution:** `operators/filter_join/filter_join.pyx` implements three join modes with correct NULL handling

### Change 3 — DEFERRED ⏳

General correlated subquery decorrelation (DependentJoin-based unnesting) not yet started. Remains as designed:
- Requires logical planner to emit `DependentJoin` node for correlated subqueries
- Requires Binder to pass `DependentJoin` through with scoped column resolution
- Requires Optimizer strategy implementing Phase 1 (simple unnesting / predicate pull-up) and Phase 2 (D-based general unnesting)

---

## Summary

| Change | Status | Location | Implemented |
|---|---|---|---|
| **EXISTS/NOT EXISTS → semi/anti** | ✓ Complete | Plan Rewriter | `exists_subquery_to_join.py` |
| **NOT IN → null-aware anti-join** | ✓ Complete | Plan Rewriter | `in_subquery_to_join.py` (expanded) + filter_join kernels |
| **General correlated decorrelation** | ⏳ Planned | Optimizer | (Not started) |

Changes 1 and 2 are production-ready. Change 3 requires logical planner, binder, and optimizer modifications.
