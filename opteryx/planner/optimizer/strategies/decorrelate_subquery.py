# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Decorrelate scalar subqueries (post-bind).

    WHERE outer.c < (SELECT AGG(x) FROM T WHERE T.k = outer.k)
->  INNER JOIN (SELECT k, AGG(x) FROM T GROUP BY k) ON outer.k = T.k
    WHERE outer.c < AGG(x)

This runs on the BOUND plan, which is what makes it correct rather than
heuristic. The binder resolves every name in the subquery against the
subquery's own scope first and the enclosing query second, tagging the latter
`is_outer_reference` (see binder.bind_correlated_subquery). So a correlation
predicate is identified by a fact — "exactly one side resolved to the outer
scope" — instead of by guessing from whether the query author happened to
write a table qualifier.

That is the whole reason this lives here and not in the plan rewriter. The
pre-bind rewriter had to infer orientation from syntax, which is undecidable
for canonical TPC-H Q17/Q20 (`WHERE l_partkey = p_partkey` — both sides bare,
no lexical signal at all).

Because the subquery's plan is already bound, its output columns carry real
schema columns and identities, so the decorrelated relation can simply be
joined to: no schema has to be synthesized and no re-binding is needed.

An UNCORRELATED scalar subquery is the same rewrite with no keys: it is one value
attached to every outer row, i.e. a cross join. It must provably yield one row
(an ungrouped aggregate), or cross-joining it would multiply the outer rows
instead of raising SQL's "more than one row returned by a subquery".

NOT handled here (each raises, never silently wrong):
  - subqueries with no aggregate (use EXISTS/IN)
  - an uncorrelated subquery that could return several rows
  - correlations that are not equalities

EXISTS is the same rewrite with a different join:

    WHERE EXISTS (SELECT 1 FROM T WHERE T.k = outer.k)
->  LEFT SEMI JOIN T ON outer.k = T.k        (NOT EXISTS -> LEFT ANTI JOIN)

It is a boolean test rather than a value, so nothing is substituted into the
predicate — the existence test IS the join, and the EXISTS node is removed. A
correlated NON-equality (`AND T.a <> outer.a`, canonical TPC-H Q21) cannot be a
join key and cannot be a post-join filter either, because SEMI/ANTI have already
collapsed the row to existence by then; it rides on the join as `residual` and is
evaluated per candidate pair inside the probe (SemiAntiProbeOperator).

`IN` is EXISTS with one more key — the membership test pairs the outer expression
with the subquery's single output column:

    WHERE x IN (SELECT y FROM T)
->  LEFT SEMI JOIN T ON x = T.y     (NOT IN -> LEFT ANTI NULL-AWARE JOIN)

`NOT IN` is null-aware and `NOT EXISTS` is not: `x NOT IN (SELECT y ...)` yields
nothing when any y is NULL, because `x <> NULL` is unknown rather than true. That
lives in the join TYPE, so it must not be "simplified" to a plain anti join.

Because a correlation simply contributes further keys, correlated `IN (SELECT ...)`
works here; the pre-bind rewrite it replaced had no correlation support at all.

Scope note: this owns EVERY subquery form that appears in a predicate — scalar,
EXISTS, IN. The plan rewriter is now purely syntactic (set operations, window
rewrites) and no longer touches subqueries.

SKIP-LEVEL correlations — a subquery nested two levels deep referencing its
GRANDPARENT scope — cannot be keys of the join built here, because the
grandparent's relation is on neither leg. They are bound on the ancestor join
that owns the relation instead (Neumann BTW2025's `Γ_{A ∪ A(D)}` rule, driven
as a walk UP the materialised plan rather than the paper's parent-pointer
recursion, because decorrelation runs outside-in and the ancestor already
exists as a node):
  - scalar: `_defer_correlation_to_ancestor` — grouping/projections on the path
    are widened by the carried column, the equality lands on the ancestor.
  - EXISTS/IN: this join is converted SEMI→INNER so the carried column can flow
    (SEMI emits left-side columns only); sound because existence absorbs the
    multiplicity (`EXISTS(o: P ∧ EXISTS(l: Q)) ≡ EXISTS((o,l): P ∧ Q)`), so it
    is only done when `_defer_existence_to_ancestor` verifies the ancestor is a
    SEMI/ANTI join with nothing multiplicity-sensitive between.

Known gaps (raise, never silently wrong):
  - UNCORRELATED `EXISTS`. Unlike the scalar case it cannot become a cross join —
    the answer is "any row at all", a zero-key semi/anti join, and the engine's
    join compiler admits zero-key only for CROSS and nested_loop.
  - An EXPRESSION on the left of `IN` (`x + 0 IN (...)`): join conditions are
    restricted to column comparisons.
  - Skip-level correlation inside NOT EXISTS / NOT IN (the inner negation blocks
    the SEMI→INNER conversion), combined with a correlated non-equality, or with
    an aggregate between it and the enclosing existence test.
  - A scalar subquery in the SELECT LIST. The logical planner refuses it before
    it reaches here (UnsupportedSyntaxError at the first walk of the projection),
    because this strategy only inspects Filter conditions and an unhandled
    SUBQUERY node in a projection used to crash the binder. Follow-on scope,
    for the architect: the rewrite is NOT the one `_decorrelate` builds. A
    WHERE-clause scalar subquery joins INNER — a missing match makes the
    comparison unknown, so dropping the row is correct. A SELECT-list scalar
    subquery is a VALUE per outer row: an outer row with no match must survive
    carrying NULL, which is a LEFT OUTER join to the same grouped relation.
    The ORDER BY ... LIMIT 1 form additionally needs the
    `_rewrite_order_limit_to_row_number` rewrite (ROW_NUMBER() OVER
    (PARTITION BY correlation keys ORDER BY sort spec), filter rn = 1) that is
    being built for WHERE-clause subqueries — same rewrite, different join type
    on top. The one-row guarantee still has to hold per partition, or the LEFT
    join multiplies outer rows instead of raising "more than one row returned".
The first two predate this strategy.
"""

from opteryx.exceptions import InvalidInternalStateError, UnsupportedSyntaxError
from opteryx.expression import NodeType, binary_operands
from opteryx.models import LogicalColumn, Node
from opteryx.planner.binder.join_helpers import extract_join_fields
from opteryx.planner.logical_planner import LogicalPlan, LogicalPlanNode, LogicalPlanStepType
from opteryx.planner.optimizer.strategies.optimization_strategy import (
    OptimizationStrategy,
    OptimizerContext,
)
from opteryx.types import logical_type as _lt
from opteryx.types.schema import SchemaColumn, mint_column_identity
from opteryx.utils import random_string


def _is_exists(node) -> bool:
    """EXISTS / NOT EXISTS — `negated` distinguishes them."""
    return (
        node is not None
        and node.node_type == NodeType.UNARY_OPERATOR
        and node.value == "Exists"
    )


def _is_in_subquery(node) -> bool:
    """`x IN (SELECT ...)` / `NOT IN`."""
    return (
        node is not None
        and node.node_type == NodeType.COMPARISON_OPERATOR
        and node.value == "InSubQuery"
    )


def _find_exists(condition):
    """Locate the first EXISTS node, as (node, replace_fn)."""
    return _find(condition, _is_exists)


def _find_in(condition):
    """Locate the first IN-subquery node, as (node, replace_fn)."""
    return _find(condition, _is_in_subquery)


def _find_subquery(condition):
    """
    Locate the first SCALAR subquery node, as (node, replace_fn).

    EXISTS and IN also wrap a SUBQUERY node, but those are not scalar — they are
    a boolean test, not a value. Descending into them would treat the same node
    as both, so they are skipped here and matched by `_find_exists` instead.
    """
    return _find(condition, lambda n: n.node_type == NodeType.SUBQUERY)


def _find(condition, predicate):
    """
    First node satisfying `predicate`, with a callable that replaces it in place.

    Returns (node, replace_fn) or (None, None). `replace_fn(new)` returns the new
    root of the expression, which matters when the match IS the root.
    """
    if condition is None:
        return None, None

    if predicate(condition):
        return condition, lambda new: new

    # An EXISTS/IN node owns the subquery beneath it; never look inside one while
    # searching for something else.
    if _is_exists(condition) or _is_in_subquery(condition):
        return None, None

    for attr in ("left", "right", "centre"):
        child = getattr(condition, attr, None)
        if child is None:
            continue
        found, replace_child = _find(child, predicate)
        if found is not None:

            def _replace(new, _c=condition, _a=attr, _rc=replace_child):
                setattr(_c, _a, _rc(new))
                return _c

            return found, _replace

    for index, param in enumerate(getattr(condition, "parameters", None) or []):
        found, replace_param = _find(param, predicate)
        if found is not None:

            def _replace(new, _c=condition, _i=index, _rc=replace_param):
                _c.parameters[_i] = _rc(new)
                return _c

            return found, _replace

    return None, None


def _is_outer(node) -> bool:
    return bool(node is not None and node.is_outer_reference)


def _unwrap(node):
    """
    Strip NESTED wrappers.

    Parentheses survive into the expression tree as NESTED nodes, so a predicate
    the user bracketed looks structurally different from the identical unbracketed
    one. Every structural match here has to see through them — BI tools bracket
    heavily, and medicare1's correlations arrive as `((a = b) OR (a IS NULL AND
    b IS NULL))`, NESTED at all three levels.
    """
    while node is not None and node.node_type == NodeType.NESTED:
        node = node.centre
    return node


def _column_identity(node):
    schema_column = getattr(node, "schema_column", None)
    return getattr(schema_column, "identity", None)


def _match_null_safe_eq(condition):
    """
    Recognise a null-safe equality spelled as an OR, and return its `Eq` part.

        (a = b) OR (a IS NULL AND b IS NULL)

    This is how Tableau (and several BI tools) emit `IS NOT DISTINCT FROM`, and it
    is the shape every correlated EXISTS in the medicare1 benchmark uses. Without
    this it reads as a bare OR, no correlation is found, and the query is rejected.

    Only matched when both branches name the SAME pair of columns, so an unrelated
    disjunction is never mistaken for one.

    NOTE: the returned key is a plain equality, so two NULL keys do NOT match — the
    `IS NULL AND IS NULL` branch is not reproduced by the join. See the module
    docstring; this mirrors what the pre-bind rewrite did.
    """
    condition = _unwrap(condition)
    if condition is None or condition.node_type != NodeType.OR:
        return None

    or_left, or_right = binary_operands(condition)
    equality, null_test = _unwrap(or_left), _unwrap(or_right)
    if equality is None or null_test is None:
        return None
    if equality.node_type != NodeType.COMPARISON_OPERATOR or equality.value != "Eq":
        equality, null_test = null_test, equality
    if equality.node_type != NodeType.COMPARISON_OPERATOR or equality.value != "Eq":
        return None
    if null_test.node_type != NodeType.AND:
        return None

    def _is_null_check(node):
        return (
            node is not None
            and node.node_type == NodeType.UNARY_OPERATOR
            and node.value == "IsNull"
        )

    left_null, right_null = _unwrap(null_test.left), _unwrap(null_test.right)
    if not (_is_null_check(left_null) and _is_null_check(right_null)):
        return None

    def _operand(node):
        operand = node.centre
        if operand is None:
            parameters = node.parameters or []
            operand = parameters[0] if parameters else None
        return _unwrap(operand)

    equality_columns = {_column_identity(equality.left), _column_identity(equality.right)}
    null_columns = {_column_identity(_operand(left_null)), _column_identity(_operand(right_null))}
    if None in equality_columns or equality_columns != null_columns:
        return None

    return equality


def _split_correlations(condition):
    """
    Split a conjunction into (correlations, remaining).

    A correlation is an equality with exactly one side resolved to the enclosing
    scope. Everything else stays inside the subquery.
    """
    condition = _unwrap(condition)
    if condition is None:
        return [], None

    null_safe = _match_null_safe_eq(condition)
    if null_safe is not None and _is_outer(null_safe.left) != _is_outer(null_safe.right):
        return [null_safe], None

    if condition.node_type == NodeType.AND:
        and_left, and_right = binary_operands(condition)
        left_corr, left_rest = _split_correlations(_unwrap(and_left))
        right_corr, right_rest = _split_correlations(_unwrap(and_right))
        if left_rest is None:
            remaining = right_rest
        elif right_rest is None:
            remaining = left_rest
        else:
            remaining = Node(node_type=NodeType.AND, do_not_create_column=True)
            remaining.left = left_rest
            remaining.right = right_rest
        return left_corr + right_corr, remaining

    if condition.node_type == NodeType.COMPARISON_OPERATOR and condition.value == "Eq":
        eq_left, eq_right = binary_operands(condition)
        if _is_outer(eq_left) != _is_outer(eq_right):
            return [condition], None

    return [], condition


def _find_outer_reference(node):
    """The first still-correlated column reference under `node`, or None."""
    if node is None:
        return None
    if _is_outer(node):
        return node
    for attr in ("left", "right", "centre"):
        found = _find_outer_reference(getattr(node, attr, None))
        if found is not None:
            return found
    for param in getattr(node, "parameters", None) or []:
        found = _find_outer_reference(param)
        if found is not None:
            return found
    return None


def _split_outer_referencing(condition):
    """
    Split a conjunction into (correlated, local).

    `correlated` is the conjunction of conjuncts that reference the enclosing
    scope; `local` is the conjunction of those that do not. Either may be None.

    A conjunct that reads only the subquery's own columns filters the inner
    relation and belongs INSIDE it, where predicate pushdown can reach the scan.
    Only what still points outwards has to ride on the join as a residual,
    because SEMI/ANTI evaluate it per candidate pair.
    """
    condition = _unwrap(condition)
    if condition is None:
        return None, None

    if condition.node_type == NodeType.AND:
        and_left, and_right = binary_operands(condition)
        left_corr, left_local = _split_outer_referencing(_unwrap(and_left))
        right_corr, right_local = _split_outer_referencing(_unwrap(and_right))

        def _conjoin(left, right):
            if left is None:
                return right
            if right is None:
                return left
            joined = Node(node_type=NodeType.AND, do_not_create_column=True)
            joined.left = left
            joined.right = right
            return joined

        return _conjoin(left_corr, right_corr), _conjoin(left_local, right_local)

    if _find_outer_reference(condition) is None:
        return None, condition
    return condition, None


def _reject_residual_correlation(plan: LogicalPlan) -> None:
    """
    Fail loudly if the subquery still depends on the outer row after the equi
    correlations have been lifted out.

    Only equalities can become join keys. A correlation like
    `WHERE t.a > outer.b` stays inside the subquery, where `outer.b` is out of
    scope the moment the subquery becomes the join's right-hand relation. Left
    alone that surfaces much later as an opaque "expression references column X
    which the stream does not carry", so it is caught here with a description of
    the actual limitation.
    """
    for _nid, node in plan.nodes(True):
        for candidate in (node.condition, *(node.columns or [])):
            found = _find_outer_reference(candidate)
            if found is not None:
                raise UnsupportedSyntaxError(
                    "A correlated scalar subquery can only be decorrelated on equality "
                    f"correlations; `{found.source_column}` correlates to the outer query "
                    "through a non-equality predicate, which is not supported."
                )


def _aggregate_node(plan: LogicalPlan):
    for nid, node in plan.nodes(True):
        if node.node_type in (
            LogicalPlanStepType.Aggregate,
            LogicalPlanStepType.AggregateAndGroup,
        ):
            return nid, node
    return None, None


def _subplan_rooted_at(plan: LogicalPlan, root_nid: str) -> LogicalPlan:
    """
    Extract the subtree feeding `root_nid` (inclusive) as a standalone plan.

    The nodes are the SAME objects as in `plan` — `copy_sub_plan` deep-copies them
    on the way out, so nothing here may be mutated before that happens.
    """
    sub = LogicalPlan()
    seen: set = set()
    stack = [root_nid]
    while stack:
        nid = stack.pop()
        if nid in seen:
            continue
        seen.add(nid)
        sub.add_node(nid, plan[nid])
        for child, _target, _relation in plan.ingoing_edges(nid):
            stack.append(child)
    for nid in seen:
        for child, _target, relation in plan.ingoing_edges(nid):
            if child in seen:
                sub.add_edge(child, nid, relation)
    return sub


def _is_restricted(plan: LogicalPlan) -> bool:
    """
    Is this subtree provably NARROWER than the relations it reads?

    The reducer's whole value is that the outer leg emits FEWER keys than the inner
    relation holds. With nothing narrowing it the key set is the full domain and the
    reducer is pure added cost — a scan, a hash build and a probe, to eliminate
    nothing.

    ⛔ A CROSS JOIN anywhere disqualifies the leg outright, and that is the load-bearing
    half of this test. This strategy runs FIRST (position 1): JoinPlanning is 15 and
    PredicatePushdown is 16, so at this point a multi-relation FROM is still a chain of
    unrestricted cross joins with every predicate sitting in the Filter node above.
    Copying that as a reducer duplicates a cartesian product to avoid a scan. TPC-H Q21
    is exactly this shape — nation × orders × lineitem × supplier, no predicates below
    the Filter at all — and it reached here with a `left semi` already grafted by an
    earlier round, so "contains a semi join" alone said yes to a cross-join tree.

    The restriction therefore has to be STRUCTURAL (a filter already below the join, or
    an existence test), because no statistics exist yet to measure a cost with.
    """
    narrowed = False
    for _nid, node in plan.nodes(True):
        if node.node_type == LogicalPlanStepType.Join:
            if node.type == "cross join":
                return False
            if node.type in ("left semi", "left anti", "right semi", "right anti"):
                narrowed = True
        elif node.node_type == LogicalPlanStepType.Filter:
            narrowed = True
    return narrowed


def _graft_key_reducer(plan: LogicalPlan, filter_nid, inner_plan, local_pairs, target_nid) -> bool:
    """
    Restrict a decorrelated subquery's input to keys the outer query can consume.

    Decorrelation WIDENS a correlated subquery. The original is evaluated once per
    outer binding, so it only ever sees keys the outer query holds; the rewrite reads
    the whole inner relation instead and lets the join above throw the excess away.
    On TPC-H Q20 that is 5,441,669 groups built to serve 58,782; on Q21 it is two full
    60M-row scans of `lineitem` hashed to answer EXISTS over 698,530 orderkeys.

    This grafts a SEMI join below `target_nid` against a fresh copy of the outer leg,
    so only reachable keys survive. The copy is what makes it possible: the outer leg
    is consumed by the join above and plans here are trees, not DAGs, so there is
    nothing to share.

    Sound for every join type this is called for. The reducer keeps exactly the inner
    rows whose key appears on the outer side, and an inner row whose key appears on
    neither side can match no outer row — so it can change no SEMI result, no ANTI
    result and no group the join above reads. It is a NECESSARY condition only; the
    join above still does the exact matching, including any residual.

    Returns True if a reducer was grafted.
    """
    from opteryx.planner.relation_resolver import copy_sub_plan
    from opteryx.planner.relation_resolver import rename_relations

    providers = list(inner_plan.ingoing_edges(target_nid))
    if len(providers) != 1:
        return False

    outer_roots = [provider for provider, _t, _r in plan.ingoing_edges(filter_nid)]
    if len(outer_roots) != 1:
        return False

    outer_subplan = _subplan_rooted_at(plan, outer_roots[0])
    if not _is_restricted(outer_subplan):
        return False

    # Fresh node ids AND fresh relation aliases/uuids. Without the rename both copies
    # claim the same relation names, and the join above — whose legs are resolved BY
    # NAME — can no longer tell which side a key belongs to.
    reducer_source = copy_sub_plan(outer_subplan)
    scans_before = {
        nid: node.alias
        for nid, node in reducer_source.nodes(True)
        if node.node_type in (LogicalPlanStepType.Scan, LogicalPlanStepType.FunctionDataset)
        and node.alias
    }
    rename_relations(reducer_source)
    alias_map = {
        old: reducer_source[nid].alias for nid, old in scans_before.items()
    }

    # Node ids survive the rename, so the pre/post alias pairing above is exact.
    # Column IDENTITIES are deliberately NOT re-minted: they are what lets the
    # copied key be located, and a LEFT SEMI join emits its LEFT side only, so no
    # copied column is ever visible above this join to collide with its original.
    on_condition = None
    join_columns: list = []
    for inner_key, outer_key in local_pairs:
        copied_key = _local_copy(outer_key)
        copied_key.source = alias_map.get(outer_key.source, outer_key.source)
        equals = Node(
            node_type=NodeType.COMPARISON_OPERATOR, value="Eq", do_not_create_column=True
        )
        equals.left = _local_copy(inner_key)
        equals.right = copied_key
        join_columns.extend((_local_copy(inner_key), copied_key))
        if on_condition is None:
            on_condition = equals
        else:
            conjunction = Node(node_type=NodeType.AND, do_not_create_column=True)
            conjunction.left = on_condition
            conjunction.right = equals
            on_condition = conjunction

    if on_condition is None:
        return False

    left_relations, left_schemas = _collect_relations(inner_plan, providers[0][0])
    reducer_exit = reducer_source.get_exit_points()[0]
    inner_plan += reducer_source
    right_relations, right_schemas = _collect_relations(inner_plan, reducer_exit)

    reducer = LogicalPlanNode(node_type=LogicalPlanStepType.Join)
    reducer.type = "left semi"
    reducer.on = on_condition
    reducer.using = None
    reducer.columns = join_columns
    reducer.left_relation_names = sorted(left_relations)
    reducer.right_relation_names = sorted(right_relations)
    reducer.all_relations = left_relations | right_relations
    reducer.schemas = {**left_schemas, **right_schemas}
    reducer.left_columns, reducer.right_columns = extract_join_fields(
        on_condition, reducer.left_relation_names, reducer.right_relation_names
    )
    # Same guard as the decorrelating joins: a key naming neither leg is the
    # silent-wrong-answer case, not something to push on through.
    if len(reducer.left_columns) != len(local_pairs) or len(reducer.right_columns) != len(
        local_pairs
    ):
        return False

    reducer_nid = random_string()
    inner_plan.insert_node_before(reducer_nid, reducer, target_nid)
    inner_plan.add_edge(reducer_exit, reducer_nid)
    return True


def _reduce_aggregate_input(plan: LogicalPlan, filter_nid, inner_plan, local_pairs) -> bool:
    """Reduce a decorrelated scalar subquery — the aggregate is the thing to protect."""
    aggregate_nid, aggregate = _aggregate_node(inner_plan)
    if aggregate_nid is None or aggregate.node_type != LogicalPlanStepType.AggregateAndGroup:
        return False
    return _graft_key_reducer(plan, filter_nid, inner_plan, local_pairs, aggregate_nid)


# NOTE: there is deliberately no reducer for the EXISTS / IN (SEMI/ANTI) path here.
# Their build side is the expensive one — TPC-H Q21 hashes 60M `lineitem` rows twice to
# answer a question about 698,530 orderkeys — but the reducer cannot be BUILT at this
# position: the outer leg of an EXISTS is still an unrestricted chain of cross joins,
# with every predicate in the Filter node above (see `_is_restricted`). Reducing those
# needs a strategy that runs after PredicatePushdown (position 16), where the leg is
# actually narrow. Not attempted here rather than shipped as a path that cannot fire.


def _expose_key(plan: LogicalPlan, key_column) -> None:
    """
    Group the subquery's aggregate by `key_column` and project it, so the
    decorrelated relation emits one row per correlation key and the join can
    match on it.
    """
    _, aggregate = _aggregate_node(plan)
    if aggregate is None:
        raise UnsupportedSyntaxError(
            "Correlated scalar subquery could not be decorrelated: it has no aggregate "
            "and is not `ORDER BY ... LIMIT 1`. Rewrite using **EXISTS** or IN."
        )

    if aggregate.node_type == LogicalPlanStepType.Aggregate:
        # An ungrouped Aggregate becomes grouped. AggregateAndGroup additionally
        # requires `projection` (its output column list, aggregates + keys) — a
        # plain Aggregate carries no such attribute, so it has to be built here or
        # the physical node fails with KeyError('projection').
        aggregate.node_type = LogicalPlanStepType.AggregateAndGroup
        aggregate.groups = [key_column]
        aggregate.projection = list(aggregate.aggregates or []) + [key_column]
    else:
        aggregate.groups = list(aggregate.groups or []) + [key_column]
        aggregate.projection = list(aggregate.projection or []) + [key_column]

    aggregate.columns = list(aggregate.columns or []) + [key_column]

    # The key has to survive every projection above the aggregate or it is not
    # visible to the join.
    for _nid, node in plan.nodes(True):
        if node.node_type == LogicalPlanStepType.Project:
            node.columns = list(node.columns or []) + [key_column]


def _carry_column_upward(node, column) -> None:
    """
    Make `column` survive one operator on the way to the ancestor join.

    Grouping and projection both drop whatever they do not name, so a correlation
    that can only be enforced further up has to be threaded through every one of
    them in between. Widening a GROUP BY is the `Γ_{A ∪ A(D)}` rule from Neumann
    BTW2025: the inner result is computed once per binding of the free variable
    instead of once overall.

    Widening alone is exactly the bug this fixes — it MULTIPLIES rows. It is only
    sound because the ancestor join then binds the column; the two changes are a
    pair and neither is correct without the other.
    """
    if node.node_type == LogicalPlanStepType.AggregateAndGroup:
        node.groups = list(node.groups or []) + [column]
        node.projection = list(node.projection or []) + [column]
        node.columns = list(node.columns or []) + [column]
    elif node.node_type == LogicalPlanStepType.Aggregate:
        # Same promotion `_expose_key` performs: a plain Aggregate has no
        # `projection`, and the physical node fails with KeyError without one.
        node.node_type = LogicalPlanStepType.AggregateAndGroup
        node.groups = [column]
        node.projection = list(node.aggregates or []) + [column]
        node.columns = list(node.columns or []) + [column]
    elif node.node_type == LogicalPlanStepType.Project:
        node.columns = list(node.columns or []) + [column]


def _attach_correlation(join, inner_key, outer_key, carried, outer_on_left: bool) -> None:
    """Bind a deferred correlation onto the ancestor join that owns its outer relation."""
    outer_reference = _local_copy(outer_key)
    equals = Node(node_type=NodeType.COMPARISON_OPERATOR, value="Eq", do_not_create_column=True)
    equals.left = outer_reference
    equals.right = carried

    if join.on is None:
        join.on = equals
    else:
        conjunction = Node(node_type=NodeType.AND, do_not_create_column=True)
        conjunction.left = join.on
        conjunction.right = equals
        join.on = conjunction

    # Appended directly rather than re-deriving with `extract_join_fields`: that
    # helper assumes the condition only names this join's two legs, and the whole
    # reason we are here is that the carried column's relation was not registered
    # on either of them until now.
    outer_identity = outer_key.schema_column.identity
    inner_identity = inner_key.schema_column.identity
    left_new, right_new = (
        (outer_identity, inner_identity) if outer_on_left else (inner_identity, outer_identity)
    )
    join.left_columns = list(join.left_columns or []) + [left_new]
    join.right_columns = list(join.right_columns or []) + [right_new]
    join.columns = list(join.columns or []) + [outer_reference, carried]

    carried_source = carried.source
    if carried_source:
        if outer_on_left:
            names = list(join.right_relation_names or [])
            if carried_source not in names:
                join.right_relation_names = sorted(names + [carried_source])
        else:
            names = list(join.left_relation_names or [])
            if carried_source not in names:
                join.left_relation_names = sorted(names + [carried_source])
        join.all_relations = set(join.all_relations or set()) | {carried_source}


def _defer_correlation_to_ancestor(plan: LogicalPlan, from_nid: str, inner_key, outer_key) -> bool:
    """
    Enforce a correlation whose outer column belongs to a scope further out than
    the subquery immediately enclosing it.

    Decorrelation runs outside-in over fixed-point rounds, so by the time an inner
    subquery is processed the join owning the grandparent relation already exists
    as a plan node. Rather than carrying a parent pointer down a recursion (the
    formulation in Neumann BTW2025 §3.2), we walk UP to that materialised join,
    widening each grouping and projection on the path so the inner column survives,
    and bind the equality there.

    Returns False if no such ancestor is reachable, leaving the caller to reject
    the query — a correlation that is carried but never bound is silently wrong.
    """
    carried = _local_copy(inner_key)
    target_relation = outer_key.source
    if not target_relation:
        return False

    current = from_nid
    while True:
        consumers = plan.outgoing_edges(current)
        if not consumers:
            return False
        _source, consumer_nid, _relation = consumers[0]
        consumer = plan[consumer_nid]
        if consumer is None:
            return False

        if consumer.node_type == LogicalPlanStepType.Join:
            outer_on_left = target_relation in (consumer.left_relation_names or [])
            if outer_on_left or target_relation in (consumer.right_relation_names or []):
                _attach_correlation(consumer, inner_key, outer_key, carried, outer_on_left)
                return True
            # A different join sits between us and the owner. Threading a column
            # through it means registering the relation on the intervening legs
            # too; unverified, so refuse rather than guess.
            return False

        _carry_column_upward(consumer, carried)
        current = consumer_nid


def _rewrite_order_limit_to_row_number(inner_plan: LogicalPlan, key_pairs) -> bool:
    """
    Turn a correlated `ORDER BY x LIMIT 1` subquery into a per-binding top-1.

    A global sort-and-limit is wrong once the subquery is decorrelated: the limit
    must apply per value of the correlation key, not once overall (Neumann BTW2025
    §4.4). The rewrite replaces Order with a ranking Window — ROW_NUMBER() OVER
    (PARTITION BY <correlation keys> ORDER BY <the sort spec>) — with the
    operator's native `top_k` set to 1, and simply deletes the Limit.

    `top_k` is used instead of the paper's `Filter rn <= 1` deliberately. A rank
    filter node is an ordinary comparison, so PredicatePushdownStrategy collects
    it, cannot place it below the Window barrier, and "restores" it with
    insert_node_before against the enclosing join — which moves BOTH of the
    join's legs under the filter and leaves the join one-legged (the engine
    refuses: "a join without labelled left/right legs"). Setting `top_k`
    directly emits the plan WindowTopKFusionStrategy would have fused to anyway,
    with nothing for other passes to pick up.

    Only `LIMIT 1` with no OFFSET qualifies — that is what makes the subquery
    provably one-row-per-binding, which is the scalar contract. Anything else
    returns False and the caller falls back to the aggregate requirement.

    The plan is bound but NOT yet optimized when this runs (decorrelation is the
    first strategy), so Order and Limit are still discrete nodes — LimitPushdown
    and the TopN fusions have not run.
    """
    limit_nid = None
    order_nid = None
    for nid, node in inner_plan.nodes(True):
        if node.node_type == LogicalPlanStepType.Limit:
            if limit_nid is not None:
                return False  # two limits — not the shape this handles
            limit_nid = nid
        elif node.node_type == LogicalPlanStepType.Order:
            if order_nid is not None:
                return False
            order_nid = nid

    if limit_nid is None or order_nid is None:
        return False
    limit_node = inner_plan[limit_nid]
    if limit_node.limit != 1 or limit_node.offset:
        return False
    # The Limit must consume the Order directly — anything between (an aggregate,
    # a distinct) means the limit is not a plain top-1 over the sort.
    providers = [source for source, _t, _r in inner_plan.ingoing_edges(limit_nid)]
    if providers != [order_nid]:
        return False

    order_by = inner_plan[order_nid].order_by or []

    # --- Order becomes the ranking window ------------------------------------
    # Post-bind construction, so this does the binder's work too (visit_window):
    # `outputs` pre-mints the row-number schema column, `window_functions` is the
    # (kind, identity) list the physical operator executes.
    rn_relation = f"$rownum-{random_string(6)}"
    rn_schema_column = SchemaColumn(
        name="$row_number",
        column_type=_lt.INT64,
        identity=mint_column_identity(rn_relation, "$row_number"),
    )
    rn_reference = LogicalColumn(
        node_type=NodeType.IDENTIFIER,
        source=rn_relation,
        source_column=rn_schema_column.name,
        schema_column=rn_schema_column,
    )

    window = LogicalPlanNode(node_type=LogicalPlanStepType.Window)
    window.partition_by = [_local_copy(inner_key) for inner_key, _outer_key in key_pairs]
    window.order_by = list(order_by)
    window.outputs = [("ROW_NUMBER", rn_schema_column)]
    window.output_relation = rn_relation
    window.window_functions = [("ROW_NUMBER", rn_schema_column.identity)]
    window.top_k = 1  # the operator keeps only rank-1 rows per partition
    # Everything the window READS as well as what it emits: projection pushdown
    # harvests referenced identities from `columns`, and the sort key is typically
    # referenced by nothing else — omit it and the pass prunes it off the Project
    # below, leaving the window ordering by a column the stream does not carry.
    window.columns = (
        [rn_reference]
        + list(window.partition_by)
        + [_local_copy(column) for column, _ascending in order_by]
    )
    inner_plan[order_nid] = window

    # The Limit's row-count job is done by top_k, per partition. Removing it with
    # heal makes the Window the subquery's exit.
    inner_plan.remove_node(limit_nid, heal=True)

    return True


def _expose_key_without_aggregate(plan: LogicalPlan, key_column) -> None:
    """
    The projection-widening half of `_expose_key`, for the window-rewritten shape:
    there is no aggregate to group, but the key still has to survive every Project
    above the scan or the join cannot see it.
    """
    for _nid, node in plan.nodes(True):
        if node.node_type == LogicalPlanStepType.Project:
            node.columns = list(node.columns or []) + [key_column]


def _defer_existence_to_ancestor(plan: LogicalPlan, from_nid: str, inner_key, outer_key) -> bool:
    """
    Bind a skip-level correlation from inside an EXISTS/IN subquery on the
    ancestor SEMI/ANTI join that owns its outer relation.

    The caller has already converted the subquery's own semi join to a plain
    inner join so the carried column can flow — SEMI emits left-side columns
    only. That conversion changes multiplicity, and it is only sound because
    existence absorbs it: `EXISTS(o: P ∧ EXISTS(l: Q))` ≡ `EXISTS((o,l): P ∧ Q)`,
    and likewise for NOT EXISTS around it. So this walk is STRICTER than the
    scalar `_defer_correlation_to_ancestor`: only Filter and Project may sit on
    the path — an Aggregate between would have its COUNT/SUM inflated by the
    conversion, and widening it (the scalar rule) cannot repair rows that were
    already duplicated below it — and the ancestor must itself be a SEMI or
    ANTI join, since only those collapse the multiplicity again. `left anti
    null-aware` is excluded: the engine's null-aware anti join is single-column.

    Returns False if the shape does not qualify; the caller must then refuse
    the query — the conversion has already happened, so continuing would be
    silently wrong.
    """
    carried = _local_copy(inner_key)
    target_relation = outer_key.source
    if not target_relation:
        return False

    current = from_nid
    while True:
        consumers = plan.outgoing_edges(current)
        if not consumers:
            return False
        _source, consumer_nid, _relation = consumers[0]
        consumer = plan[consumer_nid]
        if consumer is None:
            return False

        if consumer.node_type == LogicalPlanStepType.Join:
            if consumer.type not in ("left semi", "left anti"):
                return False
            if target_relation not in (consumer.left_relation_names or []):
                return False
            _attach_correlation(consumer, inner_key, outer_key, carried, outer_on_left=True)
            return True

        if consumer.node_type == LogicalPlanStepType.Filter:
            pass
        elif consumer.node_type == LogicalPlanStepType.Project:
            consumer.columns = list(consumer.columns or []) + [carried]
        else:
            # Aggregate, Distinct, Limit, ... — multiplicity-sensitive. Refuse.
            return False
        current = consumer_nid


def _projecting_node(plan: LogicalPlan):
    """
    The node in `plan` that defines its output columns.

    Not necessarily the exit: DISTINCT, HAVING, LIMIT and ORDER BY all sit above the
    projection and pass it through. Returns None if nothing projects.
    """
    projecting = (
        LogicalPlanStepType.Project,
        LogicalPlanStepType.Aggregate,
        LogicalPlanStepType.AggregateAndGroup,
        LogicalPlanStepType.Union,
    )
    frontier = [plan.get_exit_points()[0]]
    seen: set = set()
    while frontier:
        nid = frontier.pop(0)
        if nid in seen:
            continue
        seen.add(nid)
        candidate = plan[nid]
        if candidate is not None and candidate.node_type in projecting:
            return candidate
        frontier.extend(child for child, _t, _r in plan.ingoing_edges(nid))
    return None


def _output_column(plan: LogicalPlan):
    """
    The schema column a scalar subquery evaluates to.

    This is the column its TOP node emits, which is not necessarily the aggregate:
    `SELECT 0.2 * AVG(x)` projects an arithmetic expression over the aggregate, and
    substituting the bare AVG for it would silently drop the `0.2 *` and answer a
    different question. Canonical TPC-H Q17 and Q20 are both this shape.

    Must be read BEFORE the correlation key is appended to the projection.
    """
    # The exit is not always the node that DEFINES the output. `SELECT DISTINCT x`
    # exits through a Distinct, and `GROUP BY ... HAVING ...` exits through the
    # HAVING Filter (canonical TPC-H Q18). Those are row filters/reshapers that pass
    # their input's columns through.
    #
    # Crucially, `node.columns` does NOT mean the same thing on every node: on a
    # Project it is the OUTPUT list, but on a Filter it is the columns the predicate
    # READS. Trusting it anywhere would pick Q18's `l_quantity` (from
    # `HAVING SUM(l_quantity) > 300`) as the subquery's output instead of
    # `l_orderkey`. So only nodes that genuinely define a projection are consulted.
    node = _projecting_node(plan)

    if node is not None and node.node_type in (
        LogicalPlanStepType.Aggregate,
        LogicalPlanStepType.AggregateAndGroup,
    ):
        columns = list(node.projection or node.aggregates or [])
    else:
        columns = list(node.columns or []) if node is not None else []
    if not columns:
        _, aggregate = _aggregate_node(plan)
        if aggregate is None or not aggregate.aggregates:
            raise UnsupportedSyntaxError(
                "Correlated scalar subquery could not be decorrelated: it produces no column."
            )
        return aggregate.aggregates[0].schema_column
    if len(columns) != 1:
        raise UnsupportedSyntaxError(
            f"A scalar subquery must return exactly one column, this one returns {len(columns)}."
        )
    return columns[0].schema_column


def _collect_relations(plan: LogicalPlan, root_nid: str):
    """
    Walk a subtree and gather (relation names, {name: schema}) for the relations
    it exposes.

    A Join created after binding still has to look like a bound Join: downstream
    strategies read `schemas` / `left_relation_names` / `right_relation_names`
    off it. The binder builds those from its BindingContext, which does not exist
    here, so they are reconstructed from the already-bound nodes underneath.
    """
    relations: set = set()
    schemas: dict = {}
    stack = [root_nid]
    seen: set = set()
    while stack:
        nid = stack.pop()
        if nid in seen:
            continue
        seen.add(nid)
        node = plan[nid]
        schema = node.schema
        if schema is not None:
            name = node.alias or node.relation or schema.name
            schemas[name] = schema
            relations.add(name)
        for child, _target, _relation in plan.ingoing_edges(nid):
            stack.append(child)
    return relations, schemas


def _reference_to(column) -> LogicalColumn:
    """
    A bound IDENTIFIER referring to an already-bound schema column.

    `source` matters: predicate pushdown attributes a join condition to a leg by
    the column's relation, and a reference without one is treated as belonging to
    neither — the ON condition is then pulled apart and the join is left with
    `on = None`.
    """
    return LogicalColumn(
        node_type=NodeType.IDENTIFIER,
        source_column=column.name,
        source=(column.origin[0] if column.origin else None),
        schema_column=column,
    )


def _local_copy(column) -> LogicalColumn:
    """
    Copy a column reference, clearing the correlated marker.

    Once the subquery boundary is gone the reference is an ordinary column of one
    of the join's legs, so leaving it flagged would make a later pass treat it as
    still pointing out of scope.
    """
    local = column.copy()
    local.is_outer_reference = False
    local.outer_relation = None
    return local


def _has_work(condition) -> bool:
    return any(
        finder(condition)[0] is not None
        for finder in (_find_subquery, _find_exists, _find_in)
    )


class DecorrelateSubqueryStrategy(OptimizationStrategy):
    def should_i_run(self, plan: LogicalPlan) -> bool:
        return any(
            node.node_type == LogicalPlanStepType.Filter and _has_work(node.condition)
            for _, node in plan.nodes(True)
        )

    def visit(self, node: LogicalPlanNode, context: OptimizerContext) -> OptimizerContext:
        if not context.optimized_plan:
            context.optimized_plan = context.pre_optimized_tree.copy()

        if node.node_type == LogicalPlanStepType.Filter and _has_work(node.condition):
            context.collected_decorrelations.append(context.node_id)

        return context

    # Each pass removes at least one subquery, so this only bounds a rewrite that
    # fails to make progress. Far above any real query's nesting depth.
    MAX_ROUNDS = 100

    def complete(self, plan: LogicalPlan, context: OptimizerContext) -> LogicalPlan:
        # Graph inserts are deferred to complete(): mutating during traversal
        # corrupts the walk.
        #
        # Re-scan rather than trusting what visit() collected. A subquery's own plan
        # can contain further subqueries (canonical TPC-H Q20 nests IN inside IN
        # inside a scalar subquery); grafting it in brings those Filter nodes into
        # this plan AFTER the traversal that collected candidates, so they would
        # otherwise never be rewritten and a SUBQUERY node would reach the engine.
        for _round in range(self.MAX_ROUNDS):
            targets = [
                nid
                for nid, node in plan.nodes(True)
                if node.node_type == LogicalPlanStepType.Filter and _has_work(node.condition)
            ]
            if not targets:
                break
            plan = self._rewrite_filters(plan, targets)
        else:
            raise InvalidInternalStateError(
                f"subquery decorrelation did not converge after {self.MAX_ROUNDS} rounds"
            )
        context.collected_decorrelations = []
        return plan

    def _rewrite_filters(self, plan: LogicalPlan, filter_nids) -> LogicalPlan:
        for filter_nid in filter_nids:
            # One predicate can hold several subqueries (`EXISTS (...) AND
            # x < (SELECT ...)`). Each pass removes exactly one, so keep going
            # until none remain — otherwise a SUBQUERY node survives into the
            # compiled expression and the engine fails on an unknown node type.
            #
            # EXISTS is drained first: it can delete the Filter outright (when the
            # existence test was the whole predicate), and a scalar pass would then
            # be looking at a node that is no longer in the plan.
            for finder, rewrite in (
                (_find_exists, _decorrelate_exists),
                (_find_in, _decorrelate_in),
                (_find_subquery, _decorrelate),
            ):
                while filter_nid in plan and finder(plan[filter_nid].condition)[0] is not None:
                    plan = rewrite(plan, filter_nid, self.telemetry)
        return plan


def _lift_correlations(inner_plan: LogicalPlan):
    """
    Pull correlated equalities out of a subquery's own filters.

    Returns (key_pairs, residual) where key_pairs is [(inner_col, outer_col)] and
    residual is the conjunction of correlated NON-equality predicates that remain
    (or None). Uncorrelated predicates stay inside the subquery untouched.

    Orientation comes from the binder's resolution, not from the query text — see
    the module docstring.
    """
    correlations: list = []
    for inner_nid, inner_node in list(inner_plan.nodes(True)):
        if inner_node.node_type != LogicalPlanStepType.Filter:
            continue
        found, remaining = _split_correlations(inner_node.condition)
        if not found:
            continue
        correlations.extend(found)
        if remaining is None:
            inner_plan.remove_node(inner_nid, heal=True)
        else:
            inner_node.condition = remaining
            inner_node.columns = [
                column
                for column in (inner_node.columns or [])
                if not column.is_outer_reference
            ]

    key_pairs = []
    for correlation in correlations:
        if _is_outer(correlation.left):
            key_pairs.append((correlation.right, correlation.left))
        else:
            key_pairs.append((correlation.left, correlation.right))

    # Whatever still points outwards is a correlated non-equality. Only that part
    # becomes the residual: a filter can mix a correlated conjunct with purely
    # local ones (`l3.l_suppkey <> l1.l_suppkey AND l3.l_receiptdate >
    # l3.l_commitdate`, canonical TPC-H Q21). Lifting the whole condition out
    # stranded the local conjuncts on the join, where predicate pushdown can no
    # longer reach the scan — Q21's `l3` scan read all 6.0M lineitem rows instead
    # of the 3.8M that satisfy the local test.
    residual = None
    for inner_nid, inner_node in list(inner_plan.nodes(True)):
        if inner_node.node_type != LogicalPlanStepType.Filter:
            continue
        if _find_outer_reference(inner_node.condition) is None:
            continue
        correlated, local = _split_outer_referencing(inner_node.condition)
        residual = correlated if residual is None else residual
        if local is None:
            inner_plan.remove_node(inner_nid, heal=True)
        else:
            inner_node.condition = local
            inner_node.columns = [
                column
                for column in (inner_node.columns or [])
                if not column.is_outer_reference
            ]

    return key_pairs, residual


def _decorrelate_in(plan: LogicalPlan, filter_nid: str, telemetry) -> LogicalPlan:
    """
    Turn `x IN (subquery)` into a SEMI join and `x NOT IN` into a NULL-AWARE ANTI join.

    IN is EXISTS with one extra key: the membership test itself pairs the outer
    expression with the subquery's single output column. Any genuine correlation
    inside the subquery simply contributes further keys — which is why moving this
    here gains correlated `IN`, something the pre-bind rewrite never supported.

    NOT IN is null-aware and EXISTS is not: `x NOT IN (SELECT y ...)` yields nothing
    at all when any y is NULL, because `x <> NULL` is unknown rather than true. That
    is carried by the join TYPE (`left anti null-aware`), so it must not be
    "simplified" to a plain anti join.
    """
    filter_node = plan[filter_nid]
    in_node, _replace = _find_in(filter_node.condition)
    if in_node is None:
        return plan

    negated = bool(in_node.negated)
    inner_plan = in_node.right.value

    # The subquery's single output column is one side of the membership test. This
    # is also the "exactly one column" rule for an IN subquery.
    membership_column = _output_column(inner_plan)
    # A computed output (`SELECT CASE ... END`) is minted into `$derived` and has no
    # origin, and a key reference with no source is attributed to neither leg — the
    # join then comes out with empty key lists and is rejected. Name the relation.
    if not membership_column.origin:
        membership_column.origin = [f"$in-{random_string(6)}"]

    key_pairs, residual = _lift_correlations(inner_plan)
    key_pairs = [(_reference_to(membership_column), in_node.left)] + key_pairs

    return _build_filter_join(
        plan,
        filter_nid,
        inner_plan,
        remove=in_node,
        join_type="left anti null-aware" if negated else "left semi",
        key_pairs=key_pairs,
        residual=residual,
        telemetry=telemetry,
        counter="optimization_decorrelate_in_subquery",
        replace_projection=False,
    )


def _decorrelate_exists(plan: LogicalPlan, filter_nid: str, telemetry) -> LogicalPlan:
    """
    Turn `EXISTS (subquery)` into a SEMI join and `NOT EXISTS` into an ANTI join.

    EXISTS is a boolean test, not a value: nothing from the subquery is read, so
    unlike the scalar case there is no output column to substitute and no
    aggregate to group. The existence test becomes the join itself, and the
    EXISTS node is simply removed from the predicate.
    """
    filter_node = plan[filter_nid]
    exists_node, _replace = _find_exists(filter_node.condition)
    if exists_node is None:
        return plan

    negated = bool(exists_node.negated)
    subquery = exists_node.parameters[0]
    inner_plan = subquery.value

    key_pairs, residual = _lift_correlations(inner_plan)

    if not key_pairs:
        raise UnsupportedSyntaxError(
            "**EXISTS** requires a correlated equality predicate linking the subquery to the "
            "outer query (e.g. **EXISTS** (**SELECT** 1 **FROM** T **WHERE** T.k = outer.k)). "
            "Uncorrelated **EXISTS** is not supported."
        )

    return _build_filter_join(
        plan,
        filter_nid,
        inner_plan,
        remove=exists_node,
        join_type="left anti" if negated else "left semi",
        key_pairs=key_pairs,
        residual=residual,
        telemetry=telemetry,
        counter="optimization_decorrelate_exists_subquery",
        replace_projection=True,
    )


def _build_filter_join(
    plan: LogicalPlan,
    filter_nid: str,
    inner_plan: LogicalPlan,
    remove,
    join_type: str,
    key_pairs,
    residual,
    telemetry,
    counter: str,
    replace_projection: bool,
) -> LogicalPlan:
    """
    Replace a filtering subquery (EXISTS / IN) with a SEMI or ANTI join.

    Shared by both because they differ only in where the keys come from and which
    join type carries the semantics.
    """
    filter_node = plan[filter_nid]

    # The existence test BECOMES the join, so it has to come out of the predicate
    # — and `_split_out` can only take it out of a top-level AND chain. Refuse
    # here, before any graph surgery, when it cannot.
    #
    # This is not a nicety. `_rewrite_filters` drives this rewrite with
    # `while finder(condition) is not None`, so a target that survives the
    # removal is found again on the next turn: the planner looped forever,
    # inserting another join node each time, with no error and no result. That
    # is what this guard replaces.
    if not _is_removable_conjunct(filter_node.condition, remove):
        if _is_in_subquery(remove):
            keyword = "**IN**"
            example = (
                "**WHERE** x **NOT IN** (**SELECT** ...) rather than "
                "**WHERE** **NOT** (x **IN** (**SELECT** ...))"
            )
        else:
            keyword = "**EXISTS**"
            example = (
                "**WHERE** **NOT EXISTS** (**SELECT** ...) rather than "
                "**WHERE** **NOT** (**EXISTS** (**SELECT** ...))"
            )
        raise UnsupportedSyntaxError(
            f"An {keyword} subquery is only supported as a top-level condition of the "
            f"**WHERE** clause, alone or **AND**ed with other conditions. Here it sits inside "
            f"another expression - under **NOT**, **OR**, **IS NULL**, or a comparison - and "
            f"decorrelation turns the test into a join, which cannot express that. "
            f"Write it at the top level instead ({example})."
        )

    # The subquery must emit the join keys, and anything the residual reads (it is
    # evaluated per candidate pair). Target the node that defines the projection,
    # which is not always the exit — see _output_column.
    projection_node = _projecting_node(inner_plan)
    if projection_node is not None and projection_node.node_type == LogicalPlanStepType.Project:
        wanted = [_local_copy(inner_key) for inner_key, _ in key_pairs]
        seen = {column.schema_column.identity for column in wanted}
        for column in _inner_columns_of(residual):
            if column.schema_column is not None and column.schema_column.identity not in seen:
                seen.add(column.schema_column.identity)
                wanted.append(_local_copy(column))
        if replace_projection:
            # EXISTS projects `SELECT 1`; the literal is useless to match on, so the
            # projection is replaced outright by the keys.
            projection_node.columns = wanted
        else:
            # IN already projects the membership column, and its projection may be
            # feeding something above it (a HAVING filter reads columns the Project
            # passes through — canonical TPC-H Q18). Only ADD what is missing.
            existing = {
                column.schema_column.identity
                for column in (projection_node.columns or [])
                if column.schema_column is not None
            }
            projection_node.columns = list(projection_node.columns or []) + [
                column
                for column in wanted
                if column.schema_column is not None
                and column.schema_column.identity not in existing
            ]

    # --- graft the subquery in as the join's right-hand relation --------------
    outer_relations: set = set()
    outer_schemas: dict = {}
    for provider, _target, _relation in plan.ingoing_edges(filter_nid):
        found_relations, found_schemas = _collect_relations(plan, provider)
        outer_relations |= found_relations
        outer_schemas.update(found_schemas)

    # A correlation reaching past the immediate enclosing scope names a relation
    # that is on neither leg here, so it cannot be a key of THIS join. It is
    # bound on the ancestor SEMI/ANTI join that owns the relation instead —
    # sound because existence absorbs multiplicity:
    #     EXISTS(o: P ∧ EXISTS(l: Q))  ≡  EXISTS((o,l): P ∧ Q)
    # To let the carried inner column flow up, this join is converted from SEMI
    # to a plain INNER join (SEMI emits left-side columns only); the witness-pair
    # duplication that introduces is collapsed again by the ancestor's existence
    # test. `_defer_existence_to_ancestor` verifies the absorption argument holds
    # (only Filter/Project between, ancestor is SEMI/ANTI) and refuses otherwise.
    local_pairs = []
    deferred_pairs = []
    for inner_key, outer_key in key_pairs:
        if outer_key.source in outer_relations:
            local_pairs.append((inner_key, outer_key))
        else:
            deferred_pairs.append((inner_key, outer_key))

    inner_exit = inner_plan.get_exit_points()[0]
    plan += inner_plan
    inner_relations, inner_schemas = _collect_relations(plan, inner_exit)
    # Any relation named by a key that this leg supplies must be known as one of
    # its names, or the key resolves to neither side (see the `$in-` stamp above).
    for inner_key, _outer_key in key_pairs:
        origin = getattr(inner_key.schema_column, "origin", None)
        if origin:
            inner_relations.update(origin)

    def _skip_level_refusal(reason: str):
        names = ", ".join(f"`{outer_key.source_column}`" for _, outer_key in deferred_pairs)
        return UnsupportedSyntaxError(
            f"A correlated EXISTS/IN subquery correlates on {names}, which belongs to a "
            f"scope further out than the subquery enclosing it; {reason}."
        )

    if deferred_pairs:
        # Only a positive existence test (SEMI) can be converted to INNER — the
        # flattening identity does not hold through this join's own negation.
        if join_type != "left semi":
            raise _skip_level_refusal("NOT EXISTS / NOT IN cannot carry such a correlation")
        # A residual is evaluated per candidate pair INSIDE the semi probe; after
        # the conversion there is no probe to ride on. Unbuilt, so refuse.
        if residual is not None:
            raise _skip_level_refusal(
                "combining it with a correlated non-equality is not supported"
            )

    # A skip-level reference can also hide in the residual, where the outer
    # markers are about to be cleared — it would then be evaluated against a
    # column no leg carries. Refuse while the cause is still legible.
    for column in _all_columns_of(residual):
        if _is_outer(column) and column.source not in outer_relations:
            raise UnsupportedSyntaxError(
                f"A correlated **EXISTS**/IN subquery correlates on `{column.source_column}` "
                "through a non-equality predicate, and that column belongs to a scope "
                "further out than the subquery enclosing it. This is not supported."
            )

    on_condition = None
    for inner_key, outer_key in local_pairs:
        equals = Node(
            node_type=NodeType.COMPARISON_OPERATOR, value="Eq", do_not_create_column=True
        )
        equals.left = _local_copy(outer_key)
        equals.right = _local_copy(inner_key)
        if on_condition is None:
            on_condition = equals
        else:
            conjunction = Node(node_type=NodeType.AND, do_not_create_column=True)
            conjunction.left = on_condition
            conjunction.right = equals
            on_condition = conjunction

    join = LogicalPlanNode(node_type=LogicalPlanStepType.Join)
    # The SEMI→INNER conversion for deferred correlations (see above). With no
    # local key at all the converted join has nothing to hash on, and the engine
    # admits zero-key joins only as CROSS — which is the correct semantics here:
    # every (outer row, carried value) pair is a candidate witness.
    if deferred_pairs:
        join.type = "inner" if local_pairs else "cross join"
    else:
        join.type = join_type
    join.on = on_condition
    join.using = None
    # Every column the join reads, keys AND residual. The residual is evaluated
    # inside the existence test but lives on the node rather than in `on`, so
    # omitting its columns here hides them from projection pushdown, which then
    # prunes them off BOTH legs and the engine cannot evaluate the residual.
    # A deferred pair contributes its INNER column only — the outer one is not on
    # either leg here; it still has to be listed or projection pushdown prunes
    # the carried column off this leg before the ancestor join can bind it.
    join.columns = (
        [
            column
            for pair in local_pairs
            for column in (_local_copy(pair[1]), _local_copy(pair[0]))
        ]
        + [_local_copy(inner_key) for inner_key, _outer_key in deferred_pairs]
        + [_local_copy(column) for column in _all_columns_of(residual)]
    )
    join.left_relation_names = sorted(outer_relations)
    join.right_relation_names = sorted(inner_relations)
    join.all_relations = outer_relations | inner_relations
    join.schemas = {**outer_schemas, **inner_schemas}
    if local_pairs:
        join.left_columns, join.right_columns = extract_join_fields(
            on_condition, join.left_relation_names, join.right_relation_names
        )
        # Every local pair names both legs by construction; a shortfall means a
        # key that cannot be a key here slipped through — the silent-wrong-answer
        # case this partition exists to prevent.
        if len(join.left_columns) != len(local_pairs):
            raise InvalidInternalStateError(
                "decorrelation built a join key naming a relation that is on neither leg"
            )
    else:
        join.left_columns, join.right_columns = [], []
    # A correlated NON-equality cannot be a join key, and it cannot be a post-join
    # filter either: SEMI/ANTI emit rows already collapsed to existence, so the
    # inner side is gone by then. It rides on the join and is evaluated per
    # candidate (build, probe) pair inside the existence test — see
    # SemiAntiProbeOperator in native_join2.hpp.
    join.residual = _clear_outer_markers(residual) if residual is not None else None

    # The existence test IS the join now, so drop it from the predicate. The
    # guard at the top of this function already established that this succeeds;
    # the flag is read rather than discarded because a silently-not-removed
    # target is precisely what made the driving loop non-terminating.
    removed, remaining = _split_out(filter_node.condition, remove)
    if not removed:
        raise InvalidInternalStateError(
            "decorrelation could not remove the subquery from the predicate it had already "
            "converted into a join"
        )

    if remaining is None:
        # Nothing left to filter: the Filter node BECOMES the join, in place.
        # Deleting it and healing instead leaves other passes holding a node id
        # that no longer resolves (redundant_operators then reads None.alias).
        plan[filter_nid] = join
        plan.add_edge(inner_exit, filter_nid)
        join_nid = filter_nid
    else:
        filter_node.condition = remaining
        filter_node.columns = [
            column
            for column in (filter_node.columns or [])
            if column.node_type == NodeType.IDENTIFIER
        ]
        join_nid = random_string()
        plan.insert_node_before(join_nid, join, filter_nid)
        plan.add_edge(inner_exit, join_nid)

    # Bind deferred correlations on the ancestor existence join. Must run AFTER
    # insertion — the walk starts from this join's position in the plan. The
    # SEMI→INNER conversion is already committed, so an unbindable pair means the
    # plan is unsound and the query must be refused, not answered.
    for inner_key, outer_key in deferred_pairs:
        if not _defer_existence_to_ancestor(plan, join_nid, inner_key, outer_key):
            raise _skip_level_refusal(
                "no enclosing EXISTS/IN provides that relation through a path this "
                "rewrite can carry it (only filters and projections may sit between)"
            )

    setattr(telemetry, counter, getattr(telemetry, counter, 0) + 1)
    return plan


def _all_columns_of(node):
    """Every identifier under `node`, from either side of the correlation."""
    if node is None:
        return []
    if node.node_type == NodeType.IDENTIFIER:
        return [node]
    found: list = []
    for attr in ("left", "right", "centre"):
        found.extend(_all_columns_of(getattr(node, attr, None)))
    for param in getattr(node, "parameters", None) or []:
        found.extend(_all_columns_of(param))
    return found


def _inner_columns_of(node):
    """Every non-correlated identifier under `node`."""
    if node is None:
        return []
    if node.node_type == NodeType.IDENTIFIER:
        return [] if _is_outer(node) else [node]
    found: list = []
    for attr in ("left", "right", "centre"):
        found.extend(_inner_columns_of(getattr(node, attr, None)))
    for param in getattr(node, "parameters", None) or []:
        found.extend(_inner_columns_of(param))
    return found


def _clear_outer_markers(node):
    """
    Strip the correlated marker from a residual before it rides on the join.

    Both legs are ordinary inputs of the join by then, so a reference left flagged
    would read as still pointing out of scope.
    """
    if node is None:
        return None
    if node.node_type == NodeType.IDENTIFIER:
        return _local_copy(node) if _is_outer(node) else node
    for attr in ("left", "right", "centre"):
        child = getattr(node, attr, None)
        if child is not None:
            setattr(node, attr, _clear_outer_markers(child))
    parameters = getattr(node, "parameters", None)
    if parameters:
        node.parameters = [_clear_outer_markers(p) for p in parameters]
    return node


def _is_removable_conjunct(condition, target) -> bool:
    """
    Can `_split_out` take `target` out of `condition`?

    Mirrors `_split_out`'s search exactly, but does not mutate — so it can be
    asked BEFORE any of the rewrite's graph surgery has happened. `_split_out`
    edits the conjunction in place as it removes, and cannot be used twice as a
    look-before-you-leap.

    Only the whole condition, or a conjunct of a top-level AND chain, qualifies.
    A subquery nested under NOT / OR / IS NULL / a comparison does not, because
    the rewrite it feeds turns the test into a JOIN, and a join cannot express a
    disjunct or a negation of a row-level test.
    """
    if condition is None:
        return False
    if condition is target:
        return True
    if condition.node_type == NodeType.AND:
        and_left, and_right = binary_operands(condition)
        return _is_removable_conjunct(and_left, target) or _is_removable_conjunct(
            and_right, target
        )
    return False


def _split_out(condition, target):
    """
    Remove `target` from a conjunction.

    Returns (found, remaining); remaining is None when `target` was the whole
    predicate. `_is_removable_conjunct` answers the `found` question without
    mutating, and `_build_filter_join` asks it up front — a caller that ignores
    `found` and loops until the target is gone will never terminate.
    """
    if condition is None:
        return False, None
    if condition is target:
        return True, None
    if condition.node_type == NodeType.AND:
        and_left, and_right = binary_operands(condition)
        found_left, left = _split_out(and_left, target)
        found_right, right = _split_out(and_right, target)
        if not (found_left or found_right):
            return False, condition
        if left is None:
            return True, right
        if right is None:
            return True, left
        condition.left, condition.right = left, right
        return True, condition
    return False, condition


def _decorrelate(plan: LogicalPlan, filter_nid: str, telemetry) -> LogicalPlan:
    filter_node = plan[filter_nid]
    subquery, replace_subquery = _find_subquery(filter_node.condition)
    if subquery is None:
        return plan

    inner_plan = subquery.value

    # --- pull the correlation out of the subquery -----------------------------
    key_pairs, residual = _lift_correlations(inner_plan)

    # A scalar subquery yields a VALUE, so a correlated non-equality has nowhere to
    # go: unlike SEMI/ANTI there is no per-pair existence test to fold it into.
    # Reject it here, while the cause is still legible.
    if residual is not None:
        found = _find_outer_reference(residual)
        raise UnsupportedSyntaxError(
            "A correlated scalar subquery can only be decorrelated on equality "
            f"correlations; `{found.source_column if found else '?'}` correlates to the "
            "outer query through a non-equality predicate, which is not supported."
        )

    if not key_pairs:
        # Uncorrelated: the subquery does not depend on the outer row, so it is a
        # single value joined to every row — a cross join against a one-row relation.
        #
        # "One row" has to be established, not assumed: an ungrouped aggregate always
        # yields exactly one row, but a GROUP BY (or no aggregate at all) can yield
        # many, and cross-joining that would silently MULTIPLY the outer rows rather
        # than raise the "more than one row returned by a subquery" that SQL requires.
        _, aggregate = _aggregate_node(inner_plan)
        if aggregate is None or aggregate.node_type != LogicalPlanStepType.Aggregate:
            raise UnsupportedSyntaxError(
                "An uncorrelated scalar subquery must return exactly one row; only an "
                "ungrouped aggregate is supported here."
            )

    # A correlated subquery with no aggregate can still be provably one-row-per-
    # binding: `ORDER BY x LIMIT 1` — the top-1-per-group idiom. Rewritten to a
    # ranking window partitioned by the correlation keys (BTW2025 §4.4) BEFORE the
    # value column is read, so the plan is in its final shape.
    window_rewritten = False
    if key_pairs:
        _, aggregate = _aggregate_node(inner_plan)
        if aggregate is None:
            window_rewritten = _rewrite_order_limit_to_row_number(inner_plan, key_pairs)

    # Read the subquery's value column before the key widens the projection.
    value_column = _output_column(inner_plan)

    # Once decorrelated the subquery IS a relation, so its output column needs to
    # name one. An aggregate's output column has no origin (it is minted into the
    # `$derived` pseudo-schema), and a reference with no source belongs to neither
    # leg as far as the join operator is concerned — `HashedInnerJoinNode.supports`
    # rejects the join outright. This bites specifically when the value column
    # reaches the ON condition, which happens once cross-join-filter pushdown folds
    # `CROSS JOIN + WHERE x = <value>` into an equi-join.
    scalar_alias = f"$scalar-{random_string(6)}"
    if not value_column.origin:
        value_column.origin = [scalar_alias]

    for inner_key, _outer_key in key_pairs:
        if window_rewritten:
            # No aggregate to widen — the window's rank filter already guarantees
            # one row per key; the key only has to survive the projections.
            _expose_key_without_aggregate(inner_plan, inner_key)
        else:
            _expose_key(inner_plan, inner_key)

    # --- the subquery's value becomes an ordinary column ----------------------
    filter_node.condition = replace_subquery(_reference_to(value_column))
    filter_node.columns = [
        column for column in (filter_node.columns or []) if column.node_type == NodeType.IDENTIFIER
    ] + [_reference_to(value_column)]

    # --- graft the subquery in as a joined relation ---------------------------
    # Capture the outer leg BEFORE rewiring: insert_node_before moves every
    # provider of the filter onto the join, so this is the join's left input.
    outer_relations: set = set()
    outer_schemas: dict = {}
    for provider, _target, _relation in plan.ingoing_edges(filter_nid):
        found_relations, found_schemas = _collect_relations(plan, provider)
        outer_relations |= found_relations
        outer_schemas.update(found_schemas)

    # A correlation whose outer column belongs to THIS join's left leg can be a key
    # here. One reaching further out — to a grandparent scope — cannot: that
    # relation is not below this join, so the equality names a leg that does not
    # exist and is quietly discarded from the key lists. Those are deferred to the
    # ancestor join that does own the relation, once this join is in the plan.
    local_pairs = []
    deferred_pairs = []
    for inner_key, outer_key in key_pairs:
        if outer_key.source in outer_relations:
            local_pairs.append((inner_key, outer_key))
        else:
            deferred_pairs.append((inner_key, outer_key))

    # Narrow the aggregate to the keys the join above can actually consume. Must run
    # while `inner_plan` is still separate — after the merge below there is no inner
    # plan left to graft into — and after `_expose_key`, which is what makes the
    # aggregate grouped in the first place.
    if local_pairs and _reduce_aggregate_input(plan, filter_nid, inner_plan, local_pairs):
        setattr(
            telemetry,
            "optimization_decorrelate_aggregate_reduced",
            getattr(telemetry, "optimization_decorrelate_aggregate_reduced", 0) + 1,
        )

    inner_exit = inner_plan.get_exit_points()[0]
    plan += inner_plan
    inner_relations, inner_schemas = _collect_relations(plan, inner_exit)
    # The alias stamped onto the value column above has to be a known name of this
    # leg, or a reference carrying it still resolves to neither side.
    inner_relations.add(scalar_alias)

    on_condition = None
    for inner_key, outer_key in local_pairs:
        equals = Node(
            node_type=NodeType.COMPARISON_OPERATOR, value="Eq", do_not_create_column=True
        )
        equals.left = _local_copy(outer_key)
        equals.right = _local_copy(inner_key)
        if on_condition is None:
            on_condition = equals
        else:
            conjunction = Node(node_type=NodeType.AND, do_not_create_column=True)
            conjunction.left = on_condition
            conjunction.right = equals
            on_condition = conjunction

    join = LogicalPlanNode(node_type=LogicalPlanStepType.Join)
    # No correlation means no key to join on: the subquery is one value attached
    # to every outer row, which is exactly a cross join.
    join.type = "inner" if local_pairs else "cross join"
    join.on = on_condition
    join.using = None
    # The join's referenced columns. This must be populated: projection pushdown
    # only harvests a node's identities when `node.columns` is truthy, so leaving
    # it empty hides the join keys from that pass, which then prunes the key out
    # of the subquery's projection and leaves the build side unable to supply it.
    # A cross join has no keys, so the subquery's VALUE column takes that role —
    # it is the only thing the outer query needs from that leg.
    # A deferred pair contributes its INNER column only: the outer one is not on
    # either leg here. It still has to be listed or projection pushdown prunes the
    # carried column off this leg before the ancestor join can bind it.
    join.columns = (
        [
            column
            for pair in local_pairs
            for column in (_local_copy(pair[1]), _local_copy(pair[0]))
        ]
        + [_local_copy(inner_key) for inner_key, _outer_key in deferred_pairs]
    ) or [_reference_to(value_column)]
    join.left_relation_names = sorted(outer_relations)
    join.right_relation_names = sorted(inner_relations)
    join.all_relations = outer_relations | inner_relations
    join.schemas = {**outer_schemas, **inner_schemas}

    # The binder derives a join's key lists from its ON condition, but it ran
    # before this join existed and nothing recomputes them later — so they are
    # derived here, with the same helper, or the engine rejects the join for
    # having unaligned keys.
    if local_pairs:
        join.left_columns, join.right_columns = extract_join_fields(
            on_condition, join.left_relation_names, join.right_relation_names
        )
        # Every local pair names both legs by construction, so the helper must have
        # kept all of them. A shortfall means a pair reached this point that cannot
        # be a key here, which is the silent-wrong-answer case — refuse instead.
        if len(join.left_columns) != len(local_pairs):
            raise InvalidInternalStateError(
                "decorrelation built a join key naming a relation that is on neither leg"
            )
    else:
        join.left_columns, join.right_columns = [], []

    join_nid = random_string()
    plan.insert_node_before(join_nid, join, filter_nid)
    plan.add_edge(inner_exit, join_nid)

    # Correlations reaching past the enclosing scope are bound on the ancestor join
    # that owns their relation. This has to run AFTER the join is in the plan, since
    # the walk starts from it. A carried column that never gets bound is silently
    # wrong — worse than the original correlated query — so refuse if it cannot be.
    for inner_key, outer_key in deferred_pairs:
        if not _defer_correlation_to_ancestor(plan, join_nid, inner_key, outer_key):
            raise UnsupportedSyntaxError(
                f"A correlated subquery correlates on `{outer_key.source_column}`, which "
                "belongs to a scope further out than the subquery enclosing it, and no "
                "enclosing join provides that relation. This nesting is not supported."
            )

    telemetry.optimization_decorrelate_scalar_subquery = (
        getattr(telemetry, "optimization_decorrelate_scalar_subquery", 0) + 1
    )
    return plan
