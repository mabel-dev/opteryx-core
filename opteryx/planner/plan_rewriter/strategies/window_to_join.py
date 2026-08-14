# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Rewrites Window logical nodes (OVER (...)) as joins.

    SELECT agg(e) OVER (PARTITION BY c) FROM t

is rewritten at the plan level to:

    WITH _win AS (SELECT c, agg(e) AS $win_xxx FROM t GROUP BY c)
    SELECT $win_xxx FROM t INNER JOIN _win ON t.c = _win.c

The Window node carries the aggregate expressions and the partition columns. The
rewriter COPIES the whole sub-plan feeding the Window node, hangs an aggregate off
that copy, wraps it in a Subquery node, replaces the Window node with a Join, and
wires the subquery as the right-hand input of the join.

An empty `OVER ()` is the same rewrite with the partition list empty — one partition
holding every row:

    SELECT agg(e) OVER () FROM t
    WITH _win AS (SELECT agg(e) AS $win_xxx FROM t)      -- UNGROUPED: exactly one row
    SELECT $win_xxx FROM t CROSS JOIN _win

The CTE is an ungrouped `Aggregate` rather than an `AggregateAndGroup` over no keys,
because "exactly one row for any input" is what makes the cross join a BROADCAST and
not a multiplication of the outer rows — and only the ungrouped node states that. With
no partition key there is no ON condition, and an inner join with no ON is a cross
join; the binder back-fills a join's left relation names from its bound ON condition,
so with no ON to read the rewrite states them instead.

The CTE source is the window's INPUT, not its base table. SQL applies WHERE before
window functions, so the rows the window aggregates are the rows that survived the
filter. Building the CTE from the Scan alone aggregated rows the query had already
discarded and returned a silently wrong answer:

    SELECT name, COUNT(*) OVER (PARTITION BY number_of_moons) FROM $planets WHERE id != 1
    -- Venus answered 2; Mercury shares its partition but the filter removed it, so 1.

N distinct partition specs in one SELECT produce N stacked Window nodes (the logical
planner emits one per spec), and they are rewritten TOGETHER as a chain — one CTE and
one join per spec, every one of them built from the SAME source sub-plan, with a SINGLE
reconciling Project on top. Rewriting them one at a time instead was wrong twice over:
each rewrite copied the sub-plan BELOW its Window node, which by then held the previous
rewrite's join, so the source was duplicated exponentially in the number of specs; and
each rewrite added its own `source.*` Project, so the second one expanded the source
relation a second time and every source column arrived duplicated.

Limitations (Phase 1):
- PARTITION BY or nothing — no ORDER BY, no frame specs.
- The source must expose ONE relation name (see `_source_relation`) — no pre-existing
  join sitting directly under the window. A CTE or derived table IS one relation
  however many tables its body joins, so those are fine.
- NULL partition keys: uses Eq (= not IS NOT DISTINCT FROM); NULL partitions are excluded.
"""

from opteryx.exceptions import InvalidInternalStateError
from opteryx.exceptions import UnsupportedSyntaxError
from opteryx.expression import NodeType
from opteryx.models import LogicalColumn, Node
from opteryx.planner.logical_planner import LogicalPlan, LogicalPlanNode, LogicalPlanStepType
from opteryx.planner.plan_rewriter.strategies.rewrite_strategy import (
    PlanRewriteContext,
    PlanRewriteStrategy,
)
from opteryx.utils import random_string

# Alias prefix for the CTE's copy of the window's source relation. Minted, never typed.
WINDOW_SOURCE_ALIAS_PREFIX = "$win_src-"


def _source_relation(plan: LogicalPlan) -> LogicalPlanNode:
    """The one relation a window's source sub-plan exposes to the query above it.

    The rewriter rebuilds the outer leg of the join as a SINGLE qualified wildcard
    `<alias>.*`, and names the partition columns against that same alias, so the source
    must present exactly one relation name — a second one would have its columns
    silently dropped from the wildcard.

    Edges run leaf -> head, so walk DOWN from the sub-plan's root to the first node that
    introduces a relation. Stopping at a Subquery is the point: a derived table or an
    expanded CTE/view is ONE relation addressed by its alias, however many tables its
    body joins, and its alias is the name the outer query wrote. Counting Scans instead
    refused those for no structural reason, and only after the Relation Resolver had
    already expanded them — which is why it read as an internal error and not as a
    refusal the reader could act on.
    """
    from opteryx.planner.relation_resolver import RELATION_STEP_TYPES

    nid = plan.get_exit_points()[0]
    while True:
        node = plan[nid]
        if node.node_type in RELATION_STEP_TYPES:
            return node
        below = plan.ingoing_edges(nid)
        if len(below) != 1:
            # a branch (a join under the window) or a leaf that names no relation
            raise UnsupportedSyntaxError(
                "Window functions over multiple joined tables are not yet supported. Compute the window in a subquery over a single relation, then join to that result."
            )
        nid = below[0][0]


def _build_eq_condition(left_col: Node, right_col: Node) -> Node:
    eq = Node(node_type=NodeType.COMPARISON_OPERATOR, value="Eq", do_not_create_column=True)
    eq.left = left_col
    eq.right = right_col
    return eq


def _and_conditions(conditions: list) -> Node:
    result = conditions[0]
    for cond in conditions[1:]:
        and_node = Node(node_type=NodeType.AND, do_not_create_column=True)
        and_node.left = result
        and_node.right = cond
        result = and_node
    return result


def _window_source(plan: LogicalPlan, win_nid: str) -> str:
    """The node id feeding a Window node."""
    providers = plan.ingoing_edges(win_nid)
    if len(providers) != 1:
        raise InvalidInternalStateError(
            f"a Window node takes exactly one input, this one has {len(providers)}"
        )
    return providers[0][0]


def _build_window_cte(
    plan: LogicalPlan, source_subplan: LogicalPlan, win_node: LogicalPlanNode
) -> tuple:
    """Build one aggregate CTE for one partition spec and merge it into `plan`.

    Returns `(subquery_wrapper_nid, subquery_alias)`. The caller wires it as the right
    leg of the join that replaces the Window node.

    `source_subplan` is the window CHAIN's input — the sub-plan below the lowest Window
    node, so every spec in the chain aggregates the same rows and the source is copied
    once per spec rather than once per spec on top of every earlier spec's join.
    """
    from opteryx.planner.relation_resolver import copy_sub_plan
    from opteryx.planner.relation_resolver import rename_relations

    partition_by = win_node.partition_by  # list of LogicalColumn/Node
    agg_nodes = win_node.aggregates       # list of agg Node (each has .alias set)

    subquery_alias = f"$win-{random_string(6)}"

    # --- Build the CTE inner plan: <window's input> → AggregateAndGroup ---
    # The window's input, copied. `copy_sub_plan` gives it fresh node ids so merging it
    # back below cannot overwrite the original, and `rename_relations` gives every
    # relation in it — Scan, FunctionDataset AND Subquery — a fresh alias so the binder
    # does not see two relations of the same name. It also remaps every column reference
    # INSIDE the copy (a filter's predicate, say) onto the new alias, which is why the
    # copy can carry arbitrary nodes and not just a Scan.
    inner_plan = copy_sub_plan(source_subplan)
    rename_relations(inner_plan, prefix=WINDOW_SOURCE_ALIAS_PREFIX)
    cte_src_alias = _source_relation(inner_plan).alias
    cte_root_nid = inner_plan.get_exit_points()[0]

    # --- Build partition column refs for the CTE inner plan ---
    # These carry cte_src_alias so the binder resolves them within the CTE scope.
    inner_partition_by = []
    for pb in partition_by:
        col_name = getattr(pb, "source_column", None) or getattr(pb, "value", None)
        inner_pb = LogicalColumn(
            node_type=NodeType.IDENTIFIER,
            source=cte_src_alias,
            source_column=col_name,
        )
        inner_pb.query_column = col_name
        inner_partition_by.append(inner_pb)

    if inner_partition_by:
        # AggregateAndGroup: GROUP BY partition columns, compute window aggregates.
        # projection exposes both partition cols and aggregate results for the join.
        agg_step = LogicalPlanNode(node_type=LogicalPlanStepType.AggregateAndGroup)
        agg_step.groups = inner_partition_by
        agg_step.aggregates = list(agg_nodes)
        agg_step.projection = inner_partition_by + list(agg_nodes)
    else:
        # OVER () — one partition holding every row. An UNGROUPED aggregate, which is
        # the node type that yields exactly one row for any input including an empty
        # one; `AggregateAndGroup` with an empty group list is not the same statement
        # and has no such guarantee. That one row is what makes the cross join below a
        # broadcast rather than a multiplication of the outer rows.
        agg_step = LogicalPlanNode(node_type=LogicalPlanStepType.Aggregate)
        agg_step.groups = []
        agg_step.aggregates = list(agg_nodes)
    agg_nid = random_string()
    inner_plan.add_node(agg_nid, agg_step)
    inner_plan.add_edge(cte_root_nid, agg_nid)

    # Project node above the AggAndGroup — required so the binder renames $derived to
    # $project before the Subquery wrapper's visit_exit runs. Without it, aggregate
    # columns added to $derived by the AggAndGroup binder are popped by visit_exit and
    # never appear in the subquery's output schema.
    project_step = LogicalPlanNode(node_type=LogicalPlanStepType.Project)
    project_step.columns = list(inner_partition_by) + list(agg_nodes)
    project_step.passthrough_columns = []
    project_step.except_columns = None
    project_nid = random_string()
    inner_plan.add_node(project_nid, project_step)
    inner_plan.add_edge(agg_nid, project_nid)

    # Wrap in a Subquery node so the binder treats it as a named relation.
    subquery_wrapper = LogicalPlanNode(node_type=LogicalPlanStepType.Subquery)
    subquery_wrapper.alias = subquery_alias
    subquery_wrapper.columns = [Node(node_type=NodeType.WILDCARD)]
    subquery_wrapper_nid = random_string()

    # Merge CTE inner plan into main plan.
    plan += inner_plan
    plan.add_node(subquery_wrapper_nid, subquery_wrapper)
    plan.add_edge(project_nid, subquery_wrapper_nid)

    return subquery_wrapper_nid, subquery_alias


def _rewrite_window_chain(plan: LogicalPlan, chain: list) -> LogicalPlan:
    """Rewrite a chain of stacked Window nodes over ONE source into joins.

    `chain` is bottom-up: `chain[0]`'s input is the source sub-plan, and each later
    entry's input is the entry before it. Every spec's CTE is built from that one source
    sub-plan, and ONE Project on top of the last join reconciles the result.
    """
    from opteryx.planner.relation_resolver import subplan_rooted_at

    source_nid = _window_source(plan, chain[0])
    source_subplan = subplan_rooted_at(plan, source_nid)
    if any(_is_aggregate_window(node) for _, node in source_subplan.nodes(True)):
        # The logical planner emits every aggregate Window node for one SELECT as one
        # unbroken stack, so a chain takes all of them. If one is left below, the copy
        # taken here carries it, the fixed-point loop rewrites the copy, and the source
        # duplication this rewrite exists to remove comes back silently.
        raise InvalidInternalStateError(
            "an aggregate Window node was left below a window chain"
        )
    source_relation = _source_relation(source_subplan)
    source_alias = source_relation.alias

    # The Project above the last join. It expands ONLY the source relation (a qualified
    # wildcard) plus every window's result column: after each join both the source and
    # that join's subquery expose the partition column under the same name, and the
    # parent Project would see it twice (AmbiguousIdentifierError). The subqueries' copies
    # of the partition columns are never projected, so the parent sees each name once.
    # ONE Project for the whole chain, not one per join — a second `source.*` expands the
    # source relation a second time, which is the same ambiguity by another route.
    outer_wildcard = Node(node_type=NodeType.WILDCARD)
    outer_wildcard.value = [source_alias]  # qualified wildcard: outer_scan.*
    win_refs = []

    left_nid = source_nid
    for win_nid in chain:
        win_node = plan[win_nid]
        subquery_wrapper_nid, subquery_alias = _build_window_cte(
            plan, source_subplan, win_node
        )
        _replace_window_with_join(
            plan, win_nid, win_node, left_nid, subquery_wrapper_nid, subquery_alias, source_alias
        )
        for agg in win_node.aggregates:
            win_ref = LogicalColumn(
                node_type=NodeType.IDENTIFIER,
                source=subquery_alias,
                source_column=agg.alias,
                alias=agg.alias,
            )
            win_ref.query_column = agg.alias
            win_refs.append(win_ref)
        left_nid = win_nid

    filter_step = LogicalPlanNode(node_type=LogicalPlanStepType.Project)
    filter_step.passthrough_columns = []
    filter_step.except_columns = None
    filter_step.columns = [outer_wildcard] + win_refs
    filter_nid = random_string()
    plan.insert_node_after(filter_nid, filter_step, chain[-1])

    return plan


def _replace_window_with_join(
    plan: LogicalPlan,
    win_nid: str,
    win_node: LogicalPlanNode,
    left_nid: str,
    subquery_wrapper_nid: str,
    subquery_alias: str,
    source_alias: str,
) -> None:
    """Replace a Window node in place with the inner join onto its CTE."""
    partition_by = win_node.partition_by

    # --- Build ON condition: outer_scan.c = $win.c for each partition column ---
    # Left side explicitly references the outer scan alias to avoid post-join ambiguity.
    # It stays the SOURCE alias for every join in a chain: the source relation is still
    # addressable above the earlier joins, and its partition column is the one being
    # matched — the earlier joins added window results, they did not rename anything.
    on_parts = []
    for pb in partition_by:
        col_name = getattr(pb, "source_column", None) or getattr(pb, "value", None)
        outer_col = LogicalColumn(
            node_type=NodeType.IDENTIFIER,
            source=source_alias,
            source_column=col_name,
        )
        inner_col = LogicalColumn(
            node_type=NodeType.IDENTIFIER,
            source=subquery_alias,
            source_column=col_name,
        )
        on_parts.append(_build_eq_condition(outer_col, inner_col))

    on_condition = _and_conditions(on_parts) if on_parts else None

    # --- Replace Window node with the join onto its CTE ---
    # No PARTITION BY means no key to match on, and an inner join with no ON is a cross
    # join — against a relation the ungrouped aggregate guarantees is exactly one row,
    # so it attaches that row to every outer row rather than multiplying anything. This
    # is the shape decorrelate_subquery already lowers an uncorrelated scalar subquery
    # to, and the native compiler builds the RIGHT leg for it (see `is_cross` in
    # compiler._compile_join), which is the one-row side.
    #
    # `left_relation_names` has to be stated here rather than left None: the binder
    # back-fills it from the identifiers in a bound ON condition, and a cross join has
    # no ON to read. Left unset it reached the row-size estimate as None and died there.
    join_node = LogicalPlanNode(node_type=LogicalPlanStepType.Join)
    join_node.type = "inner" if on_condition is not None else "cross join"
    join_node.on = on_condition
    join_node.using = None
    join_node.left_relation_names = None if on_condition is not None else [source_alias]
    join_node.right_relation_names = [subquery_alias]
    join_node.columns = []
    join_node.is_window_join = True

    plan[win_nid] = join_node
    # Label the legs. The Window node's incoming edge carried no label, and when a join's
    # legs are unlabelled the native compiler falls back to in-edge ORDER (see the `legs`
    # dict in compiler._compile_join) — while `left_columns`/`right_columns` come from the
    # relation names, which do not move. The optimizer is free to reorder those edges, and
    # it does: over a derived-table source, removing the redundant Subquery wrappers put
    # the aggregate leg first and the join compiled its left key against the right leg
    # ("a build-side join key the engine could not resolve here"). The direct-Scan case
    # only ever worked because the order happened to come out right. `add_edge` updates an
    # existing edge's relationship, so the first call relabels the source leg in place.
    plan.add_edge(left_nid, win_nid, "left")
    plan.add_edge(subquery_wrapper_nid, win_nid, "right")


def _window_chains(plan: LogicalPlan, candidates: list) -> list:
    """Group Window nodes into the stacks they form, each returned bottom-up.

    N distinct partition specs in one SELECT are N Window nodes stacked directly on top
    of each other over one source, and they are rewritten as a unit — see the module
    docstring. Windows in different scopes (a subquery and the query above it) are not
    stacked and form separate chains.
    """
    members = set(candidates)
    provider_of = {nid: _window_source(plan, nid) for nid in candidates}
    # provider -> the window sitting directly on top of it
    consumer_of = {
        provider: nid for nid, provider in provider_of.items() if provider in members
    }

    chains = []
    for nid in candidates:  # candidate ORDER, so the chains come out deterministic
        if provider_of[nid] in members:
            continue  # not the bottom of its chain
        chain = [nid]
        while chain[-1] in consumer_of:
            chain.append(consumer_of[chain[-1]])
        chains.append(chain)
    return chains


def _innermost_chain_first(plan: LogicalPlan, chains: list) -> list:
    """Order chains so each is rewritten only after every chain nested inside its source.

    Window chains in different SCOPES are separate chains (`_window_chains`), and a
    subquery's chain sits inside the source sub-plan of the chain above it:

        SELECT SUM(x) OVER () FROM (SELECT COUNT(*) OVER () AS x FROM t) AS s

    The rewrite COPIES that source sub-plan once per partition spec, so copying it while
    it still holds an un-rewritten Window would duplicate the inner window's own source
    per copy — the exponential duplication this rewrite exists to remove, which is why
    `_rewrite_window_chain` refuses to copy a source with an aggregate Window in it.
    Rewritten innermost-first the source holds a join by then, and that refusal goes back
    to being an invariant check rather than a limit on perfectly legal SQL: before this,
    the statement above died with "an aggregate Window node was left below a window
    chain".

    Ordering by the SIZE of each chain's source sub-plan is a topological order for
    "nested inside": every node in a logical plan has exactly one consumer, so a nested
    chain's source is a strict SUBSET of the enclosing chain's and therefore strictly
    smaller. Chains in neither relation may run in any order. If the plan shape ever
    stops satisfying that, the refusal fires — the failure is loud, not silent.
    """
    from opteryx.planner.relation_resolver import subplan_rooted_at

    source_size = {
        chain[0]: len(subplan_rooted_at(plan, _window_source(plan, chain[0]))) for chain in chains
    }
    return sorted(chains, key=lambda chain: source_size[chain[0]])


def _is_aggregate_window(node: LogicalPlanNode) -> bool:
    """Aggregate windows (SUM/COUNT/... OVER) lower to a GROUP BY + broadcast join.

    Ranking windows (ROW_NUMBER/RANK/DENSE_RANK) carry `outputs` and are executed by
    the dedicated WindowNode operator instead — they must NOT be picked up here, or
    the fixed-point rewriter would loop on an un-rewritable node. `outputs` is set
    before this strategy runs — at logical-planning time for user-facing ranking
    windows, at plan-rewrite time by the INTERSECT/EXCEPT ALL rewrite — unlike
    `window_functions`, which the binder fills in later, so it is the reliable
    discriminator here.
    """
    return (
        node.node_type == LogicalPlanStepType.Window
        and getattr(node, "outputs", None) is None
    )


class WindowToJoinStrategy(PlanRewriteStrategy):
    def should_i_run(self, plan: LogicalPlan) -> bool:
        return any(_is_aggregate_window(node) for _, node in plan.nodes(True))

    def visit(self, node: LogicalPlanNode, context: PlanRewriteContext) -> PlanRewriteContext:
        if not context.rewritten_plan:
            context.rewritten_plan = context.pre_rewrite_tree.copy()

        if _is_aggregate_window(node):
            context.bag.setdefault("candidates", []).append(context.node_id)

        return context

    def complete(self, plan: LogicalPlan, context: PlanRewriteContext) -> LogicalPlan:
        candidates = context.bag.get("candidates", [])
        chains = _window_chains(plan, candidates)
        for chain in _innermost_chain_first(plan, chains):
            plan = _rewrite_window_chain(plan, chain)
        return plan
