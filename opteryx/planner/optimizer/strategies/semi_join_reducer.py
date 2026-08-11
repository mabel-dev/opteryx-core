# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Optimization Rule - Semi-Join Key Reducer

Type: Cost-based (runs after predicates are on the scans)
Goal: Reduce Rows

A decorrelated subquery reads its whole relation and lets the join above discard
what the outer query cannot consume. The original subquery was evaluated once per
outer binding, so it only ever saw keys the outer side holds — the rewrite is what
widened it. Measured on TPC-H at SF10:

    Q21  two SEMI/ANTI joins hash 60M and 38M `lineitem` rows to answer a question
         about 698,530 orderkeys — 91% of the query
    Q17  a grouped aggregate builds 2,000,000 groups to serve 2,044
    Q20  ... builds 5,441,669 groups to serve 58,782

This grafts a LEFT SEMI join below the expensive leg, against a fresh copy of the
opposite leg, so only reachable keys survive.

⛔ WHY THIS IS NOT IN `decorrelate_subquery` (where the widening happens). It was
built there first and had to be moved. Decorrelation is optimizer position 1;
JoinPlanning is 15 and PredicatePushdown is 16, so at position 1 a multi-relation
FROM is still a chain of unrestricted CROSS JOINs with every predicate sitting in
the Filter node above. Q21's outer leg there is literally
`nation x orders x lineitem x supplier` with nothing below the Filter — copying
that as a reducer duplicates a cartesian product to avoid a scan. Reading the
sibling conjuncts is not the same as having them MATERIALIZED below the join where
they can be copied. Here, they are.

SOUNDNESS. The reducer keeps exactly the rows whose key appears on the opposite
side. A row whose key appears on neither side can match no row of the other leg,
so it can change no SEMI result, no ANTI result, and no group the join above
reads. It is a NECESSARY condition only — the join above still does the exact
matching, including any residual. That is also why a cheaper SUPERSET of the key
set would be legal.

⛔ NOT a general "filter the probe side" rule. Reducing TPC-H Q19 — whose predicates
are already pushed, so a reducer only trims join probes — measured 0.80x, 20%
SLOWER. Probe misses are already cheap (see bench_join_csr_lookup.cpp). The gate is
not key-set selectivity (Q19's is 1.1% and it still lost); it is whether a hash
BUILD over a large relation, or a GROUP BY over one, disappears as a result.
"""

from opteryx.expression import NodeType
from opteryx.models import Node
from opteryx.planner.binder.join_helpers import extract_join_fields
from opteryx.planner.logical_planner import LogicalPlan
from opteryx.planner.logical_planner import LogicalPlanNode
from opteryx.planner.logical_planner import LogicalPlanStepType
from opteryx.utils import random_string

from .optimization_strategy import OptimizationStrategy
from .optimization_strategy import OptimizerContext

# The join types whose RIGHT leg is the build side and is therefore the expensive
# one. compiler.py pins this: "LEFT OUTER / SEMI / ANTI: the LEFT leg is the
# preserved/filtered side - it must be the PROBE; the RIGHT leg builds the table."
_BUILD_SIDE_IS_RIGHT = ("left semi", "left anti")

# Re-reading the source leg must cost materially less than the hash build it removes.
# Half is deliberately conservative: the saving is a build the reducer replaces with a
# smaller one, never the target scan itself, which still happens either way.
_COST_RATIO = 0.5


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

    The reducer's value is that the opposite leg emits FEWER keys than the target
    relation holds. With nothing narrowing it the key set is the full domain and the
    reducer is pure added cost — a scan, a hash build and a probe, to eliminate
    nothing.

    A CROSS JOIN disqualifies the leg outright: copying an unrestricted product to
    avoid a scan is never the trade we want, and it is what made this unbuildable at
    position 1.
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
        elif node.node_type == LogicalPlanStepType.Scan and getattr(node, "predicates", None):
            # ⛔ A pushed predicate narrows just as much as a Filter node, and by this
            # position most of them ARE pushed. Testing only for a Filter made firing
            # depend on whether the connector accepted the predicate: TPC-H Q17 kept a
            # Filter on the skene dataset and had none on the parquet one, so the same
            # query was reduced at SF10 and refused at SF001.
            narrowed = True
    return narrowed


def _scan_rows(plan: LogicalPlan):
    """
    Total base rows every Scan in this subtree reads, or None if any is unknown.

    ⛔ This is the cost model, and it deliberately reads `base_row_count` (manifest
    record counts) rather than the propagated row ESTIMATES. The estimates are not
    trustworthy enough to spend a subplan copy on: on the very queries this strategy
    targets, Q17's filter estimates 299,860,536 rows and returns 5,526, and Q20's
    `part` scan estimates 1 and returns 2,000,000. Base counts are read off the
    manifest and are exact.

    Unknown is fail-safe: no reducer. A missing count must never be read as "cheap".
    """
    total = 0
    for _nid, node in plan.nodes(True):
        if node.node_type == LogicalPlanStepType.Scan:
            stats = getattr(node, "statistics", None)
            rows = getattr(stats, "base_row_count", None) if stats is not None else None
            if not rows:
                return None
            total += rows
    return total


def _collect_relations(plan: LogicalPlan, root_nid: str):
    """Relation names and schemas a subtree exposes — a post-bind Join must carry
    what the binder would have given it."""
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


def _narrowest_key_source(plan: LogicalPlan, root_nid: str, relations: set):
    """
    The smallest subtree of a leg that still supplies every join key.

    The reducer needs a NECESSARY condition, so any SUPERSET of the key set is legal —
    and a superset is usually far cheaper to recompute. TPC-H Q17's left leg is
    `part_filtered JOIN lineitem`, which re-reads 60M `lineitem` rows to hand over a
    key that `part` alone supplies: descending to `Filter -> Scan part` takes the copy
    from 62M rows to 2M and turns a refusal into a 4.3x win. The keys it yields are a
    superset (a part row with no matching lineitem is not dropped), which the join
    above filters exactly, as it does anyway.

    Descends through joins only while ONE child still carries every key relation —
    when the keys straddle both children, the join itself is the narrowest source.
    """
    nid = root_nid
    for _ in range(16):
        node = plan[nid]
        if node is None or node.node_type != LogicalPlanStepType.Join:
            break
        children = [child for child, _t, _r in plan.ingoing_edges(nid)]
        carrying = [c for c in children if relations <= _collect_relations(plan, c)[0]]
        if len(carrying) != 1:
            break
        nid = carrying[0]
    return nid


def _aggregate_under(plan: LogicalPlan, root_nid: str):
    """
    The grouped aggregate at the top of a join leg, reached through pass-through nodes.

    A decorrelated scalar subquery arrives as `Project -> AggregateAndGroup -> ...`, so
    the aggregate is not the leg root. Only Project/Filter may sit above it — anything
    else (a join, a limit, another aggregate) means the rows the aggregate produces are
    no longer one-per-key by the time the join reads them, and reducing its input would
    be reasoning about the wrong relation.
    """
    nid = root_nid
    for _ in range(8):
        node = plan[nid]
        if node is None:
            return None, None
        if node.node_type == LogicalPlanStepType.AggregateAndGroup:
            return nid, node
        if node.node_type not in (LogicalPlanStepType.Project, LogicalPlanStepType.Filter):
            return None, None
        providers = plan.ingoing_edges(nid)
        if len(providers) != 1:
            return None, None
        nid = providers[0][0]
    return None, None


def _equi_pairs(condition):
    """[(left_col, right_col)] for every top-level Eq conjunct of an ON clause."""
    if condition is None:
        return []
    if condition.node_type == NodeType.AND:
        return _equi_pairs(condition.left) + _equi_pairs(condition.right)
    if condition.node_type == NodeType.COMPARISON_OPERATOR and condition.value == "Eq":
        left, right = condition.left, condition.right
        if left is not None and right is not None:
            if left.node_type == NodeType.IDENTIFIER and right.node_type == NodeType.IDENTIFIER:
                return [(left, right)]
    return []


class SemiJoinReducerStrategy(OptimizationStrategy):
    # Cost-typed so the driver refreshes statistics first; requires predicates
    # already pushed, which is the whole reason this cannot live at position 1.
    optimization_technique = "cost"
    requires = ("predicates-pushed",)

    def should_i_run(self, plan: LogicalPlan) -> bool:
        """
        Cheap pre-filter for "is there anything here at all".

        This is load-bearing beyond skipping the walk: a `cost` strategy that says yes
        makes the driver run `refresh_statistics` before it, so answering yes on every
        query would put a full statistics refresh on the path of queries that have no
        candidate join whatsoever. The real decision still happens in `complete()`.
        """
        grouped_aggregate = any(
            node.node_type == LogicalPlanStepType.AggregateAndGroup
            for _nid, node in plan.nodes(True)
        )
        for _nid, node in plan.nodes(True):
            if node.node_type != LogicalPlanStepType.Join:
                continue
            if getattr(node, "reducer_applied", False):
                continue
            if node.type in _BUILD_SIDE_IS_RIGHT:
                return True
            if node.type == "inner" and grouped_aggregate:
                return True
        return False

    def visit(self, node: LogicalPlanNode, context: OptimizerContext) -> OptimizerContext:
        if not context.optimized_plan:
            context.optimized_plan = context.pre_optimized_tree.copy()  # type: ignore
        return context

    def complete(self, plan: LogicalPlan, context: OptimizerContext) -> LogicalPlan:
        # Graph inserts are deferred: mutating during the traversal corrupts the walk.
        targets = [
            nid
            for nid, node in plan.nodes(True)
            if node.node_type == LogicalPlanStepType.Join
            and node.type in _BUILD_SIDE_IS_RIGHT + ("inner",)
            and not getattr(node, "reducer_applied", False)
        ]
        for join_nid in targets:
            plan = self._reduce_build_side(plan, join_nid)
        return plan

    def _reduce_build_side(self, plan: LogicalPlan, join_nid: str) -> LogicalPlan:
        from opteryx.planner.relation_resolver import copy_sub_plan
        from opteryx.planner.relation_resolver import rename_relations

        join = plan[join_nid]
        in_edges = plan.ingoing_edges(join_nid)
        if len(in_edges) != 2:
            return plan
        # ⛔ Legs are identified by edge INSERTION ORDER, not by the edge label — the
        # labels are None and compiler.py falls back to `left if idx == 0 else right`.
        # The right leg is therefore the LAST edge, which is what makes remove-then-add
        # below preserve the sides: the re-added edge lands last again.
        (left_root, _lt, _lr), (right_root, _rt, right_rel) = in_edges[0], in_edges[1]

        pairs = _equi_pairs(getattr(join, "on", None))
        if not pairs:
            return plan

        # Where the reducer goes. For SEMI/ANTI it is the join's own build side. For an
        # INNER join it is only worth doing when the right leg is a decorrelated scalar
        # subquery — a grouped aggregate keyed by the join key — where the reducer stops
        # groups being BUILT rather than merely stopping them being probed. That is the
        # Q19 lesson: trimming an inner join's probe side alone measured 0.80x.
        if join.type in _BUILD_SIDE_IS_RIGHT:
            target_parent, target_child, parent_rel = join_nid, right_root, right_rel
        else:
            aggregate_nid, aggregate = _aggregate_under(plan, right_root)
            if aggregate_nid is None:
                return plan
            group_ids = {
                getattr(getattr(g, "schema_column", None), "identity", None)
                for g in (aggregate.groups or [])
            }
            # Every join key on the aggregate's side must be a GROUP key. Reducing on a
            # column the aggregate does not group by would drop input rows that still
            # contribute to a surviving group — a wrong answer, not a slower one.
            right_names = set(join.right_relation_names or [])
            for first, second in pairs:
                key = first if getattr(first, "source", None) in right_names else second
                identity = getattr(getattr(key, "schema_column", None), "identity", None)
                if identity is None or identity not in group_ids:
                    return plan
            agg_providers = plan.ingoing_edges(aggregate_nid)
            if len(agg_providers) != 1:
                return plan
            target_parent = aggregate_nid
            target_child = agg_providers[0][0]
            parent_rel = agg_providers[0][2]
            right_root = target_child

        # Narrow to the cheapest subtree still supplying every key (a legal superset).
        left_names = set(join.left_relation_names or [])
        key_relations = set()
        for first, second in pairs:
            side = first if getattr(first, "source", None) in left_names else second
            source_name = getattr(side, "source", None)
            if source_name is None:
                return plan
            key_relations.add(source_name)
        left_root = _narrowest_key_source(plan, left_root, key_relations)

        source_subplan = _subplan_rooted_at(plan, left_root)
        if not _is_restricted(source_subplan):
            return plan

        # The copy is re-READ, not shared — plans here are trees, not DAGs. So the
        # reducer only pays when re-reading the source leg costs materially less than
        # the build it removes. TPC-H Q21 is the counter-example that forced this gate:
        # its source leg contains a full 60M-row `lineitem` scan of its own, so copying
        # it to reduce a 60M-row build side costs 1.25-2.25x what it saves, and the
        # query measured SLOWER with the reducer than without it. Q04's source leg is
        # `orders` alone (15M against 60M) and measured 5.3x faster.
        source_cost = _scan_rows(source_subplan)
        target_cost = _scan_rows(_subplan_rooted_at(plan, right_root))
        if source_cost is None or not target_cost:
            return plan
        if source_cost >= target_cost * _COST_RATIO:
            return plan

        # Fresh node ids AND fresh relation aliases/uuids. Without the rename both
        # copies claim the same relation names, and a join whose legs resolve BY NAME
        # can no longer tell which side a key belongs to.
        reducer_source = copy_sub_plan(source_subplan)
        scans_before = {
            nid: n.alias
            for nid, n in reducer_source.nodes(True)
            if n.node_type in (LogicalPlanStepType.Scan, LogicalPlanStepType.FunctionDataset)
            and n.alias
        }
        rename_relations(reducer_source)
        alias_map = {old: reducer_source[nid].alias for nid, old in scans_before.items()}

        on_condition = None
        join_columns: list = []
        for first, second in pairs:
            # Orient the pair: one side belongs to the outer join's LEFT leg (the one
            # being copied), the other to its RIGHT leg (the one being reduced).
            if getattr(first, "source", None) in left_names:
                source_col, target_col = first, second
            elif getattr(second, "source", None) in left_names:
                source_col, target_col = second, first
            else:
                return plan
            copied = source_col.copy()
            copied.source = alias_map.get(source_col.source, source_col.source)
            equals = Node(
                node_type=NodeType.COMPARISON_OPERATOR, value="Eq", do_not_create_column=True
            )
            equals.left = target_col.copy()
            equals.right = copied
            join_columns.extend((target_col.copy(), copied))
            if on_condition is None:
                on_condition = equals
            else:
                conjunction = Node(node_type=NodeType.AND, do_not_create_column=True)
                conjunction.left = on_condition
                conjunction.right = equals
                on_condition = conjunction

        if on_condition is None:
            return plan

        target_relations, target_schemas = _collect_relations(plan, right_root)
        reducer_exit = reducer_source.get_exit_points()[0]
        # ⛔ Collect from `reducer_source` BEFORE merging, and do not merge until every
        # guard below has passed. `plan += reducer_source` with a later `return plan`
        # leaves the copy in the plan with nothing consuming it — a second exit point,
        # which surfaces as the opaque "a plan headed by FilterJoinNode is not
        # supported" rather than as the declined optimization it actually is.
        source_relations, source_schemas = _collect_relations(reducer_source, reducer_exit)

        reducer = LogicalPlanNode(node_type=LogicalPlanStepType.Join)
        reducer.type = "left semi"
        # This node is itself a `left semi` join, so without the stamp the next pass
        # picks it up as a target and reduces the reducer, recursively.
        reducer.reducer_applied = True
        reducer.on = on_condition
        reducer.using = None
        reducer.columns = join_columns
        reducer.left_relation_names = sorted(target_relations)
        reducer.right_relation_names = sorted(source_relations)
        reducer.all_relations = target_relations | source_relations
        reducer.schemas = {**target_schemas, **source_schemas}
        reducer.left_columns, reducer.right_columns = extract_join_fields(
            on_condition, reducer.left_relation_names, reducer.right_relation_names
        )
        # A key naming neither leg is the silent-wrong-answer case, not something to
        # push on through.
        if len(reducer.left_columns) != len(pairs) or len(reducer.right_columns) != len(pairs):
            return plan

        # Every guard has passed — only now does the copy enter the plan.
        plan += reducer_source
        reducer_nid = random_string()
        plan.add_node(reducer_nid, reducer)
        plan.remove_edge(target_child, target_parent, parent_rel)
        plan.add_edge(target_child, reducer_nid, None)
        plan.add_edge(reducer_exit, reducer_nid, None)
        plan.add_edge(reducer_nid, target_parent, parent_rel)

        join.reducer_applied = True
        self.telemetry.optimization_semi_join_reducer = (
            getattr(self.telemetry, "optimization_semi_join_reducer", 0) + 1
        )
        return plan
