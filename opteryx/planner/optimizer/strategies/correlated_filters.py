# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Optimization Rule - Correlated Filters

Type: Cost-based (consumes propagated statistics)
Goal: Reduce Rows / IO

For an equi-join ``a.k = b.k`` the matching rows on one side are bounded by the
realized value range of the join key on the other side. We read that range from
the propagated ``node.statistics`` (post-filter / post-join-intersection — see
statistics_refresh) and push it onto the opposite leg's scan as a range
predicate, so the scan can prune row groups and pre-filter rows before the join.

This runs *after* PredicatePushdown so the original predicates are already on the
scans and their effect is reflected in the propagated key ranges. The derived
range predicates are appended directly onto the target scan's ``predicates``
list (the same channel PredicatePushdown feeds), so no second pushdown pass is
needed; scans whose connector can't take pushed predicates get a Filter node
instead. Only inner / nested-loop joins are eligible — the pushed range is a
necessary condition for a match, which would be unsound for outer joins.
"""

from opteryx.expression import NodeType
from opteryx.models import Node
from opteryx.planner import build_literal_node
from opteryx.planner.logical_planner import LogicalPlan, LogicalPlanNode, LogicalPlanStepType
from opteryx.utils import random_string

from .optimization_strategy import (
    OptimizationStrategy,
    OptimizerContext,
    get_nodes_of_type_from_logical_plan,
)


def _phys_identity(col):
    """Column identity for a join-key identifier, for stats.columns lookup.

    Not the name: names are not unique across a plan, so a name lookup can
    silently return an unrelated relation's range."""
    if isinstance(col, bytes):
        return col
    schema_column = getattr(col, "schema_column", None)
    identity = getattr(schema_column, "identity", None) if schema_column is not None else None
    return identity if isinstance(identity, bytes) else None


def _key_value_range(stats, col):
    """Propagated value_range for *col* from a RelationStatistics, or None when
    no bound has been established (column absent / range empty)."""
    if stats is None:
        return None
    identity = _phys_identity(col)
    if identity is None:
        return None
    col_stats = stats.columns.get(identity)
    if col_stats is None:
        return None
    value_range = col_stats.value_range
    if value_range is None or (
        value_range.lower_bound is None and value_range.upper_bound is None
    ):
        return None
    return value_range


def _get_equi_join_pairs(on_node):
    """
    Extract (left_col, right_col) identifier pairs from a (possibly AND-nested) equi-join
    ON condition.  Returns an empty list for anything that isn't a col = col comparison.
    """
    if on_node is None:
        return []
    if on_node.node_type == NodeType.AND:
        return _get_equi_join_pairs(on_node.left) + _get_equi_join_pairs(on_node.right)
    if (
        on_node.node_type == NodeType.COMPARISON_OPERATOR
        and on_node.value == "Eq"
        and getattr(on_node, "left", None) is not None
        and getattr(on_node, "right", None) is not None
        and on_node.left.node_type == NodeType.IDENTIFIER
        and on_node.right.node_type == NodeType.IDENTIFIER
    ):
        return [(on_node.left, on_node.right)]
    return []


def _range_conditions(target_col, value_range):
    """Build GtEq/LtEq COMPARISON_OPERATOR condition Nodes pushing *value_range*
    (native, post-filter bounds) onto *target_col*, correctly typed."""
    target_type = getattr(getattr(target_col, "schema_column", None), "column_type", None)
    conditions = []
    if value_range.upper_bound is not None:
        conditions.append(
            Node(
                NodeType.COMPARISON_OPERATOR,
                value="LtEq",
                left=target_col,
                right=build_literal_node(value_range.upper_bound, suggested_type=target_type),
            )
        )
    if value_range.lower_bound is not None:
        conditions.append(
            Node(
                NodeType.COMPARISON_OPERATOR,
                value="GtEq",
                left=target_col,
                right=build_literal_node(value_range.lower_bound, suggested_type=target_type),
            )
        )
    return conditions


def _predicate_already_present(predicates, condition):
    """True if *predicates* already contains an equivalent (op, column, literal)."""
    op = getattr(condition, "value", None)
    col = getattr(getattr(condition, "left", None), "value", None)
    lit = getattr(getattr(condition, "right", None), "value", None)
    for existing in predicates:
        if (
            getattr(existing, "value", None) == op
            and getattr(getattr(existing, "left", None), "value", None) == col
            and getattr(getattr(existing, "right", None), "value", None) == lit
        ):
            return True
    return False


class CorrelatedFiltersStrategy(OptimizationStrategy):
    # Cost-typed so the driver propagates statistics (refresh_statistics) before
    # this runs; requires predicates already pushed onto scans so those ranges
    # show up in the propagated key statistics.
    optimization_technique = "cost"
    requires = ("predicates-pushed",)

    def visit(self, node: LogicalPlanNode, context: OptimizerContext) -> OptimizerContext:
        if not context.optimized_plan:
            context.optimized_plan = context.pre_optimized_tree.copy()  # type: ignore

        if node.node_type == LogicalPlanStepType.Join and node.type in ("inner", "nested loop"):
            join_stats = getattr(node, "statistics", None)
            if join_stats is None:
                return context

            uuid_to_nid = {}
            for nid in list(context.optimized_plan.nodes()):
                plan_node = context.optimized_plan[nid]
                node_uuid = getattr(plan_node, "uuid", None) if plan_node is not None else None
                if node_uuid:
                    uuid_to_nid[node_uuid] = nid

            for left_key, right_key in _get_equi_join_pairs(node.on):
                left_range = _key_value_range(join_stats, left_key)
                right_range = _key_value_range(join_stats, right_key)
                # Each key's realized range constrains the *other* leg's key.
                if left_range is not None:
                    self._push_range(context, node, right_key, left_range, uuid_to_nid)
                if right_range is not None:
                    self._push_range(context, node, left_key, right_range, uuid_to_nid)

        return context

    def _push_range(self, context, join_node, target_col, value_range, uuid_to_nid):
        """Push *value_range* onto *target_col*'s scan(s): append to the scan's
        predicate list when the connector supports it, else add a Filter node."""
        target_relation = getattr(target_col, "source", None)
        if target_relation in (join_node.left_relation_names or []):
            readers = join_node.left_readers or []
        elif target_relation in (join_node.right_relation_names or []):
            readers = join_node.right_readers or []
        else:
            return

        conditions = _range_conditions(target_col, value_range)
        if not conditions:
            return

        for reader_uuid in readers:
            reader_nid = uuid_to_nid.get(reader_uuid)
            if reader_nid is None:
                continue
            scan = context.optimized_plan[reader_nid]
            if scan is None:
                continue

            # AVAILABILITY GUARD: a leg's relation names include DERIVED relations
            # (a CROSS JOIN UNNEST contributes a synthetic `$unnest-*` schema), but
            # its readers are only the base scans. Pushing a range on a derived
            # column onto a base scan attaches a predicate to a relation that does
            # not produce it — the scan's predicate resolver then dies with a
            # KeyError on the unresolvable identity. Only push onto the reader that
            # IS the target column's relation.
            scan_names = {getattr(scan, "alias", None), getattr(scan, "relation", None)}
            if target_relation not in scan_names:
                continue

            connector = getattr(scan, "connector", None)
            if connector is not None and getattr(connector, "supports_predicate_pushdown", False):
                if not scan.predicates:
                    scan.predicates = []
                for condition in conditions:
                    if not _predicate_already_present(scan.predicates, condition):
                        scan.predicates.append(condition)
                        self.telemetry.optimization_inner_join_correlated_filter += 1
            else:
                # Fallback for non-pushdown connectors: a Filter node still
                # filters at execution, just without row-group pruning.
                for condition in conditions:
                    filter_node = LogicalPlanNode(
                        node_type=LogicalPlanStepType.Filter,
                        condition=condition,
                        columns=[target_col],
                        relations={target_relation},
                        all_relations={target_relation},
                    )
                    context.optimized_plan.insert_node_after(
                        random_string(), filter_node, reader_nid
                    )
                    self.telemetry.optimization_inner_join_correlated_filter += 1

    def complete(self, plan: LogicalPlan, context: OptimizerContext) -> LogicalPlan:
        # This strategy mutates scan predicates / adds Filter nodes, so the
        # statistics propagated before it are now stale; flag them so the next
        # cost strategy refreshes. (Cost strategies don't get the heuristic
        # auto-invalidation from the driver.)
        plan.statistics_are_stale = True
        return plan

    def should_i_run(self, plan):
        candidates = get_nodes_of_type_from_logical_plan(plan, (LogicalPlanStepType.Join,))
        return len(candidates) > 0
