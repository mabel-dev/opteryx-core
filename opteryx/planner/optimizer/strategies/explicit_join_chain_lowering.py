# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Optimization Rule - Explicit Join Chain Lowering

Type: Heuristic
Goal: Let a chain of explicit ``INNER JOIN ... ON`` reach the join orderer, which only
      orders chains of implicit cross joins

Order:
    After:  JoinKeyMaterialization (expression keys are real columns, so every ON
            conjunct is a column = column equality), JoinConditionHoist and
            SplitConjunctivePredicates (single-leg ON conjuncts are already Filters).
    Before: JoinPlanning, which then finds an ordinary cross-join chain; the statistics
            refresh it triggers (the plan changed) sees the WHERE filters through the
            cross joins; and CrossJoinFilterPushdown, which keys the shells again.

Rewrite:
    FROM a INNER JOIN b ON a.x = b.x INNER JOIN c ON b.y = c.y WHERE ...
    ->
    FROM a CROSS JOIN b CROSS JOIN c WHERE a.x = b.x AND b.y = c.y AND ...
        -- the ON equalities become one Filter each, directly above the chain top

For an INNER join ``ON k`` and ``WHERE k`` are the same predicate, so the rewrite is exact
whatever order the joins are later put in.

Why this is not folded into JoinPlanning
----------------------------------------
Costing the chain as it stands is blind. ``statistics_refresh.hpp`` attributes a WHERE
conjunct to a leaf scan only through transparent steps, and an INNER join is
deliberately not one (see ``leaf_local_conjuncts``), so every dimension filter is
invisible and DPccp costs ``part`` at 200,000 rows instead of 80,000. SSB Q4.2 then
joins the 6M-row fact to its least selective dimension first and runs 1.7x slower. The
chain has to be cross joins and Filters BEFORE the refresh that JoinPlanning triggers,
which is the shape a comma join already has.

Scope: a chain of at least three relations (two joins), every join a plain
``INNER JOIN ... ON`` whose conjuncts are all column = column. Each refusal is recorded.
A chain of one join has no order to choose and records nothing. An outer join, a
semi/anti join, a USING / ASOF / band join, or a join with a residual bounds the chain;
a chain with a theta (non-equality) conjunct is declined, because lowered it would be
a Filter over joined rows rather than a per-pair residual.

If JoinPlanning then declines, CrossJoinFilterPushdown re-keys the shells in their
original order, exactly as it does for a comma join the orderer declined: every ON
conjunct is an equality, and an ON clause can only name relations to its left.
"""

from typing import List
from typing import Tuple

from opteryx.compiled.planner.plan_graph import EdgeRole
from opteryx.compiled.structures.plan_steps import FilterStep
from opteryx.expression import NodeType
from opteryx.planner.logical_planner.logical_planner import LogicalPlan
from opteryx.planner.logical_planner.logical_planner import LogicalPlanStepType
from opteryx.planner.logical_planner.logical_planner import PlanStep

from .optimization_strategy import OptimizationStrategy
from .optimization_strategy import OptimizerContext
from .optimization_strategy import filter_referenced_columns

# One join is two relations: nothing to order. Which side builds is JoinAlgorithm's call.
_MIN_CHAIN_JOINS = 2


def _is_explicit_inner_join(node: PlanStep) -> bool:
    """A plain ``a INNER JOIN b ON k``: the only shape a Filter above a cross join expresses
    exactly. Every other field an inner join can carry (USING, ASOF/band legs, a residual,
    an existence column, a window join, a group-join or key-reducer annotation) is work a
    Filter cannot do, so such a join is not lowerable and bounds the chain."""
    return (
        node.node_type == LogicalPlanStepType.Join
        and node.type == "inner"
        and node.on is not None
        and not node.using
        and not node.is_window_join
        and node.residual is None
        and node.asof_condition is None
        and node.band_column is None
        and node.existence_column is None
        and node.setop_leg_columns is None
        and node.groupjoin is None
        and node.key_reducer is None
    )


def _is_left_child_of_explicit_join(plan: LogicalPlan, nid: str) -> bool:
    return any(
        role == EdgeRole.LEFT and _is_explicit_inner_join(plan[parent_id])
        for _, parent_id, role in plan.outgoing_edges(nid)
    )


def _collect_chain_top_down(plan: LogicalPlan, top_id: str) -> List[Tuple[str, PlanStep]]:
    """Walk the LEFT spine of explicit inner joins down from ``top_id``. A RIGHT child is a
    leaf even when it is itself a join: its own chain is lowered on its own."""
    chain: List[Tuple[str, PlanStep]] = []
    current_id = top_id
    while True:
        node = plan[current_id]
        if not _is_explicit_inner_join(node):
            break
        chain.append((current_id, node))
        left_children = [c for c, _, role in plan.ingoing_edges(current_id) if role == EdgeRole.LEFT]
        if len(left_children) != 1 or not _is_explicit_inner_join(plan[left_children[0]]):
            break
        current_id = left_children[0]
    return chain


def _flatten_on(node):
    """The AND-conjuncts of an ON clause, parentheses removed."""
    while node.node_type == NodeType.NESTED:
        node = node.centre
    if node.node_type == NodeType.AND:
        return _flatten_on(node.left) + _flatten_on(node.right)
    return [node]


def _chain_equalities(chain: List[Tuple[str, PlanStep]]):
    """``(conjuncts, None)`` when every ON conjunct of every chain join is a column = column
    equality, else ``(None, reason)``."""
    conjuncts = []
    for join_id, join in chain:
        for conjunct in _flatten_on(join.on):
            if not (
                conjunct.node_type == NodeType.COMPARISON_OPERATOR
                and conjunct.value == "Eq"
                and conjunct.left.node_type == NodeType.IDENTIFIER
                and conjunct.right.node_type == NodeType.IDENTIFIER
            ):
                return None, f"ON clause of join {join_id} has a conjunct that is not column = column"
            conjuncts.append(conjunct)
    return conjuncts, None


def _to_cross_shell(join: PlanStep) -> None:
    """The same field resets JoinPlanning's ``_apply_join_tree`` makes: relation names,
    readers, schemas and edges stay, the join keeps no condition."""
    join.type = "cross join"
    join.on = None
    join.using = None
    join.left_columns = None
    join.right_columns = None
    join.columns = None


def _filter_above(plan: LogicalPlan, top_id: str, condition) -> None:
    """A Filter for one lowered conjunct, directly above the chain top where
    SplitConjunctivePredicates leaves a WHERE conjunct over a comma-join chain. Directly
    above the top, not at WHERE level: a chain under an outer join keeps the conjunct on
    the inner side of it. Built as JoinConditionHoist builds its hoisted Filter."""
    filter_node = FilterStep()
    filter_node.condition = condition
    filter_node.columns = filter_referenced_columns(condition)
    relations = set()
    for column in filter_node.columns:
        if column.source is not None:
            relations.add(column.source)
        if column.schema_column is not None:
            relations.update(column.schema_column.origin or [])
    filter_node.relations = relations
    filter_node.all_relations = set(plan[top_id].all_relations or ())
    plan.insert_node_after(filter_node, top_id)


class ExplicitJoinChainLoweringStrategy(OptimizationStrategy):
    optimization_technique = "heuristic"

    def visit(self, node: PlanStep, context: OptimizerContext) -> OptimizerContext:
        return context

    def complete(self, plan: LogicalPlan, context: OptimizerContext) -> LogicalPlan:
        tops = [
            nid
            for nid, node in plan.nodes(True)
            if _is_explicit_inner_join(node) and not _is_left_child_of_explicit_join(plan, nid)
        ]
        for top_id in tops:
            chain = _collect_chain_top_down(plan, top_id)
            if len(chain) < _MIN_CHAIN_JOINS:
                continue
            label = f"explicit join chain lowering ({top_id})"
            conjuncts, refusal = _chain_equalities(chain)
            if conjuncts is None:
                self.record_decision(label, f"declined, {refusal}: {len(chain)} join(s) in chain")
                continue
            # The Filters are inserted before the shells are converted: both touch only the
            # chain top's outgoing edge and the joins' own fields, and a raise in either
            # leaves nothing half-lowered that a later strategy would read.
            for join_id, join in chain:
                _to_cross_shell(join)
                plan[join_id] = join
            for conjunct in conjuncts:
                _filter_above(plan, top_id, conjunct)
            self.record_decision(
                label, f"lowered to cross joins plus {len(conjuncts)} filter(s): {len(chain) + 1} relations"
            )
        return plan

    def should_i_run(self, plan: LogicalPlan) -> bool:
        return any(
            _is_explicit_inner_join(node) and _is_left_child_of_explicit_join(plan, nid)
            for nid, node in plan.steps_of((LogicalPlanStepType.Join,))
        )
