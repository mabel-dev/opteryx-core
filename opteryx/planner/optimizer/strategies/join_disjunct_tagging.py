# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Optimization Rule - Join Disjunct Tagging

Type: Cost
Goal: Evaluate the single-leg branches of an OR that sits above an INNER join on
      the leg that owns them, so the join carries one bit per branch instead of
      the columns the branch reads.

Order:
    After:  PredicatePushdown (the OR this rewrites is the one pushdown could not
            place on a leg and restored above the join) and
            CrossJoinFilterPushdown (which turns `FROM a, b WHERE ...` into the
            INNER joins this matches).
    Before: ProjectionPushdown (which prunes a branch's source columns out of the
            scan once only the tag reads them).

Rewrite:
    Filter(a.x LIKE '%p%' OR b.y LIKE '%q%')
      Join(a.k = b.k)
        Scan a
        Scan b
    ->
    Filter(a.<tag> OR b.<tag>)
      Join(a.k = b.k)
        Project(a.*, a.x LIKE '%p%' AS <tag>) <- Scan a
        Project(b.*, b.y LIKE '%q%' AS <tag>) <- Scan b

A branch's NULL is preserved: the tag column is the branch's own three-valued
result, and the OR above it is the same OR — only WHERE its operands are
evaluated moves, not what they evaluate to.

Top-level OR branches ONLY, by measurement (2026-11-05): a walk that also tagged
the single-leg conjuncts inside straddling AND branches reached TPC-H Q7 and Q19
and measured 1.015 / 1.019 at SF10 — bits replacing narrow columns do not pay for
the Project operator each leg gains. The win needs wide (string) columns to leave
the payload, and those arrive as whole branches.

Why: measured 2026-11-05 on the rugo TPC-H mirrors at SF1 and SF10 with the
hand-written form of this rewrite: 0.45-0.61x on orders⋈lineitem (4x fan-out)
and customer⋈orders (10x). ~80% of the win is the string columns no longer
crossing the join (the probe side gathers payload densely, see
native_join2.hpp build_output), the rest is each branch running once per source
row instead of once per joined row. 1.5x SLOWER when the join output is small
against a leg — one day of orders ⋈ lineitem pays 6M LIKEs to serve 2k joined
rows — which is what the gate is for.

Gate, per leg: est(join output) * _TAG_MARGIN >= est(leg). Missing statistics
decline. The legs are gated separately: tagging one leg's branches and leaving
the other's is still correct, just a smaller win.

Not done: a branch that straddles both legs stays where it is, and a straddling
AND branch is not peeled for the single-leg conjuncts inside it (see above). The
Filter must sit DIRECTLY above the join; a Subquery boundary between them declines.
"""

from typing import Dict, List

from opteryx.compiled.structures.expressions import Expression
from opteryx.expression import NodeType, get_all_nodes_of_type
from opteryx.expression.formatter import format_expression
from opteryx.planner.binder.join_helpers import hoistable_operand_leg
from opteryx.planner.logical_planner import LogicalPlan
from opteryx.planner.logical_planner import LogicalPlanStepType
from opteryx.planner.logical_planner import PlanStep

from .join_key_materialization import materialize_operands_as_columns
from .optimization_strategy import OptimizationStrategy
from .optimization_strategy import OptimizerContext

# Measured ratios of est(join) / est(leg): the wins sat at 1-4, the loss at 0.0004.
# 8 leaves room for estimate noise on the winning side without reaching the loss.
_TAG_MARGIN = 8

_LABEL = "join disjunct tagging"


def _or_branches(node: Expression) -> List[Expression]:
    """The OR spine's leaves, left to right. An AND is one leaf here."""
    if node.node_type != NodeType.OR:
        return [node]
    return _or_branches(node.left) + _or_branches(node.right)


def _rows(statistics, nid) -> int:
    rows = statistics.row_count(nid)
    return 0 if rows is None or rows <= 0 else rows


class JoinDisjunctTaggingStrategy(OptimizationStrategy):
    """
    Optimization Rule - Join Disjunct Tagging

    Pattern:
        Filter(<over A> OR <over B> [OR ...]) directly above A INNER JOIN B

    Converts to:
        Filter(A.<tag> OR B.<tag>) above the join, each <tag> projected on its leg

    Impact:
        Skinnier join payload and once-per-source-row branch evaluation; gated
        on the join not pruning hard against the tagged leg.
    """

    optimization_technique = "cost"
    requires = ("predicates-pushed",)

    def visit(self, node: PlanStep, context: OptimizerContext) -> OptimizerContext:
        return context

    def complete(self, plan: LogicalPlan, context: OptimizerContext) -> LogicalPlan:
        # All mutation here, never from visit(): inserting a Project mid-traversal
        # is the eager-mutation trap join_key_materialization documents.
        statistics = context.plan_context.statistics
        for filter_id, filter_node in plan.steps_of((LogicalPlanStepType.Filter,)):
            condition = filter_node.condition
            if condition is None or condition.node_type != NodeType.OR:
                continue
            producers = plan.ingoing_edges(filter_id)
            if len(producers) != 1:
                continue
            join_id = producers[0][0]
            join = plan[join_id]
            if (
                join.node_type != LogicalPlanStepType.Join
                or join.type != "inner"
                or join.on is None
            ):
                continue
            self._tag(plan, statistics, filter_id, filter_node, join_id, join)
        return plan

    def _tag(self, plan, statistics, filter_id, filter_node, join_id, join) -> None:
        left_names = join.left_relation_names or []
        right_names = join.right_relation_names or []
        if not left_names or not right_names:
            return
        left_child, right_child = plan.legs(join_id)
        children = {"left": left_child, "right": right_child}
        relations = {"left": left_names, "right": right_names}

        # Branches per leg first, so each leg gets ONE Project holding all of its
        # tags: a second Project stacked above the first would not pass the
        # first's new column through.
        per_leg: Dict[str, List[Expression]] = {"left": [], "right": []}
        for branch in _or_branches(filter_node.condition):
            leg = hoistable_operand_leg(branch, left_names, right_names)
            if leg is not None:
                per_leg[leg].append(branch)
        if not per_leg["left"] and not per_leg["right"]:
            return

        join_rows = _rows(statistics, join_id)
        if join_rows == 0:
            self.record_decision(_LABEL, "declined: no join row estimate")
            return

        replacements: Dict[int, Expression] = {}
        for leg, branches in per_leg.items():
            if not branches:
                continue
            leg_rows = _rows(statistics, children[leg])
            if leg_rows == 0:
                self.record_decision(_LABEL, f"declined {leg} leg: no row estimate")
                continue
            if join_rows * _TAG_MARGIN < leg_rows:
                self.record_decision(
                    _LABEL,
                    f"declined {leg} leg: join est {join_rows} * {_TAG_MARGIN} < leg est {leg_rows}",
                )
                continue
            references = materialize_operands_as_columns(
                plan, children[leg], branches, relations[leg]
            )
            if references is None:
                self.record_decision(_LABEL, f"declined {leg} leg: its emitted columns cannot be resolved")
                continue
            for branch, reference in zip(branches, references):
                replacements[id(branch)] = reference
                self.telemetry.optimization_join_disjunct_tagged += 1
            self.record_decision(
                _LABEL,
                f"tagged {len(branches)} on {leg} leg: join est {join_rows} >= "
                f"leg est {leg_rows} / {_TAG_MARGIN}: "
                + ", ".join(format_expression(b) for b in branches),
            )
        if not replacements:
            return

        def rewrite(expr: Expression) -> Expression:
            if expr.node_type == NodeType.OR:
                # with_fields keeps the node when nothing under it changed; a
                # changed OR is a NEW node, the condition is shared with the
                # pre-optimization plan.
                return expr.with_fields(left=rewrite(expr.left), right=rewrite(expr.right))
            return replacements.get(id(expr), expr)

        filter_node.condition = rewrite(filter_node.condition)
        filter_node.columns = get_all_nodes_of_type(filter_node.condition, (NodeType.IDENTIFIER,))
        plan[filter_id] = filter_node

    def should_i_run(self, plan: LogicalPlan) -> bool:
        for _nid, node in plan.steps_of((LogicalPlanStepType.Filter,)):
            if node.condition is not None and node.condition.node_type == NodeType.OR:
                return True
        return False
