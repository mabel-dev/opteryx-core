# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Optimization Strategy: Join Key Materialization

Turns an ON-clause equality whose operand is an EXPRESSION into an ordinary
equi-join by projecting that expression as a real column on the leg it belongs
to:

    FROM flows f INNER JOIN lookups l ON CAST(f.client AS VARCHAR) = l.client
    →
    FROM (SELECT *, CAST(client AS VARCHAR) FROM flows) f
         INNER JOIN lookups l ON <projected column> = l.client

which is the CTE rewrite a user would otherwise have to write by hand. The
motivating shape is a key carrying different types on the two sides — an IPV4
`src_addr` against a VARCHAR `client` that holds hostnames as well as addresses —
where the cast is the whole point and cannot be optimised away.

This module also owns the two plan-mutation primitives
`cross_join_filter_pushdown` uses for the same job on the WHERE-clause form
(`FROM a, b WHERE a.x = b.y - 53`). One materialisation, two entry points: the
strategies differ in WHICH conjuncts they feed it, never in what a materialised
key looks like.

WHAT IT DOES NOT DO
    Nothing here decides whether an operand may be hoisted. That is
    `join_helpers.hoistable_operand_leg` / `plan_join_key_hoists`, shared with the
    Binder, precisely so the phase that REJECTS and the phase that REWRITES cannot
    drift apart. If this strategy is disabled, a join the Binder let through
    reaches the compiler with an unkeyed conjunct and is refused there — loudly,
    with a worse message. It never silently becomes a different join.
"""

from typing import List, Optional

from opteryx.expression import NodeType, get_all_nodes_of_type
from opteryx.models import LogicalColumn, Node
from opteryx.planner.binder.join_helpers import extract_join_fields
from opteryx.planner.binder.join_helpers import plan_join_key_hoists
from opteryx.planner.logical_planner import LogicalPlan
from opteryx.planner.logical_planner import LogicalPlanNode
from opteryx.planner.logical_planner import LogicalPlanStepType
from opteryx.utils import random_string

from .optimization_strategy import OptimizationStrategy
from .optimization_strategy import OptimizerContext


def passthrough_column(schema_column, source: Optional[str] = None) -> LogicalColumn:
    """A bare IDENTIFIER referencing an already-computed column by identity.

    `source` defaults to the column's own origin, but the caller may pin it
    explicitly -- needed for a freshly materialised column, whose relation
    membership is which SIDE of the join it was projected onto (one of that
    side's `left_relation_names`/`right_relation_names` entries), not
    anything `schema_column.origin` (an expression column has none) could
    supply.
    """
    return LogicalColumn(
        node_type=NodeType.IDENTIFIER,
        source_column=schema_column.name,
        source=source
        if source is not None
        else (schema_column.origin[0] if getattr(schema_column, "origin", None) else None),
        schema_column=schema_column,
    )


def materialize_operand_as_column(
    plan: LogicalPlan, child_id: str, expr: Node, relation_names: List[str]
) -> Optional[Node]:
    """Insert a Project above `child_id` that emits everything it already
    emits PLUS `expr` as a new column. Returns a passthrough IDENTIFIER
    referencing that new column, or None if `child_id`'s output schema
    isn't available (defensive -- leaves the plan untouched).

    `expr` keeps the `schema_column` it was bound with -- no new identity is
    minted here, so every downstream consumer keyed by that identity (join-key
    extraction, statistics) resolves to the same column this projects.

    The returned reference's `.source` is pinned to `relation_names[0]` --
    any member of the join leg's own relation-name list is a valid match for
    `extract_join_fields`'s `source in left/right_relation_names` check;
    which specific member is irrelevant, only set membership is.

    TRAP: the column this projects carries the leg's relation name as `.source`
    while its identity is produced HERE, above the scan -- not by the scan. A
    consumer that pushes something keyed by that identity down onto the scan asks
    a reader for a column it never emits. `correlated_filters`' IDENTITY GUARD is
    the worked example; it tests the identity against the scan's own output schema
    rather than trusting the relation name.
    """
    schema = getattr(plan[child_id], "schema", None)
    columns = getattr(schema, "columns", None)
    if not columns or not relation_names:
        return None
    project_columns: List[Node] = [passthrough_column(col) for col in columns]
    project_columns.append(expr)
    project_node = LogicalPlanNode(node_type=LogicalPlanStepType.Project)
    project_node.columns = project_columns
    project_node.passthrough_columns = []
    plan.insert_node_after(random_string(), project_node, child_id)
    return passthrough_column(expr.schema_column, source=relation_names[0])


def split_and_conditions(node: Optional[Node]) -> List[Node]:
    """Flatten an ON tree's AND spine into its leaf conjuncts.

    Only the AND spine is walked: an OR anywhere in an ON clause is ONE leaf here
    and carries no extractable equi key, which is the correct answer for it.

    Shared with `cross_join_filter_pushdown` rather than copied into it. Three
    walkers of this shape already exist in the tree (here, `cross_join_chain_reorder`,
    and `compiler._and_conjuncts`) and a divergent fourth is how a conjunct starts
    being read by one consumer and missed by another.
    """
    if node is None:
        return []
    if node.node_type != NodeType.AND:
        return [node]
    return split_and_conditions(node.left) + split_and_conditions(node.right)


class JoinKeyMaterializationStrategy(OptimizationStrategy):
    """
    Optimization Rule - Join Key Materialization

    Pattern:
        A INNER JOIN B ON <expression over A> = B.key

    Converts to:
        A -> Project(A.*, <expression over A>) INNER JOIN B ON <projected> = B.key

    Impact:
        Correctness of PLANNING, not a cost decision: without it the join has no
        usable key and the engine refuses it. Once keyed it is an ordinary hash
        join instead of the cartesian product a residual filter would need.
    """

    def visit(self, node: LogicalPlanNode, context: OptimizerContext) -> OptimizerContext:
        return context

    def complete(self, plan: LogicalPlan, context: OptimizerContext) -> LogicalPlan:
        """All graph mutation happens here, never from `visit`.

        Inserting a Project mid-traversal mutates a node's outgoing edge while
        ancestors are still unvisited, and expression subtrees are shared between
        nodes — the failure mode that de-mutated predicate_pushdown's Project and
        Aggregate branches. The tree is settled by the time `complete` runs and the
        resulting plan is identical.
        """
        join_ids = [
            nid
            for nid, node in plan.nodes(True)
            if node.node_type == LogicalPlanStepType.Join and node.on is not None
        ]

        for join_id in join_ids:
            self._materialize_keys(plan, join_id)

        return plan

    def _materialize_keys(self, plan: LogicalPlan, join_id: str) -> None:
        node = plan[join_id]
        left_relations = node.left_relation_names or []
        right_relations = node.right_relation_names or []
        if not left_relations or not right_relations:
            return

        # The leg is chosen by EDGE LABEL. `left_relation_names`/
        # `right_relation_names` are the join's own bookkeeping — every relation
        # folded into that leg, including ones a Subquery boundary hides — which a
        # child-subtree relation-name walk does not reproduce; those lists say which
        # names COUNT as that leg, not which child is it.
        children = {
            relationship: child_id
            for child_id, _, relationship in plan.ingoing_edges(join_id)
        }
        if "left" not in children or "right" not in children:
            return

        rewrote = False
        for conjunct in split_and_conditions(node.on):
            hoists = plan_join_key_hoists(conjunct, left_relations, right_relations)
            if hoists is None:
                continue
            for expression, leg in hoists:
                relations = left_relations if leg == "left" else right_relations
                reference = materialize_operand_as_column(
                    plan, children[leg], expression, relations
                )
                if reference is None:
                    continue
                # Replace the operand IN PLACE. Which side of the comparison it
                # sits on is not implied by the leg — `l.client = CAST(f.client)`
                # puts the left leg's expression on the RIGHT of the Eq.
                if conjunct.left is expression:
                    conjunct.left = reference
                else:
                    conjunct.right = reference
                self.telemetry.optimization_join_key_materialized += 1
                rewrote = True

        if not rewrote:
            return

        # Rebuild the join's bookkeeping from the rewritten condition. Every
        # conjunct we touched is now bare-identifier on both sides, so this is the
        # ordinary path — a conjunct that stays unkeyed is refused by the compiler
        # rather than silently dropped.
        node.left_columns, node.right_columns, _unkeyed = extract_join_fields(
            node.on, left_relations, right_relations
        )
        node.columns = get_all_nodes_of_type(node.on, (NodeType.IDENTIFIER,))
        plan[join_id] = node

    def should_i_run(self, plan: LogicalPlan) -> bool:
        for node in plan._nodes.values():
            if node.node_type == LogicalPlanStepType.Join and node.on is not None:
                return True
        return False
