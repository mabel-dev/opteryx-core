# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Optimization Rule - Join Rewriter

Type: Heuristic / Correctness
Goal: Faster Joins, and closing the "no RIGHT OUTER JOIN operator" native gap

Recognise the SQL anti-join idiom and rewrite to a native LEFT ANTI JOIN:

    A LEFT  OUTER JOIN B ON A.k = B.k WHERE B.k IS NULL  →  A LEFT ANTI JOIN B
    A RIGHT OUTER JOIN B ON A.k = B.k WHERE A.k IS NULL  →  B LEFT ANTI JOIN A

The rewrite is sound iff:

  1. The Filter sits directly above the OUTER JOIN.
  2. The Filter condition is exactly an IS NULL on a column from the
     non-preserved side.
  3. The IS NULL column is a join key (appears in the ON condition). Without
     this, NULLs may originate from the source data rather than from no-match
     semantics.
  4. No node above the join references columns from the non-preserved side —
     the runtime LEFT ANTI operator emits only the preserved side.

The runtime support already exists (filter_join.pyx, "left anti").

Second, unconditional rewrite: any RIGHT OUTER JOIN that survives the pass
above (i.e. didn't match the anti-join idiom) is canonicalised to LEFT OUTER
with its legs swapped:

    A RIGHT OUTER JOIN B ON ...  →  B LEFT OUTER JOIN A ON ...  (legs swapped)

"RIGHT OUTER JOIN B preserving A's unmatched rows" and "LEFT OUTER JOIN A
preserving A's unmatched rows" are the same relation with the leg labels
swapped — there is no native RIGHT OUTER operator (the engine only implements
LEFT OUTER: the preserved leg is always the probe), so every right-outer join
must be expressed this way to run at all. This is a correctness rewrite, not
a cost heuristic — it always applies, regardless of join size.
"""

from opteryx.expression import NodeType
from opteryx.expression import get_all_nodes_of_type
from opteryx.planner.logical_planner import LogicalPlan
from opteryx.planner.logical_planner import LogicalPlanNode
from opteryx.planner.logical_planner import LogicalPlanStepType

from .join_elimination import _right_columns_used_above
from .optimization_strategy import OptimizationStrategy
from .optimization_strategy import OptimizerContext
from .optimization_strategy import flip_join_leg_labels
from .optimization_strategy import get_nodes_of_type_from_logical_plan


def _column_identity(node):
    schema_column = getattr(node, "schema_column", None)
    return getattr(schema_column, "identity", None) if schema_column is not None else None


def _split_and(node):
    """Flatten a chain of AND nodes into a list of conjuncts."""
    if node is None:
        return []
    if node.node_type == NodeType.NESTED:
        return _split_and(node.centre)
    if node.node_type != NodeType.AND:
        return [node]
    return _split_and(node.left) + _split_and(node.right)


def _build_and(conjuncts):
    """Rebuild an AND tree from a list of conjuncts; returns None if empty, the
    sole conjunct if exactly one, otherwise a left-deep AND tree."""
    from opteryx.models import Node

    if not conjuncts:
        return None
    if len(conjuncts) == 1:
        return conjuncts[0]
    result = conjuncts[0]
    for c in conjuncts[1:]:
        n = Node(node_type=NodeType.AND)
        n.left = result
        n.right = c
        result = n
    return result


def _is_isnull(node):
    return (
        node is not None
        and node.node_type == NodeType.UNARY_OPERATOR
        and node.value == "IsNull"
    )


class JoinRewriteStrategy(OptimizationStrategy):
    def visit(self, node: LogicalPlanNode, context: OptimizerContext) -> OptimizerContext:
        if not context.optimized_plan:
            context.optimized_plan = context.pre_optimized_tree.copy()  # type: ignore

        if node.node_type == LogicalPlanStepType.Filter and node.condition is not None:
            conjuncts = _split_and(node.condition)
            if any(_is_isnull(c) for c in conjuncts):
                context.collected_predicates.append((context.node_id, node))

        return context

    def complete(self, plan: LogicalPlan, context: OptimizerContext) -> LogicalPlan:
        for filter_nid, filter_node in context.collected_predicates:
            # The filter must sit directly above a single OUTER JOIN.
            ingoing = list(plan.ingoing_edges(filter_nid))
            if len(ingoing) != 1:
                continue
            join_nid = ingoing[0][0]
            join_node = plan[join_nid]
            if join_node.node_type != LogicalPlanStepType.Join:
                continue
            original_type = join_node.type
            if original_type not in ("left outer", "right outer"):
                continue
            if not join_node.on:
                continue

            # Determine non-preserved side (the side that gets nulls when no match).
            if original_type == "left outer":
                non_preserved_relations = set(join_node.right_relation_names or [])
            else:  # right outer
                non_preserved_relations = set(join_node.left_relation_names or [])
            if not non_preserved_relations:
                continue

            # Find an IS NULL conjunct that targets a join-key column on the
            # non-preserved side. Other conjuncts stay as a residual filter.
            join_key_identities = {
                _column_identity(k)
                for k in get_all_nodes_of_type(join_node.on, (NodeType.IDENTIFIER,))
            }
            conjuncts = _split_and(filter_node.condition)
            anti_idx = None
            for i, c in enumerate(conjuncts):
                if not _is_isnull(c):
                    continue
                col = c.centre
                if col is None or col.node_type != NodeType.IDENTIFIER:
                    continue
                if getattr(col, "source", None) not in non_preserved_relations:
                    continue
                if _column_identity(col) not in join_key_identities:
                    continue
                anti_idx = i
                break
            if anti_idx is None:
                continue

            # Anti-join only emits the preserved side. If anything above the
            # filter references the non-preserved side, the rewrite would
            # silently drop those columns from the row shape. We walk from
            # filter_nid (not join_nid) because the filter itself contains the
            # IS NULL reference we're consuming.
            if _right_columns_used_above(plan, filter_nid, non_preserved_relations):
                continue

            # Rewrite: convert to LEFT ANTI, swapping sides if RIGHT OUTER so the
            # preserved side becomes the probe (FilterJoinNode's `left_columns`).
            join_node.type = "left anti"
            if original_type == "right outer":
                join_node.left_relation_names, join_node.right_relation_names = (
                    join_node.right_relation_names,
                    join_node.left_relation_names,
                )
                join_node.left_columns, join_node.right_columns = (
                    join_node.right_columns,
                    join_node.left_columns,
                )
                left_readers = getattr(join_node, "left_readers", None)
                right_readers = getattr(join_node, "right_readers", None)
                join_node.left_readers, join_node.right_readers = right_readers, left_readers
                flip_join_leg_labels(plan, join_nid)

            plan[join_nid] = join_node

            # If the filter had only the IS NULL conjunct, remove it. Otherwise
            # rewrite it without the IS NULL conjunct — the residual stays above
            # the new anti-join.
            residual = [c for i, c in enumerate(conjuncts) if i != anti_idx]
            if not residual:
                plan.remove_node(filter_nid, heal=True)
            else:
                filter_node.condition = _build_and(residual)
                filter_node.columns = get_all_nodes_of_type(
                    filter_node.condition, select_nodes=(NodeType.IDENTIFIER,)
                )
                plan[filter_nid] = filter_node

            self.telemetry.optimization_join_rewrite_anti += 1

        # Unconditional canonicalisation: any RIGHT OUTER join not already turned
        # into a LEFT ANTI above (which itself does not stay "right outer") is
        # rewritten to LEFT OUTER with its legs swapped — see module docstring.
        # This always runs (not gated by size/idiom-matching): it's the only way
        # a right-outer join can execute at all, since the engine has no native
        # RIGHT OUTER mode.
        for join_nid, join_node in get_nodes_of_type_from_logical_plan(
            plan, (LogicalPlanStepType.Join,)
        ):
            if join_node.type != "right outer":
                continue
            join_node.type = "left outer"
            join_node.left_relation_names, join_node.right_relation_names = (
                join_node.right_relation_names,
                join_node.left_relation_names,
            )
            join_node.left_columns, join_node.right_columns = (
                join_node.right_columns,
                join_node.left_columns,
            )
            left_readers = getattr(join_node, "left_readers", None)
            right_readers = getattr(join_node, "right_readers", None)
            join_node.left_readers, join_node.right_readers = right_readers, left_readers
            flip_join_leg_labels(plan, join_nid)
            plan[join_nid] = join_node
            self.telemetry.optimization_join_rewrite_right_to_left_outer += 1

        return plan

    def should_i_run(self, plan):
        candidates = get_nodes_of_type_from_logical_plan(plan, (LogicalPlanStepType.Join,))
        return any(node.type in ("left outer", "right outer") for _, node in candidates)
