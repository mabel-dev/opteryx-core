# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Optimization Rule - Projection Pushdown

Type: Heuristic
Goal: Limit columns which need to be moved around

We bind from the the scans, exposing the available columns to each operator
as we make our way to the top of the plan (usually the SELECT). The projection
pushdown is done as part of the optimizers, but isn't quite like the other
optimizations; this is collecting used column information as it goes from the
top of the plan down to the selects. The other optimizations tend to move or
remove operations, or update what a step does, this is just collecting and
updating the used columns.
"""

from typing import Set

from opteryx.expression import NodeType
from opteryx.expression import get_all_nodes_of_type
from opteryx.models import LogicalColumn
from opteryx.planner.logical_planner import LogicalPlan
from opteryx.planner.logical_planner import LogicalPlanNode
from opteryx.planner.logical_planner import LogicalPlanStepType

from .optimization_strategy import OptimizationStrategy
from .optimization_strategy import OptimizerContext


class ProjectionPushdownStrategy(OptimizationStrategy):
    provides = ("projection-pushed",)

    def visit(self, node: LogicalPlanNode, context: OptimizerContext) -> OptimizerContext:
        """
        Optimize the given node by pushing projections down in the plan.

        Args:
            node: The current node in the logical plan to be optimized.
            context: The context carrying the state and information for optimization.

        Returns:
            The updated context, including updated node information.
        """
        node.pre_update_columns = set(context.collected_identities)

        # If we're at a union, it changes what we think we know about the columns.
        if node.node_type == LogicalPlanStepType.Union:
            context.seen_unions += 1
        if node.node_type == LogicalPlanStepType.Distinct:
            context.seen_distincts += 1

        # If we're at the something other than the top project (e.g. in a subquery)
        # in a plan we may be able to remove some columns (and potentially some
        # evaluations) if the columns aren't referenced in the outer query.
        if node.node_type == LogicalPlanStepType.Project:
            if (
                context.seen_unions == 0
                and context.seen_projections > 0
                and context.seen_distincts == 0
            ):
                node.columns = [
                    n for n in node.columns if n.schema_column.identity in node.pre_update_columns
                ]
            if context.seen_unions == 0:
                context.seen_projections += 1
            context.seen_distincts = 0

        # Subqueries act like all columns are referenced
        if node.node_type != LogicalPlanStepType.Subquery:
            if node.columns:  # Assumes node.columns is an iterable or None
                collected_columns = self.collect_columns(node)
                context.collected_identities.update(collected_columns)

        # READ_JSONL and READ_PARQUET are included narrowly (by function name, not
        # node type alone): unlike VALUES/UNNEST/GENERATE_SERIES -- the other
        # LogicalPlanStepType.FunctionDataset shapes -- they have a real backing
        # reader (rugo's JSONL decoder / the native ParquetReadNode) that can honor
        # a reduced column list, so pruning `node.columns` here is safe and lets
        # physical_planner push the surviving projection into the scan. The other
        # FunctionDataset kinds are deliberately left out: they build a single
        # in-memory Morsel from `node.columns` directly and pruning their behavior
        # hasn't been vetted against this pass.
        is_pushable_function_dataset = (
            node.node_type == LogicalPlanStepType.FunctionDataset
            and getattr(node, "function", None) in ("READ_JSONL", "READ_PARQUET")
        )
        if (
            (
                node.node_type
                in (
                    LogicalPlanStepType.Scan,
                    LogicalPlanStepType.Subquery,
                    LogicalPlanStepType.Union,
                )
                or is_pushable_function_dataset
            )
            and getattr(node.schema, "columns", None) is not None
        ):
            # Push all of the projections
            node_columns = [
                LogicalColumn(
                    node_type=NodeType.IDENTIFIER,
                    source_column=col.name,
                    source=(col.origin[0] if col.origin else None),
                    schema_column=col,
                )
                for col in node.schema.columns
                if col.identity in context.collected_identities
            ]

            # A Scan/Subquery leg of a UNION whose own Project was pruned away
            # (bare `SELECT *`, no Project node at all) has a schema with its OWN,
            # independently-minted column identities (mint_column_identity is
            # random per relation, by design). `context.collected_identities` at
            # this point may instead hold the SIBLING leg's identities: identity
            # collection resets only at a Project node, and this leg has none.
            # `node_columns` then comes back short even though the full width is
            # needed — the previous behaviour silently pushed too few columns onto
            # this leg while its sibling kept its correct width, and the physical
            # compiler's positional UNION alignment then rejected the plan with
            # "a UNION leg narrower than the union schema".
            #
            # The under-match is not always EMPTY: a `WHERE` clause on this same
            # leg has its own Filter node, which (unlike Scan) already carries
            # `.columns` at generic-collection time (line ~74 above) — one of
            # THIS leg's own genuinely-correct identities (e.g. the predicate's
            # `id`) gets folded into `collected_identities` in passing, so the
            # identity intersection below can return a small NON-empty result
            # that is still wrong (verified: 1-of-20 matched, silently, via the
            # filter's `id` alone) — checking for emptiness alone misses this.
            # The only inherently sound signal is width: a UNION requires equal
            # arity across every leg by construction (see compiler.py — one
            # width is chosen and applied positionally to all legs), so ANY
            # match narrower than the union's own resolved width is a bug,
            # never a legitimate result — there is no query for which a leg is
            # SUPPOSED to contribute fewer columns than its siblings.
            #
            # UNION output is positional, not identity-matched — see compiler.py's
            # UnionNode handling ("each leg's first N columns become the union's
            # column_ids"). So an under-matched leg must supply its own first N
            # schema columns, not an identity intersection against a possibly-
            # foreign identity set. N is the width the UNION node itself was
            # just pruned to (stashed below when node_type is Union).
            #
            # A Scan/Subquery reached via its OWN Project (which re-seeds
            # collected_identities correctly before reaching its Scan) already
            # matches the full width through the ordinary identity path above
            # and is unaffected — this only fires on an actual shortfall.
            #
            # Known gap: does not re-derive N as each nested union's own width if
            # a UNION leg itself contains a further UNION — context.bag holds one
            # slot, last-write-wins. No known query pattern in the current test
            # suite exercises that; flagged rather than solved speculatively.
            if (
                node.node_type in (LogicalPlanStepType.Scan, LogicalPlanStepType.Subquery)
                and context.seen_unions > 0
                and node.schema.columns
            ):
                width = context.bag.get("_union_leg_width", len(node.schema.columns))
                if len(node_columns) < width:
                    node_columns = [
                        LogicalColumn(
                            node_type=NodeType.IDENTIFIER,
                            source_column=col.name,
                            source=(col.origin[0] if col.origin else None),
                            schema_column=col,
                        )
                        for col in node.schema.columns[:width]
                    ]

            # Update the node with the pushed columns
            node.columns = node_columns

            if node.node_type == LogicalPlanStepType.Union:
                context.bag["_union_leg_width"] = len(node_columns)

        if node.node_type == LogicalPlanStepType.Join:
            node_columns = []

            for schema in node.schemas.values():
                node_columns.extend(
                    [
                        LogicalColumn(
                            node_type=NodeType.IDENTIFIER,
                            source_column=col.name,
                            source=(col.origin[0] if col.origin else None),
                            schema_column=col,
                        )
                        for col in schema.columns
                        if col.identity in node.pre_update_columns
                    ]
                )

            # Update the node with the pushed columns
            node.columns = node_columns

        context.optimized_plan.add_node(context.node_id, LogicalPlanNode(**node.properties))
        if context.parent_nid:
            # Re-adding the edge must preserve its relationship: a join leg label
            # records which side of the parent join this branch feeds. Read it from
            # the pre-optimized tree — `optimized_plan` is rebuilt top-down from
            # empty, so the edge does not exist in it yet.
            context.optimized_plan.add_edge(
                context.node_id,
                context.parent_nid,
                context.pre_optimized_tree.relationship(context.node_id, context.parent_nid),
            )

        return context

    def complete(self, plan: LogicalPlan, context: OptimizerContext) -> LogicalPlan:
        # No finalization needed for this strategy
        return plan

    def collect_columns(self, node: LogicalPlanNode) -> Set[str]:
        """
        Collect and return the set of column identities from the given node.

        Args:
            node: The node from which to collect column identities.

        Returns:
            A set of column identities.
        """
        identities = set()
        for column in node.columns or []:  # Ensuring that node.columns is iterable
            if column.node_type == NodeType.IDENTIFIER and column.schema_column:
                identities.add(column.schema_column.identity)
            else:
                identities.update(
                    col.schema_column.identity
                    for col in get_all_nodes_of_type(column, (NodeType.IDENTIFIER,))
                    if col.schema_column
                )

        # A ranking Window node reads its PARTITION BY / ORDER BY columns at
        # execution time even though they are not in node.columns (which holds only
        # the emitted ranking outputs). They must not be pruned from the input.
        if node.node_type == LogicalPlanStepType.Window:
            for col in node.partition_by or []:
                if col.schema_column:
                    identities.add(col.schema_column.identity)
            for col, _ in node.order_by or []:
                if col.schema_column:
                    identities.add(col.schema_column.identity)

        return identities
