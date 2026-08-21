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
    rebuilds_plan = True  # rebuilds the whole plan into an empty working plan
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
        # Everything BELOW an unclosed Distinct is read by that Distinct, so nothing
        # below it is dead. A Distinct with no ON dedups on EVERY column that reaches
        # it (compiler.py's DistinctNode branch: "empty on_idx == all columns"), and a
        # DISTINCT ON's key expressions are not in `node.columns`, so neither shape's
        # demand is visible in `collected_identities`. Recording the outer demand here
        # would understate it, and an operator that prunes on `pre_update_columns`
        # then deletes the dedup key: `SELECT COUNT(*) FROM (A EXCEPT B)` reduced the
        # anti-join's EMIT set to zero columns, leaving the Distinct to dedup on
        # nothing and collapse the whole set operation to a single row — 1 where the
        # answer was 6. Empty is the established UNKNOWN sentinel every consumer reads
        # as "keep every column" (see _live_positions in compiler.py), which is
        # exactly the claim being made.
        #
        # The region closes at the next Project, where `seen_distincts` resets below:
        # a Project fixes its own output width (and is itself protected from pruning
        # by the same counter), so operators beneath it can prune normally. Plain
        # `SELECT DISTINCT` sits directly on its Project and so closes the region
        # immediately — this costs nothing there. It stays open only where there is no
        # Project between the Distinct and the operator, which is the set-operation
        # shape: Distinct straight onto the semi/anti join the set op was rewritten to.
        if context.seen_distincts:
            node.pre_update_columns = set()
        else:
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

        # Subqueries act like all columns are referenced.
        #
        # Scans are excluded for a different reason: this pass ASSIGNS a Scan's
        # `.columns`, so reading it back as evidence of demand is circular. The binder
        # seeds every Scan with its full schema (see binder/dataset.py::visit_scan, so
        # the plan is runnable when this strategy's kill-switch is set), and collecting
        # that seed would put every column of the table into `collected_identities` —
        # the "push all the projections" block below would then match the full width
        # and prune nothing, silently turning this optimization into a no-op. The
        # identities that keep a scan's columns alive come from the Project/Filter/Join
        # nodes ABOVE it, which are visited first (traversal is top-down).
        if node.node_type not in (
            LogicalPlanStepType.Subquery,
            LogicalPlanStepType.Scan,
            # same circularity as Scan: this pass ASSIGNS a ref's `.columns`
            LogicalPlanStepType.MaterializedCteRef,
        ):
            if node.columns:  # Assumes node.columns is an iterable or None
                collected_columns = self.collect_columns(node)
                context.collected_identities.update(collected_columns)

        # READ_JSONL, READ_PARQUET, and READ_CSV are included narrowly (by function
        # name, not node type alone): unlike VALUES/UNNEST/GENERATE_SERIES -- the
        # other LogicalPlanStepType.FunctionDataset shapes -- they have a real
        # backing reader (rugo's JSONL/CSV decoders / the native ParquetReadNode)
        # that can honor a reduced column list, so pruning `node.columns` here is
        # safe and lets physical_planner push the surviving projection into the
        # scan. The other FunctionDataset kinds are deliberately left out: they
        # build a single in-memory Morsel from `node.columns` directly and pruning
        # their behavior hasn't been vetted against this pass.
        is_pushable_function_dataset = (
            node.node_type == LogicalPlanStepType.FunctionDataset
            and getattr(node, "function", None) in ("READ_JSONL", "READ_PARQUET", "READ_CSV")
        )
        if (
            (
                node.node_type
                in (
                    LogicalPlanStepType.Scan,
                    LogicalPlanStepType.Subquery,
                    LogicalPlanStepType.Union,
                    LogicalPlanStepType.MaterializedCteRef,
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
            #
            # is_pushable_function_dataset (READ_JSONL/READ_PARQUET/READ_CSV) belongs
            # here too: the "push all" branch above (lines ~91-100) already treats it
            # identically to Scan/Subquery, so it hits the exact same identity-
            # collision shortfall this fallback exists for — `SELECT * FROM
            # READ_JSONL(...) AS s1 UNION ALL SELECT * FROM READ_JSONL(...) AS s2`
            # pruned s1 (or s2)'s columns to empty before this was added, and
            # JsonlReadNode.read_morsels then failed loud with "this file's columns
            # [...] do not match the expected []" — not a silent wrong answer, but
            # a real query this engine should run. Omitting it here while including
            # it above was the gap, not a deliberate narrower scope.
            if (
                node.node_type
                in (
                    LogicalPlanStepType.Scan,
                    LogicalPlanStepType.Subquery,
                    LogicalPlanStepType.MaterializedCteRef,
                )
                or is_pushable_function_dataset
            ) and context.seen_unions > 0 and node.schema.columns:
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

            # "$derived"/"$project" is the query-wide scratch registry every
            # computed expression is minted into while binding (see
            # binding_context.py's initial schemas and binder/project.py's
            # rename). A Join's `.schemas` is a REFERENCE to that same live
            # dict (binder/join.py: `node.schemas = context.schemas`), not a
            # snapshot - so by the time this optimizer pass runs, well after
            # binding has finished, it holds every derived column the WHOLE
            # query minted, including ones computed by nodes ABOVE this join
            # (e.g. the outer SELECT list's `a + b` over two single-column
            # cross-joined subqueries) that this join cannot possibly emit.
            # Treating membership there as "this join produces this column"
            # pulled `a + b`'s identity onto a bare cross join, which made
            # RedundantOperationsStrategy think the Project computing it was
            # a no-op reselection and delete it - the compiled plan then had
            # nothing left to actually compute `a + b`, and
            # compile_to_native's Exit-column resolution failed with
            # "an output column the engine could not resolve here".
            for schema_name, schema in node.schemas.items():
                if schema_name in ("$derived", "$project"):
                    continue
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
        columns = list(node.columns or [])  # Ensuring that node.columns is iterable
        # A Project emits `columns ∪ passthrough_columns` (see redundant_operators and
        # predicate_pushdown, which both union them): ORDER BY / HAVING expressions
        # absent from the SELECT list ride through the Project so the node ABOVE it can
        # read them. Collecting only from node.columns under-counts, and an
        # under-count is the one real hazard here — every node BELOW the Project then
        # records a `pre_update_columns` missing a column the Project will demand, and
        # an operator that prunes on that set (join payload, GROUP BY key store) drops
        # a live column.
        if node.node_type == LogicalPlanStepType.Project:
            columns += list(node.passthrough_columns or [])
        for column in columns:
            if column.node_type == NodeType.IDENTIFIER and column.schema_column:
                identities.add(column.schema_column.identity)
            else:
                # A COMPUTED column has TWO possible sources, and which one is used is
                # decided later, by the compiler: recompute it from its inputs, or read
                # it already-materialized off the stream (_add_computed skips an
                # identity the layout already carries). So BOTH the inputs and the
                # computed column's OWN identity are potentially live below this node,
                # and collecting only the inputs is the under-count this docstring
                # warns about.
                #
                # It bites where a computed column is materialized by the node below:
                # a GROUP BY key. `SELECT TRUNC(id,1) AS d, COUNT(*) ... GROUP BY ALL`
                # normally has its Project folded away by redundant_operators, so
                # nothing asked the question — but a UNION leg KEEPS its Project.
                # Missing the derived identity made _group_key_emit rule that key dead,
                # the groupby sink then dropped its value store, and the surviving
                # Project tried to recompute TRUNC over an `id` the aggregate no longer
                # carried: "expression references column ... which the stream does not
                # carry". The first leg escaped only by accident (union output identity
                # == first leg's identity); the second leg failed.
                if column.schema_column:
                    identities.add(column.schema_column.identity)
                identities.update(
                    col.schema_column.identity
                    for col in get_all_nodes_of_type(column, (NodeType.IDENTIFIER,))
                    if col.schema_column
                )

        # A Window node reads its PARTITION BY / ORDER BY columns — and a
        # navigation function's (LAG/LEAD) argument column — at execution time
        # even though none of them are in node.columns (which holds only the
        # emitted window outputs). They must not be pruned from the input.
        if node.node_type == LogicalPlanStepType.Window:
            for col in node.partition_by or []:
                if col.schema_column:
                    identities.add(col.schema_column.identity)
            for col, _ in node.order_by or []:
                if col.schema_column:
                    identities.add(col.schema_column.identity)
            for _kind, _identity, arg_node, _offset in node.window_functions or []:
                if arg_node is None:
                    continue
                for col in get_all_nodes_of_type(arg_node, (NodeType.IDENTIFIER,)):
                    if col.schema_column:
                        identities.add(col.schema_column.identity)

        # A FramedWindow node's argument columns (SUM/COUNT/AVG/MIN/MAX's operand)
        # are read by the native sink at execution time even though they are not in
        # node.columns — same reasoning as the ranking Window node above, and the
        # same trap: harvest ONLY here misses them and the scan prunes them away.
        if node.node_type == LogicalPlanStepType.FramedWindow:
            for col in node.partition_by or []:
                if col.schema_column:
                    identities.add(col.schema_column.identity)
            for col, _ in node.order_by or []:
                if col.schema_column:
                    identities.add(col.schema_column.identity)
            for _kind, _identity, arg_node, _frame in node.window_functions or []:
                if arg_node is None:
                    continue
                for col in get_all_nodes_of_type(arg_node, (NodeType.IDENTIFIER,)):
                    if col.schema_column:
                        identities.add(col.schema_column.identity)

        return identities
