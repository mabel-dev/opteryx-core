# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Optimization Rule - Vector Search (docs/VECTOR_INDEX_DESIGN.md §7, D-4)

Type: Physical planning, plus one rule of COSINE_DISTANCE's ordering semantics
Goal: run `ORDER BY COSINE_DISTANCE(col, 'query') LIMIT k` through the column's index

Two jobs, for every sort whose LEADING key is COSINE_DISTANCE:

1. The sort is marked `drops_unsearchable`: a row whose distance is NULL (null text) or
   NaN has no embedding and is never returned (ruled 2026-10-03) - with or without a
   vector index, so an index never changes an answer.

2. When the shape allows, the scan is stamped (`scan.vector_search`) and the compiler
   gives it a row admission (src/cpp/engine/vector_index_admission.hpp) that decodes only
   the rows the index finds - exact by default (every stored vector scored), approximate
   only under `SET nprobe` - plus every row of the files the index does not cover yet.
   The shape: COSINE_DISTANCE(<indexed text column>, '<literal>') as the SOLE, ascending
   ORDER BY key with a LIMIT, over ONE scan reached through projections only (a WHERE
   pushed into that scan is applied before the search), and an index defined against
   the active embedder. Any other query runs as written, without the index; the
   projection computes each row's distance with the COSINE_DISTANCE kernel either way.

Ordering: after OperatorFusion (the HeapSort it reads) and ProjectFusion. This strategy
can not be disabled: job 1 is semantics, not an optimization.
"""

from opteryx.expression import NodeType
from opteryx.expression import get_all_nodes_of_type
from opteryx.planner.logical_planner import LogicalPlan
from opteryx.planner.logical_planner import LogicalPlanStepType

from .optimization_strategy import OptimizationStrategy
from .optimization_strategy import OptimizerContext

FUNCTION = "COSINE_DISTANCE"
_SORTS = (LogicalPlanStepType.HeapSort, LogicalPlanStepType.Order)


def _distance_calls(expressions):
    return [
        node
        for node in get_all_nodes_of_type(list(expressions), (NodeType.FUNCTION,))
        if str(node.value).upper() == FUNCTION
    ]


def _leading_call(plan: LogicalPlan, key):
    """The COSINE_DISTANCE call a sort key IS, or that computes the column it names
    (identities are unique in a plan), else None."""
    if key.node_type == NodeType.FUNCTION:
        return key if str(key.value).upper() == FUNCTION else None
    if key.schema_column is None:
        return None
    identity = key.schema_column.identity
    for _, node in plan.nodes(True):
        if node.node_type != LogicalPlanStepType.Project:
            continue
        for expression in node.expressions():
            if (
                expression.schema_column is not None
                and expression.schema_column.identity == identity
                and expression.node_type == NodeType.FUNCTION
            ):
                return expression if str(expression.value).upper() == FUNCTION else None
    return None


class VectorSearchStrategy(OptimizationStrategy):
    """Mark COSINE_DISTANCE-led sorts; route the indexable shape through the index."""

    requires = ("heapsort-fused", "project-fused")
    provides = ("vector-search",)

    def visit(self, node, context: OptimizerContext) -> OptimizerContext:
        return context

    def should_i_run(self, plan: LogicalPlan) -> bool:
        return any(_distance_calls(node.expressions()) for _, node in plan.nodes(True))

    def complete(self, plan: LogicalPlan, context: OptimizerContext) -> LogicalPlan:
        sorts = [(nid, node) for nid, node in plan.nodes(True) if node.node_type in _SORTS]
        for sort_nid, sort in sorts:
            call = _leading_call(plan, sort.order_by[0][0])
            if call is None:
                continue
            sort.drops_unsearchable = True
            plan[sort_nid] = sort
            if sort.node_type == LogicalPlanStepType.HeapSort:
                self._route_through_index(plan, sort_nid, sort, call)
        return plan

    def _route_through_index(self, plan: LogicalPlan, sort_nid, sort, call) -> None:
        """Stamp the scan when the query has the indexable shape; otherwise leave it."""
        if not sort.limit or sort.limit <= 0 or len(sort.order_by) != 1 or not sort.order_by[0][1]:
            return
        nid = sort_nid
        while True:
            ingoing = plan.ingoing_edges(nid)
            if len(ingoing) != 1:
                return
            nid = ingoing[0][0]
            node = plan[nid]
            if node.node_type == LogicalPlanStepType.Scan:
                scan_nid, scan = nid, node
                break
            # A WHERE left above the scan would filter the index's candidates
            # afterwards; one pushed into the scan is applied before the search.
            if node.node_type != LogicalPlanStepType.Project:
                return
        column, query = call.parameters
        if column.node_type != NodeType.IDENTIFIER or column.schema_column is None:
            return
        # A text literal is held as its UTF-8 bytes.
        if query.node_type != NodeType.LITERAL or type(query.value) is not bytes:
            return
        connector = scan.connector
        if connector is None or not connector.supports_vector_indexes or scan.manifest is None:
            return
        column_name = column.schema_column.name
        definitions = [
            d for d in connector.vector_indexes() if d["column"].lower() == column_name.lower()
        ]
        if not definitions:
            return
        definition = definitions[0]

        from opteryx.types.vectors.embedding_capability import active_embedding_capability

        capability = active_embedding_capability()
        if (capability.identity, capability.dimensions) != (
            definition["embedding-identity"], definition["dimensions"]
        ):
            self.record_decision(
                "vector search",
                f"index {definition['name']} on {scan.relation} not used: defined against "
                f"{definition['embedding-identity']}, this engine embeds with {capability.identity}",
            )
            return

        scan.vector_search = {
            "index_id": definition["index-id"],
            "index_name": definition["name"],
            "column": definition["column"],
            "query": query.value.decode("utf-8"),
            "k": int(sort.limit),
            "dimensions": int(definition["dimensions"]),
        }
        plan[scan_nid] = scan
        files = scan.manifest.get_file_paths()
        covered = connector.vector_index_covered(definition["index-id"])
        exact = sum(1 for path in files if path not in covered)
        self.record_decision(
            "vector search",
            f"index {definition['name']} on {scan.relation}: k={int(sort.limit)}, "
            f"{len(files) - exact} of {len(files)} file(s) indexed, {exact} searched exactly",
        )
