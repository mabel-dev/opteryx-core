# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Optimization Rule - Vector Search (docs/VECTOR_INDEX_DESIGN.md §7, D-4)

Type: Physical planning (and the approximate form's only gate)
Goal: run `ORDER BY APPROX_COSINE_DISTANCE(col, 'query') LIMIT k` through the index

An approximate result is only ever produced by syntax that says "approximate", and only
in the one shape whose meaning is clear: APPROX_COSINE_DISTANCE as the SOLE ORDER BY key
of a query over ONE scan, with a LIMIT, on a text column that has a vector index. Here
the scan is stamped (`scan.vector_search`) and the compiler gives it a row admission
(src/cpp/engine/vector_index_admission.hpp) that decodes only the index's candidates —
plus every row of the files the index does not cover yet, searched exactly (ruled
2026-10-03). The index search itself is exact unless the session sets `nprobe`. The
rest of the plan is untouched: the projection computes each candidate's EXACT distance
and the Top-N sink orders them, so the value a reader sees is always the COSINE_DISTANCE
kernel's.

Everything else is REFUSED, never quietly run exactly: the function anywhere but that
ORDER BY key (or the same call repeated in the SELECT list), more than one key, no LIMIT,
a WHERE that was not pushed into the scan (a pushed one is applied BEFORE the search, as
its admitted set), a join, a column with no index, a query that is not a text literal, or
an index defined against another embedder.

Ordering: after OperatorFusion (the HeapSort it reads) and ProjectFusion. This strategy
can not be disabled: it is the gate as well as the planner.
"""

from opteryx.exceptions import UnsupportedSyntaxError
from opteryx.expression import NodeType
from opteryx.expression import get_all_nodes_of_type
from opteryx.expression.formatter import format_expression
from opteryx.planner.logical_planner import LogicalPlan
from opteryx.planner.logical_planner import LogicalPlanStepType

from .optimization_strategy import OptimizationStrategy
from .optimization_strategy import OptimizerContext

FUNCTION = "APPROX_COSINE_DISTANCE"
SHAPE = (
    "**APPROX_COSINE_DISTANCE**(<text column>, '<query>') is only valid as the sole "
    "**ORDER BY** key of a query over one table with a **LIMIT**, on a column with a "
    "vector index"
)


def _approx_calls(expressions):
    return [
        node
        for node in get_all_nodes_of_type(list(expressions), (NodeType.FUNCTION,))
        if str(node.value).upper() == FUNCTION
    ]


class VectorSearchStrategy(OptimizationStrategy):
    """Validate the approximate form and stamp its scan."""

    requires = ("heapsort-fused", "project-fused")
    provides = ("vector-search",)

    def visit(self, node, context: OptimizerContext) -> OptimizerContext:
        return context

    def should_i_run(self, plan: LogicalPlan) -> bool:
        return any(_approx_calls(node.expressions()) for _, node in plan.nodes(True))

    def complete(self, plan: LogicalPlan, context: OptimizerContext) -> LogicalPlan:
        holders = {nid for nid, node in plan.nodes(True) if _approx_calls(node.expressions())}
        sorts = [
            (nid, node) for nid, node in plan.nodes(True)
            if node.node_type == LogicalPlanStepType.HeapSort
        ]
        if len(sorts) != 1:
            raise UnsupportedSyntaxError(f"{SHAPE}.")
        sort_nid, sort = sorts[0]
        if not sort.limit or sort.limit <= 0 or len(sort.order_by) != 1:
            raise UnsupportedSyntaxError(f"{SHAPE}: one key, and a **LIMIT**.")
        key, ascending, _nulls_first = sort.order_by[0]

        # Down from the sort: only projections, to exactly one scan.
        path = []
        nid = sort_nid
        while True:
            ingoing = plan.ingoing_edges(nid)
            if len(ingoing) != 1:
                raise UnsupportedSyntaxError(f"{SHAPE}: one table, no join.")
            nid = ingoing[0][0]
            node = plan[nid]
            if node.node_type == LogicalPlanStepType.Scan:
                # A WHERE pushed into the scan is applied BEFORE the search: its
                # survivors are the only rows the search may admit (vector_index_
                # admission.hpp). One that could not be pushed is refused below.
                scan_nid, scan = nid, node
                break
            if node.node_type == LogicalPlanStepType.Filter:
                raise UnsupportedSyntaxError(
                    f"{SHAPE}. This **WHERE** could not be pushed into the scan, so it could "
                    "only filter the index's candidates afterwards - which could return fewer "
                    "than LIMIT rows."
                )
            if node.node_type != LogicalPlanStepType.Project:
                raise UnsupportedSyntaxError(f"{SHAPE}.")
            path.append(nid)

        # The sort key IS the call, or names the column a projection below computes with it.
        if key.node_type == NodeType.FUNCTION:
            call = key
        else:
            identity = key.schema_column.identity if key.schema_column is not None else None
            computed = [
                expression
                for project_nid in path
                for expression in plan[project_nid].expressions()
                if expression.schema_column is not None and expression.schema_column.identity == identity
            ]
            call = computed[0] if computed else None
        if call is None or call.node_type != NodeType.FUNCTION or str(call.value).upper() != FUNCTION:
            raise UnsupportedSyntaxError(f"{SHAPE}.")
        if not ascending:
            raise UnsupportedSyntaxError(
                f"{SHAPE}, ascending: the index finds the NEAREST rows, not the farthest."
            )

        # The same call may appear in the SELECT list; anywhere else it is refused.
        signature = format_expression(call)
        for nid in holders:
            if nid != sort_nid and nid not in path:
                raise UnsupportedSyntaxError(f"{SHAPE}.")
            for other in _approx_calls(plan[nid].expressions()):
                if format_expression(other) != signature:
                    raise UnsupportedSyntaxError(
                        f"{SHAPE}: one search per query - every **APPROX_COSINE_DISTANCE** in "
                        "it must be the same call."
                    )
        key = call

        column, query = key.parameters
        if column.node_type != NodeType.IDENTIFIER or column.schema_column is None:
            raise UnsupportedSyntaxError(f"{SHAPE}: its first argument names the indexed column.")
        # A text literal is held as its UTF-8 bytes.
        if query.node_type != NodeType.LITERAL or type(query.value) is not bytes:
            raise UnsupportedSyntaxError(
                f"{SHAPE}: its second argument is the query, a text literal or parameter."
            )

        connector = scan.connector
        if connector is None or not connector.supports_vector_indexes or scan.manifest is None:
            raise UnsupportedSyntaxError(
                f"{SHAPE}: {scan.relation} is not a catalog table, so it has no vector index."
            )
        column_name = column.schema_column.name
        definitions = [
            d for d in connector.vector_indexes()
            if d["column"].lower() == column_name.lower()
        ]
        if not definitions:
            raise UnsupportedSyntaxError(
                f"{SHAPE}: `{column_name}` of {scan.relation} has no vector index. Create one "
                "(**CREATE INDEX** ... **USING IVF**), or use **COSINE_DISTANCE** for an exact search."
            )
        definition = definitions[0]

        from opteryx.types.vectors.embedding_capability import active_embedding_capability

        capability = active_embedding_capability()
        if (capability.identity, capability.dimensions) != (
            definition["embedding-identity"], definition["dimensions"]
        ):
            raise UnsupportedSyntaxError(
                f"Index {definition['name']} on {scan.relation} was defined against the embedder "
                f"{definition['embedding-identity']}; this engine embeds with {capability.identity}. "
                "Searching it would compare vectors from two models."
            )

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
        return plan
