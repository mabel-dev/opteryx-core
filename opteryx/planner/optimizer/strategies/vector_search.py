# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Optimization Rule - Vector Search (docs/VECTOR_INDEX_DESIGN.md §7, D-4)

Type: Physical planning
Goal: run `ORDER BY COSINE_DISTANCE(col, 'query') LIMIT k` through the column's index

When the shape allows, the scan is stamped (`scan.vector_search`) and the compiler gives it
a row admission (src/cpp/engine/vector_index_admission.hpp) that decodes only the rows the
index finds - exact by default (every stored vector scored), approximate only under
`SET nprobe` - plus every row of the files searched exactly. The shape:
COSINE_DISTANCE(<indexed text column>, '<literal>') as the SOLE, ascending ORDER BY key
with a LIMIT, over ONE scan reached through projections only (a WHERE pushed into that
scan is applied before the search), and an index defined against the active embedder.
Each covered file then goes through the index only where that is estimated cheaper than
searching it exactly (vector_search_cost.py, ruled 2026-10-04). Any other query runs as
written, without the index.

Dropping rows with no embedding from a COSINE_DISTANCE-led sort is the query's meaning,
not this rule's: planner/unsearchable_sorts.py marks it straight after binding, so
disabling this strategy (FEATURE_DISABLE_VECTOR_INDEX_ROUTING) only stops the index being
used - it never changes an answer.

Ordering: after OperatorFusion (the HeapSort it reads) and ProjectFusion.
"""

from opteryx.expression import NodeType
from opteryx.planner.logical_planner import LogicalPlan
from opteryx.planner.logical_planner import LogicalPlanStepType
from opteryx.planner.unsearchable_sorts import distance_calls
from opteryx.planner.unsearchable_sorts import leading_call

from .optimization_strategy import OptimizationStrategy
from .optimization_strategy import OptimizerContext


class VectorSearchStrategy(OptimizationStrategy):
    """Route the indexable COSINE_DISTANCE shape through the index, file by file."""

    requires = ("heapsort-fused", "project-fused")
    provides = ("vector-search",)

    def visit(self, node, context: OptimizerContext) -> OptimizerContext:
        return context

    def should_i_run(self, plan: LogicalPlan) -> bool:
        return any(distance_calls(node.expressions()) for _, node in plan.nodes(True))

    def complete(self, plan: LogicalPlan, context: OptimizerContext) -> LogicalPlan:
        for sort_nid, sort in list(plan.nodes(True)):
            if sort.node_type != LogicalPlanStepType.HeapSort or not sort.drops_unsearchable:
                continue
            call = leading_call(plan, sort.order_by[0][0])
            self._route_through_index(plan, sort_nid, sort, call, context.plan_context.variables)
        return plan

    def _route_through_index(self, plan: LogicalPlan, sort_nid, sort, call, variables) -> None:
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

        # Per file: the index or an exact search, whichever is estimated cheaper (bytes AND
        # CPU, vector_search_cost.py). A file the index does not cover yet is searched
        # exactly regardless; a file a figure is missing for keeps its index.
        from opteryx import config
        from opteryx.variables import resolve

        from .vector_search_cost import embed_seconds_per_row
        from .vector_search_cost import file_cost

        k = int(sort.limit)
        manifest = scan.manifest
        files = manifest.get_file_paths()
        sizes = connector.vector_index_sizes(definition["index-id"])
        # A WHERE pushed into the scan forces an exact search of the index (the compiler).
        nprobe = 0 if scan.predicates else max(0, int(resolve("nprobe", variables, 0)))
        workers = config.resolve_max_execution_workers(
            resolve("max_execution_workers", variables, config.MAX_EXECUTION_WORKERS))
        embed = embed_seconds_per_row(capability.name)
        rows = manifest.record_counts()
        row_groups = manifest.row_group_counts()
        file_bytes = manifest.file_sizes()
        file_uncompressed = manifest.uncompressed_sizes()
        read_names = {column.schema_column.name for column in scan.columns}
        read_sizes = [
            manifest.column_uncompressed_sizes(name) if manifest.position_of(name) is not None
            else [None] * len(files)
            for name in read_names
        ]
        # Catalog manifests do not record row-group counts; a missing one is estimated at
        # the engine writer's row-group size, and the plan says for how many files.
        from rugo.parquet import DEFAULT_ROWS_PER_ROW_GROUP

        estimated_groups = 0
        exact_files, via_index, uncosted = [], 0, 0
        index_seconds = exact_seconds = 0.0
        for i, path in enumerate(files):
            if path not in sizes:
                continue
            per_column = [column[i] for column in read_sizes]
            groups = row_groups[i]
            if groups is None and rows[i]:
                groups = -(-rows[i] // DEFAULT_ROWS_PER_ROW_GROUP)
                estimated_groups += 1
            cost = file_cost(
                remote=path.startswith(("gs://", "s3://", "http://", "https://")),
                rows=rows[i], row_groups=groups, file_bytes=file_bytes[i],
                file_uncompressed=file_uncompressed[i],
                read_uncompressed=None if None in per_column else sum(per_column),
                index_bytes=sizes[path][0], index_footer_bytes=sizes[path][1],
                k=k, nprobe=nprobe, clusters=int(definition.get("clusters") or 0),
                dimensions=int(definition["dimensions"]), embed_per_row=embed, workers=workers,
            )
            if cost is None:
                uncosted += 1
                continue
            index_seconds += cost.index_seconds
            exact_seconds += cost.exact_seconds
            if cost.use_index:
                via_index += 1
            else:
                exact_files.append(path)
        not_covered = sum(1 for path in files if path not in sizes)
        costs = (f"est index {index_seconds:.4f}s vs exact {exact_seconds:.4f}s over "
                 f"{via_index + len(exact_files)} costed file(s), row groups estimated for "
                 f"{estimated_groups}")
        if via_index == 0 and uncosted == 0:
            self.record_decision(
                "vector search",
                f"index {definition['name']} on {scan.relation} not used: every file is "
                f"cheaper searched exactly ({costs}; {not_covered} not covered by the index)",
            )
            return

        scan.vector_search = {
            "index_id": definition["index-id"],
            "index_name": definition["name"],
            "column": definition["column"],
            "query": query.value.decode("utf-8"),
            "k": k,
            "dimensions": int(definition["dimensions"]),
            # Covered files the cost model chose to search exactly.
            "exact_files": exact_files,
        }
        plan[scan_nid] = scan
        self.record_decision(
            "vector search",
            f"index {definition['name']} on {scan.relation}: k={k}, {via_index + uncosted} of "
            f"{len(files)} file(s) via the index ({uncosted} uncosted), "
            f"{len(exact_files) + not_covered} searched exactly ({len(exact_files)} by cost, "
            f"{not_covered} not covered); {costs}",
        )
