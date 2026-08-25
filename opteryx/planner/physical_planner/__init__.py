# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
The Physical Planner is the final planning step. It converts the optimized and bound
LogicalPlan into a PhysicalPlan whose nodes are concrete execution operators.

Input:  optimized LogicalPlan + QueryProperties
Output: PhysicalPlan — a graph of operator instances ready for the execution engine

For each logical node the planner selects the appropriate physical operator from the
operator registry based on the node type and its properties:

- Scan         → ParquetReadNode (all-parquet manifests), Reader (internal datasets),
                 NullReaderNode (empty-result scans with contradictory predicates)
- Join         → DrakenInnerJoinNode, OuterJoinNode, FilterJoinNode (semi/anti),
                 ExistenceJoinNode (SELECT-list EXISTS / IN),
                 CrossJoinNode, NestedLoopJoinNode, AsofJoinNode
- Aggregate    → Aggregate or AggregateAndGroupNode
- Project      → ProjectionNode
- Filter       → FilterNode
- Order/Limit  → SortNode / HeapSortNode / LimitNode
- Set ops      → UnionNode (others are rewritten to joins by the plan rewriter)
- DDL          → ViewManagementNode, TableManagementNode

Edge topology from the logical plan is copied directly into the physical plan — no
structural changes occur at this stage.

The Physical Planner does NOT optimize, bind, or rewrite the plan.
"""

from opteryx.exceptions import InvalidInternalStateError
from opteryx.exceptions import NotSupportedError
from opteryx.exceptions import UnsupportedSyntaxError
from opteryx.expression import NodeType
from opteryx.expression import binary_operands
from opteryx.expression import get_all_nodes_of_type
from opteryx.models import PhysicalPlan
from opteryx.models.dataset_format import PARQUET
from opteryx.models.dataset_format import SCAN_READERS
from opteryx.models.dataset_format import manifest_format
from opteryx.operators.catalog import get_registry
from opteryx.operators.hashed_inner_join import DrakenInnerJoinNode
from opteryx.planner.logical_planner import LogicalPlanStepType

# Inverse of a comparison op, for normalizing `literal OP column` to
# `column OP literal` (rugo's predicate tuples are always column-relative).
_INVERT_COMPARISON_OP = {
    "Gt": "Lt", "GtEq": "LtEq", "Lt": "Gt", "LtEq": "GtEq", "Eq": "Eq", "NotEq": "NotEq",
}


def _translate_jsonl_predicates(predicates, physical_by_identity):
    """Translate pushed-down predicate condition Nodes into rugo's
    ``(physical_column_name, op, value)`` tuple form (see
    opteryx.connectors.jsonl_io.JSONL_OP_XLAT).

    Every entry here was already gated by JsonlPredicatePushable.can_push at
    optimizer time to be exactly a `column OP literal` COMPARISON_OPERATOR with
    op in JSONL_OP_XLAT -- this only re-derives that shape to build the tuple,
    it does not re-validate it. A predicate can_push declined is never in this
    list; it stays behind as an ordinary Filter node above the scan instead.
    """
    from opteryx.connectors.jsonl_io import JSONL_OP_XLAT

    translated = []
    for condition in predicates or []:
        left, right = binary_operands(condition)
        if left.node_type == NodeType.IDENTIFIER and right.node_type == NodeType.LITERAL:
            ident, literal, op = left, right, condition.value
        elif right.node_type == NodeType.IDENTIFIER and left.node_type == NodeType.LITERAL:
            ident, literal, op = right, left, _INVERT_COMPARISON_OP[condition.value]
        else:  # pragma: no cover -- can_push only admits this shape
            raise InvalidInternalStateError(
                "READ_JSONL received a pushed predicate that is not a plain "
                "column-vs-literal comparison; JsonlPredicatePushable.can_push "
                "should have declined it."
            )
        physical_name = physical_by_identity[ident.schema_column.identity]
        translated.append((physical_name, JSONL_OP_XLAT[op], literal.value))
    return translated


def _translate_csv_predicates(predicates, physical_by_identity):
    """Translate pushed-down predicate condition Nodes into rugo's
    ``(physical_column_name, op, value)`` tuple form (see
    opteryx.connectors.csv_io.CSV_OP_XLAT).

    Identical shape/reasoning to ``_translate_jsonl_predicates`` above -- every
    entry here was already gated by CsvPredicatePushable.can_push at optimizer
    time to be exactly a `column OP literal` COMPARISON_OPERATOR with op in
    CSV_OP_XLAT.
    """
    from opteryx.connectors.csv_io import CSV_OP_XLAT

    translated = []
    for condition in predicates or []:
        left, right = binary_operands(condition)
        if left.node_type == NodeType.IDENTIFIER and right.node_type == NodeType.LITERAL:
            ident, literal, op = left, right, condition.value
        elif right.node_type == NodeType.IDENTIFIER and left.node_type == NodeType.LITERAL:
            ident, literal, op = right, left, _INVERT_COMPARISON_OP[condition.value]
        else:  # pragma: no cover -- can_push only admits this shape
            raise InvalidInternalStateError(
                "READ_CSV received a pushed predicate that is not a plain "
                "column-vs-literal comparison; CsvPredicatePushable.can_push "
                "should have declined it."
            )
        physical_name = physical_by_identity[ident.schema_column.identity]
        translated.append((physical_name, CSV_OP_XLAT[op], literal.value))
    return translated


def _jsonl_scan_config(node_config):
    """Adapt a manifest-backed Scan's config to JsonlReadNode's parameters.

    JsonlReadNode serves both READ_JSONL (files resolved by the binder) and
    dataset Scans (files from the manifest) through the same fields: the file
    list, the physical (in-file) projection names parallel to `columns`, and
    pushed predicates as rugo tuples. Scan schema columns are file-named, so
    the physical name is simply schema_column.name. An empty projection is the
    genuine COUNT(*) shape (zero-column morsels), same as the parquet scan.
    """
    manifest = node_config["manifest"]
    columns = node_config.get("columns") or []
    predicates = node_config.get("predicates") or []

    physical_by_identity = {c.schema_column.identity: c.schema_column.name for c in columns}
    # A pushed predicate's column is not necessarily projected; its own
    # schema_column carries the same identity→name mapping.
    for condition in predicates:
        for side in binary_operands(condition):
            schema_column = getattr(side, "schema_column", None)
            if schema_column is not None:
                physical_by_identity.setdefault(schema_column.identity, schema_column.name)

    return {
        **node_config,
        "jsonl_files": [f.file_path for f in manifest.files],
        "jsonl_physical_columns": [c.schema_column.name for c in columns],
        "jsonl_predicates": _translate_jsonl_predicates(predicates, physical_by_identity),
    }


def _skene_scan_config(node_config):
    """Adapt a manifest-backed Scan's config to SkeneReadNode's parameters:
    the manifest's file list plus the schema columns the reader must decode
    (scan schema columns are file-named, so physical name = schema_column.name).
    Pushed predicates ride through node_config["predicates"] and are lowered by
    the compiler into scan.compiled_predicate; the reader applies them exactly
    and then selects back down to the projection."""
    manifest = node_config["manifest"]
    columns = node_config.get("columns") or []
    predicates = node_config.get("predicates") or []

    # The read set is projection ∪ predicate columns: a pushed predicate's
    # column is not necessarily projected (COUNT(*) WHERE x > 5 projects
    # nothing), but the reader must decode it to filter. The reader selects
    # back down to the projection after filtering, so predicate-only columns
    # never leave the scan.
    read_schema_columns = [c.schema_column for c in columns]
    read_identities = {sc.identity for sc in read_schema_columns}
    for condition in predicates:
        for referenced in get_all_nodes_of_type(condition, (NodeType.IDENTIFIER,)):
            schema_column = referenced.schema_column
            if schema_column.identity not in read_identities:
                read_identities.add(schema_column.identity)
                read_schema_columns.append(schema_column)

    return {
        **node_config,
        "skene_files": [f.file_path for f in manifest.files],
        "skene_read_schema_columns": read_schema_columns,
    }


def _scan_reader_for_manifest(manifest, dataset: str) -> str:
    """Operator-registry name for a manifest-backed Scan, dispatched on the
    dataset's format (FileEntry.file_format — datasets are single-format).

    An empty manifest is an empty relation: any reader yields nothing, so the
    parquet reader serves it. A mixed manifest raises in manifest_format; a
    format with no registered Scan reader raises here, by name.
    """
    file_format = manifest_format(manifest, dataset=dataset)
    if file_format is None:
        return SCAN_READERS[PARQUET]
    reader_name = SCAN_READERS.get(file_format)
    if reader_name is None:
        raise UnsupportedSyntaxError(
            f"Dataset {dataset or '(unnamed)'} is {file_format}, which has no "
            f"scan reader. Supported formats: {', '.join(sorted(SCAN_READERS))}."
        )
    return reader_name


def _create_aggregate_node(logical_node, query_properties, registry):
    node_config = logical_node.properties
    return registry.create(
        "Aggregate",
        query_properties,
        **{k: v for k, v in node_config.items() if k in ("aggregates", "all_relations")},
    )


def _create_aggregate_and_group_node(logical_node, query_properties, registry):
    node_config = logical_node.properties
    return registry.create(
        "Aggregate and Group",
        query_properties,
        # pre_update_columns: a GROUP BY key that nothing above reads still has to be
        # HASHED to separate the groups, but its values never have to be stored — the
        # grouping contract is 64-bit hash identity. Carrying the set here is what lets
        # the sink kill the key once it is hashed.
        # grouping_set_identities: GROUP BY ROLLUP's sets, as key identities (the binder
        # resolved them from the planner's positions). Absent for a plain GROUP BY.
        **{k: v for k, v in node_config.items() if k in ("aggregates", "groups", "projection", "all_relations", "having_condition", "groupby_ndv_estimate", "pre_update_columns", "grouping_set_identities")},
    )


def _create_distinct_node(logical_node, query_properties, registry):
    node_config = logical_node.properties
    return registry.create(
        "Distinct",
        query_properties,
        **{k: v for k, v in node_config.items() if k in ("on", "distinct_ndv_estimate")},
    )


def _create_window_node(logical_node, query_properties, registry):
    node_config = logical_node.properties
    return registry.create(
        "Window",
        query_properties,
        **{
            k: v
            for k, v in node_config.items()
            # pre_update_columns: same reason as the sort — the PARTITION BY / ORDER BY
            # keys are not in it, so it is the set the window must still emit.
            if k in ("partition_by", "order_by", "window_functions", "top_k",
                     "pre_update_columns")
        },
    )


def _create_framed_window_node(logical_node, query_properties, registry):
    node_config = logical_node.properties
    return registry.create(
        "Framed Window",
        query_properties,
        **{
            k: v
            for k, v in node_config.items()
            # `outputs` carries the bound SchemaColumn per function (the compiler
            # needs its ColumnType to type each output column — window_functions
            # only carries the identity) alongside window_functions and
            # partition_by/order_by, which are unpacked into per-function
            # config (`_functions`/`_partition_columns`/etc.) by FramedWindowNode.
            if k in ("partition_by", "order_by", "window_functions", "outputs", "pre_update_columns")
        },
    )


def _create_exit_node(logical_node, query_properties, registry):
    return registry.create("Exit", query_properties, **logical_node.properties)


def _create_explain_node(logical_node, query_properties, registry):
    return registry.create("Explain", query_properties, **logical_node.properties)


def _create_filter_node(logical_node, query_properties, registry):
    node_config = logical_node.properties
    return registry.create(
        "Filter",
        query_properties,
        filter=node_config["condition"],
        **{k: v for k, v in node_config.items() if k in ("all_relations", "pre_update_columns")},
    )


def _create_function_dataset_node(logical_node, query_properties, registry):
    if logical_node.function == "READ_JSONL":
        # READ_JSONL streams morsels from a real file via rugo -- it does not fit
        # the generic single-Morsel DATASET_FUNCTIONS path (VALUES/UNNEST/
        # GENERATE_SERIES), so it gets its own scan operator, the same way
        # Parquet scans are routed to "Parquet Reader" in _create_scan_node.
        node_config = dict(logical_node.properties)
        physical_by_identity = node_config.get("jsonl_physical_by_identity") or {}
        # `logical_node.columns` reflects whatever projection_pushdown pruned it
        # to; re-derive the matching physical (pre-alias) names by identity --
        # the bind-time `jsonl_physical_columns` list is the FULL, unpruned file
        # column order and would go stale/misaligned once columns are pruned.
        node_config["jsonl_physical_columns"] = [
            physical_by_identity[column.schema_column.identity]
            for column in (logical_node.columns or [])
        ]
        node_config["jsonl_predicates"] = _translate_jsonl_predicates(
            node_config.get("predicates"), physical_by_identity
        )
        return registry.create("JSONL Reader", query_properties, **node_config)
    if logical_node.function == "READ_PARQUET":
        # Unlike READ_JSONL, READ_PARQUET reuses the existing native ParquetReadNode
        # wholesale (opteryx.planner.binder.dataset's READ_PARQUET branch builds a
        # real FileSystemTable connector + Manifest at bind time, exactly like a
        # catalog-backed/ad-hoc Scan) -- no bespoke operator, no predicate
        # translation; node_config["predicates"]/["columns"] are already real column
        # identifiers (not JSONL's raw-key remap), and the manifest/connector are
        # already fully resolved.
        node_config = dict(logical_node.properties)
        return registry.create("Parquet Reader", query_properties, **node_config)
    if logical_node.function == "READ_CSV":
        # READ_CSV reads each file whole (no chunking -- see CsvReadNode's module
        # docstring), but the projection/predicate translation shape is otherwise
        # identical to READ_JSONL above.
        node_config = dict(logical_node.properties)
        physical_by_identity = node_config.get("csv_physical_by_identity") or {}
        node_config["csv_physical_columns"] = [
            physical_by_identity[column.schema_column.identity]
            for column in (logical_node.columns or [])
        ]
        node_config["csv_predicates"] = _translate_csv_predicates(
            node_config.get("predicates"), physical_by_identity
        )
        return registry.create("CSV Reader", query_properties, **node_config)
    return registry.create("Function Dataset", query_properties, **logical_node.properties)


def _create_heap_sort_node(logical_node, query_properties, registry):
    return registry.create("Heap Sort", query_properties, **logical_node.properties)


def _create_join_node(logical_node, query_properties, registry):
    node_config = logical_node.properties
    join_type = node_config.get("type")

    if join_type == "inner":
        # INNER JOIN, NATURAL JOIN
        if DrakenInnerJoinNode.supports(**node_config):
            return registry.create("Inner Join", query_properties, **node_config)
        # NotSupportedError, not UnsupportedSyntaxError: the statement parsed and bound
        # fine, so nothing about the SQL is wrong - the engine's inner-join operator
        # simply has no plan for this shape. Naming Draken told the reader about a
        # component they have no way to act on.
        raise NotSupportedError(
            "This JOIN is not supported. The engine's inner join cannot handle the "
            "shape of this query - rewriting the join conditions, or joining the "
            "relations in a different order, may let it run."
        )
    elif join_type == "nested loop":
        # NESTED LOOP JOIN (INNER JOIN)
        return registry.create("Nested Loop Join", query_properties, **node_config)
    elif join_type in ("left outer", "full outer", "right outer"):
        # LEFT JOIN, RIGHT JOIN, FULL JOIN
        return registry.create("Outer Join", query_properties, **node_config)
    elif join_type == "cross join":
        # CROSS JOIN, CROSS JOIN UNNEST
        return registry.create("Cross Join", query_properties, **node_config)
    elif join_type in (
        "left anti",
        "left semi",
        "left anti null-aware",
        "left semi not-distinct",
        "left anti not-distinct",
    ):
        # LEFT SEMI, LEFT ANTI, LEFT ANTI NULL-AWARE (NOT IN), and the two
        # not-distinct forms (INTERSECT / EXCEPT, where NULL equals NULL) JOIN
        return registry.create("Filter Join", query_properties, **node_config)
    elif join_type in ("left existence", "left existence anti"):
        # The same existence test as the filter joins above, EMITTED as a BOOL
        # column instead of applied — what a SELECT-list EXISTS / IN reads.
        return registry.create("Existence Join", query_properties, **node_config)
    elif join_type == "asof":
        # ASOF JOIN — nearest-neighbour time-series join
        return registry.create("ASOF Join", query_properties, **node_config)
    elif join_type == "band":
        # BAND JOIN — an equi-join whose ON also closes a range on one build-side
        # column, executed as a bisect into sorted per-equi-group runs instead of a
        # full equi fan-out with the range filtered off the top.
        return registry.create("Band Join", query_properties, **node_config)
    else:
        # We don't support other JOIN types, e.g. RIGHT SEMI, RIGHT ANTI
        raise InvalidInternalStateError(f"Unsupported JOIN type '{join_type}'")


def _create_scalar_guard_node(logical_node, query_properties, registry):
    node_config = logical_node.properties
    return registry.create(
        "Scalar Guard",
        query_properties,
        **{k: v for k, v in node_config.items() if k in ("all_relations",)},
    )


def _create_limit_node(logical_node, query_properties, registry):
    node_config = logical_node.properties
    return registry.create(
        "Limit",
        query_properties,
        **{k: v for k, v in node_config.items() if k in ("limit", "offset", "all_relations")},
    )


def _create_order_node(logical_node, query_properties, registry):
    node_config = logical_node.properties
    return registry.create(
        "Sort",
        query_properties,
        **{
            k: v
            for k, v in node_config.items()
            # pre_update_columns: the sort's ORDER BY keys are not in it (it is
            # snapshotted before the node's own columns are collected), so it is
            # precisely the set the sort must still emit — what lets the sink drop a
            # key column once the sort keys are built instead of gathering it into
            # every output row and having the Exit select throw it away.
            if k in ("order_by", "all_relations", "pre_update_columns")
        },
    )


def _create_project_node(logical_node, query_properties, registry):
    node_config = logical_node.properties
    return registry.create(
        "Projection",
        query_properties,
        projection=logical_node.columns,
        passthrough_columns=getattr(logical_node, "passthrough_columns", []),
        hoisted_columns=getattr(logical_node, "hoisted_columns", []),
        **{k: v for k, v in node_config.items() if k in ("projection", "all_relations")},
    )


def _create_scan_node(logical_node, query_properties, registry):
    node_config = logical_node.properties
    connector = node_config.get("connector")

    if connector == "__null__":
        # Scan marked for empty result (contradictory predicates)
        return registry.create("Null Reader", query_properties, **node_config)
    elif node_config.get("for_snapshots_only"):
        # SHOW SNAPSHOTS FOR: this Scan exists so the relation is BOUND — the
        # permission gate, the connector, and the commit history the statement
        # answers from — and is never read. serial_engine answers from the
        # ShowSnapshots node above it and never drives the pipeline.
        #
        # It carries no manifest by design: the history is the result, and
        # building one would pay binding's expensive half to produce a file list
        # nothing looks at. So it cannot take the manifest branch below, and the
        # reader that yields no rows is the honest physical form of a scan whose
        # rows are not part of the answer. SHOW MANIFEST FOR differs here — its
        # Scan does carry a Manifest, because that IS its result.
        return registry.create("Null Reader", query_properties, **node_config)
    elif connector and node_config.get("manifest") is not None:
        # Manifest-backed Scan: dispatch on the dataset's single format.
        # For parquet this is the column-chunk range-read path: footer-first
        # planning, per-row-group morsels; works for any connector (local, GCS,
        # S3, Opteryx catalog) — filesystem is resolved from file-path protocol
        # inside the reader if not provided directly by the connector.
        reader_name = _scan_reader_for_manifest(
            node_config.get("manifest"), str(node_config.get("relation", ""))
        )
        if reader_name == "JSONL Reader":
            node_config = _jsonl_scan_config(node_config)
        elif reader_name == "Skene Reader":
            node_config = _skene_scan_config(node_config)
        return registry.create(reader_name, query_properties, **node_config)
    elif connector and getattr(connector, "interal_only", False):
        # Internal virtual datasets (for example $no_table) do not use file manifests.
        return registry.create("Reader", query_properties, **node_config)
    else:
        raise UnsupportedSyntaxError(
            "Scans require a file manifest. Non-manifest external scan paths have been removed."
        )


def _create_materialized_cte_ref_node(logical_node, query_properties, registry):
    return registry.create("CTE Reference", query_properties, **logical_node.properties)


def _create_set_node(logical_node, query_properties, registry):
    return registry.create("Set Variable", query_properties, **logical_node.properties)


def _create_show_node(logical_node, query_properties, registry):
    node_config = logical_node.properties
    object_type = node_config["object_type"]

    if object_type == "VARIABLE":
        return registry.create("Show Value", query_properties, kind=node_config["items"][1], value=node_config["items"][1], **node_config)
    elif object_type == "VIEW":
        return registry.create("Show Create", query_properties, **node_config)
    else:
        raise UnsupportedSyntaxError(f"Unsupported SHOW type '{object_type}'")


def _create_create_view_node(logical_node, query_properties, registry):
    return registry.create("View Management", query_properties, action="create_view", **logical_node.properties)


def _create_alter_view_node(logical_node, query_properties, registry):
    return registry.create("View Management", query_properties, action="alter_view", **logical_node.properties)


def _create_drop_view_node(logical_node, query_properties, registry):
    return registry.create("View Management", query_properties, action="drop_view", **logical_node.properties)


def _create_show_columns_node(logical_node, query_properties, registry):
    return registry.create("Show Columns", query_properties, **logical_node.properties)


def _create_show_manifest_node(logical_node, query_properties, registry):
    return registry.create("Show Manifest", query_properties, **logical_node.properties)


def _create_show_snapshots_node(logical_node, query_properties, registry):
    return registry.create("Show Snapshots", query_properties, **logical_node.properties)


def _create_union_node(logical_node, query_properties, registry):
    return registry.create("Union", query_properties, **logical_node.properties)


def _create_unnest_node(logical_node, query_properties, registry):
    return registry.create("Unnest Join", query_properties, **logical_node.properties)


def _create_analyze_node(logical_node, query_properties, registry):
    return registry.create("Table Management", query_properties, **logical_node.properties)


def _create_comment_node(logical_node, query_properties, registry):
    # COMMENT ON VIEW/TABLE/EXTENSION - use ViewManagementNode with 'comment' action
    return registry.create("View Management", query_properties, action="comment", **logical_node.properties)


def _create_create_relation_node(logical_node, query_properties, registry):
    return registry.create("Relation Management", query_properties, action="create_relation", **logical_node.properties)


def _create_drop_relation_node(logical_node, query_properties, registry):
    return registry.create("Relation Management", query_properties, action="drop_relation", **logical_node.properties)


def _create_create_collection_node(logical_node, query_properties, registry):
    return registry.create("Relation Management", query_properties, action="create_collection", **logical_node.properties)


def _create_drop_collection_node(logical_node, query_properties, registry):
    return registry.create("Relation Management", query_properties, action="drop_collection", **logical_node.properties)


def _create_truncate_relation_node(logical_node, query_properties, registry):
    return registry.create("Relation Management", query_properties, action="truncate_relation", **logical_node.properties)


def _create_alter_relation_node(logical_node, query_properties, registry):
    return registry.create("Relation Management", query_properties, action="cluster_by", **logical_node.properties)


def _create_create_tag_node(logical_node, query_properties, registry):
    return registry.create("Relation Management", query_properties, action="create_tag", **logical_node.properties)


def _create_drop_tag_node(logical_node, query_properties, registry):
    return registry.create("Relation Management", query_properties, action="drop_tag", **logical_node.properties)


def _create_rename_relation_node(logical_node, query_properties, registry):
    return registry.create("Relation Management", query_properties, action="rename_relation", **logical_node.properties)


def _create_add_column_node(logical_node, query_properties, registry):
    return registry.create("Relation Management", query_properties, action="add_column", **logical_node.properties)


def _create_drop_column_node(logical_node, query_properties, registry):
    return registry.create("Relation Management", query_properties, action="drop_column", **logical_node.properties)


def _create_rename_column_node(logical_node, query_properties, registry):
    return registry.create("Relation Management", query_properties, action="rename_column", **logical_node.properties)


def _create_alter_column_type_node(logical_node, query_properties, registry):
    return registry.create("Relation Management", query_properties, action="alter_column_type", **logical_node.properties)


def _create_optimize_relation_node(logical_node, query_properties, registry):
    return registry.create("Relation Management", query_properties, action="optimize_relation", **logical_node.properties)


def _create_alter_workspace_node(logical_node, query_properties, registry):
    return registry.create("Relation Management", query_properties, action="alter_workspace", **logical_node.properties)


def _create_drop_workspace_node(logical_node, query_properties, registry):
    return registry.create("Relation Management", query_properties, action="drop_workspace", **logical_node.properties)


def _create_insert_node(logical_node, query_properties, registry):
    return registry.create("Insert", query_properties, **logical_node.properties)


def _create_merge_node(logical_node, query_properties, registry):
    return registry.create("Merge", query_properties, **logical_node.properties)


def _create_drop_trigger_node(logical_node, query_properties, registry):
    return registry.create("Relation Management", query_properties, action="drop_trigger", **logical_node.properties)


def _create_alter_materialized_view_owner_node(logical_node, query_properties, registry):
    return registry.create("Relation Management", query_properties, action="alter_materialized_view_owner", **logical_node.properties)


def _create_alter_materialized_view_suspended_node(logical_node, query_properties, registry):
    return registry.create("Relation Management", query_properties, action="alter_materialized_view_suspended", **logical_node.properties)


_DISPATCH = {
    LogicalPlanStepType.Aggregate:        _create_aggregate_node,
    LogicalPlanStepType.AggregateAndGroup: _create_aggregate_and_group_node,
    LogicalPlanStepType.Distinct:         _create_distinct_node,
    LogicalPlanStepType.Exit:             _create_exit_node,
    LogicalPlanStepType.Explain:          _create_explain_node,
    LogicalPlanStepType.Filter:           _create_filter_node,
    LogicalPlanStepType.FunctionDataset:  _create_function_dataset_node,
    LogicalPlanStepType.HeapSort:         _create_heap_sort_node,
    LogicalPlanStepType.Join:             _create_join_node,
    LogicalPlanStepType.Limit:            _create_limit_node,
    LogicalPlanStepType.ScalarSubqueryGuard: _create_scalar_guard_node,
    LogicalPlanStepType.Order:            _create_order_node,
    LogicalPlanStepType.Project:          _create_project_node,
    LogicalPlanStepType.Scan:             _create_scan_node,
    LogicalPlanStepType.MaterializedCteRef: _create_materialized_cte_ref_node,
    LogicalPlanStepType.Set:              _create_set_node,
    LogicalPlanStepType.Show:             _create_show_node,
    LogicalPlanStepType.CreateView:       _create_create_view_node,
    LogicalPlanStepType.AlterView:        _create_alter_view_node,
    LogicalPlanStepType.DropView:         _create_drop_view_node,
    LogicalPlanStepType.ShowColumns:      _create_show_columns_node,
    LogicalPlanStepType.ShowManifest:     _create_show_manifest_node,
    LogicalPlanStepType.ShowSnapshots:    _create_show_snapshots_node,
    LogicalPlanStepType.Union:            _create_union_node,
    LogicalPlanStepType.Window:           _create_window_node,
    LogicalPlanStepType.FramedWindow:     _create_framed_window_node,
    LogicalPlanStepType.Unnest:           _create_unnest_node,
    LogicalPlanStepType.Analyze:          _create_analyze_node,
    LogicalPlanStepType.Comment:          _create_comment_node,
    LogicalPlanStepType.CreateRelation:   _create_create_relation_node,
    LogicalPlanStepType.DropRelation:     _create_drop_relation_node,
    LogicalPlanStepType.CreateCollection: _create_create_collection_node,
    LogicalPlanStepType.DropCollection:   _create_drop_collection_node,
    LogicalPlanStepType.TruncateRelation: _create_truncate_relation_node,
    LogicalPlanStepType.AlterRelation:    _create_alter_relation_node,
    LogicalPlanStepType.RenameRelation:   _create_rename_relation_node,
    LogicalPlanStepType.CreateTag:        _create_create_tag_node,
    LogicalPlanStepType.DropTag:          _create_drop_tag_node,
    LogicalPlanStepType.AddColumn:        _create_add_column_node,
    LogicalPlanStepType.DropColumn:       _create_drop_column_node,
    LogicalPlanStepType.RenameColumn:     _create_rename_column_node,
    LogicalPlanStepType.AlterColumnType:  _create_alter_column_type_node,
    LogicalPlanStepType.OptimizeRelation: _create_optimize_relation_node,
    LogicalPlanStepType.AlterWorkspace:   _create_alter_workspace_node,
    LogicalPlanStepType.DropWorkspace:    _create_drop_workspace_node,
    LogicalPlanStepType.Insert:           _create_insert_node,
    LogicalPlanStepType.Merge:            _create_merge_node,
    LogicalPlanStepType.DropTrigger:      _create_drop_trigger_node,
    LogicalPlanStepType.AlterMaterializedViewOwner: _create_alter_materialized_view_owner_node,
    LogicalPlanStepType.AlterMaterializedViewSuspended: _create_alter_materialized_view_suspended_node,
}


def create_physical_plan(logical_plan, query_properties, shared_ctes=None) -> PhysicalPlan:
    plan = PhysicalPlan()
    registry = get_registry()

    for nid, logical_node in logical_plan.nodes(data=True):
        creator = _DISPATCH.get(logical_node.node_type)
        if creator is None:  # pragma: no cover
            raise InvalidInternalStateError(
                f"Unexpected logical node encountered during physical planning: {logical_node.node_type.name}"
            )
        node = creator(logical_node, query_properties, registry)

        # Copy optimizer/binder attached metadata from logical node to physical node
        node.manifest = logical_node.manifest
        if getattr(logical_node, "uuid", None) is not None:
            node.uuid = logical_node.uuid

        plan.add_node(nid, node)

    for source, destination, relation in logical_plan.edges():
        plan.add_edge(source, destination, relation)

    # Shared CTE bodies become physical plans of their own, carried on the main
    # physical plan (dependencies first — the plan compiler lowers each body into
    # a producer pipeline before any pipeline that reads it). A body has no Exit
    # node: its head feeds a buffer-append sink, not the output queue.
    plan.shared_ctes = {
        cte_key: create_physical_plan(body, query_properties)
        for cte_key, body in (shared_ctes or {}).items()
    }

    return plan
