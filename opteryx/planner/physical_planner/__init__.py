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
                 CrossJoinNode, NonEquiJoinNode, NestedLoopJoinNode, AsofJoinNode
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
from opteryx.exceptions import UnsupportedSyntaxError
from opteryx.expression import NodeType
from opteryx.models import PhysicalPlan
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
        left, right = condition.left, condition.right
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
        left, right = condition.left, condition.right
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


def _manifest_is_all_parquet(manifest) -> bool:
    """Return True if every file in *manifest* has a .parquet extension.

    An empty manifest (no files) is treated as parquet-compatible — it
    represents an empty relation and the parquet reader yields nothing.
    """
    if manifest is None:
        return False
    files = getattr(manifest, "files", None)
    if files is None:
        return False
    if len(files) == 0:
        return True
    return all(getattr(f, "file_path", "").endswith(".parquet") for f in files)


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
        **{k: v for k, v in node_config.items() if k in ("aggregates", "groups", "projection", "all_relations", "having_condition", "group_map_variant")},
    )


def _create_distinct_node(logical_node, query_properties, registry):
    node_config = logical_node.properties
    return registry.create(
        "Distinct",
        query_properties,
        **{k: v for k, v in node_config.items() if k in ("on", "set_variant")},
    )


def _create_window_node(logical_node, query_properties, registry):
    node_config = logical_node.properties
    return registry.create(
        "Window",
        query_properties,
        **{
            k: v
            for k, v in node_config.items()
            if k in ("partition_by", "order_by", "window_functions", "top_k")
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
        raise UnsupportedSyntaxError("Draken inner join does not support this query shape")
    elif join_type == "nested loop":
        # NESTED LOOP JOIN (INNER JOIN)
        return registry.create("Nested Loop Join", query_properties, **node_config)
    elif join_type == "non equi":
        # NON-EQUI JOIN (!=, >, >=, <, <=)
        return registry.create("Non Equi Join", query_properties, **node_config)
    elif join_type in ("left outer", "full outer", "right outer"):
        # LEFT JOIN, RIGHT JOIN, FULL JOIN
        return registry.create("Outer Join", query_properties, **node_config)
    elif join_type == "cross join":
        # CROSS JOIN, CROSS JOIN UNNEST
        return registry.create("Cross Join", query_properties, **node_config)
    elif join_type in ("left anti", "left semi", "left anti null-aware"):
        # LEFT SEMI, LEFT ANTI, LEFT ANTI NULL-AWARE (NOT IN) JOIN
        return registry.create("Filter Join", query_properties, **node_config)
    elif join_type == "asof":
        # ASOF JOIN — nearest-neighbour time-series join
        return registry.create("ASOF Join", query_properties, **node_config)
    else:
        # We don't support other JOIN types, e.g. RIGHT SEMI, RIGHT ANTI
        raise InvalidInternalStateError(f"Unsupported JOIN type '{join_type}'")


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
        **{k: v for k, v in node_config.items() if k in ("order_by", "all_relations")},
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
    elif connector and _manifest_is_all_parquet(node_config.get("manifest")):
        # Column-chunk range-read path: footer-first planning, per-row-group morsels.
        # Works for any connector (local, GCS, S3, Opteryx catalog) — filesystem
        # is resolved from file-path protocol inside ParquetReadNode if not provided
        # directly by the connector.
        return registry.create("Parquet Reader", query_properties, **node_config)
    elif connector and getattr(connector, "interal_only", False):
        # Internal virtual datasets (for example $no_table) do not use file manifests.
        return registry.create("Reader", query_properties, **node_config)
    else:
        raise UnsupportedSyntaxError(
            "Only Parquet scans are supported. Non-parquet external scan paths have been removed."
        )


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


def _create_drop_collection_node(logical_node, query_properties, registry):
    return registry.create("Relation Management", query_properties, action="drop_collection", **logical_node.properties)


def _create_truncate_relation_node(logical_node, query_properties, registry):
    return registry.create("Relation Management", query_properties, action="truncate_relation", **logical_node.properties)


def _create_alter_relation_node(logical_node, query_properties, registry):
    return registry.create("Relation Management", query_properties, action="cluster_by", **logical_node.properties)


def _create_insert_node(logical_node, query_properties, registry):
    return registry.create("Insert", query_properties, **logical_node.properties)


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
    LogicalPlanStepType.Order:            _create_order_node,
    LogicalPlanStepType.Project:          _create_project_node,
    LogicalPlanStepType.Scan:             _create_scan_node,
    LogicalPlanStepType.Set:              _create_set_node,
    LogicalPlanStepType.Show:             _create_show_node,
    LogicalPlanStepType.CreateView:       _create_create_view_node,
    LogicalPlanStepType.AlterView:        _create_alter_view_node,
    LogicalPlanStepType.DropView:         _create_drop_view_node,
    LogicalPlanStepType.ShowColumns:      _create_show_columns_node,
    LogicalPlanStepType.ShowManifest:     _create_show_manifest_node,
    LogicalPlanStepType.Union:            _create_union_node,
    LogicalPlanStepType.Window:           _create_window_node,
    LogicalPlanStepType.Unnest:           _create_unnest_node,
    LogicalPlanStepType.Analyze:          _create_analyze_node,
    LogicalPlanStepType.Comment:          _create_comment_node,
    LogicalPlanStepType.CreateRelation:   _create_create_relation_node,
    LogicalPlanStepType.DropRelation:     _create_drop_relation_node,
    LogicalPlanStepType.DropCollection:   _create_drop_collection_node,
    LogicalPlanStepType.TruncateRelation: _create_truncate_relation_node,
    LogicalPlanStepType.AlterRelation:    _create_alter_relation_node,
    LogicalPlanStepType.Insert:           _create_insert_node,
}


def create_physical_plan(logical_plan, query_properties) -> PhysicalPlan:
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

    return plan
