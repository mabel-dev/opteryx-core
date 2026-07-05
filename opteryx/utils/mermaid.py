from opteryx.models import PhysicalPlan
from opteryx.operators.catalog import OperatorCategory, get_registry


def _get_logical_node_type(node):
    # Helper function to get logical node type using catalog category or name heuristics
    try:
        # First, try to use catalog metadata for reliable type detection
        registry = get_registry()
        metadata = registry.get(node.__class__)
        if metadata:
            # Some operators share the SET_OP category but represent
            # different logical relations. Distinct is semantically an
            # aggregate-style deduplication, not a union.
            if metadata.name == "Distinct":
                return "AggregateRel"
            if metadata.name == "Union":
                return "UnionRel"
            category = metadata.category
            category_map = {
                OperatorCategory.SCAN: "ReadRel",
                OperatorCategory.JOIN: "JoinRel",
                OperatorCategory.AGGREGATE: "AggregateRel",
                OperatorCategory.PROJECT: "ProjectRel",
                OperatorCategory.FILTER: "FilterRel",
                OperatorCategory.LIMIT: "LimitRel",
                OperatorCategory.SORT: "SortRel",
                OperatorCategory.SET_OP: "SetRel",
                OperatorCategory.DDL: "DDLRel",
                OperatorCategory.IO: "IRel",
            }
            return category_map.get(category)

        # Fall back to name-based heuristics for operators not in catalog
        if getattr(node, "is_scan", False):
            return "ReadRel"
        if getattr(node, "is_join", False):
            return "JoinRel"

        candidate = getattr(node, "name", None) or getattr(node, "node_type", None)
        if candidate is None:
            return None
        s = str(candidate).lower()
        if "aggregate" in s or "group" in s or "distinct" in s:
            return "AggregateRel"
        if "project" in s or "projection" in s:
            return "ProjectRel"
        if "filter" in s or "where" in s:
            return "FilterRel"
        if "limit" in s:
            return "LimitRel"
        if "sort" in s or "order" in s:
            return "SortRel"
        if "union" in s:
            return "UnionRel"
        if "exit" in s:
            return "ExitRel"
        # default: title-case the candidate and append Rel
        token = str(candidate)
        token = token.replace(" ", "_").replace("-", "_")
        token = token[0].upper() + token[1:] if token else token
        return f"{token}Rel"
    except (AttributeError, ValueError, TypeError):
        return None


# Exact, architect-specified display label per operator CLASS — distinct from
# _get_logical_node_type's coarse category bucket, which can't tell a Parquet
# scan from a function scan, or a hashed group-by from a plain aggregate.
_OPERATOR_LABELS = {
    "ParquetReadNode": "TABLE SCAN",
    "ReaderNode": "SCAN",
    "NullReaderNode": "SCAN",
    "FunctionDatasetNode": "FUNCTION SCAN",
    "FilterNode": "FILTER",
    "ProjectionNode": "PROJECT",
    "WindowNode": "WINDOW",
    "GroupedAggregateHashedNode": "HASHED AGGREGATE",
    "UngroupedAggregateNode": "UNGROUPED AGGREGATE",
    "DistinctNode": "DISTINCT",
    "SortNode": "SORT",
    "HeapSortNode": "HEAP SORT",
    "LimitNode": "LIMIT",
    "DrakenInnerJoinNode": "HASH JOIN",
    "NestedLoopJoinNode": "NESTED JOIN",
    "NonEquiJoinNode": "NON EQUI JOIN",
    "AsofJoinNode": "ASOF JOIN",
    "CrossJoinNode": "CROSS JOIN",
    "UnnestJoinNode": "UNNEST JOIN",
    "ExitNode": "EXIT",
}

# OuterJoinNode/FilterJoinNode carry their variant in a dynamic self.join_type
# instead of a fixed class-level one — abbreviated to match the parenthetical
# convention (e.g. "OUTER JOIN (L)", "FILTER JOIN (LANA)").
_OUTER_JOIN_DIRECTIONS = {
    "left outer": "L",
    "right outer": "R",
    "full outer": "O",
}
_FILTER_JOIN_DIRECTIONS = {
    "left semi": "LS",
    "left anti": "LA",
    "left anti null-aware": "LANA",
}


def _get_operator_label(node):
    class_name = node.__class__.__name__
    if class_name == "OuterJoinNode":
        direction = _OUTER_JOIN_DIRECTIONS.get(node.join_type, node.join_type)
        return f"OUTER JOIN ({direction})"
    if class_name == "FilterJoinNode":
        direction = _FILTER_JOIN_DIRECTIONS.get(node.join_type, node.join_type)
        return f"FILTER JOIN ({direction})"
    return _OPERATOR_LABELS.get(class_name)


def _collect_node_stats(plan: PhysicalPlan, stats: list = None):
    """Build the per-node stats (keyed by node UID) and edge list for a plan.

    This is the definitive per-node/edge record: native op stats overlaid onto
    the plan-node identity, harvested after the run. As a side effect it
    populates ``node.telemetry.operations`` and the shared ``telemetry.edges``
    list — the structured source consumers should read instead of parsing a
    rendered diagram.

    Returns ``(node_stats_by_nid, node_map, excluded_nodes)`` for callers that
    still need to render a diagram from this data (e.g. EXPLAIN).
    """
    # Map node ids to node objects for telemetry fallbacks
    node_map = {nid: node for nid, node in plan.nodes(True)}

    def get_node_stats(plan: PhysicalPlan):
        stats = []
        for nid, node in plan.nodes(True):
            if node.is_not_explained:
                continue
            node_stat = {
                "identity": node.identity,
                "records_in": node.records_in,
                "bytes_in": node.bytes_in,
                "records_out": node.records_out,
                "bytes_out": node.bytes_out,
                "calls": node.calls,
            }
            # Add sensor readings from the node
            sensors = node.sensors()
            node_stat.update(sensors)

            # Native-engine per-operator telemetry, harvested after the run and keyed by
            # plan-node identity (execute_native). The plan-node Python objects never
            # execute on the native path — the C++ Engine does — so their own counters
            # stay zero; overlay the real readings here.
            native_stats = node.telemetry._reading.get("native_op_stats")
            if native_stats:
                native = native_stats.get(node.identity)
                if native:
                    node_stat.update(native)
                    # self_time == execution_time on the native path: the executor times
                    # each operator's own call (the recursive downstream forward is
                    # excluded), so there is no separate downstream component to subtract.
                    node_stat["self_time"] = native["execution_time"]
                    node_stat["downstream_time"] = 0

            # Add telemetry-specific readings for reader nodes
            if node.is_scan:
                node_stat["rows_read"] = getattr(node.telemetry, "rows_read", 0)
                node_stat["blobs_read"] = getattr(node.telemetry, "blobs_read", 0)
                node_stat["bytes_processed"] = getattr(node.telemetry, "bytes_processed", 0)
                node_stat["columns_read"] = getattr(node.telemetry, "columns_read", 0)

            # Add node-specific attributes
            if getattr(node, "columns", None):
                node_stat["columns"] = len(node.columns)
            if getattr(node, "limit", None) is not None:
                node_stat["limit"] = node.limit
            if getattr(node, "predicates", None):
                node_stat["has_filters"] = True
            if getattr(node, "left_filter", None) is not None:
                node_stat["bloom_filter"] = True
            if getattr(node, "at_date", None):
                node_stat["at_date"] = str(node.at_date)
            if getattr(node, "committed_at", None):
                node_stat["committed_at"] = node.committed_at

            stats.append(node_stat)
        return stats

    node_stats = {x["identity"]: x for x in get_node_stats(plan)}
    if stats:
        for stat in stats:
            node_stats[stat["identity"]] = stat

    # ExitNode is an internal engine relation, not a user-facing operator — it
    # carries no useful telemetry of its own (a pure pass-through) and every
    # consumer (EXPLAIN's own tree builder, Studio's operator-tree renderer)
    # already splices it out and treats its child as the root. Drop it (and
    # the edge into it) from the structured telemetry entirely, rather than
    # emitting it for every consumer to filter back out. This is scoped to
    # telemetry.operations/.edges only — plan_to_mermaid's own diagram string
    # (EXPLAIN) still draws it, unchanged, via node_stats/excluded_nodes below.
    exit_nids = {nid for nid, node in plan.nodes(True) if node.__class__.__name__ == "ExitNode"}

    # Store detailed stats in telemetry operations with node UID as key and type as field
    for nid, node in plan.nodes(True):
        if not node.is_not_explained and nid not in exit_nids:
            stat = node_stats.get(node.identity)
            if stat:
                # Add node type to the stat dictionary
                node_type = _get_logical_node_type(node)
                if node_type:
                    stat["type"] = node_type
                # Exact operator display label (TABLE SCAN, HASH JOIN, etc.) —
                # separate from the coarse "type" category bucket above.
                operator_label = _get_operator_label(node)
                if operator_label:
                    stat["operator"] = operator_label
                # Remove identity field - it's redundant with the key
                stat.pop("identity", None)
                # Use node UID (nid) as the key
                node.telemetry.operations[nid] = stat

    # Build a structured edge list so consumers can reconstruct the plan DAG
    # without parsing the Mermaid string. Direction: from = producer, to = consumer.
    _raw_edges = list(plan.edges())

    excluded_nodes = []
    _any_telemetry = None
    for nid, node in plan.nodes(True):
        if node.is_not_explained:
            excluded_nodes.append(nid)
            continue
        node_stats[nid] = node_stats.pop(node.identity, None)
        if _any_telemetry is None:
            _any_telemetry = node.telemetry

    # Write the edge list to telemetry now that excluded_nodes is finalised
    if _any_telemetry is not None:
        _any_telemetry.edges = [
            {"from": s, "to": t, **(({"leg": r}) if r else {})}
            for s, t, r in _raw_edges
            if s not in excluded_nodes and t not in excluded_nodes and t not in exit_nids
        ]

    return node_stats, node_map, excluded_nodes


def collect_plan_telemetry(plan: PhysicalPlan, stats: list = None) -> None:
    """Populate node.telemetry.operations/.edges from the plan.

    This is the data source for callers that need the plan's structure and
    metrics (e.g. per-query telemetry sent to worker/jobs) but not a rendered
    diagram — no mermaid string is built here.
    """
    _collect_node_stats(plan, stats)


def plan_to_mermaid(plan: PhysicalPlan, stats: list = None) -> str:
    node_stats, node_map, excluded_nodes = _collect_node_stats(plan, stats)
    builder = ""

    for nid, node in plan.nodes(True):
        if node.is_not_explained:
            continue
        builder += f"  {node.to_mermaid(nid)}\n"
    builder += "\n"

    for s, t, r in plan.edges():
        if t in excluded_nodes:
            continue
        stats_ = node_stats.get(s) or {}
        # Prefer node-specific stats (records_out/bytes_out). Only fall back to
        # the node's telemetry for reader/scan nodes or when the stats are
        # missing/zero. This avoids propagating reader telemetry across
        # non-scan nodes which can produce misleading arrow labels.
        source_node = node_map.get(s)
        records = stats_.get("records_out")
        bytes_ = stats_.get("bytes_out")
        if source_node is not None:
            # Use telemetry only for scan nodes or when summary stats are absent/zero
            telemetry_rows = getattr(source_node.telemetry, "rows_read", None)
            telemetry_bytes = getattr(source_node.telemetry, "bytes_processed", None)
            if (
                (records is None or records == 0)
                and getattr(source_node, "is_scan", False)
                and telemetry_rows not in (None, 0)
            ):
                records = telemetry_rows
            if (
                (bytes_ is None or bytes_ == 0)
                and getattr(source_node, "is_scan", False)
                and telemetry_bytes not in (None, 0)
            ):
                bytes_ = telemetry_bytes

        records = 0 if records is None else records
        bytes_ = 0 if bytes_ is None else bytes_
        join_leg = f"**{r.upper()}**<br />" if r else ""
        builder += (
            f'  NODE_{s} -- "{join_leg} {records:,} rows<br />{bytes_:,} bytes" --> NODE_{t}\n'
        )

    # Add termination node
    exit_points = plan.get_exit_points()
    if exit_points:
        exit_node = plan[exit_points[0]]
        total_duration = sum(node.execution_time for nid, node in plan.nodes(True)) / 1e6
        # Prefer telemetry for final counts when present
        final_rows = getattr(exit_node.telemetry, "rows_read", None) or exit_node.records_out
        final_bytes = getattr(exit_node.telemetry, "bytes_processed", None) or exit_node.bytes_out
        final_columns = len(exit_node.columns) if getattr(exit_node, "columns", None) is not None else 0

        builder += f'  NODE_TERMINUS(["{final_rows} rows<br />{final_columns} columns<br />({total_duration:,.2f}ms)"])\n'

        # Find the node feeding into ExitNode
        ingoing = plan.ingoing_edges(exit_points[0])
        if ingoing:
            source_nid = ingoing[0][0]
            builder += f'  NODE_{source_nid} -- "{final_rows:,} rows<br />{final_bytes:,} bytes" --> NODE_TERMINUS\n'

    return "flowchart LR\n\n" + builder
