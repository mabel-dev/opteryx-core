from opteryx.models import PhysicalPlan


def plan_to_mermaid(plan: PhysicalPlan, stats: list = None) -> str:
    excluded_nodes = []
    builder = ""

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

            # Add telemetry-specific readings for reader nodes
            if node.is_scan:
                node_stat["rows_read"] = getattr(node.telemetry, "rows_read", 0)
                node_stat["blobs_read"] = getattr(node.telemetry, "blobs_read", 0)
                node_stat["bytes_processed"] = getattr(node.telemetry, "bytes_processed", 0)
                node_stat["columns_read"] = getattr(node.telemetry, "columns_read", 0)

            # Add node-specific attributes
            if hasattr(node, "columns") and node.columns:
                node_stat["columns"] = len(node.columns)
            if hasattr(node, "limit") and node.limit is not None:
                node_stat["limit"] = node.limit
            if hasattr(node, "predicates") and node.predicates:
                node_stat["has_filters"] = True
            if hasattr(node, "left_filter") and node.left_filter is not None:
                node_stat["bloom_filter"] = True
            if hasattr(node, "at_date") and node.at_date:
                node_stat["at_date"] = str(node.at_date)
            if hasattr(node, "committed_at") and node.committed_at:
                node_stat["committed_at"] = node.committed_at

            stats.append(node_stat)
        return stats

    node_stats = {x["identity"]: x for x in get_node_stats(plan)}
    if stats:
        for stat in stats:
            node_stats[stat["identity"]] = stat

    # Helper function to get logical node type (same as in cursor._get_plan_dict)
    def _get_logical_node_type(node):
        try:
            if getattr(node, "is_scan", False):
                return "ReadRel"
            if getattr(node, "is_join", False):
                return "JoinRel"
            # fall back to name-based heuristics
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

    # Store detailed stats in telemetry operations with node UID as key and type as field
    for nid, node in plan.nodes(True):
        if not node.is_not_explained:
            stat = node_stats.get(node.identity)
            if stat:
                # Add node type to the stat dictionary
                node_type = _get_logical_node_type(node)
                if node_type:
                    stat["type"] = node_type
                # Remove identity field - it's redundant with the key
                stat.pop("identity", None)
                # Use node UID (nid) as the key
                node.telemetry.operations[nid] = stat

    for nid, node in plan.nodes(True):
        if node.is_not_explained:
            excluded_nodes.append(nid)
            continue
        builder += f"  {node.to_mermaid(nid)}\n"
        node_stats[nid] = node_stats.pop(node.identity, None)
    builder += "\n"
    for s, t, r in plan.edges():
        if t in excluded_nodes:
            continue
        stats = node_stats.get(s) or {}
        # Prefer node-specific stats (records_out/bytes_out). Only fall back to
        # the node's telemetry for reader/scan nodes or when the stats are
        # missing/zero. This avoids propagating reader telemetry across
        # non-scan nodes which can produce misleading arrow labels.
        source_node = node_map.get(s)
        records = stats.get("records_out")
        bytes_ = stats.get("bytes_out")
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
        final_columns = len(exit_node.columns) if hasattr(exit_node, "columns") else 0

        builder += f'  NODE_TERMINUS(["{final_rows} rows<br />{final_columns} columns<br />({total_duration:,.2f}ms)"])\n'

        # Find the node feeding into ExitNode
        ingoing = plan.ingoing_edges(exit_points[0])
        if ingoing:
            source_nid = ingoing[0][0]
            builder += f'  NODE_{source_nid} -- "{final_rows:,} rows<br />{final_bytes:,} bytes" --> NODE_TERMINUS\n'

    return "flowchart LR\n\n" + builder
