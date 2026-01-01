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
            stats.append(node_stat)
        return stats

    node_stats = {x["identity"]: x for x in get_node_stats(plan)}
    if stats:
        for stat in stats:
            node_stats[stat["identity"]] = stat

    for nid, node in plan.nodes(True):
        if node.is_not_explained:
            excluded_nodes.append(nid)
            continue
        builder += f"  {node.to_mermaid(node_stats.get(node.identity), nid)}\n"
        node_stats[nid] = node_stats.pop(node.identity, None)
    builder += "\n"
    for s, t, r in plan.edges():
        if t in excluded_nodes:
            continue
        stats = node_stats.get(s) or {}
        # Prefer telemetry values from the node when available (more accurate for reads)
        source_node = node_map.get(s)
        records = stats.get("records_out")
        bytes_ = stats.get("bytes_out")
        if source_node is not None:
            telemetry_rows = getattr(source_node.telemetry, "rows_read", None)
            telemetry_bytes = getattr(source_node.telemetry, "bytes_processed", None)
            if telemetry_rows not in (None, 0):
                records = telemetry_rows
            if telemetry_bytes not in (None, 0):
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
