from opteryx.models import PhysicalPlan


def plan_to_mermaid(plan: PhysicalPlan, stats: list = None) -> str:
    excluded_nodes = []
    builder = ""

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
        stats = node_stats.get(s)
        join_leg = f"**{r.upper()}**<br />" if r else ""
        builder += f'  NODE_{s} -- "{join_leg} {stats.get("records_out"):,} rows<br />{stats.get("bytes_out"):,} bytes" --> NODE_{t}\n'

    # Add termination node
    exit_points = plan.get_exit_points()
    if exit_points:
        exit_node = plan[exit_points[0]]
        total_duration = sum(node.execution_time for nid, node in plan.nodes(True)) / 1e6
        final_rows = exit_node.records_out
        final_bytes = exit_node.bytes_out
        final_columns = len(exit_node.columns) if hasattr(exit_node, "columns") else 0

        builder += f'  NODE_TERMINUS(["{final_rows} rows<br />{final_columns} columns<br />({total_duration:,.2f}ms)"])\n'

        # Find the node feeding into ExitNode
        ingoing = plan.ingoing_edges(exit_points[0])
        if ingoing:
            source_nid = ingoing[0][0]
            builder += f'  NODE_{source_nid} -- "{final_rows:,} rows<br />{final_bytes:,} bytes" --> NODE_TERMINUS\n'

    return "flowchart LR\n\n" + builder
