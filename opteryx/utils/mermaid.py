from opteryx.models import PhysicalPlan


def plan_to_mermaid(plan: PhysicalPlan) -> str:
    excluded_nodes = []
    builder = ""

    def get_node_stats(plan: PhysicalPlan):
        stats = []
        for nid, node in plan.nodes(True):
            if node.is_not_explained:
                continue
            node_stat = {
                "identity": node.identity,
                "records_in": node.bytes_in,
                "bytes_in": node.bytes_in,
                "records_out": node.bytes_out,
                "bytes_out": node.bytes_out,
                "calls": node.calls,
            }
            stats.append(node_stat)
        return stats

    node_stats = {x["identity"]: x for x in get_node_stats(plan)}

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

    return "flowchart LR\n\n" + builder
