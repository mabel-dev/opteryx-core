from opteryx.models.physical_plan import PhysicalPlan
from opteryx.models.query_properties import QueryProperties
from opteryx.operators._operators import DistinctNode
from opteryx.utils.mermaid import plan_to_mermaid


def test_mermaid_telemetry_marks_distinct_as_aggregate_rel():
    plan = PhysicalPlan()
    node = DistinctNode(QueryProperties(query_id="mermaid-distinct", variables={}), on=None)
    plan.add_node("N1", node)

    _ = plan_to_mermaid(plan)

    assert "N1" in node.telemetry.operations
    assert node.telemetry.operations["N1"]["type"] == "AggregateRel"
