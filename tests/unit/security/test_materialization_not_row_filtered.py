"""A row-filtered build or refresh of a row-filtered materialized view is refused.

`REFRESH MATERIALIZED VIEW` desugars to the view's defining SELECT written back
over its backing table. If the refresher's row filter applies to the sources,
the view is rebuilt with only the refresher's rows - and every other reader,
who reads the view through their OWN filter, finds nothing.

Seen in production on `platform.billing.charges_daily_*`: their refresh
triggers moved to an identity billed to the house account in Sep 2026, and
every refresh since held `billing_account = 'opteryx'` only. Every tenant's
charges read as zero, with no error anywhere.
"""

import os
import sys

import pytest
from opteryx.compiled.structures.plan_steps import ExitStep
from opteryx.compiled.structures.plan_steps import InsertStep
from opteryx.compiled.structures.plan_steps import ScanStep

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

from opteryx.exceptions import PermissionsError
from opteryx.models import QueryTelemetry
from opteryx.planner.logical_planner import LogicalPlan
from opteryx.planner.logical_planner import LogicalPlanStepType
from opteryx.planner.logical_planner import apply_visibility_filters
from opteryx.planner.plan_context import PlanContext

SOURCE = "platform.billing.usage_daily"
TARGET = "platform.billing.charges_daily_metered"
FILTER = [("billing_account", "Eq", "opteryx")]


def _plan(target: str, *, refresh=False, create=False) -> LogicalPlan:
    """Scan -> Exit -> Insert, the shape `_plan_ctas` builds."""
    plan = LogicalPlan(PlanContext())
    scan = ScanStep()
    scan.relation = SOURCE
    scan_nid = plan.add_node(scan)
    exit_nid = plan.add_node(ExitStep())
    plan.add_edge(scan_nid, exit_nid)

    insert = InsertStep()
    insert.relation_name = target
    insert.is_refresh = refresh
    insert.is_materialized_view = create
    insert_nid = plan.add_node(insert)
    plan.add_edge(exit_nid, insert_nid)
    return plan


def _apply(plan, filters):
    telemetry = QueryTelemetry.detached()
    result = apply_visibility_filters(plan, filters, telemetry, plan_context=PlanContext())
    return result, telemetry


@pytest.mark.parametrize("refresh,create", [(True, False), (False, True)])
@pytest.mark.parametrize(
    "filters",
    [
        {"platform.billing.*": FILTER},  # production's namespace pattern
        {SOURCE: FILTER, TARGET: FILTER},  # exact keys
        {"platform.billing.*": []},  # deny-all: would materialize empty
    ],
)
def test_filtered_materialization_is_refused(refresh, create, filters):
    with pytest.raises(PermissionsError):
        _apply(_plan(TARGET, refresh=refresh, create=create), filters)


def test_exempt_refresh_is_untouched():
    """The exempt caller arrives with no filters and must proceed unfiltered."""
    result, telemetry = _apply(_plan(TARGET, refresh=True), {})
    assert telemetry.visibility_filters_condition_added == 0
    assert not any(n.node_type == LogicalPlanStepType.Filter for _, n in result.nodes(True))


def test_unfiltered_target_is_a_filtered_read():
    """A user's own materialized view outside the filtered namespace has no
    other readers to short-change: it is built from the rows they may see."""
    result, telemetry = _apply(
        _plan("home.ginny.my_charges", refresh=True), {"platform.billing.*": FILTER}
    )
    assert telemetry.visibility_filters_condition_added == 1
    assert any(n.node_type == LogicalPlanStepType.Filter for _, n in result.nodes(True))


def test_plain_insert_into_filtered_namespace_is_still_filtered():
    """Only materialized-view writes refuse; an ordinary CTAS/INSERT is a read
    under the caller's filter as before."""
    result, telemetry = _apply(_plan(TARGET), {"platform.billing.*": FILTER})
    assert telemetry.visibility_filters_condition_added == 1


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__, "-v"])
