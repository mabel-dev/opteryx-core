"""A row-filtered OPTIMIZE is refused, never filtered.

`OPTIMIZE TABLE x` desugars to `SELECT * FROM x` under a CompactionCommit sink,
so it reaches `apply_visibility_filters` looking like an ordinary read. It is
not one: its rows are rewritten back and the input files are retired, so a
filter that would merely hide rows from a SELECT DELETES them here.

Seen in production on `platform.billing.*` (the one namespace carrying a
filter): compaction billed to the house account read only
`billing_account = 'opteryx'` and would have rewritten 57,469 rows as 44,377 in
`platform.billing.events`, retiring the files holding the other 13,092. The
catalog's row-count invariant refused the commit, and was the only thing that
did.
"""

import os
import sys

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

from opteryx.exceptions import PermissionsError
from opteryx.models import QueryTelemetry
from opteryx.planner.logical_planner import LogicalPlan
from opteryx.planner.logical_planner import LogicalPlanNode
from opteryx.planner.logical_planner import LogicalPlanStepType
from opteryx.planner.logical_planner import apply_visibility_filters


def _plan(relation: str, *, compaction: bool) -> LogicalPlan:
    """A one-scan plan, optionally under a compaction sink."""
    plan = LogicalPlan()
    scan = LogicalPlanNode(node_type=LogicalPlanStepType.Scan)
    scan.relation = relation
    plan.add_node("scan", scan)

    head_type = (
        LogicalPlanStepType.CompactionCommit if compaction else LogicalPlanStepType.Exit
    )
    head = LogicalPlanNode(node_type=head_type)
    head.relation_name = relation
    plan.add_node("head", head)
    plan.add_edge("scan", "head")
    return plan


FILTER = [("billing_account", "Eq", "opteryx")]


@pytest.mark.parametrize(
    "key",
    [
        "platform.billing.events",  # exact key
        "platform.billing.*",  # namespace pattern - what production uses
    ],
)
def test_filtered_compaction_is_refused(key):
    """Both match paths refuse, because both would narrow the rewrite."""
    with pytest.raises(PermissionsError):
        apply_visibility_filters(
            _plan("platform.billing.events", compaction=True),
            {key: FILTER},
            QueryTelemetry.detached(),
        )


def test_deny_all_compaction_is_refused():
    """`[]` is the deny-all and IS a match, so it must refuse rather than
    rewrite the relation as empty - the worst case this exists to prevent."""
    with pytest.raises(PermissionsError):
        apply_visibility_filters(
            _plan("platform.billing.events", compaction=True),
            {"platform.billing.*": []},
            QueryTelemetry.detached(),
        )


def test_unfiltered_compaction_is_untouched():
    """The exempt caller's path. `data_admin` lifts the filters at the front
    door, so a permitted compaction arrives here with nothing that matches and
    must proceed - no Filter inserted, nothing raised."""
    telemetry = QueryTelemetry.detached()
    plan = _plan("platform.billing.events", compaction=True)

    result = apply_visibility_filters(plan, {"public.security.*": FILTER}, telemetry)

    assert telemetry.visibility_filters_condition_added == 0
    assert not any(
        node.node_type == LogicalPlanStepType.Filter for _, node in result.nodes(True)
    )


def test_an_ordinary_read_is_still_filtered():
    """The refusal is scoped to compaction: row-level security on a SELECT is
    unchanged, or this 'fix' would be a data leak."""
    telemetry = QueryTelemetry.detached()
    plan = _plan("platform.billing.events", compaction=False)

    result = apply_visibility_filters(plan, {"platform.billing.*": FILTER}, telemetry)

    assert telemetry.visibility_filters_condition_added == 1
    assert any(
        node.node_type == LogicalPlanStepType.Filter for _, node in result.nodes(True)
    )


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__, "-v"])
