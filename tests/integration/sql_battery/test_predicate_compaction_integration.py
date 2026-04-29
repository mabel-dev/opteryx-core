import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../../orso"))
sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import opteryx
from tests.helpers import execute_and_get_arrow, execute_and_get_rowcount, execute_and_get_shape, execute_and_fetch_all


def _plan_text(result):
    return result.telemetry.get("executed_plan", "") or ""


def test_predicate_compaction_compacts_before_join_scan():
    sql = (
        "SELECT p.name FROM $planets AS p "
        "INNER JOIN testdata.satellites AS s ON p.id = s.planetId "
        "WHERE p.id > 1 AND p.id > 4"
    )

    pass  # migrated from query
    plan = _plan_text(result)

    assert "FILTER (id > 4)" in plan
    assert "id > 1" not in plan.replace("id > 4", "")
    assert result.telemetry.get("optimization_predicate_compaction", 0) >= 1

    baselinexecute_and_fetch_all(
        "SELECT p.name FROM $planets AS p INNER JOIN testdata.satellites AS s ON p.id = s.planetId WHERE p.id > 4"
    )
