import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import opteryx
from tests.helpers import execute_and_fetch_all


def _explain(sql: str):
    """Run EXPLAIN for `sql` and return (plan_text, telemetry).

    The plan is rendered as one "<node> | <details>" line per EXPLAIN row so it
    can be asserted against as text; telemetry is read from the same session so
    the optimization counters belong to this plan.
    """
    session = opteryx.session()
    rows = []
    for morsel in session.execute_to_morsels(f"EXPLAIN {sql}"):
        rows.extend(morsel.to_arrow().to_pylist())

    lines = []
    for row in rows:
        tree = row["tree"]
        if isinstance(tree, bytes):
            tree = tree.decode("utf-8")
        lines.append(f"{tree} | {row['details']}")

    return "\n".join(lines), session.telemetry


def test_predicate_compaction_compacts_before_join_scan():
    sql = (
        "SELECT p.name FROM $planets AS p "
        "INNER JOIN testdata.satellites AS s ON p.id = s.planetId "
        "WHERE p.id > 1 AND p.id > 4"
    )

    plan, telemetry = _explain(sql)

    # the two range predicates collapse to the tighter one, on the planets side
    # of the join, before the scan
    assert "Filter | id > 4" in plan, plan
    assert "id > 1" not in plan, plan
    assert telemetry.get("optimization_predicate_compaction", 0) >= 1, telemetry

    # compaction must not change the answer
    compacted = execute_and_fetch_all(sql)
    baseline = execute_and_fetch_all(
        "SELECT p.name FROM $planets AS p "
        "INNER JOIN testdata.satellites AS s ON p.id = s.planetId "
        "WHERE p.id > 4"
    )
    assert compacted == baseline, (compacted, baseline)


if __name__ == "__main__":  # pragma: no cover
    test_predicate_compaction_compacts_before_join_scan()
    print("✅ okay")
