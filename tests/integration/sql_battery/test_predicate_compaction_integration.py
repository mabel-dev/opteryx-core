import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../../orso"))
sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import opteryx


def _plan_text(result):
    return result.telemetry.get("executed_plan", "") or ""


def test_predicate_compaction_compacts_before_join_scan():
    sql = (
        "SELECT p.name FROM $planets AS p "
        "INNER JOIN testdata.satellites AS s ON p.id = s.planetId "
        "WHERE p.id > 1 AND p.id > 4"
    )

    result = opteryx.query(sql)
    plan = _plan_text(result)

    assert "FILTER (id > 4)" in plan
    assert "id > 1" not in plan.replace("id > 4", "")
    assert result.telemetry.get("optimization_predicate_compaction", 0) >= 1

    baseline = opteryx.query(
        "SELECT p.name FROM $planets AS p INNER JOIN testdata.satellites AS s ON p.id = s.planetId WHERE p.id > 4"
    )
    assert result.rowcount == baseline.rowcount


def test_predicate_compaction_in_nested_subquery():
    sql = """
        SELECT COUNT(*)
        FROM (
            SELECT id FROM $planets WHERE id > 1 AND id > 4
        ) AS sub
    """

    result = opteryx.query(sql)
    plan = _plan_text(result)

    assert "FILTER (id > 4)" in plan
    assert "id > 1" not in plan.replace("id > 4", "")
    assert result.telemetry.get("optimization_predicate_compaction", 0) >= 1
    assert result.fetchall() == [(5,)]


def test_date_trunc_filter_rewrite_and_equivalence():
    # DATE_TRUNC('year', Lauched_at) = '1970-01-01' should be rewritten to
    # Lauched_at >= '1970-01-01' AND Lauched_at < '1971-01-01'
    sql_trunc = "SELECT * FROM testdata.missions WHERE DATE_TRUNC('year', Lauched_at) = '1970-01-01'"
    sql_range = "SELECT * FROM testdata.missions WHERE Lauched_at >= '1970-01-01' AND Lauched_at < '1971-01-01'"

    res_trunc = opteryx.query(sql_trunc)
    plan_trunc = _plan_text(res_trunc)

    # ensure rewrite produced equivalent range predicates in the executed plan
    assert "Lauched_at >=" in plan_trunc
    assert "Lauched_at <" in plan_trunc

    # results should be identical
    res_range = opteryx.query(sql_range)
    assert res_trunc.rowcount == res_range.rowcount
    assert res_trunc.fetchall() == res_range.fetchall()