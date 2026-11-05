"""Join disjunct tagging: an OR above an INNER join whose branches each read one
leg is evaluated on the legs and the join carries bools.

The rewrite must be row-for-row identical to the untagged plan (the kill switch
is the oracle), including three-valued NULL behaviour, and must leave a
straddling branch alone.
"""

import os
import sys

sys.path.insert(0, os.path.join(sys.path[0], "..", ".."))

import pytest

import opteryx
from opteryx import config

# planets (9) ⋈ satellites (177): ~20x fan-out, so the gate passes on both legs.
JOIN = "testdata.planets p INNER JOIN testdata.satellites s ON p.id = s.planetId"

PREDICATES = [
    # one branch per leg
    "p.name LIKE '%a%' OR s.name LIKE '%o%'",
    # two branches on one leg, one on the other: one Project per leg
    "p.name LIKE '%a%' OR s.name LIKE '%o%' OR s.radius > 1000",
    # a single-leg AND is one tag
    "(p.name LIKE '%a%' AND p.id > 4) OR s.gm < 1",
    # NULL in a branch: albedo has NULLs; NULL OR TRUE keeps, NULL OR FALSE drops
    "s.albedo > 0.5 OR p.name = 'Jupiter'",
    # a straddling branch stays, the single-leg one is still tagged
    "p.id = s.id OR s.name LIKE '%o%'",
    # TPC-H Q19 shape: OR of straddling ANDs — nothing is tagged, answer unchanged
    "(p.name LIKE '%a%' AND s.gm > 1) OR (p.id > 4 AND s.radius > 100)",
    # NOT over a straddling OR — not an OR root, nothing is tagged
    "NOT (p.name LIKE '%a%' OR s.name LIKE '%o%')",
]


def _rows(sql):
    session = opteryx.session()
    rows = []
    for morsel in session.execute_to_morsels(sql):
        if morsel is None or morsel.num_rows == 0:
            continue
        for record in morsel.to_arrow().to_pylist():
            # repr, not the tuple: a NULL float comes back as nan, and nan != nan
            rows.append(repr(tuple(record.values())))
    return sorted(rows)


def _explain(sql):
    session = opteryx.session()
    lines = []
    for morsel in session.execute_to_morsels(f"EXPLAIN {sql}"):
        for record in morsel.to_arrow().to_pylist():
            lines.append(
                " ".join(
                    v.decode() if type(v) is bytes else str(v)
                    for v in record.values()
                    if v is not None
                )
            )
    return "\n".join(lines)


@pytest.fixture
def untagged():
    config.features.disable_join_disjunct_tagging = True
    try:
        yield
    finally:
        config.features.disable_join_disjunct_tagging = False


@pytest.mark.parametrize("predicate", PREDICATES)
def test_tagged_matches_untagged_row_for_row(predicate, request):
    sql = f"SELECT p.name, s.name, s.albedo FROM {JOIN} WHERE {predicate}"
    tagged = _rows(sql)
    request.getfixturevalue("untagged")
    oracle = _rows(sql)
    assert tagged == oracle, f"tagged {len(tagged)} rows, untagged {len(oracle)}"
    assert 0 < len(tagged) < 177, "the predicate must filter without emptying the join"


def test_tags_are_projected_on_the_legs():
    sql = f"SELECT p.name FROM {JOIN} WHERE p.name LIKE '%a%' OR s.name LIKE '%o%'"
    plan = _explain(sql)
    assert "tagged 1 on left leg" in plan, plan
    assert "tagged 1 on right leg" in plan, plan
    # each branch is computed in a Projection on its leg, below the join. (The
    # Filter still RENDERS the branch text: a tag column is named by its expression.)
    projections = [line for line in plan.splitlines() if "Projection" in line and "INSTR" in line]
    assert len(projections) == 2, plan


def test_straddling_ands_are_not_peeled():
    """Measured 2026-11-05: peeling reached TPC-H Q7/Q19 and was ~1.5% slower."""
    sql = (
        f"SELECT p.name FROM {JOIN} "
        "WHERE (p.name LIKE '%a%' AND s.gm > 1) OR (p.id > 4 AND s.radius > 100)"
    )
    assert "join disjunct tagging" not in _explain(sql)


def test_straddling_branch_is_left_alone():
    sql = f"SELECT p.name FROM {JOIN} WHERE p.id = s.id OR s.name LIKE '%o%'"
    plan = _explain(sql)
    assert "tagged 1 on right leg" in plan, plan
    assert "on left leg" not in plan, plan


def test_kill_switch_removes_the_rewrite(untagged):
    sql = f"SELECT p.name FROM {JOIN} WHERE p.name LIKE '%a%' OR s.name LIKE '%o%'"
    assert "join disjunct tagging" not in _explain(sql)


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
