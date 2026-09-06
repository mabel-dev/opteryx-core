"""FULL OUTER JOIN's NULL pad must carry the padded column's own type.

The tail pipeline that emits unmatched BUILD rows null-pads the PROBE half
(``UnmatchedBuildSource``), and it types that pad from the plan, not from data —
there is no probe morsel left to learn a type from. The compiler's type maps
knew every ordinary column but not an AGGREGATE's output: an aggregate node
carries no ``columns``, so its outputs were registered nowhere and
``_payload_types`` fell back to VARCHAR.

The pad was then VARCHAR while the matched batches were INT64, and the two only
met at the very end, as an opaque ``concat: all inputs must share one type``
raised from result assembly - a place with no causal connection to the join that
made it. Four conditions had to coincide (FULL OUTER, aggregate subquery on the
LEFT/probe side, an aggregate OUTPUT column projected, and projected at all),
which is why it survived: it is invisible from the aggregate side and invisible
from the join side.
"""

import os
import sys

import pytest

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "..", ".."))

import opteryx  # noqa: E402


def _rows(sql):
    return [
        tuple(morsel[i])
        for morsel in opteryx.session().execute_to_morsels(sql)
        for i in range(morsel.num_rows)
    ]


# `id * 2` gives keys 2,4,...,18 against $planets.id 1..9, so the join splits
# cleanly into all three outcomes and every one of them is checkable by hand:
# 4 matched (2,4,6,8), 5 build-only (1,3,5,7,9), 5 probe-only (10..18).
_FULL_OUTER = """
SELECT p.id AS pid, s.k AS k, s.c AS c
  FROM (SELECT id * 2 AS k, COUNT(*) AS c FROM $planets GROUP BY id * 2) AS s
  FULL OUTER JOIN $planets AS p ON p.id = s.k
"""


def test_aggregate_output_survives_a_full_outer_null_pad():
    rows = _rows(_FULL_OUTER)
    matched = sorted(r for r in rows if r[0] is not None and r[1] is not None)
    build_only = sorted(r[0] for r in rows if r[1] is None)
    probe_only = sorted(r[1] for r in rows if r[0] is None)

    assert matched == [(2, 2, 1), (4, 4, 1), (6, 6, 1), (8, 8, 1)]
    # The rows carrying the NULL pad. Before the fix the pad was VARCHAR and the
    # query never got this far.
    assert build_only == [1, 3, 5, 7, 9]
    assert all(r[2] is None for r in rows if r[1] is None)
    assert probe_only == [10, 12, 14, 16, 18]
    assert len(rows) == 14


def test_the_pad_is_null_of_the_right_type_not_a_string():
    """The defect's signature: the padded aggregate column arriving as VARCHAR.

    Asserted through arithmetic rather than a type API - a VARCHAR-typed NULL
    column cannot be summed, so this fails loudly if the pad ever regresses to a
    string, and it also proves the MATCHED values are still real numbers.
    """
    rows = _rows(
        """SELECT SUM(s.c) AS total, COUNT(*) AS n
             FROM (SELECT id * 2 AS k, COUNT(*) AS c FROM $planets GROUP BY id * 2) AS s
             FULL OUTER JOIN $planets AS p ON p.id = s.k"""
    )
    # 9 groups of 1, every one of them emitted (4 matched + 5 probe-only); the 5
    # build-only rows contribute a NULL each and SUM ignores them.
    assert rows == [(9, 14)]


@pytest.mark.parametrize(
    "aggregate", ["COUNT(*)", "SUM(id)", "MIN(id)", "MAX(id)", "AVG(id)"]
)
def test_every_aggregate_output_is_plan_typed(aggregate):
    """The type came from one place for all of them, so one being wrong meant all
    were - `_parse_aggregates` is where they are registered."""
    rows = _rows(
        f"""SELECT p.id AS pid, s.c AS c
              FROM (SELECT id * 2 AS k, {aggregate} AS c FROM $planets GROUP BY id * 2) AS s
              FULL OUTER JOIN $planets AS p ON p.id = s.k"""
    )
    assert len(rows) == 14
    assert all(r[1] is None for r in rows if r[0] in (1, 3, 5, 7, 9))


def test_ungrouped_aggregate_output_is_plan_typed():
    """An UngroupedAggregateNode carries no `columns` either, and read its types
    through the same helper - so it had the same hole."""
    rows = _rows(
        """SELECT p.id AS pid, s.c AS c
             FROM (SELECT MAX(id) AS k, COUNT(*) AS c FROM $planets) AS s
             FULL OUTER JOIN $planets AS p ON p.id = s.k"""
    )
    # One source row (key 9) matches planet 9; the other 8 planets are build-only.
    assert sorted(rows) == [
        (1, None), (2, None), (3, None), (4, None), (5, None),
        (6, None), (7, None), (8, None), (9, 9),
    ]


def test_a_case_over_the_padded_column_still_binds():
    """How the defect usually surfaced first: a CASE mixing the padded column with
    anything else reports a branch-type mismatch before concat is ever reached."""
    rows = _rows(
        """SELECT CASE WHEN p.id IS NULL THEN -1 ELSE s.c END AS x
             FROM (SELECT id * 2 AS k, COUNT(*) AS c FROM $planets GROUP BY id * 2) AS s
             FULL OUTER JOIN $planets AS p ON p.id = s.k"""
    )
    values = [r[0] for r in rows]
    # probe-only rows take the THEN (-1); matched rows take s.c (1); build-only
    # rows take the ELSE and read the NULL pad.
    assert sorted(v for v in values if v is not None) == [-1] * 5 + [1] * 4
    assert values.count(None) == 5


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
