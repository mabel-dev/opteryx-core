"""
Redundant parentheses must not change how a predicate is planned.

`((p))` means `(p)`, and generated SQL adds redundant pairs freely. The logical
planner used to build one NESTED wrapper per pair, and boolean simplification's NOT
rules look through exactly one, so `NOT ((x = ANY(arr)))` skipped the inversion
that `NOT (x = ANY(arr))` takes. The two then planned differently: at the time,
one answered and the other was refused (see the ALLOPNOTEQ register entry in
tests/fuzzing/single_table_known_gaps.py). The multiset TLP oracle surfaced it.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import pytest

import opteryx

TABLE = "testdata.fuzzing.mixed"


def _outcome(where: str):
    """The sorted rows, or the exception class and message — whichever happened.

    Comparing outcomes rather than asserting an answer keeps the test about
    parenthesisation: it stays valid when ALLOPNOTEQ gains a kernel.
    """
    sql = f"SELECT row_id FROM {TABLE} WHERE {where}"
    try:
        rows = []
        for morsel in opteryx.session().execute_to_morsels(sql):
            rows.extend(morsel[i] for i in range(len(morsel)))
        return sorted(rows)
    except Exception as error:  # noqa: BLE001 - the outcome under test, compared below
        return (type(error).__name__, str(error))


@pytest.mark.parametrize(
    "predicate",
    [
        '-359066 = ANY("arr_int")',  # NOT => inversion to ALLOPNOTEQ
        "i_null > 3",  # NOT => inversion to <=
        "i_null > 3 OR i_group = 2",  # NOT => De Morgan
        "NOT (i_null > 3)",  # NOT => double negation
    ],
)
def test_stacked_parentheses_plan_like_one_pair(predicate):
    reference = _outcome(f"NOT ({predicate})")
    for depth in (2, 3):
        wrapped = "(" * depth + predicate + ")" * depth
        assert _outcome(f"NOT {wrapped}") == reference, wrapped


def test_alias_survives_stacked_parentheses():
    morsel = next(iter(opteryx.session().execute_to_morsels(
        "SELECT ((id + 1)) AS x FROM testdata.planets"
    )))
    assert morsel.column_names == [b"x"]


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
