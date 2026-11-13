"""
CASE refuses branches of different temporal types, as IIF and COALESCE do.

DATE, TIME and TIMESTAMP were one CASE blend family, but nothing aligned a mixed
pair: the result resolved to a string type and the literal branches were stringified
from their native values, so `CASE WHEN c THEN <timestamp> ELSE CURRENT_DATE END`
answered `'2209248000000000'` and `'20770'` (microseconds and days, as text). Found
while checking the single-table fuzzer's ledger, 2026-11-13. Each temporal category
now blends only with itself.
"""

import datetime
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pytest

import opteryx
from opteryx.exceptions import IncompatibleTypesError


def _values(sql):
    return [m[i][0] for m in opteryx.session().execute_to_morsels(sql) for i in range(len(m))]


@pytest.mark.parametrize(
    "then,otherwise",
    [
        ("'2040-01-04'::TIMESTAMP", "CURRENT_DATE"),
        ("'2040-01-04'::TIMESTAMP", "'2000-01-01'::DATE"),
        ("TRUNC('2040-01-04'::DATE, 'month')", "'2000-01-01'::DATE"),
        ("CURRENT_TIME", "CURRENT_DATE"),
        ("CURRENT_TIMESTAMP", "CURRENT_TIME"),
    ],
)
def test_mixed_temporal_branches_are_refused(then, otherwise):
    for a, b in ((then, otherwise), (otherwise, then)):
        with pytest.raises(IncompatibleTypesError, match="CASE"):
            _values(f"SELECT CASE WHEN id > 1 THEN {a} ELSE {b} END FROM $planets")


def test_case_agrees_with_iif_and_coalesce_on_the_mix():
    for sql in (
        "SELECT IIF(id > 1, '2040-01-04'::TIMESTAMP, CURRENT_DATE) FROM $planets",
        "SELECT COALESCE(NULL, CURRENT_DATE, '2040-01-04'::TIMESTAMP) FROM $planets",
    ):
        with pytest.raises(IncompatibleTypesError):
            _values(sql)


def test_same_temporal_type_still_blends():
    assert _values(
        "SELECT CASE WHEN id > 1 THEN '2040-01-04'::DATE ELSE '2000-01-01'::DATE END "
        "FROM $planets WHERE id < 3 ORDER BY id"
    ) == [datetime.date(2000, 1, 1), datetime.date(2040, 1, 4)]
    stamps = _values(
        "SELECT CASE WHEN id > 1 THEN TRUNC('2040-01-04'::DATE, 'month') "
        "ELSE '2000-01-01'::TIMESTAMP END FROM $planets WHERE id < 3 ORDER BY id"
    )
    assert [(s.year, s.month, s.day) for s in stamps] == [(2000, 1, 1), (2040, 1, 1)]


def test_a_cast_aligns_the_mix():
    stamps = _values(
        "SELECT CASE WHEN id > 1 THEN '2040-01-04'::TIMESTAMP "
        "ELSE CAST('2000-01-01'::DATE AS TIMESTAMP) END FROM $planets WHERE id < 3 ORDER BY id"
    )
    assert [(s.year, s.month, s.day) for s in stamps] == [(2000, 1, 1), (2040, 1, 4)]


def test_a_null_branch_beside_a_temporal_one():
    assert _values(
        "SELECT CASE WHEN id > 1 THEN '2040-01-04'::DATE ELSE NULL END "
        "FROM $planets WHERE id < 3 ORDER BY id"
    ) == [None, datetime.date(2040, 1, 4)]
