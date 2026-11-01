"""
Silent wrong answers found by the reference (Opteryx vs DuckDB) fuzzer, 2026-11-01.

Each test asserts the RELATIONSHIP the answer must satisfy, not just a value:

* Expression reuse kept literal case. The binder reuses an already-computed
  expression by its rendered name, matched case-insensitively so `UPPER(Name)` reuses
  `upper(name)` — but the lower-casing reached string literals too: `name || 'x'` reused
  `name || 'X'`, and `('b') = ('B')` was TRUE.
* -0.0 folded at arithmetic output. The 2026-05-22 float ruling folds -0.0 to +0.0
  at ingestion AND arithmetic output; the binop dispatch kernels skipped it, so
  `(id - 5) * 0.0` grouped -0.0 apart from 0.0.
* Mixed INT/FLOAT comparisons use the ruled total order (NaN == NaN, NaN greatest):
  `id < NaN` was FALSE while `NaN > id` was TRUE.
* FLOAT -> DECIMAL casts range-check: 1e300 / Infinity saturated to
  9223372036854775.807, which does not fit DECIMAL(12,3).
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pytest

import opteryx


def _rows(sql):
    return [morsel[i] for morsel in opteryx.session().execute_to_morsels(sql) for i in range(len(morsel))]


def _scalar(sql):
    (row,) = _rows(sql)
    return row[0]


def test_case_distinct_literals_are_not_reused_for_each_other():
    assert _rows("SELECT ('b') = ('B') AS x") == [(False,)]
    assert _rows("SELECT name || 'x' AS a, name || 'X' AS b FROM $planets WHERE id = 1") == [("Mercuryx", "MercuryX")]
    # REPLACE of 'a' vs 'A' differs on the 4 planets with a lower-case 'a'.
    assert _scalar("SELECT COUNT(*) FROM $planets WHERE REPLACE(name, 'a', 'b') = REPLACE(name, 'A', 'b')") == 5


def test_identifier_case_still_reuses_an_expression():
    rows = _rows("SELECT UPPER(Name) AS u, COUNT(*) AS c FROM $planets GROUP BY upper(name)")
    assert len(rows) == 9 and all(c == 1 for _u, c in rows)
    assert len(_rows("SELECT name || 'x' AS a FROM $planets GROUP BY NAME || 'x'")) == 9


@pytest.mark.parametrize("expression", ["CAST(id - 5 AS FLOAT64) * 0.0", "(id - 5) * 0.0",
                                        "CAST(5 - id AS FLOAT64) / CAST('Infinity' AS FLOAT64)"])
def test_computed_negative_zero_groups_with_zero(expression):
    assert _scalar(f"SELECT COUNT(DISTINCT {expression}) FROM $planets") == 1
    assert _rows(f"SELECT COUNT(*) AS n FROM (SELECT {expression} AS z FROM $planets) AS s GROUP BY z") == [(9,)]


@pytest.mark.parametrize(
    "predicate,expected",
    [
        ("id < CAST('NaN' AS FLOAT64)", 9),
        ("CAST('NaN' AS FLOAT64) > id", 9),
        ("id <= CAST('NaN' AS FLOAT64)", 9),
        ("id > CAST('NaN' AS FLOAT64)", 0),
        ("id = CAST('NaN' AS FLOAT64)", 0),
        ("id != CAST('NaN' AS FLOAT64)", 9),
    ],
)
def test_integer_against_nan_follows_the_float_total_order(predicate, expected):
    assert _scalar(f"SELECT COUNT(*) FROM $planets WHERE {predicate}") == expected


@pytest.mark.parametrize("value", ["1e300", "Infinity", "-Infinity", "NaN"])
def test_out_of_range_float_to_decimal_raises_and_try_cast_is_null(value):
    column = f"CAST(diameter AS FLOAT64) * 0 + CAST('{value}' AS FLOAT64)"
    with pytest.raises(Exception, match="out of range for DECIMAL"):
        _rows(f"SELECT CAST({column} AS DECIMAL(12,3)) FROM $planets WHERE id = 1")
    assert _rows(f"SELECT TRY_CAST({column} AS DECIMAL(12,3)) FROM $planets WHERE id = 1") == [(None,)]


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
