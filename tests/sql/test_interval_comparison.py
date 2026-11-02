"""
INTERVAL comparison (ruled 2026-11-02): an interval orders by its normalised length,
1 month = 30 days — Postgres's rule, and the one draken/core/interval_slot.h documents.
So `INTERVAL '1' MONTH = INTERVAL '30' DAY` is TRUE and a year (360 days) is shorter
than 364 days. Before the native kernel every INTERVAL comparison died with a raw
AttributeError in the Python evaluator.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pytest

import opteryx


def _column(sql):
    session = opteryx.session()
    return [v for m in session.execute_to_morsels(sql) for v in m.column(m.column_names[0]).to_pylist()]


@pytest.mark.parametrize(
    "expression,expected",
    [
        ("INTERVAL '7' DAY < INTERVAL '8' DAY", True),
        ("INTERVAL '7' DAY = INTERVAL '7' DAY", True),
        ("INTERVAL '7' DAY != INTERVAL '7' DAY", False),
        ("INTERVAL '1' MONTH = INTERVAL '30' DAY", True),
        ("INTERVAL '1' MONTH < INTERVAL '31' DAY", True),
        ("INTERVAL '1' YEAR > INTERVAL '364' DAY", False),
        ("INTERVAL '1' YEAR = INTERVAL '360' DAY", True),
        ("INTERVAL '25' HOUR > INTERVAL '1' DAY", True),
        ("INTERVAL '24' HOUR = INTERVAL '1' DAY", True),
        ("CAST(NULL AS INTERVAL) < INTERVAL '1' DAY", None),
    ],
)
def test_literal_interval_comparison(expression, expected):
    assert _column(f"SELECT {expression} AS x") == [expected]


def test_interval_column_comparison_and_filter():
    # i = (id - 1) days, as a column
    source = (
        "(SELECT id, CAST(CONCAT('2026-01-0', CAST(id AS VARCHAR)) AS TIMESTAMP) "
        "- CAST('2026-01-01' AS TIMESTAMP) AS i FROM $planets) AS t"
    )
    assert _column(f"SELECT i > INTERVAL '4' DAY AS x FROM {source} ORDER BY id") == [False] * 5 + [True] * 4
    assert _column(f"SELECT COUNT(*) AS x FROM {source} WHERE i < INTERVAL '1' MONTH") == [9]
    assert _column(f"SELECT COUNT(*) AS x FROM {source} WHERE i = INTERVAL '96' HOUR") == [1]


@pytest.mark.parametrize(
    "expression",
    ["INTERVAL '1' DAY - current_timestamp", "INTERVAL '1' DAY - CAST('2026-01-10' AS DATE)"],
)
def test_interval_minus_an_instant_is_refused(expression):
    """An interval minus an instant means nothing; it used to answer instant - interval."""
    from opteryx.exceptions import IncorrectTypeError

    with pytest.raises(IncorrectTypeError):
        _column(f"SELECT {expression} AS x")


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-q"]))
