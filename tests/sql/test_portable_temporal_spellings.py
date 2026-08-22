"""End-to-end regression tests for the two portable temporal spellings that
every other SQL engine has and Opteryx refused until now.

Both are normalised while the LOGICAL PLAN IS BUILT
(opteryx/planner/logical_planner/logical_planner_builders.py), not in the
optimizer, so they reach the same native kernels — and the same pushdown
treatment — as the spellings they normalise to:

  * EXTRACT(EPOCH FROM x)  ->  UNIXTIME(x)     (aka TO_UNIXTIME)
    `epoch` is not one of the part ids draken_date_part implements, so an
    EXTRACT node carrying it was refused as "outside the c-native kernel set".
    UNIXTIME is the same value (whole epoch SECONDS) and is native.
    Both EXTRACT spellings normalise: the `EXTRACT(part FROM x)` syntax and the
    ordinary call `EXTRACT('epoch', x)`.

  * DATE_TRUNC(unit, value)  ->  TRUNC(value, unit)
    Argument order is the ONLY difference. The unit comes FIRST
    (Postgres/Snowflake/DuckDB/Redshift/Spark); it is not sniffed from the
    arguments, so BigQuery's value-first spelling is named and refused rather
    than guessed at.

The tests are parity tests against the spelling that already worked — that is
the point of the change, and it is the only oracle that cannot drift from the
engine's own semantics as kernels change.

Run as a script (CLAUDE.md §10) or under pytest.
"""

import os
import sys

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "..", ".."))

import pytest

import opteryx
from opteryx.exceptions import UnsupportedSyntaxError

_SESSION = opteryx.session()

# TIMESTAMP64 column, DATE32 column, and the units TRUNC accepts.
_TIMESTAMP_SOURCE = ("testdata.missions", "Lauched_at")
_DATE_SOURCE = ("testdata.astronauts", "birth_date")
_UNITS = ("second", "minute", "hour", "day", "week", "month", "quarter", "year")


def _col(sql, name="r"):
    out = []
    for morsel in _SESSION.execute_to_morsels(sql):
        out.extend(morsel.column(name.encode()).to_pylist())
    return out


# ---------------------------------------------------------------------------
# EXTRACT(EPOCH FROM ...)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("table,column", [_TIMESTAMP_SOURCE, _DATE_SOURCE])
def test_extract_epoch_matches_to_unixtime(table, column):
    """EXTRACT(EPOCH FROM x) is TO_UNIXTIME(x), value for value, over both a
    TIMESTAMP64 column and a DATE32 column."""
    extracted = _col(f"SELECT EXTRACT(EPOCH FROM {column}) AS r FROM {table}")
    unixtime = _col(f"SELECT TO_UNIXTIME({column}) AS r FROM {table}")
    assert extracted == unixtime
    # Not vacuous: the columns carry real values, not all-NULL.
    assert any(v is not None for v in extracted)


@pytest.mark.parametrize("table,column", [_TIMESTAMP_SOURCE, _DATE_SOURCE])
def test_extract_epoch_call_spelling_matches(table, column):
    """EXTRACT is a registered function, so `EXTRACT('epoch', x)` is reachable
    as an ordinary call that never passes through the EXTRACT syntax builder.
    Both spellings must normalise, or they disagree about what EXTRACT accepts."""
    syntax = _col(f"SELECT EXTRACT(EPOCH FROM {column}) AS r FROM {table}")
    call = _col(f"SELECT EXTRACT('epoch', {column}) AS r FROM {table}")
    assert syntax == call


def test_extract_epoch_is_case_insensitive():
    table, column = _TIMESTAMP_SOURCE
    reference = _col(f"SELECT TO_UNIXTIME({column}) AS r FROM {table}")
    for spelling in ("EPOCH", "epoch", "Epoch"):
        assert _col(f"SELECT EXTRACT({spelling} FROM {column}) AS r FROM {table}") == reference


def test_extract_epoch_returns_an_integer():
    """UNIXTIME is whole epoch SECONDS. A FLOAT result here would mean the
    rewrite landed somewhere other than UNIXTIME."""
    table, column = _TIMESTAMP_SOURCE
    values = [v for v in _col(f"SELECT EXTRACT(EPOCH FROM {column}) AS r FROM {table}") if v is not None]
    assert values
    assert all(isinstance(v, int) and not isinstance(v, bool) for v in values)


def test_extract_epoch_nests_and_aggregates():
    """The reason this is normalised at plan-build time rather than in the
    optimizer's function rewriter: that pass never descends into a function's or
    an aggregate's parameters, nor into an ORDER BY expression that is not also
    projected. Each of these was refused before the change."""
    table, column = _TIMESTAMP_SOURCE
    assert _col(f"SELECT ABS(EXTRACT(EPOCH FROM {column})) AS r FROM {table}") == _col(
        f"SELECT ABS(TO_UNIXTIME({column})) AS r FROM {table}"
    )
    assert _col(f"SELECT MAX(EXTRACT(EPOCH FROM {column})) AS r FROM {table}") == _col(
        f"SELECT MAX(TO_UNIXTIME({column})) AS r FROM {table}"
    )
    assert _col(
        f"SELECT Mission AS r FROM {table} ORDER BY EXTRACT(EPOCH FROM {column}) DESC LIMIT 5"
    ) == _col(f"SELECT Mission AS r FROM {table} ORDER BY TO_UNIXTIME({column}) DESC LIMIT 5")


def test_extract_epoch_in_a_filter():
    table, column = _TIMESTAMP_SOURCE
    assert _col(
        f"SELECT COUNT(*) AS r FROM {table} WHERE EXTRACT(EPOCH FROM {column}) > 0"
    ) == _col(f"SELECT COUNT(*) AS r FROM {table} WHERE TO_UNIXTIME({column}) > 0")


def test_extract_other_parts_are_untouched():
    """The epoch arm must not shadow the parts draken_date_part does implement."""
    table, column = _TIMESTAMP_SOURCE
    for part in ("year", "quarter", "month", "day", "hour", "minute", "second"):
        values = _col(f"SELECT EXTRACT({part} FROM {column}) AS r FROM {table} LIMIT 5")
        assert len(values) == 5


# ---------------------------------------------------------------------------
# DATE_TRUNC(unit, value)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("unit", _UNITS)
def test_date_trunc_matches_trunc_over_a_timestamp(unit):
    table, column = _TIMESTAMP_SOURCE
    assert _col(f"SELECT DATE_TRUNC('{unit}', {column}) AS r FROM {table}") == _col(
        f"SELECT TRUNC({column}, '{unit}') AS r FROM {table}"
    )


@pytest.mark.parametrize("unit", _UNITS)
def test_date_trunc_matches_trunc_over_a_date(unit):
    """A DATE operand. A DATE *column* is a pre-existing TRUNC gap (the native
    lowering takes TIMESTAMP64 only), so the DATE case is exercised through a
    DATE literal and through an explicit cast — both spellings still have to
    agree with TRUNC, which is what this asserts."""
    table, column = _DATE_SOURCE
    assert _col(f"SELECT DATE_TRUNC('{unit}', '1969-07-20'::DATE) AS r FROM {table} LIMIT 1") == _col(
        f"SELECT TRUNC('1969-07-20'::DATE, '{unit}') AS r FROM {table} LIMIT 1"
    )
    assert _col(
        f"SELECT DATE_TRUNC('{unit}', CAST({column} AS TIMESTAMP)) AS r FROM {table}"
    ) == _col(f"SELECT TRUNC(CAST({column} AS TIMESTAMP), '{unit}') AS r FROM {table}")


def test_date_trunc_is_case_insensitive_and_accepts_datetrunc():
    table, column = _TIMESTAMP_SOURCE
    reference = _col(f"SELECT TRUNC({column}, 'day') AS r FROM {table}")
    for sql in (
        f"SELECT DATE_TRUNC('day', {column}) AS r FROM {table}",
        f"SELECT date_trunc('DAY', {column}) AS r FROM {table}",
        f"SELECT DATETRUNC('day', {column}) AS r FROM {table}",
    ):
        assert _col(sql) == reference


def test_date_trunc_groups_and_filters_like_trunc():
    """Normalising to TRUNC rather than registering a second function is what
    keeps the TRUNC-keyed optimizer arms (the comparison-to-range rewrite and
    the scan pruning guards) applying to DATE_TRUNC. Same answers, and the same
    plan shape, is the observable half of that."""
    table, column = _TIMESTAMP_SOURCE
    assert _col(
        f"SELECT COUNT(*) AS r FROM {table} GROUP BY DATE_TRUNC('year', {column}) ORDER BY 1"
    ) == _col(f"SELECT COUNT(*) AS r FROM {table} GROUP BY TRUNC({column}, 'year') ORDER BY 1")
    assert _col(
        f"SELECT COUNT(*) AS r FROM {table} "
        f"WHERE DATE_TRUNC('year', {column}) = '2020-01-01'::TIMESTAMP"
    ) == _col(
        f"SELECT COUNT(*) AS r FROM {table} "
        f"WHERE TRUNC({column}, 'year') = '2020-01-01'::TIMESTAMP"
    )


def test_date_trunc_refuses_the_value_first_spelling():
    """BigQuery spells it DATE_TRUNC(value, unit). Sniffing which argument is a
    literal would make argument order depend on the shape of the call, so the
    swapped form is named and refused."""
    table, column = _TIMESTAMP_SOURCE
    with pytest.raises(UnsupportedSyntaxError) as caught:
        _col(f"SELECT DATE_TRUNC({column}, 'day') AS r FROM {table}")
    assert "unit FIRST" in str(caught.value)
    assert "TRUNC(value, unit)" in str(caught.value)


@pytest.mark.parametrize("call", ["DATE_TRUNC(Lauched_at)", "DATE_TRUNC('day', Lauched_at, 1)"])
def test_date_trunc_requires_two_arguments(call):
    """Falling through to TRUNC with the wrong arity would reach TRUNC's NUMERIC
    overload, which is a different function entirely."""
    with pytest.raises(UnsupportedSyntaxError) as caught:
        _col(f"SELECT {call} AS r FROM testdata.missions")
    assert "exactly two arguments" in str(caught.value)


if __name__ == "__main__":  # pragma: no cover
    sys.exit(pytest.main([__file__, "-q"]))
