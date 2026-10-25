"""GENERATE_SERIES over DATE / TIMESTAMP bounds with an INTERVAL step.

Aligned to DuckDB 1.5.4 (the expected values below were taken from it):

  * the SCALAR spelling returns an ARRAY<TIMESTAMP>; a DATE bound is midnight of
    its day and the elements are TIMESTAMPs either way
  * `stop` is included when the cursor lands on it
  * a step pointing away from `stop` is an EMPTY series, in the scalar AND the
    table spelling
  * a month step clamps the day and carries the clamp forward (31 Jan, 28 Feb,
    28 Mar) - the cursor is stepped, not re-anchored on `start`
  * the TABLE spelling with no alias names its relation and column
    `generate_series`

Deliberate divergences from DuckDB, pinned here so they cannot drift unnoticed:

  * a ZERO step is REFUSED (DuckDB answers an empty series)
  * GENERATE_SERIES(n) starts at 1 (DuckDB starts at 0)

Run as a script (CLAUDE.md §10) or under pytest.
"""

import datetime
import os
import sys

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import opteryx

def _dt(*parts):
    return datetime.datetime(*parts)


def _rows(sql):
    session = opteryx.session()
    out = []
    for morsel in session.execute_to_morsels(sql):
        if morsel is None:
            continue
        out.append(morsel.to_arrow().to_pydict())
    return out


def _scalar(sql):
    """The single cell of a one-row, one-column result."""
    (columns,) = _rows(sql)
    ((values,),) = [list(columns.values())]
    assert len(values) == 1
    return values[0]


def _column(sql):
    """Every value of a one-column result."""
    values = []
    for columns in _rows(sql):
        ((column,),) = [list(columns.values())]
        values.extend(column)
    return values


def _ts(text):
    return f"CAST('{text}' AS TIMESTAMP)"


def _date(text):
    return f"CAST('{text}' AS DATE)"


# ---------------------------------------------------------------------------
# Scalar spelling
# ---------------------------------------------------------------------------
def test_scalar_timestamp_fixed_step_includes_stop():
    got = _scalar(
        f"SELECT GENERATE_SERIES({_ts('2025-01-01')}, {_ts('2025-01-01 00:20')}, "
        "INTERVAL '5' MINUTE)"
    )
    assert got == [_dt(2025, 1, 1, 0, m) for m in (0, 5, 10, 15, 20)]


def test_scalar_stop_off_the_step_boundary_is_not_included():
    got = _scalar(
        f"SELECT GENERATE_SERIES({_ts('2025-01-01')}, {_ts('2025-01-01 00:07')}, "
        "INTERVAL '5' MINUTE)"
    )
    assert got == [_dt(2025, 1, 1, 0, 0), _dt(2025, 1, 1, 0, 5)]


def test_scalar_month_step_clamps_then_carries_the_clamp_forward():
    expected = [
        _dt(2025, 1, 31),
        _dt(2025, 2, 28),
        _dt(2025, 3, 28),
        _dt(2025, 4, 28),
        _dt(2025, 5, 28),
    ]
    got = _scalar(
        f"SELECT GENERATE_SERIES({_ts('2025-01-31')}, {_ts('2025-05-31')}, INTERVAL '1' MONTH)"
    )
    assert got == expected


def test_scalar_date_bounds_give_timestamps():
    got = _scalar(
        f"SELECT GENERATE_SERIES({_date('2025-01-31')}, {_date('2025-05-31')}, "
        "INTERVAL '1' MONTH)"
    )
    assert got == [
        _dt(2025, 1, 31),
        _dt(2025, 2, 28),
        _dt(2025, 3, 28),
        _dt(2025, 4, 28),
        _dt(2025, 5, 28),
    ]


def test_scalar_descending_series_with_negative_step():
    got = _scalar(
        f"SELECT GENERATE_SERIES({_ts('2025-01-03')}, {_ts('2025-01-01')}, INTERVAL '-1' DAY)"
    )
    assert got == [_dt(2025, 1, 3), _dt(2025, 1, 2), _dt(2025, 1, 1)]


@pytest.mark.parametrize(
    "start, stop, step",
    [
        ("2025-01-03", "2025-01-01", "INTERVAL '1' DAY"),  # counts up, bounds descend
        ("2025-01-01", "2025-01-03", "INTERVAL '-1' DAY"),  # counts down, bounds ascend
    ],
)
def test_scalar_step_pointing_away_from_stop_is_empty(start, stop, step):
    assert _scalar(f"SELECT GENERATE_SERIES({_ts(start)}, {_ts(stop)}, {step})") == []


def test_scalar_equal_bounds_is_the_one_element_series():
    got = _scalar(
        f"SELECT GENERATE_SERIES({_ts('2025-01-03')}, {_ts('2025-01-03')}, INTERVAL '1' DAY)"
    )
    assert got == [_dt(2025, 1, 3)]


def test_scalar_zero_step_is_refused():
    # DuckDB answers an empty series here; we refuse, because a zero step is a
    # mistake an empty result would hide. A ruled divergence.
    with pytest.raises(Exception, match="step must not be zero"):
        _scalar(
            f"SELECT GENERATE_SERIES({_ts('2025-01-01')}, {_ts('2025-01-03')}, "
            "INTERVAL '0' DAY)"
        )


def test_scalar_oversized_series_is_refused_by_name():
    with pytest.raises(Exception, match="more than 1000000 elements"):
        _scalar(
            f"SELECT GENERATE_SERIES({_ts('2025-01-01')}, {_ts('2026-01-01')}, "
            "INTERVAL '1' SECOND)"
        )


def test_scalar_a_column_bound_is_refused():
    with pytest.raises(Exception, match="constant"):
        _scalar(
            "SELECT GENERATE_SERIES(Lauched_at, Lauched_at, INTERVAL '1' DAY) "
            "FROM testdata.missions LIMIT 1"
        )


def test_scalar_integer_form_is_unchanged():
    assert _scalar("SELECT GENERATE_SERIES(3)") == [1, 2, 3]  # starts at 1: ruled divergence
    assert _scalar("SELECT GENERATE_SERIES(1, 5, 2)") == [1, 3, 5]
    assert _scalar("SELECT GENERATE_SERIES(5, 1)") == []
    assert _scalar("SELECT GENERATE_SERIES(CAST(NULL AS INTEGER), 3)") is None


# ---------------------------------------------------------------------------
# Table spelling
# ---------------------------------------------------------------------------
def test_table_timestamp_rows():
    got = _column(
        f"SELECT * FROM GENERATE_SERIES({_ts('2025-01-01')}, {_ts('2025-01-01 00:20')}, "
        "INTERVAL '5' MINUTE) AS g"
    )
    assert got == [_dt(2025, 1, 1, 0, m) for m in (0, 5, 10, 15, 20)]


def test_table_step_pointing_away_from_stop_is_empty():
    got = _column(
        f"SELECT * FROM GENERATE_SERIES({_ts('2025-01-03')}, {_ts('2025-01-01')}, "
        "INTERVAL '1' DAY) AS g"
    )
    assert got == []


def test_table_equal_bounds_with_a_backwards_step_is_the_one_element_series():
    got = _column(
        f"SELECT * FROM GENERATE_SERIES({_ts('2025-01-03')}, {_ts('2025-01-03')}, "
        "INTERVAL '-1' DAY) AS g"
    )
    assert got == [_dt(2025, 1, 3)]


def test_table_zero_step_is_refused():
    with pytest.raises(Exception, match="INTERVAL of zero"):
        _column(
            f"SELECT * FROM GENERATE_SERIES({_ts('2025-01-01')}, {_ts('2025-01-03')}, "
            "INTERVAL '0' DAY) AS g"
        )


def test_table_month_step_matches_the_scalar_spelling():
    bounds = f"{_ts('2025-01-31')}, {_ts('2025-05-31')}, INTERVAL '1' MONTH"
    assert _column(f"SELECT * FROM GENERATE_SERIES({bounds}) AS g") == _scalar(
        f"SELECT GENERATE_SERIES({bounds})"
    )


def test_table_without_an_alias_names_the_column_generate_series():
    assert _column("SELECT generate_series FROM GENERATE_SERIES(1, 3)") == [1, 2, 3]
    assert _column("SELECT * FROM GENERATE_SERIES(1, 3)") == [1, 2, 3]


def test_table_with_an_alias_still_names_the_column_by_the_alias():
    assert _column("SELECT gx FROM GENERATE_SERIES(1, 3) AS gx") == [1, 2, 3]


def test_table_temporal_series_can_be_filtered_and_joined_as_a_dense_axis():
    got = _column(
        f"SELECT g FROM GENERATE_SERIES({_ts('2025-01-01')}, {_ts('2025-01-05')}, "
        f"INTERVAL '1' DAY) AS g WHERE g > {_ts('2025-01-03')}"
    )
    assert got == [_dt(2025, 1, 4), _dt(2025, 1, 5)]


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
