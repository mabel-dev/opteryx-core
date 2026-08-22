"""GENERATE_SERIES has two spellings, and they must agree.

    SELECT GENERATE_SERIES(1, 3)              -> one row, the ARRAY [1, 2, 3]
    SELECT * FROM GENERATE_SERIES(1, 3) AS g  -> three rows, 1 / 2 / 3

The scalar spelling is new; the table spelling is not. The point of most of the
tests below is that one name cannot mean two different series, so the two are
compared to each other wherever both can express the same question.

WHY THE TEMPORAL FORM MATTERS

The table spelling over TIMESTAMPS with an INTERVAL step is how you build a dense
time axis, and a dense axis is the only way to ask when something STOPPED
happening: LEFT JOIN the real traffic onto every hour of the last day, and the
hours with no rows are the outage. Aggregation alone cannot surface an absence —
there is no row to aggregate.

That form was DEAD. `date_range` parsed its interval as a human-readable string
("3d5h19m") while the planner has always handed it the canonical
`(months, microseconds)` pair, so every temporal call died with a bare

    TypeError: expected string or bytes-like object, got 'tuple'

Nothing produced a string interval — the only caller of `date_range` is
GENERATE_SERIES — so the string contract had no live input at all. The
gap-filling test at the bottom of this file is the query that motivated the fix.

THE SCALAR FORM IS INTEGER-ONLY, ON PURPOSE
The table form also accepts floats, deciding whether the last element is included
by an accumulate-and-tolerance rule. A second implementation of a fuzzy boundary
is how two spellings of one name start disagreeing at the edges, so the scalar
form declares integer parameters and refuses a float at bind time. Pinned below,
so the narrowing is a decision rather than a gap someone later "fixes" by
guessing at the semantics.
"""

import datetime
import os
import sys

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import opteryx
from opteryx.exceptions import InvalidFunctionParameterError


def rows(sql):
    """Every row, in order, as tuples — row-for-row comparison, not a count."""
    session = opteryx.session()
    out = []
    for morsel in session.execute_to_morsels(sql):
        if morsel is None:
            continue
        table = morsel.to_arrow().to_pydict()
        out.extend(zip(*(table[name] for name in table)))
    return out


def values(sql):
    """The single column every test here selects, flattened."""
    return [row[0] for row in rows(sql)]


# ---------------------------------------------------------------- integers ---


@pytest.mark.parametrize(
    "call, expected",
    [
        # The ONE-argument form starts at 1: the lone argument is the END.
        ("GENERATE_SERIES(3)", [1, 2, 3]),
        ("GENERATE_SERIES(1)", [1]),
        ("GENERATE_SERIES(1, 3)", [1, 2, 3]),
        ("GENERATE_SERIES(0, 0)", [0]),
        ("GENERATE_SERIES(-2, 2)", [-2, -1, 0, 1, 2]),
        # `stop` is included only when it falls on a step boundary.
        ("GENERATE_SERIES(1, 10, 3)", [1, 4, 7, 10]),
        ("GENERATE_SERIES(1, 9, 3)", [1, 4, 7]),
        # Counting down needs a negative step.
        ("GENERATE_SERIES(5, 1, -1)", [5, 4, 3, 2, 1]),
        ("GENERATE_SERIES(10, 1, -3)", [10, 7, 4, 1]),
        # A step pointing away from `stop` is an EMPTY series, not an error —
        # the answer `range()` gives, and what a caller whose computed bounds
        # happen to cross expects.
        ("GENERATE_SERIES(5, 1)", []),
        ("GENERATE_SERIES(1, 5, -1)", []),
    ],
)
def test_scalar_integer_series(call, expected):
    assert values(f"SELECT {call} AS s") == [expected], call


@pytest.mark.parametrize(
    "call",
    [
        "GENERATE_SERIES(3)",
        "GENERATE_SERIES(1, 3)",
        "GENERATE_SERIES(1, 10, 3)",
        "GENERATE_SERIES(1, 9, 3)",
        "GENERATE_SERIES(5, 1, -1)",
        "GENERATE_SERIES(-2, 2)",
    ],
)
def test_scalar_and_table_spellings_agree(call):
    """One name, one series.

    The scalar form returns the series as an ARRAY in one row and the table form
    returns it as rows; element for element they must be identical, or the same
    query written two ways answers two things.
    """
    as_array = values(f"SELECT {call} AS s")[0]
    as_rows = values(f"SELECT * FROM {call} AS g")

    assert list(as_array) == as_rows, call


def test_scalar_series_repeats_per_row():
    """The arguments are constants, so every row gets the same series."""
    assert values("SELECT GENERATE_SERIES(1, 3) AS s FROM $planets") == [[1, 2, 3]] * 9


def test_scalar_series_is_subscriptable():
    """It is an ordinary ARRAY — zero-based, like every other array in the dialect."""
    assert values("SELECT GENERATE_SERIES(10, 30, 10)[0] AS s") == [10]
    assert values("SELECT GENERATE_SERIES(10, 30, 10)[2] AS s") == [30]
    assert values("SELECT GENERATE_SERIES(10, 30, 10)[-1] AS s") == [30]


# --------------------------------------------------------------- refusals ----


def test_zero_step_is_refused_not_hung():
    """A zero step describes a series with no end.

    Both spellings must refuse it. Left to run, the loop never terminates — the
    failure mode is a hung query, which is the hardest kind to diagnose.
    """
    with pytest.raises(Exception, match="zero"):
        values("SELECT GENERATE_SERIES(1, 3, 0) AS s")

    with pytest.raises(Exception, match="zero"):
        values("SELECT * FROM GENERATE_SERIES(1, 3, 0) AS g")


def test_oversized_series_is_refused_by_name():
    """A series is materialized in full, so an unbounded one is an OOM crash.

    Refusing at a stated limit — and naming the table spelling, which streams —
    is the honest answer. A segfault would report the wrong problem.
    """
    with pytest.raises(Exception, match="more than"):
        values("SELECT GENERATE_SERIES(1, 10000000000) AS s")


def test_column_argument_is_refused_at_compile_time():
    """A per-row series is a different computation, not this one relaxed.

    Each row's array would be a different length, driven by column values with no
    bound on them. The parameters are declared constant-only and the refusal
    names the offending argument rather than silently using the first row's value
    for every row.
    """
    with pytest.raises(InvalidFunctionParameterError, match="must be a constant"):
        values("SELECT GENERATE_SERIES(1, id) AS s FROM $planets")


def test_float_argument_is_refused():
    """The scalar form is integer-only — see this file's docstring for why."""
    with pytest.raises(Exception):
        values("SELECT GENERATE_SERIES(1.5, 3.5) AS s")


# --------------------------------------------------------------- temporal ----

_JAN_1 = "CAST('2020-01-01' AS TIMESTAMP)"
_JAN_3 = "CAST('2020-01-03' AS TIMESTAMP)"


def test_timestamp_series_by_day():
    """The form gap-filling needs. This raised a bare TypeError before the fix."""
    assert values(
        f"SELECT * FROM GENERATE_SERIES({_JAN_1}, {_JAN_3}, INTERVAL '1' DAY) AS g"
    ) == [
        datetime.datetime(2020, 1, 1),
        datetime.datetime(2020, 1, 2),
        datetime.datetime(2020, 1, 3),
    ]


def test_timestamp_series_by_hour():
    assert values(
        "SELECT * FROM GENERATE_SERIES("
        "CAST('2020-01-01 00:00' AS TIMESTAMP), "
        "CAST('2020-01-01 04:00' AS TIMESTAMP), INTERVAL '2' HOUR) AS g"
    ) == [
        datetime.datetime(2020, 1, 1, 0, 0),
        datetime.datetime(2020, 1, 1, 2, 0),
        datetime.datetime(2020, 1, 1, 4, 0),
    ]


def test_date_bounds_are_accepted():
    """A DATE start/end is a temporal series too, yielded as timestamps."""
    assert values(
        "SELECT * FROM GENERATE_SERIES("
        "CAST('2020-01-01' AS DATE), CAST('2020-01-03' AS DATE), INTERVAL '1' DAY) AS g"
    ) == [
        datetime.datetime(2020, 1, 1),
        datetime.datetime(2020, 1, 2),
        datetime.datetime(2020, 1, 3),
    ]


def test_month_steps_use_calendar_arithmetic():
    """A month is 28-31 days, so it is added by the calendar, not as a duration.

    Stepping a month from the 31st CLAMPS to the shorter month's last day and
    then continues from there — 2020-01-31 goes to 02-29, and the next step is
    from the 29th, not back to the 31st. That cumulative behaviour is DuckDB's
    (verified against it), not Postgres's, which re-anchors on the start date.
    Pinned because it is the kind of thing a later "tidy-up" silently changes.
    """
    assert values(
        "SELECT * FROM GENERATE_SERIES("
        "CAST('2020-01-31' AS TIMESTAMP), CAST('2020-04-30' AS TIMESTAMP), "
        "INTERVAL '1' MONTH) AS g"
    ) == [
        datetime.datetime(2020, 1, 31),
        datetime.datetime(2020, 2, 29),
        datetime.datetime(2020, 3, 29),
        datetime.datetime(2020, 4, 29),
    ]


def test_temporal_interval_pointing_the_wrong_way_is_refused():
    """Another shape that would otherwise hang rather than answer."""
    with pytest.raises(Exception, match="count up|count down"):
        values(
            f"SELECT * FROM GENERATE_SERIES({_JAN_3}, {_JAN_1}, INTERVAL '1' DAY) AS g"
        )


def test_temporal_series_needs_timestamp_bounds():
    """A bare string is not a timestamp, and the refusal says how to make it one."""
    with pytest.raises(Exception):
        values(
            "SELECT * FROM GENERATE_SERIES('2020-01-01', '2020-01-02', INTERVAL '1' HOUR) AS g"
        )


# ------------------------------------------------------------- gap filling ---


def test_dense_hour_axis_finds_the_hours_with_no_traffic():
    """The motivating query: WHEN DID SOMETHING STOP HAPPENING.

    Traffic exists at 00:00, 01:00 and 04:00. Hours 02:00 and 03:00 have no rows
    at all, so no amount of grouping over the traffic can produce them — the
    absence is what is being looked for. The dense axis supplies the rows and the
    LEFT JOIN leaves them null.
    """
    traffic = """(SELECT CAST(ts AS TIMESTAMP) AS ts, bytes FROM (VALUES
        ('2020-01-01 00:00', 100),
        ('2020-01-01 00:30', 150),
        ('2020-01-01 01:00', 200),
        ('2020-01-01 04:00', 400)
    ) AS v(ts, bytes)) AS t"""

    # A table-function relation is named by its alias and exposes ONE column of
    # that same name — there is no `AS axis(hour)` column-alias form here — so the
    # hour axis is `axis.axis`.
    result = rows(
        "SELECT axis.axis, SUM(t.bytes) AS total "
        "FROM GENERATE_SERIES("
        "  CAST('2020-01-01 00:00' AS TIMESTAMP), "
        "  CAST('2020-01-01 04:00' AS TIMESTAMP), INTERVAL '1' HOUR) AS axis "
        f"LEFT JOIN {traffic} ON TRUNC(t.ts, 'hour') = axis.axis "
        "GROUP BY axis.axis "
        "ORDER BY axis.axis"
    )

    assert result == [
        (datetime.datetime(2020, 1, 1, 0, 0), 250),
        (datetime.datetime(2020, 1, 1, 1, 0), 200),
        (datetime.datetime(2020, 1, 1, 2, 0), None),
        (datetime.datetime(2020, 1, 1, 3, 0), None),
        (datetime.datetime(2020, 1, 1, 4, 0), 400),
    ], result


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
