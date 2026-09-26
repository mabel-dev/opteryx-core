"""Unit tests for the timetravel expression evaluator.

These tests drive the private helpers directly so that regressions in
``_evaluate_timetravel_expression`` are caught quickly.  The bug fixed in
this pull request manifested when a date was subtracted from an interval.  In
that scenario the generic ``binary_operations`` path returned ``null`` which
caused the planner to consider the expression unresolved and the whole
query to fail.

Since the behaviour is intrinsically time dependent we check the result in
relation to ``datetime.datetime.now()`` rather than asserting an absolute
value.  The integration suite already exercises the end-to-end query
behaviour (see ``tests/integration/sql_battery/test_shapes_basic.py``) but
having a focused unit test makes the reason for this code more obvious to
future maintainers.
"""

import os
import sys
import datetime

# ensure the workspace root is on sys.path so that the local package
# is imported instead of any installed version.  This mimics the behaviour
# of the majority of existing tests, which rely on `pytest` running from the
# repository root.
sys.path.insert(0, os.path.abspath(os.getcwd()))

from opteryx.planner.logical_planner.logical_planner_builders import (
    extract_timetravel_timestamp,
)
from opteryx.third_party import sqloxide
from opteryx.planner.plan_context import PlanContext


def _parse_version(sql: str):
    # helper that returns the portion of the AST containing the timestamp
    parsed = sqloxide.parse_sql(sql, _dialect="opteryx")[0]
    return parsed["Query"]["body"]["Select"]["from"][0]["relation"]["Table"]["version"]


def test_current_date_minus_interval_evaluates_to_timestamp():
    """Subtracting an interval from CURRENT_DATE should return a datetime.

    Prior to the fix this expression resulted in ``None`` and triggered an
    ``UnsupportedSyntaxError`` when the planner attempted to build the
    version clause.  The returned value should be roughly "now minus the
    interval" and have ``datetime.datetime`` type.
    """
    plan_context = PlanContext()

    version = _parse_version("SELECT * FROM $planets TIMESTAMP AS OF CURRENT_DATE - INTERVAL '7' DAY")
    ts = extract_timetravel_timestamp(version, plan_context=plan_context)
    assert isinstance(ts, datetime.datetime)
    now = datetime.datetime.now(datetime.UTC).replace(tzinfo=None)
    # allow a few seconds of drift since NOW() is evaluated on the fly
    diff = now - ts
    assert datetime.timedelta(days=6, seconds=-10) < diff < datetime.timedelta(days=8)


def test_trunc_month_on_current_date():
    """TRUNC should be handled correctly during time-travel evaluation."""
    plan_context = PlanContext()

    version = _parse_version("SELECT * FROM $planets TIMESTAMP AS OF TRUNC(CURRENT_DATE, 'month')")
    ts = extract_timetravel_timestamp(version, plan_context=plan_context)
    assert isinstance(ts, datetime.datetime)

    # the result should be the first day of the current month
    now = datetime.datetime.now(datetime.UTC).replace(tzinfo=None)
    expected = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    # allow off-by-one-day if timezone processing changes
    assert expected - datetime.timedelta(days=1) <= ts <= expected + datetime.timedelta(days=1)


def test_interval_plus_date_is_symmetric():
    """Ensure that ``INTERVAL + DATE`` also works because the grammar permits it."""
    plan_context = PlanContext()

    version = _parse_version("SELECT * FROM $planets TIMESTAMP AS OF INTERVAL '1' DAY + CURRENT_DATE")
    ts = extract_timetravel_timestamp(version, plan_context=plan_context)
    assert isinstance(ts, datetime.datetime)
    now = datetime.datetime.now(datetime.UTC).replace(tzinfo=None)
    diff = ts - now
    assert datetime.timedelta(days=0) < diff < datetime.timedelta(days=2)


def test_a_date_typed_value_resolves_to_a_datetime():
    """THE DATE BRANCH. A time-travel value is an instant however it was typed.

    `'2026-09-20'::DATE` used to resolve to a `datetime.date`, and the
    connectors take it from here as a point in time: `opteryx_connector` calls
    `.timestamp()` on it and `date` has no such attribute, so a legal query
    died as `'datetime.date' object has no attribute 'timestamp'` - an
    attribute error wearing a Dataset Read Error's clothes.

    Asserted as equality with the undecorated spelling rather than as a type:
    the contract is that the three ways of naming one day cannot be told apart
    downstream, which is the thing that was broken.
    """
    plan_context = PlanContext()
    plain = extract_timetravel_timestamp(
        _parse_version("SELECT * FROM $planets TIMESTAMP AS OF '2026-09-20'"), 
    plan_context=plan_context)
    cast_to_date = extract_timetravel_timestamp(
        _parse_version("SELECT * FROM $planets TIMESTAMP AS OF '2026-09-20'::DATE"), 
    plan_context=plan_context)
    cast_to_timestamp = extract_timetravel_timestamp(
        _parse_version("SELECT * FROM $planets TIMESTAMP AS OF '2026-09-20'::TIMESTAMP"), 
    plan_context=plan_context)

    assert isinstance(cast_to_date, datetime.datetime)
    assert cast_to_date == plain == cast_to_timestamp == datetime.datetime(2026, 9, 20)


def test_current_date_resolves_to_a_datetime():
    """`CURRENT_DATE` is DATE-typed, so it took the same broken branch - and
    unlike the cast, nobody has to reach for an unusual spelling to hit it.
    `CURRENT_DATE - INTERVAL '7' DAY` was fine throughout (the arithmetic
    yields a timestamp), which is why the plain form went unnoticed."""
    plan_context = PlanContext()
    ts = extract_timetravel_timestamp(
        _parse_version("SELECT * FROM $planets TIMESTAMP AS OF CURRENT_DATE"), 
    plan_context=plan_context)

    assert isinstance(ts, datetime.datetime)
    today = datetime.datetime.now(datetime.UTC).replace(tzinfo=None)
    assert ts == datetime.datetime(today.year, today.month, today.day)


def test_the_resolved_value_is_what_a_connector_calls():
    """The failure was never in the planner - it was one `.timestamp()` away,
    in `opteryx_connector._to_snapshot`. This is that call, and nothing above
    asserts it: a future normalizer could satisfy every equality here with an
    object no connector can use."""
    plan_context = PlanContext()
    for sql in (
        "SELECT * FROM $planets TIMESTAMP AS OF '2026-09-20'::DATE",
        "SELECT * FROM $planets TIMESTAMP AS OF CURRENT_DATE",
    ):
        ts = extract_timetravel_timestamp(_parse_version(sql), plan_context=plan_context)
        assert isinstance(ts.timestamp(), float)


def test_a_time_of_day_survives_normalization():
    """THE GUARD ON THE FIX. `datetime` subclasses `date`, so promoting "a
    date" to midnight with a careless isinstance would truncate every
    timestamp and move a point-in-time read backwards by up to a day -
    silently, and in the direction of returning older data."""
    plan_context = PlanContext()
    ts = extract_timetravel_timestamp(
        _parse_version("SELECT * FROM $planets TIMESTAMP AS OF '2026-09-20 13:45:00'::TIMESTAMP"), 
    plan_context=plan_context)

    assert ts == datetime.datetime(2026, 9, 20, 13, 45)
