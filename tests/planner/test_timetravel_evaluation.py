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

    version = _parse_version("SELECT * FROM $planets TIMESTAMP AS OF CURRENT_DATE - INTERVAL '7' DAY")
    ts = extract_timetravel_timestamp(version)
    assert isinstance(ts, datetime.datetime)
    now = datetime.datetime.now(datetime.UTC).replace(tzinfo=None)
    # allow a few seconds of drift since NOW() is evaluated on the fly
    diff = now - ts
    assert datetime.timedelta(days=6, seconds=-10) < diff < datetime.timedelta(days=8)


def test_date_trunc_month_on_current_date():
    """DATE_TRUNC should be handled correctly during time-travel evaluation."""

    version = _parse_version("SELECT * FROM $planets TIMESTAMP AS OF DATE_TRUNC('month', CURRENT_DATE)")
    ts = extract_timetravel_timestamp(version)
    assert isinstance(ts, datetime.datetime)

    # the result should be the first day of the current month
    now = datetime.datetime.now(datetime.UTC).replace(tzinfo=None)
    expected = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    # allow off-by-one-day if timezone processing changes
    assert expected - datetime.timedelta(days=1) <= ts <= expected + datetime.timedelta(days=1)


def test_interval_plus_date_is_symmetric():
    """Ensure that ``INTERVAL + DATE`` also works because the grammar permits it."""

    version = _parse_version("SELECT * FROM $planets TIMESTAMP AS OF INTERVAL '1' DAY + CURRENT_DATE")
    ts = extract_timetravel_timestamp(version)
    assert isinstance(ts, datetime.datetime)
    now = datetime.datetime.now(datetime.UTC).replace(tzinfo=None)
    diff = ts - now
    assert datetime.timedelta(days=0) < diff < datetime.timedelta(days=2)
