"""A temporal literal in a VALUES list must build a temporal column.

    SELECT * FROM (VALUES ('a', CAST('2026-10-01 01:00:00' AS TIMESTAMP))) AS v(i, t)
      -> TypeError: timestamp sequence: element must be datetime.datetime or None, got int

Every temporal type failed the same way — DATE ("date32: element must be datetime.date
or None, got int") and TIME ("unsupported dtype name 'TIME64'") too — in any number of
rows or columns.

A folded temporal literal carries its PHYSICAL value: `CAST('2026-10-01' AS DATE)` is
the int 20727 tagged DATE32, a TIMESTAMP is epoch microseconds, a TIME is microseconds
since midnight. That is what the expression engine's constant materialisation expects.
The VALUES builder instead handed those ints to `vector_from_sequence`, whose temporal
constructors take Python datetime objects. It now builds a temporal column the way the
constant path does: an integer vector, reinterpreted as the temporal type, with the
column's logical descriptor attached.
"""

import datetime
import os
import sys

import pytest

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "..", ".."))

import opteryx


def columns(sql):
    out: dict = {}
    for morsel in opteryx.session().execute_to_morsels(sql):
        if morsel is None:
            continue
        for key, values in morsel.to_arrow().to_pydict().items():
            out.setdefault(key, []).extend(values)
    return out


@pytest.mark.parametrize(
    "sql,expected",
    [
        # The reported shape: a second column beside the timestamp.
        ("SELECT * FROM (VALUES ('a', CAST('2026-10-01 01:00:00' AS TIMESTAMP))) AS v(i, t)",
         {"i": ["a"], "t": [datetime.datetime(2026, 10, 1, 1)]}),
        # Several rows, a NULL among them, and a value before the epoch (negative).
        ("SELECT * FROM (VALUES (CAST('2026-10-01 01:00:00' AS TIMESTAMP)), (NULL), "
         "(CAST('1969-12-31 23:59:59' AS TIMESTAMP))) AS v(t)",
         {"t": [datetime.datetime(2026, 10, 1, 1), None,
                datetime.datetime(1969, 12, 31, 23, 59, 59)]}),
        ("SELECT * FROM (VALUES (CAST('2026-10-01' AS DATE)), (NULL), "
         "(CAST('1900-01-01' AS DATE))) AS v(d)",
         {"d": [datetime.date(2026, 10, 1), None, datetime.date(1900, 1, 1)]}),
        ("SELECT * FROM (VALUES (CAST('01:02:03' AS TIME)), "
         "(CAST('23:59:59.5' AS TIME))) AS v(t)",
         {"t": [datetime.time(1, 2, 3), datetime.time(23, 59, 59, 500000)]}),
        # A short row still reads NULL for its missing cell, temporal or not.
        ("SELECT * FROM (VALUES ('x', CAST('2026-10-01' AS DATE)), ('y')) AS v(a, b)",
         {"a": ["x", "y"], "b": [datetime.date(2026, 10, 1), None]}),
        # The column is a real temporal column: temporal kernels accept it.
        ("SELECT t + INTERVAL '1' DAY AS later, DATE_TRUNC('day', t) AS day FROM (VALUES "
         "(CAST('2026-10-01 01:00:00' AS TIMESTAMP)), "
         "(CAST('2026-10-02 05:00:00' AS TIMESTAMP))) AS v(t)",
         {"later": [datetime.datetime(2026, 10, 2, 1), datetime.datetime(2026, 10, 3, 5)],
          "day": [datetime.datetime(2026, 10, 1), datetime.datetime(2026, 10, 2)]}),
    ],
)
def test_values_temporal_literal(sql, expected):
    assert columns(sql) == expected


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
