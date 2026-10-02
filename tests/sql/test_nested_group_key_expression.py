"""A SELECT item that WRAPS a computed group key must read the key the aggregate emits.

    SELECT LENGTH(UPPER(name)) u, COUNT(*) n FROM t GROUP BY UPPER(name)
      -> InvalidInternalStateError: The compiled plan references a column the stream
         does not carry at this point. (column b'$pl_nam_...'; stream carries
         [b'$derived_...'])

Found in production on a dedup report over opteryx.ops.stderr_log:

    SELECT CAST(DATE_TRUNC('day', first_seen) AS VARCHAR) AS day, COUNT(*) ...
    FROM (... GROUP BY insert_id HAVING COUNT(*) > 1) AS d
    GROUP BY DATE_TRUNC('day', first_seen)

The subquery and the HAVING were incidental. The minimal shape is any SELECT expression
that contains a computed GROUP BY key below its root, with an aggregate alongside. It
ran when the key was ALSO selected bare, and it ran with no aggregate (that is the
DISTINCT path).

The binder was right: the nested `UPPER(name)` bound to the group key's identity. The
fault was projection pushdown's `collect_columns`, which credited a computed column's
OWN identity only at the root of each projected expression, plus its leaf identifiers.
For `LENGTH(UPPER(name))` that is `LENGTH(...)` and `name`, never `UPPER(name)`, so the
GROUP BY sink's `_group_key_emit` ruled the key dead and stopped emitting it. The Project
then tried to recompute `UPPER(name)` from a `name` the aggregate does not carry.

Every expectation below was worked out by hand from the data. The events fixture is
a parquet file, so the production shape is exercised over a real scan.
"""

import datetime
import os
import sys

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "..", ".."))

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import opteryx

# Duplicate ids: `a` twice and `c` three times, first seen on 2026-10-01; `b` and `d`
# once each. Two rows fall on 2026-10-02.
IDS = ["a", "a", "b", "c", "c", "c", "d"]
HOURS = [(1, 1), (1, 2), (1, 3), (1, 4), (1, 5), (2, 6), (2, 7)]


@pytest.fixture(scope="module")
def events(tmp_path_factory):
    directory = tmp_path_factory.mktemp("nested_group_key")
    pq.write_table(
        pa.table({
            "insert_id": IDS,
            "event_timestamp": pa.array(
                [datetime.datetime(2026, 10, d, h) for d, h in HOURS], pa.timestamp("us")
            ),
        }),
        os.path.join(directory, "part.parquet"),
    )
    return f"'{directory}'"


def rows(sql):
    """Every output row as a tuple, sorted — not every spelling here promises an order."""
    session = opteryx.session()
    out: dict = {}
    for morsel in session.execute_to_morsels(sql):
        if morsel is None:
            continue
        for key, values in morsel.to_arrow().to_pydict().items():
            out.setdefault(key, []).extend(values)
    return sorted(zip(*out.values()), key=repr) if out else []


@pytest.mark.parametrize(
    "sql,expected",
    [
        # The minimal shape: the key wrapped, not selected bare.
        ("SELECT LENGTH(UPPER(name)) AS u, COUNT(*) AS n FROM $planets "
         "WHERE id < 4 GROUP BY UPPER(name)",
         [(5, 1), (5, 1), (7, 1)]),
        ("SELECT UPPER(name) || '!' AS u, COUNT(*) AS n FROM $planets "
         "WHERE id < 4 GROUP BY UPPER(name)",
         [("EARTH!", 1), ("MERCURY!", 1), ("VENUS!", 1)]),
        # An arithmetic key nested in more arithmetic: (id + 1) + 1.
        ("SELECT id + 1 + 1 AS u, COUNT(*) AS n FROM $planets WHERE id < 3 GROUP BY id + 1",
         [(3, 1), (4, 1)]),
        # The key both bare AND wrapped — this already ran, and must keep running.
        ("SELECT UPPER(name) AS k, LENGTH(UPPER(name)) AS u, COUNT(*) AS n "
         "FROM $planets WHERE id < 3 GROUP BY UPPER(name)",
         [("MERCURY", 7, 1), ("VENUS", 5, 1)]),
        # CAST over DATE_TRUNC, as the production report wrote it.
        ("SELECT CAST(DATE_TRUNC('day', event_timestamp) AS VARCHAR) AS day, COUNT(*) AS n "
         "FROM {events} GROUP BY DATE_TRUNC('day', event_timestamp)",
         [("2026-10-01T00:00:00.000000", 5), ("2026-10-02T00:00:00.000000", 2)]),
        # The production query: an outer grouping over a HAVING-filtered aggregate.
        ("""SELECT CAST(DATE_TRUNC('day', first_seen) AS VARCHAR) AS day,
                   COUNT(*) AS dup_ids, SUM(n) AS rows
            FROM (SELECT insert_id, COUNT(*) AS n, MIN(event_timestamp) AS first_seen
                    FROM {events} GROUP BY insert_id HAVING COUNT(*) > 1) AS d
            GROUP BY DATE_TRUNC('day', first_seen) ORDER BY day""",
         [("2026-10-01T00:00:00.000000", 2, 5)]),
    ],
)
def test_wrapped_group_key(events, sql, expected):
    assert rows(sql.format(events=events)) == expected


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
