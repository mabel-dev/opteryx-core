"""COUNT over a `SELECT DISTINCT *` subquery must count the distinct rows.

    SELECT COUNT(*) FROM (SELECT DISTINCT * FROM t) AS d     -> 1   (expected 100,181)
    SELECT COUNT(a) FROM (SELECT DISTINCT * FROM t) AS d     -> deduped on `a` alone

Silent wrong answers. `SELECT DISTINCT *` plans with no Project, so the Distinct sits
directly on the Scan, and projection pushdown narrowed that Scan to the columns the
OUTER query reads — none for COUNT(*), only `a` for COUNT(a). The Distinct then deduped
on that narrowed set: zero columns collapse to a single row. A Distinct with no ON
dedups on every column reaching it, so the leaf beneath an open Distinct region now
keeps its full width.
"""

import os
import sys

import pytest

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "..", ".."))

import pyarrow
import pyarrow.parquet

import opteryx

ROWS = [
    (1, "x", 10.0),
    (1, "x", 10.0),  # exact duplicate
    (2, "y", 20.0),
    (2, "z", 20.0),
    (3, "w", None),
    (3, "w", None),  # exact duplicate, with a null
    (None, "v", 30.0),
]
DISTINCT_ROWS = len(set(ROWS))  # 5
DISTINCT_A = len({r[0] for r in ROWS})  # 4, null counts as a value under DISTINCT


@pytest.fixture(scope="module")
def t(tmp_path_factory):
    path = tmp_path_factory.mktemp("distinct_star") / "dups.parquet"
    a, b, c = zip(*ROWS)
    pyarrow.parquet.write_table(
        pyarrow.table({"a": list(a), "b": list(b), "c": list(c)}), str(path)
    )
    return f"READ_PARQUET('{path}')"


def rows(sql):
    out = []
    for morsel in opteryx.session().execute_to_morsels(sql):
        if morsel is not None:
            out.extend(morsel.to_arrow().to_pylist())
    return out


def scalar(sql):
    result = rows(sql)
    assert len(result) == 1, result
    return next(iter(result[0].values()))


@pytest.mark.parametrize(
    "sql, expected",
    [
        ("SELECT COUNT(*) FROM (SELECT DISTINCT * FROM {t}) AS d", DISTINCT_ROWS),
        ("SELECT COUNT(*) AS n FROM (SELECT DISTINCT * FROM {t}) d", DISTINCT_ROWS),
        ("SELECT (SELECT COUNT(*) FROM (SELECT DISTINCT * FROM {t}) AS d) AS n", DISTINCT_ROWS),
        ("SELECT COUNT(a) FROM (SELECT DISTINCT * FROM {t}) AS d", DISTINCT_ROWS - 1),
        ("SELECT COUNT(b) FROM (SELECT DISTINCT * FROM {t}) AS d", DISTINCT_ROWS),
        ("SELECT COUNT(DISTINCT b) FROM (SELECT DISTINCT * FROM {t}) AS d", 5),
        ("SELECT SUM(a) FROM (SELECT DISTINCT * FROM {t}) AS d", 1 + 2 + 2 + 3),
        ("SELECT COUNT(*) FROM (SELECT DISTINCT a, b, c FROM {t}) AS d", DISTINCT_ROWS),
        ("SELECT COUNT(*) FROM (SELECT DISTINCT a, b FROM {t}) AS d", DISTINCT_ROWS),
        ("SELECT COUNT(*) FROM (SELECT DISTINCT a FROM {t}) AS d", DISTINCT_A),
        ("SELECT COUNT(*) FROM (SELECT a, b, c FROM {t} GROUP BY a, b, c) AS d", DISTINCT_ROWS),
        ("SELECT COUNT(*) FROM (SELECT a, b FROM {t} GROUP BY a, b) AS d", DISTINCT_ROWS),
        ("SELECT COUNT(*) FROM (SELECT a FROM {t} GROUP BY a) AS d", DISTINCT_A),
        # a Filter between the Distinct and the Scan
        ("SELECT COUNT(*) FROM (SELECT DISTINCT * FROM {t} WHERE b <> 'v') AS d", DISTINCT_ROWS - 1),
        # the Distinct over a further subquery leaf
        ("SELECT COUNT(*) FROM (SELECT DISTINCT * FROM (SELECT * FROM {t}) AS i) AS d", DISTINCT_ROWS),
        # nested: the outer subquery also hides the Distinct from the aggregate
        ("SELECT COUNT(*) FROM (SELECT * FROM (SELECT DISTINCT * FROM {t}) AS i) AS d", DISTINCT_ROWS),
        ("WITH d AS (SELECT DISTINCT * FROM {t}) SELECT COUNT(*) FROM d", DISTINCT_ROWS),
        # the Distinct over a join: every column of both sides is a dedup key
        (
            "SELECT COUNT(*) FROM (SELECT DISTINCT * FROM {t} AS l CROSS JOIN (SELECT 1 AS k) AS r) AS d",
            DISTINCT_ROWS,
        ),
        ("SELECT COUNT(*) FROM (SELECT DISTINCT * FROM $planets) AS d", 9),
        ("SELECT COUNT(*) FROM (SELECT DISTINCT * FROM (SELECT gravity FROM $planets) AS g) AS d", 8),
    ],
)
def test_count_over_distinct(t, sql, expected):
    assert scalar(sql.format(t=t)) == expected


def test_distinct_star_rows_unchanged(t):
    # the subquery itself was always right; it is the reference for the counts above
    assert len(rows(f"SELECT DISTINCT * FROM {t}")) == DISTINCT_ROWS
    assert len(rows(f"SELECT * FROM {t}")) == len(ROWS)
