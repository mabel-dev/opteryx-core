"""Regression: `ORDER BY <expr> NULLS FIRST | NULLS LAST` was ignored.

Found 2026-09-27: `ORDER BY (CASE WHEN k > 5 THEN NULL ELSE k END) NULLS LAST LIMIT 1`
returned a NULL-keyed row, and `ORDER BY d NULLS LAST LIMIT 1` over a nullable DECIMAL
returned NULL. The NULLS clause was dropped in the logical planner — the ORDER BY item
was reduced to `(expression, ascending)` — and the native sort had no null placement
of its own: `SortKeyCmp` hard-coded "NULL is the lowest value" and DESC flipped it.

Now the planner resolves every key to `(expression, ascending, nulls_first)` and the
native `SortKeySpec` carries `nulls_first`, independent of direction. The DEFAULT, when
no NULLS clause is written, is unchanged: NULL is the lowest value — NULLS FIRST under
ASC, NULLS LAST under DESC (architect ruling, 2026-09-27; the inverse of Postgres).

Window and aggregate ORDER BY implement only the default placement; a different one
there is refused, not silently ignored.

Every case compares the exact key SEQUENCE against a Python reference — the keys are
distinct apart from the NULLs, so the sequence is fully determined.
"""

import os
import sys
from decimal import Decimal

import pytest

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "..", ".."))

import pyarrow as pa  # test-only dep (allowed in tests/)
import pyarrow.parquet as pq

import opteryx
from opteryx.exceptions import NotSupportedError
from opteryx.exceptions import UnsupportedSyntaxError

_SESSION = opteryx.session()

# id -> k; distinct non-null keys, NULLs interleaved (not grouped at either end).
_KEYS = [5, None, 3, None, 8, 1, None, 9, 0, 4]

_TYPES = {
    "int": (lambda k: k, "k", pa.int64()),
    "varchar": (lambda k: f"v{k}", "'v' || CAST(k AS VARCHAR)", pa.string()),
    "decimal": (lambda k: Decimal(k) + Decimal("0.25"), "CAST(k AS DECIMAL(10,2)) + 0.25", pa.decimal128(10, 2)),
}

_DIRECTIONS = ["", " ASC", " DESC"]
_PLACEMENTS = ["", " NULLS FIRST", " NULLS LAST"]
_LIMITS = [None, 1, 2, 3, 6]


def _values(sql, column="v"):
    out = []
    for morsel in _SESSION.execute_to_morsels(sql):
        out.extend(morsel.column(column).to_pylist())
    return out


def _expected(values, direction, placement, limit):
    descending = direction.strip() == "DESC"
    if placement:
        nulls_first = placement.strip() == "NULLS FIRST"
    else:
        nulls_first = not descending  # default: NULL is the lowest value
    non_null = sorted((v for v in values if v is not None), reverse=descending)
    nulls = [None] * sum(v is None for v in values)
    ordered = nulls + non_null if nulls_first else non_null + nulls
    return ordered if limit is None else ordered[:limit]


def _values_relation(expression):
    rows = ", ".join(f"({i}, {'NULL' if k is None else k})" for i, k in enumerate(_KEYS))
    return f"(SELECT id, {expression} AS v FROM (VALUES {rows}) AS t(id, k)) AS s"


@pytest.fixture(scope="module")
def parquet_path(tmp_path_factory):
    """The same rows as a parquet file, typed natively (no computed key), so the
    scan-fed sort paths are exercised as well as the in-memory one."""
    directory = tmp_path_factory.mktemp("null_placement")
    table = {"id": pa.array(range(len(_KEYS)), type=pa.int64())}
    for name, (convert, _expression, arrow_type) in _TYPES.items():
        table[name] = pa.array([None if k is None else convert(k) for k in _KEYS], type=arrow_type)
    pq.write_table(pa.table(table), str(directory / "data.parquet"), row_group_size=3)
    return str(directory)


@pytest.mark.parametrize("limit", _LIMITS)
@pytest.mark.parametrize("placement", _PLACEMENTS)
@pytest.mark.parametrize("direction", _DIRECTIONS)
@pytest.mark.parametrize("key_type", sorted(_TYPES))
def test_null_placement_in_memory(key_type, direction, placement, limit):
    convert, expression, _arrow_type = _TYPES[key_type]
    values = [None if k is None else convert(k) for k in _KEYS]
    tail = "" if limit is None else f" LIMIT {limit}"
    sql = f"SELECT v FROM {_values_relation(expression)} ORDER BY v{direction}{placement}{tail}"
    assert _values(sql) == _expected(values, direction, placement, limit), sql


@pytest.mark.parametrize("limit", _LIMITS)
@pytest.mark.parametrize("placement", _PLACEMENTS)
@pytest.mark.parametrize("direction", _DIRECTIONS)
@pytest.mark.parametrize("key_type", sorted(_TYPES))
def test_null_placement_parquet(parquet_path, key_type, direction, placement, limit):
    convert, _expression, _arrow_type = _TYPES[key_type]
    values = [None if k is None else convert(k) for k in _KEYS]
    tail = "" if limit is None else f" LIMIT {limit}"
    sql = f"SELECT {key_type} AS v FROM '{parquet_path}' ORDER BY {key_type}{direction}{placement}{tail}"
    assert _values(sql) == _expected(values, direction, placement, limit), sql


@pytest.mark.parametrize("limit", [None, 1])
def test_reported_case_expression_key(limit):
    """The reported shape: the sort key is an expression that yields NULL."""
    tail = "" if limit is None else f" LIMIT {limit}"
    sql = (
        "SELECT k AS v FROM (VALUES (0), (1), (2), (3), (4), (5), (6), (7), (8), (9)) AS t(k) "
        f"ORDER BY (CASE WHEN k > 5 THEN NULL ELSE k END) NULLS LAST{tail}"
    )
    got = _values(sql)
    assert got[0] == 0
    if limit is None:
        assert got[:6] == [0, 1, 2, 3, 4, 5]
        assert sorted(got[6:]) == [6, 7, 8, 9]


def test_reported_case_decimal_nulls_last():
    sql = (
        f"SELECT v FROM {_values_relation(_TYPES['decimal'][1])} "
        "ORDER BY v NULLS LAST LIMIT 1"
    )
    assert _values(sql) == [Decimal("0.25")]


@pytest.mark.parametrize("limit", [None, 4])
def test_multi_key_independent_placement(limit):
    """Each key carries its own placement: a DESC NULLS FIRST, b ASC NULLS LAST."""
    rows = [
        (0, 1, 1), (1, 1, None), (2, None, 2), (3, 2, None), (4, None, None),
        (5, 2, 1), (6, 1, 0), (7, None, 1), (8, 2, 3),
    ]
    literal = ", ".join(
        "(" + ", ".join("NULL" if x is None else str(x) for x in r) + ")" for r in rows
    )

    def key(r):
        _id, a, b = r
        # a DESC NULLS FIRST: NULLs rank first, then larger a first.
        a_rank = (0, 0) if a is None else (1, -a)
        # b ASC NULLS LAST: smaller b first, NULLs last.
        b_rank = (1, 0) if b is None else (0, b)
        return (a_rank, b_rank)

    expected = [r[0] for r in sorted(rows, key=key)]
    tail = "" if limit is None else f" LIMIT {limit}"
    sql = (
        f"SELECT id AS v FROM (VALUES {literal}) AS t(id, a, b) "
        f"ORDER BY a DESC NULLS FIRST, b ASC NULLS LAST{tail}"
    )
    got = _values(sql)
    # (NULL, NULL) pairs tie: ids 4 is alone, but (a NULL, b 1) vs (a NULL, b 2) are
    # distinct — every (a, b) pair in the fixture is unique, so the order is total.
    assert got == (expected if limit is None else expected[:limit]), sql


def test_order_by_over_group_by_honours_nulls_last():
    sql = (
        "SELECT g AS v FROM (VALUES (1, 5), (2, NULL), (3, 1), (2, NULL), (1, 7), (3, 2)) AS t(g, x) "
        "GROUP BY g ORDER BY MAX(x) NULLS LAST"
    )
    assert _values(sql) == [3, 1, 2]


def test_distinct_order_by_desc_nulls_first():
    sql = (
        "SELECT DISTINCT v FROM (VALUES (1), (NULL), (3), (1), (NULL), (2)) AS t(v) "
        "ORDER BY v DESC NULLS FIRST"
    )
    assert _values(sql) == [None, 3, 2, 1]


# --- where the non-default placement is refused, not ignored -------------------


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT ROW_NUMBER() OVER (ORDER BY v NULLS LAST) AS r FROM (VALUES (1), (NULL)) AS t(v)",
        "SELECT ROW_NUMBER() OVER (ORDER BY v DESC NULLS FIRST) AS r FROM (VALUES (1), (NULL)) AS t(v)",
        "SELECT SUM(v) OVER (ORDER BY v NULLS LAST ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS r "
        "FROM (VALUES (1), (NULL)) AS t(v)",
        "SELECT ARRAY_AGG(v ORDER BY v NULLS LAST) AS r FROM (VALUES (1, 1), (NULL, 1)) AS t(v, g) GROUP BY g",
    ],
)
def test_non_default_placement_refused_where_unimplemented(sql):
    # Pinned to the refusal's own wording, so an unrelated error cannot pass it.
    with pytest.raises((UnsupportedSyntaxError, NotSupportedError), match="NULLS"):
        _values(sql, "r")


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT ROW_NUMBER() OVER (ORDER BY v NULLS FIRST) AS r FROM (VALUES (1), (NULL)) AS t(v)",
        "SELECT ROW_NUMBER() OVER (ORDER BY v DESC NULLS LAST) AS r FROM (VALUES (1), (NULL)) AS t(v)",
        "SELECT ARRAY_AGG(v ORDER BY v NULLS FIRST) AS r FROM (VALUES (1, 1), (NULL, 1)) AS t(v, g) GROUP BY g",
    ],
)
def test_explicit_default_placement_accepted_where_only_default_exists(sql):
    """Writing the default placement explicitly is what those sorts already do."""
    _values(sql, "r")


def test_decorrelated_subquery_with_non_default_placement_is_refused():
    """A correlated `ORDER BY ... LIMIT 1` subquery is decorrelated into a per-key
    ROW_NUMBER window — a window sort, which implements only the default placement.
    The compiler refuses it at the sink boundary rather than sorting the default way."""
    base = "(VALUES (1, 1, 5), (2, 1, NULL), (3, 2, 7), (4, 2, NULL), (5, 2, 3)) AS s(sid, pid, r)"
    sql = (
        "SELECT id AS r FROM (VALUES (1), (2)) AS p(id) WHERE 5 = "
        f"(SELECT s.r FROM {base} WHERE s.pid = p.id ORDER BY s.r NULLS LAST LIMIT 1)"
    )
    with pytest.raises(NotSupportedError, match="NULLS"):
        _values(sql, "r")
