"""Lazy branch evaluation (BC_LAZY): a guarded operand runs ONLY on the rows its guard admits.

`CASE WHEN a = MAX THEN 0 ELSE a + 1 END` is correct SQL — the ELSE branch must not be
evaluated on the row the guard sent to THEN. Before lazy evaluation every branch ran over
every row, so INT64 overflow (which fails loud) raised on rows the guard excluded. The
same holds for AND/OR right operands, DNF/CNF terms, IIF and COALESCE arguments.

An error on a row the guard DOES admit must still raise — laziness narrows what runs, it
never swallows an error.
"""

import os
import sys

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import opteryx

MAX = 9223372036854775807


def _row(id_, a, s, d, ts, f, b):
    return (
        f"SELECT CAST({id_} AS INT64) AS id, CAST({a} AS INT64) AS a, {s} AS s, "
        f"CAST({d} AS DECIMAL(10,2)) AS d, CAST('{ts}' AS TIMESTAMP) AS t, "
        f"CAST({f} AS FLOAT64) AS f, {b} AS b"
    )


# id 1 carries a = INT64 MAX, so `a + 1` overflows on that row and only that row.
BASE = (
    "("
    + " UNION ALL ".join(
        [
            _row(1, MAX, "'x'", "1.50", "2024-01-01 00:00:00", 1.5, "TRUE"),
            _row(2, 5, "'yy'", "2.25", "2024-01-02 00:00:00", 2.5, "FALSE"),
            _row(3, 7, "CAST(NULL AS VARCHAR)", "3.75", "2024-01-03 00:00:00", 3.5,
                 "CAST(NULL AS BOOLEAN)"),
            _row(4, -3, "'zzz'", "4.00", "2024-01-04 00:00:00", 4.5, "TRUE"),
        ]
    )
    + ") AS t"
)


def _rows(sql):
    out = []
    for morsel in opteryx.session().execute_to_morsels(sql):
        out.extend(morsel.to_arrow().to_pylist())
    return out


def _col(expr, where="", order="id"):
    sql = f"SELECT id, {expr} AS r FROM {BASE} {where} ORDER BY {order}"
    return [r["r"] for r in _rows(sql)]


GUARDED = [
    # (expression, expected per id 1..4)
    (f"CASE WHEN a = {MAX} THEN 0 ELSE a + 1 END", [0, 6, 8, -2]),
    (f"IIF(a = {MAX}, 0, a + 1)", [0, 6, 8, -2]),
    (f"CASE WHEN a = {MAX} THEN 0 WHEN a > 6 THEN a + 1 WHEN a < 0 THEN a - 1 ELSE a * 2 END",
     [0, 10, 8, -4]),
    (f"CASE WHEN a < {MAX} THEN a + 1 END", [None, 6, 8, -2]),
    (f"COALESCE(CASE WHEN a = {MAX} THEN 1 END, a + 1)", [1, 6, 8, -2]),
    (f"(a < {MAX} AND a + 1 > 0)", [False, True, True, False]),
    (f"(a = {MAX} OR a + 1 > 0)", [True, True, True, False]),
    (f"(id > 0 AND a < {MAX} AND a + 1 > 0)", [False, True, True, False]),
    (f"(id < 0 OR a = {MAX} OR a + 1 > 0)", [True, True, True, False]),
]


@pytest.mark.parametrize("expr,expected", GUARDED)
def test_guarded_overflow_is_not_evaluated_on_excluded_rows(expr, expected):
    assert _col(expr) == expected


@pytest.mark.parametrize(
    "where",
    [
        f"WHERE a < {MAX} AND a + 1 > 0",
        f"WHERE id > 0 AND a < {MAX} AND a + 1 > 0",
        f"WHERE a = {MAX} OR a + 1 > 0",
    ],
)
def test_guarded_filter_predicates(where):
    got = [r["id"] for r in _rows(f"SELECT id FROM {BASE} {where} ORDER BY id")]
    assert got == ([1, 2, 3] if "OR" in where else [2, 3])


@pytest.mark.parametrize(
    "expr",
    [
        f"CASE WHEN a = {MAX} THEN a + 1 ELSE 0 END",       # THEN branch, live row
        f"IIF(a = {MAX}, a + 1, 0)",
        f"(a = {MAX} AND a + 1 > 0)",                        # AND right side, live row
        f"(a < 0 OR a + 1 > 0)",                             # OR right side runs on id 1
        f"COALESCE(NULL, a + 1)",                            # only argument that can answer
    ],
)
def test_error_on_a_live_row_still_raises(expr):
    with pytest.raises(Exception, match="INT64 addition overflow"):
        _col(expr)


def test_error_in_a_filter_on_a_live_row_still_raises():
    with pytest.raises(Exception, match="INT64 addition overflow"):
        _rows(f"SELECT id FROM {BASE} WHERE a > 0 AND a + 1 > 0")


BRANCH_TYPES = [
    ("string", "CASE WHEN a > 6 THEN s ELSE UPPER(s) END", ["x", "YY", None, "ZZZ"]),
    ("string literal", "IIF(a > 6, s, 'no')", ["x", "no", None, "no"]),
    ("float", "CASE WHEN a > 6 THEN f * 2 ELSE f END", [3.0, 2.5, 7.0, 4.5]),
    ("bool", "CASE WHEN a > 6 THEN b ELSE NOT b END", [True, True, None, False]),
    ("null branch", "CASE WHEN a > 6 THEN NULL ELSE a + 1 END", [None, 6, None, -2]),
    ("coalesce", "COALESCE(s, 'none')", ["x", "yy", "none", "zzz"]),
    ("coalesce 3", "COALESCE(s, CAST(a AS VARCHAR), 'z')", ["x", "yy", "7", "zzz"]),
    ("ifnull", "IFNULL(s, 'none')", ["x", "yy", "none", "zzz"]),
    ("ifnotnull", "IFNOTNULL(s, 'has')", ["has", "has", None, "has"]),
    ("nullif", "NULLIF(a, 5)", [MAX, None, 7, -3]),
]


@pytest.mark.parametrize("name,expr,expected", BRANCH_TYPES, ids=[t[0] for t in BRANCH_TYPES])
def test_branch_result_types_and_null_semantics(name, expr, expected):
    assert _col(expr) == expected


def test_decimal_and_timestamp_branches():
    from decimal import Decimal
    import datetime

    assert _col("CASE WHEN a > 6 THEN d * 2 ELSE d END") == [
        Decimal("3.00"), Decimal("2.25"), Decimal("7.50"), Decimal("4.00")]
    assert _col("CASE WHEN a > 6 THEN t ELSE NULL END") == [
        datetime.datetime(2024, 1, 1), None, datetime.datetime(2024, 1, 3), None]


def test_null_guard_takes_the_else_branch_and_null_and_kleene_holds():
    # CASE: a NULL condition is not TRUE, so the row takes ELSE.
    got = [r["r"] for r in _rows(
        f"SELECT id, CASE WHEN b THEN a + 1 ELSE a - 1 END AS r "
        f"FROM (SELECT * FROM {BASE} WHERE a < {MAX}) ORDER BY id")]
    assert got == [4, 6, -2]
    # AND: NULL AND TRUE is NULL, so the right side must run on NULL-guard rows too.
    got = [r["r"] for r in _rows(
        f"SELECT id, (b AND a + 1 > 0) AS r "
        f"FROM (SELECT * FROM {BASE} WHERE a < {MAX}) ORDER BY id")]
    assert got == [False, None, False]


def test_real_table_with_array_string_and_null_columns():
    astro = "testdata.astronauts"
    base = _rows(f"SELECT name, year, missions[1] AS m FROM {astro} ORDER BY name")
    lazy = _rows(
        f"SELECT name, CASE WHEN year < 1990 THEN missions[1] ELSE 'none' END AS r "
        f"FROM {astro} ORDER BY name")
    expect = [
        (x["name"], x["m"] if (x["year"] is not None and x["year"] < 1990) else "none")
        for x in base
    ]
    assert [(x["name"], x["r"]) for x in lazy] == expect

    upper = _rows(
        f"SELECT name, CASE WHEN year < 1990 THEN UPPER(name) ELSE LOWER(name) END AS r "
        f"FROM {astro} ORDER BY name")
    expect = [
        (x["name"], x["name"].upper() if (x["year"] is not None and x["year"] < 1990)
         else x["name"].lower())
        for x in _rows(f"SELECT name, year FROM {astro} ORDER BY name")
    ]
    assert [(x["name"], x["r"]) for x in upper] == expect

    filt = [x["name"] for x in _rows(
        f"SELECT name FROM {astro} WHERE year < 1990 AND UPPER(name) LIKE '%A%' ORDER BY name")]
    expect = sorted(
        x["name"] for x in _rows(f"SELECT name, year FROM {astro}")
        if x["year"] is not None and x["year"] < 1990 and "A" in x["name"].upper())
    assert filt == expect


def test_real_table_guarded_overflow():
    big = 2000000000000000000
    got = [r["r"] for r in _rows(
        f"SELECT id, CASE WHEN id < 5 THEN id * {big} ELSE 0 END AS r "
        "FROM testdata.planets ORDER BY id")]
    assert got == [1 * big, 2 * big, 3 * big, 4 * big, 0, 0, 0, 0, 0]
    got = [r["id"] for r in _rows(
        f"SELECT id FROM testdata.planets WHERE id < 5 AND id * {big} > 0 ORDER BY id")]
    assert got == [1, 2, 3, 4]
    with pytest.raises(Exception, match="INT64 multiplication overflow"):
        _rows(f"SELECT id * {big} AS r FROM testdata.planets")
