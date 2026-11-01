"""
NULL members of an IN / NOT IN list (ruled 2026-11-01).

A NULL member means what the comparisons IN expands to say it means:

    x IN (a, NULL)      ==  x = a  OR  x = NULL     ->  TRUE on a match, else NULL
    x NOT IN (a, NULL)  ==  x != a AND x != NULL    ->  FALSE on a match, else NULL

so a NOT IN list holding a NULL keeps no row in a WHERE. A NULL operand gives NULL.
These lists were refused outright (ArrayWithMixedTypesError for an untyped NULL,
"outside the c-native kernel set" for a typed one). The builder now takes the NULLs
out of the list and adds back one `x = NULL` / `x != NULL`.

The matrix test checks every column type against an independent three-valued
oracle written here, in WHERE and in a projection; the expected values of the
scenario tests come from DuckDB 2.0.0, which agrees with the oracle on all of them.
"""

import datetime
import decimal
import itertools
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pytest

import opteryx


def _rows(sql):
    return [tuple(m[i]) for m in opteryx.session().execute_to_morsels(sql) for i in range(len(m))]


# Nine rows, ids 1..9; every column but `id` is NULL on row 3.
SOURCE = (
    "(SELECT id, NULLIF(id, 3) AS i, CAST(NULLIF(id, 3) AS FLOAT64) AS f, "
    "CAST(NULLIF(id, 3) AS DECIMAL(12,3)) AS d, CAST(NULLIF(id, 3) AS VARCHAR) AS s, "
    "CASE WHEN id = 3 THEN CAST(NULL AS DATE) ELSE CAST('2020-01-0' || CAST(id AS VARCHAR) AS DATE) END AS dt, "
    "CASE WHEN id = 3 THEN CAST(NULL AS TIMESTAMP) "
    "ELSE CAST('2020-01-0' || CAST(id AS VARCHAR) || ' 10:00:00' AS TIMESTAMP) END AS ts, "
    "CASE WHEN id = 3 THEN CAST(NULL AS BOOLEAN) ELSE id > 5 END AS b FROM $planets) AS t"
)


def _value(column, row_id):
    if row_id == 3:
        return None
    return {
        "i": row_id,
        "f": float(row_id),
        "d": decimal.Decimal(row_id),
        "s": str(row_id),
        "dt": datetime.date(2020, 1, row_id),
        "ts": datetime.datetime(2020, 1, row_id, 10),
        "b": row_id > 5,
    }[column]


# column -> (SQL type, [(member SQL, its value)])
COLUMNS = {
    "i": ("INT64", [("1", 1), ("2", 2), ("5", 5)]),
    "f": ("FLOAT64", [("CAST(1 AS FLOAT64)", 1.0), ("CAST(2.0 AS FLOAT64)", 2.0), ("CAST(5 AS FLOAT64)", 5.0)]),
    "d": (
        "DECIMAL(12,3)",
        [(f"CAST('{v}' AS DECIMAL(12,3))", decimal.Decimal(v)) for v in ("1", "2.000", "5")],
    ),
    "s": ("VARCHAR", [("'1'", "1"), ("'2'", "2"), ("'5'", "5")]),
    "dt": ("DATE", [(f"CAST('2020-01-0{v}' AS DATE)", datetime.date(2020, 1, v)) for v in (1, 2, 5)]),
    "ts": (
        "TIMESTAMP",
        [(f"CAST('2020-01-0{v} 10:00:00' AS TIMESTAMP)", datetime.datetime(2020, 1, v, 10)) for v in (1, 2, 5)],
    ),
    "b": ("BOOLEAN", [("TRUE", True), ("FALSE", False)]),
}


def _three_valued_in(operand, members, negated):
    """The standard: a match decides; otherwise any NULL (operand or member) makes it NULL."""
    if operand is None:
        return None
    if any(m is not None and m == operand for m in members):
        result = True
    elif any(m is None for m in members):
        result = None
    else:
        result = False
    return result if result is None or not negated else not result


def _plain(value):
    # Opteryx hands TIMESTAMPs back UTC-aware; the oracle's are naive
    if isinstance(value, datetime.datetime) and value.tzinfo is not None:
        return value.replace(tzinfo=None)
    return value


def _matrix():
    for column, (type_name, members) in COLUMNS.items():
        typed_null = f"CAST(NULL AS {type_name})"
        for count in range(len(members) + 1):
            for chosen in itertools.combinations(members, count):
                for nulls in (("NULL",), (typed_null,), ("NULL", "NULL"), (typed_null, "NULL")):
                    for nulls_first in (False, True):
                        items = list(nulls) + [m[0] for m in chosen] if nulls_first else [m[0] for m in chosen] + list(nulls)
                        values = [None] * len(nulls) + [m[1] for m in chosen]
                        for negated in (False, True):
                            for operand in (f"t.{column}", typed_null, "NULL"):
                                yield column, operand, negated, ", ".join(items), values


MATRIX = list(_matrix())


@pytest.mark.parametrize("column,operand,negated,items,values", MATRIX)
def test_null_member_matrix(column, operand, negated, items, values):
    expression = f"{operand} {'NOT IN' if negated else 'IN'} ({items})"
    expected = {
        row_id: _three_valued_in(_value(column, row_id) if operand.startswith("t.") else None, values, negated)
        for row_id in range(1, 10)
    }

    projected = dict((row_id, _plain(x)) for row_id, x in _rows(f"SELECT id, {expression} AS x FROM {SOURCE}"))
    assert projected == expected, expression

    kept = sorted(row_id for (row_id,) in _rows(f"SELECT id FROM {SOURCE} WHERE {expression}"))
    assert kept == sorted(row_id for row_id, x in expected.items() if x is True), expression


@pytest.mark.parametrize(
    "sql,expected",
    [
        # parquet scan: predicate pushdown and statistics pruning see the rewritten form
        ("SELECT id FROM testdata.planets WHERE id IN (1, 3, NULL)", [(1,), (3,)]),
        ("SELECT id FROM testdata.planets WHERE id NOT IN (1, 3, NULL)", []),
        ("SELECT id FROM testdata.planets WHERE id IN (100, NULL)", []),
        ("SELECT id FROM testdata.planets WHERE name IN ('Earth', NULL) AND id > 1", [(3,)]),
        ("SELECT id FROM testdata.planets WHERE name NOT IN ('Earth', CAST(NULL AS VARCHAR)) OR id = 2", [(2,)]),
        ("SELECT id FROM testdata.planets WHERE id IN (1, 2) OR id IN (3, NULL)", [(1,), (2,), (3,)]),
        # negation and IS tests over the NULL result
        ("SELECT id FROM testdata.planets WHERE NOT (id IN (1, NULL))", []),
        ("SELECT id FROM testdata.planets WHERE NOT (id NOT IN (1, NULL))", [(1,)]),
        ("SELECT id FROM testdata.planets WHERE (id IN (1, NULL)) IS NULL", [(i,) for i in range(2, 10)]),
        ("SELECT id FROM testdata.planets WHERE (id NOT IN (1, NULL)) IS NOT FALSE", [(i,) for i in range(2, 10)]),
        # the OR / AND chains IN abbreviates (the OR->IN rewrite must keep the NULL)
        ("SELECT id FROM testdata.planets WHERE id = 1 OR id = CAST(NULL AS INT64) OR id = 3", [(1,), (3,)]),
        ("SELECT id FROM testdata.planets WHERE id != 1 AND id != CAST(NULL AS INT64) AND id != 3", []),
        # grouping, HAVING, aggregate conditions
        ("SELECT id IN (1, NULL) AS g, COUNT(*) AS c FROM testdata.planets GROUP BY id IN (1, NULL)", [(None, 8), (True, 1)]),
        ("SELECT id % 3 AS k FROM testdata.planets GROUP BY id % 3 HAVING COUNT(*) NOT IN (4, NULL)", []),
        ("SELECT id % 3 AS k FROM testdata.planets GROUP BY id % 3 HAVING COUNT(*) IN (3, NULL)", [(0,), (1,), (2,)]),
        ("SELECT COUNT(* WHERE id IN (1, NULL)), COUNT(* WHERE id NOT IN (1, NULL)) FROM testdata.planets", [(1, 0)]),
        # IN (subquery) whose result holds a NULL
        ("SELECT id FROM testdata.planets WHERE id IN (SELECT NULLIF(id, 2) FROM testdata.planets WHERE id < 4)", [(1,), (3,)]),
        ("SELECT id FROM testdata.planets WHERE id NOT IN (SELECT NULLIF(id, 2) FROM testdata.planets WHERE id < 4)", []),
        # NULLs in the data as well as the list
        ("SELECT COUNT(*) FROM testdata.fuzzing.mixed WHERE i_null IN (984407, 430530, NULL)", [(2,)]),
        ("SELECT COUNT(*) FROM testdata.fuzzing.mixed WHERE i_null NOT IN (984407, 430530, NULL)", [(0,)]),
        ("SELECT COUNT(*) FROM testdata.fuzzing.mixed WHERE i_null NOT IN (984407, 430530)", [(1587,)]),
        # an untyped NULL operand (its IN kernel had no NULL arm: "unsupported vector type")
        ("SELECT 1 IN (1, NULL), 2 IN (1, NULL), NULL IN (1, NULL)", [(True, None, None)]),
        ("SELECT NULL IN (1, 2), NULL NOT IN (1, 2), NULL IN ('a')", [(None, None, None)]),
        ("SELECT COUNT(*) FROM $planets WHERE NULL NOT IN (1, 2)", [(0,)]),
        # every NULL-valued comparison is a BOOLEAN, so it sorts and deduplicates
        ("SELECT DISTINCT 3 IN (NULL) AS x FROM $planets ORDER BY x", [(None,)]),
    ],
)
def test_null_member_scenarios(sql, expected):
    assert sorted(_rows(sql), key=repr) == expected


@pytest.mark.parametrize(
    "condition",
    [
        "id IN (1, CAST(NULL AS VARCHAR))",  # a typed NULL must share the members' type
        "id IN (CAST(NULL AS VARCHAR), CAST(NULL AS INT64))",
        "id IN ('a', NULL)",  # the members still must match the operand
    ],
)
def test_mistyped_lists_are_still_refused(condition):
    with pytest.raises(opteryx.exceptions.SqlError):
        _rows(f"SELECT id FROM testdata.planets WHERE {condition}")


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
