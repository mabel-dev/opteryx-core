"""
A typed NULL literal behaves like a column of its type (the BOOLEAN rule, 2026-08-14).

* DECIMAL and DATE/TIME/TIMESTAMP NULLs folded to an UNTYPED null (or, for DECIMAL, a
  runtime CAST with no kernel), so AVG/SUM/MIN/MAX, CASE branches and BETWEEN bounds
  over them were refused. They now fold to a NULL of the declared type.
* `CAST(CAST(NULL AS DATE) AS TIMESTAMP)` ran the DATE->TIMESTAMP value conversion on
  None (TypeError); a typed null cast is a null of the target type.
* `(CAST(NULL AS DECIMAL(12,3)))` and `(CAST(NULL AS TIMESTAMP))` both render `null`, so
  the binder reused the first parenthesised null for the second — wrong type.
* Constant folding read a BOOLEAN NULL as FALSE: `NULL AND FALSE` answered NULL and
  `NULL OR FALSE` answered FALSE.
* Disjunctive domain pushdown took `col < NULL` as a range bound (TypeError).
Found by the reference fuzzer once it emitted typed NULLs; answers checked against DuckDB.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pytest

import opteryx


def _rows(sql):
    return [tuple(m[i]) for m in opteryx.session().execute_to_morsels(sql) for i in range(len(m))]


@pytest.mark.parametrize(
    "sql,expected",
    [
        ("SELECT AVG(CAST(NULL AS DECIMAL(12,3))) FROM $planets", [(None,)]),
        ("SELECT SUM(CAST(NULL AS DECIMAL(12,3))), COUNT(CAST(NULL AS DECIMAL(12,3))) FROM $planets", [(None, 0)]),
        # aliased: an unaliased typed null is named `null` whatever its type
        ("SELECT MIN(CAST(NULL AS TIMESTAMP)) AS a, MAX(CAST(NULL AS DATE)) AS b, MAX(CAST(NULL AS TIME)) AS c FROM $planets", [(None, None, None)]),
        (
            "SELECT COUNT(*) FROM $planets WHERE CASE WHEN id > 3 THEN CAST(id AS DECIMAL(12,3)) "
            "ELSE CAST(NULL AS DECIMAL(12,3)) END IS NULL",
            [(3,)],
        ),
        (
            "SELECT COUNT(*) FROM $planets WHERE CAST('2020-01-01' AS TIMESTAMP) "
            "BETWEEN CAST(NULL AS TIMESTAMP) AND CAST('2021-01-01' AS TIMESTAMP)",
            [(0,)],
        ),
        ("SELECT CAST(CAST(NULL AS DATE) AS TIMESTAMP) AS x", [(None,)]),
        ("SELECT CAST(CAST(NULL AS INT64) AS DECIMAL(12,3)) AS x", [(None,)]),
    ],
)
def test_typed_null_runs(sql, expected):
    assert _rows(sql) == expected


@pytest.mark.parametrize(
    "target,type_name",
    [
        ("DECIMAL(12,3)", "DECIMAL(12, 3)"),
        ("DATE", "DATE"),
        ("TIMESTAMP", "TIMESTAMP[us]"),
        ("TIME", "TIME[us]"),
        ("VARBINARY", "VARBINARY"),
        ("UUID", "UUID"),
        ("IPV4", "IPV4"),
        ("INTERVAL", "INTERVAL"),
    ],
)
def test_typed_null_keeps_its_type(target, type_name):
    morsel = next(iter(opteryx.session().execute_to_morsels(f"SELECT CAST(NULL AS {target}) AS x")))
    assert morsel.column(morsel.column_names[0]).type_name == type_name


@pytest.mark.parametrize(
    "sql,expected",
    [
        # each was refused over "type `NULL`" while the column form runs
        ("SELECT LENGTH(CAST(NULL AS VARBINARY)) AS x", [(None,)]),
        ("SELECT MAX(CAST(NULL AS VARBINARY)) AS x FROM $planets", [(None,)]),
        ("SELECT MIN(CAST(NULL AS UUID)) AS x FROM $planets", [(None,)]),
        ("SELECT MAX(CAST(NULL AS IPV4)) AS x FROM $planets", [(None,)]),
        ("SELECT COUNT(*) FROM $planets WHERE CAST(NULL AS IPV4) <<= '10.0.0.0/8'", [(0,)]),
        ("SELECT COUNT(DISTINCT CAST(NULL AS VARBINARY)) AS x FROM $planets", [(0,)]),
        ("SELECT CAST(NULL AS VARBINARY) = CAST('a' AS VARBINARY) AS x", [(None,)]),
    ],
)
def test_more_typed_nulls_run(sql, expected):
    assert _rows(sql) == expected


def test_date_plus_null_interval_is_a_timestamp():
    morsel = next(iter(opteryx.session().execute_to_morsels(
        "SELECT CAST('2020-01-01' AS DATE) + CAST(NULL AS INTERVAL) AS x")))
    assert morsel.column(morsel.column_names[0]).type_name == "TIMESTAMP[us]"
    assert list(morsel.column(morsel.column_names[0])) == [None]


def test_parenthesised_nulls_of_different_types_stay_apart():
    sql = (
        "SELECT (CAST(NULL AS DECIMAL(12,3))) + 1 AS a, c0 >= (CAST(NULL AS TIMESTAMP)) AS b "
        "FROM (SELECT CAST('2020-01-01' AS TIMESTAMP) AS c0 FROM $planets) AS t0"
    )
    assert _rows(sql) == [(None, None)] * 9


def test_repeated_parenthesised_literal_keeps_both_aliases():
    # the same-typed `(1)` must still be reused, or the second alias is lost
    sql = "SELECT (id + (1)) AS o0, (id + (1)) AS o1 FROM $planets ORDER BY o1"
    assert _rows(sql) == [(i + 1, i + 1) for i in range(1, 10)]


@pytest.mark.parametrize(
    "sql,expected",
    [
        # a comparison with an untyped NULL is a BOOLEAN null, not an untyped one
        ("SELECT DISTINCT 3 = NULL AS x FROM $planets", [(None,)]),
        ("SELECT CAST((id != NULL) AS INT64) AS x FROM $planets", [(None,)] * 9),
        ("SELECT AVG(CAST(2 NOT IN (NULL) AS INT64)) AS x FROM $planets", [(None,)]),
        ("SELECT COUNT(*) FROM $planets WHERE NULL = 3", [(0,)]),
    ],
)
def test_comparison_with_untyped_null_is_boolean(sql, expected):
    assert _rows(sql) == expected


@pytest.mark.parametrize(
    "expression,expected",
    [
        ("CAST(NULL AS BOOLEAN) AND FALSE", False),
        ("FALSE AND CAST(NULL AS BOOLEAN)", False),
        ("CAST(NULL AS BOOLEAN) OR FALSE", None),
        ("FALSE OR CAST(NULL AS BOOLEAN)", None),
        ("CAST(NULL AS BOOLEAN) AND TRUE", None),
        ("TRUE OR CAST(NULL AS BOOLEAN)", True),
        ("(CAST(NULL AS TIMESTAMP) > CAST('2020-01-01' AS TIMESTAMP)) AND b", False),
    ],
)
def test_boolean_null_folds_three_valued(expression, expected):
    sql = f"SELECT {expression} AS x FROM (SELECT FALSE AS b FROM $planets LIMIT 1) AS t0"
    assert _rows(sql) == [(expected,)]


def test_comparison_with_null_is_not_a_domain_bound():
    sql = "SELECT COUNT(*) FROM $planets WHERE (name < CAST(NULL AS VARCHAR) AND id > 1) OR (name > 'M' AND id > 2)"
    assert _rows(sql) == [(5,)]


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
