"""`IN (...)` and `BETWEEN ... AND ...` must carry their `AS <alias>`.

Same class of defect as the unary boolean one (test_unary_boolean_alias.py) but in
two more builders: `in_list` and `between` in logical_planner_builders.py both
accepted an `alias` and returned a node without it. The binder names a projection
from the OUTERMOST node (`node.alias or <rendered text>`), so the column was named
after text the user did not write:

    SELECT id IN (1,2) AS x        ->  column 'id IN [1, 2]'
    SELECT id BETWEEN 1 AND 2 AS x ->  column '(id >= 1 AND id <= 2)'

BETWEEN is the worse of the two, and for a reason worth stating: it is LOWERED in the
planner to a pair of comparisons under an AND (or, negated, an OR). There is no
BETWEEN node to render, so the fallback name is the REWRITE — a name that describes
the planner's internals rather than the query. CREATE TABLE ... AS and CREATE
MATERIALIZED VIEW bake the projection's name into the STORED schema, so that rewrite
outlived the query that produced it.

The alias goes on the node the builder RETURNS — for BETWEEN that is the AND/OR at
the top of the lowering, not either comparison inside it. In WHERE (and every other
predicate position) the alias is None and nothing about the lowering changes.
"""

import os
import sys

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import opteryx


def column_names(sql):
    session = opteryx.session()
    for morsel in session.execute_to_morsels(sql):
        return [n.decode("utf-8") if isinstance(n, bytes) else n for n in morsel.column_names]
    return []


def results(sql):
    session = opteryx.session()
    out: dict = {}
    for morsel in session.execute_to_morsels(sql):
        if morsel is None:
            continue
        for key, values in morsel.to_arrow().to_pydict().items():
            out.setdefault(key, []).extend(values)
    return out


@pytest.mark.parametrize(
    "expression",
    [
        "id IN (1, 2)",
        "id NOT IN (1, 2)",
        "name IN ('Earth', 'Mars')",
        "id BETWEEN 1 AND 2",
        "id NOT BETWEEN 1 AND 2",  # the negated lowering, an OR rather than an AND
        "id + 1 BETWEEN 2 AND 3",  # the operand is itself an expression
    ],
)
def test_select_item_keeps_its_alias(expression):
    assert column_names(f"SELECT {expression} AS x FROM $planets") == ["x"]


def test_unaliased_forms_are_named_as_before():
    # The alias is what was missing. The fallback rendering is NOT changed by this
    # fix — including BETWEEN's, which still renders as the lowering it becomes.
    assert column_names("SELECT id IN (1, 2) FROM $planets") == ["id IN [1, 2]"]
    assert column_names("SELECT id BETWEEN 1 AND 2 FROM $planets") == [
        "(id >= 1 AND id <= 2)"
    ]


def test_alias_names_the_operator_not_its_operand():
    # The alias must land on the node the builder returns. If it were pushed down
    # onto `id`, or onto one comparison of the BETWEEN lowering, the values under
    # `x` would be the operand's rather than the predicate's.
    assert results("SELECT id IN (1, 2) AS x FROM $planets ORDER BY id LIMIT 3") == {
        "x": [True, True, False]
    }
    assert results("SELECT id BETWEEN 2 AND 3 AS x FROM $planets ORDER BY id LIMIT 4") == {
        "x": [False, True, True, False]
    }
    assert results("SELECT id NOT BETWEEN 2 AND 3 AS x FROM $planets ORDER BY id LIMIT 4") == {
        "x": [True, False, False, True]
    }


@pytest.mark.parametrize(
    "expression,counts",
    [
        ("id IN (1, 2)", {"x": [False, True], "n": [7, 2]}),
        ("id BETWEEN 1 AND 2", {"x": [False, True], "n": [7, 2]}),
        ("id NOT BETWEEN 1 AND 2", {"x": [False, True], "n": [2, 7]}),
    ],
)
def test_group_by_the_alias(expression, counts):
    # `GROUP BY x` raised ColumnNotFoundError while the alias was being dropped.
    sql = f"SELECT {expression} AS x, COUNT(*) AS n FROM $planets GROUP BY x ORDER BY x"
    assert results(sql) == counts, results(sql)


def test_order_by_the_alias():
    rows = results("SELECT id, id IN (1, 2) AS x FROM $planets ORDER BY x DESC, id LIMIT 3")
    assert rows == {"id": [1, 2, 3], "x": [True, True, False]}, rows

    rows = results("SELECT id, id BETWEEN 4 AND 9 AS x FROM $planets ORDER BY x, id LIMIT 3")
    assert rows == {"id": [1, 2, 3], "x": [False, False, False]}, rows


def test_alias_is_addressable_from_an_enclosing_scope():
    rows = results(
        "SELECT id, x FROM (SELECT id, id BETWEEN 2 AND 3 AS x FROM $planets) AS t "
        "WHERE x IS TRUE ORDER BY id"
    )
    assert rows == {"id": [2, 3], "x": [True, True]}, rows

    rows = results(
        "SELECT id FROM (SELECT id, id IN (5, 6) AS x FROM $planets) AS t "
        "WHERE x IS TRUE ORDER BY id"
    )
    assert rows == {"id": [5, 6]}, rows


@pytest.mark.parametrize(
    "predicate,expected",
    [
        ("id BETWEEN 2 AND 3", ["Venus", "Earth"]),
        ("id NOT BETWEEN 2 AND 8", ["Mercury", "Pluto"]),
        ("id IN (2, 3)", ["Venus", "Earth"]),
        ("id NOT IN (1, 2, 3, 4, 5, 6, 7, 8)", ["Pluto"]),
    ],
)
def test_predicate_positions_are_unaffected(predicate, expected):
    # In WHERE the alias is None, so the lowering is untouched. These are here so
    # that stays true — the fix must be invisible to filtering.
    rows = results(f"SELECT name FROM $planets WHERE {predicate} ORDER BY id")
    assert rows == {"name": expected}, rows


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
