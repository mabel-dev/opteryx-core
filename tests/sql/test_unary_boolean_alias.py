"""A unary operator must carry its `AS <alias>`, exactly as a binary one does.

`x IS NULL`, `x IS NOT NULL`, `x IS TRUE`/`IS FALSE`/`IS NOT TRUE`/`IS NOT FALSE`,
`NOT x` and `~x` all build a single-operand node in the logical planner
(`is_compare` and `unary_op` in logical_planner_builders.py). Every one of those
builders ACCEPTED an `alias` and threw it away, so the alias never reached the node.

The binder names a projection from the OUTERMOST node
(`query_column = node.alias or <rendered text>`), so with the alias gone the column
was named after its own SQL text:

    SELECT id IS NOT NULL AS x FROM $planets   ->  column 'id IS NOT NULL'

which is the same failure as the parenthesised-expression defect
(test_parenthesised_expression_alias.py) but from a DIFFERENT cause — the unary node
itself, not the NESTED wrapper. Parenthesising did not help, and the controls
`id > 1 AS x` / `id + 1 AS x` were always correct, because `binary_op` had always
passed the alias through.

It was never only cosmetic. The name was unreferenceable: `GROUP BY x`, `ORDER BY x`
and a reference from an enclosing scope all died with
`ColumnNotFoundError: Column *x* cannot be found`. And CREATE TABLE ... AS /
CREATE MATERIALIZED VIEW bake the projection's name into the STORED schema, so a
name full of spaces and quotes outlived the query that produced it.

The fix threads `alias` onto the node the builders return. The alias belongs to the
UNARY node, never to its operand — putting it on the operand would name the inner
expression and leave the outer one anonymous again.
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
        "id IS NULL",
        "id IS NOT NULL",
        "(id > 4) IS TRUE",
        "(id > 4) IS FALSE",
        "(id > 4) IS NOT TRUE",
        "(id > 4) IS NOT FALSE",
        "NOT (id > 1)",
        "NOT id IS NULL",  # NOT over a unary — both layers must pass the alias up
        "~id",  # BitwiseNot: same builder, same drop
        "(id IS NOT NULL)",  # parenthesised, to show the wrapper was never the cause
    ],
)
def test_unary_select_item_keeps_its_alias(expression):
    assert column_names(f"SELECT {expression} AS x FROM $planets") == ["x"]


def test_binary_equivalents_are_unchanged():
    # The controls: these always worked, and must keep working.
    assert column_names("SELECT id > 1 AS x FROM $planets") == ["x"]
    assert column_names("SELECT id + 1 AS x FROM $planets") == ["x"]
    assert column_names("SELECT -id AS x FROM $planets") == ["x"]
    assert column_names("SELECT id IS DISTINCT FROM 1 AS x FROM $planets") == ["x"]


def test_unaliased_unary_is_still_named_after_its_rendering():
    # The alias is what was missing; the fallback name is not changed by this fix.
    # An unaliased unary keeps rendering as its own SQL text, and two different
    # unaries stay two different columns.
    assert column_names("SELECT id IS NOT NULL FROM $planets") == ["id IS NOT NULL"]
    assert column_names("SELECT NOT (id > 1) FROM $planets") == ["NOT (id > 1)"]
    assert sorted(results("SELECT id IS NULL, id IS NOT NULL FROM $planets").keys()) == [
        "id IS NOT NULL",
        "id IS NULL",
    ]


def test_alias_names_the_unary_not_its_operand():
    # The alias must land on the OUTER node. If it were pushed onto the operand,
    # `id` would answer to `x` and the unary would fall back to a rendered name.
    assert results("SELECT id IS NULL AS x FROM $planets LIMIT 3") == {"x": [False] * 3}
    assert results("SELECT NOT (id > 4) AS x FROM $planets ORDER BY id LIMIT 6") == {
        "x": [True, True, True, True, False, False]
    }


@pytest.mark.parametrize(
    "group_by",
    [
        "x",  # by output alias
        "id IS NOT NULL",  # by the expression
        "1",  # by position
    ],
)
def test_group_by_over_a_unary_alias(group_by):
    # `GROUP BY x` raised ColumnNotFoundError while the alias was being dropped.
    sql = (
        f"SELECT id IS NOT NULL AS x, COUNT(*) AS n FROM $planets GROUP BY {group_by} ORDER BY x"
    )
    assert results(sql) == {"x": [True], "n": [9]}


def test_group_by_over_a_not_alias_splits_the_groups():
    sql = "SELECT NOT (id > 4) AS x, COUNT(*) AS n FROM $planets GROUP BY x ORDER BY x"
    assert results(sql) == {"x": [False, True], "n": [5, 4]}


def test_order_by_a_unary_alias():
    # Also ColumnNotFoundError before the fix.
    rows = results("SELECT id, NOT (id > 4) AS x FROM $planets ORDER BY x, id LIMIT 3")
    assert rows == {"id": [5, 6, 7], "x": [False, False, False]}, rows


def test_alias_is_addressable_from_an_enclosing_scope():
    # The consequence of the bug, not just the name: a downstream reference to the
    # alias resolved against nothing.
    rows = results("SELECT x FROM (SELECT id IS NOT NULL AS x FROM $planets) AS t LIMIT 2")
    assert rows == {"x": [True, True]}, rows

    rows = results(
        "SELECT id, x FROM (SELECT id, NOT (id > 4) AS x FROM $planets) AS t "
        "WHERE x IS TRUE ORDER BY id"
    )
    assert rows == {"id": [1, 2, 3, 4], "x": [True] * 4}, rows


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
