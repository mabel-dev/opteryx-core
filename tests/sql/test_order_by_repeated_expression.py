"""Repeating an expression in SELECT and ORDER BY must not recompute it.

    SELECT id + 1 AS u FROM $planets ORDER BY id + 1
    -> ValueError: compiled_expression: IDENTIFIER node missing schema_column

`ORDER BY <alias>` and `ORDER BY <position>` always worked; spelling the
expression out a second time did not, for any expression — arithmetic, a
function call, a CASE — aliased or not.

The chain: an expression's rendering is its identity, so the ORDER BY copy
resolves to the SAME schema column the projection already computes. The binder
handles that through its "early exit for calculated columns" path — it points
the node at the existing column and deliberately STOPS, leaving the children
unbound, because nothing is going to evaluate them.

The compiler then disagreed. `_sort_spec` picked what to compute by NODE TYPE
(anything not an IDENTIFIER), so the key went to `_add_computed` even though the
stream already carried it. `_add_computed` does have an "identity already in
layout -> skip" branch, but it sat AFTER `compile_eval_nodes` had already lowered
and built bytecode for every node — and building bytecode for a deliberately
half-bound node walks into an IDENTIFIER with no schema_column and dies.

The fix moves that skip ahead of the build, so a column the stream already
carries is read rather than recomputed. The descriptor/type-collision guard that
lived in the old branch moves with it, since it is the thing that keeps the skip
from being a wrong answer.

The assertions are on VALUES, not just on "it ran": the failure mode this guards
against is an ORDER BY key silently resolving to the wrong column, which would
still return rows.
"""

import os
import sys

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import opteryx


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
    "sql,expected",
    [
        # $planets.id is 1..9, so `id + 1` ascending starts 2, 3, 4.
        ("SELECT id + 1 AS u FROM $planets ORDER BY id + 1 LIMIT 3", {"u": [2, 3, 4]}),
        ("SELECT id + 1 AS u FROM $planets ORDER BY id + 1 DESC LIMIT 3", {"u": [10, 9, 8]}),
        # Unaliased — the expression names itself, and is still one column.
        ("SELECT id + 1 FROM $planets ORDER BY id + 1 LIMIT 3", {"id + 1": [2, 3, 4]}),
        # A function call, not just arithmetic.
        (
            "SELECT UPPER(name) AS u FROM $planets ORDER BY UPPER(name) LIMIT 3",
            {"u": ["EARTH", "JUPITER", "MARS"]},
        ),
        (
            "SELECT LENGTH(name) AS l FROM $planets ORDER BY LENGTH(name), name LIMIT 4",
            {"l": [4, 5, 5, 5]},
        ),
        # The same expression twice in the SELECT and once in ORDER BY.
        (
            "SELECT id + 1 AS u, id + 1 AS v FROM $planets ORDER BY id + 1 LIMIT 2",
            {"u": [2, 3], "v": [2, 3]},
        ),
        # Repeated under a GROUP BY: ids 1..9 give three of each residue.
        (
            "SELECT id % 3 AS m, COUNT(*) AS n FROM $planets GROUP BY id % 3 ORDER BY id % 3",
            {"m": [0, 1, 2], "n": [3, 3, 3]},
        ),
        ("SELECT DISTINCT id % 3 AS m FROM $planets ORDER BY id % 3", {"m": [0, 1, 2]}),
        # The key is computed below a filter, so the sort must read the survivors'
        # values, not recompute from a column the projection dropped.
        (
            "SELECT id * 2 AS d FROM $planets WHERE id > 2 ORDER BY id * 2 LIMIT 3",
            {"d": [6, 8, 10]},
        ),
        # Parenthesised spelling — the wrapper is stripped at both clause tops, so
        # this lands on the same column and takes the same path.
        ("SELECT (id + 1) AS u FROM $planets ORDER BY (id + 1) LIMIT 3", {"u": [2, 3, 4]}),
    ],
)
def test_expression_repeated_in_select_and_order_by(sql, expected):
    assert results(sql) == expected


def test_alias_and_position_still_work():
    """The two spellings that always worked — they route through the same code."""
    assert results("SELECT id + 1 AS u FROM $planets ORDER BY u LIMIT 3") == {"u": [2, 3, 4]}
    assert results("SELECT id + 1 AS u FROM $planets ORDER BY 1 LIMIT 3") == {"u": [2, 3, 4]}


def test_order_by_expression_not_in_the_projection_still_computes():
    """The other side of the fix: a key the stream does NOT carry must still be
    computed. Skipping on identity is only correct when the identity is present."""
    assert results("SELECT name FROM $planets ORDER BY id + 1 LIMIT 3") == {
        "name": ["Mercury", "Venus", "Earth"]
    }
    assert results("SELECT name FROM $planets ORDER BY id + 1 DESC LIMIT 3") == {
        "name": ["Pluto", "Neptune", "Uranus"]
    }


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
