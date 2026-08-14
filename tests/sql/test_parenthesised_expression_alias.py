"""A parenthesised clause expression must behave exactly like the unparenthesised one.

Parentheses parse to a `Nested` AST node, which the logical planner builds into a
`NodeType.NESTED` wrapper. That wrapper is part of the expression's RENDERING, and
the rendering IS its identity — the binder resolves an expression by looking its
rendering up in the schemas. So `(expr)` was a different column from `expr`, which
broke two things:

  1. The alias. The binder names a projection from the OUTERMOST node
     (`query_column = node.alias or <rendered text>`), and `nested()` accepted an
     `alias` and dropped it, so every parenthesised select item was named after its
     own SQL text — a name containing spaces, parens and quotes, unreferenceable
     downstream. CREATE TABLE ... AS / CREATE MATERIALIZED VIEW baked that text
     into the stored schema, so it outlived the query that caused it.

  2. GROUP BY. The projection kept the wrapper while the optimizer stripped it off
     the group key (constant_folding.py), so the aggregate emitted one identity and
     the projection asked for another; the compiler then tried to recompute the
     projection from a base column the aggregate no longer carried, and the query
     died with an internal KeyError about a column "the stream does not carry".

The fix strips the wrapper at the TOP of a clause expression, before binding
(`_strip_outer_nesting` in logical_planner.py). Only at the top — see
`test_inner_parentheses_still_disambiguate` below for what stops it going further.

Nothing here is CASE-specific; the wrapper is what mattered, so the whole
expression family is covered.
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
        "(CASE WHEN id > 4 THEN 1 ELSE 0 END)",
        "(CASE WHEN id > 4 THEN id - 4 ELSE 0 END)",
        "(id + 1)",
        "(CEILING(gravity))",
        "(id)",
        "(1)",
        "((id + 1))",  # the wrapper nests, and every layer must pass the alias up
        "(CAST(id AS VARCHAR))",
    ],
)
def test_parenthesised_select_item_keeps_its_alias(expression):
    assert column_names(f"SELECT {expression} AS big FROM $planets") == ["big"]


def test_unparenthesised_equivalents_are_unchanged():
    # The control: these always worked, and must keep working.
    assert column_names("SELECT CASE WHEN id > 4 THEN 1 ELSE 0 END AS big FROM $planets") == [
        "big"
    ]
    assert column_names("SELECT id + 1 AS big FROM $planets") == ["big"]
    assert column_names("SELECT CEILING(gravity) AS big FROM $planets") == ["big"]


def test_unaliased_parenthesised_expression_is_named_like_the_bare_one():
    # A redundant outer paren is not part of the expression, so it is not part of
    # the name either. `(id + 1)` and `id + 1` are the same expression and now
    # answer to the same name — which is the whole point: they are one column.
    assert column_names("SELECT (id + 1) FROM $planets") == ["id + 1"]
    assert column_names("SELECT (id) FROM $planets") == ["id"]
    assert column_names("SELECT (CASE WHEN id > 4 THEN 1 ELSE 0 END) FROM $planets") == [
        "CASE WHEN id > 4 THEN 1 ELSE 0 END"
    ]


@pytest.mark.parametrize(
    "group_by",
    [
        "u",  # by output alias
        "(id + 1)",  # by the parenthesised expression
        "id + 1",  # by the bare expression — only the SELECT side is wrapped
        "1",  # by position
    ],
)
def test_group_by_matches_a_parenthesised_select_item(group_by):
    # Each of these planned an aggregate emitting one identity under a projection
    # asking for another, and died in the compiler with
    #   KeyError: expression references column b'$pl_id_...' which the stream does
    #   not carry
    # The parenthesised form must agree with the bare form on names AND values.
    parens = f"SELECT (id + 1) AS u, COUNT(*) AS n FROM $planets GROUP BY {group_by} ORDER BY u"
    bare = f"SELECT id + 1 AS u, COUNT(*) AS n FROM $planets GROUP BY {group_by} ORDER BY u"
    assert results(parens) == results(bare)


def test_group_by_matches_a_parenthesised_case():
    # The shape that was reported, with a GROUP BY over it.
    parens = (
        "SELECT (CASE WHEN id > 4 THEN 1 ELSE 0 END) AS big, COUNT(*) AS n "
        "FROM $planets GROUP BY big ORDER BY big"
    )
    bare = (
        "SELECT CASE WHEN id > 4 THEN 1 ELSE 0 END AS big, COUNT(*) AS n "
        "FROM $planets GROUP BY big ORDER BY big"
    )
    assert results(parens) == {"big": [0, 1], "n": [4, 5]}
    assert results(parens) == results(bare)


@pytest.mark.parametrize(
    "plain,parens",
    [
        # ORDER BY — raised the compiler's "stream does not carry" KeyError, and
        # `ORDER BY (id)` on its own raised "an ORDER BY key the engine could not
        # resolve here is not supported".
        (
            "SELECT name FROM $planets ORDER BY id LIMIT 3",
            "SELECT name FROM $planets ORDER BY (id) LIMIT 3",
        ),
        (
            "SELECT name FROM $planets ORDER BY id + 1 LIMIT 3",
            "SELECT name FROM $planets ORDER BY (id + 1) LIMIT 3",
        ),
        (
            "SELECT name FROM $planets ORDER BY id + 1 DESC LIMIT 3",
            "SELECT name FROM $planets ORDER BY (id + 1) DESC LIMIT 3",
        ),
        # A window's PARTITION BY raised `'NoneType' object has no attribute 'lower'`.
        (
            "SELECT name, SUM(mass) OVER(PARTITION BY id) AS s FROM $planets ORDER BY name LIMIT 3",
            "SELECT name, SUM(mass) OVER(PARTITION BY (id)) AS s FROM $planets ORDER BY name LIMIT 3",
        ),
        # A window's own ORDER BY, which is built separately from the statement's.
        (
            "SELECT name, ROW_NUMBER() OVER(ORDER BY id) AS r FROM $planets ORDER BY name LIMIT 3",
            "SELECT name, ROW_NUMBER() OVER(ORDER BY (id)) AS r FROM $planets ORDER BY name LIMIT 3",
        ),
        # JOIN ... ON reported "INNER JOIN has no valid conditions, did you mean
        # CROSS JOIN?" — the condition was there, it just was not the shape the
        # key extraction looked for.
        (
            "SELECT p.name FROM $planets p JOIN $planets q ON p.id = q.id ORDER BY p.name LIMIT 3",
            "SELECT p.name FROM $planets p JOIN $planets q ON (p.id = q.id) ORDER BY p.name LIMIT 3",
        ),
        (
            "SELECT p.name FROM $planets p JOIN $planets q ON p.id = q.id AND p.id > 3 ORDER BY p.name LIMIT 2",
            "SELECT p.name FROM $planets p JOIN $planets q ON (p.id = q.id AND p.id > 3) ORDER BY p.name LIMIT 2",
        ),
        # DISTINCT ON over an expression.
        (
            "SELECT DISTINCT ON (id % 2) id % 2 AS m, name FROM $planets ORDER BY m",
            "SELECT DISTINCT ON ((id % 2)) (id % 2) AS m, name FROM $planets ORDER BY m",
        ),
        # WHERE, HAVING and QUALIFY already tolerated the wrapper — they are here
        # so they keep doing so.
        (
            "SELECT name FROM $planets WHERE id + 1 > 4 ORDER BY name LIMIT 3",
            "SELECT name FROM $planets WHERE (id + 1) > 4 ORDER BY name LIMIT 3",
        ),
        (
            "SELECT name FROM $planets WHERE id > 4 AND id < 7 ORDER BY name",
            "SELECT name FROM $planets WHERE (id > 4 AND id < 7) ORDER BY name",
        ),
        (
            "SELECT id % 3 AS m, COUNT(*) AS n FROM $planets GROUP BY id % 3 HAVING COUNT(*) > 2 ORDER BY m",
            "SELECT (id % 3) AS m, COUNT(*) AS n FROM $planets GROUP BY (id % 3) HAVING (COUNT(*) > 2) ORDER BY m",
        ),
        # QUALIFY names the window inline — `QUALIFY <select alias>` is refused by
        # this engine, with or without parentheses, and that is not this test's
        # business.
        (
            "SELECT name FROM $planets QUALIFY ROW_NUMBER() OVER(ORDER BY id) < 3",
            "SELECT name FROM $planets QUALIFY (ROW_NUMBER() OVER(ORDER BY id) < 3)",
        ),
    ],
)
def test_every_clause_top_ignores_a_redundant_paren(plain, parens):
    """A redundant outer paren must be invisible in EVERY clause, not just SELECT.

    Each pair is (unparenthesised, parenthesised) spellings of one query. They must
    agree on names AND values — an assertion that the parenthesised form merely
    "runs" would pass on a wrong answer.

    Every query orders its output, because comparing two unordered results is
    comparing row order the engine never promised.

    `results()` does not swallow exceptions, so a pair that BOTH fail cannot pass
    as "they agree" — and the rows are required to be non-empty, so a pair that
    both return nothing cannot either. Equality alone would be green on both.
    """
    rows = results(parens)
    assert rows and all(len(v) for v in rows.values()), rows
    assert rows == results(plain)


def test_inner_parentheses_still_disambiguate():
    """The guard on the fix above — this is why only the OUTERMOST wrapper goes.

    `BINARY_OPERATOR` renders without parentheses of its own, so the NESTED
    wrapper is the ONLY thing telling `(id + 2) * 3` apart from `id + (2 * 3)`.
    An expression's rendering is its identity, so if inner wrappers were stripped
    too these two would render alike, resolve to one schema column, and one of
    them would be served the other's values. That is a wrong answer, not a naming
    wrinkle. Both the names and the values are asserted.
    """
    rows = results("SELECT (id + 2) * 3 AS x, id + (2 * 3) AS y FROM $planets ORDER BY id LIMIT 2")
    assert rows == {"x": [9, 12], "y": [7, 8]}, rows

    named = results("SELECT (id + 2) * 3, id + (2 * 3) FROM $planets ORDER BY id LIMIT 2")
    assert sorted(named.keys()) == ["(id + 2) * 3", "id + (2 * 3)"], sorted(named.keys())
    assert named["(id + 2) * 3"] == [9, 12]
    assert named["id + (2 * 3)"] == [7, 8]


def test_alias_is_addressable_from_an_enclosing_scope():
    # The consequence of the bug, not just the name: a downstream reference to
    # the alias resolved against nothing.
    assert column_names("SELECT big FROM (SELECT (id + 1) AS big FROM $planets) AS t") == [
        "big"
    ]
    assert column_names("SELECT (id + 1) AS big FROM $planets ORDER BY big") == ["big"]


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
