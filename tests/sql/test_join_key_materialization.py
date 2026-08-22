"""An ON-clause equality whose operand is an EXPRESSION is an equi-join.

    FROM flows f INNER JOIN lookups l ON CAST(f.src_addr AS VARCHAR) = l.client

used to be refused outright ("JOIN conditions only support column comparisons"),
and the workaround was to hoist the expression into a CTE by hand so the ON clause
saw a plain column. `JoinKeyMaterializationStrategy` now performs that rewrite:
the expression is projected as a real column on the leg it belongs to, and the join
keys on the projected column.

WHAT THIS FILE PINS

The rewrite moves an expression to a different evaluation site, so the only thing
that makes it safe is that it answers IDENTICALLY. Every correctness test here
compares the ON form against the HAND-WRITTEN CTE rewrite — the plan a user would
have had to write — row for row, not by count.

The motivating case is a key carrying different types on the two sides: an IPV4
address column against a VARCHAR column that holds hostnames as well as addresses,
where the cast is the point of the query and cannot be optimised away.

The refusals matter as much as the rewrites. Four shapes are still not join keys,
for four different reasons, and each says which: an expression drawing on BOTH
relations, two operands drawing on the SAME one, a non-deterministic expression,
and an aggregate. A message naming the wrong reason sends the user to rewrite the
half of their query that was fine.
"""

import os
import sys

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import opteryx
from opteryx.exceptions import UnsupportedSyntaxError

# The motivating shape. `src_addr` is IPV4; `client` is VARCHAR holding BOTH
# addresses and hostnames, which is why the column cannot simply be typed IPV4 and
# why the cast has to happen in the join. 'vault.internal' is the row that makes
# the two columns genuinely different types rather than the same one spelled twice.
FLOWS = """(SELECT CAST(addr AS IPV4) AS src_addr, bytes FROM (VALUES
    ('10.0.0.1', 100),
    ('10.0.0.2', 200),
    ('10.0.0.3', 300)
) AS vf(addr, bytes)) AS f"""

LOOKUPS = """(SELECT host AS client, qname FROM (VALUES
    ('10.0.0.1', 'alpha'),
    ('vault.internal', 'beta'),
    ('10.0.0.3', 'gamma')
) AS vl(host, qname)) AS l"""


def rows(sql):
    """Every row, in order, as tuples — row-for-row comparison, not a count."""
    session = opteryx.session()
    out = []
    for morsel in session.execute_to_morsels(sql):
        if morsel is None:
            continue
        table = morsel.to_arrow().to_pydict()
        out.extend(zip(*(table[name] for name in table)))
    return out


def explain(sql):
    return [
        (r[0].decode() if isinstance(r[0], bytes) else r[0], r[1]) for r in rows("EXPLAIN " + sql)
    ]


# ── correctness: the ON form answers what the CTE rewrite answers ─────────────

# `ON <cast> = <column>`: one operand is an expression, the other a plain column.
MIXED_TYPE_ON = f"""
SELECT f.bytes, l.qname
  FROM {FLOWS} INNER JOIN {LOOKUPS} ON CAST(f.src_addr AS VARCHAR) = l.client
 ORDER BY l.qname
"""

MIXED_TYPE_CTE = f"""
WITH keyed AS (SELECT bytes, CAST(src_addr AS VARCHAR) AS join_key FROM {FLOWS})
SELECT keyed.bytes, l.qname
  FROM keyed INNER JOIN {LOOKUPS} ON keyed.join_key = l.client
 ORDER BY l.qname
"""


def test_mixed_type_ipv4_varchar_join():
    # The whole point of the feature: without the cast these two columns cannot be
    # compared at all, and the cast used to make the join unplannable.
    assert rows(MIXED_TYPE_ON) == [(100, "alpha"), (300, "gamma")], rows(MIXED_TYPE_ON)


def test_mixed_type_join_matches_the_cte_rewrite():
    assert rows(MIXED_TYPE_ON) == rows(MIXED_TYPE_CTE)


def test_uncast_mixed_type_join_is_still_a_type_error():
    # The cast is load-bearing. Removing it must NOT quietly start working just
    # because expression operands are now accepted — IPV4 and VARCHAR still do not
    # compare, and that refusal is what tells the user to write the cast.
    from opteryx.exceptions import IncompatibleTypesError

    with pytest.raises(IncompatibleTypesError):
        rows(
            f"SELECT f.bytes FROM {FLOWS} INNER JOIN {LOOKUPS} ON f.src_addr = l.client"
        )


@pytest.mark.parametrize(
    "on_clause, left_key",
    [
        # a CAST on one side, against a plain column on the other
        ("CAST(p.id AS VARCHAR) = q.name", "CAST(id AS VARCHAR)"),
        # an arithmetic expression on one side, in both orientations — whether an
        # operand is an expression must not depend on which side of the `=` it sits
        ("p.id + 1 = q.id", "id + 1"),
        ("q.id = p.id + 1", "id + 1"),
        # a function call, which the pre-existing arithmetic hoist explicitly
        # refused ("never a function call")
        ("LENGTH(p.name) = q.id", "LENGTH(name)"),
    ],
)
def test_one_sided_expression_matches_the_cte_rewrite(on_clause, left_key):
    # The right-hand operand is always a plain column of `q`, so the CTE rewrite
    # differs from the ON form ONLY in where the left key is computed.
    right_column = "q.name" if "q.name" in on_clause else "q.id"
    on_form = f"""
    SELECT p.name, q.name FROM $planets p INNER JOIN $planets q ON {on_clause}
     ORDER BY p.name, q.name
    """
    cte_form = f"""
    WITH keyed AS (SELECT name, {left_key} AS join_key FROM $planets)
    SELECT keyed.name, q.name FROM keyed INNER JOIN $planets q
        ON keyed.join_key = {right_column}
     ORDER BY keyed.name, q.name
    """
    assert rows(on_form) == rows(cte_form), on_clause
    # A rewrite that answered nothing would agree with a broken oracle, so pin
    # that these shapes actually match rows.
    if left_key != "CAST(id AS VARCHAR)":
        assert rows(on_form), on_clause


def test_expressions_on_both_sides_matches_the_cte_rewrite():
    # BOTH operands are expressions, and of different KINDS, so one leg cannot be
    # standing in for the other by accident.
    on_form = """
    SELECT p.name, q.name FROM $planets p INNER JOIN $planets q
        ON CAST(p.id AS VARCHAR) = CAST(q.id * 1 AS VARCHAR)
     ORDER BY p.name, q.name
    """
    cte_form = """
    WITH l AS (SELECT name, CAST(id AS VARCHAR) AS join_key FROM $planets),
         r AS (SELECT name, CAST(id * 1 AS VARCHAR) AS join_key FROM $planets)
    SELECT l.name, r.name FROM l INNER JOIN r ON l.join_key = r.join_key
     ORDER BY l.name, r.name
    """
    assert rows(on_form) == rows(cte_form)


def test_expression_key_beside_a_plain_equi_key():
    # A mixed ON: one conjunct already a key, one needing materialisation. The
    # plain conjunct must survive the rewrite of its neighbour.
    on_form = """
    SELECT p.name FROM $planets p INNER JOIN $planets q
        ON p.id = q.id AND CAST(p.id AS VARCHAR) = CAST(q.id AS VARCHAR)
     ORDER BY p.name
    """
    assert rows(on_form) == rows(
        "SELECT p.name FROM $planets p INNER JOIN $planets q ON p.id = q.id ORDER BY p.name"
    )


def test_outer_join_preserves_unmatched_rows():
    # An outer join's preserved rows are the shape a hoist gets wrong by pushing
    # its Project onto the wrong leg, or by turning the join inner.
    on_form = """
    SELECT p.name, q.name FROM $planets p LEFT OUTER JOIN $planets q ON p.id + 1 = q.id
     ORDER BY p.name
    """
    cte_form = """
    WITH keyed AS (SELECT name, id + 1 AS join_key FROM $planets)
    SELECT keyed.name, q.name FROM keyed LEFT OUTER JOIN $planets q ON keyed.join_key = q.id
     ORDER BY keyed.name
    """
    result = rows(on_form)
    assert result == rows(cte_form)
    # $planets has 9 rows and `id + 1` matches 8 of them, so exactly one preserved
    # row must come back unmatched. A join that silently went inner loses it.
    assert sum(1 for _name, matched in result if matched is None) == 1, result


# ── plan shape: the expression really becomes a column, and the join is keyed ──


def test_the_expression_is_projected_below_the_join():
    plan = explain(MIXED_TYPE_ON)
    trees = [tree for tree, _detail in plan]
    joined = " | ".join(trees)

    # A KEYED inner join. The two failure modes this rules out are the join
    # degenerating to a cartesian product with the condition left hanging above it
    # as a Filter, and it being re-typed as a per-pair nested loop.
    assert "Inner Join" in joined, plan
    assert "Cross Join" not in joined, plan
    assert "Nested Loop" not in joined, plan

    # The cast is evaluated by a Projection sitting BELOW the join — that Project
    # is what produces the identity the join keys on, and its absence is the
    # "scan asked for a column it never emits" failure.
    join_at = next(i for i, tree in enumerate(trees) if "Inner Join" in tree)
    below = trees[join_at + 1 :]
    assert any("Projection" in tree for tree in below), plan

    # And the rewrite is attributable: the trace names the strategy that did it,
    # so a plan that came out right for some other reason does not read as a pass.
    assert "JoinKeyMaterializationStra" in joined, plan


# ── refusals: four different reasons, each named ──────────────────────────────


@pytest.mark.parametrize(
    "on_clause, expected_phrase",
    [
        # Draws on BOTH relations: no single leg can compute it, so no projection
        # rescues it. It is a theta condition, and WHERE is where it goes.
        ("p.id + q.id = 5", "draws on both"),
        # Both operands resolve to the SAME leg — hoistable twice over and still
        # not a join key; it is a filter.
        ("CAST(p.id AS VARCHAR) = CAST(p.name AS VARCHAR)", "come from the same relation"),
        # Not deterministic: projecting it changes evaluation from once-per-pair to
        # once-per-row, and a key that reads differently each time is not a key.
        ("p.id + RANDOM() = q.id", "same every time"),
        # No per-row value at all.
        ("SUM(p.id) = q.id", "no per-row value"),
    ],
)
def test_unhoistable_operands_are_refused_by_reason(on_clause, expected_phrase):
    with pytest.raises(UnsupportedSyntaxError) as raised:
        rows(f"SELECT p.name FROM $planets p INNER JOIN $planets q ON {on_clause}")
    assert expected_phrase in str(raised.value), str(raised.value)


if __name__ == "__main__":  # pragma: no cover
    import pytest as _pytest

    raise SystemExit(_pytest.main([__file__, "-q"]))
