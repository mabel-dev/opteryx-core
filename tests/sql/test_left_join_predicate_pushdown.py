"""WHERE predicates over a LEFT OUTER JOIN: the preserved side's conjuncts move below it.

    FROM a LEFT JOIN b ON a.k = b.k WHERE a.k > 5 AND coalesce(b.v, 0) > 0

kept BOTH conjuncts above the join. The LEFT arm of PredicatePushdown reached one
verdict for the whole collected set: if any predicate named the null-supplying side
(`coalesce(b.v, 0) > 0` has to stay above, or `NULL op literal` would drop the
unmatched rows), every predicate the join could emit was placed above it too,
including the preserved-side-only `a.k > 5`. TPC-DS Q78 is written this way
(`ss_sold_year = 2000 AND (coalesce(ws_qty,0) > 0 OR coalesce(cs_qty,0) > 0)`): all
three channel aggregates ran over every year and the filter kept 207 rows of 2.4M.

Two behaviours are pinned here:

* the verdict is PER PREDICATE: a preserved-side-only conjunct keeps flowing to the
  left leg, a right-side one stays above the join;
* a preserved-side predicate on an ON-equality key is DERIVED onto the null-supplying
  leg (`a.k > 5` with `ON a.k = b.k` gives `b.k > 5`) as a pre-filter. Sound for a LEFT
  join because a right row that cannot match a surviving left row changes nothing: an
  unmatched left row is null-filled whether or not the right row was there.

Expected values are computed in Python from the raw `$planets` rows, not from the
engine. Plan shape is read from EXPLAIN ANALYZE, whose tree is indented by depth.

Run as a script (CLAUDE.md §10) or under pytest.
"""

import os
import sys

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "..", ".."))

import pytest

import opteryx


def rows(statement):
    """Every row of `statement`, as tuples in column order."""
    session = opteryx.session()
    out = []
    for morsel in session.execute_to_morsels(statement):
        morsel.materialize()
        names = list(morsel.column_names)
        out.extend(zip(*(morsel.column(name).to_pylist() for name in names)))
    return out


def plan(statement):
    """The EXPLAIN ANALYZE operator tree as `(depth, operator, details)` in plan order,
    stopping at the OPTIMIZATIONS section."""
    session = opteryx.session()
    out = []
    for morsel in session.execute_to_morsels("EXPLAIN ANALYZE " + statement):
        morsel.materialize()
        trees = [t.decode() if isinstance(t, bytes) else t for t in morsel.column("tree").to_pylist()]
        details = morsel.column("details").to_pylist()
        for tree, detail in zip(trees, details):
            if tree == "OPTIMIZATIONS":
                return out
            body = tree.lstrip(" │├└─")
            out.append((len(tree) - len(body), body, detail or ""))
    return out


PLANETS = rows("SELECT id, mass FROM $planets")  # [(id, mass)], nine planets, ids 1..9


def _filters(operators, text):
    return [i for i, (_, op, details) in enumerate(operators) if op == "Filter" and details == text]


def _join(operators):
    (index,) = [i for i, (_, op, _) in enumerate(operators) if "LEFT OUTER" in op or op.startswith("left_outer")]
    return index


@pytest.mark.parametrize("on", ["a.id = b.id", "b.id = a.id"])
def test_left_conjunct_flows_down_and_right_one_stays_above(on):
    statement = (
        f"SELECT a.id, b.mass FROM $planets AS a LEFT JOIN $planets AS b ON {on} "
        "WHERE a.id > 5 AND coalesce(b.mass, 0) > 0"
    )
    assert sorted(rows(statement)) == sorted((i, m) for i, m in PLANETS if i > 5 and (m or 0) > 0)

    operators = plan(statement)
    join = _join(operators)
    above = [i for i, (_, op, details) in enumerate(operators) if op == "Filter" and "mass" in details]
    assert above and all(i < join for i in above), operators  # the right-side conjunct stays ABOVE the join
    legs = _filters(operators, "id > 5")
    assert len(legs) == 2 and all(i > join for i in legs), operators  # one per leg, both BELOW the join


def test_right_side_predicate_alone_is_not_pushed_or_derived():
    """Only a PRESERVED-side predicate is derived onto the right leg. A WHERE predicate
    that names the right side must stay above the join, and derives nothing."""
    statement = (
        "SELECT a.id, b.mass FROM $planets AS a LEFT JOIN $planets AS b ON a.id = b.id "
        "WHERE b.id > 5"
    )
    assert sorted(rows(statement)) == sorted((i, m) for i, m in PLANETS if i > 5)
    operators = plan(statement)
    join = _join(operators)
    assert not [i for i in _filters(operators, "id > 5") if i > join and operators[i][0] > operators[join][0] + 2], (
        "a right-side WHERE predicate must not be derived onto the left leg", operators)


def test_unmatched_left_rows_survive_the_derived_right_filter():
    """The soundness claim itself: filtering the right leg by the derived predicate must
    not lose left rows that never matched. `b` holds only the even ids, so odd `a` rows
    are null-filled; `a.id > 5` is derived onto b as `b.id > 5`."""
    statement = (
        "SELECT a.id, b.id FROM $planets AS a "
        "LEFT JOIN (SELECT id FROM $planets WHERE id % 2 = 0) AS b ON a.id = b.id "
        "WHERE a.id > 5 AND coalesce(b.id, 0) >= 0"
    )
    expected = [(i, i if i % 2 == 0 else None) for i, _ in PLANETS if i > 5]
    assert sorted(rows(statement), key=lambda r: r[0]) == sorted(expected, key=lambda r: r[0])
    assert any(b is None for _, b in rows(statement)), "the fixture must contain unmatched left rows"


def test_chain_derives_onto_every_null_supplying_leg():
    """Q78's shape: `ss LEFT JOIN ws LEFT JOIN cs WHERE ss.k = c AND (ws.v > 0 OR cs.v > 0)`.
    Each right leg gets the derived pre-filter, the OR stays above the chain."""
    statement = (
        "SELECT a.id FROM $planets AS a "
        "LEFT JOIN $planets AS b ON a.id = b.id "
        "LEFT JOIN $planets AS c ON a.id = c.id "
        "WHERE a.id > 5 AND (coalesce(b.mass, 0) > 100 OR coalesce(c.mass, 0) > 100)"
    )
    assert sorted(rows(statement)) == sorted((i,) for i, m in PLANETS if i > 5 and (m or 0) > 100)

    operators = plan(statement)
    legs = _filters(operators, "id > 5")
    assert len(legs) == 3, operators  # the preserved leg plus one derived filter per null-supplying leg
    (or_filter,) = [i for i, (_, op, details) in enumerate(operators) if op == "Filter" and " OR " in details]
    assert or_filter < min(legs), operators  # the OR stays above every join


def test_non_key_left_predicate_is_not_derived():
    """A preserved-side predicate on a column that is NOT an ON-equality key derives nothing
    onto the right leg (there is no equality to carry it across)."""
    statement = (
        "SELECT a.id FROM $planets AS a LEFT JOIN $planets AS b ON a.id = b.id "
        "WHERE a.mass > 100 AND coalesce(b.mass, 0) > 0"
    )
    assert sorted(rows(statement)) == sorted((i,) for i, m in PLANETS if (m or 0) > 100)
    operators = plan(statement)
    assert len(_filters(operators, "mass > 100")) == 1, operators


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
