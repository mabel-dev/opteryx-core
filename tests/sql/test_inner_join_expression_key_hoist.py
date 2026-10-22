"""An expression equality in WHERE keys an INNER join that already has a plain key.

    FROM v, v AS lag WHERE v.k = lag.k AND v.rn = lag.rn + 1

`CrossJoinFilterPushdown` hoists `lag.rn + 1` to a real column and keys the join on it -
but only while the join is a cross join. The plain `v.k = lag.k` makes PredicatePushdown
type the join "inner" first, PredicatePushdown declines the expression side (it cannot key
one), and the equality stayed a Filter over a join keyed on `k` alone. TPC-DS Q47/Q57:
415,641 join rows for the Filter to cut to 29,532; the hand-written equivalent with the
key materialised ran in 43.5 ms against 71.7.

The hoist now runs for keyed INNER joins (not window joins) beneath a Filter as well.
Expected values are computed in Python from the raw `$planets` rows. Plan shape is read
from EXPLAIN; a hoisted key shows as a Projection on the leg and no Filter above the join.

Run as a script (CLAUDE.md §10) or under pytest.
"""

import os
import re
import sys

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "..", ".."))

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
    """The EXPLAIN operator tree as `(operator, details)` in plan order."""
    session = opteryx.session()
    out = []
    for morsel in session.execute_to_morsels("EXPLAIN " + statement):
        morsel.materialize()
        trees = [t.decode() if isinstance(t, bytes) else t for t in morsel.column("tree").to_pylist()]
        for tree, detail in zip(trees, morsel.column("details").to_pylist()):
            if tree == "OPTIMIZATIONS":
                return out
            out.append((tree.lstrip(" │├└─"), detail or ""))
    return out


def filters(statement):
    return [details for op, details in plan(statement) if op == "Filter"]


def hoisted_projections(statement):
    """Projections whose details show a computed key (`id + 3`, `id - 3`)."""
    return [
        details
        for op, details in plan(statement)
        if op == "Projection" and re.search(r"\bid [+-] \d", details)
    ]


T = "(SELECT id, id % 3 AS g FROM $planets)"
IDS = [i for (i,) in rows("SELECT id FROM $planets")]  # 1..9


def test_plain_key_and_expression_key_both_key_the_join():
    statement = f"SELECT a.id, b.id FROM {T} a, {T} b WHERE a.g = b.g AND a.id = b.id + 3"
    assert sorted(rows(statement)) == sorted((i, i - 3) for i in IDS if i - 3 in IDS)
    assert filters(statement) == [], plan(statement)
    assert hoisted_projections(statement), plan(statement)


def test_the_conjunct_order_makes_no_difference():
    statement = f"SELECT a.id, b.id FROM {T} a, {T} b WHERE a.id = b.id + 3 AND a.g = b.g"
    assert sorted(rows(statement)) == sorted((i, i - 3) for i in IDS if i - 3 in IDS)
    assert filters(statement) == [], plan(statement)


def test_the_expression_may_be_on_either_side_of_the_equals():
    statement = f"SELECT a.id, b.id FROM {T} a, {T} b WHERE a.g = b.g AND b.id + 3 = a.id"
    assert sorted(rows(statement)) == sorted((i, i - 3) for i in IDS if i - 3 in IDS)
    assert filters(statement) == [], plan(statement)


def test_the_q57_shape_two_expression_keys_across_a_three_way_chain():
    """`v.rn = lag.rn + 1 AND v.rn = lead.rn - 1` over a chain of three references."""
    statement = (
        f"SELECT v.id, lag.id, lead.id FROM {T} v, {T} lag, {T} lead "
        "WHERE v.g = lag.g AND v.g = lead.g AND v.id = lag.id + 3 AND v.id = lead.id - 3"
    )
    assert sorted(rows(statement)) == sorted(
        (i, i - 3, i + 3) for i in IDS if (i - 3) in IDS and (i + 3) in IDS
    )
    assert filters(statement) == [], plan(statement)
    assert len(hoisted_projections(statement)) == 2, plan(statement)


def test_a_null_expression_operand_never_matches():
    """The hoisted column is NULL where its operand is, and a NULL key matches nothing -
    the answer the Filter gave."""
    keyed = "(SELECT CASE WHEN id % 2 = 0 THEN NULL ELSE id END AS k, id % 3 AS g FROM $planets)"
    statement = f"SELECT a.k, b.k FROM {keyed} a, {keyed} b WHERE a.g = b.g AND a.k = b.k + 6"
    ks = [(i if i % 2 else None, i % 3) for i in IDS]
    expected = sorted(
        (a, b) for a, ga in ks for b, gb in ks if ga == gb and a is not None and b is not None and a == b + 6
    )
    assert sorted(rows(statement)) == expected
    assert expected, "the fixture must contain matches"
    assert filters(statement) == [], plan(statement)


def test_an_equality_that_cannot_be_a_key_stays_in_the_filter():
    """`a.id = b.id + b.g` draws on two columns of one side: not a hoistable shape. It
    stays a Filter, and the answer is unchanged."""
    statement = f"SELECT a.id, b.id FROM {T} a, {T} b WHERE a.g = b.g AND a.id = b.id + b.g"
    ids_g = [(i, i % 3) for i in IDS]
    expected = sorted((a, b) for a, ga in ids_g for b, gb in ids_g if ga == gb and a == b + gb)
    assert sorted(rows(statement)) == expected
    assert filters(statement), plan(statement)


def test_only_the_hoisted_conjunct_leaves_the_filter():
    """A cross-relation inequality has no key to give; it stays, the equality goes."""
    statement = f"SELECT a.id, b.id FROM {T} a, {T} b WHERE a.g = b.g AND a.id = b.id + 3 AND a.id + b.id > 8"
    assert sorted(rows(statement)) == sorted((i, i - 3) for i in IDS if i - 3 in IDS and i + (i - 3) > 8)
    remaining = filters(statement)
    assert len(remaining) == 1 and ">" in remaining[0] and "=" not in remaining[0].replace(">=", ""), plan(statement)


def test_explicit_inner_join_with_the_expression_in_where():
    statement = f"SELECT a.id, b.id FROM {T} a INNER JOIN {T} b ON a.g = b.g WHERE a.id = b.id + 3"
    assert sorted(rows(statement)) == sorted((i, i - 3) for i in IDS if i - 3 in IDS)
    assert filters(statement) == [], plan(statement)


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
