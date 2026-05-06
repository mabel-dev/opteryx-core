"""Tests for the multi-equals hash dispatch fast-path in evaluate_draken.

The fast path collapses `A = k1 AND B = k2 AND ...` into a single combined-hash
comparison when every leaf is an IDENTIFIER = LITERAL on a fixed-width numeric
or boolean column.  The tests below exercise both the eligibility filter and
correctness vs. the regular per-column evaluation path.
"""

import opteryx
import opteryx.expression.evaluator.evaluation as ev


def _run(sql):
    session = opteryx.session()
    morsels = list(session.execute_to_morsels(sql))
    return [list(m.fetchall()) for m in morsels]


def _run_count(sql):
    session = opteryx.session()
    total = 0
    for m in session.execute_to_morsels(sql):
        if m.num_rows == 0:
            continue
        # COUNT(*) result is a single int64 value in the first column.
        col = m.column(m.column_names[0])
        for i in range(m.num_rows):
            total = col[i]
    return total


def _with_fast_path_disabled(fn):
    saved = ev._try_collect_numeric_eq_predicates
    ev._try_collect_numeric_eq_predicates = lambda n: None
    try:
        return fn()
    finally:
        ev._try_collect_numeric_eq_predicates = saved


def test_two_predicate_below_threshold_matches_baseline():
    """2-pred is below the >=3 crossover threshold, so it takes the regular
    per-column path. Verify the result is still correct."""
    sql = (
        "SELECT COUNT(*) FROM testdata.clickbench_tiny "
        "WHERE CounterID = 225510 AND DontCountHits = 0"
    )
    fast = _run_count(sql)
    base = _with_fast_path_disabled(lambda: _run_count(sql))
    assert fast == base


def test_three_predicate_eq_matches_baseline():
    sql = (
        "SELECT COUNT(*) FROM testdata.clickbench_tiny "
        "WHERE CounterID = 225510 AND DontCountHits = 0 AND IsRefresh = 0"
    )
    fast = _run_count(sql)
    base = _with_fast_path_disabled(lambda: _run_count(sql))
    assert fast == base
    assert fast > 0


def test_mixed_string_predicate_falls_through():
    """Including a VARCHAR predicate makes the AND tree ineligible — must take
    the regular per-column path and still return correct results."""
    sql = (
        "SELECT COUNT(*) FROM testdata.clickbench_tiny "
        "WHERE CounterID = 225510 AND DontCountHits = 0 AND URL = 'no-such-url'"
    )
    assert _run_count(sql) == 0


def test_range_predicate_falls_through():
    """A non-equality leaf (>) makes the tree ineligible."""
    sql_eq = (
        "SELECT COUNT(*) FROM testdata.clickbench_tiny "
        "WHERE CounterID = 225510 AND IsRefresh > 0"
    )
    sql_eq_baseline = (
        "SELECT COUNT(*) FROM testdata.clickbench_tiny "
        "WHERE CounterID = 225510 AND IsRefresh = 1"
    )
    assert _run_count(sql_eq) == _run_count(sql_eq_baseline)


def test_or_under_and_falls_through():
    """An OR under the AND chain is not a flat AND tree — takes regular path."""
    sql = (
        "SELECT COUNT(*) FROM testdata.clickbench_tiny "
        "WHERE CounterID = 225510 AND (IsRefresh = 0 OR DontCountHits = 0)"
    )
    base = _with_fast_path_disabled(lambda: _run_count(sql))
    assert _run_count(sql) == base


def test_single_predicate_does_not_use_fast_path():
    """One predicate should be left to the per-column path — verify result still correct."""
    sql = "SELECT COUNT(*) FROM testdata.clickbench_tiny WHERE CounterID = 225510"
    assert _run_count(sql) == 1_000_000


def test_collect_predicates_helper_directly():
    """Direct API check on _try_collect_numeric_eq_predicates: only flat
    AND-of-eligible-equals returns a list, every other shape returns None."""
    from opteryx.expression import NodeType

    class FakeSchemaColumn:
        def __init__(self, name, otype, identity=b"id1"):
            self.name = name
            self.type = otype
            self.identity = identity

    class FakeNode:
        def __init__(self, node_type, **kwargs):
            self.node_type = node_type
            for k, v in kwargs.items():
                setattr(self, k, v)

    from opteryx.types import OrsoTypes

    def lit(val):
        return FakeNode(NodeType.LITERAL, value=val)

    def ident(name, otype):
        return FakeNode(
            NodeType.IDENTIFIER,
            schema_column=FakeSchemaColumn(name, otype),
        )

    def eq(l, r):
        return FakeNode(NodeType.COMPARISON_OPERATOR, value="Eq", left=l, right=r)

    def and_(l, r):
        return FakeNode(NodeType.AND, left=l, right=r)

    # Three-predicate eq on int + int + bool: eligible
    tree = and_(
        and_(eq(ident("a", OrsoTypes.INTEGER), lit(1)),
             eq(ident("b", OrsoTypes.BOOLEAN), lit(True))),
        eq(ident("c", OrsoTypes.INTEGER), lit(7)),
    )
    out = ev._try_collect_numeric_eq_predicates(tree)
    assert out is not None
    assert len(out) == 3

    # Two-predicate eq — below crossover threshold, returns None
    tree = and_(eq(ident("a", OrsoTypes.INTEGER), lit(1)),
                eq(ident("b", OrsoTypes.BOOLEAN), lit(True)))
    assert ev._try_collect_numeric_eq_predicates(tree) is None

    # Single eq — also below threshold
    tree = eq(ident("a", OrsoTypes.INTEGER), lit(1))
    assert ev._try_collect_numeric_eq_predicates(tree) is None

    # Mixed type (varchar) — ineligible
    tree = and_(eq(ident("a", OrsoTypes.INTEGER), lit(1)),
                eq(ident("b", OrsoTypes.VARCHAR), lit("x")))
    assert ev._try_collect_numeric_eq_predicates(tree) is None

    # Float — ineligible (no support for DOUBLE in this implementation)
    tree = and_(eq(ident("a", OrsoTypes.INTEGER), lit(1)),
                eq(ident("b", OrsoTypes.DOUBLE), lit(1.5)))
    assert ev._try_collect_numeric_eq_predicates(tree) is None

    # Non-equality op
    tree = and_(eq(ident("a", OrsoTypes.INTEGER), lit(1)),
                FakeNode(NodeType.COMPARISON_OPERATOR, value="Gt",
                         left=ident("b", OrsoTypes.INTEGER), right=lit(2)))
    assert ev._try_collect_numeric_eq_predicates(tree) is None

    # NULL literal
    tree = and_(eq(ident("a", OrsoTypes.INTEGER), lit(1)),
                eq(ident("b", OrsoTypes.INTEGER), lit(None)))
    assert ev._try_collect_numeric_eq_predicates(tree) is None

    # Reversed leaf shape (literal = identifier) is also accepted
    tree = and_(
        and_(eq(lit(1), ident("a", OrsoTypes.INTEGER)),
             eq(lit(0), ident("b", OrsoTypes.BOOLEAN))),
        eq(lit(2), ident("c", OrsoTypes.INTEGER)),
    )
    out = ev._try_collect_numeric_eq_predicates(tree)
    assert out is not None and len(out) == 3


if __name__ == "__main__":
    import sys
    import pytest

    sys.exit(pytest.main([__file__, "-v"]))
