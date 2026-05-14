"""Parity tests for evaluate_compiled vs evaluate_draken.

Wedge D1 introduces an arena-driven evaluator that walks a flat C++ tree
instead of the source Python Node tree. The two paths must agree on every
boolean orchestration node (AND, OR, NOT, XOR, NESTED, BETWEEN, DNF, CNF).

These tests synthesize Node trees over LITERAL booleans plus a single real
Morsel scanned from $planets so we exercise both the synthetic shape and
realistic row counts. Leaf dispatch (COMPARISON, BINARY, FUNCTION, etc.)
still falls through to evaluate_draken in Wedge D1, so parity at leaves is
guaranteed by construction; we don't reinforce it here.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "..", "..", ".."))

import opteryx
from opteryx.compiled.expression.compiled_expression import lower
from opteryx.expression import NodeType
from opteryx.expression.evaluator import evaluate_compiled, evaluate_draken
from opteryx.models import Node


def _planets_morsel():
    """Return the first morsel of $planets — 9 rows, all base columns."""
    session = opteryx.session()
    for morsel in session.execute_to_morsels("SELECT * FROM $planets"):
        return morsel
    raise AssertionError("$planets returned no morsels")


def _bool_vector_equal(a, b):
    """Compare two BoolVectors row-by-row."""
    assert a.length == b.length, f"length mismatch: {a.length} vs {b.length}"
    for i in range(a.length):
        assert a[i] == b[i], f"row {i}: {a[i]} vs {b[i]}"
    return True


def _parity(node, morsel):
    left = evaluate_draken(node, morsel)
    right = evaluate_compiled(lower(node), morsel)
    _bool_vector_equal(left, right)


def _lit(value):
    return Node(node_type=NodeType.LITERAL, value=value)


def test_literal_true():
    _parity(_lit(True), _planets_morsel())


def test_literal_false():
    _parity(_lit(False), _planets_morsel())


def test_nested():
    _parity(Node(node_type=NodeType.NESTED, centre=_lit(True)), _planets_morsel())


def test_and():
    _parity(
        Node(node_type=NodeType.AND, left=_lit(True), right=_lit(False)),
        _planets_morsel(),
    )


def test_or():
    _parity(
        Node(node_type=NodeType.OR, left=_lit(True), right=_lit(False)),
        _planets_morsel(),
    )


def test_xor():
    _parity(
        Node(node_type=NodeType.XOR, left=_lit(True), right=_lit(True)),
        _planets_morsel(),
    )


def test_not():
    _parity(Node(node_type=NodeType.NOT, centre=_lit(True)), _planets_morsel())


def test_nested_chain():
    n = _lit(True)
    for _ in range(8):
        n = Node(node_type=NodeType.NESTED, centre=n)
    _parity(n, _planets_morsel())


def test_and_of_or():
    tree = Node(
        node_type=NodeType.AND,
        left=Node(node_type=NodeType.OR, left=_lit(True), right=_lit(False)),
        right=Node(node_type=NodeType.OR, left=_lit(False), right=_lit(True)),
    )
    _parity(tree, _planets_morsel())


def test_dnf():
    tree = Node(
        node_type=NodeType.DNF,
        parameters=[_lit(True), _lit(True), _lit(False)],
    )
    _parity(tree, _planets_morsel())


def test_cnf():
    tree = Node(
        node_type=NodeType.CNF,
        parameters=[_lit(False), _lit(False), _lit(True)],
    )
    _parity(tree, _planets_morsel())


def test_dnf_short_circuits_on_zero():
    # First arm is True for every row; DNF logical structure here is an AND-
    # chain, but short-circuits the moment .any() is false. We want parity
    # rather than a specific short-circuit count, so check both paths agree.
    tree = Node(
        node_type=NodeType.DNF,
        parameters=[_lit(True), _lit(False), _lit(True)],
    )
    _parity(tree, _planets_morsel())


def test_cnf_short_circuits_on_all():
    tree = Node(
        node_type=NodeType.CNF,
        parameters=[_lit(True), _lit(False), _lit(False)],
    )
    _parity(tree, _planets_morsel())


def test_between_literal_in_range():
    # BETWEEN packs lower into .right, upper into .centre; value is a tuple
    # of (lower_inclusive, upper_inclusive).
    tree = Node(
        node_type=NodeType.BETWEEN,
        value=(True, True),
        left=_lit(5),
        right=_lit(1),
        centre=_lit(10),
    )
    _parity(tree, _planets_morsel())


def test_between_literal_out_of_range():
    tree = Node(
        node_type=NodeType.BETWEEN,
        value=(True, True),
        left=_lit(50),
        right=_lit(1),
        centre=_lit(10),
    )
    _parity(tree, _planets_morsel())


def test_between_exclusive_bounds():
    tree = Node(
        node_type=NodeType.BETWEEN,
        value=(False, False),
        left=_lit(5),
        right=_lit(5),
        centre=_lit(10),
    )
    _parity(tree, _planets_morsel())


def test_deeply_nested_boolean_tree():
    tree = Node(
        node_type=NodeType.AND,
        left=Node(
            node_type=NodeType.OR,
            left=Node(node_type=NodeType.NOT, centre=_lit(False)),
            right=_lit(True),
        ),
        right=Node(
            node_type=NodeType.XOR,
            left=Node(node_type=NodeType.NESTED, centre=_lit(True)),
            right=_lit(False),
        ),
    )
    _parity(tree, _planets_morsel())


if __name__ == "__main__":
    test_literal_true()
    test_literal_false()
    test_nested()
    test_and()
    test_or()
    test_xor()
    test_not()
    test_nested_chain()
    test_and_of_or()
    test_dnf()
    test_cnf()
    test_dnf_short_circuits_on_zero()
    test_cnf_short_circuits_on_all()
    test_between_literal_in_range()
    test_between_literal_out_of_range()
    test_between_exclusive_bounds()
    test_deeply_nested_boolean_tree()
    print("ok")
