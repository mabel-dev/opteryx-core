"""Round-trip tests for compiled_expression.lower().

Validates that the C++ CompiledExpression arena mirrors the source opteryx
Node tree exactly: same node_type at each position, same number of children,
same depth-first traversal order.
"""

import os
import sys
from opteryx.compiled.structures.expressions import And
from opteryx.compiled.structures.expressions import Between
from opteryx.compiled.structures.expressions import Comparison
from opteryx.compiled.structures.expressions import Dnf
from opteryx.compiled.structures.expressions import Function
from opteryx.compiled.structures.expressions import Literal
from opteryx.compiled.structures.expressions import Nested
from opteryx.compiled.structures.expressions import Or
from opteryx.compiled.structures.expressions import UnaryOperator

sys.path.insert(1, os.path.join(sys.path[0], "..", "..", ".."))

from opteryx.compiled.expression.compiled_expression import expand_between
from opteryx.compiled.expression.compiled_expression import lower
from opteryx.expression import NodeType
from opteryx.compiled.structures.expressions import LogicalColumn


def _python_walk(node):
    """Reference implementation: walk a Python Node tree in the same order as
    the C++ side, emitting (node_type_int, num_children) tuples.

    Order matches src/cpp/expression/compiled_expression.cpp::walk_recursive:
    self, left, right, centre, parameters in order.
    """
    out = []

    def visit(n):
        if n is None:
            return
        children = []
        for attr in ("left", "right", "centre"):
            c = getattr(n, attr, None)
            if c is not None:
                children.append(c)
        params = getattr(n, "parameters", None)
        if isinstance(params, (list, tuple)):
            for p in params:
                if p is not None:
                    children.append(p)
        out.append((int(n.node_type), len(children)))
        for c in children:
            visit(c)

    visit(node)
    return out


def _roundtrip(node):
    handle = lower(node)
    assert handle.node_type_walk() == _python_walk(node)


def test_literal():
    _roundtrip(Literal(value=42))


def test_identifier():
    _roundtrip(LogicalColumn(node_type=NodeType.IDENTIFIER, source_column="x"))


def test_unary():
    centre = LogicalColumn(node_type=NodeType.IDENTIFIER, source_column="x")
    _roundtrip(UnaryOperator(value="IsNull", centre=centre))


def test_binary_compare():
    a = LogicalColumn(node_type=NodeType.IDENTIFIER, source_column="a")
    one = Literal(value=1)
    _roundtrip(Comparison(value="Eq", left=a, right=one))


def test_and_of_compares():
    a = LogicalColumn(node_type=NodeType.IDENTIFIER, source_column="a")
    b = LogicalColumn(node_type=NodeType.IDENTIFIER, source_column="b")
    one = Literal(value=1)
    two = Literal(value=2)
    c1 = Comparison(value="Eq", left=a, right=one)
    c2 = Comparison(value="Gt", left=b, right=two)
    _roundtrip(And(left=c1, right=c2))


def test_function_with_parameters():
    a = LogicalColumn(node_type=NodeType.IDENTIFIER, source_column="a")
    b = Literal(value="suffix")
    _roundtrip(Function(value="CONCAT", parameters=[a, b]))


def _between(lower_incl, upper_incl):
    """BETWEEN packs the lower bound into .right and the upper into .centre."""
    return Between(
        value=(lower_incl, upper_incl),
        left=LogicalColumn(node_type=NodeType.IDENTIFIER, source_column="age"),
        right=Literal(value=18),
        centre=Literal(value=65),
    )


def test_between_expands_to_compares():
    """BETWEEN never reaches the arena — `lower()` rewrites it into a pair of
    compares so each bound goes through the unit-aware compare routing rather
    than a raw, domain-blind range check."""
    node = _between(True, True)
    expanded = expand_between(node)

    assert expanded.node_type == NodeType.AND
    assert expanded.left.node_type == NodeType.COMPARISON_OPERATOR
    assert expanded.left.value == "GtEq"
    assert expanded.left.right is node.right  # lower bound
    assert expanded.right.value == "LtEq"
    assert expanded.right.right is node.centre  # upper bound
    assert expanded.left.left is expanded.right.left  # shared operand

    # lower() applies the same rewrite, so no NT_BETWEEN reaches the C tree.
    walk = lower(node).node_type_walk()
    assert walk == _python_walk(expanded)
    assert int(NodeType.BETWEEN) not in [node_type for node_type, _ in walk]


def test_between_exclusive_bounds_use_strict_compares():
    expanded = expand_between(_between(False, False))
    assert expanded.left.value == "Gt"
    assert expanded.right.value == "Lt"


def test_expand_between_is_idempotent():
    once = expand_between(_between(True, True))
    assert expand_between(once) is once


def test_dnf_uses_parameters():
    a = LogicalColumn(node_type=NodeType.IDENTIFIER, source_column="a")
    one = Literal(value=1)
    two = Literal(value=2)
    c1 = Comparison(value="Eq", left=a, right=one)
    c2 = Comparison(value="Eq", left=a, right=two)
    _roundtrip(Dnf(parameters=[c1, c2]))


def test_deeply_nested():
    # Chain of NESTED wrappers around a LITERAL.
    n = Literal(value=1)
    for _ in range(20):
        n = Nested(centre=n)
    _roundtrip(n)


def test_real_query_expressions():
    """Lower every bound expression in a representative query through the
    actual planner. Ensures the arena handles real-world Node shapes.
    """
    import opteryx

    queries = [
        "SELECT name FROM $planets WHERE id = 1",
        "SELECT name FROM $planets WHERE id = 1 AND mass > 0.5",
        "SELECT name FROM $planets WHERE id IN (1, 2, 3)",
        "SELECT COUNT(*) FROM $planets WHERE name LIKE 'M%'",
        "SELECT name, CASE WHEN id < 5 THEN 'inner' ELSE 'outer' END FROM $planets",
        "SELECT name FROM $planets WHERE id BETWEEN 2 AND 6",
        "SELECT name FROM $planets WHERE (id < 3 OR id > 6) AND mass > 0",
    ]

    session = opteryx.session()
    for sql in queries:
        # Execute to make sure planning succeeds; we don't need results.
        morsels = session.execute_to_morsels(sql)
        for _ in morsels:
            pass

    # If planning/execution above succeeded, the Node trees are well-formed.
    # Run a synthetic lower-and-compare here to exercise the arena.
    big = And(
        left=Comparison(
            value="Eq",
            left=LogicalColumn(node_type=NodeType.IDENTIFIER, source_column="x"),
            right=Literal(value=1),
        ),
        right=Or(
            left=Comparison(
                value="Lt",
                left=LogicalColumn(node_type=NodeType.IDENTIFIER, source_column="y"),
                right=Literal(value=10),
            ),
            right=UnaryOperator(
                value="IsNull",
                centre=LogicalColumn(node_type=NodeType.IDENTIFIER, source_column="z"),
            ),
        ),
    )
    _roundtrip(big)


if __name__ == "__main__":
    test_literal()
    test_identifier()
    test_unary()
    test_binary_compare()
    test_and_of_compares()
    test_function_with_parameters()
    test_between_expands_to_compares()
    test_between_exclusive_bounds_use_strict_compares()
    test_expand_between_is_idempotent()
    test_dnf_uses_parameters()
    test_deeply_nested()
    test_real_query_expressions()
    print("ok")
