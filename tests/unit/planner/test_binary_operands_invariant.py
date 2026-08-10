# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""`binary_operands` — the enforced form of "a binary node carries both operands".

The planner is full of `condition.left` / `condition.right` reads. Each was safe
only because a COMPARISON_OPERATOR happens to always carry both sides — an
invariant nothing asserted. One site (the CROSS JOIN UNNEST branch of predicate
pushdown) gated on `.centre` instead of on the node type, and an anchored
`LIKE '%x'` — which PredicateRewriteStrategy lowers to a bare `_ENDS_WITH`
FUNCTION node with left/right/centre ALL None — took it down with

    AttributeError: 'NoneType' object has no attribute 'schema_column'

`binary_operands` exists to turn that class of failure into a named error at the
point of the malformed access. These tests pin the two things that make it worth
having: it REJECTS a node type with no operand pair, and it REJECTS a binary node
that is missing a side.

Run as a script (CLAUDE.md §10) or under pytest.
"""

import os
import sys

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "..", "..", ".."))

from opteryx.exceptions import InvalidInternalStateError
from opteryx.expression import BINARY_NODE_TYPES, NodeType, binary_operands
from opteryx.models import Node


def _literal(value):
    return Node(node_type=NodeType.LITERAL, value=value)


def _binary(node_type, left=True, right=True):
    node = Node(node_type=node_type, value="Eq", do_not_create_column=True)
    if left:
        node.left = _literal(1)
    if right:
        node.right = _literal(2)
    return node


def test_returns_both_operands():
    left, right = binary_operands(_binary(NodeType.COMPARISON_OPERATOR))
    assert (left.value, right.value) == (1, 2)


def test_every_declared_binary_type_is_accepted():
    """The frozenset is the contract — each member must actually work."""
    for node_type in BINARY_NODE_TYPES:
        left, right = binary_operands(_binary(node_type))
        assert (left.value, right.value) == (1, 2), node_type


def test_rejects_node_types_with_no_operand_pair():
    """The shapes that actually reach the planner with no left/right.

    FUNCTION is the one that caused the original crash (an anchored LIKE lowers
    to `_ENDS_WITH`); NOT and UNARY_OPERATOR carry their operand on `.centre`.
    """
    for node_type in (
        NodeType.FUNCTION,
        NodeType.NOT,
        NodeType.UNARY_OPERATOR,
        NodeType.IDENTIFIER,
        NodeType.LITERAL,
        NodeType.CAST,
        NodeType.NESTED,
        NodeType.DNF,
        NodeType.CNF,
        NodeType.CASE,
        NodeType.SUBQUERY,
        NodeType.AGGREGATOR,
        NodeType.WILDCARD,
    ):
        node = Node(node_type=node_type, value="x", do_not_create_column=True)
        try:
            binary_operands(node)
        except InvalidInternalStateError as err:
            assert "not binary" in str(err), (node_type, str(err))
            continue
        raise AssertionError(f"binary_operands accepted a {node_type} node")


def test_rejects_a_binary_node_missing_a_side():
    """A malformed tree must fail HERE, named, not as an AttributeError elsewhere."""
    for missing, kwargs in (("left", {"left": False}), ("right", {"right": False})):
        node = _binary(NodeType.COMPARISON_OPERATOR, **kwargs)
        try:
            binary_operands(node)
        except InvalidInternalStateError as err:
            assert missing in str(err), (missing, str(err))
            continue
        raise AssertionError(f"binary_operands accepted a node with no {missing}")


def test_three_operand_and_extraction_nodes_are_excluded():
    """BETWEEN (three operands) and EXTRACTION_OPERATOR are deliberately out.

    Both always carry left and right, so this is a contract decision rather than
    a safety one — pinned so a future widening is a deliberate act.
    """
    assert NodeType.BETWEEN not in BINARY_NODE_TYPES
    assert NodeType.EXTRACTION_OPERATOR not in BINARY_NODE_TYPES


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
            print(f"{name} ✅")
    print("done")
