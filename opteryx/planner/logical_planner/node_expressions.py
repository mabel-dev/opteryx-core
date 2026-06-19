# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Node Expression Roots — the single authoritative answer to "what expressions
does this plan node evaluate, and therefore which columns does it reference?"

Why this exists
---------------
Logical plan nodes hold their expression trees in ad-hoc, per-node-type
attributes: a Filter's predicate is in ``condition``, a Join's in ``on`` /
``using`` / ``asof_condition``, an AggregateAndGroup's in ``groups`` /
``aggregates`` / ``having_condition``, an Order's in ``order_by`` (a list of
``(expr, ascending)`` tuples), a Scan's pushed predicates in ``predicates``,
and so on. Historically each optimizer strategy re-derived "what does this node
touch" by calling ``get_all_nodes_of_type`` on whatever fields *that* strategy
happened to know about. That scatter is a latent correctness hazard: a strategy
that forgets a field (e.g. reads ``columns`` but not ``condition``) silently
under-counts column uses, and a transform built on an under-count can corrupt
results.

Contract
--------
``expression_roots(node)`` is **pure** — it derives from the node's *current*
properties on every call, so it can never go stale as strategies rewrite the
plan (no cached field to invalidate).

It is **conservatively complete**: it inspects every stored property and
harvests every expression ``Node``, descending only builtin containers
(list/tuple/set) — the shapes the planner uses to hold expressions. Properties
that are not expression nodes or containers of them (schemas, connectors,
strings, ints, identity-sets, child step types) are skipped.

The completeness is the point: over-collection is safe for every consumer (a
spurious "use" only ever *disables* an optimization, never corrupts a result),
whereas under-collection is the only real hazard — and scanning all properties
cannot under-collect. Strategies must consume this rather than re-deriving
column references from hand-picked fields.
"""

from typing import List
from typing import Set

from opteryx.expression import NodeType
from opteryx.expression import get_all_nodes_of_type


def _is_expression(value) -> bool:
    """True iff ``value`` is an expression node — anything tagged with a NodeType.

    Expression trees are built from two duck-compatible carriers: ``Node``
    (operators, functions, literals…) and ``LogicalColumn`` (column references,
    which are *not* a ``Node`` subclass). Both expose a ``node_type`` that is a
    ``NodeType``; a logical *plan* node exposes a ``LogicalPlanStepType``
    instead. Keying on "node_type is a NodeType" captures every expression
    carrier — including bare ``LogicalColumn``s held directly in fields like
    ``order_by`` and ``groups`` — while excluding plan nodes.
    """
    return isinstance(getattr(value, "node_type", None), NodeType)


def _collect_roots(value, out: List) -> None:
    """Append every expression node reachable from ``value`` to ``out``.

    Descends only builtin containers; an expression node is a leaf root (we do
    not descend into its operands — callers walk the tree themselves).
    """
    if value is None:
        return
    if _is_expression(value):
        out.append(value)
        return
    value_type = type(value)
    if value_type is list or value_type is tuple or value_type is set:
        for item in value:
            _collect_roots(item, out)


def expression_roots(node, exclude=()) -> List:
    """Return every top-level expression tree the plan ``node`` evaluates.

    See the module docstring for the completeness/purity contract.

    ``exclude`` names property keys to skip. The only sanctioned use is dropping
    *derived bookkeeping* column lists — e.g. an aggregate's ``columns`` (the
    input columns it reads, a projection-pushdown artifact redundant with
    ``groups``/``aggregates``) — which would otherwise read as standalone column
    references. Excluding a genuine expression-bearing field would under-count
    and is unsafe; keep this list to known-derived attributes.
    """
    roots: List = []
    for key, value in node.properties.items():
        if key == "node_type" or key == "uuid":
            continue
        if key in exclude:
            continue
        _collect_roots(value, roots)
    return roots


def referenced_identities(node) -> Set[str]:
    """Schema-column identities every expression in ``node`` references.

    The authoritative "which columns does this node touch" set. Built on
    :func:`expression_roots`, so it inherits the completeness contract.
    """
    identities: Set[str] = set()
    for root in expression_roots(node):
        if root.node_type == NodeType.IDENTIFIER and root.schema_column is not None:
            identities.add(root.schema_column.identity)
        else:
            for ident in get_all_nodes_of_type(root, (NodeType.IDENTIFIER,)):
                if ident.schema_column is not None:
                    identities.add(ident.schema_column.identity)
    return identities
