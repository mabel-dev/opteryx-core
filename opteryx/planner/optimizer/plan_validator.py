# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""WP-3: debug-mode logical-plan invariant checker.

Optimizer strategies mutate a shared plan in place. When a rewrite corrupts the
plan the symptom usually surfaces far downstream (or at execution), so the
failure names a strategy other than the one that caused it. This module
localises the corruption: when the ``VALIDATE_OPTIMIZER_PLANS`` flag is set,
``validate_plan`` runs after every strategy and raises an
:class:`InvalidInternalStateError` naming it.

The checks are intentionally structural and conservative — they must never
false-positive on a correct plan. Start narrow; tighten over time. The validator
inspects the plan only; it never mutates it.
"""

from opteryx.exceptions import InvalidInternalStateError
from opteryx.planner.logical_planner import LogicalPlan


def validate_plan(plan: LogicalPlan, where: str = "") -> None:
    """Assert structural invariants on ``plan``; raise on the first violation.

    Parameters:
        plan: the logical plan to check (not mutated).
        where: optional label (e.g. the strategy just run) prefixed to the error.

    Raises:
        InvalidInternalStateError: on the first invariant violation found.
    """
    prefix = f"plan invalid after {where}: " if where else "plan invalid: "

    nodes = dict(plan.nodes(True))
    node_ids = set(nodes.keys())

    # 1. Every edge connects two nodes that actually exist in the plan. A dangling
    #    endpoint means a rewrite removed a node without healing its edges.
    for source, target, _ in plan.edges():
        if source not in node_ids:
            raise InvalidInternalStateError(
                f"{prefix}edge references source '{source}' which is not a node in the plan"
            )
        if target not in node_ids:
            raise InvalidInternalStateError(
                f"{prefix}edge references target '{target}' which is not a node in the plan"
            )

    # An empty plan has no further structure to check.
    if not node_ids:
        return

    # 2. Exactly one exit point (a single root the physical planner can consume).
    exit_points = plan.get_exit_points()
    if len(exit_points) != 1:
        raise InvalidInternalStateError(
            f"{prefix}expected exactly one exit point, found {len(exit_points)}: "
            f"{list(exit_points)}"
        )

    # 3. No orphan nodes. In a multi-node plan every node must touch at least one
    #    edge; an isolated node is a rewrite that detached a node without removing
    #    it (and is invisible to get_exit_points, so it can mask a second root).
    if len(node_ids) > 1:
        connected: set = set()
        for source, target, _ in plan.edges():
            connected.add(source)
            connected.add(target)
        orphans = node_ids - connected
        if orphans:
            raise InvalidInternalStateError(
                f"{prefix}plan has disconnected node(s) with no edges: {sorted(orphans)}"
            )

    # 4. The plan must be a DAG; a cycle would loop the optimizer/executor forever.
    if not plan.is_acyclic():
        raise InvalidInternalStateError(f"{prefix}plan contains a cycle")

    # 5. Every node must render. A node that cannot be stringified is malformed
    #    (e.g. a rewrite left it without the attributes its renderer expects).
    #    We re-raise as a typed, localised error — never swallow.
    for nid, node in nodes.items():
        try:
            str(node)
        except Exception as render_error:
            raise InvalidInternalStateError(
                f"{prefix}node '{nid}' "
                f"({getattr(node, 'node_type', None)}) failed to render: {render_error}"
            ) from render_error
