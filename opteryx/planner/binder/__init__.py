# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
The Binder sits between the Plan Rewriter and the Optimizer. It is responsible for
resolving all names, types, and references in the logical plan against the live
catalogues and execution context.

Input:  unbound LogicalPlan — nodes carry raw identifiers and AST fragments
Output: bound LogicalPlan — nodes carry resolved column identities, types, and schemas

Every Scan node reaching the Binder names a real dataset: CTE and view references were
already expanded by the Relation Resolver, which runs before the Plan Rewriter. The
Binder does not expand relations.

The Binder performs two passes:

1. Visibility filter injection — row-level security predicates are inserted as Filter
   nodes immediately above the relevant Scan nodes.

2. Node binding (BinderVisitor) — a bottom-up traversal resolves every column reference
   against the relation schemas accumulated from the scans upward, validates types,
   checks function signatures, and attaches schema_column metadata to each identifier
   node. The bound plan carries enough information for the Optimizer and Physical
   Planner to operate without further catalogue access.

The Binder does NOT restructure the plan or make cost-based decisions; that is the
Optimizer's responsibility.
"""

from opteryx.exceptions import InvalidInternalStateError
from opteryx.planner.binder.binding_context import BindingContext
from opteryx.planner.binder.common import BinderVisitor
from opteryx.planner.logical_planner import LogicalPlan
from opteryx.planner.logical_planner import apply_visibility_filters


def do_bind_phase(
    plan: LogicalPlan,
    execution_context=None,
    query_id: str = None,
    visibility_filters: dict = None,
    telemetry=None,
) -> LogicalPlan:
    """
    Execute the bind phase of the query engine.

    Parameters:
        plan: Any
            The logical plan.
        context: BindingContext
            The context needed for the binding phase.

    Returns:
        Modified logical plan after the binding phase.

    Raises:
        InvalidInternalStateError: Raised when the logical plan has more than one root node.
    """
    if visibility_filters:
        plan = apply_visibility_filters(plan, visibility_filters, telemetry)

    binder_visitor = BinderVisitor()
    root_node = plan.get_exit_points()
    context = BindingContext.initialize(query_id=query_id, execution_context=execution_context)

    if len(root_node) > 1:
        raise InvalidInternalStateError(
            f"{context.query_id} - logical plan has {len(root_node)} heads - this is an error"
        )

    plan, _ = binder_visitor.traverse(plan, root_node[0], context=context)

    return plan
