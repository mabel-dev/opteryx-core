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


def _check_recursive_leg_types(name: str, anchor_schema, term_schema) -> None:
    """The anchor's schema is the recursive CTE's schema, and the engine's
    fixpoint appends the term's rows into the same buffers positionally — so the
    term must produce the same column count and the same physical type in every
    position. There is no silent widening: a mismatch names the column and the
    cast to write (docs/RECURSIVE_CTE_DESIGN.md §5.2)."""
    from opteryx.exceptions import UnsupportedSyntaxError

    anchor_columns = anchor_schema.columns
    term_columns = term_schema.columns

    # The anchor's positions DEFINE the fixpoint's buffers, and downstream
    # resolution (cte_column_map -> layout position) is by column IDENTITY. An
    # aliased duplicate (`SELECT ename, ename AS path`) shares its source's
    # identity — harmless in an ordinary query where both columns stay equal,
    # but a recursive CTE's positions DIVERGE over iterations and a shared
    # identity silently collapses them onto one buffer (`path` reads `ename`
    # forever). Refuse it and name the rewrite that makes the copy independent.
    seen_identities: dict = {}
    for position, column in enumerate(anchor_columns):
        identity = column.identity
        first = seen_identities.get(identity)
        if first is not None:
            source_name = anchor_columns[first].name
            type_name = column.column_type.physical.name
            raise UnsupportedSyntaxError(
                f"Recursive CTE '{name}': columns '{source_name}' and "
                f"'{column.name}' in the anchor are the same underlying column, but "
                "a recursive CTE's columns evolve independently. Make the copy its "
                f"own column — e.g. `CAST({source_name} AS {type_name}) "
                f"AS {column.name}`."
            )
        seen_identities[identity] = position

    if len(anchor_columns) != len(term_columns):
        raise UnsupportedSyntaxError(
            f"Recursive CTE '{name}': the anchor produces {len(anchor_columns)} "
            f"column(s) but the recursive term produces {len(term_columns)}; the "
            "two sides of UNION ALL must produce the same columns."
        )
    for position, (anchor_col, term_col) in enumerate(zip(anchor_columns, term_columns)):
        anchor_type = anchor_col.column_type.physical
        term_type = term_col.column_type.physical
        if anchor_type != term_type:
            raise UnsupportedSyntaxError(
                f"Recursive CTE '{name}': column '{anchor_col.name}' (position "
                f"{position + 1}) is {anchor_type.name} in the anchor but "
                f"{term_type.name} in the recursive term; add an explicit CAST in "
                "one of the terms so both sides agree."
            )


def do_bind_phase(
    plan: LogicalPlan,
    execution_context=None,
    query_id: str = None,
    visibility_filters: dict = None,
    telemetry=None,
    schema_only: bool = False,
) -> LogicalPlan:
    """
    Execute the bind phase of the query engine.

    Parameters:
        plan: Any
            The logical plan.
        context: BindingContext
            The context needed for the binding phase.
        schema_only: bool
            Resolve names and types without reading each relation's Manifest.
            Only for callers that stop at the end of binding - the plan this
            produces carries no file lists or statistics and cannot be optimized
            or executed. See BindingContext.

    Returns:
        Modified logical plan after the binding phase.

    Raises:
        InvalidInternalStateError: Raised when the logical plan has more than one root node.
    """
    if visibility_filters:
        plan = apply_visibility_filters(plan, visibility_filters, telemetry)

    binder_visitor = BinderVisitor()

    # Shared CTE bodies (relation_resolver: CTEs referenced 2+ times, executed
    # once) are bound FIRST, each as a standalone plan, in dependency order —
    # `shared_ctes` is topologically ordered, so a body reading another shared
    # CTE finds it already bound. Each body is headed by a Subquery boundary
    # whose bound schema is the output the single execution produces;
    # visit_materialized_cte_ref re-exposes it per reference under fresh
    # identities. Row-level security applies inside the bodies exactly as it
    # does in the main plan — a body holds real Scans.
    shared_ctes = getattr(plan, "shared_ctes", None) or {}
    # Recursive CTE metadata (relation_resolver): rcte_key -> anchor/term leg
    # keys. The legs are ordinary entries in shared_ctes (anchor immediately
    # before term); the CTE itself has no body of its own — its schema IS the
    # anchor's, registered under the rcte_key the references carry, and the
    # term binds against it (docs/RECURSIVE_CTE_DESIGN.md §5.2).
    recursive_ctes = getattr(plan, "recursive_ctes", None) or {}
    anchor_key_to_rcte = {meta["anchor_key"]: rkey for rkey, meta in recursive_ctes.items()}
    term_key_to_rcte = {meta["term_key"]: rkey for rkey, meta in recursive_ctes.items()}
    # One registry dict, shared BY REFERENCE into every context (including the
    # child scopes expression subqueries bind under — see BindingContext).
    shared_cte_schemas: dict = {}
    for cte_key, body in shared_ctes.items():
        if visibility_filters:
            body = apply_visibility_filters(body, visibility_filters, telemetry)
        body_heads = body.get_exit_points()
        if len(body_heads) != 1:
            raise InvalidInternalStateError(
                f"{query_id} - shared CTE body has {len(body_heads)} heads - this is an error"
            )
        body_context = BindingContext.initialize(
            query_id=query_id, execution_context=execution_context, schema_only=schema_only
        )
        body_context.shared_cte_schemas = shared_cte_schemas
        body, _ = binder_visitor.traverse(body, body_heads[0], context=body_context)
        shared_ctes[cte_key] = body
        shared_cte_schemas[cte_key] = body[body_heads[0]].schema
        rkey = anchor_key_to_rcte.get(cte_key)
        if rkey is not None:
            # the anchor's boundary schema IS the recursive CTE's schema — the
            # term's self-reference (bound next) and every outer reference
            # resolve against it
            shared_cte_schemas[rkey] = shared_cte_schemas[cte_key]
        rkey = term_key_to_rcte.get(cte_key)
        if rkey is not None:
            _check_recursive_leg_types(
                recursive_ctes[rkey]["name"],
                shared_cte_schemas[rkey],
                shared_cte_schemas[cte_key],
            )

    root_node = plan.get_exit_points()
    context = BindingContext.initialize(
        query_id=query_id, execution_context=execution_context, schema_only=schema_only
    )
    context.shared_cte_schemas = shared_cte_schemas

    if len(root_node) > 1:
        raise InvalidInternalStateError(
            f"{context.query_id} - logical plan has {len(root_node)} heads - this is an error"
        )

    plan, _ = binder_visitor.traverse(plan, root_node[0], context=context)
    plan.shared_ctes = shared_ctes
    plan.recursive_ctes = recursive_ctes

    return plan
