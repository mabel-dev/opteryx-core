# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

import copy
from typing import Tuple

from opteryx.exceptions import InvalidInternalStateError, UnsupportedSyntaxError
from opteryx.expression import NodeType, get_all_nodes_of_type
from opteryx.models import LogicalColumn
from opteryx.models import Node
from opteryx.planner.binder.binder import inner_binder
from opteryx.planner.binder.binding_context import BindingContext
from opteryx.planner.binder.join_helpers import (
    convert_using_to_on,
    locate_using_column,
    extract_join_fields,
    get_mismatched_condition_column_types,
    reject_unhoistable_join_operands,
)
from opteryx.types.logical_type import LogicalCategory
from opteryx.types.schema import RelationSchema, mint_column_identity
from opteryx.utils import random_string


def _pop_using_column(
    context: BindingContext, relation_names: list, column_name: str, side: str
) -> Tuple[object, list]:
    """Take `column_name` out of the one place on this leg that holds it.

    That place is either a relation named on the leg, or the merged column of a
    USING / NATURAL JOIN lower down the leg (a `$shared-*` schema) - see
    `locate_using_column`, which also refuses a column held twice on one leg.

    Popping matters: the column moves into this join's own merged column, so it
    must stop being visible under its old home.

    Returns (column, origins) - origins being every relation the column came from,
    which the merged column inherits, so `a.id` still resolves to it however many
    USING joins the chain has passed through.
    """
    schema_key, schema_column = locate_using_column(
        context.schemas, relation_names, column_name, side
    )
    context.schemas[schema_key].pop_column(schema_column.name)
    if schema_key.startswith("$shared-"):
        origins = list(schema_column.origin)
    else:
        origins = [schema_key]
    return schema_column, origins


def _bind_on_condition_split(
    on_node: Node, left_context: BindingContext, right_context: BindingContext, right_set: set
) -> Node:
    """
    Bind each side of an AND-tree of comparisons using a split context.

    When the ON condition comes from an IN-subquery rewrite, both the outer relation
    and the inner (subquery) relation may project the same column name. Binding the
    entire condition with a merged context would raise AmbiguousIdentifierError.

    This function routes each comparison's sides to the appropriate restricted
    context: right-side identifiers (source in right_set) use the subquery-only
    context; left-side identifiers use the outer-query context.

    Every comparison operator is split, not just Eq: a correlated EXISTS residual is
    typically an INEQUALITY spanning the two legs (TPC-H Q21's
    `l2.l_suppkey <> l1.l_suppkey`), and binding that whole node in the left context
    cannot see the subquery relation at all.
    """
    if on_node.node_type == NodeType.AND:
        on_node.left = _bind_on_condition_split(
            on_node.left, left_context, right_context, right_set
        )
        on_node.right = _bind_on_condition_split(
            on_node.right, left_context, right_context, right_set
        )
        return on_node

    if on_node.node_type == NodeType.COMPARISON_OPERATOR:
        right_source = getattr(on_node.right, "source", None)
        left_source = getattr(on_node.left, "source", None)

        if right_source in right_set:
            on_node.right, _ = inner_binder(on_node.right, right_context)
            on_node.left, _ = inner_binder(on_node.left, left_context)
        elif left_source in right_set:
            on_node.left, _ = inner_binder(on_node.left, right_context)
            on_node.right, _ = inner_binder(on_node.right, left_context)
        else:
            on_node, _ = inner_binder(on_node, left_context)
        return on_node

    on_node, _ = inner_binder(on_node, left_context)
    return on_node


def visit_join(self, node: Node, context: BindingContext) -> Tuple[Node, BindingContext]:
    """
    Visits a JOIN node and handles different types of joins.

    Parameters:
        node: Node
            The node representing the join operation.
        context: Dict
            The context containing relevant information like schemas.

    Returns:
        Tuple[Node, Dict]
            Updated node and context.
    """
    node.columns = []

    if not node.left_relation_names and node.relation_names and len(node.relation_names) >= 2:
        node.left_relation_names = (
            node.relation_names[0]
            if isinstance(node.relation_names[0], list)
            else [node.relation_names[0]]
        )
    if not node.right_relation_names and node.relation_names and len(node.relation_names) >= 2:
        node.right_relation_names = (
            node.relation_names[1]
            if isinstance(node.relation_names[1], list)
            else [node.relation_names[1]]
        )
    if node.left_readers is None and node.readers and len(node.readers) >= 2:
        node.left_readers = node.readers[0]
    if node.right_readers is None and node.readers and len(node.readers) >= 2:
        node.right_readers = node.readers[1]

    if node.type == "asof":
        node.asof_condition, context = inner_binder(node.asof_condition, context)

        comparisons = get_all_nodes_of_type(node.asof_condition, (NodeType.COMPARISON_OPERATOR,))
        if len(comparisons) != 1:
            raise UnsupportedSyntaxError(
                "ASOF **MATCH_CONDITION** must contain exactly one comparison."
            )
        asof_cmp = comparisons[0]
        if asof_cmp.value not in ("Lt", "LtEq", "Gt", "GtEq"):
            raise UnsupportedSyntaxError(
                "ASOF **MATCH_CONDITION** must use <, <=, >, or >= (not = or !=)."
            )
        node.asof_left_column = asof_cmp.left.schema_column.identity
        node.asof_right_column = asof_cmp.right.schema_column.identity
        node.asof_op = asof_cmp.value
        node.columns = list(get_all_nodes_of_type(node.asof_condition, (NodeType.IDENTIFIER,)))

        # Optional equi-partition key via ON/USING — bind it normally
        if node.using:
            node.on = convert_using_to_on(
                {n.value for n in node.using},
                node.left_relation_names,
                node.right_relation_names,
                context.schemas,
            )
        if node.on:
            node.on, context = inner_binder(node.on, context)
            node.left_columns, node.right_columns, unkeyed = extract_join_fields(
                node.on, node.left_relation_names, node.right_relation_names
            )
            # ASOF's ON is a partition key the engine reads directly; nothing
            # rewrites it later, so an expression operand here is rejected outright
            # rather than deferred to JoinKeyMaterializationStrategy.
            if unkeyed:
                raise UnsupportedSyntaxError(
                    "ASOF **JOIN** partition keys must be columns, not expressions."
                )
            node.columns += list(get_all_nodes_of_type(node.on, (NodeType.IDENTIFIER,)))

        node.schemas = context.schemas
        return node, context

    if node.type == "cross join" and node.implied_join:
        # 1438 - Check only if readers is set (not set for sequential binary joins)
        if node.readers and len(node.readers) > 2:
            raise UnsupportedSyntaxError("Cannot **CROSS JOIN** more than two relations.")
        # Extract from readers only if it's set (backward compat for old-style implicit joins)
        # For new sequential binary joins, left/right are already set in logical planner
        if node.readers:
            node.left_relation_names = (
                node.relation_names[0]
                if isinstance(node.relation_names[0], list)
                else [node.relation_names[0]]
            )
            node.right_relation_names = (
                node.relation_names[1]
                if isinstance(node.relation_names[1], list)
                else [node.relation_names[1]]
            )
            node.left_readers = node.readers[0]
            node.right_readers = node.readers[1]
        node.type = "cross join"

    # Handle 'natural join' by converting to an inner join with a 'using'
    if node.type == "natural join":
        left_columns = [
            col
            for relation_name in node.left_relation_names
            for col in context.schemas[relation_name].column_names
        ]
        right_columns = [
            col
            for relation_name in node.right_relation_names
            for col in context.schemas[relation_name].column_names
        ]
        # The same column references an explicit USING (...) builds.
        node.using = [
            LogicalColumn(node_type=NodeType.IDENTIFIER, source_column=n)
            for n in set(left_columns).intersection(right_columns)
        ]
        node.type = "inner"
    # Handle 'using' by converting to a an 'on'
    if node.using:
        node.on = convert_using_to_on(
            {n.value for n in node.using},
            node.left_relation_names,
            node.right_relation_names,
            context.schemas,
        )
    if node.on:
        # All conditions have been mapped to 'on' conditions
        comparisons = get_all_nodes_of_type(node.on, (NodeType.COMPARISON_OPERATOR,))
        if not all(com.value in ("Eq", "NotEq", "Lt", "Gt", "LtEq", "GtEq") for com in comparisons):
            raise UnsupportedSyntaxError("Only JOINs with equals comparisons supported.")

        if not node.left_relation_names and node.right_relation_names:
            # IN-subquery rewrites: both outer and inner may share a column name (e.g. "id").
            # Bind each side of the ON condition in a restricted context to avoid
            # AmbiguousIdentifierError: left side uses only non-subquery schemas, right side
            # uses only the subquery schema.
            right_set = set(node.right_relation_names)
            left_context = context.copy()
            left_context.schemas = {k: v for k, v in context.schemas.items() if k not in right_set}
            right_context = context.copy()
            right_context.schemas = {
                k: v for k, v in context.schemas.items() if k in right_set or k == "$derived"
            }
            node.on = _bind_on_condition_split(node.on, left_context, right_context, right_set)
        else:
            node.on, context = inner_binder(node.on, context)

        # When left_relation_names is not set (e.g. IN-subquery rewrites that don't know
        # the outer relation at rewrite time), infer it from the bound ON condition: any
        # identifier source that is not the right-side relation must be on the left.
        if not node.left_relation_names and node.right_relation_names:
            right_set = set(node.right_relation_names)
            left_sources = {
                n.source
                for n in get_all_nodes_of_type(node.on, (NodeType.IDENTIFIER,))
                if n.source
                and n.source not in right_set
                and n.source != "$derived"
                and not n.source.startswith("$shared-")
            }
            if left_sources:
                node.left_relation_names = list(left_sources)

        node.left_columns, node.right_columns, unkeyed = extract_join_fields(
            node.on, node.left_relation_names, node.right_relation_names
        )
        # An Eq conjunct with an expression operand is not a key YET. Reject only
        # the ones no projection can rescue; the rest are turned into real keys by
        # JoinKeyMaterializationStrategy, which shares this decision (see
        # join_helpers.hoistable_operand_leg) so the two cannot drift apart.
        reject_unhoistable_join_operands(
            unkeyed, node.left_relation_names, node.right_relation_names
        )
        mismatches = get_mismatched_condition_column_types(
            node.on,
            relaxed=False,
            allow_numeric_join_coercion=not bool(node.using),
        )
        if mismatches:
            from opteryx.exceptions import IncompatibleTypesError

            raise IncompatibleTypesError(**mismatches)

        if any(
            com.left.schema_column.category == LogicalCategory.DECIMAL and com.value not in ("Eq", "NotEq")
            for com in comparisons
        ):
            raise UnsupportedSyntaxError(
                "JOINs on DECIMAL types only supports Equals and Not Equals."
            )

        # we need to put the referenced columns into the columns attribute for the
        # optimizers
        node.columns = get_all_nodes_of_type(node.on, (NodeType.IDENTIFIER,))

        # A SEMI/ANTI join lifted out of a correlated EXISTS may carry a non-equality
        # residual (decorrelate_subquery, post-bind; TPC-H Q21). It spans both legs exactly like
        # the ON condition, so it binds the same way — including the split-context path,
        # since outer and inner can share a column name (`l1.l_suppkey` / `l2.l_suppkey`).
        residual = getattr(node, "residual", None)
        if residual is not None:
            if not node.right_relation_names:
                raise InvalidInternalStateError(
                    "join residual without a right relation to bind it against"
                )
            right_set = set(node.right_relation_names)
            left_context = context.copy()
            left_context.schemas = {k: v for k, v in context.schemas.items() if k not in right_set}
            right_context = context.copy()
            right_context.schemas = {
                k: v for k, v in context.schemas.items() if k in right_set or k == "$derived"
            }
            node.residual = _bind_on_condition_split(
                residual, left_context, right_context, right_set
            )
            node.columns = node.columns + list(
                get_all_nodes_of_type(node.residual, (NodeType.IDENTIFIER,))
            )

    if node.using:
        # Remove the columns used in the join condition from both relations, they're in
        # the result set but not belonging to either table, whilst still belonging to both.
        # We create a new schema to put them in, $shared-nnn.
        #
        # The merged column is COALESCE(left.k, right.k) (SQL-92). For INNER and LEFT
        # OUTER that is always the left value, so the left column is kept as-is. For
        # RIGHT and FULL OUTER it is not - a row only the right side produced has a
        # NULL left key - so the merged column is a NEW column the join emits
        # natively (probe-emitted rows carry the probe key, unmatched-build rows the
        # build key; see compiler._compile_join and UnmatchedBuildSource). Keeping
        # the left column there returned NULL for every right-only row's key.
        coalesces = node.type in ("right outer", "full outer")
        columns = []
        using_merged = []
        origins: list = []
        for column_name in (n.value for n in node.using):
            left_column, left_origins = _pop_using_column(
                context, node.left_relation_names, column_name, "left"
            )
            right_column, right_origins = _pop_using_column(
                context, node.right_relation_names, column_name, "right"
            )
            merged_origins = list(dict.fromkeys(left_origins + right_origins))
            origins = merged_origins

            if coalesces:
                merged = copy.copy(left_column)
                merged.identity = mint_column_identity("$shared", column_name)
                merged.aliases = None
                merged.nullable = left_column.nullable or right_column.nullable
                # Statistics describe the LEFT column; the coalesced one can hold
                # right-only values outside that range, so it carries none.
                merged.highest_value = None
                merged.lowest_value = None
                merged.null_count = None
                using_merged.append(
                    (merged.identity, left_column.identity, right_column.identity)
                )
            else:
                merged = left_column
            merged.origin = merged_origins
            columns.append(merged)

        if using_merged:
            node.using_merged = using_merged

        # shared columns exist in both schemas in some uses and in neither in others
        context.schemas[f"$shared-{random_string()}"] = RelationSchema(
            name="#".join(f"^{o}" for o in origins) + "#", columns=columns
        )

    # SEMI and ANTI joins only return columns from one table
    if node.type in (
        "left anti",
        "left semi",
        "left anti null-aware",
        "left semi not-distinct",
        "left anti not-distinct",
    ):
        for schema in node.right_relation_names:
            context.schemas.pop(schema, None)

    # Window joins: the CTE subquery exposes each partition column under the same name and
    # identity as the outer scan (because the CTE scan is a copy of the outer scan node).
    # Both schemas would otherwise contain e.g. "group", triggering AmbiguousIdentifierError
    # when the outer Project binds it. Remove the subquery's copies — only the outer scan's
    # copy should be visible downstream.
    if getattr(node, "is_window_join", False) and node.on and node.right_relation_names:
        right_set = set(node.right_relation_names)
        partition_col_names = {
            n.schema_column.name
            for n in get_all_nodes_of_type(node.on, (NodeType.IDENTIFIER,))
            if n.source in right_set and n.schema_column is not None
        }
        for right_rel in node.right_relation_names:
            if right_rel in context.schemas:
                schema = context.schemas[right_rel]
                schema.columns = [c for c in schema.columns if c.name not in partition_col_names]

    if node.type == "inner" and node.on is None:
        from opteryx.exceptions import SqlError, compose, md_syntax

        raise SqlError(
            compose(
                f"An {md_syntax('INNER JOIN')} or {md_syntax('NATURAL JOIN')} needs "
                f"either an {md_syntax('ON')} or a {md_syntax('USING')} condition to "
                f"say how the two relations line up",
                f"To combine every row with every other row, "
                f"{md_syntax('CROSS JOIN')} says so explicitly",
            )
        )

    node.schemas = context.schemas

    return node, context
