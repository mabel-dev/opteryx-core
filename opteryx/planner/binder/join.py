# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

from typing import Tuple

from opteryx.exceptions import UnsupportedSyntaxError
from opteryx.expression import NodeType, get_all_nodes_of_type
from opteryx.models import Node
from opteryx.planner.binder.binder import inner_binder
from opteryx.planner.binder.binding_context import BindingContext
from opteryx.planner.binder.join_helpers import (
    convert_using_to_on,
    extract_join_fields,
    get_mismatched_condition_column_types,
)
from opteryx.types import OrsoTypes
from opteryx.types.schema import RelationSchema
from opteryx.utils import random_string


def _bind_on_condition_split(
    on_node: Node, left_context: BindingContext, right_context: BindingContext, right_set: set
) -> Node:
    """
    Bind each side of an AND-tree of Eq conditions using a split context.

    When the ON condition comes from an IN-subquery rewrite, both the outer relation
    and the inner (subquery) relation may project the same column name. Binding the
    entire condition with a merged context would raise AmbiguousIdentifierError.

    This function routes each Eq comparison's sides to the appropriate restricted
    context: right-side identifiers (source in right_set) use the subquery-only
    context; left-side identifiers use the outer-query context.
    """
    if on_node.node_type == NodeType.AND:
        on_node.left = _bind_on_condition_split(
            on_node.left, left_context, right_context, right_set
        )
        on_node.right = _bind_on_condition_split(
            on_node.right, left_context, right_context, right_set
        )
        return on_node

    if on_node.node_type == NodeType.COMPARISON_OPERATOR and on_node.value == "Eq":
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

    if node.type == "cross join" and node.implied_join:
        # 1438 - Check only if readers is set (not set for sequential binary joins)
        if node.readers and len(node.readers) > 2:
            from opteryx.exceptions import UnsupportedSyntaxError

            raise UnsupportedSyntaxError("Cannot CROSS JOIN more than two relations.")
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
        node.using = [Node("temp", value=n) for n in set(left_columns).intersection(right_columns)]
        node.type = "inner"
    # Handle 'using' by converting to a an 'on'
    if node.using:
        node.on = convert_using_to_on(
            {n.value for n in node.using},
            node.left_relation_names,
            node.right_relation_names,
        )
    if node.on:
        # All conditions have been mapped to 'on' conditions
        comparisons = get_all_nodes_of_type(node.on, (NodeType.COMPARISON_OPERATOR,))
        if not all(com.value in ("Eq", "NotEq", "Lt", "Gt", "LtEq", "GtEq") for com in comparisons):
            from opteryx.exceptions import UnsupportedSyntaxError

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

        node.left_columns, node.right_columns = extract_join_fields(
            node.on, node.left_relation_names, node.right_relation_names
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
            com.left.schema_column.type == OrsoTypes.DECIMAL and com.value not in ("Eq", "NotEq")
            for com in comparisons
        ):
            from opteryx.exceptions import UnsupportedSyntaxError

            raise UnsupportedSyntaxError(
                "JOINs on DECIMAL types only supports Equals and Not Equals."
            )

        # we need to put the referenced columns into the columns attribute for the
        # optimizers
        node.columns = get_all_nodes_of_type(node.on, (NodeType.IDENTIFIER,))

    if node.using:
        # Remove the columns used in the join condition from both relations, they're in
        # the result set but not belonging to either table, whilst still belonging to both.
        # We create a new schema to put them in, $shared-nnn.
        columns = []

        # Loop through all using fields in the node
        left_relation_name = ""
        right_relation_name = ""
        for column_name in (n.value for n in node.using):
            # Pop the column from the left relation
            for left_relation_name in node.left_relation_names:
                left_column = context.schemas[left_relation_name].pop_column(column_name)

            # Pop the column from the right relation
            for right_relation_name in node.right_relation_names:
                right_column = context.schemas[right_relation_name].pop_column(column_name)

            # we need to decide which column we're going to keep
            left_column.origin = [left_relation_name, right_relation_name]
            columns.append(left_column)

        # shared columns exist in both schemas in some uses and in neither in others
        context.schemas[f"$shared-{random_string()}"] = RelationSchema(
            name=f"^{left_relation_name}#^{right_relation_name}#", columns=columns
        )

    # SEMI and ANTI joins only return columns from one table
    if node.type in ("left anti", "left semi", "left anti null-aware"):
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

    # This is very much not how we want to do this, but let's start somewhere
    # we're estimating the size of each side of the join, but here all we're doing is
    # using the row estimates for each table, ignoring any filtering etc.
    node.left_size = sum(
        context.schemas[relation_name].row_count_metric
        or context.schemas[relation_name].row_count_estimate
        or float("inf")
        for relation_name in node.left_relation_names
        if relation_name in context.schemas
    )
    node.right_size = sum(
        context.schemas[relation_name].row_count_metric
        or context.schemas[relation_name].row_count_estimate
        or float("inf")
        for relation_name in node.right_relation_names
        if relation_name in context.schemas
    )

    if node.type == "inner" and node.on is None:
        from opteryx.exceptions import SqlError

        raise SqlError("INNER and NATURAL joins must have a either an ON or USING condition.")

    node.schemas = context.schemas

    return node, context
