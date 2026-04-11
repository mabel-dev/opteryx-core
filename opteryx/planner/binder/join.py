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
from opteryx.planner.binder.common import (
    convert_using_to_on,
    extract_join_fields,
    get_mismatched_condition_column_types,
)
from opteryx.schema import RelationSchema
from opteryx.types import OrsoTypes
from opteryx.utils import random_string


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

    if node.type == "cross join" and node.implied_join:
        # 1438
        if len(node.readers) > 2:
            from opteryx.exceptions import UnsupportedSyntaxError

            raise UnsupportedSyntaxError("Cannot CROSS JOIN more than two relations.")
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

        node.on, context = inner_binder(node.on, context)
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
    if node.type in ("left anti", "left semi"):
        for schema in node.right_relation_names:
            context.schemas.pop(schema, None)

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
