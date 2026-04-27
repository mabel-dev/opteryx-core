# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""Common utilities and dispatcher for the BinderVisitor pattern."""

import re
from functools import lru_cache
from typing import List, Set, Tuple

from opteryx.exceptions import (
    AmbiguousDatasetError,
    InvalidFunctionParameterError,
    UnsupportedSyntaxError,
)
from opteryx.expression import NodeType, get_all_nodes_of_type
from opteryx.managers.virtual_datasets import derived
from opteryx.models import LogicalColumn, Node

# Import handler functions from modular packages
from opteryx.planner.binder.aggregate import visit_aggregate_and_group, visit_distinct
from opteryx.planner.binder.binder import inner_binder, merge_schemas
from opteryx.planner.binder.binding_context import BindingContext
from opteryx.planner.binder.dataset import visit_function_dataset, visit_scan
from opteryx.planner.binder.filter import visit_filter

# Lazily import visit_join inside the delegation method to avoid circular imports.
# from opteryx.planner.binder.join import visit_join
from opteryx.planner.binder.order import visit_order
from opteryx.planner.binder.project import visit_exit, visit_project
from opteryx.planner.binder.set_ops import visit_set, visit_union, visit_intersect, visit_except, visit_unnest
from opteryx.planner.binder.subquery import visit_comment, visit_subquery
from opteryx.planner.binder.traversal import post_bind, traverse
from opteryx.planner.binder.view import (
    visit_alter_view,
    visit_create_view,
    visit_drop_view,
    visit_show_columns,
)
from opteryx.planner.logical_planner import LogicalPlan
from opteryx.types import OrsoTypes
from opteryx.types.schema import ConstantColumn, FlatColumn, RelationSchema
from opteryx.utils import random_string

CAMEL_TO_SNAKE = re.compile(r"(?<!^)(?=[A-Z])")


def _is_numeric_join_coercible(left_type, right_type) -> bool:
    """Return True when join-side implicit numeric coercion is safe."""
    if left_type in (OrsoTypes.BOOLEAN, OrsoTypes.NULL) or right_type in (
        OrsoTypes.BOOLEAN,
        OrsoTypes.NULL,
    ):
        return False
    return left_type in (OrsoTypes.INTEGER, OrsoTypes.DOUBLE, OrsoTypes.DECIMAL) and right_type in (
        OrsoTypes.INTEGER,
        OrsoTypes.DOUBLE,
        OrsoTypes.DECIMAL,
    )


def get_mismatched_condition_column_types(
    node: Node, relaxed: bool = False, allow_numeric_join_coercion: bool = False
) -> dict:
    """
    Checks that the types of the fields involved a comparison are the same on both sides.

    Parameters:
        node: Node
            The condition node representing the condition.

    Returns:
        a dictionary describing the columns
    """
    if node.node_type in (NodeType.AND, NodeType.OR, NodeType.XOR):
        left_mismatches = get_mismatched_condition_column_types(
            node.left, relaxed, allow_numeric_join_coercion
        )
        right_mismatches = get_mismatched_condition_column_types(
            node.right, relaxed, allow_numeric_join_coercion
        )
        return left_mismatches or right_mismatches

    elif node.node_type == NodeType.COMPARISON_OPERATOR:
        if node.value in (
            "InList",
            "NotInList",
            "Arrow",
            "LongArrow",
            "AtQuestion",
            "AtArrow",
        ) or node.value.startswith(("AllOp", "AnyOp")):
            return None  # Some ops are meant to have different types
        left_type = node.left.schema_column.type if node.left.schema_column else None
        right_type = node.right.schema_column.type if node.right.schema_column else None

        if left_type and right_type and left_type != right_type:
            if (
                allow_numeric_join_coercion
                and node.left.node_type == NodeType.IDENTIFIER
                and node.right.node_type == NodeType.IDENTIFIER
                and _is_numeric_join_coercible(left_type, right_type)
            ):
                return None
            if (
                relaxed
                and (left_type.is_numeric() and right_type.is_numeric())
                or (left_type.is_temporal() and right_type.is_temporal())
                or (left_type.is_numeric() and right_type.is_temporal())
                or (left_type.is_temporal() and right_type.is_numeric())
                or (left_type.is_large_object() and right_type.is_large_object())
                or (left_type.is_string() and right_type.is_string())
                or (left_type == 0 or right_type == 0)
            ):
                return None
            if left_type == OrsoTypes.NULL or right_type == OrsoTypes.NULL:
                return None  # None comparisons are allowed
            if (
                node.left.node_type == NodeType.COMPARISON_OPERATOR
                or node.right.node_type == NodeType.COMPARISON_OPERATOR
                or node.left.node_type == NodeType.BINARY_OPERATOR
                or node.right.node_type == NodeType.BINARY_OPERATOR
                or node.left.node_type == NodeType.EXTRACTION_OPERATOR
                or node.right.node_type == NodeType.EXTRACTION_OPERATOR
            ):
                return None  # it's compound so don't make a decision here
            return {
                "left_column": f"{node.left.source}.{node.left.value}",
                "left_type": left_type.name,
                "left_node": node.left,
                "right_column": f"{node.right.source}.{node.right.value}",
                "right_type": right_type.name,
                "right_node": node.right,
            }

    return None  # if we reach here, it means we didn't find any inconsistencies


def extract_join_fields(
    condition_node: Node,
    left_relation_names: List[str],
    right_relation_names: List[str],
) -> Tuple[List[str], List[str]]:
    """
    Extracts join fields from a condition node that may have multiple ANDed conditions.

    Parameters:
        condition_node: Node
            The condition node in the join clause.
        left_relation_name: str
            Name of the left relation.
        right_relation_name: str
            Name of the right relation.

    Returns:
        Tuple[List[str], List[str]]
            Lists of columns participating in the join from the left and right tables.
    """
    left_fields = []
    right_fields = []

    if condition_node.node_type == NodeType.AND:
        left_fields_1, right_fields_1 = extract_join_fields(
            condition_node.left, left_relation_names, right_relation_names
        )
        left_fields_2, right_fields_2 = extract_join_fields(
            condition_node.right, left_relation_names, right_relation_names
        )

        left_fields.extend(left_fields_1)
        left_fields.extend(left_fields_2)

        right_fields.extend(right_fields_1)
        right_fields.extend(right_fields_2)

    elif condition_node.node_type == NodeType.COMPARISON_OPERATOR and condition_node.value == "Eq":
        if any(
            [
                condition_node.left.node_type not in (NodeType.IDENTIFIER, NodeType.LITERAL),
                condition_node.right.node_type not in (NodeType.IDENTIFIER, NodeType.LITERAL),
            ]
        ):
            raise UnsupportedSyntaxError("JOIN conditions only support column comparisons.")
        if (
            condition_node.left.source in left_relation_names
            and condition_node.right.source in right_relation_names
        ):
            left_fields.append(condition_node.left.schema_column.identity)
            right_fields.append(condition_node.right.schema_column.identity)
        elif (
            condition_node.left.source in right_relation_names
            and condition_node.right.source in left_relation_names
        ):
            right_fields.append(condition_node.left.schema_column.identity)
            left_fields.append(condition_node.right.schema_column.identity)

    return left_fields, right_fields


def convert_using_to_on(
    using_fields: Set[str],
    left_relation_names: List[str],
    right_relation_names: List[str],
) -> Node:
    """
    Converts a USING field to an ON field for JOIN operations.

    Parameters:
        using_fields: Set[str]
            Set of common fields to use for joining.
        left_relation_names: List[str]
            Names of the left relations.
        right_relation_names: List[str]
            Names of the right relations.

    Returns:
        Node
            The condition node representing the ON clause.
    """
    all_conditions = []

    # Loop through all combinations of left and right relation names
    for left_relation_name in left_relation_names:
        for right_relation_name in right_relation_names:
            conditions = []
            for field in using_fields:
                condition = Node(
                    node_type=NodeType.COMPARISON_OPERATOR,
                    value="Eq",
                    do_not_create_column=True,
                )
                condition.left = LogicalColumn(
                    node_type=NodeType.IDENTIFIER,
                    source=left_relation_name,
                    source_column=field,
                )
                condition.right = LogicalColumn(
                    node_type=NodeType.IDENTIFIER,
                    source=right_relation_name,
                    source_column=field,
                )
                conditions.append(condition)

            if len(conditions) == 1:
                all_conditions.append(conditions[0])
            else:
                # Create a tree of ANDed conditions
                while len(conditions) > 1:
                    new_conditions = []
                    for i in range(0, len(conditions), 2):
                        if i + 1 < len(conditions):
                            and_node = Node(node_type=NodeType.AND, do_not_create_column=True)
                            and_node.left = conditions[i]
                            and_node.right = conditions[i + 1]
                            new_conditions.append(and_node)
                        else:
                            new_conditions.append(conditions[i])
                    conditions = new_conditions
                all_conditions.append(conditions[0])

    return conditions[0]


@lru_cache(maxsize=128)
def node_type_to_method_name(node_type: str) -> str:
    return f"visit_{CAMEL_TO_SNAKE.sub('_', node_type).lower()}"


class BinderVisitor:
    """
    The BinderVisitor visits each node in the query plan and adds catalogue information
    to each node. This includes:

    - identifiers, bound from the schemas
    - functions and aggregatros, bound from the function catalogue
    - variables, bound from the variables collection

    """

    def visit_node(self, node: Node, context: BindingContext) -> Tuple[Node, BindingContext]:
        """
        Visits a given node and returns a new node and context after binding catalog information.

        Parameters:
            node: Node
                The query plan node to visit.
            context: Dict[str, Any]
                The current binding context.

        Returns:
            Tuple[Node, Dict]
            The node and context after binding.
        """
        node_type = node.node_type.name  # type: ignore
        visit_method_name = node_type_to_method_name(node_type)
        visit_method = getattr(self, visit_method_name, None)
        if visit_method is None:
            # DEBUG: print(f"BinderVisitor: No method found for {visit_method_name}")
            return node, context

        return_node, return_context = visit_method(node, context)

        # DEBUG: from opteryx.exceptions import InvalidInternalStateError
        # DEBUG:
        # DEBUG: if not isinstance(return_context, BindingContext):
        # DEBUG:     raise InvalidInternalStateError(
        # DEBUG:         f"Internal Error - function '{visit_method_name}' didn't return a BindingContext"
        # DEBUG:     )
        # DEBUG:
        # DEBUG: if not all(isinstance(schema, RelationSchema) for schema in context.schemas.values()):
        # DEBUG:     raise InvalidInternalStateError(
        # DEBUG:         f"Internal Error - function '{visit_method_name}' returned invalid Schemas"
        # DEBUG:     )
        # DEBUG:
        # DEBUG: if not all(isinstance(col, (Node, LogicalColumn)) for col in return_node.columns or []):
        # DEBUG:     raise InvalidInternalStateError(
        # DEBUG:         f"Internal Error - function '{visit_method_name}' put unexpected items in 'columns' attribute"
        # DEBUG:     )
        # DEBUG:
        # DEBUG: if return_node.node_type.name != "Scan" and return_node.columns is None:
        # DEBUG:     raise InvalidInternalStateError(
        # DEBUG:         f"Internal Error - function {visit_method_name} did not populate 'columns'"
        # DEBUG:     )

        return return_node, return_context

    # Delegation methods for aggregation operations
    def visit_aggregate_and_group_impl(
        self, node: Node, context: BindingContext
    ) -> Tuple[Node, BindingContext]:
        return visit_aggregate_and_group(self, node, context)

    def visit_distinct_impl(
        self, node: Node, context: BindingContext
    ) -> Tuple[Node, BindingContext]:
        return visit_distinct(self, node, context)

    visit_distinct = visit_distinct_impl

    visit_aggregate_and_group = visit_aggregate_and_group_impl
    visit_aggregate = visit_aggregate_and_group_impl

    # Delegation methods for filter operations
    def visit_filter_impl(self, node: Node, context: BindingContext) -> Tuple[Node, BindingContext]:
        return visit_filter(self, node, context)

    visit_filter = visit_filter_impl

    # Delegation methods for ordering operations
    def visit_order_impl(self, node: Node, context: BindingContext) -> Tuple[Node, BindingContext]:
        return visit_order(self, node, context)

    visit_order = visit_order_impl

    # Delegation methods for join operations
    def visit_join_impl(self, node: Node, context: BindingContext) -> Tuple[Node, BindingContext]:
        # Import locally to avoid circular import at module import time.
        from opteryx.planner.binder.join import visit_join

        return visit_join(self, node, context)

    visit_join = visit_join_impl

    # Delegation methods for projection operations
    def visit_exit_impl(self, node: Node, context: BindingContext) -> Tuple[Node, BindingContext]:
        return visit_exit(self, node, context)

    def visit_project_impl(
        self, node: Node, context: BindingContext
    ) -> Tuple[Node, BindingContext]:
        return visit_project(self, node, context)

    visit_exit = visit_exit_impl
    visit_project = visit_project_impl

    # Delegation methods for dataset operations
    def visit_scan_impl(self, node: Node, context: BindingContext) -> Tuple[Node, BindingContext]:
        return visit_scan(self, node, context)

    def visit_function_dataset_impl(
        self, node: Node, context: BindingContext
    ) -> Tuple[Node, BindingContext]:
        return visit_function_dataset(self, node, context)

    visit_scan = visit_scan_impl
    visit_function_dataset = visit_function_dataset_impl

    # Delegation methods for set operations
    def visit_set_impl(self, node: Node, context: BindingContext) -> Tuple[Node, BindingContext]:
        return visit_set(self, node, context)

    def visit_union_impl(self, node: Node, context: BindingContext) -> Tuple[Node, BindingContext]:
        return visit_union(self, node, context)

    def visit_intersect_impl(self, node: Node, context: BindingContext) -> Tuple[Node, BindingContext]:
        return visit_intersect(self, node, context)

    def visit_except_impl(self, node: Node, context: BindingContext) -> Tuple[Node, BindingContext]:
        return visit_except(self, node, context)

    def visit_unnest_impl(self, node: Node, context: BindingContext) -> Tuple[Node, BindingContext]:
        return visit_unnest(self, node, context)

    visit_set = visit_set_impl
    visit_union = visit_union_impl
    visit_intersect = visit_intersect_impl
    visit_except = visit_except_impl
    visit_unnest = visit_unnest_impl

    # Delegation methods for view operations
    def visit_show_columns_impl(
        self, node: Node, context: BindingContext
    ) -> Tuple[Node, BindingContext]:
        return visit_show_columns(self, node, context)

    def visit_create_view_impl(
        self, node: Node, context: BindingContext
    ) -> Tuple[Node, BindingContext]:
        return visit_create_view(self, node, context)

    def visit_alter_view_impl(
        self, node: Node, context: BindingContext
    ) -> Tuple[Node, BindingContext]:
        return visit_alter_view(self, node, context)

    def visit_drop_view_impl(
        self, node: Node, context: BindingContext
    ) -> Tuple[Node, BindingContext]:
        return visit_drop_view(self, node, context)

    visit_show_columns = visit_show_columns_impl
    visit_create_view = visit_create_view_impl
    visit_alter_view = visit_alter_view_impl
    visit_drop_view = visit_drop_view_impl

    # Delegation methods for subquery operations
    def visit_comment_impl(
        self, node: Node, context: BindingContext
    ) -> Tuple[Node, BindingContext]:
        return visit_comment(self, node, context)

    def visit_subquery_impl(
        self, node: Node, context: BindingContext
    ) -> Tuple[Node, BindingContext]:
        return visit_subquery(self, node, context)

    visit_comment = visit_comment_impl
    visit_subquery = visit_subquery_impl

    # Delegation methods for traversal operations
    def post_bind_impl(self, node):
        return post_bind(self, node)

    def traverse_impl(
        self, graph: LogicalPlan, node: Node, context: BindingContext
    ) -> Tuple[LogicalPlan, BindingContext]:
        return traverse(self, graph, node, context)

    post_bind = post_bind_impl
    traverse = traverse_impl
