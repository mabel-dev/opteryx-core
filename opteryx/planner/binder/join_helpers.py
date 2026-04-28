# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Join helper functions shared by the binder and join visitor.

Isolated here to break the circular import between common.py (which imports join.py)
and join.py (which needs these functions from common.py).
"""

from typing import List, Set, Tuple

from opteryx.exceptions import UnsupportedSyntaxError
from opteryx.expression import NodeType
from opteryx.models import LogicalColumn, Node
from opteryx.types import OrsoTypes


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
        left_relation_names: List[str]
            Names of the left relations.
        right_relation_names: List[str]
            Names of the right relations.

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
