# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
This is both a marker and a wrapper for key functionality to support predicate/filter
pushdowns. This is where we a sending filters to the thing that is acquiring the data
for the query. For example sending filters to remote database servers, or to pyarrow
readers. This allows for data to be prefiltered before reaching Opteryx - this is
almost always going to be faster than reading, loading and filtering.

Note that for some file types, although we accept the pushdown, we fake it by reading,
loading and filtering. We do this because we have a single file interface and some
accept filters and others don't so we 'fake' the read-time filtering.
"""

import datetime
from typing import Dict

from opteryx.exceptions import NotSupportedError
from opteryx.expression import NodeType, get_all_nodes_of_type
from opteryx.models import Node
from opteryx.types.logical_type import LogicalCategory
from opteryx.utils import single_item_cache


class PredicatePushable:
    PUSHABLE_OPS: Dict[str, bool] = {
        "Eq": False,
        "NotEq": False,
        "Gt": False,
        "GtEq": False,
        "Lt": False,
        "LtEq": False,
        "Like": False,
        "NotLike": False,
        "Between": False,
        "IsNull": False,
        "IsNotNull": False,
        "IsEmpty": False,
        "IsNotEmpty": False,
    }

    OPS_XLAT: Dict[str, str] = {
        "Eq": "=",
        "NotEq": "!=",
        "Gt": ">",
        "GtEq": ">=",
        "Lt": "<",
        "LtEq": "<=",
        "Like": "LIKE",
        "NotLike": "NOT LIKE",
    }

    PUSHABLE_TYPES: set = {t for t in LogicalCategory}

    # Node kinds a pushed predicate may be built from. A boolean-returning FUNCTION
    # is itself a predicate, so FUNCTION joins the set for that shape only — CASE and
    # everything else stay out.
    _SIMPLE_NODE_TYPES = (
        NodeType.IDENTIFIER,
        NodeType.LITERAL,
        NodeType.COMPARISON_OPERATOR,
        NodeType.BETWEEN,
        NodeType.UNARY_OPERATOR,
    )

    def can_push(self, operator: Node, types: set = None) -> bool:
        condition = operator.condition
        # Boolean-returning functions are their own predicate (LIKE lowers to
        # _STARTS_WITH / InStr / ... this way). They still have to satisfy the node
        # and type gates: returning True here unconditionally let a CASE, and a
        # DECIMAL column the connector never declared it could handle, through into
        # the scan.
        is_boolean_function = (
            condition.node_type == NodeType.FUNCTION
            and getattr(getattr(condition, "schema_column", None), "category", None)
            == LogicalCategory.BOOLEAN
        )

        # we can only push simple expressions
        allowed_node_types = self._SIMPLE_NODE_TYPES
        if is_boolean_function:
            allowed_node_types = allowed_node_types + (NodeType.FUNCTION,)
        all_nodes = get_all_nodes_of_type(condition, ("*",))
        if any(n.node_type not in allowed_node_types for n in all_nodes):
            return False

        # we can only push certain types. `types` is derived by the caller from the
        # condition's left/right legs, which are None for a FUNCTION — so read the
        # categories off every referenced column rather than trusting that alone.
        column_types = {
            n.schema_column.category
            for n in all_nodes
            if n.node_type == NodeType.IDENTIFIER and n.schema_column is not None
        }
        effective_types = set(types or ()) | column_types
        if effective_types and not effective_types.issubset(self.PUSHABLE_TYPES):
            return False

        if is_boolean_function:
            return True

        # we can only push certain operators
        # BETWEEN nodes store inclusivity flags in .value (a tuple), not a string op name
        op_key = "Between" if condition.node_type == NodeType.BETWEEN else condition.value
        return self.PUSHABLE_OPS.get(op_key, False)

    def __init__(self, **kwargs):
        pass

    @staticmethod
    @single_item_cache
    def to_dnf(root):
        """
        Convert a filter to DNF form, this is the form used by PyArrow.

        This is specifically opinionated for the Parquet reader for PyArrow.
        """

        def _predicate_to_dnf(root):
            # Reduce look-ahead effort by using Exceptions to control flow
            if root.node_type == NodeType.AND:  # pragma: no cover
                left = _predicate_to_dnf(root.left)
                right = _predicate_to_dnf(root.right)
                if not isinstance(left, list):
                    left = [left]
                if not isinstance(right, list):
                    right = [right]
                left.extend(right)
                return left
            if root.node_type != NodeType.COMPARISON_OPERATOR:
                raise NotSupportedError()

            # If identifier is on the right, swap sides and invert operator
            op = root.value
            if root.left.node_type != NodeType.IDENTIFIER:
                root.left, root.right = root.right, root.left
                INVERT_OP = {
                    "Gt": "Lt",
                    "GtEq": "LtEq",
                    "Lt": "Gt",
                    "LtEq": "GtEq",
                    "Eq": "Eq",
                    "NotEq": "NotEq",
                    "InList": "InList",
                    "NotInList": "NotInList",
                }
                op = INVERT_OP.get(op, op)

            from opteryx.types.logical_type import TIMESTAMP, VARBINARY

            if root.right.schema_column.category == LogicalCategory.DATE:
                date_val = root.right.value
                if getattr(date_val, "item", None) is not None:
                    date_val = date_val.item()
                root.right.value = datetime.datetime.combine(date_val, datetime.time.min)
                root.right.schema_column.column_type = TIMESTAMP()
            if root.left.node_type != NodeType.IDENTIFIER:
                raise NotSupportedError()
            if root.right.node_type != NodeType.LITERAL:
                raise NotSupportedError()
            if root.left.schema_column.category == LogicalCategory.VARCHAR:
                root.left.schema_column.column_type = VARBINARY
            if root.right.schema_column.category == LogicalCategory.VARCHAR:
                root.right.schema_column.column_type = VARBINARY
            if root.right.schema_column.category != root.left.schema_column.category:
                raise NotSupportedError()
            return (
                root.left.value,
                PredicatePushable.OPS_XLAT[op],
                root.right.value,
            )

        not_converted = []
        dnf = []
        if not isinstance(root, list):
            root = [root]
        for predicate in root:
            try:
                converted = _predicate_to_dnf(predicate)
                dnf.append(converted)
            except NotSupportedError:
                not_converted.append(predicate)
        return dnf if dnf else None, not_converted
