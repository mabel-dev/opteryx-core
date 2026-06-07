# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
# cython: initializedcheck=False

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Expressions describe a calculation or evaluation of some sort.

It is defined as an expression tree of binary and unary operators, and functions.

Expressions are evaluated against an entire morsel at a time.
"""

import datetime
from enum import Enum
from typing import Callable, Dict, List

import draken.draken_native as _draken_native_expr

from opteryx.exceptions import (
    ColumnReferencedBeforeEvaluationError,
    IncorrectTypeError,
    UnsupportedSyntaxError,
)
from opteryx.expression.operations import filter_operations
from opteryx.models import LogicalColumn, Node
from opteryx.types.logical_type import LogicalCategory
from opteryx.types._datetime_conversion import date_to_int64_days, timestamp_to_int64_us
from opteryx.utils import random_string

# These are bit-masks
LOGICAL_TYPE: int = int("00010000", 2)
INTERNAL_TYPE: int = int("00100000", 2)
MAX_COLUMN_BYTE_SIZE: int = 50000000

# ---------------------------------------------------------------------------
# Top-level expression leaves — textually included so the package compiles
# to a single .so. intervals must come before formatter (formatter references
# MICROSECONDS_PER_SECOND). operator_catalog has a lazy `from . import
# NodeType` inside one function, which the umbrella defines below — safe
# because that import only fires at call time, after this module finishes
# initialising.
# ---------------------------------------------------------------------------
include "intervals.pyx"
include "casts.pyx"
include "formatter.pyx"
include "binary_operators.pyx"
include "unary_operations.pyx"
include "operator_catalog.pyx"

__all__ = ("NodeType", "get_all_nodes_of_type")


class NodeType(int, Enum):
    """
    The types of Nodes we will see.

    The second nibble (4 bits) is a category marker, the first nibble is just an
    enumeration of the values in that category.

    This allows us to use bitmasks to add a category to the enumerations.
    """

    # fmt:off

    # 00000000
    UNKNOWN = 0

    # LOGICAL OPERATORS
    # 0001 nnnn
    AND = 17  # 0001 0001
    OR = 18  # 0001 0010
    XOR = 19  # 0001 0011
    NOT = 20  # 0001 0100
    DNF = 21  # 0001 0101 - n-ary AND (parameters list)
    CNF = 22  # 0001 0110 - n-ary OR  (parameters list)

    # INTERAL IDENTIFIERS
    # 0010 nnnn
    CASE = 32  # 0010 0000 — control-flow CASE statement (lazy evaluator)
    WILDCARD = 33  # 0010 0001
    COMPARISON_OPERATOR = 34  # 0010 0010
    BINARY_OPERATOR = 35  # 0010 0011
    UNARY_OPERATOR = 36  # 0010 0100
    FUNCTION = 37  # 0010 0101
    IDENTIFIER = 38  # 0010 0110
    SUBQUERY = 39  # 0010 0111
    NESTED = 40  # 0010 1000
    AGGREGATOR = 41  # 0010 1001
    LITERAL = 42  # 0010 1010
    EXPRESSION_LIST = 43  # 0010 1011 (CASE WHEN)
    EVALUATED = 44  # 0010 1100 - memoize results
    CAST = 45  # 0010 1101 - type casting
    EXTRACTION_OPERATOR = 46  # 0010 1110 - value extraction: ->, ->>, []
    BETWEEN = 47  # 0010 1111 - range comparison: lower <= col <= upper (optimizer-created)


def _typed_constant_vector(value, length: int, schema_column):
    """
    Create a typed constant-encoded Draken vector when the output type is known.

    Returns `None` when the type is not yet supported by typed constant encoding.
    """
    if schema_column is None or length < 0:
        return None

    target_type = getattr(schema_column, "category", None)
    is_null = value is None

    if target_type == LogicalCategory.BOOLEAN:
        from draken.vectors.bool_vector import BoolVector

        return BoolVector.from_constant(False if is_null else value, length, is_null=is_null)

    if target_type == LogicalCategory.INTEGER:
        return _draken_native_expr.vector_int32_from_constant(
            None if is_null else int(value), length
        )

    if target_type == LogicalCategory.FLOAT:
        return _draken_native_expr.vector_float64_from_constant(
            None if is_null else float(value), length
        )

    if target_type == LogicalCategory.VARBINARY:
        # Explicit binary: opaque bytes, VARBINARY tag.
        return _draken_native_expr.vector_varbinary_from_constant(
            None if is_null else value, length
        )

    if target_type == LogicalCategory.VARCHAR:
        # VARCHAR carries raw bytes; the constant ctor stores str/bytes verbatim
        # (no decode), so non-UTF-8 literal data is preserved.
        return _draken_native_expr.vector_varchar_from_constant(
            None if is_null else value, length
        )

    if target_type == LogicalCategory.DATE:
        if not is_null:
            if isinstance(value, datetime.datetime):
                value = value.date()
            if isinstance(value, int):
                # ordinal days since epoch → datetime.date
                value = datetime.date(1970, 1, 1) + datetime.timedelta(days=value)
            elif not isinstance(value, datetime.date):
                value = datetime.date.fromisoformat(str(value))
        return _draken_native_expr.vector_date32_from_constant(
            None if is_null else value, length
        )

    if target_type == LogicalCategory.TIMESTAMP:
        if not is_null:
            if isinstance(value, datetime.datetime):
                pass  # already correct type
            elif isinstance(value, int):
                # microseconds since epoch → datetime.datetime
                value = datetime.datetime(1970, 1, 1) + datetime.timedelta(microseconds=value)
            else:
                value = datetime.datetime.fromisoformat(str(value))
        return _draken_native_expr.vector_timestamp_from_constant(
            None if is_null else value, length
        )

    if target_type == LogicalCategory.TIME:
        if not is_null:
            if isinstance(value, datetime.time):
                value = (
                    value.hour * 3_600_000_000
                    + value.minute * 60_000_000
                    + value.second * 1_000_000
                    + value.microsecond
                )
            else:
                value = int(value)
        return _draken_native_expr.vector_time64_from_constant(
            None if is_null else int(value), length
        )

    if target_type == LogicalCategory.DECIMAL:
        return _draken_native_expr.vector_decimal_from_constant(
            None if is_null else value, length
        )

    return None


LOGICAL_OPERATIONS: Dict[NodeType, Callable] = {}


def _filter_indices_by_mask(indices, mask):
    """
    Filter indices based on a boolean mask.

    Args:
        indices: list of indices [0, 1, 2, ..., n-1]
        mask: list of boolean values (True/False/None)

    Returns:
        list of indices where mask is True
    """
    return [i for i, m in zip(indices, mask) if m is True]


def _invert_bool_mask(mask):
    """
    Invert a boolean mask, preserving None values.

    Args:
        mask: list of boolean values (True/False/None)

    Returns:
        list where True→False, False→True, None→None
    """
    result = []
    for m in mask:
        if m is None:
            result.append(None)
        else:
            result.append(not m)
    return result


def _is_null_mask(values):
    """
    Create a null mask from values.

    Args:
        values: list of values (may contain None)

    Returns:
        list of boolean values indicating which positions are None
    """
    return [v is None for v in values]


def _restore_nulls(result, null_mask):
    """
    Restore null values in result based on null_mask.

    Args:
        result: list of boolean values
        null_mask: list indicating which positions were originally null

    Returns:
        list where null positions are set to None
    """
    if not any(null_mask):
        return result

    result_obj = [v for v in result]
    for i, is_null in enumerate(null_mask):
        if is_null:
            result_obj[i] = None
    return result_obj


def prioritize_evaluation(expressions):
    non_dependent_expressions = []
    dependent_expressions = []

    for expression in expressions:
        if not get_all_nodes_of_type(expression, (NodeType.EVALUATED,)):
            non_dependent_expressions.append(expression)
        else:
            dependent_expressions.append(expression)

    # Now that we have split the expressions into non-dependent and dependent,
    # we can return them in the desired order of evaluation.
    return non_dependent_expressions + dependent_expressions



def get_all_nodes_of_type(root, select_nodes: tuple) -> list:
    """
    Walk an expression tree collecting all nodes of a specified type.
    """
    if root is None:
        return []
    if not isinstance(root, (set, tuple, list)):
        root = [root]

    # Prepare to collect all nodes if select_nodes is ('*',), else convert to a set
    collect_all = "*" in select_nodes
    select_nodes_set = set(select_nodes) if not collect_all else set()

    identifiers = []
    stack = list(root)
    appender = stack.append

    while stack:
        node = stack.pop()

        # Check whether to collect the node
        if collect_all or node.node_type in select_nodes_set:
            identifiers.append(node)

        # Append parameters if they are valid nodes
        if node.parameters:
            stack.extend(
                [param for param in node.parameters if isinstance(param, (Node, LogicalColumn))]
            )

        # NodeType.CASE uses conditions/results/else_result instead of parameters
        if node.node_type == NodeType.CASE:
            if node.conditions:
                stack.extend(c for c in node.conditions if isinstance(c, (Node, LogicalColumn)))
            if node.results:
                stack.extend(r for r in node.results if isinstance(r, (Node, LogicalColumn)))
            if node.else_result is not None and isinstance(node.else_result, (Node, LogicalColumn)):
                appender(node.else_result)

        # Append child nodes
        child = node.right
        if child:
            appender(child)
        child = node.centre
        if child:
            appender(child)
        child = node.left
        if child:
            appender(child)

    return identifiers


def should_evaluate(statement):
    """Determine if the given statement should be evaluated.

    Skips nodes that are already-resolved column references (IDENTIFIER, AGGREGATOR,
    EVALUATED) or structural placeholders (WILDCARD, SUBQUERY).
    """
    valid_node_types = {
        NodeType.CASE,
        NodeType.FUNCTION,
        NodeType.CAST,
        NodeType.BINARY_OPERATOR,
        NodeType.EXTRACTION_OPERATOR,
        NodeType.COMPARISON_OPERATOR,
        NodeType.UNARY_OPERATOR,
        NodeType.NESTED,
        NodeType.NOT,
        NodeType.AND,
        NodeType.OR,
        NodeType.XOR,
        NodeType.LITERAL,
    }
    return statement.node_type in valid_node_types




# Fail-fast: keep the Cython DEF constants in evaluation.pyx in sync with the
# NodeType enum values above. Runs at module import; if it ever fires, fix the
# DEFs at the top of opteryx/expression/evaluator/evaluation.pyx and rebuild.
from opteryx.expression.evaluator import _verify_node_type_constants
_verify_node_type_constants()
del _verify_node_type_constants


# Submodule-alias shims so legacy `from opteryx.expression.LEAF import name`
# imports keep working after consolidation. Each alias points at this same
# module — the leaf names are already in this namespace via the includes.
import sys as _sys
_self = _sys.modules[__name__]
for _leaf in (
    "binary_operators",
    "casts",
    "formatter",
    "intervals",
    "operator_catalog",
    "unary_operations",
):
    globals()[_leaf] = _self
    _sys.modules[f"{__name__}.{_leaf}"] = _self
del _leaf, _self, _sys
