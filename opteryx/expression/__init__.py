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

from opteryx.exceptions import (
    ColumnReferencedBeforeEvaluationError,
    IncorrectTypeError,
    UnsupportedSyntaxError,
)
from opteryx.expression.binary_operators import binary_operations
from opteryx.expression.evaluator import apply_bounded_function
from opteryx.expression.operations import filter_operations
from opteryx.expression.unary_operations import UNARY_OPERATIONS
from opteryx.models import LogicalColumn, Node
from opteryx.types import OrsoTypes
from opteryx.types._datetime_conversion import date_to_int64_days, timestamp_to_int64_us
from opteryx.utils import random_string

from .formatter import (
    ExpressionColumn,  # this is used
    format_expression,
)

# These are bit-masks
LOGICAL_TYPE: int = int("00010000", 2)
INTERNAL_TYPE: int = int("00100000", 2)
MAX_COLUMN_BYTE_SIZE: int = 50000000

__all__ = ("NodeType", "evaluate", "evaluate_and_append", "get_all_nodes_of_type")


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

    target_type = getattr(schema_column, "type", None)
    is_null = value is None

    if target_type == OrsoTypes.BOOLEAN:
        from draken.vectors.bool_vector import BoolVector

        return BoolVector.from_constant(False if is_null else value, length, is_null=is_null)

    if target_type == OrsoTypes.INTEGER:
        from draken.vectors.integer_vector import IntegerVector

        return IntegerVector.from_constant(0 if is_null else value, length, is_null=is_null)

    if target_type == OrsoTypes.DOUBLE:
        from draken.vectors.float64_vector import Float64Vector

        return Float64Vector.from_constant(0.0 if is_null else value, length, is_null=is_null)

    if target_type in (OrsoTypes.VARCHAR, OrsoTypes.BLOB):
        from draken.vectors.string_vector import StringVector

        return StringVector.from_constant(b"" if is_null else value, length, is_null=is_null)

    if target_type == OrsoTypes.DATE:
        from draken.vectors.date32_vector import Date32Vector

        if not is_null:
            if isinstance(value, datetime.datetime):
                value = value.date()
            if isinstance(value, datetime.date):
                value = (value - datetime.date(1970, 1, 1)).days
            elif not isinstance(value, int):
                try:
                    value = (
                        datetime.date.fromisoformat(str(value)) - datetime.date(1970, 1, 1)
                    ).days
                except (ValueError, TypeError):
                    value = int(value)
        return Date32Vector.from_constant(0 if is_null else value, length, is_null=is_null)

    if target_type == OrsoTypes.TIMESTAMP:
        from draken.vectors.timestamp_vector import TimestampVector

        # Default to microsecond precision for constant-encoded timestamps
        timestamp_unit = "us"
        if not is_null:
            value = timestamp_to_int64_us(value)
        return TimestampVector.from_constant(
            0 if is_null else value,
            length,
            is_null=is_null,
            timestamp_unit=timestamp_unit,
        )

    if target_type == OrsoTypes.TIME:
        from draken.vectors.time_vector import TimeVector

        # Default to time64 (microsecond precision) for constant-encoded times
        is_time64 = True
        if not is_null:
            if isinstance(value, datetime.time):
                if is_time64:
                    value = (
                        value.hour * 3_600_000_000
                        + value.minute * 60_000_000
                        + value.second * 1_000_000
                        + value.microsecond
                    )
                else:
                    value = value.hour * 3600 + value.minute * 60 + value.second
            else:
                value = int(value)
        return TimeVector.from_constant(
            0 if is_null else value,
            length,
            is_null=is_null,
            is_time64=is_time64,
        )

    if target_type == OrsoTypes.DECIMAL:
        from draken.vectors._decimal_vector import DecimalVector

        return DecimalVector.from_constant(None if is_null else value, length, is_null=is_null)

    return None


LOGICAL_OPERATIONS: Dict[NodeType, Callable] = {}


def evaluate_dnf(expressions: List[Node], table) -> list:
    num_rows = table.num_rows
    true_indices = list(range(num_rows))
    working_table = table

    for i, predicate in enumerate(expressions):
        result = evaluate(predicate, working_table)

        # Convert to Python list
        if hasattr(result, "to_pylist"):
            result_bool = result.to_pylist()
        else:
            result_bool = list(result)

        # Convert to bool list, treating None as False for filtering
        result_bool = [bool(v) if v is not None else False for v in result_bool]

        if not any(result_bool):
            return [False] * num_rows

        # Filter indices where result is True
        true_indices = [idx for idx, res in zip(true_indices, result_bool) if res]

        if i < len(expressions) - 1:
            working_table = table.take(true_indices)

    # Create final result list
    final_result = [False] * num_rows
    for idx in true_indices:
        final_result[idx] = True
    return final_result


def evaluate_cnf(expressions: List[Node], table) -> list:
    num_rows = table.num_rows
    result = [False] * num_rows

    for predicate in expressions:
        branch = evaluate(predicate, table)
        if hasattr(branch, "to_pylist"):
            branch = branch.to_pylist()
        else:
            branch = list(branch)
        for i, v in enumerate(branch):
            if v:
                result[i] = True
        if all(result):
            return result

    return result


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


def short_cut_and(root, table):
    # Evaluate left expression
    left_result = evaluate(root.left, table)

    # Convert to Python list if needed
    if hasattr(left_result, "to_pylist"):
        left_result = left_result.to_pylist()
    else:
        left_result = list(left_result)

    # Track null positions before coercing to bool
    null_mask = _is_null_mask(left_result)

    # Convert to boolean, treating None as False for masking purposes
    bool_result = [bool(v) if v is not None else False for v in left_result]

    # If all values in left_result are False, no need to evaluate the right expression
    if not any(bool_result):
        return _restore_nulls(bool_result, null_mask)

    # Find indices where left_result is True (for subset evaluation)
    true_indices = [i for i, v in enumerate(bool_result) if v]

    # Create a subset table for evaluating the right expression
    subset_table = table.take(true_indices)

    # Evaluate right expression on the subset table
    right_result = evaluate(root.right, subset_table)

    # Convert to Python list if needed
    if hasattr(right_result, "to_pylist"):
        right_result = right_result.to_pylist()
    else:
        right_result = list(right_result)

    # Combine results: copy bool_result and update true_indices positions
    combined = [v for v in bool_result]
    for i, idx in enumerate(true_indices):
        combined[idx] = right_result[i]

    # Restore nulls from left operand
    return _restore_nulls(combined, null_mask)


def short_cut_or(root, table):
    # Evaluate left expression
    left_result = evaluate(root.left, table)

    # Convert to Python list if needed
    if hasattr(left_result, "to_pylist"):
        left_result = left_result.to_pylist()
    else:
        left_result = list(left_result)

    # Track null positions before coercing to bool
    null_mask = _is_null_mask(left_result)

    # Convert to boolean, treating None as False for masking purposes
    bool_result = [bool(v) if v is not None else False for v in left_result]

    # If all values in left_result are True, short-circuit (no need to evaluate right)
    if all(bool_result):
        return _restore_nulls(bool_result, null_mask)

    # Find indices where left_result is False (need to evaluate right)
    false_indices = [i for i, v in enumerate(bool_result) if not v]

    if not false_indices:
        return _restore_nulls(bool_result, null_mask)

    # Create a subset table for evaluating the right expression
    subset_table = table.take(false_indices)

    # Evaluate right expression on the subset table
    right_result = evaluate(root.right, subset_table)

    # Convert to Python list if needed
    if hasattr(right_result, "to_pylist"):
        right_result = right_result.to_pylist()
    else:
        right_result = list(right_result)

    # Combine results: copy bool_result and update false_indices positions with OR logic
    combined = [v for v in bool_result]
    for i, idx in enumerate(false_indices):
        combined[idx] = bool_result[idx] or right_result[i]

    # Restore nulls from left operand
    return _restore_nulls(combined, null_mask)


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


def _inner_evaluate(root: Node, table):
    node_type = root.node_type  # type: ignore

    if node_type == NodeType.DNF:
        return evaluate_dnf(root.parameters, table)

    if node_type == NodeType.CNF:
        return evaluate_cnf(root.parameters, table)

    if node_type == NodeType.SUBQUERY:
        raise UnsupportedSyntaxError("IN (<subquery>) temporarily not supported.")

    identity = root.schema_column.identity if root.schema_column else random_string().encode("utf-8")

    # if we have this column already, return it from the Morsel
    # Morsels use bytes for column names; identity is now always bytes
    col_names = table.column_names

    if identity in col_names:
        col = table.column(identity)
        return col

    # LITERAL TYPES - return Draken constant vectors
    if node_type == NodeType.LITERAL:
        from draken.vectors.bool_vector import BoolVector
        from draken.vectors.float64_vector import Float64Vector
        from draken.vectors.int64_vector import Int64Vector
        from draken.vectors.string_vector import StringVector

        literal_type = root.type or (
            root.schema_column.type if getattr(root, "schema_column", None) else None
        )
        value = root.value
        length = table.num_rows

        # Normalize NumPy scalar values before constructing constant vectors
        if hasattr(value, "item") and not isinstance(value, (bytes, bytearray, str)):
            try:
                value = value.item()
            except Exception:
                pass

        if literal_type == OrsoTypes.DATE:
            if isinstance(value, datetime.date):
                value = date_to_int64_days(value)
            elif isinstance(value, int):
                pass
            else:
                value = int(value)
            return Int64Vector.from_constant(value, length)

        if literal_type == OrsoTypes.TIMESTAMP:
            if isinstance(value, datetime.datetime) or isinstance(value, datetime.date):
                value = timestamp_to_int64_us(value)
            elif not isinstance(value, int):
                value = int(value)
            return Int64Vector.from_constant(value, length)

        if literal_type == OrsoTypes.INTEGER:
            return Int64Vector.from_constant(int(value), length)

        if literal_type == OrsoTypes.DOUBLE:
            return Float64Vector.from_constant(float(value), length)

        if literal_type == OrsoTypes.BOOLEAN:
            return BoolVector.from_constant(bool(value), length)

        if literal_type == OrsoTypes.VARCHAR:
            return StringVector.from_constant(value, length)

        if literal_type == OrsoTypes.BLOB:
            return StringVector.from_constant(value, length)

        if literal_type in (OrsoTypes.ARRAY, OrsoTypes.VECTOR):
            from draken.interop.vector_sequence import vector_from_sequence

            return vector_from_sequence([value] * length)

        if literal_type == OrsoTypes.INTERVAL:
            return [value] * length

        return [value]

    # BOOLEAN OPERATORS
    if node_type & LOGICAL_TYPE == LOGICAL_TYPE:  # type: ignore
        if node_type == NodeType.OR:
            return short_cut_or(root, table)
        if node_type == NodeType.AND:
            return short_cut_and(root, table)

        if node_type in LOGICAL_OPERATIONS:
            left = _inner_evaluate(root.left, table) if root.left else None
            right = _inner_evaluate(root.right, table) if root.right else None

            if left.__class__.__name__ != "BoolVector":
                raise TypeError(
                    f"Boolean operator `{node_type}` requires BoolVector inputs; got {type(left).__name__}"
                )
            if right.__class__.__name__ != "BoolVector":
                raise TypeError(
                    f"Boolean operator `{node_type}` requires BoolVector inputs; got {type(right).__name__}"
                )

            if node_type == NodeType.AND:
                return left.and_vector(right)
            if node_type == NodeType.OR:
                return left.or_vector(right)
            if node_type == NodeType.XOR:
                return left.xor_vector(right)
            raise NotImplementedError(f"Boolean operator `{node_type}` is not implemented")

        if node_type == NodeType.NOT:
            if root.centre:
                centre = _inner_evaluate(root.centre, table)
            else:
                from draken.vectors.bool_vector import BoolVector

                centre = BoolVector.from_constant(None, 1, is_null=True)
            if centre.__class__.__name__ == "BoolVector":
                return centre.xor_vector(centre.not_vector())
            raise TypeError(f"Boolean NOT requires BoolVector input; got {type(centre).__name__}")

    # INTERAL IDENTIFIERS
    if node_type == NodeType.CASE:
        from opteryx.expression.evaluator.case_eval import evaluate_case
        from draken.morsels.morsel import Morsel as _Morsel
        if not isinstance(table, _Morsel):
            raise TypeError(
                f"_inner_evaluate: NodeType.CASE requires a Draken Morsel; got {type(table).__name__}"
            )
        return evaluate_case(root, table)
    if node_type & INTERNAL_TYPE == INTERNAL_TYPE:  # type: ignore
        if node_type == NodeType.FUNCTION:
            if root.value == "_PASSTHRU":
                # PASSTHRU is an optimizer-created identity wrapper (no function_ref).
                # Just evaluate the inner parameter and return it.
                return _inner_evaluate(root.parameters[0], table)
            parameters = [_inner_evaluate(param, table) for param in root.parameters]
            # zero parameter functions get the number of rows as the parameter
            if len(parameters) == 0:
                parameters = [table.num_rows]

            result = apply_bounded_function(root, *parameters)
            # Normalize function outputs to Draken vectors for morsel compatibility.
            from opteryx.utils.vector_types import is_draken_vector

            if not is_draken_vector(result):
                raise TypeError(
                    "FUNCTION evaluation expected Draken vector result; "
                    f"got {type(result).__name__}."
                )
            return result
        if node_type == NodeType.CAST:
            # Handle CAST operations (CAST(expr AS type), TRY_CAST, SAFE_CAST)
            from opteryx.expression.casts import cast

            # Evaluate source expression
            source = _inner_evaluate(root.left, table)

            # Get the target type name (remove TRY_ prefix if present for TRY_CAST/SAFE_CAST)
            target_type = root.value[4:] if root.value.startswith("TRY_") else root.value

            # Extract unit from internal temporal type forms (e.g., "_TIMESTAMP_MS" → unit="ms")
            # These forms come from the SQL rewriter, which converts user syntax like TIMESTAMP[ms]
            unit = None
            unit_map = {
                "_TIMESTAMP_NS": ("TIMESTAMP", "ns"),
                "_TIMESTAMP_MS": ("TIMESTAMP", "ms"),
                "_TIMESTAMP_S": ("TIMESTAMP", "s"),
                "_TIMESTAMP_US": ("TIMESTAMP", "us"),
                "_TIMESTAMP_DAYS": ("TIMESTAMP", "days"),
            }
            if target_type in unit_map:
                canonical_type, unit = unit_map[target_type]
                target_type = canonical_type

            # Handle optional precision/scale/length parameters from node.parameters
            params = []
            if root.parameters:
                # Parameters were already bound by binder if needed
                params = [_inner_evaluate(param, table) for param in root.parameters]

            # Get the cast kernel - cast() is a factory that returns a callable
            kernel = cast(None, target_type, tuple(params), unit=unit)

            # Apply the cast kernel to the source
            result = kernel(source)

            from opteryx.utils.vector_types import is_draken_vector
            if not is_draken_vector(result):
                raise TypeError(
                    "CAST evaluation expected Draken vector result; "
                    f"got {type(result).__name__}."
                )

            # Propagate Draken vectors directly; keep fallback values native.
            return result
        if node_type == NodeType.AGGREGATOR:
            # detected as an aggregator, but here it's an identifier because it
            # will have already been evaluated
            node_type = NodeType.EVALUATED
            root.value = format_expression(root)
            root.node_type = NodeType.EVALUATED
        if node_type == NodeType.EVALUATED:
            # Get the column from the Morsel
            col_names = table.column_names
            col_identity = root.schema_column.identity

            if col_identity not in col_names:
                raise ColumnReferencedBeforeEvaluationError(column=root.schema_column.name)

            col = table.column(col_identity)
            return col
        if node_type == NodeType.COMPARISON_OPERATOR:
            if (
                root.left.node_type == NodeType.LITERAL
                and root.right.node_type == NodeType.LITERAL
                and root.left.type in (OrsoTypes.DATE, OrsoTypes.TIMESTAMP)
                and root.right.type in (OrsoTypes.DATE, OrsoTypes.TIMESTAMP)
            ):

                def _literal_temporal_value(node):
                    value = node.value
                    if node.type == OrsoTypes.DATE:
                        if isinstance(value, int):
                            return datetime.datetime(1970, 1, 1) + datetime.timedelta(
                                days=int(value)
                            )
                        if isinstance(value, datetime.datetime):
                            return value.replace(hour=0, minute=0, second=0, microsecond=0)
                        if isinstance(value, datetime.date):
                            return datetime.datetime(value.year, value.month, value.day)
                    if node.type == OrsoTypes.TIMESTAMP:
                        if isinstance(value, int):
                            ivalue = int(value)
                            if abs(ivalue) < 100_000_000_000 and ivalue % 1_000_000 == 0:
                                return datetime.datetime(1970, 1, 1) + datetime.timedelta(
                                    days=ivalue // 1_000_000
                                )
                            return datetime.datetime(1970, 1, 1) + datetime.timedelta(
                                microseconds=ivalue
                            )
                        if isinstance(value, datetime.date) and not isinstance(
                            value, datetime.datetime
                        ):
                            return datetime.datetime(value.year, value.month, value.day)
                    return value

                left_value = _literal_temporal_value(root.left)
                right_value = _literal_temporal_value(root.right)
                _cmp = {
                    "Eq": left_value == right_value,
                    "NotEq": left_value != right_value,
                    "Lt": left_value < right_value,
                    "Gt": left_value > right_value,
                    "LtEq": left_value <= right_value,
                    "GtEq": left_value >= right_value,
                }[root.value]
                return [_cmp] * table.num_rows

            right = None
            left = None

            if root.right.node_type == NodeType.LITERAL:
                right = [root.right.value]

            if right is None:
                if root.right.node_type == NodeType.IDENTIFIER:
                    # Get the column from the Morsel; identity is bytes
                    col = table.column(root.right.schema_column.identity)
                    right = col
                else:
                    right = _inner_evaluate(root.right, table)
            if left is None:
                if root.left.node_type == NodeType.IDENTIFIER:
                    # Get the column from the Morsel; identity is bytes
                    col = table.column(root.left.schema_column.identity)
                    left = col
                else:
                    left = _inner_evaluate(root.left, table)

            result = filter_operations(
                left,
                root.left.schema_column.type,
                root.value,
                right,
                root.right.schema_column.type,
            )
            return result
        if node_type == NodeType.BINARY_OPERATOR:
            left = _inner_evaluate(root.left, table)
            right = _inner_evaluate(root.right, table)
            result = binary_operations(
                left,
                root.left.schema_column.type,
                root.value,
                right,
                root.right.schema_column.type,
            )
            return result
        if node_type == NodeType.EXTRACTION_OPERATOR:
            left = _inner_evaluate(root.left, table)
            right = _inner_evaluate(root.right, table)
            result = binary_operations(
                left,
                root.left.schema_column.type,
                root.value,
                right,
                root.right.schema_column.type,
            )
            return result
        if node_type == NodeType.WILDCARD:
            return ["*"] * table.num_rows
        if node_type == NodeType.SUBQUERY:
            raise UnsupportedSyntaxError(
                "Subqueries must be planned away before reaching expression evaluation."
            )
        if node_type == NodeType.NESTED:
            return _inner_evaluate(root.centre, table)
        if node_type == NodeType.UNARY_OPERATOR:
            centre = _inner_evaluate(root.centre, table)
            result = UNARY_OPERATIONS[root.value](centre)
            return result
        if node_type == NodeType.EXPRESSION_LIST:
            values = [_inner_evaluate(val, table) for val in root.parameters]
            return values
        from opteryx.exceptions import ColumnNotFoundError

        raise ColumnNotFoundError(
            message=f"Unable to locate column '{root.source_column}' this is likely due to differences in SELECT and GROUP BY clauses."
        )


def evaluate(expression: Node, table):
    result = _inner_evaluate(root=expression, table=table)
    if result.__class__.__name__ == "BoolVector":
        return result
    from opteryx.utils.vector_types import is_draken_vector

    if is_draken_vector(result):
        return result
    return result


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

    Used by the Cython evaluate_and_append_draken to skip nodes that are
    already-resolved column references (IDENTIFIER, AGGREGATOR, EVALUATED)
    or structural placeholders (WILDCARD, SUBQUERY).
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


# evaluate_and_append is now a thin wrapper over the Cython evaluator. The
# legacy _evaluate_and_append_morsel / evaluate_statement / is_mask / create_mask
# chain was retired when projection, sort, heap_sort moved to evaluate_and_append_draken;
# only this top-level name is kept for backwards-compatibility with test fixtures
# and any external embedders.
def evaluate_and_append(expressions, table):
    from opteryx.expression.evaluator import evaluate_and_append_draken
    return evaluate_and_append_draken(expressions, table)


# Fail-fast: keep the Cython DEF constants in evaluation.pyx in sync with the
# NodeType enum values above. Runs at module import; if it ever fires, fix the
# DEFs at the top of opteryx/expression/evaluator/evaluation.pyx and rebuild.
from opteryx.expression.evaluator import _verify_node_type_constants
_verify_node_type_constants()
del _verify_node_type_constants
