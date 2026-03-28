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
from typing import Callable
from typing import Dict
from typing import List

import numpy
import pyarrow
from opteryx.exceptions import ColumnReferencedBeforeEvaluationError
from opteryx.exceptions import IncorrectTypeError
from opteryx.exceptions import UnsupportedSyntaxError
from opteryx.expression.binary_operators import binary_operations
from opteryx.expression.evaluator import apply_bounded_function
from opteryx.expression.operations import filter_operations
from opteryx.expression.unary_operations import UNARY_OPERATIONS
from opteryx.models import LogicalColumn
from opteryx.models import Node
from orso.tools import random_string
from orso.types import OrsoTypes
from pyarrow import Table
from pyarrow import compute

from .formatter import ExpressionColumn  # this is used
from .formatter import format_expression

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
    DNF = 21  # 0001 0101

    # INTERAL IDENTIFIERS
    # 0010 nnnn
    WILDCARD = 33  # 0010 0001
    COMPARISON_OPERATOR = 34  # 0010 0010
    BINARY_OPERATOR = 35  # 0010 0011
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


def _arrow_type_for_schema_column(schema_column):
    if schema_column is None:
        return None
    if getattr(schema_column, "type", None) == OrsoTypes.VECTOR:
        return pyarrow.list_(pyarrow.float64())
    arrow_field = getattr(schema_column, "arrow_field", None)
    return getattr(arrow_field, "type", None)


def _typed_constant_vector(value, length: int, schema_column):
    """
    Create a typed constant-encoded Draken vector when the output type is known.

    Returns `None` when the type is not yet supported by typed constant encoding,
    allowing callers to fall back to Arrow materialization if the type is still unknown.
    """
    if schema_column is None or length < 0:
        return None

    target_type = getattr(schema_column, "type", None)
    arrow_type = _arrow_type_for_schema_column(schema_column)
    is_null = value is None

    if target_type == OrsoTypes.BOOLEAN:
        from opteryx.compiled.draken.vectors.bool_vector import BoolVector

        return BoolVector.from_constant(False if is_null else value, length, is_null=is_null)

    if target_type == OrsoTypes.INTEGER:
        from opteryx.compiled.draken.vectors.integer_vector import IntegerVector

        return IntegerVector.from_constant(0 if is_null else value, length, is_null=is_null)

    if target_type == OrsoTypes.DOUBLE:
        from opteryx.compiled.draken.vectors.float64_vector import Float64Vector

        return Float64Vector.from_constant(0.0 if is_null else value, length, is_null=is_null)

    if target_type in (OrsoTypes.VARCHAR, OrsoTypes.BLOB):
        from opteryx.compiled.draken.vectors.string_vector import StringVector

        return StringVector.from_constant(b"" if is_null else value, length, is_null=is_null)

    if target_type == OrsoTypes.DATE:
        from opteryx.compiled.draken.vectors.date32_vector import Date32Vector

        if not is_null:
            if isinstance(value, datetime.datetime):
                value = value.date()
            if isinstance(value, numpy.datetime64):
                value = value.astype("datetime64[D]").astype(numpy.int64)
            if isinstance(value, datetime.date):
                value = (value - datetime.date(1970, 1, 1)).days
            else:
                value = (
                    pyarrow.array([value], type=pyarrow.date32()).cast(pyarrow.int32())[0].as_py()
                )
        return Date32Vector.from_constant(0 if is_null else value, length, is_null=is_null)

    if target_type == OrsoTypes.TIMESTAMP:
        from opteryx.compiled.draken.vectors.timestamp_vector import TimestampVector

        timestamp_type = (
            arrow_type if pyarrow.types.is_timestamp(arrow_type) else pyarrow.timestamp("us")
        )
        timestamp_unit = timestamp_type.unit
        if not is_null:
            value = pyarrow.array([value], type=timestamp_type).cast(pyarrow.int64())[0].as_py()
        return TimestampVector.from_constant(
            0 if is_null else value,
            length,
            is_null=is_null,
            timestamp_unit=timestamp_unit,
        )

    if target_type == OrsoTypes.TIME:
        from opteryx.compiled.draken.vectors.time_vector import TimeVector

        is_time64 = bool(arrow_type and pyarrow.types.is_time64(arrow_type))
        time_type = arrow_type or pyarrow.time64("us")
        if not is_null:
            cast_type = pyarrow.int64() if pyarrow.types.is_time64(time_type) else pyarrow.int32()
            value = pyarrow.array([value], type=time_type).cast(cast_type)[0].as_py()
        return TimeVector.from_constant(
            0 if is_null else value,
            length,
            is_null=is_null,
            is_time64=is_time64,
        )

    return None


LOGICAL_OPERATIONS: Dict[NodeType, Callable] = {
    NodeType.AND: pyarrow.compute.and_,
    NodeType.OR: pyarrow.compute.or_,
    NodeType.XOR: pyarrow.compute.xor,
}


def evaluate_dnf(expressions: List[Node], table: Table) -> numpy.ndarray:
    num_rows = table.num_rows
    true_indices = numpy.arange(num_rows)
    working_table = table

    for i, predicate in enumerate(expressions):
        result = evaluate(predicate, working_table)

        if isinstance(result, pyarrow.Array):
            result_bool = result.to_numpy(zero_copy_only=False).astype(bool, copy=False)
        else:
            result_bool = numpy.asarray(result, dtype=bool)

        if not result_bool.any():
            return numpy.zeros(num_rows, dtype=bool)

        true_indices = true_indices[result_bool]

        if i < len(expressions) - 1:
            working_table = table.take(true_indices)

    final_result = numpy.zeros(num_rows, dtype=bool)
    final_result[true_indices] = True
    return final_result


def short_cut_and(root, table):
    # Convert to NumPy arrays
    true_indices = numpy.arange(table.num_rows)

    # Evaluate left expression
    left_result = numpy.array(evaluate(root.left, table))
    null_indices = compute.is_null(left_result, nan_is_null=True).to_numpy(False)
    left_result = numpy.asarray(left_result, dtype=numpy.bool_)

    # If all values in left_result are False, no need to evaluate the right expression
    if not left_result.any():
        return left_result

    # Filter out indices where left_result is FALSE
    subset_indices = true_indices[left_result]

    # Create a subset table for evaluating the right expression
    subset_table = table.take(subset_indices)

    # Evaluate right expression on the subset table
    right_result = numpy.array(evaluate(root.right, subset_table))

    # Combine results
    left_result[subset_indices] = right_result

    # handle nulls
    if null_indices.any():
        left_result = left_result.astype(object)
        numpy.place(left_result, null_indices, [None])
        return left_result

    return left_result


def short_cut_or(root, table):
    # Assuming table.num_rows returns the number of rows in the table
    false_indices = numpy.arange(table.num_rows)

    # Evaluate left expression
    left_result = numpy.array(evaluate(root.left, table))
    null_indices = compute.is_null(left_result, nan_is_null=True).to_numpy(False)
    left_result = numpy.asarray(left_result, dtype=numpy.bool_)

    # Filter out indices where left_result is TRUE
    subset_indices = false_indices[~left_result]

    if subset_indices.size == 0:
        return left_result

    # Create a subset table for evaluating the right expression
    subset_table = table.take(subset_indices)

    # Evaluate right expression on the subset table
    right_result = numpy.array(evaluate(root.right, subset_table), dtype=numpy.bool_)

    # Combine results
    # Update left_result with the right_result where left_result was False
    left_result[subset_indices] = left_result[subset_indices] | right_result

    # handle nulls
    if null_indices.any():
        left_result = left_result.astype(object)
        numpy.place(left_result, null_indices, [None])
        return left_result

    return left_result


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


def _inner_evaluate(root: Node, table: Table):
    node_type = root.node_type  # type: ignore

    if node_type == NodeType.DNF:
        return evaluate_dnf(root.parameters, table)

    if node_type == NodeType.SUBQUERY:
        raise UnsupportedSyntaxError("IN (<subquery>) temporarily not supported.")

    identity = root.schema_column.identity if root.schema_column else random_string()

    # if we have this column already, just return it
    if identity in table.column_names:
        return table[identity].to_numpy(False)

    # LITERAL TYPES
    if node_type == NodeType.LITERAL:
        # if it's a literal value, return it once for every value in the table
        literal_type = root.type
        if literal_type in (OrsoTypes.ARRAY, OrsoTypes.VECTOR):
            # creating ARRAY/VECTOR columns is expensive, so we don't create one full length
            array_literal = numpy.empty(1, dtype=object)
            if isinstance(root.value, (list, tuple)):
                array_literal[0] = numpy.asarray(root.value, dtype=object)
            else:
                array_literal[0] = root.value
            return array_literal
        if literal_type == OrsoTypes.VARCHAR:
            return numpy.array([root.value] * table.num_rows, dtype=numpy.str_)
        if literal_type == OrsoTypes.BLOB:
            return numpy.array([root.value] * table.num_rows, dtype=numpy.bytes_)
        if literal_type == OrsoTypes.INTERVAL:
            return pyarrow.array([root.value] * table.num_rows)
        if literal_type == OrsoTypes.DATE and isinstance(root.value, (int, numpy.integer)):
            value = numpy.datetime64(
                datetime.date(1970, 1, 1) + datetime.timedelta(days=int(root.value)), "D"
            )
            return numpy.full(shape=table.num_rows, fill_value=value, dtype="datetime64[D]")
        if literal_type == OrsoTypes.DATE and isinstance(root.value, datetime.date):
            value = numpy.datetime64(root.value, "D")
            return numpy.full(shape=table.num_rows, fill_value=value, dtype="datetime64[D]")
        if literal_type == OrsoTypes.TIMESTAMP and isinstance(root.value, (int, numpy.integer)):
            ivalue = int(root.value)
            if abs(ivalue) < 100_000_000_000 and ivalue % 1_000_000 == 0:
                dt = datetime.datetime(1970, 1, 1) + datetime.timedelta(days=ivalue // 1_000_000)
                value = numpy.datetime64(dt, "us")
            else:
                value = numpy.datetime64(ivalue, "us")
            return numpy.full(shape=table.num_rows, fill_value=value, dtype="datetime64[us]")
        if literal_type == OrsoTypes.TIMESTAMP and isinstance(root.value, datetime.datetime):
            value = numpy.datetime64(root.value, "us")
            return numpy.full(shape=table.num_rows, fill_value=value, dtype="datetime64[us]")
        if literal_type == OrsoTypes.TIMESTAMP and isinstance(root.value, datetime.date):
            value = numpy.datetime64(
                datetime.datetime(root.value.year, root.value.month, root.value.day), "us"
            )
            return numpy.full(shape=table.num_rows, fill_value=value, dtype="datetime64[us]")
        if isinstance(literal_type, OrsoTypes):
            literal_type = literal_type.numpy_dtype
        return numpy.full(
            shape=table.num_rows,
            fill_value=root.value,
            dtype=literal_type,
        )  # type: ignore

    # BOOLEAN OPERATORS
    if node_type & LOGICAL_TYPE == LOGICAL_TYPE:  # type: ignore
        if node_type == NodeType.OR:
            return short_cut_or(root, table)
        if node_type == NodeType.AND:
            return short_cut_and(root, table)

        if node_type in LOGICAL_OPERATIONS:
            left = (
                _inner_evaluate(root.left, table)
                if root.left
                else pyarrow.nulls(1, type=pyarrow.bool_())
            )
            right = (
                _inner_evaluate(root.right, table)
                if root.right
                else pyarrow.nulls(1, type=pyarrow.bool_())
            )

            if not isinstance(left, pyarrow.Array):
                if left.__class__.__name__ == "BoolVector":
                    left = left.to_arrow()
                else:
                    left = pyarrow.array(left, type=pyarrow.bool_())
            if not isinstance(right, pyarrow.Array):
                if right.__class__.__name__ == "BoolVector":
                    right = right.to_arrow()
                else:
                    right = pyarrow.array(right, type=pyarrow.bool_())

            return LOGICAL_OPERATIONS[node_type](left, right)  # type: ignore

        if node_type == NodeType.NOT:
            centre = (
                _inner_evaluate(root.centre, table)
                if root.centre
                else pyarrow.nulls(1, type=pyarrow.bool_())
            )
            # Convert to numpy array if it's not already a PyArrow array
            # This handles memoryviews, Cython memoryviewslices, and other array-like objects
            if not isinstance(centre, pyarrow.Array):
                if centre.__class__.__name__ == "BoolVector":
                    centre = centre.to_arrow()
                else:
                    centre = numpy.asarray(centre)
                    # Convert numeric types (e.g., uint8 from list_contains_any) to boolean
                    if numpy.issubdtype(centre.dtype, numpy.integer):
                        centre = centre.astype(numpy.bool_)
                    centre = pyarrow.array(centre, type=pyarrow.bool_())
            return pyarrow.compute.invert(centre)

    # INTERAL IDENTIFIERS
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
            return apply_bounded_function(root, *parameters)
        if node_type == NodeType.CAST:
            # Handle CAST operations (CAST(expr AS type), TRY_CAST, SAFE_CAST)
            from opteryx.expression.casts import cast
            from opteryx.expression.casts import try_cast

            # Evaluate source expression
            source = _inner_evaluate(root.left, table)

            # Determine if this is a safe cast (TRY_CAST/SAFE_CAST) or regular cast
            # TRY_ prefix in node.value indicates safe cast
            is_safe_cast = root.value.startswith("TRY_")

            # Get the target type name (remove TRY_ prefix if present)
            target_type = root.value[4:] if is_safe_cast else root.value

            # Get the appropriate cast kernel
            kernel = try_cast(target_type) if is_safe_cast else cast(target_type)

            # Handle optional precision/scale/length parameters from node.parameters
            params = []
            if root.parameters:
                # Parameters were already bound by binder if needed
                params = [_inner_evaluate(param, table) for param in root.parameters]

            # Apply the cast kernel(s, *params)
            result = kernel(source, *params)

            # Ensure result is a numpy array
            if isinstance(result, list):
                result = numpy.array(result)

            return result
        if node_type == NodeType.AGGREGATOR:
            # detected as an aggregator, but here it's an identifier because it
            # will have already been evaluated
            node_type = NodeType.EVALUATED
            root.value = format_expression(root)
            root.node_type = NodeType.EVALUATED
        if node_type == NodeType.EVALUATED:
            if root.schema_column.identity not in table.column_names:
                raise ColumnReferencedBeforeEvaluationError(column=root.schema_column.name)
            return table[root.schema_column.identity].to_numpy(zero_copy_only=False)
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
                        if isinstance(value, (int, numpy.integer)):
                            return datetime.datetime(1970, 1, 1) + datetime.timedelta(
                                days=int(value)
                            )
                        if isinstance(value, datetime.datetime):
                            return value.replace(hour=0, minute=0, second=0, microsecond=0)
                        if isinstance(value, datetime.date):
                            return datetime.datetime(value.year, value.month, value.day)
                    if node.type == OrsoTypes.TIMESTAMP:
                        if isinstance(value, (int, numpy.integer)):
                            ivalue = int(value)
                            if abs(ivalue) < 100_000_000_000 and ivalue % 1_000_000 == 0:
                                return datetime.datetime(1970, 1, 1) + datetime.timedelta(
                                    days=ivalue // 1_000_000
                                )
                            return datetime.datetime(1970, 1, 1) + datetime.timedelta(
                                microseconds=ivalue
                            )
                        if isinstance(value, numpy.datetime64):
                            micros = int(value.astype("datetime64[us]").astype(numpy.int64))
                            return datetime.datetime(1970, 1, 1) + datetime.timedelta(
                                microseconds=micros
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
                return numpy.full(table.num_rows, _cmp, dtype=numpy.bool_)

            right = None
            left = None

            if root.right.node_type == NodeType.LITERAL:
                right = [root.right.value]

            if right is None:
                if root.right.node_type == NodeType.IDENTIFIER:
                    right = table[root.right.schema_column.identity]
                else:
                    right = _inner_evaluate(root.right, table)
            if left is None:
                if root.left.node_type == NodeType.IDENTIFIER:
                    left = table[root.left.schema_column.identity]
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
            return numpy.full(table.num_rows, "*", dtype=numpy.str_)
        if node_type == NodeType.SUBQUERY:
            # we should have a query plan here
            sub = root.value.execute()
            return pyarrow.concat_tables(sub, promote_options="none")
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


def evaluate(expression: Node, table: Table):
    result = _inner_evaluate(root=expression, table=table)
    if result.__class__.__name__ == "BoolVector":
        return result
    if not isinstance(result, (pyarrow.Array, numpy.ndarray)):
        result = numpy.array(result)
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


def _evaluate_and_append_arrow(expressions, table: Table):
    """
    Evaluate an expression and add it to the table.

    This needs to be able to deal with and avoid cascading problems where field names
    are duplicated, this is most common when performing many joins on the same table.
    """
    prioritized_expressions = prioritize_evaluation(expressions)
    existing_cols = set(table.column_names)

    for statement in prioritized_expressions:
        identity = statement.schema_column.identity
        if identity in existing_cols:
            continue

        if not should_evaluate(statement):
            continue

        new_column = None
        if statement.node_type == NodeType.LITERAL and statement.type not in (
            OrsoTypes.DATE,
            OrsoTypes.TIMESTAMP,
        ):
            from opteryx.compiled.draken.vectors.scalar_constructors import (
                from_scalar as constant_from_scalar,
            )

            target_type = _arrow_type_for_schema_column(statement.schema_column)
            literal_vec = constant_from_scalar(statement.value, table.num_rows, dtype=target_type)
            if literal_vec is not None:
                new_column = literal_vec.to_arrow()

        if new_column is None:
            if table.num_rows > 0:
                new_column = evaluate_statement(statement, table)
            else:
                # we make all unknown fields to object type
                new_column = pyarrow.array(
                    [], type=_arrow_type_for_schema_column(statement.schema_column)
                )

        # if we know the intended type of the result column, cast it
        field = statement.schema_column.identity
        if statement.schema_column.type not in (
            0,
            OrsoTypes._MISSING_TYPE,
            OrsoTypes.NULL,
            OrsoTypes.INTERVAL,
        ):
            field = pyarrow.field(
                name=identity,
                type=_arrow_type_for_schema_column(statement.schema_column),
            )
            try:
                if isinstance(new_column, (pyarrow.Array, pyarrow.ChunkedArray)):
                    new_column = new_column.cast(field.type)
                else:
                    temporal_numpy = isinstance(new_column, numpy.ndarray) and numpy.issubdtype(
                        new_column.dtype, numpy.datetime64
                    )
                    temporal_python = isinstance(new_column, (list, tuple)) and any(
                        isinstance(value, (datetime.date, datetime.datetime))
                        for value in new_column
                        if value is not None
                    )
                    if temporal_numpy or temporal_python:
                        new_column = pyarrow.array(new_column)
                    else:
                        # Use Draken's vector_from_sequence for efficient array construction
                        from opteryx.compiled.draken.interop.arrow import vector_from_sequence

                        # Convert numpy arrays to lists to avoid dimension issues
                        if hasattr(new_column, "tolist"):
                            new_column = new_column.tolist()

                        vec = vector_from_sequence(new_column)
                        new_column = vec.to_arrow()
                    # Cast to the expected type if needed
                    if new_column.type != field.type:
                        try:
                            new_column = new_column.cast(field.type)
                        except pyarrow.lib.ArrowInvalid:
                            # If safe casting fails, try unsafe cast
                            new_column = new_column.cast(field.type, safe=False)
            except pyarrow.lib.ArrowInvalid as e:
                raise IncorrectTypeError(
                    f"Unable to cast '{statement.schema_column.name}' to {field.type}"
                ) from e
        elif not isinstance(new_column, (pyarrow.Array, pyarrow.ChunkedArray)):
            new_column = pyarrow.array(new_column)

        table = table.append_column(field, new_column)
        existing_cols.add(identity)

    return table


def _evaluate_and_append_morsel(expressions, morsel):
    """
    Evaluate expressions against a Draken Morsel.

    Typed literal expressions are appended natively as typed constant-encoded
    vectors where possible. If a non-literal expression is encountered we fall
    back to Arrow evaluation for the remaining expressions, then convert back
    to a Morsel.
    """
    from opteryx.compiled.draken.interop.arrow import vector_from_arrow
    from opteryx.compiled.draken.morsels.morsel import Morsel
    from opteryx.compiled.draken.vectors.scalar_constructors import (
        from_scalar as constant_from_scalar,
    )

    prioritized_expressions = prioritize_evaluation(expressions)
    names = list(morsel.column_names)
    vectors = [
        morsel.column(name if isinstance(name, bytes) else name.encode("utf-8")) for name in names
    ]
    existing_cols = {name.decode("utf-8") if isinstance(name, bytes) else name for name in names}

    for statement in prioritized_expressions:
        identity = statement.schema_column.identity
        if identity in existing_cols:
            continue

        if not should_evaluate(statement):
            continue

        if statement.node_type == NodeType.LITERAL:
            literal_vec = _typed_constant_vector(
                statement.value, morsel.num_rows, statement.schema_column
            )
            if literal_vec is None:
                target_type = _arrow_type_for_schema_column(statement.schema_column)
                literal_vec = constant_from_scalar(
                    statement.value, morsel.num_rows, dtype=target_type
                )
            if literal_vec is not None:
                names.append(identity)
                vectors.append(literal_vec)
                existing_cols.add(identity)
                continue

        # Non-literal expressions still evaluate through Arrow today.
        # Evaluate only the current expression and append its result vector so
        # previously appended constant vectors remain native.
        working_morsel = Morsel.from_vectors(names, vectors)
        working_table = working_morsel.to_arrow()
        evaluated = _evaluate_and_append_arrow([statement], working_table)
        new_column = evaluated.column(identity)
        if isinstance(new_column, pyarrow.ChunkedArray):
            if new_column.num_chunks == 1:
                new_column = new_column.chunk(0)
            else:
                new_column = new_column.combine_chunks()
        names.append(identity)
        vectors.append(vector_from_arrow(new_column))
        existing_cols.add(identity)
        continue

    return Morsel.from_vectors(names, vectors)


def evaluate_and_append(expressions, table: Table):
    if table.__class__.__name__ == "Morsel":
        return _evaluate_and_append_morsel(expressions, table)
    return _evaluate_and_append_arrow(expressions, table)


def should_evaluate(statement):
    """Determine if the given statement should be evaluated."""
    valid_node_types = {
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


def evaluate_statement(statement, table):
    """Evaluate a statement and return the corresponding column."""
    new_column = evaluate(statement, table)
    if is_mask(new_column, statement, table):
        new_column = create_mask(new_column, table.num_rows)
    return new_column


def is_mask(new_column, statement, table):
    """Determine if the given column represents a mask."""
    return len(new_column) < table.num_rows or statement.node_type == NodeType.UNARY_OPERATOR


def create_mask(column, num_rows):
    """Create a boolean mask based on the given column."""
    bool_list = numpy.full(num_rows, False)
    bool_list[column] = True
    return bool_list
