# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
This module contains various converters for parts of the AST, this
helps to ensure new AST-based functionality can be added by adding
a function and a reference to it in the dictionary.
"""

import datetime
import warnings
from typing import List, Optional

import numpy
from orso.types import OrsoTypes

from opteryx.exceptions import ArrayWithMixedTypesError, SqlError, UnsupportedSyntaxError
from opteryx.expression import NodeType, format_expression
from opteryx.expression.binary_operators import binary_operations
from opteryx.expression.functions import functions as _list_functions
from opteryx.expression.functions import is_function as _is_function
from opteryx.expression.intervals import (
    MICROSECONDS_PER_DAY,
    MICROSECONDS_PER_HOUR,
    MICROSECONDS_PER_MINUTE,
    MICROSECONDS_PER_SECOND,
)
from opteryx.expression.operator_catalog import get_operator_node_type
from opteryx.models import LogicalColumn, Node
from opteryx.operators.aggregate_helpers import aggregator_names, is_aggregator
from opteryx.utils import dates, suggest_alternative

# Epoch constants for converting datetime literals to Draken-native integers.
# DATE literals are stored as int (days since epoch, fits int32).
# TIMESTAMP literals are stored as int (microseconds since epoch, int64).
_EPOCH_DATE = datetime.date(1970, 1, 1)
_EPOCH_DT = datetime.datetime(1970, 1, 1)


def _evaluate_fixed_temporal_function(function_name: str):
    now = datetime.datetime.now(datetime.UTC).replace(tzinfo=None)
    if function_name in ("NOW", "CURRENT_TIMESTAMP", "UTC_TIMESTAMP"):
        return now, OrsoTypes.TIMESTAMP
    if function_name in ("CURRENT_DATE", "TODAY"):
        return now.date(), OrsoTypes.DATE
    if function_name == "YESTERDAY":
        return (now - datetime.timedelta(days=1)).date(), OrsoTypes.DATE
    if function_name == "CURRENT_TIME":
        return now.time(), OrsoTypes.TIME
    return None, None


def _extract_single_scalar(value):
    if hasattr(value, "to_numpy"):
        value = value.to_numpy(zero_copy_only=False)
    if isinstance(value, numpy.ndarray):
        if value.size != 1:
            raise UnsupportedSyntaxError(
                "Time-travel expressions must evaluate to a single scalar value."
            )
        value = value.reshape(-1)[0]
    elif isinstance(value, (list, tuple)):
        if len(value) != 1:
            raise UnsupportedSyntaxError(
                "Time-travel expressions must evaluate to a single scalar value."
            )
        value = value[0]

    if hasattr(value, "as_py"):
        value = value.as_py()
    if isinstance(value, numpy.generic):
        value = value.item()
    return value


def _as_binary_operand_array(value, value_type):
    if value_type == OrsoTypes.INTERVAL:
        arr = numpy.empty(1, dtype=object)
        arr[0] = value
        return arr
    if value_type == OrsoTypes.TIMESTAMP:
        timestamp = dates.parse_iso(value)
        if timestamp is None:
            raise UnsupportedSyntaxError(
                "Unable to parse timestamp value in time-travel expression."
            )
        return numpy.array([int((timestamp - _EPOCH_DT).total_seconds() * 1_000_000)])
    if value_type == OrsoTypes.DATE:
        dt = dates.parse_iso(value)
        if dt is None:
            raise UnsupportedSyntaxError("Unable to parse date value in time-travel expression.")
        return numpy.array([(dt.date() - _EPOCH_DATE).days])
    return numpy.array([value])


def _as_function_parameter_array(value, value_type):
    if value_type == OrsoTypes.TIMESTAMP:
        dt = dates.parse_iso(value)
        if dt is None:
            raise UnsupportedSyntaxError("Unable to parse temporal function argument.")
        return numpy.array([int((dt - _EPOCH_DT).total_seconds() * 1_000_000)])
    if value_type == OrsoTypes.DATE:
        dt = dates.parse_iso(value)
        if dt is None:
            raise UnsupportedSyntaxError("Unable to parse temporal function argument.")
        return numpy.array([(dt.date() - _EPOCH_DATE).days])
    return numpy.array([value])


def _type_from_value(value):
    if value is None:
        return OrsoTypes.NULL
    if isinstance(value, bool):
        return OrsoTypes.BOOLEAN
    if isinstance(value, (numpy.datetime64, datetime.datetime)):
        return OrsoTypes.TIMESTAMP
    if isinstance(value, datetime.time):
        return OrsoTypes.TIME
    if isinstance(value, datetime.date):
        return OrsoTypes.DATE
    if isinstance(value, (int, numpy.integer)):
        return OrsoTypes.INTEGER
    if isinstance(value, (float, numpy.floating)):
        return OrsoTypes.DOUBLE
    if isinstance(value, (bytes, bytearray)):
        return OrsoTypes.BLOB
    if isinstance(value, str):
        return OrsoTypes.VARCHAR
    if isinstance(value, tuple) and len(value) == 2:
        return OrsoTypes.INTERVAL
    if isinstance(value, numpy.ndarray) and value.shape == (2,):
        return OrsoTypes.INTERVAL
    return OrsoTypes._MISSING_TYPE


def _interval_to_past_timestamp(interval_value):
    months = abs(int(interval_value[0]))
    microseconds = abs(int(interval_value[1]))
    now = datetime.datetime.now(datetime.UTC).replace(tzinfo=None)
    dt = dates.add_months(now, -months) if months else now
    return dt - datetime.timedelta(microseconds=microseconds)


def _apply_interval_scalar(base_value, base_type, interval_value, operator: str):
    """Apply an INTERVAL to a DATE or TIMESTAMP scalar.

    The expression engine relies on the generic ``binary_operations`` path
    which converts dates to plain integers.  Unfortunately the interval
    kernels expect a PyArrow temporal array and therefore return ``null``
    when provided with a raw integer.  That bug manifests during
    timetravel evaluation, e.g. ``CURRENT_DATE - INTERVAL '7' DAY``;
    the arithmetic ends up producing ``None`` which is later treated as a
    failure to resolve the expression.

    To keep the timetravel evaluator simple we perform the arithmetic
    ourselves using Python's ``datetime`` helpers.  This bypasses the
    broken kernel and ensures the result is a sensible ``datetime``.
    The returned value is always a ``datetime.datetime`` so that callers
    can assume a timestamp-like object (even if the input was a plain
    date).
    """
    # convert date-only values into datetimes at midnight
    if isinstance(base_value, datetime.date) and not isinstance(base_value, datetime.datetime):
        result = datetime.datetime.combine(base_value, datetime.time())
    else:
        result = base_value

    sign = 1 if operator == "Plus" else -1
    months, micros = interval_value
    if months:
        result = dates.add_months(result, sign * int(months))
    if micros:
        result = result + datetime.timedelta(microseconds=sign * int(micros))
    return result


def _evaluate_timetravel_expression(node, apply_interval_literal_to_now: bool = False):
    if node.node_type == NodeType.LITERAL:
        if node.type == OrsoTypes.INTERVAL and apply_interval_literal_to_now:
            return _interval_to_past_timestamp(node.value), OrsoTypes.TIMESTAMP
        return node.value, node.type

    if node.node_type == NodeType.NESTED:
        return _evaluate_timetravel_expression(node.centre, apply_interval_literal_to_now)

    if node.node_type == NodeType.FUNCTION:
        fixed_value, fixed_type = _evaluate_fixed_temporal_function(node.value)
        if fixed_type is not None:
            return fixed_value, fixed_type

        parameter_values = []
        for parameter in node.parameters:
            value, value_type = _evaluate_timetravel_expression(parameter)
            parameter_values.append(_as_function_parameter_array(value, value_type))

        try:
            from opteryx.expression.functions import FunctionResolutionContext
            from opteryx.expression.functions import get_catalog as _get_catalog

            _catalog = _get_catalog()
            _func_def = _catalog.get_definition(node.value)
            if _func_def is None or not _func_def.overloads:
                raise UnsupportedSyntaxError(f"Unknown function '{node.value}'.")
            resolved = _catalog.resolve(
                node.value, node.parameters, FunctionResolutionContext(schema={}, bound_args={})
            )
            if resolved is None:
                raise UnsupportedSyntaxError(f"Unknown function '{node.value}'.")
            result = resolved.selected_overload.kernel.callable_ref(*parameter_values)
        except UnsupportedSyntaxError:
            raise
        except Exception as err:
            raise UnsupportedSyntaxError(
                f"Unable to evaluate time-travel function '{node.value}'."
            ) from err

        scalar = _extract_single_scalar(result)
        return scalar, _type_from_value(scalar)

    if node.node_type in (NodeType.BINARY_OPERATOR, NodeType.EXTRACTION_OPERATOR):
        left_value, left_type = _evaluate_timetravel_expression(node.left)
        right_value, right_type = _evaluate_timetravel_expression(node.right)

        # short‑circuit arithmetic when one side is a DATE/TIMESTAMP and the
        # other is an INTERVAL.  The generic kernel path converts dates to
        # plain integers which then causes ``IntervalVector.apply_to_temporal``
        # to bail out and return null.  We can evaluate these cases directly
        # using Python's datetime helpers.
        if node.value in ("Plus", "Minus"):
            if (
                left_type in (OrsoTypes.DATE, OrsoTypes.TIMESTAMP)
                and right_type == OrsoTypes.INTERVAL
            ):
                return (
                    _apply_interval_scalar(left_value, left_type, right_value, node.value),
                    OrsoTypes.TIMESTAMP,
                )
            if left_type == OrsoTypes.INTERVAL and right_type in (
                OrsoTypes.DATE,
                OrsoTypes.TIMESTAMP,
            ):
                # interval +/- date is effectively the same as date +/- interval
                return (
                    _apply_interval_scalar(right_value, right_type, left_value, node.value),
                    OrsoTypes.TIMESTAMP,
                )

        left = _as_binary_operand_array(left_value, left_type)
        right = _as_binary_operand_array(right_value, right_type)

        try:
            result = binary_operations(left, left_type, node.value, right, right_type)
        except Exception as err:
            raise UnsupportedSyntaxError(
                f"Unable to evaluate time-travel expression with operator '{node.value}'."
            ) from err

        scalar = _extract_single_scalar(result)
        return scalar, _type_from_value(scalar)

    raise UnsupportedSyntaxError("Time-travel expression must resolve to a scalar value.")


def _extract_version_expression(version_clause):
    if "ForSystemTimeAsOf" in version_clause:
        raise UnsupportedSyntaxError(
            "FOR SYSTEM_TIME AS OF is not supported. Use `TIMESTAMP AS OF <expression>`."
        )

    if "TimestampAsOf" in version_clause:
        # Legacy/alternate parser shape:
        # {"TimestampAsOf": <expr>}
        return version_clause["TimestampAsOf"]

    if "Function" in version_clause:
        function_wrapper = version_clause["Function"]
        function_branch = function_wrapper.get("Function", function_wrapper)
    else:
        raise UnsupportedSyntaxError(
            "Unsupported table version syntax. Use `TIMESTAMP AS OF <expression>`."
        )

    if "Function" in function_branch:
        function_branch = function_branch["Function"]

    if "name" not in function_branch:
        raise UnsupportedSyntaxError(
            "Invalid time-travel clause, use `TIMESTAMP AS OF <expression>`."
        )

    function_name = function_branch["name"][0]["Identifier"]["value"].upper()
    if function_name != "AT":
        raise UnsupportedSyntaxError(
            f"Unsupported time-travel function '{function_name}'. Use `TIMESTAMP AS OF <expression>`."
        )

    args = function_branch.get("args", {}).get("List", {}).get("args", [])
    if len(args) != 1:
        raise UnsupportedSyntaxError(
            f"Time-travel syntax expects exactly 1 argument, got {len(args)}."
        )

    raise UnsupportedSyntaxError("Time-travel syntax must be `TIMESTAMP AS OF <expression>`.")


def extract_timetravel_timestamp(version_clause) -> Optional[object]:
    """
    Extract and evaluate a time-travel timestamp from the table version clause.

    Supported syntax:
        TIMESTAMP AS OF INTERVAL '1' DAY -- interpreted as current time minus 1 day
        TIMESTAMP AS OF '2024-12-15 00:00:00'
        TIMESTAMP AS OF CURRENT_DATE - INTERVAL '7' DAY
        TIMESTAMP AS OF TRUNC(CURRENT_DATE, 'month')
        AT(TIMESTAMP => '2024-12-15 00:00:00') -- legacy/alternate syntax

    Args:
        version_clause: The version field from the table AST

    Returns:
        Parsed datetime object or None if no version clause

    Raises:
        UnsupportedSyntaxError: If syntax is unsupported or doesn't evaluate to one timestamp
    """
    if version_clause is None:
        return None

    expression_ast = _extract_version_expression(version_clause)
    expression_node = build(expression_ast)
    value, _ = _evaluate_timetravel_expression(expression_node, apply_interval_literal_to_now=True)

    if value is None:
        raise UnsupportedSyntaxError(
            "Time-travel expression must be `TIMESTAMP AS OF <expression>`."
        )

    return value


def any_op(branch, alias: Optional[List[str]] = None, key=None):
    return Node(
        NodeType.COMPARISON_OPERATOR,
        value="AnyOp" + branch.get("compare_op", "Unsupported"),
        left=build(branch["left"]),
        right=build(branch["right"]),
    )


def all_op(branch, alias: Optional[List[str]] = None, key=None):
    return Node(
        NodeType.COMPARISON_OPERATOR,
        value="AllOp" + branch.get("compare_op", "Unsupported"),
        left=build(branch["left"]),
        right=build(branch["right"]),
    )


def array(branch, alias: Optional[List[str]] = None, key=None):
    value_nodes = [build(elem) for elem in branch["elem"]]
    value_list = [v.value for v in value_nodes]
    element_type = {v.type for v in value_nodes}
    if len(element_type) > 1:
        raise ArrayWithMixedTypesError("Literal ARRAY has values with mixed types.")
    element_type = element_type.pop() if len(element_type) == 1 else OrsoTypes.VARCHAR
    literal_type = OrsoTypes.ARRAY
    if element_type in (OrsoTypes.INTEGER, OrsoTypes.DOUBLE, OrsoTypes.DECIMAL):
        literal_type = OrsoTypes.VECTOR
        element_type = OrsoTypes.DOUBLE

    return Node(
        node_type=NodeType.LITERAL,
        type=literal_type,
        element_type=element_type,
        value=value_list,
    )


def between(branch, alias: Optional[List[str]] = None, key=None):
    expr = build(branch["expr"])
    low = build(branch["low"])
    high = build(branch["high"])
    inverted = branch["negated"]

    if inverted:
        # LEFT <= LOW AND LEFT >= HIGH (not between)
        left_node = Node(
            NodeType.COMPARISON_OPERATOR,
            value="Lt",
            left=expr,
            right=low,
        )
        right_node = Node(
            NodeType.COMPARISON_OPERATOR,
            value="Gt",
            left=expr,
            right=high,
        )

        return Node(NodeType.OR, left=left_node, right=right_node)
    else:
        # LEFT > LOW and LEFT < HIGH (between)
        left_node = Node(
            NodeType.COMPARISON_OPERATOR,
            value="GtEq",
            left=expr,
            right=low,
        )
        right_node = Node(
            NodeType.COMPARISON_OPERATOR,
            value="LtEq",
            left=expr,
            right=high,
        )

        return Node(NodeType.AND, left=left_node, right=right_node)


def binary_op(branch, alias: Optional[List[str]] = None, key=None):
    left = build(branch["left"])
    operator = branch["op"]
    right = build(branch["right"])

    # Dialect-specific operator mapping
    if isinstance(operator, dict):
        operator = operator["Custom"]

    if operator in ("PGRegexMatch", "SimilarTo"):
        operator = "RLike"
    if operator in ("PGRegexNotMatch", "NotSimilarTo"):
        operator = "NotRLike"

    operator_type = get_operator_node_type(operator)
    if operator_type is None:
        raise UnsupportedSyntaxError(f"Unsupported operator '{operator}'.")

    return Node(
        operator_type,
        value=operator,
        left=left,
        right=right,
        alias=alias,
    )


def case_when(value, alias: Optional[List[str]] = None, key=None):
    fixed_operand = build(value["operand"])
    else_result = build(value["else_result"])

    conditions = []
    results = []
    for condition in value["conditions"]:
        operand = build(condition["condition"])
        if fixed_operand is None:
            conditions.append(operand)
        else:
            conditions.append(
                Node(
                    NodeType.COMPARISON_OPERATOR,
                    value="Eq",
                    left=fixed_operand,
                    right=operand,
                )
            )
        result = build(condition["result"])
        results.append(result)

    if else_result is not None:
        conditions.append(Node(NodeType.LITERAL, type=OrsoTypes.BOOLEAN, value=True))
        results.append(else_result)
    conditions_node = Node(NodeType.EXPRESSION_LIST, parameters=conditions)
    results_node = Node(NodeType.EXPRESSION_LIST, parameters=results)

    return Node(
        NodeType.FUNCTION,
        value="_CASE",
        parameters=[conditions_node, results_node],
        alias=alias,
    )


def cast(branch, alias: Optional[List[str]] = None, key=None):
    """
    Convert CAST(<expr> AS <type>) to a typed function call <type>(<expr>).
    Handles literal value casting at compile time when possible.
    """
    from opteryx.planner import build_literal_node

    source_expr = build(branch["expr"])
    kind = branch["kind"]
    raw_data_type = branch["data_type"]

    cast_parameters = []

    # Extract the base data type from the AST structure
    data_type = _extract_data_type(raw_data_type, branch, cast_parameters, build_literal_node)

    # Validate and normalize the data type
    normalized_type = _normalize_cast_type(data_type)

    # Apply TRY_CAST or SAFE_CAST prefix if needed
    if kind in {"TryCast", "SafeCast"}:
        normalized_type = "TRY_" + normalized_type

    # Handle literal value casting at compile time
    if source_expr.node_type == NodeType.LITERAL:
        return _cast_literal_value(source_expr, normalized_type, kind, alias)

    # For non-literals, return a CAST node that will be evaluated at runtime
    # CAST nodes have the source in 'left', target type in 'value', and optional params in 'parameters'
    return Node(
        NodeType.CAST,
        left=source_expr,
        value=normalized_type.upper(),
        parameters=cast_parameters,
        alias=alias,
    )


def _extract_data_type(raw_data_type, branch, args, build_literal_node):
    """Extract and process the data type from the AST structure."""
    data_type = raw_data_type

    # Handle dictionary-wrapped types (e.g., Timestamp with timezone info)
    if isinstance(data_type, dict):
        type_key = next(iter(data_type))
        if type_key == "Timestamp" and data_type[type_key] not in (
            (None, "None"),
            (None, "WithoutTimeZone"),
        ):
            raise UnsupportedSyntaxError("TIMESTAMPS do not support `TIME ZONE`")
        data_type = type_key

    # Handle custom types
    if "Custom" in data_type:
        data_type = branch["data_type"]["Custom"][0][0]["Identifier"]["value"].upper()

    # Handle DECIMAL precision and scale
    if "decimal" in data_type.lower() and "PrecisionAndScale" in branch["data_type"].get(
        "Decimal", {}
    ):
        precision = branch["data_type"]["Decimal"]["PrecisionAndScale"][0]
        scale = branch["data_type"]["Decimal"]["PrecisionAndScale"][1]
        args.append(build_literal_node(precision))
        args.append(build_literal_node(scale))

    # Handle ARRAY element types
    if "array" in data_type.lower():
        element_key = branch["data_type"]["Array"].get("AngleBracket", {"Varchar": None})
        if isinstance(element_key, dict):
            element_key = next(iter(element_key))
        if isinstance(element_key, str):
            element_key = build_literal_node(element_key.upper())
            args.append(element_key)

    return data_type


def _normalize_cast_type(data_type: str) -> str:
    """Normalize and validate the cast target type."""
    lower_type = data_type.lower()
    upper_type = data_type.upper()

    # Map of substring patterns to normalized types
    type_mappings = {
        "timestamp": "TIMESTAMP",
        "date": "DATE",
        "varchar": "VARCHAR",
        "decimal": "DECIMAL",
        "integer": "INTEGER",
        "double": "DOUBLE",
        "boolean": "BOOLEAN",
        "struct": "STRUCT",
        "blob": "BLOB",
        "array": "ARRAY",
        "vector": "VECTOR",
    }

    # Check type mappings
    for pattern, normalized in type_mappings.items():
        if pattern in lower_type:
            return normalized

    # Check binary types separately
    if any(token in lower_type for token in ("varbinary", "binary", "raw")):
        return "VARBINARY"

    # Handle unsupported type aliases with helpful error messages
    type_suggestions = {
        ("STRING", "CHAR", "TEXT", "NVARCHAR"): "VARCHAR",
        ("FLOAT", "NUMERIC", "REAL"): "DOUBLE",
        ("INT", "SMALLINT", "TINYINT", "BIGINT", "BYTE"): "INTEGER",
        ("BOOL", "BIT"): "BOOLEAN",
    }

    for aliases, suggestion in type_suggestions.items():
        if upper_type in aliases:
            raise SqlError(
                f"Unsupported type for CAST - '{upper_type}'. Did you mean '{suggestion}'?"
            )

    raise SqlError(f"Unsupported type for CAST - '{data_type}'.")


def _cast_literal_value(literal_node, target_type: str, kind: str, alias):
    """Cast a literal value at compile time."""
    from opteryx.expression.casts import parse_timestamp_value

    # NULL values remain NULL regardless of target type
    if literal_node.type == OrsoTypes.NULL:
        return Node(NodeType.LITERAL, type=OrsoTypes.NULL, alias=alias)

    # Strip TRY_ prefix for type lookup
    base_type = target_type.replace("TRY_", "")

    # Special case: VARBINARY maps to BLOB in Orso types
    if base_type == "VARBINARY":
        orso_type = OrsoTypes.BLOB
    elif base_type == "DATE" and literal_node.type in (OrsoTypes.INTEGER, OrsoTypes.DATE):
        value = _EPOCH_DATE + datetime.timedelta(days=int(literal_node.value))
        return Node(NodeType.LITERAL, type=OrsoTypes.DATE, value=value, alias=alias)
    # Special case: INTEGER to TIMESTAMP conversion using PyArrow
    elif base_type == "TIMESTAMP" and (
        literal_node.type in (OrsoTypes.INTEGER, OrsoTypes.DATE)
        or isinstance(literal_node.value, (int, numpy.integer))
    ):
        int_value = int(literal_node.value)
        if literal_node.type == OrsoTypes.DATE or abs(int_value) < 100_000:
            value = (_EPOCH_DT + datetime.timedelta(days=int_value)).replace(tzinfo=None)
        else:
            value = parse_timestamp_value(int_value)
        return Node(NodeType.LITERAL, type=OrsoTypes.TIMESTAMP, value=value, alias=alias)
    else:
        orso_type = OrsoTypes.from_name(base_type)[0]

    # Attempt to parse and cast the literal value
    try:
        parsed_value = orso_type.parse(literal_node.value)
        return Node(NodeType.LITERAL, type=orso_type, value=parsed_value, alias=alias)
    except Exception as e:
        # For TRY_CAST/SAFE_CAST, return NULL on failure
        if kind in {"TryCast", "SafeCast"}:
            return Node(NodeType.LITERAL, type=OrsoTypes.NULL, alias=alias)
        # For regular CAST, raise an error
        raise SqlError(f"Error casting value '{literal_node.value}' to type '{base_type}': {e}")


def ceiling(value, alias: Optional[List[str]] = None, key=None):
    data_value = build(value["expr"])
    scale = build(value["field"]["Scale"]) if "Scale" in value["field"] else literal_number([0])
    return Node(NodeType.FUNCTION, value="CEILING", parameters=[data_value, scale], alias=alias)


def compound_identifier(branch, alias: Optional[List[str]] = None, key=None):
    column = LogicalColumn(
        node_type=NodeType.IDENTIFIER,  # column type
        alias=alias,  # type: ignore
        source_column=branch[-1]["value"],  # the source column
        source=".".join(p["value"] for p in branch[:-1]),  # the source relation
    )
    alias_name = alias[0] if isinstance(alias, list) and alias else alias
    if alias_name:
        column.query_column = alias_name
    else:
        qualifier = column.source
        column.query_column = (
            f"{qualifier}.{column.source_column}" if qualifier else column.source_column
        )
    return column


def expression_with_alias(branch, alias: Optional[List[str]] = None, key=None):
    """an alias"""
    return build(branch["expr"], alias=branch["alias"]["value"])


def exists(branch, alias: Optional[List[str]] = None, key=None):
    from opteryx.planner.logical_planner.logical_planner import plan_query

    subplan = plan_query(branch["subquery"])
    not_exists = Node(NodeType.LITERAL, type=OrsoTypes.BOOLEAN, value=branch["negated"])

    raise UnsupportedSyntaxError("EXISTS is not supported in Opteryx")

    return Node(
        NodeType.UNARY_OPERATOR,
        value="EXISTS",
        parameters=[Node(NodeType.SUBQUERY, plan=subplan), not_exists],
        alias=alias,
    )


def expressions(branch, alias: Optional[List[str]] = None, key=None):
    return [build(part) for part in branch]


def extract(branch, alias: Optional[List[str]] = None, key=None):
    # EXTRACT(part FROM timestamp)
    datepart_value = branch["field"]
    if isinstance(datepart_value, dict):
        datepart_value = list(datepart_value)[0]
    datepart = Node(NodeType.LITERAL, type=OrsoTypes.VARCHAR, value=datepart_value)
    identifier = build(branch["expr"])

    return Node(
        NodeType.FUNCTION,
        value="EXTRACT",
        parameters=[datepart, identifier],
        alias=alias,
    )


def floor(value, alias: Optional[List[str]] = None, key=None):
    data_value = build(value["expr"])
    scale = build(value["field"]["Scale"]) if "Scale" in value["field"] else literal_number([0])
    return Node(NodeType.FUNCTION, value="FLOOR", parameters=[data_value, scale], alias=alias)


def function(branch, alias: Optional[List[str]] = None, key=None):
    func = ".".join(build(p).value for p in branch["name"]).upper()

    order_by = None
    limit = None
    duplicate_treatment = None
    null_treatment = None
    filter_condition = None
    args = []

    if branch["args"] != "None":
        args = [build(a) for a in branch["args"]["List"]["args"]]

        for clause in branch["args"]["List"]["clauses"]:
            if "OrderBy" in clause:
                order_by = [
                    (
                        build(item["expr"]),
                        True if item["options"]["asc"] is None else item["options"]["asc"],
                    )
                    for item in clause["OrderBy"]
                ]
            elif "Limit" in clause:
                limit = build(clause["Limit"]).value

        duplicate_treatment = branch["args"]["List"].get("duplicate_treatment")
        null_treatment = branch["args"].get("null_treatment")
        filter_condition = branch.get("filter")

    if func == "MATCH_AGAINST" or func.startswith("_"):
        raise UnsupportedSyntaxError(f"`{func}` is internal. Use documented SQL syntax instead.")

    if _is_function(func):
        node_type = NodeType.FUNCTION
        if filter_condition is not None:
            raise UnsupportedSyntaxError("Filters are not supported with function calls.")
    elif is_aggregator(func):
        node_type = NodeType.AGGREGATOR
        if filter_condition is not None:
            if func != "COUNT":
                raise UnsupportedSyntaxError(
                    f"Filters are not supported with aggregate function '{func}'."
                )
            if duplicate_treatment == "Distinct":
                raise UnsupportedSyntaxError(
                    "Filters are not supported with aggregate function 'COUNT' with DISTINCT."
                )
            filter_condition = build(filter_condition)
    else:  # pragma: no cover
        from opteryx.exceptions import FunctionNotFoundError

        # Rewrite type-names used as cast functions: VARCHAR(x) → CAST(x AS VARCHAR)
        _TYPE_CAST_NAMES = {
            "VARCHAR": "VARCHAR",
            "INT": "INTEGER",
            "INTEGER": "INTEGER",
            "DOUBLE": "DOUBLE",
            "TIMESTAMP": "TIMESTAMP",
            "DATE": "DATE",
            "BOOLEAN": "BOOLEAN",
            "BLOB": "BLOB",
            "VARBINARY": "VARBINARY",
            "FLOAT": "FLOAT",
        }
        if func in _TYPE_CAST_NAMES and len(args) == 1:
            return Node(
                NodeType.CAST,
                left=args[0],
                value=_TYPE_CAST_NAMES[func],
                alias=alias,
            )

        likely_match = suggest_alternative(func, aggregator_names() + _list_functions())
        if likely_match is None:
            raise UnsupportedSyntaxError(f"Unknown function or aggregate '{func}'")
        raise FunctionNotFoundError(
            f"Unknown function or aggregate '{func}'. Did you mean '{likely_match}'?"
        )

    # rewrite COUNT_DISTINCT() to COUNT(DISTINCT)
    if func == "COUNT_DISTINCT":
        func = "COUNT"
        duplicate_treatment = "Distinct"

    node = Node(
        node_type=node_type,
        value=func,
        parameters=args,
        alias=alias,
        duplicate_treatment=duplicate_treatment,
        null_treatment=null_treatment,
        condition=filter_condition,
        order=order_by,
        limit=limit,
    )
    node.qualified_name = format_expression(node)
    return node


def hex_literal(branch, alias: Optional[List[str]] = None, key=None):
    value = int(branch, 16)
    return Node(
        NodeType.LITERAL,
        type=OrsoTypes.INTEGER,
        value=value,
        #    alias=alias or f"0x{branch}"
    )


def identifier(branch, alias: Optional[List[str]] = None, key=None):
    """idenitifier doesn't have a qualifier (recorded in source)"""
    if "Identifier" in branch:
        return build(branch["Identifier"], alias=alias)
    column = LogicalColumn(
        node_type=NodeType.IDENTIFIER,  # column type
        alias=alias,  # type: ignore
        source_column=branch["value"],  # the source column
    )
    alias_name = alias[0] if isinstance(alias, list) and alias else alias
    column.query_column = alias_name or column.source_column
    return column


def in_list(branch, alias: Optional[List[str]] = None, key=None):
    left_node = build(branch["expr"])
    value_nodes = [build(v) for v in branch["list"]]
    value_list = {v.value for v in value_nodes}
    element_type = {v.type for v in value_nodes}
    if len(element_type) > 1:
        raise ArrayWithMixedTypesError("Array in IN condition has values with mixed types.")
    element_type = element_type.pop()
    operator = "NotInList" if branch["negated"] else "InList"
    right_node = Node(
        node_type=NodeType.LITERAL,
        type=OrsoTypes.ARRAY,
        value=value_list,
        element_type=element_type,
    )
    return Node(
        node_type=NodeType.COMPARISON_OPERATOR,
        value=operator,
        left=left_node,
        right=right_node,
    )


def in_subquery(branch, alias: Optional[List[str]] = None, key=None):
    # if it's a sub-query we create a plan for it

    from opteryx.exceptions import UnsupportedSyntaxError

    raise UnsupportedSyntaxError("IN subqueries are currently not supported in Opteryx")

    from opteryx.planner.logical_planner.logical_planner import plan_query

    left = build(branch["expr"])
    ast = {}
    ast["Query"] = branch["subquery"]
    subquery_plan = plan_query(ast)
    exit_node = subquery_plan.get_exit_points()[0]
    subquery_plan.remove_node(exit_node, heal=True)
    operator = "NotInSubQuery" if branch["negated"] else "InSubQuery"

    sub_query = Node(NodeType.SUBQUERY, value=subquery_plan)
    return Node(
        NodeType.COMPARISON_OPERATOR,
        value=operator,
        left=left,
        right=sub_query,
    )


def in_unnest(branch, alias: Optional[List[str]] = None, key=None):
    left_node = build(branch["expr"])
    operator = "AllOpNotEq" if branch["negated"] else "AnyOpEq"
    right_node = build(branch["array_expr"])
    return Node(
        node_type=NodeType.COMPARISON_OPERATOR,
        value=operator,
        left=left_node,
        right=right_node,
    )


def is_compare(branch, alias: Optional[List[str]] = None, key=None):
    centre = build(branch)
    return Node(NodeType.UNARY_OPERATOR, value=key, centre=centre)


def json_access(branch, alias: Optional[List[str]] = None, key=None):
    identifier_node = build(branch["value"])
    key_node = build(branch["path"]["path"][0]["Bracket"]["key"])

    from opteryx.exceptions import IncorrectTypeError, UnsupportedSyntaxError

    if key_node.node_type == NodeType.IDENTIFIER:
        raise UnsupportedSyntaxError(
            "Subscript values must be integer literals, use `->` to access JSON fields."
        )

    if key_node.type != OrsoTypes.INTEGER:
        raise IncorrectTypeError(
            "Subscript values must be integer literals, use `->` to access JSON fields."
        )

    key_value = key_node.value
    if isinstance(key_value, str):
        key_value = f"'{key_value}'"
        return Node(
            NodeType.EXTRACTION_OPERATOR,
            value="Arrow",
            left=identifier_node,
            right=key_node,
            alias=alias or f"{identifier_node.current_name} -> {key_value}",
        )

    return Node(
        NodeType.EXTRACTION_OPERATOR,
        value="MapAccess",
        left=identifier_node,
        right=key_node,
        alias=alias or f"{identifier_node.current_name}[{key_value}]",
    )


def literal_boolean(branch, alias: Optional[List[str]] = None, key=None):
    """create node for a literal boolean branch"""
    return Node(NodeType.LITERAL, type=OrsoTypes.BOOLEAN, value=branch, alias=alias)


def literal_interval(branch, alias: Optional[List[str]] = None, key=None):
    """
    Create node for a time literal.

    This should look like this in the SQL:
        INTERVAL '1' YEAR
        INTERVAL '1 3' YEAR TO MONTH
    """
    parts = ("Year", "Month", "Day", "Hour", "Minute", "Second")

    if "Value" not in branch["value"]:
        raise SqlError("Invalid INTERVAL, expected format `INTERVAL '1' MONTH`")
    values = build(branch["value"]["Value"]).value
    if not isinstance(values, str):
        raise SqlError("Invalid INTERVAL, values must be provided as a VARCHAR.")

    values = values.split(" ")
    leading_unit = branch["leading_field"]

    if leading_unit is None:
        raise SqlError(f"Invalid INTERVAL, valid units are {', '.join(p.upper() for p in parts)}")

    unit_index = parts.index(leading_unit)

    month, microseconds = (0, 0)

    for index, value in enumerate(values):
        value = int(value)
        unit = parts[unit_index + index]
        if unit == "Year":
            month += 12 * value
        if unit == "Month":
            month += value
        if unit == "Day":
            microseconds += value * MICROSECONDS_PER_DAY
        if unit == "Hour":
            microseconds += value * MICROSECONDS_PER_HOUR
        if unit == "Minute":
            microseconds += value * MICROSECONDS_PER_MINUTE
        if unit == "Second":
            microseconds += value * MICROSECONDS_PER_SECOND

    interval = (month, microseconds)

    return Node(NodeType.LITERAL, type=OrsoTypes.INTERVAL, value=interval, alias=alias)


def literal_null(branch=None, alias: Optional[List[str]] = None, key=None):
    """create node for a literal null branch"""
    return Node(NodeType.LITERAL, type=OrsoTypes.NULL, alias=alias)


def literal_number(branch, alias: Optional[List[str]] = None, key=None):
    """create node for a literal number branch"""
    # we have one internal numeric type

    value = branch[0]
    try:
        # Try converting to int first
        value = int(value)
        return Node(
            NodeType.LITERAL,
            type=OrsoTypes.INTEGER,
            value=numpy.int64(branch[0]),  # value
            alias=alias,
        )
    except ValueError:
        # If int conversion fails, try converting to float
        value = float(value)
        return Node(
            NodeType.LITERAL,
            type=OrsoTypes.DOUBLE,
            value=numpy.float64(branch[0]),  # value
            alias=alias,
        )


def literal_string(branch, alias: Optional[List[str]] = None, key=None):
    """create node for a string branch, this is either a date or a string"""
    if not str(branch).isdigit():
        dte_value = dates.parse_iso(branch)
        if dte_value:
            if len(branch) <= 10:
                return Node(
                    NodeType.LITERAL,
                    type=OrsoTypes.DATE,
                    value=(dte_value.date() - _EPOCH_DATE).days,
                    alias=alias,
                )
            return Node(
                NodeType.LITERAL,
                type=OrsoTypes.TIMESTAMP,
                value=int((dte_value - _EPOCH_DT).total_seconds() * 1_000_000),
                alias=alias,
            )
    return Node(NodeType.LITERAL, type=OrsoTypes.VARCHAR, value=branch, alias=alias)


def match_against(branch, alias: Optional[List[str]] = None, key=None):
    columns = [identifier(col["Identifier"]) for col in branch["columns"][0]]
    match_to = build(branch["match_value"])

    return Node(
        NodeType.FUNCTION,
        value="_MATCH_AGAINST",
        parameters=[columns[0], match_to],
        alias=alias or f"MATCH ({columns[0].value}) AGAINST ({match_to.value})",
    )


def nested(branch, alias: Optional[List[str]] = None, key=None):
    return Node(
        node_type=NodeType.NESTED,
        centre=build(branch),
    )


def pattern_match(branch, alias: Optional[List[str]] = None, key=None):
    negated = branch["negated"]
    left = build(branch["expr"])
    right = build(branch["pattern"])
    is_any = branch.get("any", False)
    if key in ("PGRegexMatch", "SimilarTo"):
        key = "RLike"
    if negated:
        key = f"Not{key}"
    if is_any:
        key = f"AnyOp{key}"
        if right.node_type == NodeType.IDENTIFIER:
            raise UnsupportedSyntaxError(
                "LIKE ANY syntax incorrect, `column LIKE ANY (patterns)` expected."
            )
        if right.node_type == NodeType.NESTED:
            right = right.centre
        if right.type != OrsoTypes.ARRAY:
            right.value = (right.value,)
            right.type = OrsoTypes.ARRAY
    return Node(
        NodeType.COMPARISON_OPERATOR,
        value=key,
        left=left,
        right=right,
        alias=alias,
    )


def placeholder(value, alias: Optional[List[str]] = None, key=None):
    from opteryx.exceptions import ParameterError

    raise ParameterError("Unresolved parameter in query.")


def position(value, alias: Optional[List[str]] = None, key=None):
    sub = build(value["expr"])
    string = build(value["in"])
    return Node(NodeType.FUNCTION, value="POSITION", parameters=[sub, string], alias=alias)


def qualified_wildcard(branch, alias: Optional[List[str]] = None, key=None):
    parts = [build(part).value for part in branch[0]["ObjectName"]]
    qualifier = (".".join(parts),)
    return Node(NodeType.WILDCARD, value=qualifier, alias=alias)


def substring(branch, alias: Optional[List[str]] = None, key=None):
    node_node = Node(NodeType.LITERAL, type=OrsoTypes.NULL, value=None)
    string = build(branch["expr"])
    substring_from = build(branch["substring_from"]) or node_node
    substring_for = build(branch["substring_for"]) or node_node
    return Node(
        NodeType.FUNCTION,
        value="SUBSTRING",
        parameters=[string, substring_from, substring_for],
        alias=alias,
    )


def trim_string(branch, alias: Optional[List[str]] = None, key=None):
    who = build(branch["trim_what"])
    what = build(branch["expr"])
    where = branch["trim_where"] or "Both"

    function = "TRIM"
    if where == "Leading":
        function = "LTRIM"
    if where == "Trailing":
        function = "RTRIM"

    parameters = [what]
    if who is not None:
        parameters.append(who)

    return Node(
        NodeType.FUNCTION,
        value=function,
        parameters=parameters,
        alias=alias,
    )


def tuple_literal(branch, alias: Optional[List[str]] = None, key=None):
    # Tuples can have values of different types
    # if they all are the same type, be explicit about it
    node_values = [build(t) for t in branch]
    values = [t.value for t in node_values]

    # see if we can specify the element type for the arrat
    node_types = {t.type for t in node_values}
    element_type = None
    if len(node_types) == 1:
        element_type = node_types.pop()
    literal_type = OrsoTypes.ARRAY
    if element_type in (OrsoTypes.INTEGER, OrsoTypes.DOUBLE, OrsoTypes.DECIMAL):
        literal_type = OrsoTypes.VECTOR
        element_type = OrsoTypes.DOUBLE

    if values and isinstance(values[0], dict):
        values = [build(val["Identifier"]).value for val in values]
    return Node(
        NodeType.LITERAL,
        type=literal_type,
        element_type=element_type,
        value=tuple(values),
        alias=alias,
    )


def typed_string(branch, alias: Optional[List[str]] = None, key=None):
    data_type = branch["data_type"]

    if isinstance(data_type, dict):
        type_key = next(iter(data_type))
        if type_key == "Timestamp" and data_type[type_key] not in (
            (None, "None"),
            (None, "WithoutTimeZone"),
        ):
            raise UnsupportedSyntaxError("TIMESTAMPS do not support `TIME ZONE`")
        data_type = type_key
    data_type = data_type.upper()

    raise UnsupportedSyntaxError(
        f"Type-prefixed string literals are no longer supported for {data_type}. "
        f"Use CAST(... AS {data_type}) instead. Only INTERVAL retains prefix literal syntax."
    )


def unary_op(branch, alias: Optional[List[str]] = None, key=None):
    if branch["op"] == "Not":
        centre = build(branch["expr"])
        return Node(node_type=NodeType.NOT, centre=centre)
    if branch["op"] == "Minus":
        node = literal_number(branch["expr"]["Value"]["value"]["Number"], alias=alias)
        node.value = 0 - node.value
        return node
    if branch["op"] == "Plus":
        return literal_number(branch["expr"]["Value"]["value"]["Number"], alias=alias)


def wildcard_filter(branch, alias: Optional[List[str]] = None, key=None):
    """a wildcard"""
    except_columns = None
    if isinstance(branch, dict) and branch.get("opt_except") is not None:
        except_columns = [build({"Identifier": branch["opt_except"]["first_element"]})]
        except_columns.extend(
            [build({"Identifier": e}) for e in branch["opt_except"]["additional_elements"]]
        )
    return Node(NodeType.WILDCARD, except_columns=except_columns)


# ----------


def unsupported(branch, alias: Optional[List[str]] = None, key=None):
    """raise an error"""
    print("[INTERNAL]", branch)
    raise SqlError(f"Unhandled token in Syntax Tree `{key}`")


def build(value, alias: Optional[List[str]] = None, key=None):
    """
    Extract values from a value node in the AST and create a ExpressionNode for it

    More of the builders will be migrated to this approach to keep the code
    more succinct and easier to read.
    """
    ignored = ("filter",)

    if value in ("Null", "Wildcard"):
        return BUILDERS[value](value)
    if isinstance(value, dict):
        key = next(iter(value))
        if key in ignored:
            return None
        return BUILDERS.get(key, unsupported)(value[key], alias, key)
    if isinstance(value, list):
        return [build(item, alias) for item in value]
    return None


# parts to build the literal parts of a query
BUILDERS = {
    "AnyOp": any_op,
    "All": lambda x, y, z: [NodeType.WILDCARD],
    "AllOp": all_op,
    "Array": array,  # not actually implemented
    "Between": between,
    "BinaryOp": binary_op,
    "Boolean": literal_boolean,
    "Case": case_when,
    "Cast": cast,
    "Ceil": ceiling,
    "CompoundIdentifier": compound_identifier,
    "DoubleQuotedString": literal_string,
    "Exists": exists,
    "Expr": build,
    "Expressions": expressions,
    "ExprWithAlias": expression_with_alias,
    "Extract": extract,
    "Floor": floor,
    "Function": function,
    "HexStringLiteral": hex_literal,
    "Identifier": identifier,
    "ILike": pattern_match,
    "InList": in_list,
    "InSubquery": in_subquery,
    "Interval": literal_interval,
    "InUnnest": in_unnest,
    "IsFalse": is_compare,
    "IsNotFalse": is_compare,
    "IsNotNull": is_compare,
    "IsNotTrue": is_compare,
    "IsNull": is_compare,
    "IsTrue": is_compare,
    "JsonAccess": json_access,
    "Like": pattern_match,
    "MatchAgainst": match_against,
    "Nested": nested,
    "Null": literal_null,
    "Number": literal_number,
    "Placeholder": placeholder,
    "Position": position,
    "QualifiedWildcard": qualified_wildcard,
    "RLike": pattern_match,
    "SingleQuotedString": literal_string,
    "SimilarTo": pattern_match,
    "Substring": substring,
    "Tuple": tuple_literal,
    "Trim": trim_string,
    "TypedString": typed_string,
    "UnaryOp": unary_op,
    "Unnamed": build,
    "Value": build,
    "value": build,
    "Wildcard": wildcard_filter,
    "UnnamedExpr": build,
}
