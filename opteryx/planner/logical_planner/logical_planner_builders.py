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
import decimal
from typing import List, Optional

from draken.draken_native import LogicalKind
from draken.draken_native import ipv4_format
from draken.draken_native import ipv4_parse

from opteryx.compiled.expression.compiled_expression import _BOP_CODE
from opteryx.exceptions import (
    ArrayWithMixedTypesError,
    FunctionNotFoundError,
    SqlError,
    UnsupportedSyntaxError,
    compose,
    did_you_mean,
    md_code,
    md_syntax,
)
from opteryx.expression import NodeType, format_expression
from opteryx.expression.evaluator.arithmetic import resolve_binary_op
from opteryx.expression.functions import functions as _list_functions
from opteryx.expression.functions import is_function as _is_function
from opteryx.expression.intervals import (
    MICROSECONDS_PER_DAY,
    MICROSECONDS_PER_HOUR,
    MICROSECONDS_PER_MINUTE,
    MICROSECONDS_PER_SECOND,
)
from opteryx.expression.operator_catalog import get_operator_for_sql_symbol
from opteryx.expression.operator_catalog import get_operator_node_type
from opteryx.models import LogicalColumn, Node
from opteryx.operators.aggregate.helpers import aggregator_names, is_aggregator
from opteryx.operators.window.helpers import NAVIGATION_FUNCTIONS, WINDOW_FUNCTIONS
from opteryx.types.logical_type import (
    ARRAY as _CT_ARRAY,
)
from opteryx.types.logical_type import integer_bounds
from opteryx.types.logical_type import (
    BOOLEAN as _CT_BOOLEAN,
)
from opteryx.types.logical_type import (
    DATE as _CT_DATE,
)
from opteryx.types.logical_type import (
    DECIMAL as _CT_DECIMAL,
)
from opteryx.types.logical_type import (
    FLOAT64 as _CT_FLOAT64,
)
from opteryx.types.logical_type import (
    INT64 as _CT_INT64,
)
from opteryx.types.logical_type import (
    INTERVAL as _CT_INTERVAL,
)
from opteryx.types.logical_type import (
    NULL as _CT_NULL,
)
from opteryx.types.logical_type import (
    TIME as _CT_TIME,
)
from opteryx.types.logical_type import (
    UINT64 as _CT_UINT64,
)
from opteryx.types.logical_type import (
    TIMESTAMP as _CT_TIMESTAMP,
)
from opteryx.types.logical_type import (
    VARBINARY as _CT_VARBINARY,
)
from opteryx.types.logical_type import (
    VARCHAR as _CT_VARCHAR,
)
from opteryx.types.logical_type import (
    VARIANT as _CT_VARIANT,
)
from opteryx.types.logical_type import (
    ColumnType,
    LogicalCategory,
    TimestampUnit,
)
from opteryx.utils import dates, suggest_alternative
from opteryx.utils.vector_types import VectorType, get_vector_type


def _span_of(first: dict, last: Optional[dict] = None):
    """Flatten sqlparser's span onto an identifier we are building.

    sqlparser hangs `{"start": {"line", "column"}, "end": {...}}` off every `Ident`,
    indexing the text the parser was given. It is dropped for anything without one -
    an AST node the rewriter synthesized, or a parameter substituted after the parse -
    because a position we made up is worse than no position at all.

    Pass `last` to span a dotted name from its first part to its last.
    """
    start = first.get("span")
    if start is None:
        return None
    end = start if last is None else last.get("span")
    if end is None:
        end = start
    return (
        start["start"]["line"],
        start["start"]["column"],
        end["end"]["line"],
        end["end"]["column"],
    )


# Epoch constants for converting datetime literals to Draken-native integers.
# DATE literals are stored as int (days since epoch, fits int32).
# TIMESTAMP literals are stored as int (microseconds since epoch, int64).
_EPOCH_DATE = datetime.date(1970, 1, 1)
_EPOCH_DT = datetime.datetime(1970, 1, 1)

# Module-level ColumnType sentinels for factories (singletons can be compared directly).
_SENTINEL_TIMESTAMP = _CT_TIMESTAMP()
_SENTINEL_TIME = _CT_TIME()

# Misspellings the edit-distance detector will not reach - LENGTH is three edits from
# LEN, past the cap, and widening the cap to catch it would let the detector start
# guessing at names that are not near-misses at all.
_COMMON_FUNCTION_ERRORS = {
    "LEN": "LENGTH",
}


def _unknown_function(func: str, span=None) -> FunctionNotFoundError:
    """The one Unknown Function error, with a suggestion when there is a near-miss.

    Returned rather than raised so call sites keep their `raise` visible. Built
    through the exception's own constructor: the message and the "did you mean"
    formatting live there, and the two call sites that hand-built their own string
    are how the product ended up with two different spellings of this error, one of
    which ended in a comma.

    `span` is where the name was written, so the planner boundary can put a caret
    under it - see `SqlError`. Pass it whenever it is to hand; a misspelled function
    is exactly the case a reader wants pointed at.
    """
    likely = _COMMON_FUNCTION_ERRORS.get(func) or suggest_alternative(
        func, aggregator_names() + _list_functions()
    )
    return FunctionNotFoundError(function=func, suggestion=likely, span=span)


def _evaluate_fixed_temporal_function(function_name: str):
    now = datetime.datetime.now(datetime.UTC).replace(tzinfo=None)
    if function_name in ("NOW", "CURRENT_TIMESTAMP", "UTC_TIMESTAMP"):
        return now, _SENTINEL_TIMESTAMP
    if function_name in ("CURRENT_DATE", "TODAY"):
        return now.date(), _CT_DATE
    if function_name == "YESTERDAY":
        return (now - datetime.timedelta(days=1)).date(), _CT_DATE
    if function_name == "CURRENT_TIME":
        return now.time(), _SENTINEL_TIME
    return None, None


def _extract_single_scalar(value):
    value_type = get_vector_type(value)
    if value_type != VectorType.UNKNOWN:
        if len(value) != 1:
            raise UnsupportedSyntaxError(
                "Time-travel expressions must evaluate to a single scalar value. Time travel takes a value that can be worked out before the query runs, for example `TIMESTAMP AS OF '2024-01-01'` or `TIMESTAMP AS OF NOW() - INTERVAL '1' DAY`."
            )
        return value[0]
    # Already a Python scalar
    return value


def _as_binary_operand_array(value, value_type):
    if value_type is None:
        return [value]
    cat = value_type.category
    if cat == LogicalCategory.INTERVAL:
        return [value]
    if cat == LogicalCategory.TIMESTAMP:
        timestamp = dates.parse_iso(value)
        if timestamp is None:
            raise UnsupportedSyntaxError(
                "Unable to parse timestamp value in time-travel expression. Write it as a timestamp literal, for example `'2024-01-01 12:00'`."
            )
        return [int((timestamp - _EPOCH_DT).total_seconds() * 1_000_000)]
    if cat == LogicalCategory.DATE:
        dt = dates.parse_iso(value)
        if dt is None:
            raise UnsupportedSyntaxError("Unable to parse date value in time-travel expression. Write it as a date literal, for example `'2024-01-01'`.")
        return [(dt.date() - _EPOCH_DATE).days]
    return [value]


def _as_function_parameter_array(value, value_type):
    if value_type is None:
        return [value]
    cat = value_type.category
    if cat == LogicalCategory.TIMESTAMP:
        dt = dates.parse_iso(value)
        if dt is None:
            raise UnsupportedSyntaxError("Unable to parse temporal function argument. Time travel takes a value that can be worked out before the query runs, for example `TIMESTAMP AS OF '2024-01-01'` or `TIMESTAMP AS OF NOW() - INTERVAL '1' DAY`.")
        return [int((dt - _EPOCH_DT).total_seconds() * 1_000_000)]
    if cat == LogicalCategory.DATE:
        dt = dates.parse_iso(value)
        if dt is None:
            raise UnsupportedSyntaxError("Unable to parse temporal function argument. Time travel takes a value that can be worked out before the query runs, for example `TIMESTAMP AS OF '2024-01-01'` or `TIMESTAMP AS OF NOW() - INTERVAL '1' DAY`.")
        return [(dt.date() - _EPOCH_DATE).days]
    return [value]


def _type_from_value(value):
    if value is None:
        return _CT_NULL
    if isinstance(value, bool):
        return _CT_BOOLEAN
    if isinstance(value, datetime.datetime):
        return _SENTINEL_TIMESTAMP
    if isinstance(value, datetime.time):
        return _SENTINEL_TIME
    if isinstance(value, datetime.date):
        return _CT_DATE
    if isinstance(value, int):
        return _CT_INT64
    if isinstance(value, float):
        return _CT_FLOAT64
    if isinstance(value, (bytes, bytearray)):
        return _CT_VARBINARY
    if isinstance(value, str):
        return _CT_VARCHAR
    if isinstance(value, tuple) and len(value) == 2:
        return _CT_INTERVAL
    return None


def _interval_to_past_timestamp(interval_value):
    months = abs(int(interval_value[0]))
    microseconds = abs(int(interval_value[1]))
    now = datetime.datetime.now(datetime.UTC).replace(tzinfo=None)
    dt = dates.add_months(now, -months) if months else now
    return dt - datetime.timedelta(microseconds=microseconds)


def _apply_interval_scalar(base_value, base_type, interval_value, operator: str):
    """Apply an INTERVAL to a DATE or TIMESTAMP scalar.

    The bind-time kernel resolver converts dates to plain integers.
    Unfortunately the interval kernels expect a PyArrow temporal array and
    therefore return ``null`` when provided with a raw integer.  That bug
    manifests during timetravel evaluation, e.g. ``CURRENT_DATE - INTERVAL '7' DAY``;
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
        _node_ct = node.type  # ColumnType or None
        if (
            _node_ct is not None
            and _node_ct.category == LogicalCategory.INTERVAL
            and apply_interval_literal_to_now
        ):
            return _interval_to_past_timestamp(node.value), _SENTINEL_TIMESTAMP
        return node.value, _node_ct

    if node.node_type == NodeType.NESTED:
        return _evaluate_timetravel_expression(node.centre, apply_interval_literal_to_now)

    if node.node_type == NodeType.FUNCTION:
        fixed_value, fixed_type = _evaluate_fixed_temporal_function(node.value)
        if fixed_type is not None:
            return fixed_value, fixed_type

        parameter_values = []
        resolved_parameters = []
        scalar_parameters = []
        for parameter in node.parameters:
            value, value_type = _evaluate_timetravel_expression(parameter)
            scalar_parameters.append((value, value_type))
            parameter_values.append(_as_function_parameter_array(value, value_type))
            resolved_parameters.append(Node(NodeType.LITERAL, type=value_type, value=value))

        if node.value == "TRUNC" and len(scalar_parameters) == 2:
            trunc_value, trunc_value_type = scalar_parameters[0]
            unit_value, unit_type = scalar_parameters[1]
            _tcat = trunc_value_type.category if trunc_value_type is not None else None
            _ucat = unit_type.category if unit_type is not None else None
            if (
                _tcat in (LogicalCategory.DATE, LogicalCategory.TIMESTAMP)
                and _ucat == LogicalCategory.VARCHAR
            ):
                if isinstance(trunc_value, datetime.date) and not isinstance(
                    trunc_value, datetime.datetime
                ):
                    trunc_value = datetime.datetime.combine(trunc_value, datetime.time.min)
                return dates.truncate_single(trunc_value, unit_value.lower()), _SENTINEL_TIMESTAMP

        try:
            from opteryx.expression.functions import FunctionResolutionContext
            from opteryx.expression.functions import get_catalog as _get_catalog

            _catalog = _get_catalog()
            _func_def = _catalog.get_definition(node.value)
            if _func_def is None or not _func_def.overloads:
                raise _unknown_function(node.value, node.span)
            resolved = _catalog.resolve(
                node.value, resolved_parameters, FunctionResolutionContext(schema={}, bound_args={})
            )
            if resolved is None:
                raise _unknown_function(node.value, node.span)
            result = resolved.selected_overload.kernel.callable_ref(*parameter_values)
        except (UnsupportedSyntaxError, FunctionNotFoundError):
            raise
        except Exception as err:
            raise UnsupportedSyntaxError(
                f"Unable to evaluate time-travel function '{node.value}'. Time travel takes a value that can be worked out before the query runs, for example `TIMESTAMP AS OF '2024-01-01'` or `TIMESTAMP AS OF NOW() - INTERVAL '1' DAY`."
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
            _lcat = left_type.category if left_type is not None else None
            _rcat = right_type.category if right_type is not None else None
            if (
                _lcat in (LogicalCategory.DATE, LogicalCategory.TIMESTAMP)
                and _rcat == LogicalCategory.INTERVAL
            ):
                return (
                    _apply_interval_scalar(left_value, left_type, right_value, node.value),
                    _SENTINEL_TIMESTAMP,
                )
            if _lcat == LogicalCategory.INTERVAL and _rcat in (
                LogicalCategory.DATE,
                LogicalCategory.TIMESTAMP,
            ):
                # interval +/- date is effectively the same as date +/- interval
                return (
                    _apply_interval_scalar(right_value, right_type, left_value, node.value),
                    _SENTINEL_TIMESTAMP,
                )

        left = _as_binary_operand_array(left_value, left_type)
        right = _as_binary_operand_array(right_value, right_type)

        op_code = _BOP_CODE.get(node.value, 0)
        if op_code == 0:
            raise UnsupportedSyntaxError(
                f"Time-travel expression: unsupported operator '{node.value}'. Only `+` and `-` over a timestamp and an **INTERVAL** can be used here."
            )
        kernel = resolve_binary_op(op_code, left_type, right_type)
        try:
            result = kernel(left, right)
        except Exception as err:
            raise UnsupportedSyntaxError(
                f"Unable to evaluate time-travel expression with operator '{node.value}'. Time travel takes a value that can be worked out before the query runs, for example `TIMESTAMP AS OF '2024-01-01'` or `TIMESTAMP AS OF NOW() - INTERVAL '1' DAY`."
            ) from err

        scalar = _extract_single_scalar(result)
        return scalar, _type_from_value(scalar)

    raise UnsupportedSyntaxError("Time-travel expression must resolve to a scalar value. Time travel takes a value that can be worked out before the query runs, for example `TIMESTAMP AS OF '2024-01-01'` or `TIMESTAMP AS OF NOW() - INTERVAL '1' DAY`.")


def _normalize_timetravel_value(value, value_type: Optional[ColumnType]):
    """Coerce a resolved time-travel scalar into a real ``datetime``/``date``.

    Literal DATE/TIMESTAMP values are constant-folded elsewhere into their
    Draken-native physical representation (int days/microseconds since
    epoch), and plain string literals are never parsed. Connectors need a
    real Python temporal object (they call ``.timestamp()`` on it), so
    normalize here based on the resolved ``ColumnType`` category.
    """
    if value is None or isinstance(value, (datetime.datetime, datetime.date)):
        return value

    cat = value_type.category if value_type is not None else None

    if cat == LogicalCategory.TIMESTAMP:
        if isinstance(value, str):
            dt = dates.parse_iso(value)
            if dt is None:
                raise UnsupportedSyntaxError(
                    "Unable to parse timestamp value in time-travel expression."
                )
            return dt
        if isinstance(value, (int, float)):
            return _EPOCH_DT + datetime.timedelta(microseconds=int(value))
        return value

    if cat == LogicalCategory.DATE:
        if isinstance(value, str):
            dt = dates.parse_iso(value)
            if dt is None:
                raise UnsupportedSyntaxError(
                    "Unable to parse date value in time-travel expression."
                )
            return dt.date()
        if isinstance(value, (int, float)):
            return _EPOCH_DATE + datetime.timedelta(days=int(value))
        return value

    if isinstance(value, str):
        dt = dates.parse_iso(value)
        if dt is None:
            raise UnsupportedSyntaxError(
                "Unable to parse time-travel expression value. Time travel takes a value that can be worked out before the query runs, for example `TIMESTAMP AS OF '2024-01-01'` or `TIMESTAMP AS OF NOW() - INTERVAL '1' DAY`."
            )
        return dt

    return value


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
            f"Time-travel syntax expects exactly 1 argument, got {len(args)}. Write `TIMESTAMP AS OF <expression>`."
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
    value, value_type = _evaluate_timetravel_expression(
        expression_node, apply_interval_literal_to_now=True
    )

    if value is None:
        raise UnsupportedSyntaxError(
            "Time-travel expression must be `TIMESTAMP AS OF <expression>`."
        )

    return _normalize_timetravel_value(value, value_type)


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
    from opteryx.types import logical_type as _lt

    value_nodes = [build(elem) for elem in branch["elem"]]
    value_list = [v.value for v in value_nodes]
    element_ct_set = {v.type for v in value_nodes}
    if len(element_ct_set) > 1:
        raise ArrayWithMixedTypesError("Literal ARRAY has values with mixed types. Every element has to share one type; cast them so they match.")
    # element_ct is ColumnType (Phase 2); extract its category for numeric check.
    element_ct = element_ct_set.pop() if len(element_ct_set) == 1 else _CT_VARCHAR
    elem_cat = element_ct.category if isinstance(element_ct, ColumnType) else element_ct
    if elem_cat in (LogicalCategory.INTEGER, LogicalCategory.FLOAT, LogicalCategory.DECIMAL):
        # Numeric arrays become ARRAY<FLOAT64>; dimension unknown at parse time.
        element_ct = _CT_FLOAT64
    literal_type = _CT_ARRAY(element_ct)

    return Node(
        node_type=NodeType.LITERAL,
        type=literal_type,
        element_type=None,  # element embedded in type
        value=value_list,
    )


def between(branch, alias: Optional[List[str]] = None, key=None):
    # BETWEEN is LOWERED to a pair of comparisons, so the node returned here renders
    # as the rewrite (`(id >= 1 AND id <= 2)`), not as the SQL the user wrote. The
    # alias must reach that outermost AND/OR node — without it the binder names the
    # column after the rewrite, and CREATE TABLE ... AS stores that name.
    expr = build(branch["expr"])
    low = build(branch["low"])
    high = build(branch["high"])
    inverted = branch["negated"]

    if inverted:
        # NOT BETWEEN: expr < low OR expr > high — the negation of the inclusive
        # form below, so the bounds are STRICT here and the connective is OR.
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

        return Node(NodeType.OR, left=left_node, right=right_node, alias=alias)
    else:
        # BETWEEN: expr >= low AND expr <= high — INCLUSIVE at both ends.
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

        return Node(NodeType.AND, left=left_node, right=right_node, alias=alias)


def binary_op(branch, alias: Optional[List[str]] = None, key=None):
    left = build(branch["left"])
    operator = branch["op"]
    right = build(branch["right"])

    # Dialect-specific operator mapping. A custom operator arrives as its SQL
    # spelling (`<<=`), because that is what sqlparser writes when an AST is
    # serialised back to SQL to store a view; the canonical name is recovered
    # here. An unknown symbol is left as-is so the error below names the text
    # that was actually parsed.
    if isinstance(operator, dict):
        symbol = operator["Custom"]
        operator = get_operator_for_sql_symbol(symbol) or symbol

    if operator in ("PGRegexMatch", "SimilarTo"):
        operator = "RLike"
    if operator in ("PGRegexNotMatch", "NotSimilarTo"):
        operator = "NotRLike"

    operator_type = get_operator_node_type(operator)
    if operator_type is None:
        raise UnsupportedSyntaxError(f"Unsupported operator '{operator}'. Check the spelling against the operators the dialect accepts.")

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

    return Node(
        NodeType.CASE,
        conditions=conditions,
        results=results,
        else_result=else_result,
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

    # CAST ... FORMAT '<pattern>' — the sqlparser AST already parses this (BigQuery/
    # Snowflake syntax) into branch["format"]; only a literal string pattern is
    # supported (no `AT TIME ZONE`, no computed pattern — the native kernel bakes
    # the pattern into a bind-time context struct, see compiled_expression.pyx).
    raw_format = branch.get("format")
    format_literal_node = None
    if raw_format is not None:
        _stripped_type = normalized_type.replace("TRY_", "")
        if _stripped_type not in ("VARCHAR", "BLOB", "TIMESTAMP", "DATE"):
            raise UnsupportedSyntaxError(
                f"**CAST** ... FORMAT is only supported for VARCHAR/BLOB/TIMESTAMP/DATE targets, not {_stripped_type}."
            )
        if "Value" not in raw_format:
            raise UnsupportedSyntaxError("**CAST** ... FORMAT does not support `AT TIME ZONE`.")
        _fmt_value = raw_format["Value"]["value"]
        if "SingleQuotedString" in _fmt_value:
            _fmt_str = _fmt_value["SingleQuotedString"]
        elif "DoubleQuotedString" in _fmt_value:
            _fmt_str = _fmt_value["DoubleQuotedString"]
        else:
            raise UnsupportedSyntaxError("**CAST** ... FORMAT requires a string literal pattern.")
        format_literal_node = build_literal_node(_fmt_str)

    # Handle literal value casting at compile time.
    # NVARCHAR is routed through the runtime CAST node instead, so literals go
    # through the same UTF-8-validating kernel and materialise as a true
    # DRAKEN_NVARCHAR vector (constant-folding would yield a VARCHAR constant).
    # DECIMAL is likewise routed through the runtime CAST node: the literal-fold path
    # (_cast_literal_value) ignores the precision/scale parameters, so folding
    # CAST(<lit> AS DECIMAL(p,s)) would silently drop (p,s) and skip quantization.
    # The runtime CAST node carries `parameters=[precision, scale]` and threads them
    # through _build_decimal_closure (bare DECIMAL → DECIMAL(18,6), Decision F).
    # ARRAY splits on the SOURCE literal, because only some sources are readable by the
    # native kernel. `_extract_data_type` puts the `ARRAY<element>` element type in
    # `cast_parameters` (the VECTOR(384) channel), so whichever way this goes the element
    # type is carried — `_cast_literal_value` folds it into the literal's ColumnType, and
    # the runtime CAST node hands it to the binder as `parameters=[element]`.
    #   - array literal / NULL source -> FOLD. draken_cast_to_array reads its elements
    #     from the column owner's CHILD vector, which only a real column has. Such a
    #     literal has no child, so the kernel cannot see its own input and silently
    #     yields empty arrays (it does not refuse). Folding is not an optimization here;
    #     it is the only way these shapes can run — the same reason VECTOR folds.
    #   - every other literal source (notably VARCHAR holding JSON array text) -> runtime
    #     CAST node. That input the kernel CAN read, and folding it would be a second,
    #     Python-side implementation of draken_cast_to_array (CLAUDE.md §3/§11).
    # A FORMAT-bearing cast is likewise never folded: the pattern semantics live
    # entirely in the native kernel (sql_temporal_format.h) — folding would mean a
    # second, Python-side implementation of the same token engine (CLAUDE.md §3/§11
    # bans duplicated logic between Python and native).
    _base_target = normalized_type.replace("TRY_", "")
    _source_category = source_expr.type.category if source_expr.type is not None else None
    if _base_target == "ARRAY":
        _fold_target = _source_category in (LogicalCategory.ARRAY, LogicalCategory.NULL)
    else:
        _fold_target = _base_target not in ("NVARCHAR", "DECIMAL")

    if (
        source_expr.node_type == NodeType.LITERAL
        and _fold_target
        and format_literal_node is None
    ):
        return _cast_literal_value(source_expr, normalized_type, kind, alias, cast_parameters)

    # For non-literals, return a CAST node that will be evaluated at runtime
    # CAST nodes have the source in 'left', target type in 'value', and optional params in 'parameters'
    return Node(
        NodeType.CAST,
        left=source_expr,
        value=normalized_type.upper(),
        parameters=cast_parameters,
        format=format_literal_node,
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
        # FLOAT(p) — the SQL standard's binary precision: p <= 24 is single
        # precision, above it double. Resolved HERE because the dict flattening
        # below keeps only the key, which would drop the argument silently — and a
        # dropped precision means `FLOAT(24)` quietly returning a double, i.e. the
        # opposite of what was asked for. Bare FLOAT (no precision) is not this
        # case and stays FLOAT64 (see _normalize_cast_type).
        if type_key == "Float" and isinstance(data_type[type_key], dict):
            _precision = data_type[type_key].get("Precision")
            if _precision is not None:
                return "FLOAT32" if int(_precision) <= 24 else "FLOAT64"
        data_type = type_key

    # Handle custom types
    if "Custom" in data_type:
        # Custom is [ObjectName, args]: `VECTOR(384)` parses as
        # [[{Identifier: VECTOR}], ["384"]]. Only the NAME was read, so every
        # parenthesised argument on a custom type was silently dropped and
        # `CAST(x AS VECTOR(384))` arrived at the binder as a bare "VECTOR" — which
        # parse_column_type rejects, since a VECTOR has no meaning without a width.
        # Carry the args through the same channel DECIMAL's precision/scale use.
        _custom = branch["data_type"]["Custom"]
        data_type = _custom[0][0]["Identifier"]["value"].upper()
        for _param in (_custom[1] if len(_custom) > 1 else []):
            # sqlparser hands these back as strings; keep an integral one integral so
            # the binder can read a dimension without re-parsing.
            _p = str(_param)
            args.append(build_literal_node(int(_p) if _p.lstrip("-").isdigit() else _p))

    # Both parameter reads below key off the AST NODE, not off a substring of the
    # type's NAME. Asking `"array" in data_type.lower()` said yes to SUBARRAY and
    # MY_ARRAY — custom type names that have no `Array` node — and the very next
    # line indexed `["Array"]` unconditionally, so a name that merely CONTAINED
    # the word crashed with a raw `KeyError: 'Array'` instead of an SqlError.
    _raw_type = branch["data_type"]

    # Handle DECIMAL precision and scale
    if isinstance(_raw_type, dict) and "PrecisionAndScale" in _raw_type.get("Decimal", {}):
        precision = _raw_type["Decimal"]["PrecisionAndScale"][0]
        scale = _raw_type["Decimal"]["PrecisionAndScale"][1]
        args.append(build_literal_node(precision))
        args.append(build_literal_node(scale))

    # Handle ARRAY element types
    if isinstance(_raw_type, dict) and "Array" in _raw_type:
        element_key = _raw_type["Array"].get("AngleBracket", {"Varchar": None})
        if isinstance(element_key, dict):
            element_key = next(iter(element_key))
        if isinstance(element_key, str):
            element_key = build_literal_node(element_key.upper())
            args.append(element_key)

    return data_type


# The candidate set the typo detector matches an unrecognized name against.
#
# CANONICAL names ONLY — these are the names `str(ColumnType)` renders and the
# catalog stores. A suggestion is an instruction to the user, so it must name the
# type they will see echoed back. That rules out the three implied aliases the
# dialect still ACCEPTS in CREATE and CAST (INTEGER, FLOAT, and the DOUBLE
# spelling of FLOAT64) — accepted is not the same as recommended, and showing
# INTEGER would teach a name the rest of the engine never uses. STRUCT and BLOB
# are absent for the same reason: they resolve to NVARCHAR and VARBINARY.
_CAST_TARGET_NAMES = (
    "TIMESTAMP",
    "TIME",
    "DATE",
    "VARCHAR",
    "NVARCHAR",
    "DECIMAL",
    "BOOL",
    "ARRAY",
    "VECTOR",
    "INTERVAL",
    "VARBINARY",
    "INT8",
    "INT16",
    "INT32",
    "INT64",
    "FLOAT32",
    "FLOAT64",
    "UINT8",
    "UINT16",
    "UINT32",
    "UINT64",
    "IPV4",
)

# The THREE implied aliases the dialect ACCEPTS in CREATE and CAST, mapped to the
# canonical name they mean. They are typo-MATCHED (so `UINTEGER` is still
# recognized as a slip for the INTEGER spelling) but never RENDERED — the
# suggestion always names the canonical type, so we never teach a spelling the
# rest of the engine does not use. Anything not canonical and not one of these
# three is rejected outright, which is why DOUBLE, BLOB and STRUCT are absent.
_IMPLIED_ALIAS_CANONICAL = {
    "INTEGER": "INT64",
    "FLOAT": "FLOAT64",
    "BOOLEAN": "BOOL",
}


def _normalize_cast_type(data_type: str) -> str:
    """Normalize and validate the cast target type."""
    lower_type = data_type.lower()
    upper_type = data_type.upper()

    # Preserve internal temporal type forms (from SQL rewriter)
    # These should pass through unchanged to the binder for unit extraction
    if upper_type in (
        "_TIMESTAMP_NS",
        "_TIMESTAMP_MS",
        "_TIMESTAMP_S",
        "_TIMESTAMP_US",
        "_TIMESTAMP_DAYS",
    ):
        return upper_type

    # NVARCHAR is its own name, matched exactly. It was `"nvarchar" in lower_type`,
    # which also claimed MYNVARCHAR and every other name ending in it — the same
    # silent-wrong-type the exact table below exists to prevent. It no longer needs
    # to precede VARCHAR either, now that VARCHAR is matched exactly too.
    if lower_type == "nvarchar":
        return "NVARCHAR"

    # Unsigned integer widths (E33) — exact match, ahead of the substring rules below
    # (sqlparser-rs parses UINT8/16/32/64 as a distinct DataType, e.g. "UInt32").
    if upper_type in ("UINT8", "UINT16", "UINT32", "UINT64"):
        return upper_type

    # Signed integer widths and FLOAT32 — exact match, and it MUST sit ahead of the
    # substring table below, which would otherwise never see them anyway but would
    # catch a future alias.
    #
    # ⚠ INT8 HERE MEANS EIGHT BITS, NOT EIGHT BYTES. Postgres spells BIGINT as
    # `int8` (8 BYTES) and sqlparser-rs's DataType is named after that spelling —
    # so this is a deliberate divergence from Postgres, not an oversight. It is
    # forced: this engine's own type vocabulary is INT8/INT16/INT32/INT64 by BIT
    # width (draken's DrakenType, `str(ColumnType)`, and the catalog's stored
    # names all agree), and INT8 meaning 8 bytes next to INT64 meaning 8 bytes
    # would be indefensible. Postgres's `int8` is spelled BIGINT/INTEGER here.
    #
    # INT64 is the CANONICAL name — it is what `str(ColumnType)` renders and what
    # the catalog stores, so a user must be able to type back the name the engine
    # showed them. It was the only widthed numeric spelling the dialect rejected
    # (INT8/16/32, UINT8..64, FLOAT32/64 all worked), which left `CAST(x AS INT64)`
    # failing with "did you mean 'UINT64'?" — a signed request pointed at the
    # UNSIGNED type. INTEGER remains accepted as an implied alias for it.
    if upper_type in ("INT8", "INT16", "INT32", "INT64", "FLOAT32"):
        return upper_type

    # FLOAT and FLOAT64 are DOUBLE — one spelling reaches the tables downstream.
    #
    # FLOAT means DOUBLE PRECISION here, matching `_SQL_NAME_ALIASES` on the read
    # side: FLOAT is what the catalog persists for the FLOAT category, so pointing
    # it at FLOAT32 would narrow every stored float column. REAL is the single-
    # precision spelling per the standard, and FLOAT32 is the canonical name for
    # it. FLOAT(p) never reaches here — _extract_data_type resolves the precision
    # to an exact width first.
    if upper_type in ("FLOAT", "FLOAT64"):
        return "DOUBLE"

    # IPv4 — exact match, and it MUST sit ahead of the substring rules below:
    # sqlparser hands this through as a custom type name, and "ipv4" contains no
    # mapped substring today, but the substring table is a trap waiting to catch
    # any future alias. Matched exactly so it cannot be shadowed.
    if upper_type == "IPV4":
        return "IPV4"

    # Map of type spellings to normalized types, matched EXACTLY.
    #
    # ⚠ This was a SUBSTRING match, and a substring match on a type name silently
    # answers a question the user did not ask. `UINTEGER` contains "integer", so
    # `CAST(x AS UINTEGER)` returned a SIGNED INT64 — an unsigned cast quietly
    # became signed, with no error. The same trap caught any custom type name
    # ending in a mapped word. Nothing that reaches here needs a substring match:
    # `_extract_data_type` flattens the AST to a bare identifier token before we
    # see it (`TIMESTAMP(6)` -> "Timestamp", `ARRAY<VARCHAR>` -> "Array"), so the
    # parameterised forms the substring rule looked like it was for never arrive
    # carrying their parameters. The multi-word spellings sqlparser concatenates
    # are listed explicitly instead — there is no rule to infer them from.
    type_mappings = {
        "timestamp": "TIMESTAMP",
        "time": "TIME",
        "date": "DATE",
        "varchar": "VARCHAR",
        "decimal": "DECIMAL",
        # Only THREE implied aliases are accepted: INTEGER (INT64), FLOAT
        # (FLOAT64, handled above) and BOOLEAN (BOOL). Every other spelling must be
        # the canonical name or be rejected — so DOUBLE, DOUBLE PRECISION, BLOB and
        # STRUCT are gone from the dialect and now raise pointing at FLOAT64,
        # VARBINARY and NVARCHAR. The VALUES here are the engine's INTERNAL cast
        # target names, which are a separate vocabulary from the surface spellings
        # (casts.pyx translates VARBINARY to its own "BLOB" at the boundary) — so
        # removing a surface spelling never disturbs the dispatch below it.
        "integer": "INTEGER",
        "bool": "BOOLEAN",
        "boolean": "BOOLEAN",
        "array": "ARRAY",
        "vector": "VECTOR",
        "interval": "INTERVAL",
    }

    normalized = type_mappings.get(lower_type)
    if normalized is not None:
        return normalized

    # Check binary types separately
    if lower_type in ("varbinary", "binary", "raw"):
        return "VARBINARY"

    # Handle unsupported type aliases with helpful error messages
    # The suggestion now points at the EXACT width where one exists — TINYINT means
    # INT8 and REAL means FLOAT32 in this engine's vocabulary (the same mapping
    # `_SQL_NAME_ALIASES` uses to read a stored schema), so suggesting INTEGER or
    # DOUBLE for them would send the reader to a wider type than they asked for.
    type_suggestions = {
        ("STRING", "CHAR", "TEXT"): "VARCHAR",
        # NUMERIC is the standard's exact-numeric spelling — DECIMAL, not DOUBLE.
        # Suggesting DOUBLE sent the reader to a type that cannot hold what they
        # asked for.
        ("NUMERIC",): "DECIMAL",
        ("REAL",): "FLOAT32",
        ("TINYINT", "BYTE"): "INT8",
        ("SMALLINT",): "INT16",
        # INT64, never INTEGER. A suggestion must name the CANONICAL type — the
        # name `str(ColumnType)` renders and the catalog stores — so that what we
        # tell a user to type is the same name the engine will show them back.
        ("INT", "BIGINT"): "INT64",
        ("BIT",): "BOOL",
        # Accepted until the canonical-only ruling: these resolve to FLOAT64,
        # VARBINARY and NVARCHAR, and a spelling that is not the canonical name of
        # what you get is a second vocabulary waiting to drift from the first.
        ("DOUBLE", "DOUBLEPRECISION"): "FLOAT64",
        ("BLOB",): "VARBINARY",
        ("STRUCT",): "NVARCHAR",
    }

    rejected_alias_suggestion = {
        alias: suggestion for aliases, suggestion in type_suggestions.items() for alias in aliases
    }
    suggestion = rejected_alias_suggestion.get(upper_type)
    if suggestion is not None:
        raise SqlError(
            compose(
                f"{md_code(upper_type)} is not a type {md_syntax('CAST')} can produce",
                did_you_mean(suggestion),
            )
        )

    # Anything still unrecognized gets the same treatment a mistyped column or
    # function name gets: a typo detector, not intent inference. It answers "did
    # you fat-finger a name we have?", so `UINTEGER` -> INTEGER (one inserted
    # character) but `UBIGINT`/`USMALLINT`/`UTINYINT` get no suggestion — they are
    # a different type system's vocabulary, not a slip, and guessing which of our
    # widths they meant is inference this does not do.
    # The spellings the canonical-only ruling REMOVED stay in the typo candidate
    # pool: they were accepted names until that ruling, so a typo of one
    # ('DOUBEL') is still a slip of a name a user plausibly knows, and it gets
    # the same canonical suggestion the exact spelling gets (FLOAT64) — mapped
    # through, never surfaced as the thing to type. The always-rejected aliases
    # (BIGINT, TINYINT, ...) are deliberately NOT in the pool: putting them there
    # made UBIGINT a near-miss of BIGINT, handing a suggestion to exactly the
    # foreign vocabulary the paragraph above refuses to guess about.
    formerly_accepted = ("DOUBLE", "DOUBLEPRECISION", "BLOB", "STRUCT")
    suggestion = suggest_alternative(
        upper_type,
        _CAST_TARGET_NAMES + tuple(_IMPLIED_ALIAS_CANONICAL) + formerly_accepted,
    )
    if suggestion is not None:
        suggestion = _IMPLIED_ALIAS_CANONICAL.get(
            suggestion, rejected_alias_suggestion.get(suggestion, suggestion)
        )
        raise SqlError(
            compose(
                f"{md_code(upper_type)} is not a type {md_syntax('CAST')} can produce",
                did_you_mean(suggestion),
            )
        )

    # Report the name UPPERCASED, not sqlparser's internal spelling: the token we
    # are handed is its variant name ("UBigInt", "Datetime"), which is not what
    # the user typed and reads like the engine mangled their SQL.
    raise SqlError(
        compose(
            f"{md_code(upper_type)} is not a type {md_syntax('CAST')} can produce",
            f"{md_syntax('SHOW COLUMNS FROM')} on any table lists the type names in use",
        )
    )


# The internal temporal forms the SQL rewriter produces for TIMESTAMP[unit], as
# the TimestampUnit a declared column type resolves to. `_TIMESTAMP_DAYS` is
# absent deliberately: TimestampUnit has no day resolution, so a column cannot be
# DECLARED at it (see the raise in column_type_from_ast). It remains valid on a
# CAST, where it is a scaling instruction and the result is canonical
# microseconds — a different thing from a storage width.
def _timestamp_unit_forms():
    from draken.draken_native import TimestampUnit as _TU

    return {
        "_TIMESTAMP_NS": _TU.NANOSECONDS,
        "_TIMESTAMP_MS": _TU.MILLISECONDS,
        "_TIMESTAMP_S": _TU.SECONDS,
        "_TIMESTAMP_US": _TU.MICROSECONDS,
    }


def column_type_from_ast(branch) -> "ColumnType":
    """Resolve a DECLARED column type (its AST `data_type` node) to a ColumnType.

    For anywhere a type is WRITTEN rather than cast to — CREATE TABLE today. It
    deliberately runs the same two steps a cast target does, `_extract_data_type`
    then `_normalize_cast_type`, so a type name means the same thing in a DDL
    column as it does in a CAST. DDL previously carried its own hand-written
    sqlparser-key → name map, which is how it ended up rejecting NVARCHAR,
    VARBINARY, DECIMAL, TIME, INTERVAL, IPV4, TIMESTAMP[unit] and every exact
    integer width, while quietly widening TINYINT and SMALLINT to INTEGER and
    REAL to DOUBLE — a second vocabulary, free to drift, and drifted (§14: there
    is ONE type object, from schema through AST to kernels).

    `branch` is the AST node holding a "data_type" key (a column definition).
    """
    from opteryx.planner import build_literal_node
    from opteryx.types.logical_type import TIMESTAMP as _CT_TIMESTAMP_F
    from opteryx.types.logical_type import parse_column_type, try_parse_column_type

    params: list = []
    raw_name = _extract_data_type(branch["data_type"], branch, params, build_literal_node)
    try:
        normalized = _normalize_cast_type(raw_name)
    except SqlError:
        # The cast surface deliberately REFUSES alias spellings (TINYINT, BIGINT,
        # TEXT, REAL) and points at the exact name instead. A DECLARED type is a
        # different question: it says what the catalog will STORE, so it accepts
        # everything a stored schema may say — `_SQL_NAME_ALIASES`, the same table
        # the schema reader resolves against. Rejecting BIGINT or TEXT in DDL
        # because a CAST rejects them would break working schemas for a rule that
        # is about cast targets.
        #
        # This is resolution ORDER, not a second vocabulary: canonical names
        # first, persisted-schema aliases second, and both tables already exist
        # and are already authoritative for their own surface.
        _aliased = try_parse_column_type(str(raw_name).upper())
        if _aliased is not None:
            return _aliased
        raise

    # TIMESTAMP[unit] — the unit is part of the type, not a parameter of it.
    _units = _timestamp_unit_forms()
    if normalized in _units:
        return _CT_TIMESTAMP_F(_units[normalized])
    if normalized == "_TIMESTAMP_DAYS":
        raise UnsupportedSyntaxError(
            "TIMESTAMP[d] cannot be a declared column type — there is no day "
            "resolution to store it at. Declare DATE, or TIMESTAMP[s]."
        )

    # Parameterized forms. `_extract_data_type` puts the parenthesised arguments
    # in `params` (the same channel the cast path reads), and parse_column_type
    # already understands the canonical string spelling of each — so rebuild that
    # spelling rather than growing a third construction path.
    if normalized == "DECIMAL":
        if len(params) < 2:
            raise UnsupportedSyntaxError("DECIMAL requires a precision and scale, e.g. DECIMAL(10, 2)")
        return parse_column_type(f"DECIMAL({int(params[0].value)}, {int(params[1].value)})")
    if normalized == "VECTOR":
        if not params:
            raise UnsupportedSyntaxError("VECTOR requires a dimension, e.g. VECTOR(384)")
        return parse_column_type(f"VECTOR({int(params[0].value)})")
    if normalized == "ARRAY":
        return parse_column_type(f"ARRAY<{_array_element_type(params)}>")

    return parse_column_type(normalized)


def _array_element_type(params):
    """The ARRAY<element> element type, read out of the cast's parameters channel.

    `_extract_data_type` flattens a dict-shaped AST data_type to its top-level key, so
    `ARRAY<VARCHAR>` would arrive as the bare name "ARRAY". The element type survives only
    because it is copied into the cast's parameters — the same channel VECTOR's width and
    DECIMAL's precision/scale use. This is the single place the fold path reads it back;
    the binder reads the same parameters for the runtime-CAST path.
    """
    from opteryx.types.logical_type import parse_column_type

    if not params or params[0].node_type != NodeType.LITERAL or params[0].value is None:
        raise UnsupportedSyntaxError(
            "**CAST** to ARRAY requires an element type, e.g. **CAST**(['a'] AS ARRAY<VARCHAR>)."
        )
    return parse_column_type(str(params[0].value).upper())


def _cast_literal_value(literal_node, target_type: str, kind: str, alias, params=()):
    """Cast a literal value at compile time.

    `params` carries the TYPE's parenthesized arguments (VECTOR's width today). Folding
    otherwise drops them, which for a parameterized target means folding to a constant
    that has lost its declared type — the reason NVARCHAR/DECIMAL are routed to the
    runtime CAST node instead.
    """
    from opteryx.expression.casts import parse_timestamp_value
    from opteryx.types.timestamps._datetime_conversion import (
        date_to_int64_days,
        timestamp_to_int64_us,
    )

    _node_cat = literal_node.type.category if literal_node.type is not None else None

    # NULL values stay NULL, but must carry the target type. An untyped NULL
    # literal loses the physical tag string kernels dispatch on: CAST(NULL AS
    # VARCHAR) folded to an untyped NULL is later materialised as a DRAKEN_NULL
    # vector (data==NULL, validity==NULL ⇒ all-valid) and read as a garbage
    # string arena by concat/LIKE, emitting non-null junk. Stamp the resolved
    # string-family ColumnType so the constant materialises as a typed null
    # string. NVARCHAR/DECIMAL never reach here (routed to the runtime CAST
    # node). NUMERIC targets are stamped too — see the branch below for why the
    # "kernels short-circuit on DRAKEN_NULL" reasoning did not hold for them.
    if _node_cat == LogicalCategory.NULL:
        if target_type.replace("TRY_", "") == "VARCHAR":
            return Node(NodeType.LITERAL, value=None, type=_CT_VARCHAR, alias=alias)
        if target_type.replace("TRY_", "") == "ARRAY":
            # CAST(NULL AS ARRAY<E>) is NULL — but a *typed* NULL. The untyped NULL would
            # drop the declared element type, leaving a UNION arm or a projection with
            # nothing to recover ARRAY<E> from. The native kernel never sees this: it
            # takes VARIANT/VARCHAR sources only, so a NULL source has no runtime path
            # and folding is the only way the shape can run.
            from opteryx.types.logical_type import ARRAY as _CT_ARRAY_OF

            return Node(
                NodeType.LITERAL,
                value=None,
                type=_CT_ARRAY_OF(_array_element_type(params)),
                alias=alias,
            )
        # Numeric targets: stamp the width. The untyped NULL was justified on the
        # grounds that "numeric kernels short-circuit on the DRAKEN_NULL tag" —
        # the ARITHMETIC kernels do not. `10 / CAST(NULL AS FLOAT)` reached the
        # binary op as INT64 / DRAKEN_NULL and died with "cross-type vector
        # arithmetic not supported", and `id + CAST(NULL AS INTEGER)` was refused
        # at the compiler gate for the same reason. A typed null constant is a
        # normal operand of its own type, so both become ordinary promotions.
        #
        # BOOLEAN is stamped for the same reason, and more sharply: BOOLEAN is where
        # a CONDITION comes from, and every path that consumes one type-checks it
        # rather than short-circuiting. `CAST(NULL AS BOOLEAN)` as a sort key was
        # refused ("ORDER BY on *null* (type `NULL`)"), as an IIF or FILTER
        # condition it was refused ("IIF arg1 (NULL): expected BOOLEAN"), as a CASE
        # condition it raised a raw Cython `TypeError: Argument 'bv' has incorrect
        # type`, `MAX(CAST(NULL AS BOOLEAN))` was refused over "type `NULL`", and
        # `CAST(NULL AS BOOLEAN) = TRUE` DECLARED NULL for a comparison that returns
        # BOOLEAN. Every one of those works for a BOOLEAN COLUMN, which is the test
        # of whether a typed null is behaving like its type.
        #
        # Consequence, and intended: `CONCAT(CAST(NULL AS BOOLEAN), 'x')` now fails
        # like `CONCAT(b_value, 'x')` already does ("expected VARCHAR") instead of
        # quietly returning VARCHAR nulls. The untyped null was passing a
        # type-invalid query, not supporting one.
        #
        # Pairs with the BOOL arm in `_materialise_constant_literal`
        # (compiled_expression.pyx): without it the stamp here would be undone at
        # materialization, which is where every BOOL null became DRAKEN_NULL again.
        #
        # `logical is None` deliberately excludes IPV4, whose category is INTEGER
        # (so ordering/grouping/joins run on the raw uint32) but which carries a
        # descriptor — stamping that onto a folded null is the attach-vs-skip
        # question that path answers separately, and this is not the place to
        # re-answer it. VARBINARY and temporal targets are likewise untouched: a
        # VARBINARY null reaching the string-concat closure would be stringified
        # (VARBINARY is not in its string allow-list). Those two, plus VECTOR, are
        # the same shape as the BOOLEAN gap above and are still open.
        from opteryx.types.logical_type import try_parse_column_type as _try_parse_ct

        _null_ct = _try_parse_ct(target_type.replace("TRY_", ""))
        if (
            _null_ct is not None
            and _null_ct.logical is None
            and _null_ct.category
            in (LogicalCategory.INTEGER, LogicalCategory.FLOAT, LogicalCategory.BOOLEAN)
        ):
            return Node(NodeType.LITERAL, value=None, type=_null_ct, alias=alias)
        return Node(NodeType.LITERAL, type=_CT_NULL, alias=alias)

    # Strip TRY_ prefix for type lookup
    base_type = target_type.replace("TRY_", "")

    if base_type == "ARRAY":
        # CAST(<array literal> AS ARRAY<E>): folded for the same reason VECTOR is — the
        # native kernel reads its elements from the column owner's CHILD vector, which
        # only a real column has. An array literal has no child, so the kernel cannot see
        # its own input; it does not refuse, it silently returns empty arrays. Folding is
        # not an optimization here; it is the only way this shape can run. `cast()` routes
        # every other literal source (VARCHAR holding JSON text) to the native kernel.
        #
        # This is a RETYPE, not a conversion. The declared element type must already match
        # the literal's own, because the native kernel's rule 3 — an element type mismatch
        # fails, never implicit stringification, never implicit parsing, never truncation —
        # has to hold identically here, or the folded and native paths would answer the
        # same question two different ways. Rule 4 likewise: plain `::` raises, TRY_ nulls.
        from opteryx.types.logical_type import ARRAY as _CT_ARRAY_OF

        _element_ct = _array_element_type(params)
        _vals = literal_node.value
        if not isinstance(_vals, (list, tuple)):
            raise UnsupportedSyntaxError("**CAST** to ARRAY expects an array literal.")
        _source_element = literal_node.type.element if literal_node.type is not None else None
        if _source_element != _element_ct:
            if target_type.startswith("TRY_"):
                return Node(
                    NodeType.LITERAL, value=None, type=_CT_ARRAY_OF(_element_ct), alias=alias
                )
            raise UnsupportedSyntaxError(
                f"**CAST** ARRAY<{_source_element}> → ARRAY<{_element_ct}>: element does not match "
                "the declared element type. An array literal is retyped, never converted "
                "element-by-element — cast the elements at the source, or use **TRY_CAST**."
            )
        return Node(
            NodeType.LITERAL, value=list(_vals), type=_CT_ARRAY_OF(_element_ct), alias=alias
        )

    if base_type == "VECTOR":
        # CAST(<array literal> AS VECTOR(n)): the values are known here, so fold to a
        # VECTOR-typed literal rather than emit a runtime CAST. The runtime cast reads
        # its elements from the column owner's CHILD vector, which only a real column
        # has — a literal array has no child, so the kernel could not see its own input.
        # Folding is not an optimization here; it is the only way this shape can run.
        from opteryx.types.logical_type import VECTOR as _CT_VECTOR

        if not params or params[0].node_type != NodeType.LITERAL:
            raise UnsupportedSyntaxError(
                "**CAST** to VECTOR requires a dimension, e.g. **CAST**([1.0, 0.0] AS VECTOR(2))."
            )
        _dims = int(params[0].value)
        _vals = literal_node.value
        if not isinstance(_vals, (list, tuple)):
            raise UnsupportedSyntaxError("**CAST** to VECTOR expects an array literal.")
        if len(_vals) != _dims:
            raise UnsupportedSyntaxError(
                f"**CAST** to VECTOR({_dims}) got a {len(_vals)}-element array literal."
            )
        _floats = []
        for _v in _vals:
            if _v is None or isinstance(_v, bool) or not isinstance(_v, (int, float)):
                raise UnsupportedSyntaxError(
                    "**CAST** to VECTOR expects an array literal of numbers with no nulls."
                )
            _floats.append(float(_v))
        return Node(NodeType.LITERAL, value=_floats, type=_CT_VECTOR(_dims), alias=alias)

    # Extract unit from internal temporal type forms
    unit = None
    if base_type == "_TIMESTAMP_NS":
        unit = "ns"
        base_type = "TIMESTAMP"
    elif base_type == "_TIMESTAMP_MS":
        unit = "ms"
        base_type = "TIMESTAMP"
    elif base_type == "_TIMESTAMP_S":
        unit = "s"
        base_type = "TIMESTAMP"
    elif base_type == "_TIMESTAMP_US":
        unit = "us"
        base_type = "TIMESTAMP"
    elif base_type == "_TIMESTAMP_DAYS":
        unit = "days"
        base_type = "TIMESTAMP"

    # Special case: VARBINARY maps to BLOB in Sql types
    if base_type == "VARBINARY":
        sql_type = _CT_VARBINARY
    elif base_type == "DATE" and _node_cat in (LogicalCategory.INTEGER, LogicalCategory.DATE):
        # An integer DATE literal is days-since-epoch — the same reading the column
        # kernel (draken_cast_integer_to_date32 / _uint_to_date32) uses, so the
        # folded and the executed form of CAST(<int> AS DATE) cannot disagree.
        # A day count with no representable date raises out of timedelta; this
        # branch returns BEFORE the try/except at the end of the function that
        # applies the two cast dispositions, so it has to apply them itself —
        # otherwise a plain CAST surfaced a bare OverflowError instead of the
        # SqlError every other target gives, and TRY_CAST raised instead of
        # yielding the NULL that is its entire contract.
        try:
            value = date_to_int64_days(
                _EPOCH_DATE + datetime.timedelta(days=int(literal_node.value))
            )
        except Exception as e:
            if kind in {"TryCast", "SafeCast"}:
                return Node(NodeType.LITERAL, value=None, type=_CT_NULL, alias=alias)
            raise SqlError(
                f"Error casting value '{literal_node.value}' to type '{base_type}': {e}"
            )
        return Node(NodeType.LITERAL, type=_CT_DATE, value=value, alias=alias)
    elif base_type == "DATE" and _node_cat == LogicalCategory.TIMESTAMP:
        # TIMESTAMP → DATE: truncate the time component, the same floor-divide by
        # the source unit's ticks-per-day that draken_cast_timestamp_to_date32
        # (draken/ops/kernels/cast_temporal.cpp) performs on a column. Without
        # this the literal fell through to the generic parser below, which was
        # handed the raw epoch tick count and rejected it — `CAST('2020-01-01
        # 00:00:00'::TIMESTAMP AS DATE)` raised "Cannot parse 1577836800000000
        # as date" while the identical cast on a COLUMN ran. Floor, not integer
        # division: a pre-epoch timestamp must truncate toward -inf so the two
        # forms agree there too.
        _ticks_per_day = {
            TimestampUnit.SECONDS: 86_400,
            TimestampUnit.MILLISECONDS: 86_400_000,
            TimestampUnit.MICROSECONDS: 86_400_000_000,
            TimestampUnit.NANOSECONDS: 86_400_000_000_000,
        }
        _logical = literal_node.type.logical if literal_node.type is not None else None
        _unit = _logical.unit if _logical is not None else TimestampUnit.MICROSECONDS
        return Node(
            NodeType.LITERAL,
            type=_CT_DATE,
            value=int(literal_node.value) // _ticks_per_day[_unit],
            alias=alias,
        )
    # Special case: INTEGER to TIMESTAMP conversion
    elif base_type == "TIMESTAMP" and (
        _node_cat in (LogicalCategory.INTEGER, LogicalCategory.DATE)
        or isinstance(literal_node.value, int)
    ):
        # Require explicit unit for INTEGER to TIMESTAMP conversion
        if _node_cat == LogicalCategory.INTEGER and unit is None:
            raise UnsupportedSyntaxError(
                "Ambiguous cast: INTEGER → TIMESTAMP requires a unit. "
                "Use `expr::TIMESTAMP[ms]`, `expr::TIMESTAMP[s]`, or `expr::TIMESTAMP[us]`."
            )

        int_value = int(literal_node.value)
        # If unit was specified, use it; otherwise use default behavior for dates
        if unit:
            value = timestamp_to_int64_us(parse_timestamp_value(int_value, unit=unit))
        elif _node_cat == LogicalCategory.DATE or abs(int_value) < 100_000:
            value = timestamp_to_int64_us(
                (_EPOCH_DT + datetime.timedelta(days=int_value)).replace(tzinfo=None)
            )
        else:
            value = timestamp_to_int64_us(parse_timestamp_value(int_value))
        return Node(NodeType.LITERAL, type=_CT_TIMESTAMP(), value=value, alias=alias)
    else:
        from opteryx.types.logical_type import parse_column_type

        sql_type = parse_column_type(base_type)  # ColumnType

    # Temporal → VARCHAR: format as ISO string rather than calling str() on the raw int.
    # VARBINARY, not BLOB: the surface spelling BLOB was removed with the
    # canonical-only ruling, so the normalized name that arrives here is
    # VARBINARY. Gating on the dead spelling skipped this whole rendering block —
    # CAST(ip AS VARBINARY) on a literal folded to b'3232235777' where the column
    # kernel yields b'192.168.1.1'.
    if base_type in ("VARCHAR", "VARBINARY"):
        # IPV4 → string family renders dotted-decimal, matching the column path's
        # draken_cast_ipv4_to_string. The DESCRIPTOR is the discriminant, never the
        # category: IPv4's category is deliberately INTEGER (so ordering, grouping
        # and joins run on the raw uint32), so left to fall through this folds to
        # str(uint32) — '3232235777' where a column yields '192.168.1.1'. That is
        # the literal value/type-tag divergence class of bug: a wrong row, not an
        # error.
        #
        # Rendering routes through draken.ipv4_format → draken::ipv4::format, the
        # SAME writer the kernel (and to_pylist, and the text writers) use, so a
        # folded literal and a scanned column cannot print an address differently.
        # The rendered text then goes through the same parser_for the generic path
        # below uses, so VARCHAR keeps the str and BLOB gets its UTF-8 bytes from
        # one rendering rule rather than two.
        #
        # Sits OUTSIDE the try block below: a NULL-valued IPv4 literal is not an
        # int and must fall through to the generic path (which yields NULL), and
        # TRY_/SAFE_ must not be able to turn a rendering into a NULL.
        _lit_lt = literal_node.type.logical if literal_node.type is not None else None
        if (
            _lit_lt is not None
            and _lit_lt.kind == LogicalKind.IPV4
            and isinstance(literal_node.value, int)
        ):
            from opteryx.types.scalars.value_parsing import parser_for

            return Node(
                NodeType.LITERAL,
                type=sql_type,
                value=parser_for(sql_type.category)(ipv4_format(literal_node.value)),
                alias=alias,
            )
        if _node_cat == LogicalCategory.TIMESTAMP and isinstance(literal_node.value, int):
            from opteryx.expression.formatter import _format_timestamp_micros

            parsed_value = _format_timestamp_micros(literal_node.value)
            return Node(NodeType.LITERAL, type=sql_type, value=parsed_value, alias=alias)
        if _node_cat == LogicalCategory.DATE and isinstance(literal_node.value, int):
            from opteryx.expression.formatter import _format_date_days

            parsed_value = _format_date_days(literal_node.value)
            return Node(NodeType.LITERAL, type=sql_type, value=parsed_value, alias=alias)
        if _node_cat == LogicalCategory.TIME and isinstance(literal_node.value, datetime.time):
            # Matches draken_cast_time_to_string's fixed "HH:MM:SS.ffffff" format
            # (the runtime kernel) rather than Python's str(time), which omits
            # the fractional part when microsecond == 0.
            t = literal_node.value
            parsed_value = f"{t.hour:02d}:{t.minute:02d}:{t.second:02d}.{t.microsecond:06d}"
            return Node(NodeType.LITERAL, type=sql_type, value=parsed_value, alias=alias)
        if _node_cat == LogicalCategory.INTERVAL and isinstance(literal_node.value, tuple):
            from opteryx.expression.formatter import _format_interval_iso8601

            parsed_value = _format_interval_iso8601(literal_node.value)
            return Node(NodeType.LITERAL, type=sql_type, value=parsed_value, alias=alias)

    # Attempt to parse and cast the literal value
    try:
        from opteryx.types.scalars.value_parsing import parser_for

        # IPv4 is UINT32 refined by a LogicalKind.IPV4 descriptor, and its category
        # is deliberately INTEGER (ordering, grouping, joins and comparison all run
        # on the raw uint32). So the DESCRIPTOR, never the category, is the
        # discriminant here — parser_for(category) would hand '192.168.1.1' to
        # int(). Parsing routes through draken::ipv4::parse, the same strict parser
        # draken_cast_string_to_ipv4 runs on a column, so a folded literal and a
        # scanned column can never disagree about what '010.1' means.
        if (
            isinstance(sql_type, ColumnType)
            and sql_type.logical is not None
            and sql_type.logical.kind == LogicalKind.IPV4
            and isinstance(literal_node.value, (str, bytes))
        ):
            # Value AND type together: an int tagged VARCHAR, or dotted-decimal
            # text tagged IPV4, is the literal value/type-tag divergence that
            # silently produces wrong rows downstream.
            return Node(
                NodeType.LITERAL,
                type=sql_type,
                value=ipv4_parse(literal_node.value),
                alias=alias,
            )

        _sql_type_lc = sql_type.category if isinstance(sql_type, ColumnType) else sql_type
        # parser_for(), not parse_value(): parse_value() silently swallows parse
        # failures and returns the value unchanged (fine for its other callers,
        # which reconcile already-compatible literal branches) — but CAST on a
        # literal must fail loud on bad input, matching the runtime CAST path.
        parsed_value = (
            None if literal_node.value is None else parser_for(_sql_type_lc)(literal_node.value)
        )
        if isinstance(parsed_value, datetime.datetime):
            parsed_value = timestamp_to_int64_us(parsed_value)
        elif isinstance(parsed_value, datetime.date):
            parsed_value = date_to_int64_days(parsed_value)
        # A literal that does not FIT the declared width is a failed cast, and it
        # has to be caught HERE: parsing `300` succeeds, so without this the value
        # sailed through typed INT8 and only blew up later inside the vector
        # constructor — as a bare OverflowError for plain CAST, and for TRY_CAST
        # as an error at all, when the whole point of TRY_CAST is a NULL. Raising
        # inside this try hands both dispositions to the handler below, which
        # already knows which one applies.
        _bounds = integer_bounds(sql_type)
        if _bounds is not None and isinstance(parsed_value, int) and not isinstance(
            parsed_value, bool
        ):
            if parsed_value < _bounds[0] or parsed_value > _bounds[1]:
                raise ValueError(
                    f"value {parsed_value} is out of range for {sql_type}"
                )
        return Node(NodeType.LITERAL, type=sql_type, value=parsed_value, alias=alias)
    except Exception as e:
        # For TRY_CAST/SAFE_CAST, return NULL on failure
        if kind in {"TryCast", "SafeCast"}:
            return Node(NodeType.LITERAL, type=_CT_NULL, alias=alias)
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
        # The whole dotted name, `t.col` and not just `col` - an error about it names
        # the qualified form, so that is what the caret should cover.
        span=_span_of(branch[0], branch[-1]),
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


def scalar_subquery(branch, alias: Optional[List[str]] = None, key=None):
    """
    Scalar subquery used as an expression value, e.g.:
        WHERE col = (SELECT MAX(x) FROM T WHERE T.k = outer.k)

    The inner plan is embedded as a NodeType.SUBQUERY expression node. It stays
    a subquery through the plan rewriter and is bound in place (binder's
    bind_correlated_subquery); DecorrelateSubqueryStrategy in the OPTIMIZER then
    removes it, using the binder's resolution of each name to tell a correlated
    reference from a local one.
    """
    from opteryx.planner.logical_planner.logical_planner import plan_query

    subquery_plan = plan_query(branch)
    exit_node = subquery_plan.get_exit_points()[0]
    subquery_plan.remove_node(exit_node, heal=True)

    return Node(NodeType.SUBQUERY, value=subquery_plan, alias=alias)


def exists(branch, alias: Optional[List[str]] = None, key=None):
    from opteryx.planner.logical_planner.logical_planner import plan_query

    ast = {"Query": branch["subquery"]}
    subquery_plan = plan_query(ast)
    exit_node = subquery_plan.get_exit_points()[0]
    subquery_plan.remove_node(exit_node, heal=True)

    sub_query = Node(NodeType.SUBQUERY, value=subquery_plan)
    node = Node(NodeType.UNARY_OPERATOR, value="Exists", alias=alias)
    node.parameters = [sub_query]
    node.negated = branch["negated"]
    return node


class GroupingConstruct:
    """One `GROUP BY` grouping construct — `ROLLUP (...)`, `CUBE (...)`, `GROUPING SETS (...)`.

    NOT an expression, and deliberately not a `Node`: it is a clause construct that
    describes WHICH SETS of keys to group by, and it only ever appears as a member of
    the GROUP BY list. `expand_grouping_elements` in the logical planner is its one
    consumer, which turns it into the flat key list plus the grouping-set index tuples
    the aggregate carries. Anything else that receives one has a bug — every other
    consumer of a GROUP BY member expects an expression and will fail on the attribute
    access, loudly, at the point of the mistake.

    `elements` is a list of grouping ELEMENTS, each itself a list of expressions, which
    is the shape sqlparser produces (`Vec<Vec<Expr>>`). The nesting is load-bearing:
    `ROLLUP((a, b), c)` has two elements, the first a composite of two columns, and
    rolls up to `(a,b,c)`, `(a,b)`, `()` — three sets, not four.
    """

    __slots__ = ("kind", "elements")

    def __init__(self, kind: str, elements: List[List]):
        self.kind = kind
        self.elements = elements

    def grouping_sets(self) -> List[List]:
        """The ordered list of grouping sets this construct denotes, each a list of
        expressions. Coarsest-last, matching the standard's presentation order."""
        if self.kind == "ROLLUP":
            # (e1..en), (e1..en-1), ..., (e1), ()
            return [
                [expr for element in self.elements[:depth] for expr in element]
                for depth in range(len(self.elements), -1, -1)
            ]
        raise SqlError(f"Unhandled grouping construct `{self.kind}`")


def rollup(branch, alias: Optional[List[str]] = None, key=None):
    return GroupingConstruct("ROLLUP", [[build(part) for part in element] for element in branch])


def unsupported_grouping_construct(branch, alias: Optional[List[str]] = None, key=None):
    """`CUBE` and `GROUPING SETS` parse — they are in the same family as `ROLLUP` and the
    dialect enables the whole production — but nothing lowers them yet. Refuse them here,
    named, rather than let a half-understood construct reach the aggregate: the internal
    representation (an explicit list of grouping sets) already accommodates both, so this
    is a lowering gap, not a design one."""
    spelling = {"Cube": "CUBE", "GroupingSets": "GROUPING SETS"}[key]
    raise UnsupportedSyntaxError(
        f"**{spelling}** is not implemented. **ROLLUP** is the supported "
        f"**GROUP BY** grouping construct."
    )


def expressions(branch, alias: Optional[List[str]] = None, key=None):
    return [build(part) for part in branch]


def extract(branch, alias: Optional[List[str]] = None, key=None):
    # EXTRACT(part FROM timestamp)
    datepart_value = branch["field"]
    if isinstance(datepart_value, dict):
        datepart_value = list(datepart_value)[0]
    datepart = Node(NodeType.LITERAL, type=_CT_VARCHAR, value=datepart_value)
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
    # The whole dotted name, first part to last - that is what the error names.
    name_span = _span_of(branch["name"][0].get("Identifier", {}), branch["name"][-1].get("Identifier", {}))

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

    if func in WINDOW_FUNCTIONS:
        # Window functions. Parsed as AGGREGATOR so the window-function detection
        # in the logical planner picks them up; they are only valid with an
        # OVER (...) clause (enforced there).
        node_type = NodeType.AGGREGATOR
        if filter_condition is not None:
            raise UnsupportedSyntaxError(
                f"Filters are not supported with window function '{func}'. Filter in a subquery first, then apply the window to its result."
            )
        if func in NAVIGATION_FUNCTIONS:
            # LAG(expr[, offset]) / LEAD(expr[, offset]). The 3-argument default
            # form and null treatment are refused, never silently ignored.
            if null_treatment is not None:
                raise UnsupportedSyntaxError(
                    f"**IGNORE NULLS** / **RESPECT NULLS** are not supported with {func}()."
                )
            if len(args) == 3:
                raise UnsupportedSyntaxError(
                    f"{func}(expr, offset, default) — the 3-argument form is not supported. "
                    f"Wrap the result instead: `COALESCE({func}(expr, offset), default)`."
                )
            if len(args) not in (1, 2):
                raise UnsupportedSyntaxError(
                    f"{func}() takes 1 or 2 arguments: {func}(expr) or {func}(expr, offset)."
                )
            if len(args) == 2:
                _offset = args[1]
                if (
                    _offset.node_type != NodeType.LITERAL
                    or not isinstance(_offset.value, int)
                    or isinstance(_offset.value, bool)
                    or _offset.value < 0
                ):
                    raise UnsupportedSyntaxError(
                        f"{func}()'s offset must be a non-negative integer literal."
                    )
        elif args:
            raise UnsupportedSyntaxError(
                f"{func}() takes no arguments — the window's **ORDER BY** is its input."
            )
    elif func == "GROUPING":
        # GROUPING(col) — GROUP BY ROLLUP's companion: 1 when `col` was rolled up
        # (NULL) to produce this row's subtotal, 0 when it is a real per-group
        # value. Parsed as AGGREGATOR, the same way window functions are above, so
        # the aggregate-hoist sweep in the logical planner pulls it up to the
        # AggregateAndGroup node alongside SUM/COUNT/etc — that hoist is what lets
        # it appear in ORDER BY and a window's PARTITION BY, where it is otherwise
        # bound as an ordinary column reference. It is NOT in AGGREGATORS
        # (opteryx/operators/aggregate/helpers.py): it is not a per-group
        # reduction — its value is a lookup against the grouping set that produced
        # the row — so it is deliberately routed around `_parse_aggregates` /
        # `_AGG_FNS` in the physical compiler instead of through them. Requiring
        # ROLLUP/CUBE/GROUPING SETS in scope, and that the argument is one of this
        # query's GROUP BY keys, is validated at bind time
        # (opteryx/planner/binder/aggregate.py), once the GROUP BY keys are known.
        node_type = NodeType.AGGREGATOR
        if filter_condition is not None:
            raise UnsupportedSyntaxError("Filters are not supported with GROUPING().")
        if len(args) != 1:
            raise UnsupportedSyntaxError(
                "GROUPING() takes exactly one argument: the GROUP BY column to test."
            )
    elif _is_function(func):
        node_type = NodeType.FUNCTION
        if filter_condition is not None:
            raise UnsupportedSyntaxError("Filters are not supported with function calls. Move the condition into a **WHERE** clause.")
    elif is_aggregator(func):
        node_type = NodeType.AGGREGATOR
        if filter_condition is not None:
            if func not in _NULL_IGNORING_AGGREGATES:
                raise UnsupportedSyntaxError(
                    f"Filters are not supported with aggregate function '{func}', because it does "
                    f"not ignore NULL input and so cannot be expressed as a filtered argument. "
                    f"Move the condition into a **WHERE** clause, or filter the grouped result "
                    f"with **HAVING**."
                )
            filter_condition = build(filter_condition)
    else:  # pragma: no cover
        # Rewrite type-names used as cast functions: VARCHAR(x) → CAST(x AS VARCHAR)
        # The VALUE is what we tell the user to type, so it must be the CANONICAL
        # name — this advice is executed verbatim. It used to say `::DOUBLE`,
        # `::BLOB` and `::INTEGER`; the first two are no longer accepted at all,
        # which would have made the error message itself a dead end.
        _TYPE_CAST_NAMES = {
            "VARCHAR": "VARCHAR",
            "INT": "INT64",
            "INT64": "INT64",
            "INTEGER": "INT64",
            "DOUBLE": "FLOAT64",
            "TIMESTAMP": "TIMESTAMP",
            "DATE": "DATE",
            "BOOLEAN": "BOOL",
            "BLOB": "VARBINARY",
            "VARBINARY": "VARBINARY",
            "FLOAT": "FLOAT64",
            "TIME": "TIME",
        }
        if func in _TYPE_CAST_NAMES and len(args) == 1:
            from opteryx.exceptions import md_code, md_syntax

            raise UnsupportedSyntaxError(
                compose(
                    f"{md_syntax(func)} is a type name, not a function, so "
                    f"{md_code(f'{func}({args[0].value})')} is not a cast",
                    f"Write the cast as "
                    f"{md_code(f'{args[0].value}::{_TYPE_CAST_NAMES[func]}')}",
                )
            )
        raise _unknown_function(func, name_span)

    # rewrite COUNT_DISTINCT() to COUNT(DISTINCT)
    if func == "COUNT_DISTINCT":
        func = "COUNT"
        duplicate_treatment = "Distinct"

    # AGG(x) FILTER (WHERE p) -> AGG(IIF(p, x, NULL))
    #
    # FILTER keeps only the rows where `p` is TRUE, and every aggregate reaching
    # here IGNORES NULL input (_NULL_IGNORING_AGGREGATES, verified against the
    # engine rather than assumed), so substituting the argument is exact:
    # a row `p` rejects contributes NULL, which the aggregate then skips.
    #
    # IIF is the right lowering rather than a convenience. FILTER admits a row
    # only when `p` is TRUE — UNKNOWN excludes, exactly like FALSE — and
    # `vector_iif` already treats an UNKNOWN condition as false, so the two agree
    # on three-valued logic without a special case. Placed after the
    # COUNT_DISTINCT rewrite so `COUNT_DISTINCT(x) FILTER (...)` and
    # `COUNT(DISTINCT x) FILTER (...)` lower identically, and DISTINCT then
    # applies to the IIF, which is what the standard requires.
    #
    # Until this existed the condition was parsed, bound, attached to the node as
    # `condition` — and read by nothing, so `COUNT(*) FILTER (WHERE id > 6)`
    # silently answered COUNT(*).
    if filter_condition is not None:
        original_argument = args[0]
        filtered_source = original_argument
        if filtered_source.node_type == NodeType.WILDCARD:
            # COUNT(*) counts ROWS, so there is no argument to filter; count a
            # constant instead and let the NULL branch remove the rejected rows.
            filtered_source = Node(NodeType.LITERAL, type=_CT_INT64, value=1)
        args = [
            Node(
                node_type=NodeType.FUNCTION,
                value="IIF",
                parameters=[
                    filter_condition,
                    filtered_source,
                    Node(NodeType.LITERAL, type=_CT_NULL, value=None),
                ],
            )
        ]
        # Name the output column after what the caller WROTE. Without this the
        # lowering renames the result to `COUNT(IIF(...))`, which leaks an
        # internal rewrite into the caller's schema. Spelled out rather than taken
        # from `format_expression`, which renders an aggregate's FILTER clause as
        # if it were not there (see logical_planner_rewriter._aggregate_key).
        alias = alias or _filtered_aggregate_spelling(
            func, original_argument, duplicate_treatment, filter_condition
        )
        filter_condition = None

    # COALESCE(a) → a. The catalog declares COALESCE's minimum arity as 1 and the
    # binder accepted one argument, but draken_coalesce requires two and raised a
    # raw `ValueError: draken_coalesce: expected at least 2 arguments` at execution.
    # The first non-null of a single argument IS that argument, so the one-argument
    # form folds exactly — which makes the catalog's claim true rather than
    # retreating from it, and matches what Postgres and DuckDB accept.
    if func == "COALESCE" and len(args) == 1:
        folded = args[0].copy()
        # As in the NULLIF fold below: without an explicit alias the folded node
        # would name the output column after itself, silently renaming the result.
        folded.alias = alias or format_expression(
            Node(node_type=NodeType.FUNCTION, value="COALESCE", parameters=[args[0]])
        )
        if folded.node_type == NodeType.LITERAL:
            folded.qualified_name = format_expression(folded)
        return folded

    # NULLIF(a, b) → IIF(a = b, NULL, a). Lowered to a native comparison + IIF at
    # plan-build time so it never touches a Python kernel. `a` is shared between the
    # equality and the else-branch — the evaluator CSEs by node identity, matching
    # the existing CASE→IIF / CASE→IFNULL rewrites. The NULL literal carries type
    # NULL; vector_iif adopts `a`'s type for the result (incl. DECIMAL/TIMESTAMP).
    if func == "NULLIF":
        if len(args) != 2:
            raise SqlError("NULLIF expects exactly two arguments. Write `NULLIF(value, value_to_treat_as_null)`.")
        value_node, compare_node = args[0], args[1]

        # A NULL operand is folded away rather than lowered, because the lowering
        # cannot express it: `a = NULL` is UNKNOWN, and constant-folding that
        # comparison yields a NULL scalar, which is not the BOOLEAN vector
        # `vector_iif` requires — every literal NULLIF against a NULL died as
        # `draken_iif: condition must be BOOLEAN`. (A NULL *column* was fine: the
        # comparison there produces a real all-NULL BOOLEAN vector.)
        #
        # Both folds are exact, from NULLIF(a, b) == CASE WHEN a = b THEN NULL ELSE a END:
        #   - `a` NULL  -> both branches are NULL, so the answer is NULL.
        #   - `b` NULL  -> `a = NULL` is never TRUE, so the answer is always `a`.
        def _is_null_literal(operand) -> bool:
            return operand.node_type == NodeType.LITERAL and operand.value is None

        if _is_null_literal(value_node) or _is_null_literal(compare_node):
            # `value_node` answers both folds. When IT is the NULL, returning it
            # keeps its own type — a typed NULL (CAST(NULL AS VARCHAR)) must stay
            # VARCHAR, since nothing downstream could recover the type.
            folded = value_node.copy()
            # Without an explicit alias the folded node would name the output
            # column after itself ("name"), silently renaming the result of a
            # NULLIF. Carry the original spelling so folding stays invisible.
            folded.alias = alias or format_expression(
                Node(
                    node_type=NodeType.FUNCTION,
                    value="NULLIF",
                    parameters=[value_node, compare_node],
                )
            )
            # LogicalColumn derives qualified_name as a read-only property; only a
            # plain literal Node carries a settable one.
            if folded.node_type == NodeType.LITERAL:
                folded.qualified_name = format_expression(folded)
            return folded

        equality = Node(
            NodeType.COMPARISON_OPERATOR, value="Eq", left=value_node, right=compare_node
        )
        null_literal = Node(NodeType.LITERAL, type=_CT_NULL, value=None)
        node = Node(
            node_type=NodeType.FUNCTION,
            value="IIF",
            parameters=[equality, null_literal, value_node],
            alias=alias,
        )
        node.qualified_name = format_expression(node)
        return node

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
        # Where the name was written. Constant folding resolves this node again later
        # (`_evaluate_timetravel_expression`) and can reject it there, by which point
        # the AST branch is long gone - so the position travels with the node.
        span=name_span,
    )
    node.qualified_name = format_expression(node)

    over = branch.get("over")
    if over and over != "None" and isinstance(over, dict) and "WindowSpec" in over:
        node.over = over["WindowSpec"]

    return node


#: Aggregates that IGNORE NULL input, and can therefore express
#: `AGG(x) FILTER (WHERE p)` exactly as `AGG(IIF(p, x, NULL))`. Verified against
#: the engine, not assumed: for each of these, `AGG(col)` over a column with
#: NULLs equals `AGG(col)` over the same column filtered to non-NULL.
#:
#: ARRAY_AGG is deliberately absent — it COLLECTS NULLs rather than skipping
#: them, so the substitution would turn rejected rows into NULL elements instead
#: of removing them. ANY_VALUE is absent for the same reason: it may return the
#: NULL the rewrite introduced. CORR takes two arguments and has no single
#: argument to substitute.
_NULL_IGNORING_AGGREGATES = frozenset(
    {
        "APPROX_COUNT_DISTINCT",
        "AVG",
        "COUNT",
        "COUNT_DISTINCT",
        "MAX",
        "MEDIAN",
        "MIN",
        "STDDEV",
        "STDDEV_POP",
        "STDDEV_SAMP",
        "SUM",
        "VAR_POP",
        "VAR_SAMP",
    }
)


def _filtered_aggregate_spelling(func, argument, duplicate_treatment, condition) -> str:
    """How the caller wrote a filtered aggregate, for use as its output name."""
    distinct = "DISTINCT " if duplicate_treatment == "Distinct" else ""
    rendered = "*" if argument.node_type == NodeType.WILDCARD else format_expression(argument)
    return f"{func}({distinct}{rendered}) FILTER (WHERE {format_expression(condition)})"


def hex_literal(branch, alias: Optional[List[str]] = None, key=None):
    value = int(branch, 16)
    return Node(
        NodeType.LITERAL,
        type=_CT_INT64,
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
        span=_span_of(branch),
    )
    alias_name = alias[0] if isinstance(alias, list) and alias else alias
    column.query_column = alias_name or column.source_column
    return column


def in_list(branch, alias: Optional[List[str]] = None, key=None):
    left_node = build(branch["expr"])
    value_nodes = [build(v) for v in branch["list"]]

    # A list element is usually already a LITERAL, but templates like TPC-DS
    # write adjacent values as arithmetic — `d_year IN (1999, 1999+1, 1999+2)`.
    # binary_op() builds that element as an untyped BINARY_OPERATOR (it sets
    # left/right/value but no `.type`), so the type/value reads below would
    # see `.type is None` and either crash building the array or read a false
    # "mixed types" mismatch against the literal beside it. Fold each
    # non-literal element to a concrete literal now, reusing the optimizer's
    # constant folder — there's no schema/binder context yet for its usual
    # (later) pass to run against, but every element here is a closed constant
    # subtree by construction: the IN-list grammar admits no column
    # references (`x IN (SELECT ...)` is the separate in_subquery() builder).
    if any(v.node_type != NodeType.LITERAL for v in value_nodes):
        from opteryx.models.query_telemetry import _QueryTelemetry
        from opteryx.planner.optimizer.strategies.constant_folding import fold_constants

        _telemetry = _QueryTelemetry()
        value_nodes = [
            v if v.node_type == NodeType.LITERAL else fold_constants(v, _telemetry)
            for v in value_nodes
        ]
        not_literal = [v for v in value_nodes if v.node_type != NodeType.LITERAL]
        if not_literal:
            raise UnsupportedSyntaxError(
                "IN-list values must be constant — every element has to fold to a literal, "
                f"but `{format_expression(not_literal[0])}` does not."
            )

    element_ct_set = {v.type for v in value_nodes}
    if len(element_ct_set) > 1:
        raise ArrayWithMixedTypesError("Array in IN condition has values with mixed types. Every element has to share one type; cast them so they match.")
    element_ct = element_ct_set.pop() if element_ct_set else _CT_VARIANT
    operator = "NotInList" if branch["negated"] else "InList"
    right_node = Node(
        node_type=NodeType.LITERAL,
        type=_CT_ARRAY(element_ct),
        value=[v.value for v in value_nodes],
    )
    return Node(
        node_type=NodeType.COMPARISON_OPERATOR,
        value=operator,
        left=left_node,
        right=right_node,
        alias=alias,
    )


def in_subquery(branch, alias: Optional[List[str]] = None, key=None):
    from opteryx.planner.logical_planner.logical_planner import plan_query

    left = build(branch["expr"])
    ast = {"Query": branch["subquery"]}
    subquery_plan = plan_query(ast)
    exit_node = subquery_plan.get_exit_points()[0]
    subquery_plan.remove_node(exit_node, heal=True)

    sub_query = Node(NodeType.SUBQUERY, value=subquery_plan)
    node = Node(
        NodeType.COMPARISON_OPERATOR,
        value="InSubQuery",
        left=left,
        right=sub_query,
    )
    node.negated = branch["negated"]
    return node


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


def _null_test(operand, negated: bool = False):
    """`operand IS NULL` / `operand IS NOT NULL`, as a UNARY_OPERATOR node."""
    return Node(
        NodeType.UNARY_OPERATOR, value="IsNotNull" if negated else "IsNull", centre=operand
    )


def _all_of(*conditions):
    """Left-deep AND over `conditions`."""
    combined = conditions[0]
    for condition in conditions[1:]:
        combined = Node(NodeType.AND, left=combined, right=condition)
    return combined


def _any_of(*conditions):
    """Left-deep OR over `conditions`."""
    combined = conditions[0]
    for condition in conditions[1:]:
        combined = Node(NodeType.OR, left=combined, right=condition)
    return combined


def distinct_from(branch, alias: Optional[List[str]] = None, key=None):
    """`a IS [NOT] DISTINCT FROM b` — null-safe comparison.

    Lowered to null tests, AND/OR and one ordinary comparison:

        a IS DISTINCT FROM b
          ->    (a IS NULL     AND b IS NOT NULL)
             OR (a IS NOT NULL AND b IS NULL)
             OR (a IS NOT NULL AND b IS NOT NULL AND a != b)

    TWO PROPERTIES MAKE THIS THE RIGHT SHAPE, and both are load-bearing.

    It is TOTAL — it can never answer UNKNOWN, which is the entire point of the
    operator. `IS NULL` is total by definition, and the comparison is GUARDED by
    both null tests, so the only branch that could be UNKNOWN is unreachable:
    with either side NULL the guard is FALSE, and `FALSE AND UNKNOWN` is FALSE in
    three-valued logic. Drop the guard and `NULL IS DISTINCT FROM NULL` answers
    UNKNOWN instead of FALSE.

    It is C-NATIVE — every operation is a comparison, boolean algebra or a null
    test, so the program's final opcode produces a mask and the native filter
    accepts it (`bytecode_is_c_native_predicate`). The obvious alternative,
    nesting IIFs, is equally total; it was rejected because `WHERE IIF(...)` did
    not run at all — the call compiles to a function whose result is a value
    vector, not a mask. A boolean-branched IIF is now marked bool-final
    (compiled_expression.pyx) and IS filterable, so that objection has lifted and
    an IIF lowering would be admissible. This shape is kept anyway: it is fewer
    instructions, and it stays on compare/boolean-algebra opcodes that every
    predicate consumer has always accepted, rather than depending on one kernel's
    result shape.

    Note this is NOT the `(a = b) OR (a IS NULL AND b IS NULL)` spelling that
    `decorrelate_subquery._match_null_safe_eq` recognises. That form is fine as a
    filter but not total — with exactly one side NULL it yields UNKNOWN where the
    standard requires FALSE — so it cannot be used for a projected value. If IS
    DISTINCT FROM ever needs to decorrelate, extend that matcher to this shape.
    """
    left, right = build(branch[0]), build(branch[1])
    distinct = key == "IsDistinctFrom"

    both_known = _all_of(_null_test(left, negated=True), _null_test(right, negated=True))
    comparison = Node(
        NodeType.COMPARISON_OPERATOR,
        value="NotEq" if distinct else "Eq",
        left=left,
        right=right,
    )

    if distinct:
        node = _any_of(
            _all_of(_null_test(left), _null_test(right, negated=True)),
            _all_of(_null_test(left, negated=True), _null_test(right)),
            _all_of(both_known, comparison),
        )
    else:
        node = _any_of(
            _all_of(_null_test(left), _null_test(right)),
            _all_of(both_known, comparison),
        )

    node.alias = alias
    node.qualified_name = format_expression(node)
    return node


def overlay_string(branch, alias: Optional[List[str]] = None, key=None):
    """`OVERLAY(s PLACING r FROM start [FOR length])` — splice `r` into `s`.

    Lowered to the definition, which is exactly what the standard gives:

        SUBSTRING(s, 1, start - 1) || r || SUBSTRING(s, start + length, LENGTH(s))

    `length` defaults to LENGTH(r) — the replacement covers as many characters as
    it brings. The trailing SUBSTRING is given an explicit third argument rather
    than using the two-argument form, because that overload is declared in the
    catalog but unbindable (single_table_known_gaps/
    two-argument-substring-is-unbindable); LENGTH(s) is an upper bound the kernel
    already clamps to, so it reads to the end.
    """
    source = build(branch["expr"])
    replacement = build(branch["overlay_what"])
    start = build(branch["overlay_from"])
    length = build(branch.get("overlay_for"))
    if length is None:
        length = Node(node_type=NodeType.FUNCTION, value="LENGTH", parameters=[replacement])

    def _function(name, *parameters):
        return Node(node_type=NodeType.FUNCTION, value=name, parameters=list(parameters))

    def _arithmetic(operator, left, right):
        return Node(NodeType.BINARY_OPERATOR, value=operator, left=left, right=right)

    one = Node(NodeType.LITERAL, type=_CT_INT64, value=1)
    head = _function(
        "SUBSTRING", source, one, _arithmetic("Minus", start, one)
    )
    tail = _function(
        "SUBSTRING",
        source,
        _arithmetic("Plus", start, length),
        _function("LENGTH", source),
    )
    spliced = Node(
        NodeType.BINARY_OPERATOR,
        value="StringConcat",
        left=Node(
            NodeType.BINARY_OPERATOR, value="StringConcat", left=head, right=replacement
        ),
        right=tail,
    )
    spliced.alias = alias
    spliced.qualified_name = format_expression(spliced)
    return spliced


def is_compare(branch, alias: Optional[List[str]] = None, key=None):
    # The alias belongs to the UNARY node, not to its operand — the binder names a
    # projection from the OUTERMOST node (`node.alias or <rendered text>`), so
    # dropping it here named `x IS NOT NULL AS y` after its own SQL text.
    centre = build(branch)
    return Node(NodeType.UNARY_OPERATOR, value=key, centre=centre, alias=alias)


def json_access(branch, alias: Optional[List[str]] = None, key=None):
    identifier_node = build(branch["value"])
    key_node = build(branch["path"]["path"][0]["Bracket"]["key"])

    from opteryx.exceptions import IncorrectTypeError, UnsupportedSyntaxError

    if key_node.node_type == NodeType.IDENTIFIER:
        raise UnsupportedSyntaxError(
            "Subscript values must be integer literals, use `->` to access JSON fields."
        )

    _key_cat = key_node.type.category if key_node.type is not None else None
    if _key_cat != LogicalCategory.INTEGER:
        raise IncorrectTypeError(
            "Subscript values must be integer literals, use `->` to access JSON fields."
        )

    key_value = key_node.value
    identifier_name = format_expression(identifier_node)
    return Node(
        NodeType.EXTRACTION_OPERATOR,
        value="MapAccess",
        left=identifier_node,
        right=key_node,
        alias=alias or f"{identifier_name}[{key_value}]",
    )


def literal_boolean(branch, alias: Optional[List[str]] = None, key=None):
    """create node for a literal boolean branch"""
    return Node(NodeType.LITERAL, type=_CT_BOOLEAN, value=branch, alias=alias)


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
        raise SqlError("Invalid INTERVAL, values must be provided as a VARCHAR. Quote the value, for example `INTERVAL '1' MONTH`.")

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

    return Node(NodeType.LITERAL, type=_CT_INTERVAL, value=interval, alias=alias)


def literal_null(branch=None, alias: Optional[List[str]] = None, key=None):
    """create node for a literal null branch"""
    return Node(NodeType.LITERAL, type=_CT_NULL, alias=alias)


def integer_literal_node(value: int, alias: Optional[List[str]] = None) -> Node:
    """Build a LITERAL node for an exact integer, choosing the narrowest native tier
    that holds it. The SINGLE place integer literal typing is decided, so the type tag
    and the value can never disagree — the failure mode that motivated it is a fold
    that rewrites `value` and leaves `type` describing the old one (see `unary_op`,
    and _coerce_literal's note in binder.py for the same bug in the binder).

    INT64 -> UINT64 -> DECIMAL(precision, 0). The UINT64 tier matters because it keeps
    a literal in (2^63-1, 2^64-1] in the INTEGER category, the same category as the
    unsigned columns it gets compared against; as a DECIMAL it reached the compare as
    a type no kernel handles and failed at RUN time (err_op=11). Above UINT64 the
    engine's only wider native numeric tier is DECIMAL128 (int128-backed, max 38
    digits) — e.g. CAST(123456789012345678901234567890 AS DECIMAL(38,3)) — and an
    exact DECIMAL(precision, 0) is used rather than letting the value reach
    _materialise_constant_literal's generic INT64 fallback, which nb::cast<int64_t>()s
    and throws std::bad_cast for out-of-range values.
    """
    if -(2**63) <= value <= 2**63 - 1:
        return Node(NodeType.LITERAL, type=_CT_INT64, value=value, alias=alias)
    if 0 <= value <= 2**64 - 1:
        return Node(NodeType.LITERAL, type=_CT_UINT64, value=value, alias=alias)
    decimal_value = decimal.Decimal(value)
    precision = len(decimal_value.as_tuple().digits)
    if precision > 38:
        raise SqlError(
            f"Integer literal {value} has {precision} digits; the maximum "
            "supported precision is 38. Split the value, or carry it as text if it does not need arithmetic."
        )
    return Node(
        NodeType.LITERAL,
        type=_CT_DECIMAL(precision, 0),
        value=decimal_value,
        alias=alias,
    )


def literal_number(branch, alias: Optional[List[str]] = None, key=None):
    """create node for a literal number branch"""
    # we have one internal numeric type

    value = branch[0]
    try:
        # Try converting to int first
        return integer_literal_node(int(value), alias)
    except ValueError:
        # If int conversion fails, try converting to float
        value = float(value)
        return Node(
            NodeType.LITERAL,
            type=_CT_FLOAT64,
            value=value,
            alias=alias,
        )


def literal_string(branch, alias: Optional[List[str]] = None, key=None):
    """create node for a string branch"""
    return Node(NodeType.LITERAL, type=_CT_VARCHAR, value=branch, alias=alias)


def match_against(branch, alias: Optional[List[str]] = None, key=None):
    # `columns` is a list of compound identifiers; only the first was ever read, so
    # `MATCH (a, b) AGAINST (...)` silently answered on `a` alone. The declared arity of
    # _MATCH_AGAINST is 2 (one column, one query), so a second column cannot reach the
    # kernel at all — refuse rather than drop it.
    if len(branch["columns"]) != 1:
        raise UnsupportedSyntaxError(
            "MATCH supports a single column: `MATCH (column) AGAINST (string)`. Match one column at a time."
        )
    # MySQL's search modifiers select a full-text search STRATEGY. This MATCH is cosine
    # similarity over embeddings, which has no counterpart to them, and they were being
    # accepted and ignored — a silently different query from the one written.
    if branch.get("opt_search_modifier") is not None:
        raise UnsupportedSyntaxError(
            f"MATCH does not support the `{branch['opt_search_modifier']}` search modifier; "
            "matching is by embedding cosine similarity. Tune it with `SET match_threshold`."
        )
    columns = [identifier(col["Identifier"]) for col in branch["columns"][0]]
    match_to = build(branch["match_value"])

    return Node(
        NodeType.FUNCTION,
        value="_MATCH_AGAINST",
        parameters=[columns[0], match_to],
        alias=alias or f"MATCH ({columns[0].value}) AGAINST ({match_to.value})",
    )


def nested(branch, alias: Optional[List[str]] = None, key=None):
    # The alias belongs on the wrapper, not the centre: the binder names the
    # projection from the outermost node (`query_column = alias or rendered
    # text`), and NESTED is that node for a parenthesised select item. Dropping
    # it here silently named `(<expr>) AS x` after the expression's SQL text —
    # a name containing quotes and parentheses, so the column was unaddressable
    # downstream, and CREATE TABLE/MATERIALIZED VIEW baked that text into the
    # stored schema.
    return Node(
        node_type=NodeType.NESTED,
        centre=build(branch),
        alias=alias,
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
                "**LIKE** ANY syntax incorrect, `column LIKE ANY (patterns)` expected. The patterns go in brackets, for example `name LIKE ANY ('a%', 'b%')`."
            )
        if right.node_type == NodeType.NESTED:
            right = right.centre
        _right_cat = right.type.category if isinstance(right.type, ColumnType) else right.type
        if _right_cat != LogicalCategory.ARRAY:
            right.value = (right.value,)
            right.type = _CT_ARRAY(
                right.type if isinstance(right.type, ColumnType) else _CT_VARCHAR
            )
    return Node(
        NodeType.COMPARISON_OPERATOR,
        value=key,
        left=left,
        right=right,
        alias=alias,
    )


def placeholder(value, alias: Optional[List[str]] = None, key=None):
    from opteryx.exceptions import ParameterError

    raise ParameterError("Unresolved parameter in query. Supply a value for every placeholder in the statement.")


def position(value, alias: Optional[List[str]] = None, key=None):
    sub = build(value["expr"])
    string = build(value["in"])
    return Node(NodeType.FUNCTION, value="POSITION", parameters=[sub, string], alias=alias)


def qualified_wildcard(branch, alias: Optional[List[str]] = None, key=None):
    parts = [build(part).value for part in branch[0]["ObjectName"]]
    qualifier = (".".join(parts),)
    return Node(NodeType.WILDCARD, value=qualifier, alias=alias)


def substring(branch, alias: Optional[List[str]] = None, key=None):
    """SUBSTRING(s FROM a [FOR b]) — emit only the arguments that were written.

    Both slots used to be padded with an untyped NULL literal, so every call
    carried three parameters and only the SUBSTRING_3 overload was ever
    reachable. `SUBSTRING(name, 2)` then failed with "SUBSTRING arg3 (NULL):
    expected INTEGER" — the binder objecting to an argument the caller never
    wrote, and function_signatures.json's SUBSTRING_2 overload was unreachable.

    A missing FROM defaults to 1, which is what the SQL standard says
    `SUBSTRING(s FOR n)` means; a missing FOR is the real two-argument form and
    is passed through as such.
    """
    string = build(branch["expr"])
    substring_from = build(branch["substring_from"]) or Node(
        NodeType.LITERAL, type=_CT_INT64, value=1
    )
    substring_for = build(branch["substring_for"])
    parameters = [string, substring_from]
    if substring_for is not None:
        parameters.append(substring_for)
    return Node(
        NodeType.FUNCTION,
        value="SUBSTRING",
        parameters=parameters,
        alias=alias,
    )


def trim_string(branch, alias: Optional[List[str]] = None, key=None):
    """TRIM in all three of its spellings, onto TRIM / LTRIM / RTRIM.

    The direction picks the function; the characters, whichever spelling carried
    them, become the second argument:

        TRIM(str)                              -> TRIM(str)
        TRIM(LEADING 'x' FROM str)             -> LTRIM(str, 'x')      `trim_what`
        TRIM(str, 'x')                         -> TRIM(str, 'x')       `trim_characters`

    `trim_characters` is a LIST because the dialect's comma form parses a
    comma-separated tail. Only one element is meaningful — the argument is already
    a SET of characters — so the rest are passed through as further arguments and
    the binder refuses them by arity, which names the function and the count.
    A direction cannot reach this branch: parse_opteryx_trim (src/opteryx_dialect.rs)
    refuses `TRIM(LEADING str, 'x')` outright rather than dropping the LEADING.
    """
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
    for characters in branch["trim_characters"] or []:
        parameters.append(build(characters))

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

    # Infer element ColumnType: homogeneous only
    node_types = {t.type for t in node_values}
    element_ct = node_types.pop() if len(node_types) == 1 else None

    # ALWAYS an ARRAY — a literal is never a VECTOR (architect, 2026-07-16).
    #
    # A numeric-homogeneous tuple previously became _CT_VECTOR(len(values)), which
    # made `(1,2)` and `('a','b')` bind to different type families for no reason the
    # rest of the system honours. It bought nothing and actively hurt: it let
    # COSINE_SIMILARITY((1.0,2.0),(1.0,2.0)) PASS the binder's NUMERIC_VECTOR check
    # and then die at run time inside draken_vector_unwrap ("expected Vector, got
    # tuple") — strictly worse than the bracket form `[1.0,2.0]`, which is an ARRAY
    # and fails cleanly at bind time. Nothing constructs a VECTOR_FP16 column, so no
    # reachable path consumed this. Both literal syntaxes now agree, and a numeric
    # tuple can reach the ARRAY-typed containment operators (`@>` / `@>>`) that a
    # VECTOR-typed one was rejected by.
    literal_type = _CT_ARRAY(element_ct if element_ct is not None else _CT_VARIANT)

    if values and isinstance(values[0], dict):
        values = [build(val["Identifier"]).value for val in values]
    return Node(
        NodeType.LITERAL,
        type=literal_type,
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
        f"Use **CAST**(... AS {data_type}) instead. Only INTERVAL retains prefix literal syntax."
    )


def unary_op(branch, alias: Optional[List[str]] = None, key=None):
    if branch["op"] == "Not":
        # As in is_compare: the alias names the NOT node, never its operand.
        centre = build(branch["expr"])
        return Node(node_type=NodeType.NOT, centre=centre, alias=alias)
    if branch["op"] == "Minus":
        centre = build(branch["expr"], alias=alias)
        # Constant-fold numeric literals (e.g. `-5`). An INTEGER literal must be
        # RE-TYPED from the negated value, not just have its value flipped: the
        # parser hands us `-N` as unary minus over the POSITIVE literal N, so a wide
        # value lands here already tagged UINT64, and negating in place left a
        # negative int under an unsigned tag (std::bad_cast at materialisation).
        # integer_literal_node is the single typing rule — see its docstring.
        if centre.node_type == NodeType.LITERAL and isinstance(centre.value, (int, float)):
            if isinstance(centre.value, float):
                centre.value = 0 - centre.value
                return centre
            # int (and bool, its subclass — `-TRUE` folds to -1 as it always has,
            # now tagged INT64 rather than keeping the operand's BOOLEAN tag).
            return integer_literal_node(0 - centre.value, alias)
        # General case: lower unary minus on an expression to `0 - expr`.
        zero = Node(NodeType.LITERAL, type=_CT_INT64, value=0)
        return Node(
            get_operator_node_type("Minus"),
            value="Minus",
            left=zero,
            right=centre,
            alias=alias,
        )
    if branch["op"] == "Plus":
        return build(branch["expr"], alias=alias)
    if branch["op"] == "BitwiseNot":
        centre = build(branch["expr"])
        return Node(
            node_type=NodeType.UNARY_OPERATOR, value="BitwiseNot", centre=centre, alias=alias
        )


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
    raise SqlError(f"Unhandled token in Syntax Tree `{key}`")


def build(value, alias: Optional[List[str]] = None, key=None):
    """
    Extract values from a value node in the AST and create a ExpressionNode for it

    More of the builders will be migrated to this approach to keep the code
    more succinct and easier to read.
    """
    ignored = ("filter",)

    if value in ("Null", "Wildcard"):
        # `Null` and `Wildcard` are BARE STRING ast nodes, not dicts, so they are
        # dispatched here rather than through the keyed branch below — but the
        # alias must still be threaded. Dropping it left `SELECT NULL AS a` named
        # `None` instead of `a`, and, because the projection's duplicate check
        # keys on (identity, alias-or-value), made every pair of NULL literals
        # look like the SAME output column: `SELECT NULL AS a, NULL AS b` died in
        # the binder. That also broke FULL OUTER JOIN, which
        # FullOuterToUnionStrategy rewrites into a union whose anti-join leg
        # synthesizes one NULL literal per column of the non-preserved side —
        # two such columns collided and the union concatenated mismatched types.
        # Both builders already accept and honour `alias`.
        return BUILDERS[value](value, alias)
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
    "Cube": unsupported_grouping_construct,
    "GroupingSets": unsupported_grouping_construct,
    "Rollup": rollup,
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
    "Subquery": scalar_subquery,
    "Substring": substring,
    "Tuple": tuple_literal,
    "IsDistinctFrom": distinct_from,
    "IsNotDistinctFrom": distinct_from,
    "Overlay": overlay_string,
    "Trim": trim_string,
    "TypedString": typed_string,
    "UnaryOp": unary_op,
    "Unnamed": build,
    "Value": build,
    "value": build,
    "Wildcard": wildcard_filter,
    "UnnamedExpr": build,
}
