from dataclasses import dataclass
from typing import Any

from opteryx.expression.intervals import MICROSECONDS_PER_SECOND
from opteryx.types import OrsoTypes
from opteryx.types.schema import FlatColumn
from opteryx.utils import random_string


@dataclass
class ExpressionColumn(FlatColumn):
    expression: Any = None


def _format_interval(value):
    months, microseconds = value

    seconds = microseconds / MICROSECONDS_PER_SECOND
    days, seconds = divmod(seconds, 86400)
    hours, seconds = divmod(seconds, 3600)
    minutes, seconds = divmod(seconds, 60)
    years, months = divmod(months, 12)
    parts = []
    if years >= 1:
        parts.append(f"{int(years)} YEAR")
    if months >= 1:
        parts.append(f"{int(months)} MONTH")
    if days >= 1:
        parts.append(f"{int(days)} DAY")
    if hours >= 1:
        parts.append(f"{int(hours)} HOUR")
    if minutes >= 1:
        parts.append(f"{int(minutes)} MINUTE")
    if abs(seconds) > 0:
        parts.append(f"{seconds:.6f} SECOND")
    return " ".join(parts)


def format_expression(root, qualify: bool = False):
    # circular imports
    from . import INTERNAL_TYPE, NodeType
    from .operator_catalog import get_operator_token

    if root is None:
        return "null"

    if not qualify and root.left and root.right:
        # if left and right would look the same without qualifying, force qualification
        qualify = (root.left.current_name == root.right.current_name) and (
            root.right.current_name is not None
        )

    if type(root) is list:
        return [format_expression(item, qualify) for item in root]

    node_type = root.node_type
    _map: dict = {}

    # LITERAL TYPES
    if node_type == NodeType.LITERAL:
        literal_type = root.type
        if literal_type == OrsoTypes.VARCHAR:
            return "'" + root.value.replace("'", "'") + "'"
        if literal_type == OrsoTypes.TIMESTAMP:
            return "'" + str(root.value) + "'"
        if literal_type == OrsoTypes.INTERVAL:
            return _format_interval(root.value)
        if literal_type == OrsoTypes.NULL:
            return "null"
        return str(root.value)
    # INTERAL IDENTIFIERS
    if node_type & INTERNAL_TYPE == INTERNAL_TYPE:
        if node_type in (NodeType.FUNCTION, NodeType.AGGREGATOR):
            if root.value in ("CASE", "_CASE"):
                con = [format_expression(a, qualify) for a in root.parameters[0].parameters]
                vals = [format_expression(a, qualify) for a in root.parameters[1].parameters]
                return "CASE " + "".join([f"WHEN {c} THEN {v} " for c, v in zip(con, vals)]) + "END"
            distinct = "DISTINCT " if root.duplicate_treatment else ""
            order = ""
            if root.order:
                order = f" ORDER BY {', '.join(item[0].value + (' DESC' if not item[1] else '') for item in (root.order or []))}"
            if root.value == "ARRAY_AGG":
                limit = f" LIMIT {root.limit}" if root.limit else ""
                return f"{root.value.upper()}({distinct}{root.parameters[0].current_name}{order}{limit})"
            return f"{root.value.upper()}({distinct}{','.join([format_expression(e, qualify) for e in root.parameters])}{order})"
        if node_type == NodeType.CAST:
            # Format CAST expressions: expr::TYPE or expr::TYPE(params)
            source_expr = format_expression(root.left, qualify)
            target_type = root.value
            if root.parameters:
                # Include parameters like precision/scale if present
                params = ",".join([format_expression(p, qualify) for p in root.parameters])
                return f"{source_expr}::{target_type}({params})"
            return f"{source_expr}::{target_type}"
        if node_type == NodeType.WILDCARD:
            if root.value:
                return f"{root.value[0]}.*"
            return "*"
        if node_type == NodeType.BINARY_OPERATOR:
            token = (get_operator_token(root.value) or root.value).upper()
            return f"{format_expression(root.left, qualify)} {token} {format_expression(root.right, qualify)}"
        if node_type == NodeType.EXTRACTION_OPERATOR:
            if root.value == "MapAccess":
                return (
                    f"{format_expression(root.left, qualify)}"
                    f"[{format_expression(root.right, qualify)}]"
                )
            token = (get_operator_token(root.value) or root.value).upper()
            return f"{format_expression(root.left, qualify)} {token} {format_expression(root.right, qualify)}"
        if node_type == NodeType.EXPRESSION_LIST:
            return f"<EXPRESSIONS {random_string(4)}>"
    if node_type == NodeType.COMPARISON_OPERATOR:
        token = (get_operator_token(root.value) or root.value).upper()
        return f"{format_expression(root.left, qualify)} {token} {format_expression(root.right, qualify)}"
    if node_type == NodeType.UNARY_OPERATOR:
        _map = {
            "IsNull": "%s IS NULL",
            "IsNotNull": "%s IS NOT NULL",
            "BitwiseNot": "~%s",
        }
        return _map.get(root.value, root.value + "(%s)").replace(
            "%s", format_expression(root.centre, qualify)
        )
    if node_type == NodeType.NOT:
        return f"NOT {format_expression(root.centre, qualify)}"
    if node_type in (NodeType.AND, NodeType.OR, NodeType.XOR):
        _map = {
            NodeType.AND: "AND",
            NodeType.OR: "OR",
            NodeType.XOR: "XOR",
        }  # type: ignore
        return f"({format_expression(root.left, qualify)} {_map[node_type]} {format_expression(root.right, qualify)})"
    if node_type == NodeType.NESTED:
        return f"({format_expression(root.centre, qualify)})"
    if node_type == NodeType.IDENTIFIER:
        if qualify and root.source:
            return root.qualified_name
        return root.current_name
    if node_type == NodeType.DNF:
        return " AND ".join([format_expression(e, qualify) for e in root.parameters])
    if node_type == NodeType.BETWEEN:
        col = format_expression(root.left, qualify)
        lower = format_expression(root.right, qualify)
        upper = format_expression(root.centre, qualify)
        lower_inclusive, upper_inclusive = root.value
        if lower_inclusive and upper_inclusive:
            return f"{col} BETWEEN {lower} AND {upper}"
        lower_op = ">=" if lower_inclusive else ">"
        upper_op = "<=" if upper_inclusive else "<"
        return f"({col} {lower_op} {lower} AND {col} {upper_op} {upper})"
    return str(root.value)
