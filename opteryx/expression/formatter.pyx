"""Expression rendering for diagnostics / EXPLAIN / display.

Recursive walk over a bound expression tree producing a SQL-like string.
Not on the per-row hot path — called once per shown expression.
"""

from dataclasses import dataclass

from opteryx.types.logical_type import LogicalCategory
from opteryx.types.schema import SchemaColumn
from opteryx.utils import random_string


@dataclass
class ExpressionColumn(SchemaColumn):
    expression: object = None

    def __post_init__(self):
        # Expression/predicate columns are computed, not relation-sourced; mint a
        # unique `$derived_` identity rather than hitting the base-class raise.
        if self.identity is None:
            self.identity = f"$derived_{random_string(8)}".encode("utf-8")
        super().__post_init__()


cpdef str _format_interval(value):
    """Render an interval (months, microseconds) as a SQL INTERVAL literal."""
    months, microseconds = value

    seconds = microseconds / MICROSECONDS_PER_SECOND
    days, seconds = divmod(seconds, 86400)
    hours, seconds = divmod(seconds, 3600)
    minutes, seconds = divmod(seconds, 60)
    years, months = divmod(months, 12)
    cdef list parts = []
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


def format_expression(root, qualify=False):
    # Lazy: opteryx.expression imports format_expression at module load,
    # and opteryx.expression.operator_catalog imports from .expression — a
    # cycle that's only safe to break at first call.
    from . import INTERNAL_TYPE, NodeType
    from .operator_catalog import get_operator_token

    if root is None:
        return "null"

    cdef bint qualify_b = qualify
    if not qualify_b and root.left and root.right:
        # Force qualification when both sides render identically.
        qualify_b = (root.left.current_name == root.right.current_name) and (
            root.right.current_name is not None
        )

    if type(root) is list:
        return [format_expression(item, qualify_b) for item in root]

    node_type = root.node_type
    cdef dict _map

    # LITERALS
    if node_type == NodeType.LITERAL:
        literal_type = root.type
        if literal_type == LogicalCategory.VARCHAR:
            return "'" + root.value.replace("'", "'") + "'"
        if literal_type == LogicalCategory.TIMESTAMP:
            return "'" + str(root.value) + "'"
        if literal_type == LogicalCategory.INTERVAL:
            return _format_interval(root.value)
        if literal_type == LogicalCategory.NULL:
            return "null"
        if literal_type == LogicalCategory.ARRAY:
            display = getattr(root, "display_values", None)
            if display is not None:
                shown = display[:3]
                rest = len(display) - len(shown)
                items = ", ".join(shown)
                return "{" + items + (f", ...{rest} more" if rest else "") + "}"
        return str(root.value)

    if node_type == NodeType.CASE:
        parts = "".join(
            f"WHEN {format_expression(c, qualify_b)} THEN {format_expression(v, qualify_b)} "
            for c, v in zip(root.conditions or [], root.results or [])
        )
        else_part = (
            f"ELSE {format_expression(root.else_result, qualify_b)} "
            if root.else_result is not None
            else ""
        )
        return f"CASE {parts}{else_part}END"

    if node_type & INTERNAL_TYPE == INTERNAL_TYPE:
        if node_type == NodeType.FUNCTION or node_type == NodeType.AGGREGATOR:
            distinct = "DISTINCT " if root.duplicate_treatment else ""
            order = ""
            if root.order:
                order = " ORDER BY " + ", ".join(
                    item[0].value + (" DESC" if not item[1] else "")
                    for item in (root.order or [])
                )
            if root.value == "ARRAY_AGG":
                limit = f" LIMIT {root.limit}" if root.limit else ""
                return (
                    f"{root.value.upper()}({distinct}"
                    f"{root.parameters[0].current_name}{order}{limit})"
                )
            params = ",".join(
                [format_expression(e, qualify_b) for e in root.parameters]
            )
            return f"{root.value.upper()}({distinct}{params}{order})"
        if node_type == NodeType.CAST:
            source_expr = format_expression(root.left, qualify_b)
            target_type = root.value
            if root.parameters:
                params = ",".join(
                    [format_expression(p, qualify_b) for p in root.parameters]
                )
                return f"{source_expr}::{target_type}({params})"
            return f"{source_expr}::{target_type}"
        if node_type == NodeType.WILDCARD:
            if root.value:
                return f"{root.value[0]}.*"
            return "*"
        if node_type == NodeType.BINARY_OPERATOR:
            token = (get_operator_token(root.value) or root.value).upper()
            return (
                f"{format_expression(root.left, qualify_b)} {token} "
                f"{format_expression(root.right, qualify_b)}"
            )
        if node_type == NodeType.EXTRACTION_OPERATOR:
            if root.value == "MapAccess":
                return (
                    f"{format_expression(root.left, qualify_b)}"
                    f"[{format_expression(root.right, qualify_b)}]"
                )
            token = (get_operator_token(root.value) or root.value).upper()
            return (
                f"{format_expression(root.left, qualify_b)} {token} "
                f"{format_expression(root.right, qualify_b)}"
            )
        if node_type == NodeType.EXPRESSION_LIST:
            return f"<EXPRESSIONS {random_string(4)}>"

    if node_type == NodeType.COMPARISON_OPERATOR:
        token = (get_operator_token(root.value) or root.value).upper()
        return (
            f"{format_expression(root.left, qualify_b)} {token} "
            f"{format_expression(root.right, qualify_b)}"
        )
    if node_type == NodeType.UNARY_OPERATOR:
        _map = {
            "IsNull": "%s IS NULL",
            "IsNotNull": "%s IS NOT NULL",
            "IsEmpty": "%s IS EMPTY",
            "IsNotEmpty": "%s IS NOT EMPTY",
            "BitwiseNot": "~%s",
        }
        return _map.get(root.value, root.value + "(%s)").replace(
            "%s", format_expression(root.centre, qualify_b)
        )
    if node_type == NodeType.NOT:
        return f"NOT {format_expression(root.centre, qualify_b)}"
    if node_type == NodeType.AND or node_type == NodeType.OR or node_type == NodeType.XOR:
        _map = {
            NodeType.AND: "AND",
            NodeType.OR: "OR",
            NodeType.XOR: "XOR",
        }
        return (
            f"({format_expression(root.left, qualify_b)} "
            f"{_map[node_type]} {format_expression(root.right, qualify_b)})"
        )
    if node_type == NodeType.NESTED:
        return f"({format_expression(root.centre, qualify_b)})"
    if node_type == NodeType.IDENTIFIER:
        if qualify_b and root.source:
            return root.qualified_name
        return root.current_name
    if node_type == NodeType.DNF:
        return " AND ".join(
            [format_expression(e, qualify_b) for e in root.parameters]
        )
    if node_type == NodeType.CNF:
        return " OR ".join(
            [format_expression(e, qualify_b) for e in root.parameters]
        )
    if node_type == NodeType.BETWEEN:
        col = format_expression(root.left, qualify_b)
        lower = format_expression(root.right, qualify_b)
        upper = format_expression(root.centre, qualify_b)
        lower_inclusive, upper_inclusive = root.value
        if lower_inclusive and upper_inclusive:
            return f"{col} BETWEEN {lower} AND {upper}"
        lower_op = ">=" if lower_inclusive else ">"
        upper_op = "<=" if upper_inclusive else "<"
        return f"({col} {lower_op} {lower} AND {col} {upper_op} {upper})"
    return str(root.value)
