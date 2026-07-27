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


cpdef str _format_interval_iso8601(value):
    """Render an interval (months, microseconds) as an ISO-8601 duration.

    Mirrors iso8601_duration_emit in draken/ops/sql_temporal_format.h byte-for-byte
    (used for CAST(<interval literal> AS VARCHAR) plan-time folding — the native
    kernel handles the column case; this is the ONLY other place this format is
    produced, so keep both in sync deliberately if either changes).
    """
    months, microseconds = value
    negative = months < 0 or microseconds < 0
    am = -months if months < 0 else months
    au = -microseconds if microseconds < 0 else microseconds

    years, months_rem = divmod(am, 12)
    days, rem = divmod(au, 86400000000)
    hours, rem = divmod(rem, 3600000000)
    minutes, rem = divmod(rem, 60000000)
    seconds, frac = divmod(rem, 1000000)

    cdef list parts = ["-"] if negative else []
    parts.append("P")
    any_part = False
    if years > 0:
        parts.append(f"{years}Y")
        any_part = True
    if months_rem > 0:
        parts.append(f"{months_rem}M")
        any_part = True
    if days > 0:
        parts.append(f"{days}D")
        any_part = True
    has_time = hours > 0 or minutes > 0 or seconds > 0 or frac > 0
    if has_time:
        parts.append("T")
        if hours > 0:
            parts.append(f"{hours}H")
            any_part = True
        if minutes > 0:
            parts.append(f"{minutes}M")
            any_part = True
        if seconds > 0 or frac > 0:
            if frac > 0:
                parts.append(f"{seconds}.{frac:06d}S")
            else:
                parts.append(f"{seconds}S")
            any_part = True
    if not any_part:
        parts.append("T0S")
    return "".join(parts)


def format_expression(root, qualify=False, cache=None):
    """Render an expression tree as a SQL-like string.

    ``cache`` is an optional ``id(node) -> str`` memo threaded through the
    recursion. Contract: entries are only valid while the tree is unmutated
    and for a single ``qualify`` setting. The binder supplies a fresh cache
    per root bind (where every subtree is rendered pristine, top-down) to
    collapse its O(n²) per-node memo-key renders to O(n); other callers
    should omit it.
    """
    if cache is None:
        return _format_expression_inner(root, qualify, None)
    key = id(root)
    cached = cache.get(key)
    if cached is not None:
        return cached
    result = _format_expression_inner(root, qualify, cache)
    if type(result) is str:
        cache[key] = result
    return result


def _format_expression_inner(root, qualify, cache):
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
        return [format_expression(item, qualify_b, cache) for item in root]

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
            f"WHEN {format_expression(c, qualify_b, cache)} THEN {format_expression(v, qualify_b, cache)} "
            for c, v in zip(root.conditions or [], root.results or [])
        )
        else_part = (
            f"ELSE {format_expression(root.else_result, qualify_b, cache)} "
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
                [format_expression(e, qualify_b, cache) for e in root.parameters]
            )
            return f"{root.value.upper()}({distinct}{params}{order})"
        if node_type == NodeType.CAST:
            source_expr = format_expression(root.left, qualify_b, cache)
            target_type = root.value
            if root.parameters:
                params = ",".join(
                    [format_expression(p, qualify_b, cache) for p in root.parameters]
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
                f"{format_expression(root.left, qualify_b, cache)} {token} "
                f"{format_expression(root.right, qualify_b, cache)}"
            )
        if node_type == NodeType.EXTRACTION_OPERATOR:
            if root.value == "MapAccess":
                return (
                    f"{format_expression(root.left, qualify_b, cache)}"
                    f"[{format_expression(root.right, qualify_b, cache)}]"
                )
            token = (get_operator_token(root.value) or root.value).upper()
            return (
                f"{format_expression(root.left, qualify_b, cache)} {token} "
                f"{format_expression(root.right, qualify_b, cache)}"
            )
        if node_type == NodeType.EXPRESSION_LIST:
            return f"<EXPRESSIONS {random_string(4)}>"

    if node_type == NodeType.COMPARISON_OPERATOR:
        token = (get_operator_token(root.value) or root.value).upper()
        return (
            f"{format_expression(root.left, qualify_b, cache)} {token} "
            f"{format_expression(root.right, qualify_b, cache)}"
        )
    if node_type == NodeType.UNARY_OPERATOR:
        _map = {
            "IsNull": "%s IS NULL",
            "IsNotNull": "%s IS NOT NULL",
            "IsEmpty": "%s IS EMPTY",
            "IsNotEmpty": "%s IS NOT EMPTY",
            "BitwiseNot": "~%s",
        }
        # Most unary operators carry their operand in `centre`, but EXISTS puts its
        # subquery in `parameters`. Reading only `centre` rendered EVERY `EXISTS` as
        # "Exists(null)", so two different EXISTS in one predicate produced the same
        # text — and since the binder treats an expression's rendering as its
        # identity, the second resolved to the first and both subqueries collapsed
        # into one. `negated` is part of the meaning, so it is part of the rendering.
        operand = root.centre
        if operand is None:
            parameters = root.parameters or []
            operand = parameters[0] if parameters else None
        rendered = _map.get(root.value, root.value + "(%s)").replace(
            "%s", format_expression(operand, qualify_b, cache)
        )
        return f"NOT {rendered}" if root.negated else rendered
    if node_type == NodeType.NOT:
        return f"NOT {format_expression(root.centre, qualify_b, cache)}"
    if node_type == NodeType.AND or node_type == NodeType.OR or node_type == NodeType.XOR:
        _map = {
            NodeType.AND: "AND",
            NodeType.OR: "OR",
            NodeType.XOR: "XOR",
        }
        return (
            f"({format_expression(root.left, qualify_b, cache)} "
            f"{_map[node_type]} {format_expression(root.right, qualify_b, cache)})"
        )
    if node_type == NodeType.NESTED:
        return f"({format_expression(root.centre, qualify_b, cache)})"
    if node_type == NodeType.IDENTIFIER:
        if qualify_b and root.source:
            return root.qualified_name
        return root.current_name
    if node_type == NodeType.DNF:
        return " AND ".join(
            [format_expression(e, qualify_b, cache) for e in root.parameters]
        )
    if node_type == NodeType.CNF:
        return " OR ".join(
            [format_expression(e, qualify_b, cache) for e in root.parameters]
        )
    if node_type == NodeType.SUBQUERY:
        # A subquery has no textual form here — `root.value` is a whole LogicalPlan,
        # so the fallback `str(root.value)` renders it as "Graph - N nodes, M edges".
        # Two DIFFERENT subqueries of the same size then render identically, and the
        # binder uses this rendering as an expression's identity: the second
        # subquery resolves to the first one's column and the two collapse into one.
        # `uuid` is unique per node and preserved across plan copies, so it
        # distinguishes them without depending on the plan's shape.
        return f"SUBQUERY-{root.uuid}"
    if node_type == NodeType.BETWEEN:
        col = format_expression(root.left, qualify_b, cache)
        lower = format_expression(root.right, qualify_b, cache)
        upper = format_expression(root.centre, qualify_b, cache)
        lower_inclusive, upper_inclusive = root.value
        if lower_inclusive and upper_inclusive:
            return f"{col} BETWEEN {lower} AND {upper}"
        lower_op = ">=" if lower_inclusive else ">"
        upper_op = "<=" if upper_inclusive else "<"
        return f"({col} {lower_op} {lower} AND {col} {upper_op} {upper})"
    return str(root.value)
