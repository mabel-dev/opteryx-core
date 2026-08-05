"""Expression rendering for diagnostics / EXPLAIN / display.

Recursive walk over a bound expression tree producing a SQL-like string.
Not on the per-row hot path — called once per shown expression.
"""

import datetime
from dataclasses import dataclass

from draken.draken_native import DrakenType
from draken.draken_native import LogicalKind
from draken.draken_native import ipv4_format

from opteryx.types.logical_type import ColumnType
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


cdef inline tuple _civil_from_days(long long days):
    """(year, month, day) from days since 1970-01-01 (Hinnant's civil_from_days).

    The one implementation. A DATE literal is stored as days and a TIMESTAMP
    literal as microseconds, so both the plan-time CAST-to-VARCHAR fold in
    logical_planner_builders and the rendering below need this — and two copies
    of a calendar algorithm is two calendars.
    """
    cdef long long z = days + 719468
    cdef long long era = (z if z >= 0 else z - 146096) // 146097
    cdef long long doe = z - era * 146097
    cdef long long yoe = (doe - doe // 1460 + doe // 36524 - doe // 146096) // 365
    cdef long long yr = yoe + era * 400
    cdef long long doy = doe - (365 * yoe + yoe // 4 - yoe // 100)
    cdef long long mp = (5 * doy + 2) // 153
    cdef long long d = doy - (153 * mp + 2) // 5 + 1
    cdef long long m = mp + 3 if mp < 10 else mp - 9
    return (yr + (1 if m <= 2 else 0), m, d)


cpdef str _format_date_days(long long days):
    """Days since the epoch -> 'YYYY-MM-DD' — the physical form of a DATE literal
    rendered as the date it is."""
    y, m, d = _civil_from_days(days)
    return f"{y:04d}-{m:02d}-{d:02d}"


cpdef str _format_timestamp_micros(long long us):
    """Microseconds since the epoch -> 'YYYY-MM-DDTHH:MM:SS.ffffff'.

    Matches draken_cast_timestamp_to_string (the runtime kernel), so a folded
    literal and a scanned column render a timestamp identically. Floor division
    carries the borrow for pre-epoch values without a sign correction: Python's
    `//` and `%` floor, so the remainders here are never negative.
    """
    cdef long long sec = us // 1000000
    cdef long long usec = us % 1000000
    cdef long long days64 = sec // 86400
    cdef long long tod = sec % 86400
    cdef long long hh = tod // 3600
    cdef long long mm = (tod % 3600) // 60
    cdef long long ss = tod % 60
    y, m, d = _civil_from_days(days64)
    return f"{y:04d}-{m:02d}-{d:02d}T{hh:02d}:{mm:02d}:{ss:02d}.{usec:06d}"


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


cdef str _format_literal(root):
    """Render a LITERAL node as the value a user wrote, not as the bits it is stored as.

    A literal's `.type` is a `ColumnType` (§14), so the PHYSICAL tag plus its
    descriptor decides the rendering. This used to compare `.type` against
    `LogicalCategory` members; since the type unification every one of those
    comparisons was permanently False (`ColumnType.__eq__` on a `LogicalCategory`
    returns NotImplemented), so every literal fell through to `str(value)` and was
    named with its raw PHYSICAL value — an INTERVAL as the `(months, microseconds)`
    tuple it is stored as, a TIMESTAMP as int64 microseconds, a DATE as
    days-since-epoch, a NULL as Python's `None`, a string unquoted.

    Every case here must be INJECTIVE within its type: this rendering doubles as an
    expression's identity (the binder resolves a literal to an existing column by
    it, and the planner dedups projections by it), so two distinct literals that
    render alike become one column carrying one value.
    """
    literal_type = root.type
    value = root.value

    # An unbound or synthetic literal with no ColumnType has nothing to dispatch on.
    if not isinstance(literal_type, ColumnType):
        return str(value)

    cdef object physical = literal_type.physical

    # NULL first: a typed NULL (`CAST(NULL AS VARCHAR)`) is physically VARCHAR but
    # holds no value, and every branch below would be rendering `None`.
    if value is None or physical == DrakenType.NULL:
        return "null"

    # IPv4 renders from its DESCRIPTOR, never its category — the category is
    # deliberately INTEGER, so an address would otherwise print as the uint32 it is.
    # Routed through draken.ipv4_format, the same writer the kernel and to_pylist
    # use, so a literal and a column cannot spell an address differently.
    if (
        literal_type.logical is not None
        and literal_type.logical.kind == LogicalKind.IPV4
        and isinstance(value, int)
    ):
        return ipv4_format(value)

    if physical == DrakenType.VARCHAR or physical == DrakenType.NVARCHAR:
        # Bound VARCHAR literals carry bytes, unbound ones str — this runs on both
        # sides of the binder. Quoting is what keeps a string literal distinct from
        # anything else that renders the same text: without it `'192.168.1.1'` and
        # an IPv4 literal are the same expression.
        text = value.decode("utf-8") if isinstance(value, bytes) else value
        return "'" + text.replace("'", "''") + "'"

    # Temporal literals carry their type word. Quoting alone is not enough: the
    # string literal `'2020-01-01'` and `CAST('2020-01-01' AS DATE)` are different
    # expressions of different types that would otherwise both render
    # `'2020-01-01'` — and two expressions with one rendering are one column.
    if physical == DrakenType.TIMESTAMP64 and isinstance(value, int):
        return "TIMESTAMP '" + _format_timestamp_micros(value) + "'"

    if physical == DrakenType.DATE32 and isinstance(value, int):
        return "DATE '" + _format_date_days(value) + "'"

    if (physical == DrakenType.TIME32 or physical == DrakenType.TIME64) and isinstance(
        value, datetime.time
    ):
        return "TIME '" + value.isoformat() + "'"

    if physical == DrakenType.INTERVAL and isinstance(value, tuple):
        return _format_interval(value)

    # BOOL, the integer and float widths, DECIMAL, VARBINARY and ARRAY already
    # render as themselves, and their `str()` is injective.
    return str(value)


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
        return _format_literal(root)

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
        if root.current_name is not None:
            return root.current_name
        # Post-bind synthetic identifiers (e.g. a Projection passing through an
        # aggregate-output column built directly against a schema_column, never
        # parsed from SQL text) carry no current_name. schema_column.name is the
        # established fallback for "the user-facing name" elsewhere in the planner
        # (group_key_reduction.py, filter_implied_group_key_reduction.py).
        if root.schema_column is not None:
            return root.schema_column.name
        return "null"
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
