import datetime
from typing import List

# `_make` is the registrar package-level helper which constructs a
# FunctionDefinition with a single overload. Use the shorthand form where
# the second argument is the return type for zero-argument constants.
from opteryx.types import SqlType


def get_builtin_constant_functions() -> List:
    """
    Return zero-argument plan-time constant function definitions.

    These functions are folded to literals by the binder at planning time
    (the binder rewrites them to LITERAL nodes). They remain present in the
    function catalog so `is_function()` and name-based validation succeed
    during AST construction.
    """
    return [
        _make("CURRENT_DATE", SqlType.DATE, summary="Current date."),
        _make("CURRENT_TIME", SqlType.TIME, summary="Current time."),
        _make(
            "CURRENT_TIMESTAMP",
            SqlType.TIMESTAMP,
            aliases=("NOW",),
            summary="Current timestamp.",
        ),
        _make("UTC_TIMESTAMP", SqlType.TIMESTAMP, summary="Current UTC timestamp."),
        _make("VERSION", SqlType.VARCHAR, summary="Database version string."),
        _make("CONNECTION_ID", SqlType.INTEGER, summary="Current connection identifier."),
        _make("DATABASE", SqlType.VARCHAR, summary="Current database name."),
        _make("USER", SqlType.VARCHAR, summary="Current user name."),
        _make("PI", SqlType.DOUBLE, summary="Mathematical constant π."),
        _make("PHI", SqlType.DOUBLE, summary="Golden ratio φ."),
        _make("E", SqlType.DOUBLE, summary="Euler's number e."),
    ]


def fixed_value_function(function, context):
    """Get the fixed value for a compile-time constant function.

    Used by the planner to fold constant function calls at planning time.
    """
    if function in ("VERSION",):
        import opteryx

        return SqlType.VARCHAR, opteryx.__version__
    if function in ("NOW", "UTC_TIMESTAMP"):
        return SqlType.TIMESTAMP, context.execution_context.connected_at
    if function in ("CURRENT_TIME",):
        # CURRENT_TIME is an alias for NOW, so we return the same value
        return SqlType.TIME, context.execution_context.connected_at.time()
    if function in ("CURRENT_TIMESTAMP",):
        # CURRENT_TIMESTAMP is an alias for NOW, so we return the same value
        return SqlType.TIMESTAMP, context.execution_context.connected_at
    if function in ("CURRENT_DATE", "TODAY"):
        return SqlType.DATE, context.execution_context.connected_at.date()
    if function in ("YESTERDAY",):
        return SqlType.DATE, context.execution_context.connected_at.date() - datetime.timedelta(days=1)
    if function == "CONNECTION_ID":
        return SqlType.INTEGER, context.execution_context.query_id
    if function == "DATABASE":
        return SqlType.VARCHAR, context.execution_context.schema or "DEFAULT"
    if function == "USER":
        return SqlType.VARCHAR, context.execution_context.user or "ANONYMOUS"
    if function == "PI":
        return SqlType.DOUBLE, 3.14159265358979323846264338327950288419716939937510
    if function == "PHI":
        # the golden ratio
        return SqlType.DOUBLE, 1.61803398874989484820458683436563811772030917980576
    if function == "E":
        # eulers number
        return SqlType.DOUBLE, 2.71828182845904523536028747135266249775724709369995
    if function == "UNIXTIME":
        # We should only ever get here if the function is called without parameters
        return SqlType.INTEGER, context.execution_context.connected_at.timestamp()
    return None, None
