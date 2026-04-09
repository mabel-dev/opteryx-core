from __future__ import annotations

import datetime
from typing import List

import numpy
from orso.types import OrsoTypes

# `_make` is the registrar package-level helper which constructs a
# FunctionDefinition with a single overload. Use the shorthand form where
# the second argument is the return type for zero-argument constants.
from opteryx.expression.functions.registrar import _make  # type: ignore


def get_builtin_constant_functions() -> List:
    """
    Return zero-argument plan-time constant function definitions.

    These functions are folded to literals by the binder at planning time
    (the binder rewrites them to LITERAL nodes). They remain present in the
    function catalog so `is_function()` and name-based validation succeed
    during AST construction.
    """
    return [
        _make("CURRENT_DATE", OrsoTypes.DATE, summary="Current date."),
        _make("CURRENT_TIME", OrsoTypes.TIME, summary="Current time."),
        _make(
            "CURRENT_TIMESTAMP",
            OrsoTypes.TIMESTAMP,
            aliases=("NOW",),
            summary="Current timestamp.",
        ),
        _make("UTC_TIMESTAMP", OrsoTypes.TIMESTAMP, summary="Current UTC timestamp."),
        _make("VERSION", OrsoTypes.VARCHAR, summary="Database version string."),
        _make("CONNECTION_ID", OrsoTypes.INTEGER, summary="Current connection identifier."),
        _make("DATABASE", OrsoTypes.VARCHAR, summary="Current database name."),
        _make("USER", OrsoTypes.VARCHAR, summary="Current user name."),
        _make("PI", OrsoTypes.DOUBLE, summary="Mathematical constant π."),
        _make("PHI", OrsoTypes.DOUBLE, summary="Golden ratio φ."),
        _make("E", OrsoTypes.DOUBLE, summary="Euler's number e."),
    ]


def fixed_value_function(function, context):
    """Get the fixed value for a compile-time constant function.

    Used by the planner to fold constant function calls at planning time.
    """
    if function in ("VERSION",):
        import opteryx

        return OrsoTypes.VARCHAR, opteryx.__version__
    if function in ("NOW", "UTC_TIMESTAMP"):
        return OrsoTypes.TIMESTAMP, numpy.datetime64(context.execution_context.connected_at, "us")
    if function in ("CURRENT_TIME",):
        # CURRENT_TIME is an alias for NOW, so we return the same value
        return OrsoTypes.TIME, context.execution_context.connected_at.time()
    if function in ("CURRENT_TIMESTAMP",):
        # CURRENT_TIMESTAMP is an alias for NOW, so we return the same value
        return OrsoTypes.TIMESTAMP, numpy.datetime64(context.execution_context.connected_at, "us")
    if function in ("CURRENT_DATE", "TODAY"):
        return OrsoTypes.DATE, numpy.datetime64(context.execution_context.connected_at.date(), "D")
    if function in ("YESTERDAY",):
        return OrsoTypes.DATE, numpy.datetime64(
            context.execution_context.connected_at.date() - datetime.timedelta(days=1), "D"
        )
    if function == "CONNECTION_ID":
        return OrsoTypes.INTEGER, context.execution_context.query_id
    if function == "DATABASE":
        return OrsoTypes.VARCHAR, context.execution_context.schema or "DEFAULT"
    if function == "USER":
        return OrsoTypes.VARCHAR, context.execution_context.user or "ANONYMOUS"
    if function == "PI":
        return OrsoTypes.DOUBLE, 3.14159265358979323846264338327950288419716939937510
    if function == "PHI":
        # the golden ratio
        return OrsoTypes.DOUBLE, 1.61803398874989484820458683436563811772030917980576
    if function == "E":
        # eulers number
        return OrsoTypes.DOUBLE, 2.71828182845904523536028747135266249775724709369995
    if function == "UTC_TIMESTAMP":
        # UTC timestamp
        return OrsoTypes.TIMESTAMP, numpy.datetime64(datetime.datetime.now(datetime.UTC), "us")
    if function == "UNIXTIME":
        # We should only ever get here if the function is called without parameters
        return OrsoTypes.INTEGER, context.execution_context.connected_at.timestamp()
    return None, None
