from __future__ import annotations

from typing import List

# `_make` is the registrar package-level helper which constructs a
# FunctionDefinition with a single overload. Use the shorthand form where
# the second argument is the return type for zero-argument constants.
from opteryx.expression.functions.registrar import _make  # type: ignore
from orso.types import OrsoTypes


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
