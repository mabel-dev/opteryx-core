# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
# cython: initializedcheck=False

"""
Registrar package initializer.

Holds shared registrar helper functions and the top-level collector
`get_builtin_functions()` that aggregates per-domain registrar modules.

Domain modules (text, arithmetic, utility, ...) are expected to provide
a getter function returning a list[FunctionDefinition], for example:

    def get_builtin_text_functions() -> list[FunctionDefinition]:
        ...

This module exposes helpers so domain modules can import them as:

    from opteryx.expression.functions.registrar import _make, _coalesce_return_type
"""

from typing import Any

from opteryx.expression.functions import (
    FunctionDefinition,
    FunctionOverload,
    KernelSpec,
    LifecycleSpec,
    ParameterSpec,
    ReturnSpec,
)
from opteryx.types.logical_type import LogicalCategory
from opteryx.types.logical_type import (
    BOOLEAN as _CT_BOOLEAN,
    INT64 as _CT_INT64,
    FLOAT64 as _CT_FLOAT64,
    VARCHAR as _CT_VARCHAR,
    NVARCHAR as _CT_NVARCHAR,
    VARBINARY as _CT_VARBINARY,
    DATE as _CT_DATE,
    INTERVAL as _CT_INTERVAL,
    VARIANT as _CT_VARIANT,
    NULL as _CT_NULL,
    TIMESTAMP as _CT_TIMESTAMP,  # factory: _CT_TIMESTAMP() → ColumnType
    TIME as _CT_TIME,            # factory: _CT_TIME() → ColumnType
    ARRAY as _CT_ARRAY,          # factory: _CT_ARRAY(element) → ColumnType
)


# Kernel decorators for common iteration patterns
def _iterate_single_parameter(func):
    """Decorator for functions that iterate over a single array parameter."""

    def _inner(array):
        return list(map(func, array))

    return _inner


def _sort(func):
    """Decorator for sort/ordering functions that process arrays."""

    def _inner(array):
        return [func(item) for item in array]

    return _inner


def _iterate_double_parameter(func):
    """For functions called FUNCTION(field, literal)."""

    def _inner(array, literal):
        if isinstance(array, str):
            array = [array]
        return [func(item, literal[index]) for index, item in enumerate(array)]

    return _inner


def _iterate_double_parameter_swapped(func):
    """For functions called FUNCTION(literal, field) when planner supplies (literal_values, field_values)."""

    def _inner(array, literal):
        if isinstance(array, str):
            array = [array]
        return [func(literal[index], item) for index, item in enumerate(array)]

    return _inner


def _make(
    name: str,
    callable_or_ret: Any,
    maybe_ret: Any = None,
    params: tuple = (),
    *,
    aliases: tuple = (),
    category: str = "misc",
    volatility: Any = "immutable",
    deterministic: bool | None = None,
    lifecycle: LifecycleSpec | None = None,
    engine: Any = "draken",
    kernel_id: str = "default",
    id_suffix: str = "default",
    null_policy: Any = "compress",
    cost: float = 1.0,
    summary: str = "",
    documentation: str | None = None,
) -> FunctionDefinition:
    """
    Central helper to construct a FunctionDefinition with a single overload.

    Accepts either the (callable_ref, ret, params, ...) form or the
    shorthand (ret, ...) form used for zero-arg/constant definitions.

    Usage examples:
      _make("FOO", callable_ref, LogicalCategory.VARCHAR, (ParameterSpec(...),), cost=2.0)
      _make("NOW", LogicalCategory.TIMESTAMP, summary="Current timestamp.")
    """
    # Distinguish calling form: callable_or_ret is a callable -> full form,
    # otherwise it's the return-type shorthand.
    if callable(callable_or_ret):
        callable_ref = callable_or_ret
        ret = maybe_ret
        params = params or ()
    else:
        callable_ref = lambda *a: None
        ret = callable_or_ret
        params = () if maybe_ret is None else maybe_ret

    if deterministic is None:
        deterministic = volatility == "immutable"

    # Ensure `deterministic` is a plain bool before constructing FunctionDefinition.
    deterministic = bool(deterministic)
    assert isinstance(deterministic, bool)

    if lifecycle is None:
        lifecycle = LifecycleSpec(status="active")

    overload_id = f"{name}_{id_suffix}" if id_suffix else f"{name}_{kernel_id}"

    return FunctionDefinition(
        name=name,
        aliases=aliases,
        category=category,
        volatility=volatility,
        deterministic=deterministic,
        lifecycle=lifecycle,
        summary=summary or name,
        documentation=documentation or summary or name,
        overloads=(
            FunctionOverload(
                id=overload_id,
                parameters=params,
                return_spec=ReturnSpec(mode="fixed", fixed_type=ret),
                kernel=KernelSpec(
                    engine=engine,
                    id=kernel_id or "default",
                    callable_ref=callable_ref,
                    null_policy=null_policy,
                    cost_us_per_million=cost,
                ),
            ),
        ),
    )


def _coalesce_return_type(arg_nodes):
    """Return the first non-null compatible ColumnType across all args."""
    from opteryx.types import find_compatible_type

    column_types = [
        n.schema_column.column_type
        for n in arg_nodes
        if getattr(n, "schema_column", None) is not None
        and n.schema_column.column_type is not None
        and n.schema_column.column_type.category not in (LogicalCategory.NULL, None)
    ]
    return find_compatible_type(column_types) or _CT_NULL


def _datepart_return_type(arg_nodes):
    """EXTRACT/DATEPART return type depends on the part name literal."""
    part_val = getattr(arg_nodes[0], "value", None) if arg_nodes else None
    if part_val is None:
        return _CT_INT64
    part = str(part_val).lower()
    if part in ("epoch", "julian"):
        return _CT_FLOAT64
    if part == "date":
        return _CT_DATE
    return _CT_INT64


# ---------------------------------------------------------------------------
# Consolidated leaf includes.
#
# The per-domain registrar files (aggregate / arithmetic / text / …) are
# textually included here so the package compiles to a single .so. Their
# getter functions land in this module's namespace, so get_builtin_functions
# below can call them directly without intermediate module objects.
# Order isn't significant for these — none reference each other.
# ---------------------------------------------------------------------------
include "aggregate.pyx"
include "arithmetic.pyx"
include "constant.pyx"
include "hash_encoding.pyx"
include "logical.pyx"
include "temporal.pyx"
include "temporal_extra.pyx"
include "text.pyx"
include "utility.pyx"
# `get_builtin_array_misc_functions` is defined directly in utility.pyx;
# the former array_misc.pyx forwarder was redundant and has been removed.


def get_builtin_functions() -> list[FunctionDefinition]:
    """Aggregate builtin function definitions across all registrar domains.

    Each domain's getter is defined directly in this module via the include
    statements above; no submodule round-trip is needed.
    """
    functions: list[FunctionDefinition] = []
    functions.extend(get_builtin_text_functions())
    functions.extend(get_builtin_text_extended_functions())
    functions.extend(get_builtin_arithmetic_functions())
    functions.extend(get_builtin_arithmetic_extended_functions())
    functions.extend(get_builtin_logical_functions())
    functions.extend(get_builtin_aggregate_functions())
    functions.extend(get_builtin_constant_functions())
    functions.extend(get_builtin_temporal_extra_functions())
    functions.extend(get_builtin_temporal_functions())
    functions.extend(get_builtin_utility_functions())
    functions.extend(get_builtin_hash_encoding_functions())
    functions.extend(get_builtin_array_misc_functions())
    return functions


# Re-export helpers for domain modules to use
__all__ = [
    "_coalesce_return_type",
    "_datepart_return_type",
    "_iterate_double_parameter",
    "_iterate_double_parameter_swapped",
    "_iterate_single_parameter",
    "_make",
    "_sort",
    "get_builtin_functions",
]
