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

from __future__ import annotations

from typing import Any

from opteryx.expression.functions import FunctionDefinition
from opteryx.expression.functions import FunctionOverload
from opteryx.expression.functions import KernelSpec
from opteryx.expression.functions import LifecycleSpec
from opteryx.expression.functions import ParameterSpec
from opteryx.expression.functions import ReturnSpec
from orso.types import OrsoTypes


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
    engine: Any = "arrow",
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
      _make("FOO", callable_ref, OrsoTypes.VARCHAR, (ParameterSpec(...),), cost=2.0)
      _make("NOW", OrsoTypes.TIMESTAMP, summary="Current timestamp.")
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


def _coalesce_return_type(arg_nodes) -> OrsoTypes:
    """Return the first non-null compatible type across all args."""
    from orso.types import find_compatible_type

    types = [
        n.schema_column.type
        for n in arg_nodes
        if getattr(n, "schema_column", None) is not None
        and n.schema_column.type not in (OrsoTypes.NULL, 0, OrsoTypes._MISSING_TYPE)
    ]
    return find_compatible_type(types) or OrsoTypes.NULL


def _case_return_type(arg_nodes) -> OrsoTypes:
    """Return the type of the first non-null THEN/ELSE branch.

    CASE node structure: parameters[0] is the scrutinee, parameters[1] is a
    node whose .parameters list holds the WHEN/THEN/ELSE expressions.
    """
    branches = getattr(arg_nodes[1], "parameters", []) if len(arg_nodes) > 1 else []
    for param in branches:
        sc = getattr(param, "schema_column", None)
        if sc is not None and sc.type not in (OrsoTypes.NULL, 0, OrsoTypes._MISSING_TYPE):
            return sc.type
    return OrsoTypes.NULL


def _datepart_return_type(arg_nodes) -> OrsoTypes:
    """EXTRACT/DATEPART return type depends on the part name literal."""
    part_val = getattr(arg_nodes[0], "value", None) if arg_nodes else None
    if part_val is None:
        return OrsoTypes.INTEGER
    part = str(part_val).lower()
    if part in ("epoch", "julian"):
        return OrsoTypes.DOUBLE
    if part == "date":
        return OrsoTypes.DATE
    return OrsoTypes.INTEGER


def get_builtin_functions() -> list[FunctionDefinition]:
    """Aggregate builtin function definitions from registrar submodules.

    Each registrar submodule must expose a getter function returning a list
    of FunctionDefinition objects. Example names:

      - get_builtin_text_functions()
      - get_builtin_text_extended_functions()
      - get_builtin_arithmetic_functions()
      - get_builtin_arithmetic_extended_functions()
      - get_builtin_type_conversion_functions()
      - get_builtin_logical_functions()
      - get_builtin_aggregate_functions()
      - get_builtin_constant_functions()
      - get_builtin_temporal_extra_functions()
      - get_builtin_temporal_functions()
      - get_builtin_utility_functions()
      - get_builtin_hash_encoding_functions()
      - get_builtin_array_misc_functions()

    Using relative imports allows domain modules to import helpers from this
    package without creating import cycles with the top-level native registrar
    (which is being removed).
    """
    functions: list[FunctionDefinition] = []

    # Import registrar domain modules relatively; each module must provide the
    # appropriate getter as noted above.
    from . import aggregate as _agg
    from . import arithmetic as _arith
    from . import arithmetic_extended as _arith_ext
    from . import array_misc as _array
    from . import constant as _const
    from . import hash_encoding as _hash
    from . import logical as _logical
    from . import temporal as _temp
    from . import temporal_extra as _temp_ext
    from . import text as _text
    from . import text_extended as _text_ext
    from . import type_conversion as _type_conv
    from . import utility as _util

    # Collect from each domain getter. Domain modules should implement these
    # functions (or alias them) to return lists of FunctionDefinition objects.
    functions.extend(_text.get_builtin_text_functions())
    functions.extend(_text_ext.get_builtin_text_extended_functions())
    functions.extend(_arith.get_builtin_arithmetic_functions())
    functions.extend(_arith_ext.get_builtin_arithmetic_extended_functions())
    functions.extend(_type_conv.get_builtin_type_conversion_functions())
    functions.extend(_logical.get_builtin_logical_functions())
    functions.extend(_agg.get_builtin_aggregate_functions())
    functions.extend(_const.get_builtin_constant_functions())
    functions.extend(_temp_ext.get_builtin_temporal_extra_functions())
    functions.extend(_temp.get_builtin_temporal_functions())
    functions.extend(_util.get_builtin_utility_functions())
    functions.extend(_hash.get_builtin_hash_encoding_functions())
    functions.extend(_array.get_builtin_array_misc_functions())

    return functions


# Re-export helpers for domain modules to use
__all__ = [
    "_make",
    "_coalesce_return_type",
    "_case_return_type",
    "_datepart_return_type",
    "get_builtin_functions",
]
