from __future__ import annotations

import math as _math
from typing import List

# Local implementation imports (kept as late imports inside function if heavy)
from opteryx.compiled.vector_ops import vector_iif as _vector_iif
from opteryx.expression.functions import (
    FunctionDefinition,
    FunctionOverload,
    KernelSpec,
    LifecycleSpec,
    ParameterSpec,
    ReturnSpec,
)
from opteryx.expression.functions.implementations.logical import (
    array_contains as _lf_array_contains,
)
from opteryx.expression.functions.implementations.logical import if_null as _lf_if_null
from opteryx.expression.functions.implementations.logical import null_if as _lf_null_if
from opteryx.expression.functions.implementations.utility import (
    cosine_similarity as _lf_cosine_similarity,
)
from opteryx.expression.functions.implementations.utility import humanize as _lf_humanize
from opteryx.expression.functions.implementations.utility import (
    jsonb_object_keys as _lf_jsonb_object_keys,
)

# Local helpers provided by registrar package
from opteryx.expression.functions.registrar import _case_return_type, _coalesce_return_type
from opteryx.types import OrsoTypes


def _coalesce(*arrays):
    """
    Element-wise coalesce: return the first non-null value across arrays.
    Treats Python None and float NaN as null.
    """

    def _is_null_val(v):
        if v is None:
            return True
        if isinstance(v, float) and _math.isnan(v):
            return True
        return False

    def to_pylist(a):
        if hasattr(a, "to_pylist"):
            return a.to_pylist()
        if isinstance(a, list):
            return a
        return list(a)

    lists = [to_pylist(a) for a in arrays]
    n = max(len(lst) for lst in lists)
    result = [None] * n

    for lst in lists:
        broadcast = len(lst) == 1
        for i in range(n):
            if _is_null_val(result[i]):
                val = lst[0] if broadcast else lst[i]
                if not _is_null_val(val):
                    result[i] = val

    return result


def select_values(boolean_arrays, value_arrays):
    """
    Build a result array based on CASE conditions and corresponding value arrays.
    Conditions are evaluated in reverse priority order (last wins → applied first).

    Parameters:
    - boolean_arrays: list of boolean arrays/vectors representing conditions.
    - value_arrays:   list of value arrays/vectors corresponding to each condition.

    Returns:
    - list: result values, None where no condition matched.
    """
    if not boolean_arrays or not value_arrays or len(boolean_arrays) != len(value_arrays):
        raise ValueError("Input lists must be non-empty and of the same length.")

    def _to_bool_list(v, n):
        if hasattr(v, "to_pylist"):
            lst = v.to_pylist()
        elif isinstance(v, list):
            lst = v
        else:
            lst = [bool(v)] * n
        if len(lst) == 1 and n != 1:
            lst = lst * n
        return [bool(b) if b is not None else False for b in lst]

    def _to_val_list(v, n):
        if hasattr(v, "to_pylist"):
            lst = v.to_pylist()
        elif isinstance(v, list):
            lst = v
        else:
            lst = [v] * n
        if len(lst) == 1 and n != 1:
            lst = lst * n
        return lst

    first_condition = boolean_arrays[0]
    if hasattr(first_condition, "__len__") and not isinstance(first_condition, (str, bytes)):
        n = len(first_condition)
    elif hasattr(first_condition, "to_pylist"):
        n = len(first_condition.to_pylist())
    else:
        n = 1

    result = [None] * n

    for condition, values in zip(reversed(boolean_arrays), reversed(value_arrays)):
        cond = _to_bool_list(condition, n)
        vals = _to_val_list(values, n)
        for i in range(n):
            if cond[i]:
                result[i] = vals[i]

    return result


def get_builtin_logical_functions() -> List[FunctionDefinition]:
    """
    Logical and control-flow function registrar entries.

    Provides:
      - COALESCE (variadic, resolver-based return)
      - IFNULL / IFNOTNULL
      - NULLIF
      - IIF (vectorized conditional)
      - _PASSTHRU (utility, for tests/compat)
      - _CASE (variadic CASE expression)
    """

    # Small adapter object bundling kernels implemented elsewhere
    class other_functions:
        array_contains = staticmethod(_lf_array_contains)
        if_null = staticmethod(_lf_if_null)
        null_if = staticmethod(_lf_null_if)
        cosine_similarity = staticmethod(_lf_cosine_similarity)
        humanize = staticmethod(_lf_humanize)
        jsonb_object_keys = staticmethod(_lf_jsonb_object_keys)

    _coalesce_kernel = _coalesce
    _iif_kernel = _vector_iif
    _case_kernel = select_values

    _variadic_any = (
        ParameterSpec(name="arg0", type_family="any"),
        ParameterSpec(name="args", type_family="any", variadic=True, optional=True),
    )

    return [
        FunctionDefinition(
            name="COALESCE",
            aliases=(),
            category="logical",
            volatility="immutable",
            deterministic=True,
            lifecycle=LifecycleSpec(status="active"),
            summary="Return first non-null argument.",
            documentation="Returns the first non-null value from the list of arguments.",
            overloads=(
                FunctionOverload(
                    id="COALESCE_variadic",
                    parameters=_variadic_any,
                    return_spec=ReturnSpec(mode="resolver", resolver=_coalesce_return_type),
                    kernel=KernelSpec(
                        engine="draken",
                        id="default",
                        callable_ref=_coalesce_kernel,
                        null_policy="passthru",
                        cost_us_per_million=15852.0,
                    ),
                ),
            ),
        ),
        FunctionDefinition(
            name="IFNULL",
            aliases=(),
            category="logical",
            volatility="immutable",
            deterministic=True,
            lifecycle=LifecycleSpec(status="active"),
            summary="Return value if not null, else default.",
            documentation="Returns first argument if not null, otherwise returns second argument.",
            overloads=(
                FunctionOverload(
                    id="IFNULL_1",
                    parameters=(
                        ParameterSpec(name="value", type_family="any"),
                        ParameterSpec(name="default", type_family="any"),
                    ),
                    return_spec=ReturnSpec(mode="resolver", resolver=_coalesce_return_type),
                    kernel=KernelSpec(
                        engine="draken",
                        id="default",
                        callable_ref=other_functions.if_null,
                        null_policy="passthru",
                        cost_us_per_million=1.53,
                    ),
                ),
            ),
        ),
        FunctionDefinition(
            name="IFNOTNULL",
            aliases=(),
            category="logical",
            volatility="immutable",
            deterministic=True,
            lifecycle=LifecycleSpec(status="active"),
            summary="Return second argument if first is not null.",
            documentation="Returns second argument if first argument is not null, otherwise null.",
            overloads=(
                FunctionOverload(
                    id="IFNOTNULL_1",
                    parameters=(
                        ParameterSpec(name="value", type_family="any"),
                        ParameterSpec(name="result", type_family="any"),
                    ),
                    return_spec=ReturnSpec(mode="resolver", resolver=_coalesce_return_type),
                    kernel=KernelSpec(
                        engine="draken",
                        id="default",
                        callable_ref=other_functions.if_null,  # same kernel, semantics handled by evaluator
                        null_policy="passthru",
                        cost_us_per_million=0.74,
                    ),
                ),
            ),
        ),
        FunctionDefinition(
            name="NULLIF",
            aliases=(),
            category="logical",
            volatility="immutable",
            deterministic=True,
            lifecycle=LifecycleSpec(status="active"),
            summary="Return null if equal, else first value.",
            documentation="Returns null if arguments are equal, otherwise returns first argument.",
            overloads=(
                FunctionOverload(
                    id="NULLIF_1",
                    parameters=(
                        ParameterSpec(name="value", type_family="any"),
                        ParameterSpec(name="compare", type_family="any"),
                    ),
                    return_spec=ReturnSpec(mode="same_as_arg", arg_index=0),
                    kernel=KernelSpec(
                        engine="draken",
                        id="default",
                        callable_ref=other_functions.null_if,
                        cost_us_per_million=0.72,
                    ),
                ),
            ),
        ),
        FunctionDefinition(
            name="IIF",
            aliases=(),
            category="logical",
            volatility="immutable",
            deterministic=True,
            lifecycle=LifecycleSpec(status="active"),
            summary="Inline if: return second or third arg based on condition.",
            documentation="Returns second argument if condition is true, otherwise third argument.",
            overloads=(
                FunctionOverload(
                    id="IIF_1",
                    parameters=(
                        ParameterSpec(name="condition", type_family="boolean"),
                        ParameterSpec(name="true_value", type_family="any"),
                        ParameterSpec(name="false_value", type_family="any"),
                    ),
                    return_spec=ReturnSpec(mode="same_as_arg", arg_index=1),
                    kernel=KernelSpec(
                        engine="draken",
                        id="default",
                        callable_ref=_iif_kernel,
                        null_policy="bypass",
                        cost_us_per_million=1.38,
                    ),
                ),
            ),
        ),
        FunctionDefinition(
            name="_PASSTHRU",
            aliases=(),
            category="logical",
            volatility="immutable",
            deterministic=True,
            lifecycle=LifecycleSpec(status="active"),
            summary="Return input unchanged.",
            documentation="Returns the input value unchanged. Used for testing and compatibility.",
            overloads=(
                FunctionOverload(
                    id="_PASSTHRU_1",
                    parameters=(ParameterSpec(name="value", type_family="any"),),
                    return_spec=ReturnSpec(mode="same_as_arg", arg_index=0),
                    kernel=KernelSpec(
                        engine="draken",
                        id="default",
                        callable_ref=lambda x: x,
                        cost_us_per_million=0.28,
                    ),
                ),
            ),
        ),
        FunctionDefinition(
            name="_CASE",
            aliases=(),
            category="logical",
            volatility="immutable",
            deterministic=True,
            lifecycle=LifecycleSpec(status="active"),
            summary="Conditional value selection.",
            documentation="Returns a value based on conditional expressions.",
            overloads=(
                FunctionOverload(
                    id="_CASE_variadic",
                    parameters=_variadic_any,
                    return_spec=ReturnSpec(mode="resolver", resolver=_case_return_type),
                    kernel=KernelSpec(
                        engine="draken",
                        id="default",
                        callable_ref=_case_kernel,
                        null_policy="passthru",
                        cost_us_per_million=1.04,
                    ),
                ),
            ),
        ),
    ]


__all__ = ["get_builtin_logical_functions"]
