from __future__ import annotations

from typing import List

import numpy

# Local implementation imports (kept as late imports inside function if heavy)
from opteryx.compiled.vector_ops import vector_iif as _vector_iif
from opteryx.expression.functions import FunctionDefinition
from opteryx.expression.functions import FunctionOverload
from opteryx.expression.functions import KernelSpec
from opteryx.expression.functions import LifecycleSpec
from opteryx.expression.functions import ParameterSpec
from opteryx.expression.functions import ReturnSpec
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
from opteryx.expression.functions.registrar import _case_return_type
from opteryx.expression.functions.registrar import _coalesce_return_type
from orso.types import OrsoTypes


def _coalesce(*arrays):
    """
    Element-wise coalesce function for multiple numpy arrays.
    Selects the first non-None item in each row across the input arrays.

    Parameters:
        arrays: tuple of numpy arrays

    Returns:
        numpy array with coalesced values
    """
    # Start with an array full of None values
    result = numpy.array(arrays[0], dtype=object)

    mask = result == None

    for arr in arrays[1:]:
        mask = numpy.array([None if value != value else value for value in result]) == None
        numpy.copyto(result, arr, where=mask)

    return result


def select_values(boolean_arrays, value_arrays):
    """
    Build a result array based on boolean conditions and corresponding value arrays.

    Parameters:
    - boolean_arrays: List[np.ndarray], list of boolean arrays representing conditions.
    - value_arrays: List[np.ndarray], list of arrays with values corresponding to each condition.

    Returns:
    - np.ndarray: Result array with selected values or False where no condition is met.
    """

    def _to_numpy_condition(values, target_length):
        if hasattr(values, "to_numpy"):
            arr = values.to_numpy(zero_copy_only=False)
        elif hasattr(values, "to_pylist"):
            arr = numpy.asarray(values.to_pylist(), dtype=object)
        elif isinstance(values, (list, tuple, numpy.ndarray)):
            arr = numpy.asarray(values)
        else:
            arr = numpy.full(target_length, bool(values), dtype=bool)

        if arr.shape == ():
            arr = numpy.full(target_length, bool(arr.item()), dtype=bool)
        elif len(arr) == 1 and target_length != 1:
            arr = numpy.full(target_length, bool(arr[0]), dtype=bool)
        return arr.astype(bool, copy=False)

    def _to_numpy_values(values, target_length):
        if hasattr(values, "to_numpy"):
            arr = values.to_numpy(zero_copy_only=False)
        elif hasattr(values, "to_pylist"):
            arr = numpy.asarray(values.to_pylist(), dtype=object)
        elif isinstance(values, numpy.ndarray):
            arr = values
        elif isinstance(values, (list, tuple)):
            arr = numpy.asarray(values, dtype=object)
        else:
            arr = numpy.full(target_length, values, dtype=object)

        if arr.shape == ():
            arr = numpy.full(target_length, arr.item(), dtype=object)
        elif len(arr) == 1 and target_length != 1:
            arr = numpy.full(target_length, arr[0], dtype=object)
        return arr

    # Ensure the input lists are not empty and have the same length
    if not boolean_arrays or not value_arrays or len(boolean_arrays) != len(value_arrays):
        raise ValueError("Input lists must be non-empty and of the same length.")

    first_condition = boolean_arrays[0]
    if hasattr(first_condition, "__len__"):
        target_length = len(first_condition)
    elif hasattr(first_condition, "to_pylist"):
        target_length = len(first_condition.to_pylist())
    else:
        target_length = 1

    # Initialize the result array with False, assuming no condition will be met
    result = numpy.full(target_length, None, dtype=object)

    # Iterate over pairs of boolean and value arrays
    for condition, values in zip(reversed(boolean_arrays), reversed(value_arrays)):
        # Update the result array where the condition is True
        numpy.putmask(
            result,
            _to_numpy_condition(condition, target_length),
            _to_numpy_values(values, target_length),
        )

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
                        engine="arrow",
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
                        engine="arrow",
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
                        engine="arrow",
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
                        engine="arrow",
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
                        engine="arrow",
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
                        engine="arrow",
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
