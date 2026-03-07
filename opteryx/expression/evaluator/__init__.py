"""Expression evaluator: hotpath for function execution.

The evaluator executes bound function expressions with minimal dispatch overhead.
The binder attaches a ResolvedFunction reference (node.function_ref) to each FUNCTION node;
the evaluator uses that to dispatch directly to the kernel, bypassing all name-based lookup.
"""

from typing import Any

import numpy
import pyarrow.compute as compute

from opteryx.exceptions import FunctionExecutionError


def apply_bounded_function(node, *parameters) -> Any:
    """Apply a bound FUNCTION node to its already-evaluated parameters.

    Uses node.function_ref (set by binder) for kernel dispatch and null policy.

    Null policy (kernel.null_policy):
        "strict"      — strip null rows before calling the kernel and fill them back after.
                        Fast path for functions that return NULL on any NULL input.
        "passthrough" — pass all rows including nulls; the kernel handles nulls itself.
                        Required for COALESCE, CASE, IIF, IFNULL, CONCAT, SUBSTRING, etc.
        "custom"      — reserved for kernels with bespoke null handling logic (not yet used).
    """
    func_ref = getattr(node, "function_ref", None)
    if func_ref is None:
        raise FunctionExecutionError(
            message=f"Function '{node.value}' was not bound — function_ref is None.",
            function=node.value,
        )

    kernel = func_ref.selected_overload.kernel

    compressed = False
    if (
        kernel.null_policy == "strict"
        and len(parameters) > 0
        and not isinstance(parameters[0], int)
        and all(isinstance(arr, numpy.ndarray) for arr in parameters)
    ):
        morsel_size = len(parameters[0])
        null_positions = numpy.zeros(morsel_size, dtype=numpy.bool_)

        for arr in parameters:
            if arr.dtype.kind == "f":
                null_positions = numpy.logical_or(
                    null_positions, compute.is_null(arr, nan_is_null=True)
                )
            else:
                null_positions = numpy.logical_or(null_positions, compute.is_null(arr))

        if null_positions.all():
            return numpy.full(morsel_size, None, dtype=object)

        if null_positions.any():
            valid_positions = ~null_positions
            parameters = [arr.compress(valid_positions) for arr in parameters]
            compressed = True

    try:
        result = kernel.callable_ref(*parameters)
    except FunctionExecutionError as e:
        raise e
    except Exception as e:
        raise FunctionExecutionError(message=e, function=node.value) from e

    if isinstance(result, list):
        result = numpy.array(result)

    if compressed:
        out = numpy.full(morsel_size, None, dtype=object)
        numpy.place(out, valid_positions, result)
        return out

    return result
