"""Function execution helpers for the expression evaluator.

Updated to keep null handling explicit and avoid NumPy-style coercion in the
compression path while preserving the existing kernel execution contract.
"""

from typing import Any

from opteryx.exceptions import FunctionExecutionError


def _is_draken_vector(value) -> bool:
    return value.__class__.__module__.startswith("opteryx.compiled.draken.vectors.")


def _coerce_param_for_draken(p):
    """Validate draken-kernel parameters.

    Column data must already be Draken vectors. Literal scalars are allowed.
    """
    if _is_draken_vector(p):
        return p

    if isinstance(p, (bool, int, float, str, bytes, type(None))):
        return p

    raise FunctionExecutionError(
        message=(
            "Draken kernel received non-Draken column data. "
            f"Expected Draken vector or literal scalar, got {type(p).__name__}."
        ),
        function=None,
    )


def _normalize_null_policy(null_policy: str) -> str:
    """Normalize old null_policy labels to their new semantic equivalents."""
    if null_policy == "strict":
        return "compress"
    if null_policy == "custom":
        return "bypass"
    if null_policy == "passthrough":
        return "passthru"
    return null_policy


def apply_bounded_function(node, *parameters) -> Any:
    """Apply a bound FUNCTION node to its already-evaluated parameters."""
    func_ref = node.function_ref
    if func_ref is None:
        raise FunctionExecutionError(
            message=f"Function '{node.value}' was not bound — function_ref is None.",
            function=node.value,
        )

    kernel = func_ref.selected_overload.kernel
    engine = kernel.engine

    if engine is None:
        raise FunctionExecutionError(
            message=("KernelSpec.engine is required; expected 'draken'."),
            function=node.value,
        )

    null_policy = _normalize_null_policy(kernel.null_policy)

    compressed = False
    valid_positions = ()
    morsel_size = 0
    if (
        null_policy == "compress"
        and len(parameters) > 0
        and not isinstance(parameters[0], int)
        and all(hasattr(arr, "ndim") and arr.ndim == 1 for arr in parameters)
    ):
        morsel_size = len(parameters[0])
        null_positions = None

        for arr in parameters:
            if not hasattr(arr, "is_null"):
                continue

            arr_nulls = arr.is_null()
            if not hasattr(arr_nulls, "__len__"):
                raise FunctionExecutionError(
                    message=(
                        "Function compression requires null-aware vector inputs; "
                        f"received unsupported null result from {type(arr).__name__}."
                    ),
                    function=node.value,
                )

            null_positions = arr_nulls if null_positions is None else null_positions | arr_nulls

        if null_positions is not None and null_positions.all():
            return [None] * morsel_size

        if null_positions is not None and null_positions.any():
            valid_positions = ~null_positions
            parameters = tuple(arr.compress(valid_positions) for arr in parameters)
            compressed = True

    if engine == "draken":
        parameters = tuple(_coerce_param_for_draken(p) for p in parameters)
    else:
        raise FunctionExecutionError(
            message=(
                f"Unknown kernel engine '{engine}' for function '{node.value}'. "
                "Expected: 'draken'."
            ),
            function=node.value,
        )

    try:
        result = kernel.callable_ref(*parameters)
    except FunctionExecutionError as e:
        raise e
    except Exception as e:
        raise FunctionExecutionError(message=str(e), function=node.value) from e

    if compressed:
        out = [None] * morsel_size
        result_iter = iter(result)
        for idx, is_valid in enumerate(valid_positions):
            if is_valid:
                out[idx] = next(result_iter)
        return out

    return result


__all__ = ["apply_bounded_function"]
