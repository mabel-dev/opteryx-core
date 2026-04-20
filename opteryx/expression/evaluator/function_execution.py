"""Function execution helpers for the expression evaluator."""

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

    result = kernel.callable_ref(*parameters)
    return result


__all__ = ["apply_bounded_function"]
