"""Function execution helpers for the expression evaluator."""

from typing import Any

from opteryx.exceptions import FunctionExecutionError
from opteryx.utils.vector_types import is_draken_vector


def apply_bounded_function(node, *parameters) -> Any:
    """Apply a bound FUNCTION node to its already-evaluated parameters."""
    func_ref = node.function_ref
    if func_ref is None:
        raise FunctionExecutionError(
            message=f"Function '{node.value}' was not bound — function_ref is None.",
            function=node.value,
        )

    kernel = func_ref.selected_overload.kernel
    result = kernel.callable_ref(*parameters)
    return result


__all__ = ["apply_bounded_function", "is_draken_vector"]
