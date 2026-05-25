"""Function execution helpers for the expression evaluator.

Cython migration of the former function_execution.py. The kernel call itself
is a Python callable resolved at bind-time; this layer surfaces a clean
error if binding never happened and forwards the *parameters tuple.
"""

from opteryx.exceptions import FunctionExecutionError

# Re-exported for callers that historically imported it from this module.
from opteryx.utils.vector_types import is_draken_vector

from draken.vectors.vector import Vector as _ShimVectorBase
from draken.draken_native import Vector as _NbVectorBase
from draken.vectors.scalar_constructors import wrap_nb_vector as _wrap_nb_vector


def apply_bounded_function(node, *parameters):
    """Apply a bound FUNCTION node to its already-evaluated parameters.

    `node` must be a FUNCTION carrying a non-None `function_ref` (set by the
    binder). Fail fast if binding never happened — silent fallback would
    mask a planner bug.
    """
    func_ref = node.function_ref
    if func_ref is None:
        raise FunctionExecutionError(
            message=f"Function '{node.value}' was not bound — function_ref is None.",
            function=node.value,
        )

    kernel = func_ref.selected_overload.kernel
    unwrapped = tuple(p._nb if isinstance(p, _ShimVectorBase) else p for p in parameters)
    result = kernel.callable_ref(*unwrapped)
    if isinstance(result, _NbVectorBase):
        return _wrap_nb_vector(result)
    return result


__all__ = ["apply_bounded_function", "is_draken_vector"]
