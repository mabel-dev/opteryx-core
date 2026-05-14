# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
# cython: initializedcheck=False

"""Function execution helpers for the expression evaluator.

Cython migration of the former function_execution.py. The kernel call itself
is a Python callable resolved at bind-time; this layer surfaces a clean
error if binding never happened and forwards the *parameters tuple.
"""

from opteryx.exceptions import FunctionExecutionError

# Re-exported for callers that historically imported it from this module.
from opteryx.utils.vector_types import is_draken_vector


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
    return kernel.callable_ref(*parameters)


__all__ = ["apply_bounded_function", "is_draken_vector"]
