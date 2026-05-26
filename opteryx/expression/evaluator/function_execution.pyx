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
    callable_obj = kernel.callable_ref
    if type(callable_obj).__name__ != "nb_func":
        return callable_obj(*parameters)

    # Nanobind callable: unwrap any Cython Vector shims → raw nanobind Vectors.
    unwrapped = []
    for p in parameters:
        nb = getattr(p, "_nb", None)
        unwrapped.append(nb if nb is not None else p)
    result = callable_obj(*unwrapped)

    # Nanobind callables return raw nanobind Vectors — wrap in Cython shim.
    if type(result).__name__ == "Vector":
        import draken.draken_native as _dn
        if result.type == _dn.DrakenType.BOOL:
            from draken.vectors.bool_vector import BoolVector
            return BoolVector(result)
        from draken.vectors.vector import Vector
        return Vector(result)
    return result


__all__ = ["apply_bounded_function", "is_draken_vector"]
