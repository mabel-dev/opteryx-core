"""Function execution helpers for the expression evaluator.

Updated to keep null handling explicit and avoid NumPy-style coercion in the
compression path while preserving the existing kernel execution contract.
"""

from typing import Any

import pyarrow as _pa

from opteryx.compiled.draken.vectors.scalar_constructors import from_scalar as _const_scalar
from opteryx.exceptions import FunctionExecutionError


def _is_draken_vector(value) -> bool:
    return value.__class__.__module__.startswith("opteryx.compiled.draken.vectors.")


def _coerce_param_for_kernel(p, pa):
    """Convert a Draken vector to a PyArrow array for kernel dispatch."""
    if not _is_draken_vector(p):
        return p
    arr = p.to_arrow()
    if pa.types.is_dictionary(arr.type):
        arr = arr.cast(arr.type.value_type)
    if pa.types.is_binary(arr.type) or pa.types.is_large_binary(arr.type):
        arr = arr.cast(pa.utf8())
    return arr


def _coerce_param_for_draken(p):
    """Coerce inputs into native Draken vectors for draken kernels."""
    if _is_draken_vector(p):
        return p

    if isinstance(p, (_pa.Array, _pa.ChunkedArray)):
        from opteryx.compiled.draken.interop.arrow import vector_from_arrow

        return vector_from_arrow(p)

    if hasattr(p, "as_py") and not isinstance(p, (bytes, str)):
        try:
            p = p.as_py()
        except Exception:
            pass

    # Fast-path plain Python scalars before sequence coercion.
    if isinstance(p, bool):
        return p

    if isinstance(p, (int, float, str, bytes, type(None))):
        vec = _const_scalar(p, 1)
        if vec is not None:
            return vec

    if isinstance(p, (list, tuple)):
        from opteryx.compiled.draken.interop.arrow import vector_from_sequence

        try:
            return vector_from_sequence(p)
        except Exception as e:
            raise FunctionExecutionError(
                message=(
                    "Failed to coerce list/tuple to Draken vector for draken kernel. "
                    f"Inner error: {e}"
                ),
                function=None,
            )

    return p


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
            message=("KernelSpec.engine is required; please specify one of: 'arrow', 'draken'."),
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

    if engine == "arrow":
        import pyarrow as _pa_abf

        parameters = tuple(_coerce_param_for_kernel(p, _pa_abf) for p in parameters)
    elif engine == "draken":
        parameters = tuple(_coerce_param_for_draken(p) for p in parameters)
    elif engine == "python":
        pass
    else:
        raise FunctionExecutionError(
            message=(
                f"Unknown kernel engine '{engine}' for function '{node.value}'. "
                "Expected one of: 'arrow', 'draken', 'python'."
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
