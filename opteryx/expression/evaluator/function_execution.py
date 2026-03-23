"""Function execution helpers for the expression evaluator."""

from typing import Any

import numpy
import pyarrow as _pa
import pyarrow.compute as compute
from opteryx.exceptions import FunctionExecutionError


def _is_draken_vector(value) -> bool:
    return value.__class__.__module__.startswith("opteryx.draken.vectors.")


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
        from opteryx.draken.interop.arrow import vector_from_arrow

        return vector_from_arrow(p)

    if hasattr(p, "as_py") and not isinstance(p, (bytes, str)):
        try:
            p = p.as_py()
        except Exception:
            pass

    try:
        import numpy as np

        if isinstance(p, np.generic):
            p = p.item()
        elif isinstance(p, np.ndarray):
            p = p.item() if p.ndim == 0 else p.tolist()
    except Exception:
        pass

    if isinstance(p, (list, tuple)):
        from opteryx.draken.interop.arrow import vector_from_sequence

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

    from opteryx.draken.vectors.scalar_constructors import from_scalar as _const_scalar

    if isinstance(p, (bool, int, float, str, bytes, type(None))):
        vec = _const_scalar(p, 1)
        if vec is not None:
            return vec

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
    if (
        null_policy == "compress"
        and len(parameters) > 0
        and not isinstance(parameters[0], int)
        and all(isinstance(arr, numpy.ndarray) for arr in parameters)
        and all(arr.ndim == 1 for arr in parameters)
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
        raise FunctionExecutionError(message=e, function=node.value) from e

    if isinstance(result, list):
        result = numpy.array(result)

    if compressed:
        out = numpy.full(morsel_size, None, dtype=object)
        numpy.place(out, valid_positions, result)
        return out

    return result


__all__ = ["apply_bounded_function"]
