"""Arithmetic kernel registry for Draken vectors.

Maps (left_type, right_type, op_string) → callable(left, right) → Vector.

New draken: all vectors are the unified Vector class with native arithmetic
methods (add, sub, mul, div, mod, neg). The kernel registry delegates to
these methods for all numeric type combinations.
"""

from opteryx.utils.vector_types import VectorType

# Operator string → Vector method name
_OP_TO_METHOD = {
    "Plus": "add",
    "Minus": "sub",
    "Multiply": "mul",
    "Divide": "div",
    "Modulo": "mod",
}

# Type pairs that support numeric arithmetic
_NUMERIC = frozenset((
    VectorType.INT64,
    VectorType.INTEGER,
    VectorType.FLOAT64,
    VectorType.DECIMAL,
))


def _unwrap(v):
    """Extract the nanobind Vector from a Cython shim, or return as-is if already nanobind."""
    nb = getattr(v, "_nb", None)
    return nb if nb is not None else v


def _make_kernel(method_name):
    """Return a kernel that calls vec.method_name(other)."""
    def kernel(left, right):
        left_nb = _unwrap(left)
        right_nb = _unwrap(right)
        method = getattr(left_nb, method_name, None)
        if method is not None:
            return method(right_nb)
        method = getattr(right_nb, method_name, None)
        if method is not None:
            return method(left_nb)
        return None
    return kernel


# Pre-build kernels for each op
_KERNELS = {op: _make_kernel(method) for op, method in _OP_TO_METHOD.items()}


def get_arithmetic_kernel(left_type, right_type, op):
    """Return a kernel callable for the given type pair and op, or None if unsupported.

    Parameters
    ----------
    left_type : VectorType or None
    right_type : VectorType or None
    op : str
        Operator name (e.g. "Plus", "Minus", "Multiply", "Divide", "Modulo").

    Returns
    -------
    callable or None
    """
    if op not in _KERNELS:
        return None

    # Both sides must be numeric (or one may be None = scalar deferred type)
    left_ok = left_type is None or left_type in _NUMERIC
    right_ok = right_type is None or right_type in _NUMERIC

    if not (left_ok and right_ok):
        return None

    return _KERNELS[op]
