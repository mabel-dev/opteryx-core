# cython: language_level=3
# Arithmetic kernel dispatch shim — E.24.

from draken.vectors.vector import Vector as _ShimVectorBase
from draken.draken_native import Vector as _NbVectorBase
from draken.vectors.scalar_constructors import wrap_nb_vector as _wrap_nb_vector


cdef _nb(v):
    """Extract raw nanobind vector from Cython shim or return as-is."""
    if isinstance(v, _ShimVectorBase):
        return v._nb
    return v


cdef _wrap(v):
    """Re-wrap raw nanobind vector result in typed shim if needed."""
    if isinstance(v, _NbVectorBase):
        return _wrap_nb_vector(v)
    return v


def _make_kernel(method_name):
    def kernel(left, right):
        nb_left = _nb(left)
        nb_right = _nb(right)
        return _wrap(getattr(nb_left, method_name)(nb_right))
    return kernel


_ARITHMETIC_KERNELS = {
    "Plus":            _make_kernel("add"),
    "Minus":           _make_kernel("sub"),
    "Multiply":        _make_kernel("mul"),
    "Divide":          _make_kernel("div"),
    "Modulo":          _make_kernel("mod"),
    "MyIntegerDivide": _make_kernel("div"),
}


cpdef get_arithmetic_kernel(left_type, right_type, operator):
    return _ARITHMETIC_KERNELS.get(operator)
