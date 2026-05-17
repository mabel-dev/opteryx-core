"""Constant-encoded vector fast path for filter operations.

When a column has been materialised as a `CONSTANT_ENCODED` vector (every
row carries the same scalar), most comparison operators reduce to a single
scalar comparison whose result is then broadcast back. The kernels live in
opteryx/compiled/vector_ops; this layer routes the operator to the right
kernel and falls back to a typed scalar compare when the kernel doesn't
recognise the operand pair.
"""

from opteryx.compiled.vector_ops import build_in_list_carchar, vector_in_list

from draken.vectors.bool_vector import BoolVector
from draken.vectors.vector import Vector


cdef _constant_vector(arr):
    """Return `arr` iff it's a constant-encoded Draken vector."""
    if not isinstance(arr, Vector):
        return None
    if arr.is_constant_encoded():
        return arr
    return None


cpdef bint has_constant_candidate(arr):
    """True if `arr` is a constant-encoded vector candidate."""
    return _constant_vector(arr) is not None


cdef _typed_constant_scalar(arr):
    """Pull out the underlying scalar of a 1-broadcast constant vector."""
    if len(arr) == 0:
        return None
    return arr[0]


cdef _normalize_typed_constant_compare_value(scalar, value):
    """Promote `value` to bytes when the scalar is bytes — string literals
    coming from the planner are often str even though the column is bytes."""
    if isinstance(scalar, bytes):
        if isinstance(value, str):
            return value.encode()
        if isinstance(value, (list, tuple, set)):
            return [
                item.encode() if isinstance(item, str) else item for item in value
            ]
    return value


cpdef _coerce_in_list_values(value):
    """Coerce `value` into the iterable form expected by `vector_in_list`."""
    pylist_fn = getattr(value, "to_pylist", None)
    if pylist_fn is not None:
        value = pylist_fn()
    if isinstance(value, (list, tuple, set)):
        return value
    return [value]


cdef _typed_constant_fastpath(arr, str operator, value):
    """Pure-Python scalar compare for constant vectors when the native
    kernels can't handle the operand pair. Returns a BoolVector broadcast
    to `len(arr)`, or None if the operator is unsupported.
    """
    scalar = _typed_constant_scalar(arr)
    value = _normalize_typed_constant_compare_value(scalar, value)
    cdef Py_ssize_t n = len(arr)

    if scalar is None:
        if operator == "InList":
            result = None in _coerce_in_list_values(value)
            return BoolVector.from_constant(bool(result), n)
        if operator == "NotInList":
            result = None not in _coerce_in_list_values(value)
            return BoolVector.from_constant(bool(result), n)
        return BoolVector(n)

    # numpy-scalar interop: some upstream callers pass np.int64 etc., which
    # don't compare equal to Python ints in all paths. .item() unwraps to
    # the native Python scalar. Narrow except — the only realistic failure
    # is a non-numpy object whose .item() needs different args.
    item_fn = getattr(value, "item", None)
    if item_fn is not None:
        try:
            value = item_fn()
        except (TypeError, ValueError):
            pass

    # Narrow except: the only failure mode from a Python `<` / `>` / etc.
    # is TypeError (incompatible operand pair, e.g. bytes < int). Eq/NotEq
    # and the membership operators don't raise on cross-type input.
    try:
        if operator == "Eq":
            result = scalar == value
        elif operator == "NotEq":
            result = scalar != value
        elif operator == "InList":
            result = scalar in _coerce_in_list_values(value)
        elif operator == "NotInList":
            result = scalar not in _coerce_in_list_values(value)
        elif operator == "Lt":
            result = scalar < value
        elif operator == "Gt":
            result = scalar > value
        elif operator == "LtEq":
            result = scalar <= value
        elif operator == "GtEq":
            result = scalar >= value
        else:
            return None
    except TypeError:
        # Operand pair the comparison can't handle. Caller treats None as
        # "fast path declined" and falls back to the generic dispatch.
        return None

    return BoolVector.from_constant(bool(result), n)


cpdef constant_fastpath(arr, str operator, value):
    """Fast path for constant-encoded vectors.

    Tries the native kernel first; on failure, falls back to a typed scalar
    compare. Returns None if `arr` isn't constant-encoded.
    """
    vec = _constant_vector(arr)
    if vec is None:
        return None

    scalar = _typed_constant_scalar(vec)
    value = _normalize_typed_constant_compare_value(scalar, value)

    try:
        if operator == "Eq":
            return vec.equals(value)
        if operator == "NotEq":
            return vec.not_equals(value)
        if operator == "InList" or operator == "NotInList":
            return vector_in_list(
                vec,
                build_in_list_carchar(_coerce_in_list_values(value)),
                operator == "NotInList",
            )
        if operator == "Lt":
            return vec.less_than(value)
        if operator == "Gt":
            return vec.greater_than(value)
        if operator == "LtEq":
            return vec.less_than_or_equals(value)
        if operator == "GtEq":
            return vec.greater_than_or_equals(value)
    except (TypeError, ValueError, AttributeError):
        # Native kernel can't handle this operand pair. Narrow except so
        # genuine bugs (KeyError, IndexError, …) still surface.
        return _typed_constant_fastpath(vec, operator, value)

    return _typed_constant_fastpath(vec, operator, value)


cdef frozenset _CONSTANT_FASTPATH_OPS = frozenset(
    ("Eq", "NotEq", "InList", "NotInList", "Lt", "Gt", "LtEq", "GtEq")
)


cpdef bint supports_constant_fastpath(str operator):
    """True if `operator` is dispatched by `constant_fastpath`."""
    return operator in _CONSTANT_FASTPATH_OPS
