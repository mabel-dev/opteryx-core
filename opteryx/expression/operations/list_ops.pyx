"""List operations (InList, NotInList) — Draken-native only.

Array input must be a Draken vector. The `values` parameter can be any
iterable, or any object exposing `to_pylist()`. If `arr` isn't Draken, an
AttributeError surfaces from the downstream kernel — that's an upstream
bug, not a fallback to mask.
"""

from opteryx.compiled.nanobind.vector_misc import vector_in_list as _vector_in_list
from draken.vectors.vector import Vector


cdef set _coerce_value_set(value):
    """Convert `value` into a `set` of comparison candidates.

    Vectors materialize through `to_pylist`; non-Vector iterables go through
    `set(...)` directly.
    """
    if isinstance(value, Vector):
        pylist_fn = getattr(value, "to_pylist", None)
        if pylist_fn is not None:
            return set(pylist_fn())
    return set(value)


cpdef in_list(arr, value):
    """Vectorised InList: True where `arr[i]` is in `value`."""

    cdef set values = _coerce_value_set(value)
    return _vector_in_list(arr, values)


cpdef not_in_list(arr, value):
    """Vectorised NotInList: True where `arr[i]` is NOT in `value`."""

    cdef set values = _coerce_value_set(value)
    return _vector_in_list(arr, values, True)
