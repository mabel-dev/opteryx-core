"""List operations (InList, NotInList) — Draken-native only.

Array input must be a Draken vector. The `values` parameter can be any
iterable, or any object exposing `to_pylist()`. If `arr` isn't Draken, an
AttributeError surfaces from the downstream kernel — that's an upstream
bug, not a fallback to mask.
"""

from opteryx.compiled import vector_ops
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


cpdef in_list(arr, value, bint dict_candidate=False):
    """Vectorised InList: True where `arr[i]` is in `value`."""
    if dict_candidate:
        fast = dictionary_fastpath(arr, "InList", value)
        if fast is not None:
            record_dict_fastpath_hit()
            return fast
        raise RuntimeError("Dictionary fastpath failed for `InList`.")

    cdef set values = _coerce_value_set(value)
    return vector_ops.vector_in_list(arr, vector_ops.build_in_list_carchar(values))


cpdef not_in_list(arr, value, bint dict_candidate=False):
    """Vectorised NotInList: True where `arr[i]` is NOT in `value`."""
    if dict_candidate:
        fast = dictionary_fastpath(arr, "NotInList", value)
        if fast is not None:
            record_dict_fastpath_hit()
            return fast
        raise RuntimeError("Dictionary fastpath failed for `NotInList`.")

    cdef set values = _coerce_value_set(value)
    # Fused negation: kernel writes the inverted result directly, no second
    # full-vector pass.
    return vector_ops.vector_in_list(
        arr, vector_ops.build_in_list_carchar(values), True
    )
