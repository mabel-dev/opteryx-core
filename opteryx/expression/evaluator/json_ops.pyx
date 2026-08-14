# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False
"""ARRAY containment operations.

Called from comparisons.pyx for the `@>` (AtArrow) and `@>>` (ArrayContainsAll)
operators on ArrayVector columns.

The file is named json_ops because it also held `@?` (AtQuestion), which parsed
each row's JSON document here with yyjson. `@?` now has a native kernel
(draken_json_path_exists), so that function, the yyjson cimport and the arena
row loop are gone, and nothing in this file reads JSON any more — the name is a
leftover, not a description.
"""

from draken.vectors.vector cimport Vector

from opteryx.compiled.nanobind.vectors import (
    vector_contains_all,
    vector_contains_any,
)
from draken.vectors.bool_vector import BoolVector


cdef set _encode_items(right):
    if right is None:
        return set()
    cdef set out = set()
    for v in right:
        if isinstance(v, str):
            out.add(v.encode())
        else:
            out.add(v)
    return out


cpdef _json_at_arrow(left, right):
    """ArrayVector @> any-of: True where the row's array contains any item."""
    cdef set items = _encode_items(right)
    cdef object left_nb = (<Vector>left)._nb if isinstance(left, Vector) else left
    return BoolVector(vector_contains_any(left_nb, items))


cpdef _json_array_contains_all(left, right):
    """ArrayVector contains-all: True where the row's array contains all items."""
    cdef set items = _encode_items(right)
    cdef object left_nb = (<Vector>left)._nb if isinstance(left, Vector) else left
    return BoolVector(vector_contains_all(left_nb, items))
