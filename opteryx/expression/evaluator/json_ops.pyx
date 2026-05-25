"""JSON and array vector operations.

Cython migration of the former json_ops.py. Called from comparisons.pyx
for the @>, @?, AtArrow and AtQuestion operators on ArrayVector /
StringVector-of-JSON columns.
"""

from opteryx.compiled.nanobind.vector_string_search import (
    vector_contains_all,
    vector_contains_any,
)
from opteryx.third_party import yyjson

from draken.draken_native import vector_from_sequence


cpdef _json_at_question(left, str right):
    """Check whether each JSON document in `left` contains the path `right`.

    Returns a Draken vector parallel to `left`; entries are True/False where
    the doc was non-null, and None where the doc was null.
    """
    cdef Py_ssize_t n = len(left)
    cdef Py_ssize_t i
    cdef list result
    cdef str json_pointer

    parser = yyjson.Parser()

    if right.startswith("$."):
        # JSONPath format ($.foo.bar) — yyjson handles existence by `in`.
        result = [None] * n
        for i in range(n):
            doc = left[i]
            if doc is None:
                result[i] = None
            else:
                result[i] = right in parser.parse(doc)
    else:
        # JSON Pointer format (/foo/bar): rewrite path syntax once.
        json_pointer = right[1:].replace(".", "/").replace("[", "/").replace("]", "")
        result = [None] * n
        for i in range(n):
            doc = left[i]
            if doc is None:
                result[i] = None
                continue
            try:
                parser.parse(doc).at_pointer(json_pointer)
                result[i] = True
            except Exception:
                # yyjson surfaces missing-path as an exception; map to False.
                result[i] = False

    return vector_from_sequence(result)


cdef set _encode_items(right):
    """Coerce a Python iterable of items into a set of byte/native values."""
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
    return vector_contains_any(left, items)


cpdef _json_array_contains_all(left, right):
    """ArrayVector contains-all: True where the row's array contains all items."""
    cdef set items = _encode_items(right)
    return vector_contains_all(left, items)
