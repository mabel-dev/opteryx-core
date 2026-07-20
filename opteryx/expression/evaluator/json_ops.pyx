# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False
"""JSON and array vector operations.

Called from comparisons.pyx for the @>, @?, AtArrow and AtQuestion operators
on ArrayVector / StringVector-of-JSON columns.

Strings are read directly from the Draken string arena — no Python objects
are materialised on the hot path.
"""

from libc.stdint cimport uint8_t, uint32_t
from libc.stddef cimport size_t

from draken.core.buffers cimport (
    DrakenVector, DrakenStringArena, DrakenStringSlot,
    str_data, str_length,
)
from draken.vectors.vector cimport Vector

from opteryx.third_party.yyjson.cyyjson cimport (
    yyjson_doc, yyjson_val, yyjson_read_err,
    yyjson_read_opts, yyjson_doc_free,
    yyjson_doc_get_root,
    yyjson_doc_ptr_getn,
    yyjson_obj_get,
)

from opteryx.compiled.nanobind.vectors import (
    vector_contains_all,
    vector_contains_any,
)
from draken.draken_native import vector_from_bool_sequence
from draken.vectors.bool_vector import BoolVector


cdef inline bint _bitmap_is_valid(const uint8_t* bitmap, Py_ssize_t i) noexcept nogil:
    return (bitmap[i >> 3] >> (i & 7)) & 1


cpdef _json_at_question(left, right):
    """Check whether each JSON document in `left` contains the path `right`.

    Returns a BoolVector parallel to `left`.
    """
    cdef Vector vec = <Vector>left
    cdef DrakenVector* dv = vec.unified()
    cdef DrakenStringArena* arena = <DrakenStringArena*>dv.data
    cdef DrakenStringSlot* slots = arena.slots
    cdef uint8_t* arena_base = arena.arena
    cdef const uint32_t* sel = dv.selection
    cdef uint8_t* nulls = dv.validity
    cdef Py_ssize_t n = dv.length
    cdef Py_ssize_t i

    # Coerce the path operand to a C bytes buffer once.
    cdef str right_path
    if isinstance(right, str):
        right_path = right
    elif isinstance(right, (bytes, bytearray)):
        right_path = bytes(right).decode("utf-8")
    else:
        scalar = right[0] if len(right) else None
        if scalar is None:
            raise ValueError("@? requires a non-null path literal")
        right_path = scalar.decode("utf-8") if isinstance(scalar, (bytes, bytearray)) else str(scalar)

    # Normalise to a JSON Pointer (RFC 6901).
    cdef str pp = right_path
    if pp.startswith("$"):
        pp = pp[1:]
    if not pp.startswith("/"):
        pp = pp.lstrip(".")
        pp = "/" + pp.replace(".", "/").replace("[", "/").replace("]", "")
    cdef bytes ptr_bytes = pp.encode("utf-8")
    cdef const char* ptr_c = ptr_bytes
    cdef size_t ptr_len = len(ptr_bytes)

    cdef list result = [None] * n
    cdef yyjson_doc* doc
    cdef yyjson_read_err err
    cdef const char* row_ptr
    cdef size_t row_len

    for i in range(n):
        if nulls != NULL and not _bitmap_is_valid(nulls, i):
            result[i] = None
            continue
        row_ptr = <const char*>str_data(&slots[sel[i]], arena_base)
        row_len = str_length(&slots[sel[i]])
        doc = yyjson_read_opts(<char*>row_ptr, row_len, 0, NULL, &err)
        if doc == NULL:
            result[i] = False
            continue
        result[i] = yyjson_doc_ptr_getn(doc, ptr_c, ptr_len) != NULL
        yyjson_doc_free(doc)

    return BoolVector(vector_from_bool_sequence(result))


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
