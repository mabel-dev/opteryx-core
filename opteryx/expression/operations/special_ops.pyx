# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False
"""Special filter operations (JSON path existence)."""

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

import draken.draken_native as _draken_native_special


cdef inline bint _bitmap_is_valid(const uint8_t* bitmap, Py_ssize_t i) noexcept nogil:
    return (bitmap[i >> 3] >> (i & 7)) & 1


cdef str _jsonpath_to_pointer(str jsonpath):
    """Translate `$.key1.list[0]` → `/key1/list/0` (JSON Pointer)."""
    cdef str pointer = jsonpath[1:]
    pointer = pointer.replace(".", "/").replace("[", "/").replace("]", "")
    return pointer


cpdef json_path_exists(left, str value):
    """Vectorised `@?` (AtQuestion): True per row where `value` resolves in the
    JSON document. Returns a Draken vector.
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
    cdef list result = [False] * n
    cdef yyjson_doc* doc
    cdef yyjson_read_err err
    cdef const char* row_ptr
    cdef size_t row_len
    cdef bytes key_bytes
    cdef const char* key_c
    cdef bytes ptr_bytes
    cdef const char* ptr_c
    cdef size_t ptr_len
    cdef str json_pointer

    if not value.startswith("$."):
        key_bytes = value.encode("utf-8")
        key_c = key_bytes
        for i in range(n):
            if nulls != NULL and not _bitmap_is_valid(nulls, i):
                continue
            row_ptr = <const char*>str_data(&slots[sel[i]], arena_base)
            row_len = str_length(&slots[sel[i]])
            doc = yyjson_read_opts(<char*>row_ptr, row_len, 0, NULL, &err)
            if doc == NULL:
                continue
            result[i] = yyjson_obj_get(yyjson_doc_get_root(doc), key_c) != NULL
            yyjson_doc_free(doc)
        return _draken_native_special.vector_from_sequence(result)

    json_pointer = _jsonpath_to_pointer(value)
    ptr_bytes = json_pointer.encode("utf-8")
    ptr_c = ptr_bytes
    ptr_len = len(ptr_bytes)

    for i in range(n):
        if nulls != NULL and not _bitmap_is_valid(nulls, i):
            continue
        row_ptr = <const char*>str_data(&slots[sel[i]], arena_base)
        row_len = str_length(&slots[sel[i]])
        doc = yyjson_read_opts(<char*>row_ptr, row_len, 0, NULL, &err)
        if doc == NULL:
            continue
        result[i] = yyjson_doc_ptr_getn(doc, ptr_c, ptr_len) != NULL
        yyjson_doc_free(doc)

    return _draken_native_special.vector_from_sequence(result)
