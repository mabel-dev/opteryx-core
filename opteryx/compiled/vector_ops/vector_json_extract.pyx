# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False
# cython: optimize.use_switch=True
# cython: optimize.unpack_method_calls=True

# JSON extraction operators -> and ->> over StringVector.
#
# NOTE: _json_extract_text_value and _json_extract_variant_value both use
# `hasattr`, which is banned per CLAUDE.md §8 (exception-based flow control).
# These calls are inherited verbatim from the pre-split vector_get_element.pyx.
# Fixing them is out of scope for E.6 — tracked separately.

from libc.stdint cimport uint8_t, uint32_t

from draken.core.buffers cimport DrakenVector, DrakenStringArena, DrakenStringSlot, str_length, str_data
from draken.vectors.string_vector cimport StringVector, StringVectorBuilder
from opteryx.third_party import yyjson


cdef object _json_extract_text_value(bytes doc_bytes, bytes key):
    """Extract a key from a JSON document and return bytes or None."""
    from opteryx.exceptions import IncorrectTypeError
    cdef object parser = yyjson.Parser()
    cdef object value
    cdef object mini
    cdef bytes out_bytes
    try:
        value = parser.parse(doc_bytes).get(key)
    except ValueError as err:
        raise IncorrectTypeError("The `->>` operator can only be used on JSON documents.") from err
    if value is None:
        return None
    if hasattr(value, "mini"):
        mini = value.mini
        if mini is None:
            return None
        return mini if isinstance(mini, bytes) else str(mini).encode("utf8")
    if isinstance(value, bytes):
        return <bytes>value
    return str(value).encode("utf8")


cpdef StringVector vector_json_extract_text(StringVector docs, bytes key):
    """
    JSON extraction for ->> over StringVector documents.

    Returns:
        StringVector containing UTF-8 bytes (NULL for null/missing).
    """
    cdef DrakenVector* uv = docs.unified()
    cdef DrakenStringArena* arena = <DrakenStringArena*>uv.data
    cdef uint32_t* sel = <uint32_t*>uv.selection
    cdef uint8_t* nulls = uv.validity
    cdef Py_ssize_t n = <Py_ssize_t>uv.length
    cdef Py_ssize_t i
    cdef bytes doc_bytes
    cdef bytes out_bytes
    cdef DrakenStringSlot* slot
    cdef const uint8_t* sdata
    cdef uint32_t slen
    cdef StringVectorBuilder builder = StringVectorBuilder.with_estimate(n, 16)

    for i in range(n):
        if nulls != NULL and not ((nulls[i >> 3] >> (i & 7)) & 1):
            builder.append_null()
            continue
        slot = &arena.slots[sel[i]]
        slen = str_length(slot)
        sdata = str_data(slot, arena.arena)
        doc_bytes = bytes(sdata[:slen])
        out_bytes = _json_extract_text_value(doc_bytes, key)
        if out_bytes is None:
            builder.append_null()
        else:
            builder.append_bytes(<const char*>out_bytes, len(out_bytes))

    return builder.finish()


cdef object _json_extract_variant_value(bytes doc_bytes, bytes key):
    """Extract a key from a JSON document and return a Python object or None."""
    from opteryx.exceptions import IncorrectTypeError
    cdef object parser = yyjson.Parser()
    cdef object value
    try:
        value = parser.parse(doc_bytes).get(key)
    except ValueError as err:
        raise IncorrectTypeError("The `->` operator can only be used on JSON documents.") from err
    if hasattr(value, "as_list"):
        return value.as_list()
    if hasattr(value, "as_dict"):
        return value.as_dict()
    return value


cpdef list vector_json_extract_variant(StringVector docs, bytes key):
    """
    JSON extraction for -> over StringVector documents.

    Returns:
        Python list of extracted values (scalar/list/dict/None).
    """
    cdef DrakenVector* uv = docs.unified()
    cdef DrakenStringArena* arena = <DrakenStringArena*>uv.data
    cdef uint32_t* sel = <uint32_t*>uv.selection
    cdef uint8_t* nulls = uv.validity
    cdef Py_ssize_t n = <Py_ssize_t>uv.length
    cdef Py_ssize_t i
    cdef bytes doc_bytes
    cdef DrakenStringSlot* slot
    cdef const uint8_t* sdata
    cdef uint32_t slen
    cdef list result = [None] * n

    for i in range(n):
        if nulls != NULL and not ((nulls[i >> 3] >> (i & 7)) & 1):
            continue
        slot = &arena.slots[sel[i]]
        slen = str_length(slot)
        sdata = str_data(slot, arena.arena)
        doc_bytes = bytes(sdata[:slen])
        result[i] = _json_extract_variant_value(doc_bytes, key)

    return result
