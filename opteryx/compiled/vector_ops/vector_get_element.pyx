# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False
# cython: optimize.use_switch=True
# cython: optimize.unpack_method_calls=True

from libc.stdint cimport int32_t, int64_t, uint8_t, uint32_t

from draken.core.buffers cimport DrakenVarBuffer, DrakenVector, DrakenStringArena, DrakenStringSlot, str_length, str_data
from draken.vectors.array_vector cimport ArrayVector
from draken.vectors.integer64_vector cimport Integer64Vector
from draken.vectors.string_vector cimport StringVector, StringVectorBuilder
from opteryx.third_party import yyjson


cpdef list vector_get_element(ArrayVector vec, int key):
    """
    Extract element at index 'key' from each row of an ArrayVector.

    Parameters:
        vec: ArrayVector of lists.
        key: zero-based index to retrieve.

    Returns:
        Python list of extracted elements (None for nulls or out-of-range rows).
    """
    cdef Py_ssize_t n = vec._unified_view.length
    cdef Py_ssize_t i
    cdef object row
    cdef list result = [None] * n

    for i in range(n):
        row = vec[i]
        if row is not None and len(row) > key:
            result[i] = row[key]

    return result


cpdef list vector_map_access_array(ArrayVector vec, Integer64Vector key):
    """
    Map/array subscript over ArrayVector using a constant Integer64Vector key.

    Returns:
        Python list of extracted elements (NULL for null/out-of-range rows).
    """
    cdef int64_t index
    cdef Py_ssize_t n = vec._unified_view.length
    cdef Py_ssize_t i
    cdef object row
    cdef Py_ssize_t row_len
    cdef list result = [None] * n

    # MapAccess enforces constant-encoded Integer64Vector keys at the Python layer.
    # We still extract defensively here.
    index = key[0]

    for i in range(n):
        row = vec[i]
        if row is None:
            continue

        row_len = len(row)
        if index >= 0:
            if index < row_len:
                result[i] = row[index]
        else:
            if index >= -row_len:
                result[i] = row[index]

    return result


cpdef StringVector vector_map_access_string(StringVector vec, Integer64Vector key):
    """
    Map/array subscript over StringVector using a constant Integer64Vector key.

    Returns:
        StringVector of one-byte slices; NULL for null/out-of-range rows.
    """
    cdef int64_t index
    cdef DrakenVector* uv = vec.unified()
    cdef DrakenStringArena* arena = <DrakenStringArena*>uv.data
    cdef uint32_t* sel = <uint32_t*>uv.selection
    cdef uint8_t* nulls = uv.validity
    cdef Py_ssize_t n = <Py_ssize_t>uv.length
    cdef Py_ssize_t i
    cdef DrakenStringSlot* slot
    cdef const uint8_t* sdata
    cdef uint32_t slen
    cdef int64_t pos
    cdef StringVectorBuilder builder = StringVectorBuilder.with_estimate(n, 1)

    index = key[0]

    for i in range(n):
        if nulls != NULL and not ((nulls[i >> 3] >> (i & 7)) & 1):
            builder.append_null()
            continue
        slot = &arena.slots[sel[i]]
        sdata = str_data(slot, arena.arena)
        slen = str_length(slot)
        pos = index if index >= 0 else <int64_t>slen + index
        if pos < 0 or pos >= <int64_t>slen:
            builder.append_null()
        else:
            builder.append_bytes(<const char*>sdata + pos, 1)

    return builder.finish()


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
