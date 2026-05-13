# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False
# cython: optimize.use_switch=True
# cython: optimize.unpack_method_calls=True

from libc.stdint cimport int32_t, int64_t, uint8_t

from draken.core.buffers cimport DrakenConstantStringPayload, DrakenVarBuffer, DRAKEN_ENCODING_DICTIONARY
from draken.vectors.array_vector cimport ArrayVector
from draken.vectors.int64_vector cimport Int64Vector
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
    cdef Py_ssize_t n = vec.ptr.length
    cdef Py_ssize_t i
    cdef object row
    cdef list result = [None] * n

    for i in range(n):
        row = vec[i]
        if row is not None and len(row) > key:
            result[i] = row[key]

    return result


cpdef list vector_map_access_array(ArrayVector vec, Int64Vector key):
    """
    Map/array subscript over ArrayVector using a constant Int64Vector key.

    Returns:
        Python list of extracted elements (NULL for null/out-of-range rows).
    """
    cdef int64_t index
    cdef Py_ssize_t n = vec.ptr.length
    cdef Py_ssize_t i
    cdef object row
    cdef Py_ssize_t row_len
    cdef list result = [None] * n

    # MapAccess enforces constant-encoded Int64Vector keys at the Python layer.
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


cpdef StringVector vector_map_access_string(StringVector vec, Int64Vector key):
    """
    Map/array subscript over StringVector using a constant Int64Vector key.

    Returns:
        StringVector of one-byte slices; NULL for null/out-of-range rows.
    """
    cdef int64_t index
    cdef DrakenVarBuffer* ptr = vec.ptr
    cdef Py_ssize_t n = ptr.length
    cdef Py_ssize_t i
    cdef uint8_t* null_bm
    cdef int32_t start, end, row_len
    cdef int64_t pos
    cdef DrakenConstantStringPayload* const_val
    cdef int32_t const_len
    cdef StringVectorBuilder builder = StringVectorBuilder.with_estimate(n, 1)

    index = key[0]

    if vec._has_const:
        if vec._const_is_null or vec._const_value == NULL:
            for i in range(n):
                builder.append_null()
        else:
            const_val = vec._const_value
            const_len = const_val.length
            pos = index if index >= 0 else const_len + index
            if pos < 0 or pos >= const_len:
                for i in range(n):
                    builder.append_null()
            else:
                for i in range(n):
                    builder.append_bytes(<const char*>const_val.data + pos, 1)
        return builder.finish()

    null_bm = ptr.null_bitmap
    for i in range(n):
        if null_bm != NULL and not ((null_bm[i >> 3] >> (i & 7)) & 1):
            builder.append_null()
            continue

        start = ptr.offsets[i]
        end = ptr.offsets[i + 1]
        row_len = end - start
        pos = index if index >= 0 else row_len + index
        if pos < 0 or pos >= row_len:
            builder.append_null()
        else:
            builder.append_bytes(<const char*>ptr.data + start + pos, 1)

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
    cdef Py_ssize_t n = docs.ptr.length
    cdef Py_ssize_t i
    cdef object doc
    cdef bytes doc_bytes
    cdef bytes out_bytes
    cdef StringVectorBuilder builder = StringVectorBuilder.with_estimate(n, 16)
    cdef DrakenVarBuffer* dict_ptr
    cdef Py_ssize_t dict_size
    cdef int32_t start, end
    cdef uint8_t* null_bm
    cdef uint32_t code

    if docs._encoding == DRAKEN_ENCODING_DICTIONARY:
        dict_ptr = docs._dict_values
        dict_size = dict_ptr.length
        dict_results = [None] * dict_size
        for i in range(dict_size):
            start = dict_ptr.offsets[i]
            end = dict_ptr.offsets[i + 1]
            doc_bytes = bytes(dict_ptr.data[start:end])
            dict_results[i] = _json_extract_text_value(doc_bytes, key)

        null_bm = docs._dict_accessor.row_nulls
        for i in range(n):
            if null_bm != NULL and not ((null_bm[i >> 3] >> (i & 7)) & 1):
                builder.append_null()
                continue
            code = _read_packed_code(docs._dict_codes, docs._dict_code_width, i)
            out_bytes = dict_results[code]
            if out_bytes is None:
                builder.append_null()
            else:
                builder.append_bytes(<const char*>out_bytes, len(out_bytes))
        return builder.finish()

    for i in range(n):
        doc = docs[i]
        if doc is None:
            builder.append_null()
            continue

        if isinstance(doc, str):
            doc_bytes = (<str>doc).encode("utf8")
        else:
            doc_bytes = <bytes>doc

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
    cdef Py_ssize_t n = docs.ptr.length
    cdef Py_ssize_t i
    cdef object doc
    cdef bytes doc_bytes
    cdef list result = [None] * n
    cdef DrakenVarBuffer* dict_ptr
    cdef Py_ssize_t dict_size
    cdef int32_t start, end
    cdef uint8_t* null_bm
    cdef uint32_t code

    if docs._encoding == DRAKEN_ENCODING_DICTIONARY:
        dict_ptr = docs._dict_values
        dict_size = dict_ptr.length
        dict_results = [None] * dict_size
        for i in range(dict_size):
            start = dict_ptr.offsets[i]
            end = dict_ptr.offsets[i + 1]
            doc_bytes = bytes(dict_ptr.data[start:end])
            dict_results[i] = _json_extract_variant_value(doc_bytes, key)

        null_bm = docs._dict_accessor.row_nulls
        for i in range(n):
            if null_bm != NULL and not ((null_bm[i >> 3] >> (i & 7)) & 1):
                continue
            code = _read_packed_code(docs._dict_codes, docs._dict_code_width, i)
            result[i] = dict_results[code]
        return result

    for i in range(n):
        doc = docs[i]
        if doc is None:
            continue

        if isinstance(doc, str):
            doc_bytes = (<str>doc).encode("utf8")
        else:
            doc_bytes = <bytes>doc

        result[i] = _json_extract_variant_value(doc_bytes, key)

    return result
