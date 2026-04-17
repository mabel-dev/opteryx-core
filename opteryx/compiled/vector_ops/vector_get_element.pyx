# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

from libc.stdint cimport int32_t, int64_t, uint8_t

from opteryx.compiled.draken.core.buffers cimport DrakenConstantStringPayload, DrakenVarBuffer
from opteryx.compiled.draken.vectors.array_vector cimport ArrayVector
from opteryx.compiled.draken.vectors.int64_vector cimport Int64Vector
from opteryx.compiled.draken.vectors.string_vector cimport StringVector
from opteryx.compiled.draken.vectors.string_vector cimport StringVectorBuilder
from opteryx.third_party.tktech import csimdjson as simdjson


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


cpdef StringVector vector_json_extract_text(StringVector docs, bytes key):
    """
    JSON extraction for ->> over StringVector documents.

    Returns:
        StringVector containing UTF-8 bytes (NULL for null/missing).
    """
    from opteryx.exceptions import IncorrectTypeError

    cdef Py_ssize_t n = docs.ptr.length
    cdef Py_ssize_t i
    cdef object doc
    cdef object value
    cdef object mini
    cdef object parser
    cdef bytes doc_bytes
    cdef bytes out_bytes
    cdef StringVectorBuilder builder = StringVectorBuilder.with_estimate(n, 16)

    for i in range(n):
        doc = docs[i]
        if doc is None:
            builder.append_null()
            continue

        if isinstance(doc, str):
            doc_bytes = (<str>doc).encode("utf8")
        else:
            doc_bytes = <bytes>doc

        parser = simdjson.Parser()
        try:
            value = parser.parse(doc_bytes).get(key)  # type: ignore
        except ValueError as err:
            raise IncorrectTypeError("The `->>` operator can only be used on JSON documents.") from err

        if value is None:
            builder.append_null()
            continue

        if hasattr(value, "mini"):
            mini = value.mini
            if mini is None:
                builder.append_null()
                continue
            out_bytes = mini if isinstance(mini, bytes) else str(mini).encode("utf8")
        elif isinstance(value, bytes):
            out_bytes = <bytes>value
        else:
            out_bytes = str(value).encode("utf8")

        builder.append_bytes(<const char*>out_bytes, len(out_bytes))

    return builder.finish()


cpdef list vector_json_extract_variant(StringVector docs, bytes key):
    """
    JSON extraction for -> over StringVector documents.

    Returns:
        Python list of extracted values (scalar/list/dict/None).
    """
    from opteryx.exceptions import IncorrectTypeError

    cdef Py_ssize_t n = docs.ptr.length
    cdef Py_ssize_t i
    cdef object doc
    cdef object value
    cdef object parser
    cdef bytes doc_bytes
    cdef list result = [None] * n

    for i in range(n):
        doc = docs[i]
        if doc is None:
            continue

        if isinstance(doc, str):
            doc_bytes = (<str>doc).encode("utf8")
        else:
            doc_bytes = <bytes>doc

        parser = simdjson.Parser()
        try:
            value = parser.parse(doc_bytes).get(key)  # type: ignore
        except ValueError as err:
            raise IncorrectTypeError("The `->` operator can only be used on JSON documents.") from err

        if hasattr(value, "as_list"):
            result[i] = value.as_list()  # type: ignore[attr-defined]
        elif hasattr(value, "as_dict"):
            result[i] = value.as_dict()  # type: ignore[attr-defined]
        else:
            result[i] = value

    return result
