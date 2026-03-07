# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

from libc.stdint cimport int32_t, uint8_t

from opteryx.draken.vectors.string_vector cimport StringVector
from opteryx.draken.vectors import string_vector as string_vector_module
from opteryx.draken.core.buffers cimport DrakenVarBuffer


cdef inline bytes _get_bytes_row(object col, Py_ssize_t i, DrakenVarBuffer* ptr):
    """Extract raw bytes for row i from a StringVector buffer."""
    cdef int32_t start = ptr.offsets[i]
    cdef int32_t end = ptr.offsets[i + 1]
    return bytes(<uint8_t*>ptr.data + start)[:end - start]


cpdef StringVector vector_replace(StringVector data, object search, object replace_val):
    """
    Vectorized string replace: for each row, replace occurrences of 'search'
    in 'data' with 'replace_val'. Both search and replace_val may be a
    StringVector (column) or a Python bytes/str scalar.

    Parameters:
        data: StringVector of strings to search within.
        search: StringVector or bytes/str — the substring to find.
        replace_val: StringVector or bytes/str — the replacement string.

    Returns:
        StringVector: result after replacements.
    """
    cdef DrakenVarBuffer* data_ptr = data.ptr
    cdef Py_ssize_t n = data_ptr.length
    cdef uint8_t* null_bm = data_ptr.null_bitmap
    cdef Py_ssize_t i

    # Determine if search/replace are vectors or scalars
    cdef bint search_is_vec = isinstance(search, StringVector)
    cdef bint replace_is_vec = isinstance(replace_val, StringVector)

    cdef DrakenVarBuffer* search_ptr = NULL
    cdef DrakenVarBuffer* replace_ptr = NULL

    cdef bytes search_scalar = None
    cdef bytes replace_scalar = None

    if search_is_vec:
        search_ptr = (<StringVector>search).ptr
    else:
        if isinstance(search, bytes):
            search_scalar = search
        else:
            search_scalar = str(search).encode("utf-8")

    if replace_is_vec:
        replace_ptr = (<StringVector>replace_val).ptr
    else:
        if isinstance(replace_val, bytes):
            replace_scalar = replace_val
        else:
            replace_scalar = str(replace_val).encode("utf-8")

    builder = string_vector_module.StringVectorBuilder.with_estimate(n, 16)

    cdef bytes val_b, srch_b, repl_b
    cdef uint8_t* snull
    cdef uint8_t* rnull

    for i in range(n):
        if null_bm != NULL and not ((null_bm[i >> 3] >> (i & 7)) & 1):
            builder.append_null()
            continue

        val_b = _get_bytes_row(data, i, data_ptr)

        if search_is_vec:
            snull = search_ptr.null_bitmap
            if snull != NULL and not ((snull[i >> 3] >> (i & 7)) & 1):
                builder.append_null()
                continue
            srch_b = _get_bytes_row(search, i, search_ptr)
        else:
            srch_b = search_scalar

        if replace_is_vec:
            rnull = replace_ptr.null_bitmap
            if rnull != NULL and not ((rnull[i >> 3] >> (i & 7)) & 1):
                builder.append_null()
                continue
            repl_b = _get_bytes_row(replace_val, i, replace_ptr)
        else:
            repl_b = replace_scalar

        builder.append(val_b.replace(srch_b, repl_b))

    return builder.finish()
