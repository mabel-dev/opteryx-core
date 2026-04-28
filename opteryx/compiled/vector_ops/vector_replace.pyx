# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

from cpython.bytes cimport PyBytes_FromStringAndSize
from libc.stdint cimport int32_t, uint8_t

from draken.vectors.string_vector cimport StringVector
from draken.vectors import string_vector as string_vector_module
from draken.core.buffers cimport DrakenVarBuffer


cpdef StringVector vector_replace(StringVector data, StringVector search, StringVector replace_val):
    """
    Vectorized string replace: for each row, replace occurrences of 'search'
    in 'data' with 'replace_val'. All parameters are StringVectors.

    Parameters:
        data: StringVector of strings to search within.
        search: StringVector — the substring to find.
        replace_val: StringVector — the replacement string.

    Returns:
        StringVector: result after replacements.
    """
    cdef Py_ssize_t n = data.ptr.length
    cdef Py_ssize_t i
    cdef StringRow data_row, search_row, replace_row
    cdef bytes val_b, srch_b, repl_b

    builder = string_vector_module.StringVectorBuilder.with_estimate(n, 16)

    for i in range(n):
        data_row = string_vec_get_at(data, i)
        if data_row.is_null:
            builder.append_null()
            continue

        search_row = string_vec_get_at(search, i)
        if search_row.is_null:
            builder.append_null()
            continue
        srch_b = PyBytes_FromStringAndSize(search_row.data, search_row.length)

        replace_row = string_vec_get_at(replace_val, i)
        if replace_row.is_null:
            builder.append_null()
            continue
        repl_b = PyBytes_FromStringAndSize(replace_row.data, replace_row.length)

        val_b = PyBytes_FromStringAndSize(data_row.data, data_row.length)
        builder.append(val_b.replace(srch_b, repl_b))

    return builder.finish()
