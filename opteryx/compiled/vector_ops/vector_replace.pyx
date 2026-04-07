# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

from cpython.bytes cimport PyBytes_FromStringAndSize
from libc.stdint cimport int32_t, uint8_t

from opteryx.compiled.draken.vectors.string_vector cimport StringVector
from opteryx.compiled.draken.vectors import string_vector as string_vector_module
from opteryx.compiled.draken.core.buffers cimport DrakenVarBuffer


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
    cdef Py_ssize_t n = data.ptr.length
    cdef Py_ssize_t i
    cdef StringRow data_row, search_row, replace_row
    cdef bint search_is_vec = isinstance(search, StringVector)
    cdef bint replace_is_vec = isinstance(replace_val, StringVector)
    cdef bytes search_scalar = None
    cdef bytes replace_scalar = None
    cdef bytes val_b, srch_b, repl_b

    if not search_is_vec:
        if isinstance(search, bytes):
            search_scalar = search
        else:
            search_scalar = str(search).encode("utf-8")

    if not replace_is_vec:
        if isinstance(replace_val, bytes):
            replace_scalar = replace_val
        else:
            replace_scalar = str(replace_val).encode("utf-8")

    builder = string_vector_module.StringVectorBuilder.with_estimate(n, 16)

    for i in range(n):
        data_row = string_vec_get_at(data, i)
        if data_row.is_null:
            builder.append_null()
            continue

        if search_is_vec:
            search_row = string_vec_get_at(<StringVector>search, i)
            if search_row.is_null:
                builder.append_null()
                continue
            srch_b = PyBytes_FromStringAndSize(search_row.data, search_row.length)
        else:
            srch_b = search_scalar

        if replace_is_vec:
            replace_row = string_vec_get_at(<StringVector>replace_val, i)
            if replace_row.is_null:
                builder.append_null()
                continue
            repl_b = PyBytes_FromStringAndSize(replace_row.data, replace_row.length)
        else:
            repl_b = replace_scalar

        val_b = PyBytes_FromStringAndSize(data_row.data, data_row.length)
        builder.append(val_b.replace(srch_b, repl_b))

    return builder.finish()
