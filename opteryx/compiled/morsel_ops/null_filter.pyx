# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False
# cython: optimize.use_switch=True
# cython: optimize.unpack_method_calls=True

"""opteryx.compiled.morsel_ops.null_filter

Find row indices where all specified columns are non-null, using the draken
Vector validity bitmap directly. No PyArrow, no Python objects in the inner loop.
"""

from libcpp.vector cimport vector
from libc.stdint cimport int64_t, uint8_t, uint32_t
from libc.stdlib cimport malloc, free
from libc.string cimport memset

from draken.morsels.morsel cimport Morsel
from draken.vectors.vector cimport Vector
from draken.core.buffers cimport DrakenVector


cdef vector[int64_t] non_null_row_indices(Morsel morsel, list column_names):
    """
    Return a vector of logical row indices where all columns in column_names
    are non-null.

    Reads the draken Vector.validity bitmap directly (1-bit per logical row,
    LSB-first; NULL pointer means all valid).
    """
    cdef vector[int64_t] result
    cdef Vector col_vec
    cdef const DrakenVector* dv
    cdef uint32_t n_rows, i
    cdef const uint8_t* validity
    cdef uint8_t* combined

    if not column_names:
        n_rows = <uint32_t>morsel.num_rows
        result.reserve(n_rows)
        for i in range(n_rows):
            result.push_back(<int64_t>i)
        return result

    col_vec = morsel.column(column_names[0])
    if col_vec is None:
        return result

    n_rows = col_vec._dv.length
    if n_rows == 0:
        return result

    combined = <uint8_t*>malloc(n_rows * sizeof(uint8_t))
    if combined == NULL:
        raise MemoryError()
    memset(combined, 1, n_rows)

    try:
        for col_name in column_names:
            col_vec = morsel.column(col_name)
            if col_vec is None:
                continue
            dv = col_vec._dv
            validity = dv.validity
            if validity == NULL:
                continue  # all rows valid in this column
            for i in range(dv.length):
                if not ((validity[i >> 3] >> (i & 7)) & 1):
                    combined[i] = 0

        result.reserve(n_rows)
        for i in range(n_rows):
            if combined[i]:
                result.push_back(<int64_t>i)
    finally:
        free(combined)

    return result
