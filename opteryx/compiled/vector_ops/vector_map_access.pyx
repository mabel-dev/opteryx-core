# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False
# cython: optimize.use_switch=True
# cython: optimize.unpack_method_calls=True

# Map/array subscript and element-access kernels.
#
# vector_map_access_string is kept here (not ported to C++) because building
# a DrakenStringArena output from a nanobind C++ extension requires the
# single-block ownership pattern used in draken_native.cpp internally — there
# is no bridge function for it. Deferred to a future phase.

from libc.stdint cimport int64_t, uint8_t, uint32_t

from draken.core.buffers cimport DrakenVarBuffer, DrakenVector, DrakenStringArena, DrakenStringSlot, str_length, str_data
from draken.vectors.array_vector cimport ArrayVector
from draken.vectors.integer64_vector cimport Integer64Vector
from draken.vectors.string_vector cimport StringVector, StringVectorBuilder


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
