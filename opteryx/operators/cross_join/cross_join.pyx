# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False
# cython: optimize.use_switch=True
# cython: optimize.unpack_method_calls=True

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
`build_cartesian_indices` — a standalone Draken-native utility that builds
Cartesian-product row indices. CROSS JOIN itself is a PhysicalStep the native
compiler lowers as a zero-key inner join; it has no operator class.
"""

from libc.stdint cimport int64_t, uint32_t
from libc.stddef cimport size_t

from draken.vectors.vector cimport Vector, from_decoded as _vector_from_decoded, dict_int64_from_decoded as _dict_int64_from_decoded
from draken.core.buffers cimport DRAKEN_INT64

cdef extern from "core/alloc.h" nogil:
    void* draken_malloc(size_t n) nogil
    void  draken_free(void* p) nogil

cpdef tuple build_cartesian_indices(int64_t left_rows, int64_t right_rows):
    """
    Build row indices for a Cartesian product (CROSS JOIN).

    Left index is dictionary-encoded (unique values [0..left_rows-1], codes expand runs).
    Right index is dense ([0..right_rows-1] repeated left_rows times).

    Returns:
        tuple of (Vector dict-encoded, Vector dense) — left and right row indices
    """
    cdef int64_t total_rows = left_rows * right_rows
    cdef int64_t i, j
    cdef Vector left_vec, right_vec
    cdef uint32_t* codes = NULL
    cdef int64_t* dict_vals = NULL
    cdef int64_t* rvals = NULL

    if total_rows == 0:
        return (_draken_native.vector_int64_from_sequence([]), _draken_native.vector_int64_from_sequence([]))

    # dict values: [0, 1, ..., left_rows-1] — draken_malloc'd, ownership transferred to left_vec.
    dict_vals = <int64_t*>draken_malloc(<size_t>left_rows * sizeof(int64_t))
    if dict_vals == NULL:
        raise MemoryError()

    # codes[i * right_rows + j] = i  (same run value repeated right_rows times)
    codes = <uint32_t*>draken_malloc(<size_t>total_rows * sizeof(uint32_t))
    if codes == NULL:
        draken_free(dict_vals)
        raise MemoryError()

    # Right side: draken_malloc'd, ownership transferred via _vector_from_decoded.
    rvals = <int64_t*>draken_malloc(<size_t>total_rows * sizeof(int64_t))
    if rvals == NULL:
        draken_free(codes)
        draken_free(dict_vals)
        raise MemoryError()

    with nogil:
        for i in range(left_rows):
            dict_vals[i] = i

        for i in range(left_rows):
            for j in range(right_rows):
                codes[i * right_rows + j] = <uint32_t>i

        for i in range(left_rows):
            for j in range(right_rows):
                rvals[i * right_rows + j] = j

    # Transfer ownership of rvals to right_vec (dense int64 vector).
    right_vec = _vector_from_decoded(<void*>rvals, NULL, <uint32_t>total_rows, DRAKEN_INT64)

    # Transfer ownership of dict_vals and codes to left_vec (dict-encoded int64 vector).
    # After this call, dict_vals and codes MUST NOT be freed — ownership is transferred.
    left_vec = _dict_int64_from_decoded(
        <void*>dict_vals, <uint32_t>left_rows,
        codes, <uint32_t>total_rows, NULL
    )

    return (left_vec, right_vec)
