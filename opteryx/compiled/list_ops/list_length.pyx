# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=True
# cython: boundscheck=False

import pyarrow
import array as _array

# Native implementation provided by the vendored extension.
# It accepts any object exposing the buffer protocol (e.g., NumPy arrays,
# PyArrow buffers) and returns a `bytearray` containing packed uint32
# values (native endianness).
from opteryx.nanobind.list_length import offsets_to_lengths_into
from libc.stdint cimport uint32_t

cpdef uint32_t[::1] list_length(object array):

    cdef Py_ssize_t n
    cdef uint32_t[::1] mv
    cdef Py_ssize_t total_res_len = 0
    cdef Py_ssize_t chunk_res_len = 0
    cdef Py_ssize_t start = 0

    # Uses PyArrow offsets buffer
    if isinstance(array, pyarrow.ChunkedArray):
        # Precompute total length (sum of len(chunk) for each chunk)
        for chunk in array.chunks:
            total_res_len += len(chunk)

        out = _array.array('I', [0]) * total_res_len
        # Fill each chunk into the appropriate slice of the out buffer
        for chunk in array.chunks:
            chunk_res_len = len(chunk)
            if chunk_res_len > 0:
                view = memoryview(out)[start:(start + chunk_res_len)]
                offsets_buffer = chunk.buffers()[1]
                offsets_to_lengths_into(offsets_buffer, view)
                start += chunk_res_len

        mv = memoryview(out)
        return mv

    n = len(array)

    offsets_buffer = array.buffers()[1]

    res_len = n
    out = _array.array('I', [0]) * res_len
    offsets_to_lengths_into(offsets_buffer, out)

    # Convert the filled array to a typed memoryview of uint32
    mv = memoryview(out)
    return mv
