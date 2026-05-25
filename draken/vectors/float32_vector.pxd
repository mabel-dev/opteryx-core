# Stub .pxd for draken.vectors.float32_vector.

from libc.stdint cimport uint8_t
from libc.stddef cimport size_t

from draken.vectors.vector cimport Vector


cdef class Float32Vector(Vector):
    pass


cdef Float32Vector from_decoded(void* data, uint8_t* null_bitmap, size_t length)

cdef Float32Vector from_packed_dict(
    const uint8_t* codes,
    uint8_t code_width,
    Py_ssize_t row_count,
    const float* dict_ptr,
    Py_ssize_t dict_size,
    const uint8_t* null_bitmap=*,
)
