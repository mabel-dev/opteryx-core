# Stub .pxd for draken.vectors.float64_vector.

from libc.stdint cimport int32_t, uint8_t, uint32_t
from libc.stddef cimport size_t

from draken.vectors.vector cimport Vector


cdef class Float64Vector(Vector):
    pass


cdef Float64Vector from_decoded(void* data, uint8_t* null_bitmap, size_t length)

cdef Float64Vector make_float64_dict_only(
    const uint32_t* codes,
    Py_ssize_t row_count,
    const double* dictionary,
    Py_ssize_t dict_size,
    const uint8_t* valid_bits,
)

cdef Float64Vector from_packed_dict(
    const uint8_t* codes,
    uint8_t code_width,
    Py_ssize_t row_count,
    const double* dict_ptr,
    Py_ssize_t dict_size,
    const uint8_t* null_bitmap=*,
)

cdef Float64Vector from_dict(
    const int32_t[::1] codes,
    const double[::1] dictionary,
)

cdef Float64Vector from_dict_nullable(
    const int32_t[::1] codes,
    const double[::1] dictionary,
    const uint8_t[::1] validity,
)
