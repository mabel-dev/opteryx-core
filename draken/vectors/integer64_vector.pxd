# Stub .pxd for draken.vectors.integer64_vector.
# Declares the cdef class and factory cdef functions that opteryx/ consumers
# cimport.  Must stay byte-for-byte consistent with the C-level signatures in
# the compiled integer64_vector.so (generated from integer64_vector.cpp).

from libc.stdint cimport int32_t, int64_t, uint8_t, uint32_t
from libc.stddef cimport size_t

from draken.vectors.vector cimport Vector


cdef class Integer64Vector(Vector):
    cdef uint8_t* null_bitmap_ptr(self) noexcept


cdef Integer64Vector from_decoded(void* data, uint8_t* null_bitmap, size_t length)

cdef Integer64Vector from_sequence(const int64_t[::1] data)

cdef Integer64Vector make_int64_dict_only(
    const uint32_t* codes,
    Py_ssize_t row_count,
    const int64_t* dictionary,
    Py_ssize_t dict_size,
    const uint8_t* valid_bits,
)

cdef Integer64Vector from_packed_dict(
    const uint8_t* codes,
    uint8_t code_width,
    Py_ssize_t row_count,
    const int64_t* dict_ptr,
    Py_ssize_t dict_size,
    const uint8_t* null_bitmap=*,
)

cdef Integer64Vector from_dict(
    const int32_t[::1] codes,
    const int64_t[::1] dictionary,
)

cdef Integer64Vector from_dict_nullable(
    const int32_t[::1] codes,
    const int64_t[::1] dictionary,
    const uint8_t[::1] validity,
)
