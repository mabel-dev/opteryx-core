# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

from libc.stddef cimport size_t
from libc.stdint cimport uint64_t, int64_t, uint8_t

from opteryx.compiled.draken.core.buffers cimport ConstAccessor, DictAccessor, DrakenEncoding

from opteryx.compiled.draken.interop.arrow import vector_from_arrow
from opteryx.compiled.draken.interop.vector_sequence import vector_from_sequence

cdef const uint64_t MIX_HASH_CONSTANT
cdef const uint64_t NULL_HASH

cdef extern from "simd_hash.h":
    void simd_mix_hash(uint64_t* dest, const uint64_t* values, size_t count) nogil

cdef extern from "simd_bitops.h":
    size_t simd_popcount(const uint8_t* data, size_t n) nogil

cdef inline uint64_t mix_hash(uint64_t current, uint64_t value) nogil:
    cdef uint64_t mixed = current ^ value
    mixed = mixed * MIX_HASH_CONSTANT + 1
    return mixed ^ (mixed >> 32)

cdef class Vector:
    cdef bint here
    cdef DrakenEncoding _encoding
    cpdef object null_bitmap(self)
    cdef DictAccessor* dict_accessor(self) noexcept
    cdef ConstAccessor* const_accessor(self) noexcept
    cdef void* dense_ptr(self) noexcept
    cdef uint8_t* null_bitmap_ptr(self) noexcept
    cdef void hash_into(self, uint64_t[::1] out_buf, Py_ssize_t offset=*) except *
    cdef bint c_hash_into(self, uint64_t* out, Py_ssize_t n) noexcept nogil
    cpdef uint64_t[::1] hash(self)

    # Compress: convert each value to a signed 64-bit integer using the
    # same semantics as `to_int` in relation_statistics. Implementations
    # should write into an int64 buffer provided by the caller.
    cdef void compress_into(self, int64_t[::1] out_buf, Py_ssize_t offset=*) except *
    # Convenience Python-callable constructor that allocates the output
    # buffer, calls `compress_into`, and returns it.
    cpdef int64_t[::1] compress(self)

    # Compare two values at given indices. Returns -1, 0, 1.
    # Does not check nulls; caller must handle via is_null_at.
    cpdef int compare_at(self, Py_ssize_t left_idx, Py_ssize_t right_idx) except? 0

    # Check if value at index is null.
    cpdef bint is_null_at(self, Py_ssize_t idx) except? False
