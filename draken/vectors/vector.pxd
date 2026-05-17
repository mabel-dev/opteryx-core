# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

from libc.stddef cimport size_t
from libc.stdint cimport uint64_t, int64_t, uint8_t, uint16_t, uint32_t

from draken.core.buffers cimport DrakenVector

from draken.interop.arrow import vector_from_arrow
from draken.interop.vector_sequence import vector_from_sequence

cdef const uint64_t MIX_HASH_CONSTANT
cdef const uint64_t NULL_HASH

cdef uint64_t _EMPTY_UINT64_SENTINEL

cdef class _Uint64Buffer:
    cdef uint64_t* data
    cdef Py_ssize_t n
    cdef Py_ssize_t _shape[1]
    cdef Py_ssize_t _strides[1]
    @staticmethod
    cdef _Uint64Buffer create(Py_ssize_t n)

cdef extern from "simd_hash.h":
    void simd_mix_hash(uint64_t* dest, const uint64_t* values, size_t count) nogil
    void simd_mix_hash_from_dict_cw1(uint64_t* dest, const uint64_t* dict_lookup,
                                      const uint8_t* codes, size_t count) nogil
    void simd_mix_hash_from_dict_cw2(uint64_t* dest, const uint64_t* dict_lookup,
                                      const uint16_t* codes, size_t count) nogil
    void simd_mix_hash_from_dict_cw4(uint64_t* dest, const uint64_t* dict_lookup,
                                      const uint32_t* codes, size_t count) nogil
    void simd_mix_hash_from_dict_nullable_cw1(uint64_t* dest, const uint64_t* dict_lookup,
                                               const uint8_t* codes, const uint8_t* null_bitmap,
                                               size_t start_row, size_t count) nogil
    void simd_mix_hash_from_dict_nullable_cw2(uint64_t* dest, const uint64_t* dict_lookup,
                                               const uint16_t* codes, const uint8_t* null_bitmap,
                                               size_t start_row, size_t count) nogil
    void simd_mix_hash_from_dict_nullable_cw4(uint64_t* dest, const uint64_t* dict_lookup,
                                               const uint32_t* codes, const uint8_t* null_bitmap,
                                               size_t start_row, size_t count) nogil

cdef extern from "simd_bitops.h":
    size_t simd_popcount(const uint8_t* data, size_t n) nogil

cdef inline uint64_t mix_hash(uint64_t current, uint64_t value) nogil:
    cdef uint64_t mixed = current ^ value
    mixed = mixed * MIX_HASH_CONSTANT + 1
    return mixed ^ (mixed >> 32)

cdef class Vector:
    cdef bint here
    cdef DrakenVector _unified_view   # scratch storage for unified() return value
    cpdef object null_bitmap(self)
    cdef void* dense_ptr(self) noexcept
    cdef uint8_t* null_bitmap_ptr(self) noexcept
    cdef void hash_into(self, uint64_t[::1] out_buf, Py_ssize_t offset=*) except *
    cdef bint c_hash_into(self, uint64_t* out, Py_ssize_t n) noexcept nogil
    cdef bint c_hash_single(self, uint64_t* out, Py_ssize_t n) noexcept nogil
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

    # Return a dense (non-encoded) version of this vector.
    # Default: return self (already dense). Subclasses override for dict/const/RLE.
    cpdef Vector materialize(self)

    # Return the value at index i as a Python object, or None if null.
    # Base delegates to __getitem__; hot subtypes override with a direct cdef.
    cdef object item_at(self, Py_ssize_t i)

    # Return a DrakenVector* view over this vector's fields (Phase 1 skeleton).
    # The returned pointer is &self._unified_view — lifetime == self.
    # RLE encoding aborts (must be expanded at scan boundaries before execution).
    cdef DrakenVector* unified(self) noexcept

    # Python-accessible constant-encoding check: True iff data_length == 1.
    # Used by included .pyx files that cannot cimport (e.g. expression/__init__).
    cpdef bint is_constant_encoded(self)
