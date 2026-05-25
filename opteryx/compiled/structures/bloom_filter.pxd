# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

from libc.stdint cimport uint8_t, uint32_t, uint64_t

cdef class BloomFilter:
    cdef uint64_t* bit_array
    cdef uint32_t  bit64_array_size
    cdef uint32_t  bit_array_size_bits
    cdef uint64_t  bit_mask

    cdef inline void  _add(self, const uint64_t item) nogil
    cdef inline bint  _possibly_contains_fast(self, const uint64_t item) nogil
    cpdef void        add(self, const uint64_t item)
    cpdef bint        possibly_contains(self, const uint64_t item)
    cpdef uint8_t[::1] possibly_contains_many_direct(self, uint64_t[::1] hashes)

cpdef BloomFilter  create_bloom_filter_from_hashes(uint64_t[::1] hashes)
