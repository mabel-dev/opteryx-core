# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

from libc.stddef cimport size_t
from libc.stdint cimport uint8_t

cdef extern from "base85/_base85.h":
    const char B85_ENCODE_LUT[85]
    const uint8_t B85_DECODE_LUT[256]

    void* b85tobin(void* dest, const char* src) nogil
    void* b85tobin_len(void* dest, const char* src, size_t len) nogil
    char* bintob85(char* dest, const void* src, size_t size) nogil

    size_t b85_encoded_size(size_t bin_size)
    size_t b85_decoded_size(size_t b85_len)


cpdef bytes encode(bytes data)
cpdef bytes decode(bytes data)
