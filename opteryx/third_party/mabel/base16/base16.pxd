# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

from libc.stddef cimport size_t
from libc.stdint cimport uint8_t

cdef extern from "_base16.h":
    const uint8_t B16_DECODE_LUT[256]
    const char B16_ENCODE_LUT[16]

    void* b16tobin(void* dest, const char* src)
    void* b16tobin_len(void* dest, const char* src, size_t len)
    char* bintob16(char* dest, const void* src, size_t size)

    size_t b16_encoded_size(size_t bin_size)
    size_t b16_decoded_size(size_t b16_len)


cpdef bytes encode(bytes data)
cpdef bytes decode(bytes data)
