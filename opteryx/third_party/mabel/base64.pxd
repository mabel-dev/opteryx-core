# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

from libc.stddef cimport size_t
from libc.stdint cimport uint8_t

cdef extern from "base64/_base64.h":
    const uint8_t B64_DECODE_LUT[256]
    const char B64_ENCODE_LUT[64]

    void* b64tobin(void* dest, const char* src)
    void* b64tobin_len(void* dest, const char* src, size_t len)
    char* bintob64(char* dest, const void* src, size_t size)

    size_t b64_encoded_size(size_t bin_size)
    size_t b64_decoded_size(size_t b64_len)


cpdef bytes encode(bytes data)
cpdef bytes decode(bytes data)
