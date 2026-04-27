# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

from libc.stddef cimport size_t
from libc.stdint cimport uint8_t

cdef extern from "_base64.h":
    # Lookup tables
    const uint8_t B64_DECODE_LUT[256]
    const char B64_ENCODE_LUT[64]

    # Basic functions (with auto-dispatch)
    void* b64tobin(void* dest, const char* src)
    void* b64tobin_len(void* dest, const char* src, size_t len)
    char* bintob64(char* dest, const void* src, size_t size)

    # Optimized versions
    void* b64tobin_scalar(void* dest, const char* src, size_t len)
    void* b64tobin_neon(void* dest, const char* src, size_t len)
    void* b64tobin_avx2(void* dest, const char* src, size_t len)
    void* b64tobin_avx512(void* dest, const char* src, size_t len)

    char* bintob64_scalar(char* dest, const void* src, size_t size)
    char* bintob64_neon(char* dest, const void* src, size_t size)
    char* bintob64_avx2(char* dest, const void* src, size_t size)
    char* bintob64_avx512(char* dest, const void* src, size_t size)

    # Utility functions
    size_t b64_encoded_size(size_t bin_size)
    size_t b64_decoded_size(size_t b64_len)

    # CPU feature detection
    int b64_has_neon()
    int b64_has_avx2()
    int b64_has_avx512()


cpdef bytes encode(bytes data)
cpdef bytes decode(bytes data)
cpdef bint has_neon()
cpdef bint has_avx2()
