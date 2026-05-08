# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

from libc.stddef cimport size_t
from libc.stdint cimport uint8_t


cdef const char* _HEX = "0123456789abcdef"


cdef inline void _to_hex(const unsigned char* digest, size_t dlen, char* out) noexcept nogil:
    cdef size_t i
    cdef unsigned char b
    for i in range(dlen):
        b = digest[i]
        out[2 * i] = _HEX[(b >> 4) & 0x0F]
        out[2 * i + 1] = _HEX[b & 0x0F]
