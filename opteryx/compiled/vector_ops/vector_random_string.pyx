# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

from libc.time cimport time

from opteryx.draken.vectors.string_vector cimport StringVector
from opteryx.draken.vectors import string_vector as string_vector_module


cdef bytes _ALPHABET = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_/"
cdef unsigned int _rng_state = <unsigned int>time(NULL)


cdef inline unsigned int _xorshift32() nogil:
    global _rng_state
    cdef unsigned int x = _rng_state
    x ^= x << 13
    x ^= x >> 17
    x ^= x << 5
    _rng_state = x
    return x


def vector_random_strings(int row_count, int width) -> StringVector:
    """
    Generate row_count random fixed-width ASCII strings.

    Parameters:
        row_count: number of strings to generate.
        width: length of each string in bytes.

    Returns:
        StringVector of random strings.
    """
    builder = string_vector_module.StringVectorBuilder.with_estimate(row_count, width)

    cdef int i, j
    cdef unsigned int rv
    cdef char buf[4096]

    for i in range(row_count):
        for j in range(width):
            rv = _xorshift32() & 0x3F
            buf[j] = _ALPHABET[rv]
        builder.append_bytes(buf, width)

    return builder.finish()
