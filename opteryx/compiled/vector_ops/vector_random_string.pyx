# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False
# cython: optimize.use_switch=True
# cython: optimize.unpack_method_calls=True

from libc.stdlib cimport malloc, free
from libc.stdint cimport uint8_t, uint32_t, uint64_t

from draken.vectors.string_vector cimport StringVector
from draken.vectors import string_vector as string_vector_module
from opteryx.third_party.pcg.pcg cimport oneseq_xsh_rs_32_16, static_arbitrary_seed


cdef bytes _ALPHABET = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_/"



def vector_random_strings(int row_count, int width) -> StringVector:
    """
    Generate row_count random fixed-width ASCII strings using PCG PRNG.

    Parameters:
        row_count: number of strings to generate.
        width: length of each string in bytes.

    Returns:
        StringVector of random strings.

    Uses PCG32 (PCG-XSH-RS) for high-quality random number generation with
    excellent statistical properties and minimal collision risk.
     """
    builder = string_vector_module.StringVectorBuilder.with_estimate(row_count, width)

    cdef int i, j
    cdef oneseq_xsh_rs_32_16 rng
    rng.seed(static_arbitrary_seed())
    cdef uint32_t rv
    cdef char* buf

    # Allocate buffer once and reuse for all rows
    buf = <char*>malloc(width + 1)
    if buf is NULL:
        raise MemoryError("Failed to allocate buffer for random strings")

    try:
        for i in range(row_count):
            for j in range(width):
                rv = rng() & 0x3F
                buf[j] = _ALPHABET[rv]
            buf[width] = 0  # Null-terminate for safety
            builder.append_bytes(buf, width)
    finally:
        free(buf)

    return builder.finish()
