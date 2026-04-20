# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

"""
PCG PRNG (Permuted Congruential Generator) declarations for Cython.

PCG is a high-quality PRNG with excellent statistical properties and
low collision rates. We use it for generating random data in vector ops.

Based on the reference implementation from third_party/pcg/pcg_random.hpp
"""

from libc.stdint cimport uint64_t, uint32_t

cdef extern from "opteryx/third_party/pcg/pcg_random.hpp" namespace "pcg_engines":
    """
    PCG 32-bit one-seq engine with XSH-RS output transformation.

    This provides a fast, high-quality PRNG suitable for generating
    random bytes for vector operations.
    """

    cdef cppclass oneseq_xsh_rs_32_16:
        uint64_t state_

        oneseq_xsh_rs_32_16()
        oneseq_xsh_rs_32_16(uint64_t seed)

        uint32_t operator()() nogil
        uint32_t operator()(uint32_t upper_bound) nogil
        void seed(uint64_t seed) nogil
        void advance(uint64_t delta) nogil


cdef extern from "opteryx/third_party/pcg/pcg_extras.hpp" namespace "pcg_extras":
    """
    PCG helper functions for seeding and other utilities.
    """

    uint64_t static_arbitrary_seed() nogil
