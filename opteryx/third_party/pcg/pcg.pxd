# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False


from libc.stdint cimport uint64_t, uint32_t

cdef extern from "../../third_party/pcg/pcg_random.hpp" namespace "pcg_engines":
    cdef cppclass oneseq_xsh_rs_32_16:
        uint64_t state_

        oneseq_xsh_rs_32_16()
        oneseq_xsh_rs_32_16(uint64_t seed)

        uint32_t operator()() nogil
        uint32_t operator()(uint32_t upper_bound) nogil
        void seed(uint64_t seed) nogil
        void advance(uint64_t delta) nogil


cdef extern from "../../third_party/pcg/pcg_pyhelpers.hpp":
    uint64_t nondeterministic_seed() nogil
