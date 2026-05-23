# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False
#
# Milestone E.1 — typed kernel layer (zero object anywhere in this file).
#
# Pattern:
#   .pyx files contain ONLY cdef kernels taking C structs.
#   Python entry points live in nanobind C++ (poc_e1_nanobind.cpp).
#   Both are compiled into the same extension (poc_e1.so).
#
# `cdef public` makes the function externally visible (not static) with
# C++ linkage (__PYX_EXTERN_C = extern "C++" in Cython's C++ mode).
# poc_e1_nanobind.cpp forward-declares them as plain C++ (no extern "C" block)
# so the C++ name mangling matches:
#
#   int64_t poc_e1_sum_kernel(const DrakenVector* dv, uint32_t* nonnull);
#
# No module init is required to call public cdef functions — unlike
# `cdef api`, which requires import_poc_e1_kernel() first.

from libc.stdint cimport int64_t, uint32_t

from draken.core.buffers cimport DrakenVector

cdef extern from "ops/int64_reductions.h" namespace "draken::ops" nogil:
    uint32_t i64_sum(const DrakenVector& v, int64_t* out_value)
    uint32_t i64_min(const DrakenVector& v, int64_t* out_value)
    uint32_t i64_max(const DrakenVector& v, int64_t* out_value)


# ---------------------------------------------------------------------------
# Public cdef kernels — called from poc_e1_nanobind.cpp via extern "C".
# Zero object. No Python state touched.
# ---------------------------------------------------------------------------

cdef public int64_t poc_e1_sum_kernel(
        const DrakenVector* dv, uint32_t* nonnull) nogil:
    cdef int64_t result = 0
    nonnull[0] = i64_sum(dv[0], &result)
    return result


cdef public int64_t poc_e1_min_kernel(
        const DrakenVector* dv, uint32_t* nonnull) nogil:
    cdef int64_t result = 0
    nonnull[0] = i64_min(dv[0], &result)
    return result


cdef public int64_t poc_e1_max_kernel(
        const DrakenVector* dv, uint32_t* nonnull) nogil:
    cdef int64_t result = 0
    nonnull[0] = i64_max(dv[0], &result)
    return result
