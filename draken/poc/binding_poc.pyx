# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False
#
# Milestone E.0 — Standalone binding POC.
#
# Proves the canonical pattern every compiled Cython consumer will follow:
#
#   1. `cimport draken.core.buffers` binds the frozen DrakenVector ABI via the
#      hand-written buffers.pxd (cdef extern from "core/buffers.h").
#
#   2. A raw DrakenVector struct can be populated from C-level data and passed
#      to a C++ op function declared via `cdef extern from "ops/..."`.
#
#   3. `cdef extern from "ops/int64_reductions.h" namespace "draken::ops"` works:
#      namespaced C++ functions are callable from Cython nogil.
#
#   4. `i64_sum / i64_min / i64_max` produce correct results on the constructed
#      DrakenVector (data[selection[i]] path, no IDENTITY flag set).
#
# This file intentionally avoids:
#   - Python Vector objects (draken_vector_unwrap is a Phase 0 plumbing item)
#   - draken_vector_from_dense (needs vector_alloc.cpp + mimalloc)
#   - Any allocation inside the kernel
#
# to keep the POC dependency-free beyond libc + the draken headers.
# The identity selection is hand-built on the stack so the C++ path exercises
# the data[selection[i]] access pattern without needing the global lazy tables.
#
# BUILD:
#   cd draken/poc
#   python setup_poc.py build_ext --inplace
#
# RUN:
#   python run_poc.py

from libc.stdint cimport int64_t, uint32_t, uint8_t
from libc.stdlib cimport malloc, free

# 1. Struct ABI — via the hand-written buffers.pxd.
from draken.core.buffers cimport (
    DrakenVector,
    DRAKEN_INT64,
)

# 2. Op functions — declared directly from int64_reductions.h.
#    This header is self-contained (only needs core/buffers.h + core/alloc.h) and
#    avoids the carchar / simd_hash transitive deps that hash.h pulls in.
#    "ops/int64_reductions.h" resolves via `draken` in include_dirs.
#
#    Two equivalent extern forms are shown:
#      a) free-wrapper style (if you add a thin free wrapper in a bridge header)
#      b) namespaced direct form — what the rewrite will use for i64 kernels.
#
#    The POC uses form (b) throughout.

cdef extern from "ops/int64_reductions.h" namespace "draken::ops" nogil:
    uint32_t i64_sum(const DrakenVector& v, int64_t* out_value)
    uint32_t i64_min(const DrakenVector& v, int64_t* out_value)
    uint32_t i64_max(const DrakenVector& v, int64_t* out_value)


# ---------------------------------------------------------------------------
# POC entry point
# ---------------------------------------------------------------------------

def run_binding_poc(list values):
    """
    Accepts a Python list of ints, constructs a DrakenVector directly at the
    C level, calls i64_sum + i64_min + i64_max (draken::ops namespace), and
    returns a dict of results for the caller to validate.

    Returns:
        {
          'length': int,
          'sum': int,             # draken::ops::i64_sum
          'min': int,             # draken::ops::i64_min
          'max': int,             # draken::ops::i64_max
          'non_null_count': int,
        }
    """
    cdef uint32_t n = len(values)
    if n == 0:
        return {'length': 0, 'sum': 0, 'min': 0, 'max': 0, 'non_null_count': 0}

    # Allocate C buffers for data (int64 array) and selection (identity uint32 array).
    # No mimalloc needed: plain malloc is fine for the POC (no cross-thread free).
    cdef int64_t* data = <int64_t*> malloc(n * sizeof(int64_t))
    cdef uint32_t* sel  = <uint32_t*> malloc(n * sizeof(uint32_t))
    if data == NULL or sel == NULL:
        free(data); free(sel)
        raise MemoryError()

    # Fill data + identity selection from the Python list.
    cdef uint32_t i
    for i in range(n):
        data[i] = <int64_t> values[i]
        sel[i] = i  # identity permutation

    # Construct DrakenVector directly (no draken_vector_from_dense: avoids
    # vector_alloc.cpp + mimalloc, which are Phase-0 plumbing).
    cdef DrakenVector v
    v.data        = <void*> data
    v.selection   = <const uint32_t*> sel
    v.data_length = n      # dense: all unique
    v.length      = n
    v.validity    = NULL   # no nulls
    v.type        = DRAKEN_INT64
    v.flags       = 0      # no layout hint; forces data[selection[i]] path

    # 4. Call C++ ops (all nogil — proves the access pattern).
    cdef int64_t  sum_val = 0
    cdef int64_t  min_val = 0
    cdef int64_t  max_val = 0
    cdef uint32_t nonnull_sum, nonnull_min, nonnull_max

    with nogil:
        nonnull_sum = i64_sum(v, &sum_val)
        nonnull_min = i64_min(v, &min_val)
        nonnull_max = i64_max(v, &max_val)

    result = {
        'length': n,
        'sum': <object> sum_val,
        'min': <object> min_val,
        'max': <object> max_val,
        'non_null_count': nonnull_sum,
    }

    free(data)
    free(sel)
    return result
