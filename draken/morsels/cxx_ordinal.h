#pragma once
// draken/morsels/cxx_ordinal.h — C++ declaration of the draken ordinal-bounds seam.
//
// `cxx_ordinal_bounds_c` is defined in draken/draken_native.cpp as a pure C++,
// extern "C", nogil-safe function. Same seam pattern, and the same reason, as
// morsels/cxx_hash.h: ops/hash.h's dispatch table is `static inline`, so a second
// shared object that included it would get its own copy of the table. One symbol,
// one table, one definition of a value's ordinal.
//
// Contract (see the definition):
//   cxx_ordinal_bounds_c(m, col_idx, &lo, &hi) computes the draken ORDINAL min
//   and max of morsel column `col_idx`, over its NON-NULL rows only.
//   Returns 1 and writes lo/hi when a bound exists; returns 0 — "no bound",
//   i.e. PRUNE NOTHING — for a type with no ordinalize kernel (DECIMAL128 has
//   none, deliberately), for ARRAY / VECTOR_FP16 / NULL, for zero rows, and for
//   an all-null column. Never throws.
//
// The ordinal space is the one skene::compute_statistics writes file min/max in
// and Manifest._ordinalize_literal produces plan-time zone terms in — see
// docs/RUNTIME_MINMAX_FILTER_DESIGN.md.

#include <cstdint>

#include "morsels/cxx_morsel.h"  // CxxMorsel

extern "C" int cxx_ordinal_bounds_c(const CxxMorsel* m, int32_t col_idx,
                                    int64_t* out_lo, int64_t* out_hi);

// cxx_ordinal_topn_c(m, col_idx, n, ascending, heap, &heap_len) keeps the n BEST
// non-null ordinals of column `col_idx` across calls, in caller-owned heap state
// (room for min(n, heap_len + rows) entries). Once heap_len == n, heap[0] is the
// n-th best value seen — the Top-N runtime boundary
// (docs/TOPN_RUNTIME_BOUNDARY_DESIGN.md §3). Returns 1 when the column has an
// ordinal, 0 (heap untouched) when it does not. Never throws on a type it
// refuses; allocation failure of its per-thread scratch is the only throw.
extern "C" int cxx_ordinal_topn_c(const CxxMorsel* m, int32_t col_idx, uint32_t n,
                                  int ascending, int64_t* heap, uint32_t* heap_len);
