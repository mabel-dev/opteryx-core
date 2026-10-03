#pragma once
// draken/morsels/cxx_morsel_c.h — the extern "C" morsel-op ABI over CxxMorsel.
//
// Python-free: no <Python.h>, no nanobind. Definitions live in
// morsels/cxx_morsel_ops.cpp (compiled into draken_native.so, resolved by other
// .so's through the RTLD_GLOBAL load in draken/__init__.py). One declaration per
// symbol: the hash and ordinal seams keep their own headers (cxx_hash.h,
// cxx_ordinal.h) and are pulled in here, so including this header gives the
// whole surface.
//
// Every function returning CxxMorsel* returns a heap morsel THE CALLER OWNS —
// free it with cxx_morsel_delete. Results are move-constructed, so the columns'
// shared_ptr<VectorOwner> are shared, not copied.

#include <cstdint>

#include "core/buffers.h"         // DrakenVector
#include "morsels/cxx_morsel.h"   // CxxMorsel
#include "morsels/cxx_hash.h"     // cxx_hash_c, cxx_morsel_delete
#include "morsels/cxx_ordinal.h"  // cxx_ordinal_bounds_c, cxx_ordinal_topn_c

extern "C" {

// Gather all columns at the given row indices.
CxxMorsel* cxx_take_c(const CxxMorsel* m, const int32_t* idx, uint32_t n);

// CROSS JOIN UNNEST over ARRAY column `array_idx` (rule stated at cxx_unnest in
// cxx_morsel_ops.cpp). `child_mask`: optional one byte per logical child
// position (0 = drop), NULL for no filter.
CxxMorsel* cxx_unnest_c(const CxxMorsel* m, uint32_t array_idx,
                        const char* target_name, uint32_t target_name_len,
                        int drop_source, const uint8_t* child_mask);

// CROSS JOIN UNNEST over a literal: `vals` is a one-column morsel of elements.
// NULL when `vals` is not exactly one column.
CxxMorsel* cxx_unnest_literal_c(const CxxMorsel* m, const CxxMorsel* vals,
                                const char* target_name, uint32_t target_name_len);

// Row window [start, start + length) of every column.
CxxMorsel* cxx_slice_c(const CxxMorsel* m, uint32_t start, uint32_t length);

// WP-07 join-key cast of columns[col_idx]: target 0 = FLOAT64, 1 = INT64.
// NULL on cast error / out-of-range col_idx.
CxxMorsel* cxx_cast_column_c(const CxxMorsel* m, uint32_t col_idx, int target);

// Keep rows where the DRAKEN_BOOL mask is valid AND true.
CxxMorsel* cxx_mask_c(const CxxMorsel* m, const DrakenVector* mask);

// cxx_mask_c, with columns in const_col_idx broadcast from never-null scalars
// (cloned into the result) instead of gathered.
CxxMorsel* cxx_mask_with_consts_c(const CxxMorsel* m, const DrakenVector* mask,
                                  const int32_t* const_col_idx,
                                  const DrakenVector* const* const_scalar_dv,
                                  uint32_t n_consts);

// Rebuild every column as a plain-C++-owned VectorOwner (strips py_deleter
// ownership before a morsel crosses into a nogil engine Source).
CxxMorsel* cxx_morsel_materialize_native_c(const CxxMorsel* m);

// Select/reorder columns by identity name (opaque bytes as ptr+len arrays).
CxxMorsel* cxx_select_c(const CxxMorsel* m, const char** name_ptrs,
                        const uint32_t* name_lens, uint32_t n);

// Owned heap morsel sharing m's column owners.
CxxMorsel* cxx_morsel_shallow_copy(const CxxMorsel* m);

// End-of-stream marker: an empty morsel carrying MorselState::END_OF_STREAM.
CxxMorsel* cxx_morsel_new_eos();

}  // extern "C"
