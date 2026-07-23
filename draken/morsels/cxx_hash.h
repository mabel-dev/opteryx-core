#pragma once
// draken/morsels/cxx_hash.h — C++ declaration of the draken morsel-hash seam.
//
// `cxx_hash_c` is defined in draken/draken_native.cpp as a pure C++, extern "C",
// nogil-safe function (no PyObject / nanobind in its body). Until now it was
// declared ONLY in the Cython .pxd, so C++ translation units in src/cpp/engine/
// could not call it without a local forward declaration. This header is the one
// shared C++ declaration: it is the `morsel.hash(columns)` surface the execution
// operators (GROUP BY / DISTINCT / JOIN) key on.
//
// Contract (see draken_native.cpp:cxx_hash / hash_shaped_impl):
//   cxx_hash_c(m, col_idxs, n_cols) hashes the n_cols key columns of morsel `m`
//   (identified by column index) into a NEW single-column CxxMorsel whose
//   columns[0].view is a DRAKEN_INT64 hash vector — one hash per input row.
//     * single key  -> shape-preserving (a dict/compressed key yields a
//                       compressed hash vector: data_length distinct hashes
//                       addressed by the selection codes — the "hash each
//                       distinct value once" fast path).
//     * multi  key  -> dense per-row mix (draken_hash per column + simd_mix_hash).
//   NULL rows are baked to the NULL_HASH sentinel; the hash vector itself is
//   always fully valid (a hash is never "absent"). Unsupported column types throw
//   at draken_hash's single choke point.
//   Returns nullptr on allocation failure. THE CALLER OWNS the result and MUST
//   free it with cxx_morsel_delete.

#include <cstdint>

#include "morsels/cxx_morsel.h"  // CxxMorsel

extern "C" CxxMorsel* cxx_hash_c(const CxxMorsel* m, const int32_t* col_idxs,
                                 uint32_t n_cols);
extern "C" void cxx_morsel_delete(CxxMorsel* m);
