#pragma once
// draken/morsels/cxx_morsel_ops.h — nogil C++ morsel-op surface over CxxMorsel (S0).
//
// These are the operations the converted operators will call directly on a C++
// morsel, with NO PyObject and NO GIL. They read columns via the uniform
// data[selection[i]] path (CxxColumn.view), so dense/constant/dict all work, and
// build results as shared_ptr<VectorOwner> (RAII, GIL-free free).

#include <cstdint>
#include <cstring>
#include <memory>
#include <stdexcept>

#include "morsels/cxx_morsel.h"
#include "core/vector_owner.h"
#include "core/vector_alloc.h"   // draken_vector_from_dense
#include "core/alloc.h"          // draken_malloc / draken_free
#include "ops/hash.h"            // draken_hash (global, nogil)
#include "simd_hash.h"           // simd_mix_hash (global, nogil)

// Dense per-row keying hash over the given columns. Mirrors Morsel.hash / the
// c_hash dense-mix path: single column → that column's hash; multi-column → mix
// the per-column hashes with simd_mix_hash. Returns an INT64 dense vector holding
// the row hashes. Fully nogil-eligible: no Python, all kernels are nogil.
//
// `cols` lists the column indices to hash (n_cols entries). ARRAY columns cannot
// be hashed via this path — the caller must guard against them upstream.
static inline std::shared_ptr<VectorOwner>
cxx_hash(const CxxMorsel& m, const uint32_t* cols, uint32_t n_cols) {
    const uint32_t n = m.num_rows();
    const size_t alloc_n = (n != 0u) ? static_cast<size_t>(n) : 1u;
    uint64_t* buf = static_cast<uint64_t*>(draken_malloc(alloc_n * sizeof(uint64_t)));
    if (!buf) throw std::bad_alloc();
    std::memset(buf, 0, alloc_n * sizeof(uint64_t));  // simd_mix_hash requires zeroed

    if (n != 0u && n_cols != 0u) {
        if (n_cols == 1u) {
            draken_hash(m.columns[cols[0]].view, buf, n);
        } else {
            uint64_t* tmp = static_cast<uint64_t*>(draken_malloc(static_cast<size_t>(n) * sizeof(uint64_t)));
            if (!tmp) { draken_free(buf); throw std::bad_alloc(); }
            for (uint32_t c = 0u; c < n_cols; ++c) {
                draken_hash(m.columns[cols[c]].view, tmp, n);
                simd_mix_hash(buf, tmp, static_cast<size_t>(n));
            }
            draken_free(tmp);
        }
    }

    // buf is draken_malloc'd; the VectorOwner adopts it (freed on destruct).
    DrakenVector dv = draken_vector_from_dense(buf, n, DRAKEN_INT64, nullptr);
    return std::make_shared<VectorOwner>(
        dv, OwnedBuffer<void>(buf), OwnedBuffer<uint8_t>(nullptr));
}
