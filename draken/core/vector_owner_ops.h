#pragma once
// draken/core/vector_owner_ops.h — VectorOwner-level operations shared by the
// nanobind binding (draken_native.cpp) and the native morsel ops
// (morsels/cxx_morsel_ops.cpp).
//
// Pure C++: no <Python.h>, no nanobind. Draken must execute without Python
// (CLAUDE.md §1/§2), and these are on the native engine's per-morsel path. The
// definitions live in morsels/cxx_morsel_ops.cpp, compiled into draken_native.so.
//
// Errors are C++ exceptions (std::out_of_range / std::invalid_argument /
// std::bad_alloc). At the Python edge nanobind translates them (out_of_range →
// IndexError); on the native path the extern "C" wrappers own the contract.
//
// Namespaced because draken_native.so is loaded RTLD_GLOBAL with default
// visibility: generic names at global scope could interpose across .so's.

#include <atomic>
#include <cstdint>
#include <vector>

#include "core/buffers.h"
#include "core/vector_owner.h"
#include "ops/vec_result.h"

namespace draken::owner_ops {

// E37 TEMP diagnostic counter: scan-carried key-hash seed reuse. Bumped by
// hash_shaped_impl and the binding's string ingestion; dumped by the
// OPTERYX_HASH_TIMING atexit hook (morsels/cxx_morsel_ops.cpp).
extern std::atomic<uint64_t> g_e37_carried_hits;

inline bool row_is_valid(const DrakenVector& v, uint32_t i) noexcept {
    if (v.validity == nullptr) return true;
    return static_cast<bool>((v.validity[i / 8u] >> (i % 8u)) & 1u);
}

// int128 unscaled readback for DRAKEN_DECIMAL128 (16-byte storage).
inline __int128 row_int128(const DrakenVector& v, uint32_t i) noexcept {
    const __int128* data = static_cast<const __int128*>(v.data);
    return data[v.selection[i]];
}

// Bool readback: bit-extract at position selection[i] in the bit-packed data buffer.
// Uniform access pattern: bit(data, selection[i]) — same as int64 but sub-byte element.
inline bool row_bool(const DrakenVector& v, uint32_t i) noexcept {
    const uint32_t    bit_idx = v.selection[i];
    const uint8_t*    data    = static_cast<const uint8_t*>(v.data);
    return static_cast<bool>((data[bit_idx >> 3] >> (bit_idx & 7)) & 1u);
}

// FNV-1a seed over the 2*dim raw fp16 bytes for a single row.
// Passed through simd_hash_i64 at the call site for distribution consistency.
inline uint64_t fp16_row_fnv_seed(const uint16_t* fp16_data, uint32_t dim) {
    const uint8_t* bytes = reinterpret_cast<const uint8_t*>(fp16_data);
    uint64_t h = 14695981039346656037ULL;
    const uint32_t nbytes = dim * 2u;
    for (uint32_t k = 0u; k < nbytes; ++k) {
        h ^= static_cast<uint64_t>(bytes[k]);
        h *= 1099511628211ULL;
    }
    return h;
}

// Convert a VecResult into a VectorOwner, transferring ownership (consumes r).
VectorOwner vecresult_to_owner(VecResult r);

// All-null DRAKEN_NULL vector of `length` rows (no data, no validity).
VectorOwner make_null_vector(uint32_t length);

// VECTOR_FP16 needs a logical-type descriptor with dimension >= 1.
void require_fp16_descriptor(const VectorOwner& v, const char* ctx);

// Typed gathers. Negative indices wrap once (Python semantics); out of range
// throws std::out_of_range.
VectorOwner make_fp16_take(const VectorOwner& v, const int32_t* indices, uint32_t n);
VectorOwner make_bool_take(const VectorOwner& v, const int32_t* indices, uint32_t n);
VectorOwner make_array_take(const VectorOwner& v, const int32_t* indices, uint32_t n);

// Take child elements by a flat index array (non-negative raw indices).
VectorOwner take_child(const VectorOwner& src_child, const std::vector<int32_t>& cidx);

// take / slice — shared type dispatch for every vector type.
VectorOwner vector_take_impl(const VectorOwner& v, const int32_t* idx, uint32_t n);
VectorOwner vector_slice_impl(const VectorOwner& v, uint32_t start, uint32_t length);

// Surviving-row indices (valid AND true) of a DRAKEN_BOOL mask.
std::vector<int32_t> mask_indices(const DrakenVector& m);

// Shape-preserving keying hash — ONE implementation shared by Vector.hash_shaped,
// cxx_hash and draken_hash_rows.
VectorOwner hash_shaped_impl(const VectorOwner& v);

}  // namespace draken::owner_ops
