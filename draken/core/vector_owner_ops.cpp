// draken/core/vector_owner_ops.cpp — VectorOwner-level operations shared by the
// nanobind binding and the native morsel ops. Pure C++ (no <Python.h>, no
// nanobind): see core/vector_owner_ops.h. Compiled into draken_native.so.

#include <cstdint>
#include <cstdio>
#include <cstring>
#include <memory>
#include <new>
#include <stdexcept>
#include <string>
#include <vector>

#include "core/vector_owner_ops.h"
#include "core/draken_capi.h"
#include "core/alloc.h"
#include "core/vector_alloc.h"
#include "logical_type.h"
#include "ops/hash.h"      // draken_take / draken_slice / draken_hash_shaped(_carried), simd_hash_i64, NULL_HASH

namespace draken::owner_ops {

std::atomic<uint64_t> g_e37_carried_hits{0};

// ---------------------------------------------------------------------------
// D.11: null vector — self-describing, no data buffer, no validity buffer.
// All rows are null; type tag is the sole signal — short-circuit on type==NULL.
// ---------------------------------------------------------------------------
VectorOwner make_null_vector(uint32_t length) {
    DrakenVector v;
    v.data        = nullptr;
    v.selection   = draken_zero_sel(length > 0u ? length : 1u);
    v.data_length = 0u;
    v.length      = length;
    v.validity    = nullptr;
    v.type        = DRAKEN_NULL;
    v.flags       = 0u;
    return VectorOwner(v, OwnedBuffer<void>(nullptr), OwnedBuffer<uint8_t>(nullptr));
}

// ---------------------------------------------------------------------------
// D.11: fp16 descriptor helpers.
// ---------------------------------------------------------------------------
void require_fp16_descriptor(const VectorOwner& v, const char* ctx) {
    if (!v.logical_type || v.logical_type->kind != LogicalKind::VECTOR
            || v.logical_type->dimension == 0u)
        throw std::invalid_argument(
            std::string(ctx) +
            ": VECTOR_FP16 requires a logical-type descriptor with dimension >= 1");
}

// D.11: fp16 take — gather rows by index list, producing a dense output vector.
VectorOwner make_fp16_take(const VectorOwner& v,
                                  const int32_t* indices, uint32_t n) {
    require_fp16_descriptor(v, "take");
    const uint32_t dim = v.logical_type->dimension;
    const uint16_t* src = static_cast<const uint16_t*>(v.vec.data);

    const size_t data_bytes = static_cast<size_t>(n > 0u ? n : 1u)
                              * dim * sizeof(uint16_t);
    uint16_t* dst = static_cast<uint16_t*>(draken_malloc(data_bytes));
    if (!dst) throw std::bad_alloc();
    std::memset(dst, 0, data_bytes);
    OwnedBuffer<void> data_buf(dst);

    bool has_nulls = false;
    for (uint32_t i = 0u; i < n; ++i) {
        int32_t idx = indices[i];
        const int32_t vlen = static_cast<int32_t>(v.vec.length);
        if (idx < 0) idx += vlen;
        if (idx < 0 || idx >= vlen)
            throw std::out_of_range("take: index out of range");
        if (!row_is_valid(v.vec, static_cast<uint32_t>(idx))) {
            has_nulls = true;  // data row zeroed by memset
        } else {
            std::memcpy(dst + static_cast<size_t>(i) * dim,
                        src + v.vec.selection[static_cast<uint32_t>(idx)]
                              * static_cast<size_t>(dim),
                        dim * sizeof(uint16_t));
        }
    }

    OwnedBuffer<uint8_t> validity_buf;
    uint8_t* validity = nullptr;
    if (has_nulls) {
        const uint32_t bm     = (n + 7u) / 8u;
        const uint32_t padded = ((bm + 7u) & ~7u);
        const size_t   vbytes = padded > 0u ? padded : 8u;
        validity = static_cast<uint8_t*>(draken_malloc(vbytes));
        if (!validity) throw std::bad_alloc();
        validity_buf.reset(validity);
        std::memset(validity, 0xFF, vbytes);
        for (uint32_t i = 0u; i < n; ++i) {
            int32_t idx = indices[i];
            const int32_t vlen = static_cast<int32_t>(v.vec.length);
            if (idx < 0) idx += vlen;
            if (!row_is_valid(v.vec, static_cast<uint32_t>(idx)))
                validity[i / 8u] &= static_cast<uint8_t>(~(1u << (i % 8u)));
        }
    }

    DrakenVector vr = draken_vector_from_dense(dst, n, DRAKEN_VECTOR_FP16, validity);
    VectorOwner owner(vr, std::move(data_buf), std::move(validity_buf));
    owner.logical_type = v.logical_type;
    return owner;
}

// D.12: bool take — gather logical rows of a bit-packed DRAKEN_BOOL vector by index.
// Uniform access pattern: data[selection[i]] at bit level.
VectorOwner make_bool_take(const VectorOwner& v,
                                   const int32_t* indices, uint32_t n) {
    if (v.vec.type != DRAKEN_BOOL)
        throw std::invalid_argument("make_bool_take: expected DRAKEN_BOOL");

    const uint8_t* src_data = static_cast<const uint8_t*>(v.vec.data);

    const uint32_t bm_out    = (n + 7u) >> 3;
    const size_t   alloc_out = (bm_out > 0u) ? static_cast<size_t>((bm_out + 7u) & ~7u) : 8u;
    uint8_t* out_data = static_cast<uint8_t*>(draken_malloc(alloc_out));
    if (!out_data) throw std::bad_alloc();
    std::memset(out_data, 0, alloc_out);
    OwnedBuffer<void> data_buf(out_data);

    bool has_nulls = false;
    for (uint32_t i = 0u; i < n; ++i) {
        int32_t idx = indices[i];
        const int32_t vlen = static_cast<int32_t>(v.vec.length);
        if (idx < 0) idx += vlen;
        if (idx < 0 || idx >= vlen)
            throw std::out_of_range("take: index out of range");
        if (!row_is_valid(v.vec, static_cast<uint32_t>(idx))) {
            has_nulls = true;
        } else {
            const uint32_t code = v.vec.selection[static_cast<uint32_t>(idx)];
            const uint32_t bit  = (src_data[code >> 3] >> (code & 7u)) & 1u;
            if (bit)
                out_data[i >> 3] |= static_cast<uint8_t>(1u << (i & 7u));
        }
    }

    OwnedBuffer<uint8_t> validity_buf;
    uint8_t* validity = nullptr;
    if (has_nulls) {
        const uint32_t bm     = (n + 7u) / 8u;
        const uint32_t padded = ((bm + 7u) & ~7u);
        const size_t   vbytes = padded > 0u ? padded : 8u;
        validity = static_cast<uint8_t*>(draken_malloc(vbytes));
        if (!validity) throw std::bad_alloc();
        validity_buf.reset(validity);
        std::memset(validity, 0xFF, vbytes);
        for (uint32_t i = 0u; i < n; ++i) {
            int32_t idx = indices[i];
            const int32_t vlen = static_cast<int32_t>(v.vec.length);
            if (idx < 0) idx += vlen;
            if (!row_is_valid(v.vec, static_cast<uint32_t>(idx)))
                validity[i / 8u] &= static_cast<uint8_t>(~(1u << (i % 8u)));
        }
    }

    DrakenVector vr = draken_vector_from_dense(out_data, n, DRAKEN_BOOL, validity);
    return VectorOwner(vr, std::move(data_buf), std::move(validity_buf));
}

// ---------------------------------------------------------------------------
// C.2: convert a VecResult into a VectorOwner, transferring ownership.
// Consumes r — do not use r after this call.
// ---------------------------------------------------------------------------
VectorOwner vecresult_to_owner(VecResult r) {
    DrakenVector v;
    v.data        = r.data;
    v.selection   = r.selection;
    v.data_length = r.data_length;
    v.length      = r.length;
    v.validity    = r.validity;
    v.type        = r.type;
    v.flags       = r.flags;

    OwnedBuffer<void>    data_buf(r.data);
    // Phase 9c: when validity is embedded in the data block (string-family
    // output), it is freed with the block — do NOT own it as a second buffer.
    OwnedBuffer<uint8_t> val_buf(r.validity_embedded ? nullptr : r.validity);
    OwnedBuffer<void>    codes_buf(r.owns_selection
                                    ? const_cast<void*>(static_cast<const void*>(r.selection))
                                    : nullptr);
    // A string result that kept its byte arena as a separate allocation hands it
    // over here; nullptr (every consolidated result) leaves arena_buf empty and
    // the block owns its own bytes exactly as before. Freeing this is not
    // optional — the vector's slots point into it.
    OwnedBuffer<uint8_t> arena_buf(r.arena);
    VectorOwner owner(v, std::move(data_buf), std::move(val_buf), std::move(codes_buf),
                      std::move(arena_buf));

    // Phase 9c: attach the timestamp unit descriptor when the kernel set one.
    // DrakenVector/VecResult carry no LogicalType; it lives on the VectorOwner.
    if (r.type == DRAKEN_TIMESTAMP64 && r.ts_unit != 0xFFu) {
        LogicalType lt;
        lt.kind           = LogicalKind::TIMESTAMP;
        lt.unit           = static_cast<TimestampUnit>(r.ts_unit);
        lt.offset_minutes = 0;
        owner.logical_type = logical_type_intern(lt);
    }
    // TIME32/TIME64 results (Phase 9c cast kernels): mirrors the TIMESTAMP64
    // block above. Both TIME tiers are parameterized-physical types — a result
    // with no LogicalType attached would fail downstream (to_pylist etc.).
    if ((r.type == DRAKEN_TIME32 || r.type == DRAKEN_TIME64) && r.ts_unit != 0xFFu) {
        LogicalType lt;
        lt.kind           = LogicalKind::TIME;
        lt.unit           = static_cast<TimestampUnit>(r.ts_unit);
        lt.offset_minutes = 0;
        owner.logical_type = logical_type_intern(lt);
    }
    // S-A.2: attach the DECIMAL precision/scale descriptor when the kernel set one
    // (dec_precision > 0). Mirrors the timestamp block; the arena DV*/VecResult
    // carry no LogicalType, so DECIMAL results would otherwise fail to_pylist.
    if ((r.type == DRAKEN_DECIMAL || r.type == DRAKEN_DECIMAL128) && r.dec_precision > 0u) {
        LogicalType lt;
        lt.kind      = LogicalKind::DECIMAL;
        lt.precision = r.dec_precision;
        lt.scale     = r.dec_scale;
        owner.logical_type = logical_type_intern(lt);
    }
    // Vector results: attach the width descriptor the kernel set. Mirrors the two
    // blocks above. A VECTOR_FP16 with no dimension is a hard error downstream
    // (make_fp16_* enforce dimension >= 1), so fail here — where the producing
    // kernel is still named — rather than at an opaque readback.
    if (r.type == DRAKEN_VECTOR_FP16) {
        if (r.vec_dimension == 0u)
            throw std::invalid_argument(
                "vecresult_to_owner: VECTOR_FP16 result requires vec_dimension >= 1");
        LogicalType lt;
        lt.kind      = LogicalKind::VECTOR;
        lt.dimension = r.vec_dimension;
        owner.logical_type = logical_type_intern(lt);
    }
    // ARRAY results: adopt the owned child element VecResult into child_owner.
    // `data` above owns only the int32_t offsets[length+1]; the elements are the
    // child. Recursive, so ARRAY<ARRAY<T>> chains; VectorOwner's destructor then
    // frees the whole subtree. `new`-allocated by the kernel, deleted here — the
    // VecResult is consumed exactly once, on this path only.
    if (r.child) {
        owner.child_owner =
            std::make_unique<VectorOwner>(vecresult_to_owner(*r.child));
        delete r.child;
    }
    return owner;
}

// ---------------------------------------------------------------------------
// Helper: take child elements by a flat index array (non-negative raw indices).
// Routes by child type; recursive for DRAKEN_ARRAY children.
// ---------------------------------------------------------------------------
VectorOwner take_child(const VectorOwner& src_child,
                               const std::vector<int32_t>& cidx) {
    const uint32_t cn = static_cast<uint32_t>(cidx.size());
    if (src_child.vec.type == DRAKEN_ARRAY)
        return make_array_take(src_child, cidx.data(), cn);
    if (src_child.vec.type == DRAKEN_VECTOR_FP16)
        return make_fp16_take(src_child, cidx.data(), cn);
    // bool is bit-packed; draken_take has no bool slot — mirror vector_take_impl.
    if (src_child.vec.type == DRAKEN_BOOL)
        return make_bool_take(src_child, cidx.data(), cn);
    auto result = vecresult_to_owner(draken_take(src_child.vec, cidx.data(), cn));
    result.vec.type     = src_child.vec.type;
    result.logical_type = src_child.logical_type;
    return result;
}

// ---------------------------------------------------------------------------
// D.13: array take — gather rows by index array → new owned DRAKEN_ARRAY.
// Indices are int32_t; negative values are resolved before calling this function.
// Result owns its own offsets + a new child (recursive RAII).
// ---------------------------------------------------------------------------
VectorOwner make_array_take(const VectorOwner& v,
                                   const int32_t* indices, uint32_t n) {
    const int32_t* src_offsets = static_cast<const int32_t*>(v.vec.data);
    const int32_t  vlen        = static_cast<int32_t>(v.vec.length);

    // Build new offsets and child index list in one pass.
    const size_t off_bytes = (n + 1u) * sizeof(int32_t);
    int32_t* new_offsets = static_cast<int32_t*>(
        draken_malloc(off_bytes > 0u ? off_bytes : sizeof(int32_t)));
    if (!new_offsets) throw std::bad_alloc();
    OwnedBuffer<void> data_buf(new_offsets);
    new_offsets[0] = 0;

    std::vector<int32_t> child_idx;
    bool has_nulls = false;

    for (uint32_t i = 0u; i < n; ++i) {
        int32_t idx = indices[i];
        if (idx < 0) idx += vlen;
        if (idx < 0 || idx >= vlen)
            throw std::out_of_range("take: array index out of range");
        if (!row_is_valid(v.vec, static_cast<uint32_t>(idx))) {
            has_nulls = true;
            new_offsets[i + 1u] = new_offsets[i];
        } else {
            const uint32_t sel_i = v.vec.selection[static_cast<uint32_t>(idx)];
            const int32_t  start = src_offsets[sel_i];
            const int32_t  end   = src_offsets[sel_i + 1u];
            for (int32_t j = start; j < end; ++j)
                child_idx.push_back(j);
            new_offsets[i + 1u] = new_offsets[i] + (end - start);
        }
    }

    // Build validity bitmap for output rows.
    OwnedBuffer<uint8_t> validity_buf;
    uint8_t* validity = nullptr;
    if (has_nulls) {
        const uint32_t bm     = (n + 7u) / 8u;
        const uint32_t padded = ((bm + 7u) & ~7u);
        const size_t   vbytes = padded > 0u ? padded : 8u;
        validity = static_cast<uint8_t*>(draken_malloc(vbytes));
        if (!validity) throw std::bad_alloc();
        validity_buf.reset(validity);
        std::memset(validity, 0xFF, vbytes);
        for (uint32_t i = 0u; i < n; ++i) {
            int32_t idx = indices[i];
            if (idx < 0) idx += vlen;
            if (!row_is_valid(v.vec, static_cast<uint32_t>(idx)))
                validity[i / 8u] &= static_cast<uint8_t>(~(1u << (i % 8u)));
        }
    }

    // Gather child elements (recursive RAII — result owns its child).
    std::unique_ptr<VectorOwner> new_child;
    if (v.child_owner)
        new_child = std::make_unique<VectorOwner>(take_child(*v.child_owner, child_idx));

    DrakenVector vr = draken_vector_from_dense(new_offsets, n, DRAKEN_ARRAY, validity);
    VectorOwner owner(vr, std::move(data_buf), std::move(validity_buf));
    owner.child_owner = std::move(new_child);
    return owner;
}

// ---------------------------------------------------------------------------
// take — shared dispatch over a raw int32 index buffer. The nb::list `take`
// binding boxes into a std::vector and calls this; the C bridge
// (draken_vector_take_buffer) passes a typed memoryview pointer directly, so
// hot-path Cython callers (Morsel.take / _take_inplace / align_tables) avoid
// per-row PyObject boxing entirely.
// ---------------------------------------------------------------------------
VectorOwner vector_take_impl(const VectorOwner& v, const int32_t* idx, uint32_t n) {
    // D.11: null — taking from null always produces a null vector of length n.
    if (v.vec.type == DRAKEN_NULL) return make_null_vector(n);
    // D.13: array — gather rows with owned child copy.
    if (v.vec.type == DRAKEN_ARRAY) return make_array_take(v, idx, n);
    // D.11: fp16 — gather rows by index.
    if (v.vec.type == DRAKEN_VECTOR_FP16) return make_fp16_take(v, idx, n);
    // D.12: bool — bit-packed gather.
    if (v.vec.type == DRAKEN_BOOL) return make_bool_take(v, idx, n);
    auto result = vecresult_to_owner(draken_take(v.vec, idx, n));
    // Typed kernels hardcode their own type tag in VecResult (e.g. i64_take
    // always emits DRAKEN_INT64). Restore the original physical type so that
    // TIMESTAMP64 (and any future aliased type) stays correct after gather.
    result.vec.type     = v.vec.type;
    result.logical_type = v.logical_type;
    return result;
}

// S0: slice/mask compute extracted from the nanobind Vector.slice/.mask lambdas so
// cxx_slice/cxx_mask share ONE body (no duplication). Pure C++ over DrakenVector
// structs — the callers (the lambdas, and the nogil cxx_* ops) manage the GIL.
VectorOwner vector_slice_impl(const VectorOwner& v, uint32_t start, uint32_t length) {
    if (static_cast<uint64_t>(start) + length > v.vec.length)
        throw std::out_of_range("Vector.slice: start + length exceeds vector length");
    if (v.vec.type == DRAKEN_NULL) return make_null_vector(length);
    if (v.vec.type == DRAKEN_ARRAY || v.vec.type == DRAKEN_VECTOR_FP16 ||
        v.vec.type == DRAKEN_BOOL) {
        std::vector<int32_t> idx_vec(length);
        for (uint32_t i = 0; i < length; ++i)
            idx_vec[i] = static_cast<int32_t>(start + i);
        if (v.vec.type == DRAKEN_ARRAY)       return make_array_take(v, idx_vec.data(), length);
        if (v.vec.type == DRAKEN_VECTOR_FP16) return make_fp16_take(v, idx_vec.data(), length);
        return make_bool_take(v, idx_vec.data(), length);
    }
    auto result = vecresult_to_owner(draken_slice(v.vec, start, length));
    result.vec.type     = v.vec.type;
    result.logical_type = v.logical_type;
    // Slicing rows copies the dictionary VERBATIM (same values, same order) when the
    // result keeps the full dictionary (data_length unchanged), so a sorted dict stays
    // sorted — carry the DRAKEN_DICT_KEYS_SORTED hint through (CLAUDE.md §6).
    if ((v.vec.flags & DRAKEN_DICT_KEYS_SORTED) && draken_is_dict(&result.vec) &&
        result.vec.data_length == v.vec.data_length)
        result.vec.flags |= DRAKEN_DICT_KEYS_SORTED;
    // A contiguous row-range slice of a row-sorted vector is still row-sorted
    // (a sub-range of a monotonic sequence is monotonic) — carry ROW_SORTED
    // (+DESC) through by the same reasoning as DICT_KEYS_SORTED above. Without
    // this, execute_to_morsels' chunking (Morsel.slice, always applied even to
    // a full-length no-op slice) silently drops the flag before any caller of
    // the standard query API ever sees it.
    if (v.vec.flags & DRAKEN_ROW_SORTED)
        result.vec.flags |= (v.vec.flags & (DRAKEN_ROW_SORTED | DRAKEN_ROW_SORTED_DESC));
    return result;
}

// Derive the surviving-row indices (valid AND true) from a DRAKEN_BOOL mask.
// Shared by the single-column vector_mask_impl and the whole-morsel cxx_mask so
// the index list is built ONCE per mask, not re-scanned per column.
std::vector<int32_t> mask_indices(const DrakenVector& m) {
    if (m.type != DRAKEN_BOOL)
        throw std::invalid_argument("mask: expected a DRAKEN_BOOL mask vector");
    const uint32_t mn = m.length;
    std::vector<int32_t> idx_vec;
    idx_vec.reserve(mn);
    for (uint32_t i = 0; i < mn; ++i)
        if (row_is_valid(m, i) && row_bool(m, i))
            idx_vec.push_back(static_cast<int32_t>(i));
    return idx_vec;
}

// ── Shape-preserving keying hash — ONE implementation shared by the
//    Vector.hash_shaped binding and the nogil C-ABI cxx_hash_c. Pure C++,
//    GIL-free (no PyObject/nanobind). Mirrors the old hash_shaped lambda body.
VectorOwner hash_shaped_impl(const VectorOwner& v) {
    if (v.vec.type == DRAKEN_ARRAY)
        throw std::invalid_argument("hash_shaped: not supported for DRAKEN_ARRAY");
    const uint32_t n = v.vec.length;
    if (v.vec.type != DRAKEN_NULL && v.vec.type != DRAKEN_VECTOR_FP16
            && v.vec.type != DRAKEN_DECIMAL128) {
        // E37: reuse the scan-carried seed if present (string columns only; the
        // producer sets keyhash_buf iff it equals str_hash_seed — presence ==
        // validity). Byte-identical to draken_hash_shaped; recompute otherwise.
        if (v.keyhash_buf) {
            g_e37_carried_hits.fetch_add(1, std::memory_order_relaxed);
            return vecresult_to_owner(draken_hash_shaped_carried(v.vec, v.keyhash_buf.get()));
        }
        return vecresult_to_owner(draken_hash_shaped(v.vec));
    }
    // Dense fallback for NULL / FP16 / DECIMAL128: materialise n row hashes.
    // DECIMAL128 has no OpsTable hash slot (its hash is boundary-only); the
    // per-row hash here is the SAME cross-tier-consistent logic as .hash(), so a
    // DECIMAL128 key collides with the int64-decimal of the same value.
    uint64_t* out = static_cast<uint64_t*>(
        draken_malloc((n > 0u ? n : 1u) * sizeof(uint64_t)));
    if (!out) throw std::bad_alloc();
    OwnedBuffer<uint64_t> out_owned(out);
    uint64_t scratch[1024];
    uint32_t i = 0u;
    if (v.vec.type == DRAKEN_NULL) {
        while (i < n) {
            const uint32_t block = (n - i < 1024u) ? (n - i) : 1024u;
            for (uint32_t j = 0u; j < block; ++j) scratch[j] = NULL_HASH;
            simd_hash_i64(scratch, out + i, block);
            i += block;
        }
    } else if (v.vec.type == DRAKEN_DECIMAL128) {
        while (i < n) {
            const uint32_t block = (n - i < 1024u) ? (n - i) : 1024u;
            for (uint32_t j = 0u; j < block; ++j) {
                if (!row_is_valid(v.vec, i + j)) {
                    scratch[j] = NULL_HASH;
                } else {
                    const __int128 x = row_int128(v.vec, i + j);
                    const uint64_t lo = static_cast<uint64_t>(x);
                    const uint64_t hi = static_cast<uint64_t>(x >> 64);
                    scratch[j] = (hi == static_cast<uint64_t>(static_cast<int64_t>(lo) >> 63))
                        ? lo
                        : (lo ^ (hi * 0x9E3779B97F4A7C15ULL));
                }
            }
            simd_hash_i64(scratch, out + i, block);
            i += block;
        }
    } else {  // FP16
        require_fp16_descriptor(v, "hash_shaped");
        const uint32_t dim = v.logical_type->dimension;
        const uint16_t* data = static_cast<const uint16_t*>(v.vec.data);
        while (i < n) {
            const uint32_t block = (n - i < 1024u) ? (n - i) : 1024u;
            for (uint32_t j = 0u; j < block; ++j) {
                scratch[j] = row_is_valid(v.vec, i + j)
                    ? fp16_row_fnv_seed(data + v.vec.selection[i + j]
                                             * static_cast<size_t>(dim), dim)
                    : NULL_HASH;
            }
            simd_hash_i64(scratch, out + i, block);
            i += block;
        }
    }
    VecResult r;
    r.data = out_owned.release();
    r.validity = nullptr;
    r.selection = draken_identity_sel(n);
    r.owns_selection = false;
    r.data_length = n;
    r.length = n;
    r.type = DRAKEN_INT64;
    r.flags = static_cast<uint8_t>(DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION);
    return vecresult_to_owner(r);
}
}  // namespace draken::owner_ops

using namespace draken::owner_ops;

// draken_vecresult_child_owner_new_c — see core/draken_capi.h. Delegates to the SAME
// vecresult_to_owner used by draken_vector_own, so a child gets identical
// TIMESTAMP64/DECIMAL descriptor handling and recursive child adoption — just
// without the Python-wrapping step.
extern "C" VectorOwner* draken_vecresult_child_owner_new_c(VecResult res) {
    return new VectorOwner(vecresult_to_owner(res));
}

extern "C" void draken_vecresult_discard_c(VecResult* res) {
    if (res == nullptr) return;
    { VectorOwner tmp = vecresult_to_owner(*res); }   // RAII frees data/validity/selection/child
    delete res;
}

// draken_hash_rows — each logical row's Vector.hash_shaped() value, through the
// SAME hash_shaped_impl, into out[0..v->vec.length). For native statistics that
// must agree with what the binding computes (the KMV sketches a manifest
// stores). Pure C++, GIL-free. Returns 0, or -1 with the reason in `error`
// (hash_shaped refuses DRAKEN_ARRAY; a VECTOR needs its logical type).
extern "C" int draken_hash_rows(const VectorOwner* v, uint64_t* out, char* error, size_t error_len) {
    try {
        const VectorOwner hashed = hash_shaped_impl(*v);
        const uint64_t* data = static_cast<const uint64_t*>(hashed.vec.data);
        for (uint32_t i = 0; i < hashed.vec.length; ++i) out[i] = data[hashed.vec.selection[i]];
        return 0;
    } catch (const std::exception& e) {
        std::snprintf(error, error_len, "%s", e.what());
        return -1;
    }
}
