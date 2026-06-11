#pragma once
// draken/ops/interval_ops.h — All kernels for DRAKEN_INTERVAL (Milestone D.12).
//
// Physical layout: DrakenIntervalSlot { int64_t months; int64_t ms; }, 16 bytes/row.
// Access pattern: data[selection[i]] as DrakenIntervalSlot* for logical row i.
//
// NORMALIZATION for compare/hash/order/between/in_list:
//   total_ms = months × INTERVAL_MONTH_MS + ms
//   (INTERVAL_MONTH_MS = 2_592_000_000; 1 month = 30 days; 1 day = 86_400_000 ms)
//   Normalization overflow → std::overflow_error (fail loud, never silent).
//
// ARITHMETIC is component-wise (months and ms independently):
//   add/sub: months±months, ms±ms.  neg: −months, −ms.
//   mul/div/mod/scalar arithmetic: unsupported (null dispatch slots → throw).
//
// MOVEMENT (take/materialize/compress): standard 16-byte slot gather.
//
// MIN/MAX: use interval_find_min / interval_find_max (custom scans) at the
//   Python edge; not registered in the ReduceFn slots (different return shape).

#include <cstdint>
#include <stddef.h>
#include <string.h>
#include <stdexcept>
#include <climits>
#include <unordered_map>
#include <vector>

#include "core/buffers.h"
#include "core/alloc.h"
#include "core/vector_alloc.h"
#include "core/interval_slot.h"
#include "ops/vec_result.h"
#include "simd_hash.h"
#include "ops/int64_predicates.h"   // CarcharSet (in_list)

namespace draken { namespace ops {

// ---------------------------------------------------------------------------
// Normalization helpers
// ---------------------------------------------------------------------------

// Unchecked: used in kernels that trust stored data (validated at ingestion).
static inline int64_t interval_normalize_unchecked(int64_t months, int64_t ms) noexcept {
    return months * INTERVAL_MONTH_MS + ms;
}

// Checked: used at ingestion and for Python-edge scalars.
// Throws std::overflow_error on months × INTERVAL_MONTH_MS overflow or
// subsequent addition overflow with ms.
static inline int64_t interval_normalize_checked(int64_t months, int64_t ms) {
    // Check months × INTERVAL_MONTH_MS for int64 overflow.
    if (months != 0) {
        if ((months > 0 && months > INT64_MAX / INTERVAL_MONTH_MS) ||
            (months < 0 && months < INT64_MIN / INTERVAL_MONTH_MS))
            throw std::overflow_error(
                "interval: normalization overflow (months value too large)");
    }
    const int64_t months_ms = months * INTERVAL_MONTH_MS;
    // Check months_ms + ms for int64 overflow.
    if ((ms > 0 && months_ms > INT64_MAX - ms) ||
        (ms < 0 && months_ms < INT64_MIN - ms))
        throw std::overflow_error(
            "interval: normalization overflow (months × INTERVAL_MONTH_MS + ms)");
    return months_ms + ms;
}

// ---------------------------------------------------------------------------
// Internal validity helpers (mirrors int64_gather.h)
// ---------------------------------------------------------------------------

static inline bool iv_row_valid(const uint8_t* validity, uint32_t i) noexcept {
    return (validity == nullptr) || ((validity[i >> 3] >> (i & 7)) & 1u);
}

static inline void iv_set_valid(uint8_t* bitmap, uint32_t i) noexcept {
    bitmap[i >> 3] |= static_cast<uint8_t>(1u << (i & 7));
}

// Normalize: if every logical bit is set (all valid), free it and return nullptr
// so downstream code can take the validity==nullptr fast path. Mirrors
// int64_gather.h::normalize_validity.
static inline uint8_t* iv_normalize_validity(uint8_t* validity, uint32_t n) noexcept {
    if (validity == nullptr) return nullptr;
    const uint32_t nb = (n + 7u) >> 3;
    for (uint32_t k = 0; k < nb; ++k) {
        uint8_t expected = 0xFFu;
        if (k == nb - 1 && (n & 7u) != 0)
            expected = static_cast<uint8_t>((1u << (n & 7u)) - 1u);
        if (validity[k] != expected) return validity;
    }
    draken_free(validity);
    return nullptr;
}

// ---------------------------------------------------------------------------
// Bool result allocation helper
// ---------------------------------------------------------------------------
static inline void iv_alloc_bool_result(uint32_t n,
    uint8_t*& out_bits, uint8_t*& out_null, uint8_t*& out_null_if_any)
{
    const uint32_t nb     = (n + 7u) >> 3;
    const uint32_t padded = ((nb + 7u) & ~7u);
    const size_t   alloc  = (padded > 0u) ? padded : 8u;
    out_bits = static_cast<uint8_t*>(draken_malloc(alloc));
    if (!out_bits) throw std::bad_alloc();
    memset(out_bits, 0, alloc);
    out_null = nullptr;
    out_null_if_any = nullptr;
}

static inline uint8_t* iv_alloc_null_bitmap(uint32_t n) {
    const uint32_t nb     = (n + 7u) >> 3;
    const uint32_t padded = ((nb + 7u) & ~7u);
    const size_t   alloc  = (padded > 0u) ? padded : 8u;
    uint8_t* bm = static_cast<uint8_t*>(draken_malloc(alloc));
    if (!bm) throw std::bad_alloc();
    memset(bm, 0xFF, alloc);  // all-valid; nulls cleared below
    return bm;
}

static inline VecResult iv_make_bool_result(uint8_t* bits, uint8_t* validity, uint32_t n) {
    VecResult r;
    r.data           = bits;
    r.validity       = validity;
    r.selection      = draken_identity_sel(n);
    r.owns_selection = false;
    r.data_length    = n;
    r.length         = n;
    r.type           = DRAKEN_BOOL;
    r.flags          = static_cast<uint8_t>(DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION);
    return r;
}

// ---------------------------------------------------------------------------
// HASH — normalize each row to total_ms, pass through simd_hash_i64.
// Null rows receive NULL_HASH (same convention as other types).
// ---------------------------------------------------------------------------
static inline void interval_hash(const DrakenVector& v, uint64_t* out, uint32_t n) {
    if (n == 0) return;
    const DrakenIntervalSlot* data = static_cast<const DrakenIntervalSlot*>(v.data);
    const uint8_t*            validity = v.validity;
    uint64_t scratch[1024];

    uint32_t i = 0;
    while (i < n) {
        const uint32_t block = (n - i < 1024u) ? (n - i) : 1024u;
        if (validity != nullptr) {
            for (uint32_t j = 0; j < block; ++j) {
                const uint64_t is_valid =
                    (validity[(i + j) >> 3] >> ((i + j) & 7u)) & 1u;
                const DrakenIntervalSlot& s = data[v.selection[i + j]];
                const int64_t norm = interval_normalize_unchecked(s.months, s.ms);
                scratch[j] =
                    (static_cast<uint64_t>(norm) * is_valid)
                    | (NULL_HASH * (1u - is_valid));
            }
        } else {
            for (uint32_t j = 0; j < block; ++j) {
                const DrakenIntervalSlot& s = data[v.selection[i + j]];
                scratch[j] = static_cast<uint64_t>(
                    interval_normalize_unchecked(s.months, s.ms));
            }
        }
        simd_hash_i64(scratch, out + i, block);
        i += block;
    }
}

// ---------------------------------------------------------------------------
// COMPARE_SCALAR — compare each row's normalized total_ms against norm_scalar.
// norm_scalar: already normalized total_ms (caller normalizes with _checked).
// op: 0=eq 1=ne 2=gt 3=ge 4=lt 5=le. Result: bit-packed DRAKEN_BOOL.
// Null row → null result (3VL).
// ---------------------------------------------------------------------------
static inline VecResult interval_compare_scalar(
    const DrakenVector& v, int64_t norm_scalar, int op)
{
    const uint32_t n = v.length;
    const DrakenIntervalSlot* data = static_cast<const DrakenIntervalSlot*>(v.data);
    const uint8_t* validity = v.validity;

    const uint32_t nb     = (n + 7u) >> 3;
    const uint32_t padded = ((nb + 7u) & ~7u);
    const size_t   alloc  = (padded > 0u) ? padded : 8u;

    uint8_t* bits = static_cast<uint8_t*>(draken_malloc(alloc));
    if (!bits) throw std::bad_alloc();
    memset(bits, 0, alloc);

    uint8_t* out_null = nullptr;
    if (validity != nullptr && n > 0) {
        out_null = static_cast<uint8_t*>(draken_malloc(alloc));
        if (!out_null) { draken_free(bits); throw std::bad_alloc(); }
        memset(out_null, 0, alloc);
    }

    for (uint32_t i = 0; i < n; ++i) {
        if (validity != nullptr && !((validity[i >> 3] >> (i & 7u)) & 1u)) {
            // null row → null result; bit stays 0 in both bits and out_null
            continue;
        }
        const DrakenIntervalSlot& s = data[v.selection[i]];
        const int64_t row_ms = interval_normalize_unchecked(s.months, s.ms);
        bool result;
        switch (op) {
            case 0: result = (row_ms == norm_scalar); break;
            case 1: result = (row_ms != norm_scalar); break;
            case 2: result = (row_ms >  norm_scalar); break;
            case 3: result = (row_ms >= norm_scalar); break;
            case 4: result = (row_ms <  norm_scalar); break;
            case 5: result = (row_ms <= norm_scalar); break;
            default:
                draken_free(bits);
                if (out_null) draken_free(out_null);
                throw std::invalid_argument("interval_compare_scalar: unknown op code");
        }
        if (result) bits[i >> 3] |= static_cast<uint8_t>(1u << (i & 7u));
        if (out_null) iv_set_valid(out_null, i);
    }

    return iv_make_bool_result(bits, out_null, n);
}

// ---------------------------------------------------------------------------
// COMPARE_VECTOR — row-wise compare of two INTERVAL vectors.
// Normalizes both sides. Both must be DRAKEN_INTERVAL and same length.
// op: 0=eq 1=ne 2=gt 3=ge 4=lt 5=le. Result: bit-packed DRAKEN_BOOL.
// Either row null → null result (3VL).
// ---------------------------------------------------------------------------
static inline VecResult interval_compare_vector(
    const DrakenVector& a, const DrakenVector& b, int op)
{
    const uint32_t n = a.length;
    const DrakenIntervalSlot* da = static_cast<const DrakenIntervalSlot*>(a.data);
    const DrakenIntervalSlot* db = static_cast<const DrakenIntervalSlot*>(b.data);

    const uint32_t nb     = (n + 7u) >> 3;
    const uint32_t padded = ((nb + 7u) & ~7u);
    const size_t   alloc  = (padded > 0u) ? padded : 8u;

    uint8_t* bits = static_cast<uint8_t*>(draken_malloc(alloc));
    if (!bits) throw std::bad_alloc();
    memset(bits, 0, alloc);

    const bool has_nulls = (a.validity != nullptr || b.validity != nullptr);
    uint8_t* out_null = nullptr;
    if (has_nulls && n > 0) {
        out_null = static_cast<uint8_t*>(draken_malloc(alloc));
        if (!out_null) { draken_free(bits); throw std::bad_alloc(); }
        memset(out_null, 0, alloc);
    }

    for (uint32_t i = 0; i < n; ++i) {
        const bool va = iv_row_valid(a.validity, i);
        const bool vb = iv_row_valid(b.validity, i);
        if (!va || !vb) continue;  // null result; bit stays 0
        const int64_t ma = interval_normalize_unchecked(
            da[a.selection[i]].months, da[a.selection[i]].ms);
        const int64_t mb = interval_normalize_unchecked(
            db[b.selection[i]].months, db[b.selection[i]].ms);
        bool result;
        switch (op) {
            case 0: result = (ma == mb); break;
            case 1: result = (ma != mb); break;
            case 2: result = (ma >  mb); break;
            case 3: result = (ma >= mb); break;
            case 4: result = (ma <  mb); break;
            case 5: result = (ma <= mb); break;
            default:
                draken_free(bits);
                if (out_null) draken_free(out_null);
                throw std::invalid_argument("interval_compare_vector: unknown op code");
        }
        if (result) bits[i >> 3] |= static_cast<uint8_t>(1u << (i & 7u));
        if (out_null) iv_set_valid(out_null, i);
    }

    return iv_make_bool_result(bits, out_null, n);
}

// ---------------------------------------------------------------------------
// BETWEEN — range membership on normalized total_ms.
// lo_ms / hi_ms: normalized total_ms for the bounds (caller checks with _checked).
// lo_incl / hi_incl: whether the bounds are inclusive.
// Null row → null result (3VL).
// ---------------------------------------------------------------------------
static inline VecResult interval_between(
    const DrakenVector& v,
    int64_t lo_ms, int64_t hi_ms,
    bool lo_incl, bool hi_incl)
{
    const uint32_t n = v.length;
    const DrakenIntervalSlot* data = static_cast<const DrakenIntervalSlot*>(v.data);
    const uint8_t* validity = v.validity;

    const uint32_t nb     = (n + 7u) >> 3;
    const uint32_t padded = ((nb + 7u) & ~7u);
    const size_t   alloc  = (padded > 0u) ? padded : 8u;

    uint8_t* bits = static_cast<uint8_t*>(draken_malloc(alloc));
    if (!bits) throw std::bad_alloc();
    memset(bits, 0, alloc);

    uint8_t* out_null = nullptr;
    if (validity != nullptr && n > 0) {
        out_null = static_cast<uint8_t*>(draken_malloc(alloc));
        if (!out_null) { draken_free(bits); throw std::bad_alloc(); }
        memset(out_null, 0, alloc);
    }

    for (uint32_t i = 0; i < n; ++i) {
        if (validity != nullptr && !((validity[i >> 3] >> (i & 7u)) & 1u)) continue;
        const DrakenIntervalSlot& s = data[v.selection[i]];
        const int64_t ms = interval_normalize_unchecked(s.months, s.ms);
        const bool lo_ok = lo_incl ? (ms >= lo_ms) : (ms > lo_ms);
        const bool hi_ok = hi_incl ? (ms <= hi_ms) : (ms < hi_ms);
        if (lo_ok && hi_ok) bits[i >> 3] |= static_cast<uint8_t>(1u << (i & 7u));
        if (out_null) iv_set_valid(out_null, i);
    }

    return iv_make_bool_result(bits, out_null, n);
}

// ---------------------------------------------------------------------------
// IN_LIST — hash-only set membership on normalized total_ms.
// §1 exception (same as int64): hash probe only, no key verification.
// Caller pre-builds the CarcharSet from normalized total_ms values.
// Null row → null result (3VL).
// ---------------------------------------------------------------------------
static inline VecResult interval_in_list(
    const DrakenVector& v, const opteryx::carchar::CarcharSet& set)
{
    const uint32_t n = v.length;
    const DrakenIntervalSlot* data = static_cast<const DrakenIntervalSlot*>(v.data);
    const uint8_t* validity = v.validity;

    const uint32_t nb     = (n + 7u) >> 3;
    const uint32_t padded = ((nb + 7u) & ~7u);
    const size_t   alloc  = (padded > 0u) ? padded : 8u;

    uint8_t* bits = static_cast<uint8_t*>(draken_malloc(alloc));
    if (!bits) throw std::bad_alloc();
    memset(bits, 0, alloc);

    uint8_t* out_null = nullptr;
    if (validity != nullptr && n > 0) {
        out_null = static_cast<uint8_t*>(draken_malloc(alloc));
        if (!out_null) { draken_free(bits); throw std::bad_alloc(); }
        memset(out_null, 0, alloc);
    }

    for (uint32_t i = 0; i < n; ++i) {
        if (validity != nullptr && !((validity[i >> 3] >> (i & 7u)) & 1u)) continue;
        const DrakenIntervalSlot& s = data[v.selection[i]];
        const int64_t norm = interval_normalize_unchecked(s.months, s.ms);
        uint64_t raw = static_cast<uint64_t>(norm);
        uint64_t h;
        simd_hash_i64(&raw, &h, 1u);
        if (set.contains(h)) bits[i >> 3] |= static_cast<uint8_t>(1u << (i & 7u));
        if (out_null) iv_set_valid(out_null, i);
    }

    return iv_make_bool_result(bits, out_null, n);
}

// ---------------------------------------------------------------------------
// ADD / SUB — component-wise interval arithmetic.
// Both vectors must be DRAKEN_INTERVAL and same length.
// Result: dense DRAKEN_INTERVAL with component-wise sums.
// Either row null → null output row.
// ---------------------------------------------------------------------------
static inline VecResult interval_add(const DrakenVector& a, const DrakenVector& b) {
    const uint32_t n = a.length;
    const DrakenIntervalSlot* da = static_cast<const DrakenIntervalSlot*>(a.data);
    const DrakenIntervalSlot* db = static_cast<const DrakenIntervalSlot*>(b.data);

    const size_t data_bytes = (n > 0u ? n : 1u) * sizeof(DrakenIntervalSlot);
    DrakenIntervalSlot* dst = static_cast<DrakenIntervalSlot*>(draken_malloc(data_bytes));
    if (!dst) throw std::bad_alloc();

    const bool has_nulls = (a.validity != nullptr || b.validity != nullptr);
    uint8_t* out_null = nullptr;
    if (has_nulls && n > 0) {
        const uint32_t nb = (n + 7u) >> 3;
        out_null = static_cast<uint8_t*>(draken_malloc(nb));
        if (!out_null) { draken_free(dst); throw std::bad_alloc(); }
        memset(out_null, 0, nb);
    }

    for (uint32_t i = 0; i < n; ++i) {
        if (!iv_row_valid(a.validity, i) || !iv_row_valid(b.validity, i)) {
            dst[i] = {0, 0};
            continue;
        }
        const DrakenIntervalSlot& sa = da[a.selection[i]];
        const DrakenIntervalSlot& sb = db[b.selection[i]];
        dst[i].months = sa.months + sb.months;
        dst[i].ms     = sa.ms     + sb.ms;
        if (out_null) iv_set_valid(out_null, i);
    }

    VecResult r;
    r.data           = dst;
    r.validity       = out_null;
    r.selection      = draken_identity_sel(n);
    r.owns_selection = false;
    r.data_length    = n;
    r.length         = n;
    r.type           = DRAKEN_INTERVAL;
    r.flags          = static_cast<uint8_t>(DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION);
    return r;
}

static inline VecResult interval_sub(const DrakenVector& a, const DrakenVector& b) {
    const uint32_t n = a.length;
    const DrakenIntervalSlot* da = static_cast<const DrakenIntervalSlot*>(a.data);
    const DrakenIntervalSlot* db = static_cast<const DrakenIntervalSlot*>(b.data);

    const size_t data_bytes = (n > 0u ? n : 1u) * sizeof(DrakenIntervalSlot);
    DrakenIntervalSlot* dst = static_cast<DrakenIntervalSlot*>(draken_malloc(data_bytes));
    if (!dst) throw std::bad_alloc();

    const bool has_nulls = (a.validity != nullptr || b.validity != nullptr);
    uint8_t* out_null = nullptr;
    if (has_nulls && n > 0) {
        const uint32_t nb = (n + 7u) >> 3;
        out_null = static_cast<uint8_t*>(draken_malloc(nb));
        if (!out_null) { draken_free(dst); throw std::bad_alloc(); }
        memset(out_null, 0, nb);
    }

    for (uint32_t i = 0; i < n; ++i) {
        if (!iv_row_valid(a.validity, i) || !iv_row_valid(b.validity, i)) {
            dst[i] = {0, 0};
            continue;
        }
        const DrakenIntervalSlot& sa = da[a.selection[i]];
        const DrakenIntervalSlot& sb = db[b.selection[i]];
        dst[i].months = sa.months - sb.months;
        dst[i].ms     = sa.ms     - sb.ms;
        if (out_null) iv_set_valid(out_null, i);
    }

    VecResult r;
    r.data           = dst;
    r.validity       = out_null;
    r.selection      = draken_identity_sel(n);
    r.owns_selection = false;
    r.data_length    = n;
    r.length         = n;
    r.type           = DRAKEN_INTERVAL;
    r.flags          = static_cast<uint8_t>(DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION);
    return r;
}

// ---------------------------------------------------------------------------
// NEG — negate both components.
// Null row → null output row.
// ---------------------------------------------------------------------------
static inline VecResult interval_neg(const DrakenVector& v) {
    const uint32_t n = v.length;
    const DrakenIntervalSlot* data = static_cast<const DrakenIntervalSlot*>(v.data);

    const size_t data_bytes = (n > 0u ? n : 1u) * sizeof(DrakenIntervalSlot);
    DrakenIntervalSlot* dst = static_cast<DrakenIntervalSlot*>(draken_malloc(data_bytes));
    if (!dst) throw std::bad_alloc();

    uint8_t* out_null = nullptr;
    if (v.validity != nullptr && n > 0) {
        const uint32_t nb = (n + 7u) >> 3;
        out_null = static_cast<uint8_t*>(draken_malloc(nb));
        if (!out_null) { draken_free(dst); throw std::bad_alloc(); }
        memcpy(out_null, v.validity, nb);
    }

    for (uint32_t i = 0; i < n; ++i) {
        if (!iv_row_valid(v.validity, i)) {
            dst[i] = {0, 0};
            continue;
        }
        const DrakenIntervalSlot& s = data[v.selection[i]];
        dst[i].months = -s.months;
        dst[i].ms     = -s.ms;
    }

    VecResult r;
    r.data           = dst;
    r.validity       = out_null;
    r.selection      = draken_identity_sel(n);
    r.owns_selection = false;
    r.data_length    = n;
    r.length         = n;
    r.type           = DRAKEN_INTERVAL;
    r.flags          = static_cast<uint8_t>(DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION);
    return r;
}

// ---------------------------------------------------------------------------
// TAKE — gather rows by index list; result is dense DRAKEN_INTERVAL.
// Null source row → null output row; propagates validity.
// ---------------------------------------------------------------------------
static inline VecResult interval_slice(const DrakenVector& v, uint32_t start, uint32_t length) {
    const uint32_t n = length;
    const DrakenIntervalSlot* data = static_cast<const DrakenIntervalSlot*>(v.data);
    const uint8_t* src_null = v.validity;

    const size_t data_bytes = (n > 0u ? n : 1u) * sizeof(DrakenIntervalSlot);
    DrakenIntervalSlot* dst = static_cast<DrakenIntervalSlot*>(draken_malloc(data_bytes));
    if (!dst) throw std::bad_alloc();

    // Physical memcpy valid ONLY when selection is identity; data_length==length
    // also admits a PERMUTATION which would silently reorder. Require IDENTITY.
    if (draken_is_dense(&v) && (v.flags & DRAKEN_SEL_IDENTITY)) {
        std::memcpy(dst, data + start, n * sizeof(DrakenIntervalSlot));
    } else {
        for (uint32_t i = 0; i < n; ++i)
            dst[i] = data[v.selection[start + i]];
    }

    uint8_t* out_null = nullptr;
    if (src_null != nullptr && n > 0u) {
        const uint32_t nb = (n + 7u) >> 3;
        out_null = static_cast<uint8_t*>(draken_malloc(nb));
        if (!out_null) { draken_free(dst); throw std::bad_alloc(); }
        copy_validity_range(out_null, src_null, start, n);
        out_null = iv_normalize_validity(out_null, n);
    }

    VecResult r;
    r.data           = dst;
    r.validity       = out_null;
    r.selection      = draken_identity_sel(n);
    r.owns_selection = false;
    r.data_length    = n;
    r.length         = n;
    r.type           = DRAKEN_INTERVAL;
    r.flags          = static_cast<uint8_t>(DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION);
    return r;
}

static inline VecResult interval_take(
    const DrakenVector& v, const int32_t* indices, uint32_t n_indices)
{
    const uint32_t n = n_indices;
    const DrakenIntervalSlot* data = static_cast<const DrakenIntervalSlot*>(v.data);
    const uint8_t* src_null = v.validity;

    const size_t data_bytes = (n > 0u ? n : 1u) * sizeof(DrakenIntervalSlot);
    DrakenIntervalSlot* dst = static_cast<DrakenIntervalSlot*>(draken_malloc(data_bytes));
    if (!dst) throw std::bad_alloc();

    uint8_t* out_null = nullptr;
    if (src_null != nullptr && n > 0) {
        const uint32_t nb = (n + 7u) >> 3;
        out_null = static_cast<uint8_t*>(draken_malloc(nb));
        if (!out_null) { draken_free(dst); throw std::bad_alloc(); }
        memset(out_null, 0, nb);
    }

    for (uint32_t i = 0; i < n; ++i) {
        const uint32_t src_idx = static_cast<uint32_t>(indices[i]);
        if (!iv_row_valid(src_null, src_idx)) {
            dst[i] = {0, 0};
        } else {
            dst[i] = data[v.selection[src_idx]];
            if (out_null) iv_set_valid(out_null, i);
        }
    }

    VecResult r;
    r.data           = dst;
    r.validity       = out_null;
    r.selection      = draken_identity_sel(n);
    r.owns_selection = false;
    r.data_length    = n;
    r.length         = n;
    r.type           = DRAKEN_INTERVAL;
    r.flags          = static_cast<uint8_t>(DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION);
    return r;
}

// ---------------------------------------------------------------------------
// MATERIALIZE — expand any shape to a dense identity-selection vector.
// ---------------------------------------------------------------------------
static inline VecResult interval_materialize(const DrakenVector& v) {
    const uint32_t n = v.length;
    const DrakenIntervalSlot* data = static_cast<const DrakenIntervalSlot*>(v.data);
    const uint8_t* src_null = v.validity;

    const size_t data_bytes = (n > 0u ? n : 1u) * sizeof(DrakenIntervalSlot);
    DrakenIntervalSlot* dst = static_cast<DrakenIntervalSlot*>(draken_malloc(data_bytes));
    if (!dst) throw std::bad_alloc();

    for (uint32_t i = 0; i < n; ++i) {
        if (!iv_row_valid(src_null, i)) {
            dst[i] = {0, 0};
        } else {
            dst[i] = data[v.selection[i]];
        }
    }

    uint8_t* out_null = nullptr;
    if (src_null != nullptr && n > 0) {
        const uint32_t nb = (n + 7u) >> 3;
        out_null = static_cast<uint8_t*>(draken_malloc(nb));
        if (!out_null) { draken_free(dst); throw std::bad_alloc(); }
        memcpy(out_null, src_null, nb);
    }

    VecResult r;
    r.data           = dst;
    r.validity       = out_null;
    r.selection      = draken_identity_sel(n);
    r.owns_selection = false;
    r.data_length    = n;
    r.length         = n;
    r.type           = DRAKEN_INTERVAL;
    r.flags          = static_cast<uint8_t>(DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION);
    return r;
}

// ---------------------------------------------------------------------------
// COMPRESS — keep only valid rows in a dense all-valid output.
// ---------------------------------------------------------------------------
static inline VecResult interval_compress(const DrakenVector& v) {
    const uint32_t n = v.length;
    const DrakenIntervalSlot* data = static_cast<const DrakenIntervalSlot*>(v.data);
    const uint8_t* src_null = v.validity;

    // Count valid rows.
    uint32_t valid_count = 0u;
    if (src_null == nullptr) {
        valid_count = n;
    } else {
        for (uint32_t i = 0; i < n; ++i)
            if (iv_row_valid(src_null, i)) ++valid_count;
    }

    if (n == 0) {
        DrakenIntervalSlot* d = static_cast<DrakenIntervalSlot*>(
            draken_malloc(sizeof(DrakenIntervalSlot)));
        if (!d) throw std::bad_alloc();
        d[0] = {0, 0};
        VecResult r;
        r.data = d; r.validity = nullptr;
        r.selection = draken_identity_sel(0u); r.owns_selection = false;
        r.data_length = 0; r.length = 0; r.type = DRAKEN_INTERVAL; r.flags = 0;
        return r;
    }

    const size_t data_bytes = (valid_count > 0u ? valid_count : 1u)
                              * sizeof(DrakenIntervalSlot);
    DrakenIntervalSlot* dst = static_cast<DrakenIntervalSlot*>(draken_malloc(data_bytes));
    if (!dst) throw std::bad_alloc();

    uint32_t out_idx = 0u;
    for (uint32_t i = 0; i < n; ++i) {
        if (iv_row_valid(src_null, i)) {
            dst[out_idx++] = data[v.selection[i]];
        }
    }

    VecResult r;
    r.data           = dst;
    r.validity       = nullptr;  // all-valid output
    r.selection      = draken_identity_sel(valid_count);
    r.owns_selection = false;
    r.data_length    = valid_count;
    r.length         = valid_count;
    r.type           = DRAKEN_INTERVAL;
    r.flags          = static_cast<uint8_t>(DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION);
    return r;
}

// ---------------------------------------------------------------------------
// MIN / MAX — custom scans returning the original (months, ms) slot of the
// row with the minimum/maximum normalized total_ms.
//
// These don't fit ReduceFn (need two output values); called directly from the
// Python binding, not via the dispatch table.
// ---------------------------------------------------------------------------

struct IntervalMinMaxResult {
    int64_t months;
    int64_t ms;
    bool    found;
};

static inline IntervalMinMaxResult interval_find_min(const DrakenVector& v) {
    const uint32_t n = v.length;
    const DrakenIntervalSlot* data = static_cast<const DrakenIntervalSlot*>(v.data);
    const uint8_t* validity = v.validity;

    IntervalMinMaxResult result = {0, 0, false};
    int64_t best = INT64_MAX;

    for (uint32_t i = 0; i < n; ++i) {
        if (!iv_row_valid(validity, i)) continue;
        const DrakenIntervalSlot& s = data[v.selection[i]];
        const int64_t norm = interval_normalize_unchecked(s.months, s.ms);
        if (!result.found || norm < best) {
            best = norm;
            result.months = s.months;
            result.ms     = s.ms;
            result.found  = true;
        }
    }
    return result;
}

static inline IntervalMinMaxResult interval_find_max(const DrakenVector& v) {
    const uint32_t n = v.length;
    const DrakenIntervalSlot* data = static_cast<const DrakenIntervalSlot*>(v.data);
    const uint8_t* validity = v.validity;

    IntervalMinMaxResult result = {0, 0, false};
    int64_t best = INT64_MIN;

    for (uint32_t i = 0; i < n; ++i) {
        if (!iv_row_valid(validity, i)) continue;
        const DrakenIntervalSlot& s = data[v.selection[i]];
        const int64_t norm = interval_normalize_unchecked(s.months, s.ms);
        if (!result.found || norm > best) {
            best = norm;
            result.months = s.months;
            result.ms     = s.ms;
            result.found  = true;
        }
    }
    return result;
}

}} // namespace draken::ops
