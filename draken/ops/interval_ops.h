#pragma once
// draken/ops/interval_ops.h — All kernels for DRAKEN_INTERVAL (Milestone D.12).
//
// Physical layout: DrakenIntervalSlot { int64_t months; int64_t us; }, 16 bytes/row.
// The sub-month field `us` carries MICROSECONDS (the canonical engine unit).
// Access pattern: data[selection[i]] as DrakenIntervalSlot* for logical row i.
//
// NORMALIZATION for compare/hash/order/between/in_list:
//   total_us = months × INTERVAL_MONTH_US + us
//   (INTERVAL_MONTH_US = 2_592_000_000_000; 1 month = 30 days; 1 day = 86_400_000_000 µs)
//   Normalization overflow → std::overflow_error (fail loud, never silent).
//
// ARITHMETIC is component-wise (months and us independently):
//   add/sub: months±months, us±us.  neg: −months, −us.
//   mul/div/mod/scalar arithmetic: unsupported (null dispatch slots → throw).
//
// MOVEMENT (take/materialize/dictionary_encode): standard 16-byte slot gather.
//
// MIN/MAX: use interval_find_min / interval_find_max (custom scans) at the
//   Python edge; not registered in the ReduceFn slots (different return shape).

#include <cstdint>
#include <stddef.h>
#include <string.h>
#include <stdexcept>
#include <climits>
#include <string>
#include <cmath>       // std::trunc / std::nearbyint / std::isfinite (interval_scale)
#include <unordered_map>
#include <vector>

#include <new>        // std::bad_alloc / placement new — not reliably pulled in by <stdexcept> on stricter libc++
#include "core/buffers.h"
#include "core/alloc.h"
#include "core/vector_alloc.h"
#include "core/interval_slot.h"
#include "logical_type.h"           // TimestampUnit / ts_to_us
#include "ops/vec_result.h"
#include "ops/int64_gather.h"       // copy_validity_range (used by interval_slice/take)
#include "ops/temporal_arith.h"     // ta_days_to_ymd / ta_ymd_to_days / ta_floor_div
#include "simd_hash.h"
#include "ops/slice_shape.h"  // slice_keep_dict — the shared keep-or-flatten rule
#include "ops/int64_predicates.h"   // CarcharSet (in_list)

namespace draken { namespace ops {

// ---------------------------------------------------------------------------
// Normalization helpers
// ---------------------------------------------------------------------------

// Unchecked: used in kernels that trust stored data (validated at ingestion).
static inline int64_t interval_normalize_unchecked(int64_t months, int64_t us) noexcept {
    return months * INTERVAL_MONTH_US + us;
}

// Checked: used at ingestion and for Python-edge scalars.
// Throws std::overflow_error on months × INTERVAL_MONTH_US overflow or
// subsequent addition overflow with us.
static inline int64_t interval_normalize_checked(int64_t months, int64_t us) {
    // Check months × INTERVAL_MONTH_US for int64 overflow.
    if (months != 0) {
        if ((months > 0 && months > INT64_MAX / INTERVAL_MONTH_US) ||
            (months < 0 && months < INT64_MIN / INTERVAL_MONTH_US))
            throw std::overflow_error(
                "interval: normalization overflow (months value too large)");
    }
    const int64_t months_us = months * INTERVAL_MONTH_US;
    // Check months_us + us for int64 overflow.
    if ((us > 0 && months_us > INT64_MAX - us) ||
        (us < 0 && months_us < INT64_MIN - us))
        throw std::overflow_error(
            "interval: normalization overflow (months × INTERVAL_MONTH_US + us)");
    return months_us + us;
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
// HASH — normalize each row to total_us, pass through simd_hash_i64.
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
                const int64_t norm = interval_normalize_unchecked(s.months, s.us);
                scratch[j] =
                    (static_cast<uint64_t>(norm) * is_valid)
                    | (NULL_HASH * (1u - is_valid));
            }
        } else {
            for (uint32_t j = 0; j < block; ++j) {
                const DrakenIntervalSlot& s = data[v.selection[i + j]];
                scratch[j] = static_cast<uint64_t>(
                    interval_normalize_unchecked(s.months, s.us));
            }
        }
        simd_hash_i64(scratch, out + i, block);
        i += block;
    }
}

// ---------------------------------------------------------------------------
// COMPARE_SCALAR — compare each row's normalized total_us against norm_scalar.
// norm_scalar: already normalized total_us (caller normalizes with _checked).
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
        const int64_t row_us = interval_normalize_unchecked(s.months, s.us);
        bool result;
        switch (op) {
            case 0: result = (row_us == norm_scalar); break;
            case 1: result = (row_us != norm_scalar); break;
            case 2: result = (row_us >  norm_scalar); break;
            case 3: result = (row_us >= norm_scalar); break;
            case 4: result = (row_us <  norm_scalar); break;
            case 5: result = (row_us <= norm_scalar); break;
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
            da[a.selection[i]].months, da[a.selection[i]].us);
        const int64_t mb = interval_normalize_unchecked(
            db[b.selection[i]].months, db[b.selection[i]].us);
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
// BETWEEN — range membership on normalized total_us.
// lo_us / hi_us: normalized total_us for the bounds (caller checks with _checked).
// lo_incl / hi_incl: whether the bounds are inclusive.
// Null row → null result (3VL).
// ---------------------------------------------------------------------------
static inline VecResult interval_between(
    const DrakenVector& v,
    int64_t lo_us, int64_t hi_us,
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
        const int64_t total_us = interval_normalize_unchecked(s.months, s.us);
        const bool lo_ok = lo_incl ? (total_us >= lo_us) : (total_us > lo_us);
        const bool hi_ok = hi_incl ? (total_us <= hi_us) : (total_us < hi_us);
        if (lo_ok && hi_ok) bits[i >> 3] |= static_cast<uint8_t>(1u << (i & 7u));
        if (out_null) iv_set_valid(out_null, i);
    }

    return iv_make_bool_result(bits, out_null, n);
}

// ---------------------------------------------------------------------------
// IN_LIST — hash-only set membership on normalized total_us.
// §1 exception (same as int64): hash probe only, no key verification.
// Caller pre-builds the CarcharSet from normalized total_us values.
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
        const int64_t norm = interval_normalize_unchecked(s.months, s.us);
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
        dst[i].us     = sa.us     + sb.us;
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
        dst[i].us     = sa.us     - sb.us;
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
        dst[i].us     = -s.us;
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
// SCALE — INTERVAL × number, number × INTERVAL, INTERVAL ÷ number → INTERVAL.
//
// Postgres semantics. Months and µs are scaled separately; the fractional part of
// the scaled months SPILLS into µs at 30 days/month (INTERVAL_MONTH_US):
//   (1mo, 0) × 1.5 = (1mo, 15d)        (1mo, 0) ÷ 2 = (0mo, 15d)
//   (1mo, 1d) × 3  = (3mo, 3d)         (2y, 0)  ÷ 3 = (8mo, 0)
// An INTEGER factor/divisor is EXACT: int128 throughout, the month remainder of a
// division carried into µs before µs is divided, µs rounded half-to-even. A FLOAT
// factor goes through double (as Postgres does) — so a factor like 1/3.0 can land
// a hair off a whole month, e.g. (3mo) × (1/3.0) = 30 days, not 1 month.
//
// Fails loud, never a wrapped answer: overflow of either component, or of the
// normalised total (months × MONTH_US + µs — the ingestion invariant), throws, as
// does division by zero and a NaN/±inf factor. Null row on either side → null.
// `num` is any signed/unsigned integer or float vector (read via its selection).
// ---------------------------------------------------------------------------
static inline __int128 iv_div_round_half_even(__int128 n, __int128 d) {
    // d != 0. Quotient rounded to nearest, ties to even (C++ `/` truncates).
    if (d < 0) { n = -n; d = -d; }
    __int128 q = n / d;
    __int128 r = n % d;                  // same sign as n
    const __int128 twice = (r < 0 ? -r : r) * 2;
    if (twice > d || (twice == d && (q % 2 != 0))) q += (n < 0 ? -1 : 1);
    return q;
}

static inline void iv_store_checked(DrakenIntervalSlot& dst, __int128 months, __int128 us,
                                    const char* what) {
    // Components first: only once both are int64 is the total safe to form in int128.
    bool ok = months <= INT64_MAX && months >= INT64_MIN && us <= INT64_MAX && us >= INT64_MIN;
    if (ok) {
        const __int128 total = months * static_cast<__int128>(INTERVAL_MONTH_US) + us;
        ok = total <= INT64_MAX && total >= INT64_MIN;
    }
    if (!ok)
        throw std::overflow_error(std::string(what) +
            " overflow: the result does not fit an INTERVAL — fail loud, never a wrapped answer");
    dst.months = static_cast<int64_t>(months);
    dst.us     = static_cast<int64_t>(us);
}

static inline VecResult interval_scale(const DrakenVector& iv, const DrakenVector& num, bool divide) {
    const uint32_t n = iv.length;
    const DrakenIntervalSlot* data = static_cast<const DrakenIntervalSlot*>(iv.data);
    const char* what = divide ? "INTERVAL division" : "INTERVAL multiplication";

    bool is_int = true;
    switch (num.type) {
        case DRAKEN_INT8: case DRAKEN_INT16: case DRAKEN_INT32: case DRAKEN_INT64:
        case DRAKEN_UINT8: case DRAKEN_UINT16: case DRAKEN_UINT32: case DRAKEN_UINT64:
            break;
        case DRAKEN_FLOAT32: case DRAKEN_FLOAT64:
            is_int = false;
            break;
        default:
            throw std::invalid_argument("interval_scale: the factor must be an integer or float");
    }
    auto read_int = [&](uint32_t p) -> __int128 {
        switch (num.type) {
            case DRAKEN_INT8:   return static_cast<const int8_t*>(num.data)[p];
            case DRAKEN_INT16:  return static_cast<const int16_t*>(num.data)[p];
            case DRAKEN_INT32:  return static_cast<const int32_t*>(num.data)[p];
            case DRAKEN_INT64:  return static_cast<const int64_t*>(num.data)[p];
            case DRAKEN_UINT8:  return static_cast<const uint8_t*>(num.data)[p];
            case DRAKEN_UINT16: return static_cast<const uint16_t*>(num.data)[p];
            case DRAKEN_UINT32: return static_cast<const uint32_t*>(num.data)[p];
            default:            return static_cast<const uint64_t*>(num.data)[p];
        }
    };
    auto read_f64 = [&](uint32_t p) -> double {
        return num.type == DRAKEN_FLOAT32
            ? static_cast<double>(static_cast<const float*>(num.data)[p])
            : static_cast<const double*>(num.data)[p];
    };

    const size_t data_bytes = (n > 0u ? n : 1u) * sizeof(DrakenIntervalSlot);
    DrakenIntervalSlot* dst = static_cast<DrakenIntervalSlot*>(draken_malloc(data_bytes));
    if (!dst) throw std::bad_alloc();

    const bool has_nulls = (iv.validity != nullptr || num.validity != nullptr);
    uint8_t* out_null = nullptr;
    if (has_nulls && n > 0) {
        const uint32_t nb = (n + 7u) >> 3;
        out_null = static_cast<uint8_t*>(draken_malloc(nb));
        if (!out_null) { draken_free(dst); throw std::bad_alloc(); }
        memset(out_null, 0, nb);
    }

    const __int128 MONTH_US = INTERVAL_MONTH_US;
    // INT64 range as doubles: 2^63 is exact; anything >= it (or < -2^63) cannot convert.
    const double TWO_63 = 9223372036854775808.0;

    try {
        for (uint32_t i = 0; i < n; ++i) {
            if (!iv_row_valid(iv.validity, i) || !iv_row_valid(num.validity, i)) {
                dst[i] = {0, 0};
                continue;
            }
            const DrakenIntervalSlot& s = data[iv.selection[i]];
            const uint32_t p = num.selection[i];

            if (is_int) {
                const __int128 k = read_int(p);
                if (!divide) {
                    // |k| > 2^63 (a UINT64 factor) times any non-zero component is out
                    // of range anyway — and int64 × uint64 can overflow int128 itself.
                    if (k > INT64_MAX && (s.months != 0 || s.us != 0))
                        throw std::overflow_error(std::string(what) +
                            " overflow: the result does not fit an INTERVAL — fail loud, never a wrapped answer");
                    const __int128 kk = k > INT64_MAX ? 0 : k;
                    iv_store_checked(dst[i], s.months * kk, s.us * kk, what);
                } else {
                    if (k == 0) throw std::domain_error("INTERVAL division by zero");
                    const __int128 m = s.months;
                    const __int128 mq = m / k;                 // whole months, toward zero
                    const __int128 carried = s.us + (m - mq * k) * MONTH_US;
                    iv_store_checked(dst[i], mq, iv_div_round_half_even(carried, k), what);
                }
            } else {
                const double f = read_f64(p);
                if (!std::isfinite(f))
                    throw std::domain_error(std::string(what) + " by a NaN or infinite factor");
                if (divide && f == 0.0) throw std::domain_error("INTERVAL division by zero");
                const double mf = divide ? static_cast<double>(s.months) / f
                                         : static_cast<double>(s.months) * f;
                const double whole = std::trunc(mf);
                const double usf = (divide ? static_cast<double>(s.us) / f
                                           : static_cast<double>(s.us) * f)
                                   + (mf - whole) * static_cast<double>(INTERVAL_MONTH_US);
                const double usr = std::nearbyint(usf);   // default FE mode: ties to even
                if (!(whole < TWO_63 && whole >= -TWO_63 && usr < TWO_63 && usr >= -TWO_63))
                    throw std::overflow_error(std::string(what) +
                        " overflow: the result does not fit an INTERVAL — fail loud, never a wrapped answer");
                iv_store_checked(dst[i], static_cast<int64_t>(whole), static_cast<int64_t>(usr), what);
            }
            if (out_null) iv_set_valid(out_null, i);
        }
    } catch (...) {
        draken_free(dst);
        if (out_null) draken_free(out_null);
        throw;
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

    uint8_t* out_null = nullptr;
    if (src_null != nullptr && n > 0u) {
        const uint32_t nb = (n + 7u) >> 3;
        out_null = static_cast<uint8_t*>(draken_malloc(nb));
        if (!out_null) throw std::bad_alloc();
        copy_validity_range(out_null, src_null, start, n);
        out_null = iv_normalize_validity(out_null, n);
    }

    // Keep the source's dictionary when that copies strictly fewer bytes than
    // flattening it. One rule for every fixed-width family (ops/slice_shape.h);
    // the width arithmetic there makes this unreachable for types <= 4 bytes.
    VecResult kept;
    if (slice_keep_dict<DrakenIntervalSlot>(v, start, n, out_null, DRAKEN_INTERVAL, kept)) return kept;


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
static inline VecResult interval_dictionary_encode(const DrakenVector& v) {
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
// MIN / MAX — custom scans returning the original (months, us) slot of the
// row with the minimum/maximum normalized total_us.
//
// These don't fit ReduceFn (need two output values); called directly from the
// Python binding, not via the dispatch table.
// ---------------------------------------------------------------------------

struct IntervalMinMaxResult {
    int64_t months;
    int64_t us;
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
        const int64_t norm = interval_normalize_unchecked(s.months, s.us);
        if (!result.found || norm < best) {
            best = norm;
            result.months = s.months;
            result.us     = s.us;
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
        const int64_t norm = interval_normalize_unchecked(s.months, s.us);
        if (!result.found || norm > best) {
            best = norm;
            result.months = s.months;
            result.us     = s.us;
            result.found  = true;
        }
    }
    return result;
}

// ---------------------------------------------------------------------------
// CALENDAR ARITHMETIC — date/timestamp ± interval.
//
// Restores the SQL/DuckDB calendar semantics that the Arrow-backed
// IntervalVector.apply_to_temporal carried before the C++-first rebuild.
//
// Result is always DRAKEN_TIMESTAMP64 in MICROSECONDS (matches the binder's
// `_T` result type for DATE/TIMESTAMP ± INTERVAL, and DuckDB which returns
// TIMESTAMP for both date+interval and timestamp+interval).
//
// Algorithm (per row, matching DuckDB's component order — MONTHS FIRST):
//   1. Decompose the temporal value into (epoch_days, day_us).
//   2. Apply the months component on the calendar date with day-clamping:
//      ymd → add months (floor-div year rollover) → clamp day to last day of
//      the target month → back to epoch_days.
//   3. Apply the sub-month (µs) component: day_us += interval.us * signum, then
//      floor-divmod into (carry_days, day_us); epoch_days += carry_days.
//   4. result_us = epoch_days * US_PER_DAY + day_us.
//
// Component order MUST be months-then-µs to match DuckDB:
//   2020-02-29 + (1mo + 10day) = (2020-02-29 +1mo=2020-03-29) +10day = 2020-04-08,
//   NOT the days-first 2020-04-10. Verified against DuckDB.
//
// `src_is_date`: true  → temporal is DRAKEN_DATE32 (int32 days, day_us = 0).
//                false → temporal is DRAKEN_TIMESTAMP64 (int64 ticks in src_unit).
// `src_unit`: TimestampUnit (0..3) of the source TIMESTAMP64 (ignored for date).
// `signum`: +1 for Plus, -1 for Minus.
// Null temporal row OR null interval row → null output row.
// ---------------------------------------------------------------------------

// Days in (year, month [1-12]) — last calendar day, leap-year aware.
static inline int64_t iv_days_in_month(int year, int month) noexcept {
    int ny = year, nm = month + 1;
    if (nm > 12) { nm = 1; ny += 1; }
    return ta_ymd_to_days(ny, nm, 1) - ta_ymd_to_days(year, month, 1);
}

// ONE timestamp (µs since epoch) + ONE interval component pair, months first with
// day-clamping, then the µs component. This is steps 1-4 of the algorithm above
// for a single value; interval_apply_to_temporal and GENERATE_SERIES (which steps
// a cursor by repeated application, so Jan 31 +1mo = Feb 28, +1mo = Mar 28 —
// DuckDB's drift, not a re-anchor on the start) both call it, so there is exactly
// one definition of what "+ INTERVAL" means. `months` / `sub_us` are already
// signed. Unchecked: the caller owns range validation.
static inline int64_t interval_add_to_us(int64_t us, int64_t months, int64_t sub_us) noexcept {
    const int64_t US_PER_DAY = 86400000000LL;
    int64_t epoch_days = ta_floor_div(us, US_PER_DAY);
    int64_t day_us = us - epoch_days * US_PER_DAY;

    if (months != 0) {
        int year, month, day;
        ta_days_to_ymd(epoch_days, &year, &month, &day);
        const int64_t month_index = static_cast<int64_t>(month - 1) + months;
        const int64_t month_div   = ta_floor_div(month_index, 12);
        year  = static_cast<int>(year + month_div);
        month = static_cast<int>(month_index - month_div * 12 + 1);
        const int64_t last_day = iv_days_in_month(year, month);
        if (day > last_day) day = static_cast<int>(last_day);
        epoch_days = ta_ymd_to_days(year, month, day);
    }

    const int64_t us_total = day_us + sub_us;
    const int64_t carry_days = ta_floor_div(us_total, US_PER_DAY);
    day_us = us_total - carry_days * US_PER_DAY;
    epoch_days += carry_days;
    return epoch_days * US_PER_DAY + day_us;
}

static inline VecResult interval_apply_to_temporal(
    const DrakenVector& temporal, const DrakenVector& interval,
    bool src_is_date, int src_unit, int signum)
{
    const uint32_t n = temporal.length;
    const DrakenIntervalSlot* iv =
        static_cast<const DrakenIntervalSlot*>(interval.data);

    const int64_t US_PER_DAY = 86400000000LL;

    int64_t* dst = static_cast<int64_t*>(
        draken_malloc((n > 0u ? n : 1u) * sizeof(int64_t)));
    if (!dst) throw std::bad_alloc();

    const bool has_nulls = (temporal.validity != nullptr || interval.validity != nullptr);
    uint8_t* out_null = nullptr;
    if (has_nulls && n > 0) {
        const uint32_t nb = (n + 7u) >> 3;
        out_null = static_cast<uint8_t*>(draken_malloc(nb));
        if (!out_null) { draken_free(dst); throw std::bad_alloc(); }
        memset(out_null, 0, nb);
    }

    // Source-typed views (only the matching one is dereferenced).
    const int32_t* date_data = src_is_date
        ? static_cast<const int32_t*>(temporal.data) : nullptr;
    const int64_t* ts_data = src_is_date
        ? nullptr : static_cast<const int64_t*>(temporal.data);

    for (uint32_t i = 0; i < n; ++i) {
        if (!iv_row_valid(temporal.validity, i) || !iv_row_valid(interval.validity, i)) {
            dst[i] = 0;
            continue;
        }
        // The source value as µs since epoch (a DATE is midnight of its day).
        const int64_t src_us = src_is_date
            ? static_cast<int64_t>(date_data[temporal.selection[i]]) * US_PER_DAY
            : ts_to_us(ts_data[temporal.selection[i]], static_cast<TimestampUnit>(src_unit));

        const DrakenIntervalSlot& s = iv[interval.selection[i]];
        dst[i] = interval_add_to_us(src_us, s.months * static_cast<int64_t>(signum),
                                    s.us * static_cast<int64_t>(signum));
        if (out_null) iv_set_valid(out_null, i);
    }

    VecResult r;
    r.data           = dst;
    r.validity       = out_null;
    r.selection      = draken_identity_sel(n);
    r.owns_selection = false;
    r.data_length    = n;
    r.length         = n;
    r.type           = DRAKEN_TIMESTAMP64;
    r.flags          = static_cast<uint8_t>(DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION);
    r.ts_unit        = static_cast<uint8_t>(TimestampUnit::MICROSECONDS);
    return r;
}

// ---------------------------------------------------------------------------
// TEMPORAL − TEMPORAL → INTERVAL.
//
// Matches the engine's operator_map: DATE−DATE, DATE−TIMESTAMP, TIMESTAMP−DATE,
// TIMESTAMP−TIMESTAMP all yield INTERVAL (the `_G` result type).
//
// The difference is computed in microseconds (both sides normalised to µs since
// epoch) and stored as the interval's `us` component with months = 0. This mirrors
// the component-wise interval model (total_us = months×MONTH_US + us): a pure
// time delta has no month component, so the full delta lives in `us`.
//
// `a_is_date`/`b_is_date`, `a_unit`/`b_unit`: source descriptors as above.
// Either row null → null output row.
// ---------------------------------------------------------------------------
static inline VecResult temporal_minus_temporal(
    const DrakenVector& a, const DrakenVector& b,
    bool a_is_date, int a_unit, bool b_is_date, int b_unit)
{
    const uint32_t n = a.length;
    const int64_t US_PER_DAY = 86400000000LL;

    const int32_t* a_date = a_is_date ? static_cast<const int32_t*>(a.data) : nullptr;
    const int64_t* a_ts   = a_is_date ? nullptr : static_cast<const int64_t*>(a.data);
    const int32_t* b_date = b_is_date ? static_cast<const int32_t*>(b.data) : nullptr;
    const int64_t* b_ts   = b_is_date ? nullptr : static_cast<const int64_t*>(b.data);

    DrakenIntervalSlot* dst = static_cast<DrakenIntervalSlot*>(
        draken_malloc((n > 0u ? n : 1u) * sizeof(DrakenIntervalSlot)));
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
        const int64_t a_us = a_is_date
            ? static_cast<int64_t>(a_date[a.selection[i]]) * US_PER_DAY
            : ts_to_us(a_ts[a.selection[i]], static_cast<TimestampUnit>(a_unit));
        const int64_t b_us = b_is_date
            ? static_cast<int64_t>(b_date[b.selection[i]]) * US_PER_DAY
            : ts_to_us(b_ts[b.selection[i]], static_cast<TimestampUnit>(b_unit));
        // The interval slot's `us` component holds MICROSECONDS, matching the
        // temporal delta, so store it directly.
        dst[i].months = 0;
        dst[i].us     = (a_us - b_us);
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

}} // namespace draken::ops
