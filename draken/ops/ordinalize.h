#pragma once
// draken/ops/ordinalize.h — per-type int64 ORDINAL KEY kernels.
//
// Replaces the Python `Vector.ordinalize()` shim (draken/vectors/_vector_shim.pyx,
// 2026-07-30) that boxed every value via to_pylist() and looped in Python —
// interim debt, not the design (see .claude/CLAUDE.md §2). This is the native
// replacement: used by the catalog manifest builder to compute per-column
// min/max/histogram bins over Tb-scale data, and by plan-time file pruning.
//
// PUBLIC ENTRY POINTS:
//   void draken_ordinalize(const DrakenVector& v, int64_t* out, uint32_t n)
//     One int64_t ordinal key per logical row. Table-dispatched like
//     draken_hash. Throws std::invalid_argument for unsupported types.
//   VecResult draken_ordinalize_shaped(const DrakenVector& v)
//     Shape-preserving twin (mirrors draken_hash_shaped): dict-compressed
//     input -> ordinalize only the data_length distinct values, not every
//     row. This is what Vector.ordinalize() exposes to Python.
//   ordinalize_scalar_*(...)
//     Scalar per-type helpers. Shared by the vector kernels' inner loop AND
//     by DrakenType.ordinalize(value) at the nanobind boundary (used at plan
//     time to compare a predicate literal's ordinal key against a file's
//     precomputed min/max ordinal bounds, without needing a vector).
//
// CONTRACT: an ordinal key is MONOTONIC (preserves the type's natural order)
// -- not a total order in every case. VARCHAR/NVARCHAR/VARBINARY/VARIANT keys
// can collide on a shared prefix. That's acceptable for histogram bucketing /
// coarse range pruning; NOT safe as a sort key or an equality proxy.
//
// ORDINAL_NULL (INT64_MIN) sorts every null first, matching the removed
// Python shim's convention.
//
// FLOAT32/64: relies on float_ops.h's ingestion-time canonicalization
// (-0.0 -> +0.0, any NaN bit-pattern -> one canonical quiet NaN). This kernel
// does not re-canonicalize. A canonical quiet NaN's raw bits are the largest
// non-negative float64 pattern, so it naturally orders highest, matching the
// "NaN highest" convention documented in float_ops.h without special-casing.
//
// DECIMAL (int64-backed): the ordinal key is the RAW UNSCALED mantissa --
// comparing DECIMAL ordinal keys across two columns/literals is only
// meaningful if they share scale (a binder/cast responsibility, not this
// kernel's -- see redundant_cast_context_rules).
//
// DECIMAL128 is NOT supported -- deliberately no ordinalize entry, so calls
// throw rather than silently returning a saturated, low-resolution int64
// proxy for a type whose entire reason to exist is full 128-bit precision.
// Broken but honest beats green but fake (.claude/CLAUDE.md §1).
//
// INTERVAL: reuses interval_ops.h's interval_normalize_unchecked (the same
// months*INTERVAL_MONTH_US+us normalization interval_compare_scalar/vector/
// between already use) -- not a second hand-derived ordering.
//
// VARCHAR/NVARCHAR/VARBINARY/VARIANT: packs the first 8 content bytes
// big-endian into a uint64_t and right-shifts by 1 -- halving always lands
// in [0, INT64_MAX], so the result fits a non-negative int64 while
// preserving order (shift-by-1 is monotonic). Zero-padded if shorter, so a
// strict prefix always sorts before its longer extension. No separate
// length field needed. Long strings cost one arena read (str_data() past
// the slot's own precomputed 4-byte ext.prefix) -- bounded, not a full
// payload read, so still cheap at Tb scale.

#include <cstdint>
#include <cstring>
#include <climits>

#include "buffers.h"
#include "core/string_slot.h"
#include "ops/vec_result.h"
#include "ops/interval_ops.h"  // interval_normalize_unchecked, DrakenIntervalSlot

namespace draken { namespace ops {

static constexpr int64_t ORDINAL_NULL = INT64_MIN;

// ---------------------------------------------------------------------------
// Scalar helpers -- shared by the vector kernels below and by
// DrakenType.ordinalize(value) at the nanobind boundary.
// ---------------------------------------------------------------------------

template<typename T>
static inline int64_t ordinalize_scalar_widen(T v) noexcept {
    // Safe for int8/16/32/64 and uint8/16/32 -- every value of those widths
    // fits an int64_t with its natural ordering preserved by the widen.
    return static_cast<int64_t>(v);
}

static inline int64_t ordinalize_scalar_u64(uint64_t v) noexcept {
    // uint64_t's range exceeds int64_t's -- bias by the sign bit so the
    // full unsigned range maps onto int64_t with order preserved.
    return static_cast<int64_t>(v ^ 0x8000000000000000ULL);
}

static inline int64_t ordinalize_scalar_f64(double x) noexcept {
    uint64_t bits;
    std::memcpy(&bits, &x, sizeof(bits));
    if (bits & 0x8000000000000000ULL) {
        // Negative: keep the sign bit (already 1), flip the remaining 63 --
        // reverses raw-magnitude order into value order within negatives,
        // and (via the retained sign bit) keeps the whole negative domain
        // below the whole positive domain under SIGNED int64 comparison.
        bits = (~bits) | 0x8000000000000000ULL;
    }
    // Positive (or canonical +0.0 / canonical NaN): raw bits are already in
    // value order and already have sign bit 0 -- no transform needed.
    int64_t key;
    std::memcpy(&key, &bits, sizeof(key));
    return key;
}

static inline int64_t ordinalize_scalar_f32(float x) noexcept {
    // Widen to double and reuse the f64 transform: correctness over a
    // hand-duplicated 32-bit bit-trick. float32 canonicalization already
    // applies at ingestion (float_ops.h), so this promotion is exact for
    // every representable float32 value including canonical NaN/-0.0.
    return ordinalize_scalar_f64(static_cast<double>(x));
}

static inline int64_t ordinalize_scalar_interval(int64_t months, int64_t us) noexcept {
    return interval_normalize_unchecked(months, us);
}

// String ordinal key -- see file header. NOT a total order past 8 bytes:
// two strings sharing an 8-byte prefix collide onto the same key. Acceptable
// for histogram bucketing / coarse range pruning. Shared by the scalar
// Python-facing entry point (raw bytes, no slot) and the slot-based vector
// kernel below, so both produce identical keys for identical content.
static inline int64_t ordinalize_scalar_bytes8(const uint8_t* p, uint32_t len) noexcept {
    uint64_t prefix = 0u;
    for (uint32_t i = 0; i < 8u; ++i) {
        const uint8_t byte = (i < len) ? p[i] : 0u;
        prefix = (prefix << 8) | byte;
    }
    return static_cast<int64_t>(prefix >> 1);
}

// arena_base: pass sa->arena (may be NULL when every row is inline -- str_data
// never dereferences it for an inline slot, so NULL is safe there).
static inline int64_t ordinalize_scalar_string_slot(const DrakenStringSlot* slot,
                                                     const uint8_t* arena_base) noexcept {
    return ordinalize_scalar_bytes8(str_data(slot, arena_base), str_length(slot));
}

// ---------------------------------------------------------------------------
// Vector kernels -- write ORDINAL_NULL for null rows, one int64_t per
// logical row into out[0..n). Mirror hash.h's null-select / selection-index
// access pattern (no SIMD block batching -- there is no downstream mixing
// step to batch for, unlike hash).
// ---------------------------------------------------------------------------

template<typename T>
static inline void ordinalize_widen(const DrakenVector& v, int64_t* out, uint32_t n) {
    if (n == 0) return;
    const T* data = static_cast<const T*>(v.data);
    const uint8_t* validity = v.validity;
    const uint32_t* sel = v.selection;
    for (uint32_t i = 0; i < n; ++i) {
        if (validity != nullptr) {
            const uint64_t is_valid = (validity[i >> 3] >> (i & 7u)) & 1u;
            if (!is_valid) { out[i] = ORDINAL_NULL; continue; }
        }
        out[i] = ordinalize_scalar_widen<T>(data[sel[i]]);
    }
}

// DRAKEN_NULL — self-describing null (buffers.h: type==NULL ⟹ every row
// null; no data, no validity). Every row ordinalizes trivially to
// ORDINAL_NULL rather than throwing, matching draken_hash's DRAKEN_NULL
// handling (draken_native.cpp's boxed hash() method).
static inline void ordinalize_null(const DrakenVector&, int64_t* out, uint32_t n) {
    for (uint32_t i = 0; i < n; ++i) out[i] = ORDINAL_NULL;
}

// BOOL is bit-packed (see fixed_int_ops.h's hash_bool) -- data[code>>3]>>(code&7)
// -- NOT a byte array, so it can't reuse ordinalize_widen<uint8_t>.
static inline void ordinalize_bool(const DrakenVector& v, int64_t* out, uint32_t n) {
    if (n == 0) return;
    const uint8_t* data = static_cast<const uint8_t*>(v.data);
    const uint8_t* validity = v.validity;
    const uint32_t* sel = v.selection;
    for (uint32_t i = 0; i < n; ++i) {
        if (validity != nullptr) {
            const uint64_t is_valid = (validity[i >> 3] >> (i & 7u)) & 1u;
            if (!is_valid) { out[i] = ORDINAL_NULL; continue; }
        }
        const uint32_t code = sel[i];
        out[i] = static_cast<int64_t>((data[code >> 3] >> (code & 7u)) & 1u);
    }
}

static inline void ordinalize_uint64(const DrakenVector& v, int64_t* out, uint32_t n) {
    if (n == 0) return;
    const uint64_t* data = static_cast<const uint64_t*>(v.data);
    const uint8_t* validity = v.validity;
    const uint32_t* sel = v.selection;
    for (uint32_t i = 0; i < n; ++i) {
        if (validity != nullptr) {
            const uint64_t is_valid = (validity[i >> 3] >> (i & 7u)) & 1u;
            if (!is_valid) { out[i] = ORDINAL_NULL; continue; }
        }
        out[i] = ordinalize_scalar_u64(data[sel[i]]);
    }
}

static inline void ordinalize_float32(const DrakenVector& v, int64_t* out, uint32_t n) {
    if (n == 0) return;
    const float* data = static_cast<const float*>(v.data);
    const uint8_t* validity = v.validity;
    const uint32_t* sel = v.selection;
    for (uint32_t i = 0; i < n; ++i) {
        if (validity != nullptr) {
            const uint64_t is_valid = (validity[i >> 3] >> (i & 7u)) & 1u;
            if (!is_valid) { out[i] = ORDINAL_NULL; continue; }
        }
        out[i] = ordinalize_scalar_f32(data[sel[i]]);
    }
}

static inline void ordinalize_float64(const DrakenVector& v, int64_t* out, uint32_t n) {
    if (n == 0) return;
    const double* data = static_cast<const double*>(v.data);
    const uint8_t* validity = v.validity;
    const uint32_t* sel = v.selection;
    for (uint32_t i = 0; i < n; ++i) {
        if (validity != nullptr) {
            const uint64_t is_valid = (validity[i >> 3] >> (i & 7u)) & 1u;
            if (!is_valid) { out[i] = ORDINAL_NULL; continue; }
        }
        out[i] = ordinalize_scalar_f64(data[sel[i]]);
    }
}

static inline void ordinalize_interval(const DrakenVector& v, int64_t* out, uint32_t n) {
    if (n == 0) return;
    const DrakenIntervalSlot* data = static_cast<const DrakenIntervalSlot*>(v.data);
    const uint8_t* validity = v.validity;
    const uint32_t* sel = v.selection;
    for (uint32_t i = 0; i < n; ++i) {
        if (validity != nullptr) {
            const uint64_t is_valid = (validity[i >> 3] >> (i & 7u)) & 1u;
            if (!is_valid) { out[i] = ORDINAL_NULL; continue; }
        }
        const DrakenIntervalSlot& s = data[sel[i]];
        out[i] = ordinalize_scalar_interval(s.months, s.us);
    }
}

// VARCHAR/NVARCHAR/VARBINARY/VARIANT — German-string storage. Reads up to 8
// content bytes per row via str_data(); inline strings never touch sa->arena,
// long strings do (see file header).
static inline void ordinalize_string(const DrakenVector& v, int64_t* out, uint32_t n) {
    if (n == 0) return;
    const DrakenStringArena* sa = static_cast<const DrakenStringArena*>(v.data);
    const DrakenStringSlot* slots = sa->slots;
    const uint8_t* arena_base = sa->arena;
    const uint8_t* validity = v.validity;
    const uint32_t* sel = v.selection;
    for (uint32_t i = 0; i < n; ++i) {
        if (validity != nullptr) {
            const uint64_t is_valid = (validity[i >> 3] >> (i & 7u)) & 1u;
            if (!is_valid) { out[i] = ORDINAL_NULL; continue; }
        }
        out[i] = ordinalize_scalar_string_slot(&slots[sel[i]], arena_base);
    }
}

} }  // namespace draken::ops
