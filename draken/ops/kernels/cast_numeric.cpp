#include "ops/kernels/cast_kernels.h"
#include "ops/kernels/error_handling.h"
#include "ops/kernels/result_helpers.h"
#include "ops/kernels/kernel_context.h"
#include "core/alloc.h"
#include "core/vector_alloc.h"
#include "core/string_slot.h"
#include "ryu.h"          // d2fixed_buffered_n / d2s_buffered_n (FLOAT64 → ASCII)
#include <cstring>
#include <cmath>          // std::isfinite / std::isnan
#include <vector>         // single-pass format staging (avoid double ryu)
#include <stdexcept>      // ARRAY->VARCHAR fail-loud element-type guard

/**
 * Cast kernels: numeric / boolean conversions (Phase 9c).
 *
 * Real VecResult-producing bodies extracted from the proven nanobind compute in
 * opteryx/compiled/nanobind/vector_casts.cpp. The nanobind functions now shim to
 * these kernels (core lives in draken; the binding layer calls across).
 *
 * Dense output: out[i] holds logical row i; selection is the global identity.
 * Null rows write a placeholder value and clear their validity bit (copied 1:1
 * from the input bitmap, which is also logical-row indexed).
 */

// File-scope string literals for bool→string. Defined outside any kernel body:
// brace-initializer commas would otherwise be parsed as DRAKEN_KERNEL_TRY macro
// argument separators.
static const uint8_t kBoolTrue[]  = { 't', 'r', 'u', 'e' };
static const uint8_t kBoolFalse[] = { 'f', 'a', 'l', 's', 'e' };

extern "C" {

// --- int → decimal ASCII (hand-rolled; matches the old .pyx fast path) -------

static inline int i64_to_ascii(int64_t value, char* buf) noexcept {
    if (value == 0) { buf[0] = '0'; return 1; }
    int i = 20;
    bool neg = value < 0;
    uint64_t uval = neg ? static_cast<uint64_t>(-value) : static_cast<uint64_t>(value);
    while (uval) { buf[--i] = '0' + static_cast<int>(uval % 10u); uval /= 10u; }
    if (neg) buf[--i] = '-';
    int len = 20 - i;
    if (i) std::memmove(buf, buf + i, static_cast<size_t>(len));
    return len;
}

static inline int u64_to_ascii(uint64_t value, char* buf) noexcept {
    if (value == 0) { buf[0] = '0'; return 1; }
    int i = 20;
    while (value) { buf[--i] = '0' + static_cast<int>(value % 10u); value /= 10u; }
    int len = 20 - i;
    if (i) std::memmove(buf, buf + i, static_cast<size_t>(len));
    return len;
}

// Width-aware signed load: read the j-th physical value at the source's native
// stride and sign-extend to int64. The digit conversion is value-based and
// width-agnostic; only the buffer read depends on width (INT8/16/32/64).
static inline int64_t load_int_signed(const void* data, uint32_t j, DrakenType t) noexcept {
    switch (t) {
        case DRAKEN_INT8:  return static_cast<const int8_t*>(data)[j];
        case DRAKEN_INT16: return static_cast<const int16_t*>(data)[j];
        case DRAKEN_INT32: return static_cast<const int32_t*>(data)[j];
        default:           return static_cast<const int64_t*>(data)[j];  // DRAKEN_INT64
    }
}

static inline uint64_t load_int_unsigned(const void* data, uint32_t j, DrakenType t) noexcept {
    switch (t) {
        case DRAKEN_UINT8:  return static_cast<const uint8_t*>(data)[j];
        case DRAKEN_UINT16: return static_cast<const uint16_t*>(data)[j];
        case DRAKEN_UINT32: return static_cast<const uint32_t*>(data)[j];
        default:            return static_cast<const uint64_t*>(data)[j];  // DRAKEN_UINT64
    }
}

// Shared integer → VARCHAR core, signed and unsigned. Compression-aware: format
// the data_length PHYSICAL values into a value block (1 for a constant, K for a
// dict, length for dense) and carry the input's selection + validity through — the
// output keeps the input's encoding (constant→constant string, dict→dict string).
// Two passes over the K values: size the arena, then fill. Int formatting never
// fails (no null-introduction), so validity is preserved 1:1.
//
// Signedness is read off the vector's own type tag rather than taken as a
// parameter: every width is read at its native stride, so there is no width at
// which the caller could meaningfully disagree with the tag. (The former
// `treat_as_unsigned` parameter only accepted DRAKEN_INT64 and no caller ever
// passed true — a reinterpret-the-bits path that had no route in from SQL.)
//
// A UINT32 carrying LogicalKind::IPV4 must NOT reach here: it renders as
// dotted-decimal via draken_cast_ipv4_to_string. A DrakenVector carries no
// descriptor, so that choice is made at bind time in casts.pyx, never here.
static VecResult int_to_string_core(const DrakenVector* v) {
    if (!v) return draken_error_sentinel("Input vector is null");
    const DrakenType st = v->type;
    const bool is_signed = (st == DRAKEN_INT8 || st == DRAKEN_INT16 ||
                            st == DRAKEN_INT32 || st == DRAKEN_INT64);
    const bool is_unsigned = (st == DRAKEN_UINT8 || st == DRAKEN_UINT16 ||
                              st == DRAKEN_UINT32 || st == DRAKEN_UINT64);
    if (!is_signed && !is_unsigned)
        return draken_error_sentinel_fmt(
            "cast-to-string: expected INT8/16/32/64 or UINT8/16/32/64, got %d", st);

    const uint32_t k   = v->data_length;   // physical value count

    char tmp[21];
    size_t total_extern = 0u;
    for (uint32_t j = 0u; j < k; ++j) {
        int len = is_unsigned ? u64_to_ascii(load_int_unsigned(v->data, j, st), tmp)
                              : i64_to_ascii(load_int_signed(v->data, j, st), tmp);
        if (static_cast<uint32_t>(len) > STR_INLINE_MAX) total_extern += static_cast<size_t>(len);
    }

    // Value block holds K slots; validity is preserved separately by
    // kernel_preserve_shape (per logical row), so the block embeds none.
    DrakenStringSlot* slots;
    uint8_t* arena;
    uint8_t* vunused;
    uint8_t* block = vecresult_string_block_alloc(k, total_extern, 0, &slots, &arena, &vunused);
    if (!block) return draken_error_sentinel("Allocation failed");
    (void)vunused;

    size_t arena_used = 0u;
    for (uint32_t j = 0u; j < k; ++j) {
        int len = is_unsigned ? u64_to_ascii(load_int_unsigned(v->data, j, st), tmp)
                              : i64_to_ascii(load_int_signed(v->data, j, st), tmp);
        if (static_cast<uint32_t>(len) > STR_INLINE_MAX) {
            const uint32_t off = static_cast<uint32_t>(arena_used);
            std::memcpy(arena + off, tmp, static_cast<size_t>(len));
            draken_build_string_slot(&slots[j], reinterpret_cast<const uint8_t*>(tmp),
                                     static_cast<uint32_t>(len), off);
            arena_used += static_cast<size_t>(len);
        } else {
            draken_build_string_slot(&slots[j], reinterpret_cast<const uint8_t*>(tmp),
                                     static_cast<uint32_t>(len), 0u);
        }
    }

    VecResult r = vecresult_from_string_block(block, k, total_extern, 0, DRAKEN_VARCHAR);
    kernel_preserve_shape(r, v);  // overrides length/selection/validity to the input's shape
    return r;
}

VecResult draken_cast_int64_to_float64(void* ctx, const DrakenVector* v) {
    DRAKEN_KERNEL_TRY({
        if (!v) return draken_error_sentinel("Input vector is null");
        if (v->type != DRAKEN_INT64)
            return draken_error_sentinel_fmt("cast int64->float64: expected INT64, got %d", v->type);
        // Compression-aware: cast the data_length PHYSICAL values (1 for a constant,
        // K for a dict, length for dense) and carry the input's selection + validity
        // through unchanged. A constant column casts one value; a dict casts K.
        const uint32_t k = v->data_length;
        const int64_t* src = static_cast<const int64_t*>(v->data);
        double* out = static_cast<double*>(draken_malloc((k > 0u ? k : 1u) * sizeof(double)));
        if (!out) return draken_error_sentinel("Allocation failed");
        for (uint32_t j = 0u; j < k; ++j)
            out[j] = static_cast<double>(src[j]);

        VecResult r;
        r.data = out; r.type = DRAKEN_FLOAT64; r.validity_embedded = 0u; r.ts_unit = 0xFFu;
        kernel_preserve_shape(r, v);
        return r;
    });
}

VecResult draken_cast_int64_to_bool(void* ctx, const DrakenVector* v) {
    DRAKEN_KERNEL_TRY({
        if (!v) return draken_error_sentinel("Input vector is null");
        if (v->type != DRAKEN_INT64)
            return draken_error_sentinel_fmt("cast int64->bool: expected INT64, got %d", v->type);
        const uint32_t k = v->data_length;   // physical value count → K-bit output bitmap
        const int64_t* src = static_cast<const int64_t*>(v->data);
        const size_t nbytes = (k > 0u ? (k + 7u) / 8u : 1u);
        uint8_t* out = static_cast<uint8_t*>(draken_malloc(nbytes));
        if (!out) return draken_error_sentinel("Allocation failed");
        std::memset(out, 0, nbytes);
        for (uint32_t j = 0u; j < k; ++j)
            if (src[j] != 0) out[j >> 3u] |= static_cast<uint8_t>(1u << (j & 7u));

        VecResult r;
        r.data = out; r.type = DRAKEN_BOOL; r.validity_embedded = 0u; r.ts_unit = 0xFFu;
        kernel_preserve_shape(r, v);
        return r;
    });
}

VecResult draken_cast_int64_to_string(void* ctx, const DrakenVector* v) {
    DRAKEN_KERNEL_TRY({ return int_to_string_core(v); });
}

// Narrow signed int (INT8/16/32) → VARCHAR. Single pass at the source's native
// stride — no widen-to-int64 detour; int_to_string_core reads the value at the
// correct width. (INT64 keeps its own entry point above.)
VecResult draken_cast_integer_to_string(void* ctx, const DrakenVector* v) {
    DRAKEN_KERNEL_TRY({ return int_to_string_core(v); });
}

// Unsigned int (UINT8/16/32/64) → VARCHAR. One entry point for all four widths:
// the stride comes from the vector's type tag, so there is nothing to specialize
// per width beyond the load. A UINT64 above INT64_MAX formats correctly here —
// funnelling the unsigned family through the signed path would print it negative.
//
// This is the PLAIN unsigned render. An IPv4 column is also DRAKEN_UINT32, and
// must NOT arrive here — see draken_cast_ipv4_to_string in cast_string.cpp.
VecResult draken_cast_uint_to_string(void* ctx, const DrakenVector* v) {
    DRAKEN_KERNEL_TRY({ return int_to_string_core(v); });
}

VecResult draken_cast_bool_to_float64(void* ctx, const DrakenVector* v) {
    DRAKEN_KERNEL_TRY({
        if (!v) return draken_error_sentinel("Input vector is null");
        if (v->type != DRAKEN_BOOL)
            return draken_error_sentinel_fmt("cast bool->float64: expected BOOL, got %d", v->type);
        const uint32_t k = v->data_length;
        const uint8_t* src = static_cast<const uint8_t*>(v->data);
        double* out = static_cast<double*>(draken_malloc((k > 0u ? k : 1u) * sizeof(double)));
        if (!out) return draken_error_sentinel("Allocation failed");
        for (uint32_t j = 0u; j < k; ++j)
            out[j] = static_cast<double>((src[j >> 3u] >> (j & 7u)) & 1u);

        VecResult r;
        r.data = out; r.type = DRAKEN_FLOAT64; r.validity_embedded = 0u; r.ts_unit = 0xFFu;
        kernel_preserve_shape(r, v);
        return r;
    });
}

VecResult draken_cast_bool_to_int64(void* ctx, const DrakenVector* v) {
    DRAKEN_KERNEL_TRY({
        if (!v) return draken_error_sentinel("Input vector is null");
        if (v->type != DRAKEN_BOOL)
            return draken_error_sentinel_fmt("cast bool->int64: expected BOOL, got %d", v->type);
        const uint32_t k = v->data_length;
        const uint8_t* src = static_cast<const uint8_t*>(v->data);
        int64_t* out = static_cast<int64_t*>(draken_malloc((k > 0u ? k : 1u) * sizeof(int64_t)));
        if (!out) return draken_error_sentinel("Allocation failed");
        for (uint32_t j = 0u; j < k; ++j)
            out[j] = static_cast<int64_t>((src[j >> 3u] >> (j & 7u)) & 1u);

        VecResult r;
        r.data = out; r.type = DRAKEN_INT64; r.validity_embedded = 0u; r.ts_unit = 0xFFu;
        kernel_preserve_shape(r, v);
        return r;
    });
}

// BOOL → VARCHAR: only "false"/"true"/null exist, so emit a DICTIONARY-encoded
// vector — a 2-slot value block ("false"=code 0, "true"=code 1) plus one uint32
// code per row (the bool bit). Writes 2 slots + n*4 codes instead of n*16 slots;
// nulls ride the validity mask. Downstream reads via the uniform data[selection[i]]
// path (§11), so dict shape is transparent to consumers.
VecResult draken_cast_bool_to_string(void* ctx, const DrakenVector* v) {
    DRAKEN_KERNEL_TRY({
        if (!v) return draken_error_sentinel("Input vector is null");
        if (v->type != DRAKEN_BOOL)
            return draken_error_sentinel_fmt("cast bool->string: expected BOOL, got %d", v->type);
        const uint32_t n = v->length;
        const uint8_t* src = static_cast<const uint8_t*>(v->data);

        // Value block: 2 unique inline strings. (length == data_length == 2 here.)
        DrakenStringSlot* dslots;
        uint8_t* darena;
        uint8_t* dval;
        uint8_t* block = vecresult_string_block_alloc(2u, 0u, 0, &dslots, &darena, &dval);
        if (!block) return draken_error_sentinel("Allocation failed");
        (void)darena; (void)dval;
        str_init_inline(&dslots[0], kBoolFalse, 5u);  // code 0 = "false"
        str_init_inline(&dslots[1], kBoolTrue, 4u);   // code 1 = "true"

        // Per-row codes = the bool bit. Null rows → code 0; the validity mask hides them.
        uint32_t* codes = static_cast<uint32_t*>(draken_malloc((n > 0u ? n : 1u) * sizeof(uint32_t)));
        if (!codes) { draken_free(block); return draken_error_sentinel("Allocation failed"); }
        for (uint32_t i = 0u; i < n; ++i) {
            if (kernel_row_is_null(v, i)) { codes[i] = 0u; continue; }
            const uint32_t b = v->selection[i];
            codes[i] = (src[b >> 3u] >> (b & 7u)) & 1u;
        }

        uint8_t* validity = kernel_copy_validity(v);  // separate; dict block embeds none

        // Re-shape the dense-2 result into the dict: codes selection over the value block.
        VecResult r = vecresult_from_string_block(block, 2u, 0u, 0, DRAKEN_VARCHAR);
        r.selection         = codes;
        r.owns_selection    = true;
        r.data_length       = 2u;
        r.length            = n;
        r.validity          = validity;
        r.validity_embedded = 0u;
        return r;
    });
}

VecResult draken_cast_float64_to_int64(void* ctx, const DrakenVector* v) {
    DRAKEN_KERNEL_TRY({
        if (!v) return draken_error_sentinel("Input vector is null");
        if (v->type != DRAKEN_FLOAT64 && v->type != DRAKEN_FLOAT32)
            return draken_error_sentinel_fmt("cast float->int64: expected FLOAT, got %d", v->type);
        const uint32_t k = v->data_length;
        const bool is64 = (v->type == DRAKEN_FLOAT64);
        int64_t* out = static_cast<int64_t*>(draken_malloc((k > 0u ? k : 1u) * sizeof(int64_t)));
        if (!out) return draken_error_sentinel("Allocation failed");
        for (uint32_t j = 0u; j < k; ++j) {
            const double d = is64 ? static_cast<const double*>(v->data)[j]
                                  : static_cast<double>(static_cast<const float*>(v->data)[j]);
            out[j] = static_cast<int64_t>(d);  // truncate toward zero
        }
        VecResult r;
        r.data = out; r.type = DRAKEN_INT64; r.validity_embedded = 0u; r.ts_unit = 0xFFu;
        kernel_preserve_shape(r, v);
        return r;
    });
}

VecResult draken_cast_float64_to_bool(void* ctx, const DrakenVector* v) {
    DRAKEN_KERNEL_TRY({
        if (!v) return draken_error_sentinel("Input vector is null");
        if (v->type != DRAKEN_FLOAT64 && v->type != DRAKEN_FLOAT32)
            return draken_error_sentinel_fmt("cast float->bool: expected FLOAT, got %d", v->type);
        const uint32_t k = v->data_length;
        const bool is64 = (v->type == DRAKEN_FLOAT64);
        const size_t nbytes = (k > 0u ? (k + 7u) / 8u : 1u);
        uint8_t* out = static_cast<uint8_t*>(draken_malloc(nbytes));
        if (!out) return draken_error_sentinel("Allocation failed");
        std::memset(out, 0, nbytes);
        for (uint32_t j = 0u; j < k; ++j) {
            const double d = is64 ? static_cast<const double*>(v->data)[j]
                                  : static_cast<double>(static_cast<const float*>(v->data)[j]);
            if (d != 0.0) out[j >> 3u] |= static_cast<uint8_t>(1u << (j & 7u));
        }
        VecResult r;
        r.data = out; r.type = DRAKEN_BOOL; r.validity_embedded = 0u; r.ts_unit = 0xFFu;
        kernel_preserve_shape(r, v);
        return r;
    });
}

// Ryu d2fixed formatting (ported from draken_native.cpp make_string_from_float_vector).
// Trailing zeros trimmed; bare "x." gets a single "0". NaN/Infinity spelled out;
// very-large magnitudes fall back to %.17g. precision matches the cast default (6).
static inline size_t ryu_format_double(char* buf, double d, uint32_t precision) {
    if (!std::isfinite(d)) {
        if (std::isnan(d)) { std::memcpy(buf, "NaN", 3u); return 3u; }
        if (d > 0.0) { std::memcpy(buf, "Infinity", 8u); return 8u; }
        std::memcpy(buf, "-Infinity", 9u); return 9u;
    }
    // Very large magnitudes: d2fixed would emit a 25+ digit integer part (buffer
    // overflow + absurd string). Use Ryu's exponential formatter (same d2fixed.c
    // translation unit) — still Ryu, never libc snprintf.
    if (d >= 9.9e24 || d <= -9.9e24) {
        return static_cast<size_t>(d2exp_buffered_n(d, precision, buf));
    }
    int len = d2fixed_buffered_n(d, precision, buf);
    while (len > 0 && buf[len - 1] == '0') --len;
    if (len > 0 && buf[len - 1] == '.') buf[len++] = '0';
    return static_cast<size_t>(len);
}

// FLOAT64/FLOAT32 → VARCHAR via Ryu (d2fixed for |d| < 9.9e24, d2exp above).
// Compression-aware: format the data_length PHYSICAL values once into staging,
// build a K-slot value block, then preserve the input's selection + validity.
// A constant float formats one value; a dict formats K.
VecResult draken_cast_float64_to_string(void* ctx, const DrakenVector* v) {
    DRAKEN_KERNEL_TRY({
        if (!v) return draken_error_sentinel("Input vector is null");
        if (v->type != DRAKEN_FLOAT64 && v->type != DRAKEN_FLOAT32)
            return draken_error_sentinel_fmt("cast float64->string: expected FLOAT, got %d", v->type);

        const uint32_t k = v->data_length;
        const bool is64 = (v->type == DRAKEN_FLOAT64);
        const uint32_t precision = 6u;

        // Pass 1: format each unique value ONCE (Ryu is the costly step) into a
        // staging buffer; record per-value length (null rows handled by validity,
        // so we format every physical value unconditionally).
        std::vector<char> stage;
        stage.reserve(static_cast<size_t>(k) * 16u);
        std::vector<uint32_t> rlen(k > 0u ? k : 1u, 0u);
        char tmp[40];
        size_t total_extern = 0u;
        for (uint32_t j = 0u; j < k; ++j) {
            const double d = is64 ? static_cast<const double*>(v->data)[j]
                                  : static_cast<double>(static_cast<const float*>(v->data)[j]);
            const size_t len = ryu_format_double(tmp, d, precision);
            rlen[j] = static_cast<uint32_t>(len);
            stage.insert(stage.end(), tmp, tmp + len);
            if (len > STR_INLINE_MAX) total_extern += len;
        }

        DrakenStringSlot* slots;
        uint8_t* arena;
        uint8_t* vunused;
        uint8_t* block = vecresult_string_block_alloc(k, total_extern, 0, &slots, &arena, &vunused);
        if (!block) return draken_error_sentinel("Allocation failed");
        (void)vunused;

        const char* sbase = stage.data();
        size_t soff = 0u;
        size_t arena_used = 0u;
        for (uint32_t j = 0u; j < k; ++j) {
            const uint8_t* bytes = reinterpret_cast<const uint8_t*>(sbase + soff);
            const uint32_t len = rlen[j];
            soff += len;
            if (len > STR_INLINE_MAX) {
                std::memcpy(arena + arena_used, bytes, len);
                draken_build_string_slot(&slots[j], bytes, len, static_cast<uint32_t>(arena_used));
                arena_used += len;
            } else {
                draken_build_string_slot(&slots[j], bytes, len, 0u);
            }
        }

        VecResult r = vecresult_from_string_block(block, k, total_extern, 0, DRAKEN_VARCHAR);
        kernel_preserve_shape(r, v);
        return r;
    });
}

// --- DECIMAL → VARCHAR --------------------------------------------------------
//
// A DECIMAL vector stores the UNSCALED integer (int64 for DRAKEN_DECIMAL p≤18,
// __int128 for DRAKEN_DECIMAL128 p≤38); the decimal point position is the
// LogicalType `scale`, which the DrakenVector does NOT carry (§11/§14). The
// binder therefore hands the source scale in a binary_op_ctx.left_scale at bind
// time (see the `_to_string` DECIMAL arm in compiled_expression.pyx), same
// mechanism the DECIMAL binop/function kernels use.
//
// Text follows SQL DECIMAL semantics: EXACTLY `scale` fractional digits, trailing
// zeros preserved (DECIMAL(3,1) 30 → "3.0", not "3"), matching Python str(Decimal).

// Format a signed unscaled decimal `value` at `scale` into `buf`; returns byte
// length. Magnitude fits: |value| < 10^38 < 2^127, so -value is never UB and the
// digit buffer never exceeds 39 digits (+ sign + point).
static int decimal_to_ascii(__int128 value, uint32_t scale, char* buf) noexcept {
    const bool neg = value < 0;
    unsigned __int128 u = neg ? static_cast<unsigned __int128>(-value)
                              : static_cast<unsigned __int128>(value);
    char digits[48];
    int nd = 0;
    if (u == 0) { digits[nd++] = '0'; }
    else { while (u) { digits[nd++] = static_cast<char>('0' + static_cast<int>(u % 10u)); u /= 10u; } }
    // digits[] holds least-significant digit first.

    int p = 0;
    if (neg) buf[p++] = '-';
    if (scale == 0u) {
        for (int i = nd - 1; i >= 0; --i) buf[p++] = digits[i];
        return p;
    }
    const int int_digits = nd - static_cast<int>(scale);
    if (int_digits <= 0) {
        buf[p++] = '0';
        buf[p++] = '.';
        for (int z = 0; z < -int_digits; ++z) buf[p++] = '0';  // leading fractional zeros
        for (int i = nd - 1; i >= 0; --i) buf[p++] = digits[i];
    } else {
        for (int i = nd - 1; i >= nd - int_digits; --i) buf[p++] = digits[i];
        buf[p++] = '.';
        for (int i = nd - int_digits - 1; i >= 0; --i) buf[p++] = digits[i];
    }
    return p;
}

// Shared DECIMAL/DECIMAL128 → VARCHAR core. Compression-aware like
// int_to_string_core: format the data_length PHYSICAL unscaled values (1 for a
// constant, K for a dict, length for dense) into a K-slot value block, then carry
// the input's selection + validity through. Formatting never fails (no
// null-introduction), so validity is preserved 1:1.
static VecResult decimal_to_string_core(void* ctx, const DrakenVector* v, bool is128) {
    if (!v) return draken_error_sentinel("Input vector is null");
    const DrakenType want = is128 ? DRAKEN_DECIMAL128 : DRAKEN_DECIMAL;
    if (v->type != want)
        return draken_error_sentinel_fmt("cast decimal->string: expected %d, got %d", want, v->type);
    // Scale is mandatory for a correct answer; a missing ctx is a bind bug — fail
    // loud rather than silently emit an unscaled integer.
    if (!ctx) return draken_error_sentinel("cast decimal->string: missing scale context");
    const uint32_t scale = static_cast<const binary_op_ctx*>(ctx)->left_scale;

    const uint32_t k = v->data_length;
    const int64_t*   src64  = static_cast<const int64_t*>(v->data);
    const __int128*  src128 = static_cast<const __int128*>(v->data);

    char tmp[48];
    size_t total_extern = 0u;
    for (uint32_t j = 0u; j < k; ++j) {
        const __int128 val = is128 ? src128[j] : static_cast<__int128>(src64[j]);
        const int len = decimal_to_ascii(val, scale, tmp);
        if (static_cast<uint32_t>(len) > STR_INLINE_MAX) total_extern += static_cast<size_t>(len);
    }

    DrakenStringSlot* slots;
    uint8_t* arena;
    uint8_t* vunused;
    uint8_t* block = vecresult_string_block_alloc(k, total_extern, 0, &slots, &arena, &vunused);
    if (!block) return draken_error_sentinel("Allocation failed");
    (void)vunused;

    size_t arena_used = 0u;
    for (uint32_t j = 0u; j < k; ++j) {
        const __int128 val = is128 ? src128[j] : static_cast<__int128>(src64[j]);
        const int len = decimal_to_ascii(val, scale, tmp);
        const uint8_t* bytes = reinterpret_cast<const uint8_t*>(tmp);
        if (static_cast<uint32_t>(len) > STR_INLINE_MAX) {
            const uint32_t off = static_cast<uint32_t>(arena_used);
            std::memcpy(arena + off, tmp, static_cast<size_t>(len));
            draken_build_string_slot(&slots[j], bytes, static_cast<uint32_t>(len), off);
            arena_used += static_cast<size_t>(len);
        } else {
            draken_build_string_slot(&slots[j], bytes, static_cast<uint32_t>(len), 0u);
        }
    }

    VecResult r = vecresult_from_string_block(block, k, total_extern, 0, DRAKEN_VARCHAR);
    kernel_preserve_shape(r, v);
    return r;
}

// DECIMAL (int64-backed, p≤18) → VARCHAR.
VecResult draken_cast_decimal_to_string(void* ctx, const DrakenVector* v) {
    DRAKEN_KERNEL_TRY({ return decimal_to_string_core(ctx, v, /*is128=*/false); });
}

// DECIMAL128 (int128-backed, p≤38) → VARCHAR.
VecResult draken_cast_decimal128_to_string(void* ctx, const DrakenVector* v) {
    DRAKEN_KERNEL_TRY({ return decimal_to_string_core(ctx, v, /*is128=*/true); });
}

// → VARBINARY (BLOB) thin wrappers. A numeric/bool/decimal-to-string cast
// formats the exact same ASCII bytes regardless of whether the SQL target is
// VARCHAR or VARBINARY — VARCHAR and VARBINARY share the identical
// DrakenStringArena layout (buffers.h §11), differing only in the type tag. So
// each wrapper below reuses the already-correct `_to_string` kernel body and
// retags a successful result, rather than duplicating the formatting logic.
// (Prior to this, `_c_native_cast` routed numeric->VARBINARY casts straight to
// the `_to_string` kernel, which mistagged the result as VARCHAR — a silent
// wrong-type bug, not just a missing-kernel gap.)
#define DRAKEN_CAST_TO_BLOB(fn_blob, fn_string)                    \
    VecResult fn_blob(void* ctx, const DrakenVector* v) {          \
        VecResult r = fn_string(ctx, v);                           \
        if (r.data != nullptr) r.type = DRAKEN_VARBINARY;          \
        return r;                                                  \
    }

DRAKEN_CAST_TO_BLOB(draken_cast_int64_to_blob, draken_cast_int64_to_string)
DRAKEN_CAST_TO_BLOB(draken_cast_integer_to_blob, draken_cast_integer_to_string)
DRAKEN_CAST_TO_BLOB(draken_cast_uint_to_blob, draken_cast_uint_to_string)
DRAKEN_CAST_TO_BLOB(draken_cast_float64_to_blob, draken_cast_float64_to_string)
DRAKEN_CAST_TO_BLOB(draken_cast_bool_to_blob, draken_cast_bool_to_string)
DRAKEN_CAST_TO_BLOB(draken_cast_decimal_to_blob, draken_cast_decimal_to_string)
DRAKEN_CAST_TO_BLOB(draken_cast_decimal128_to_blob, draken_cast_decimal128_to_string)

#undef DRAKEN_CAST_TO_BLOB

// INTEGER (INT32/INT16/INT8) → FLOAT64: lossless widening.
VecResult draken_cast_integer_to_float64(void* ctx, const DrakenVector* v) {
    DRAKEN_KERNEL_TRY({
        if (!v) return draken_error_sentinel("Input vector is null");
        if (v->type != DRAKEN_INT32 && v->type != DRAKEN_INT16 && v->type != DRAKEN_INT8)
            return draken_error_sentinel_fmt("cast integer->float64: expected INT32/16/8, got %d", v->type);
        const uint32_t k = v->data_length;
        const DrakenType st = v->type;
        double* out = static_cast<double*>(draken_malloc((k > 0u ? k : 1u) * sizeof(double)));
        if (!out) return draken_error_sentinel("Allocation failed");
        for (uint32_t j = 0u; j < k; ++j) {
            if (st == DRAKEN_INT32)
                out[j] = static_cast<double>(static_cast<const int32_t*>(v->data)[j]);
            else if (st == DRAKEN_INT16)
                out[j] = static_cast<double>(static_cast<const int16_t*>(v->data)[j]);
            else
                out[j] = static_cast<double>(static_cast<const int8_t*>(v->data)[j]);
        }
        VecResult r;
        r.data = out; r.type = DRAKEN_FLOAT64; r.validity_embedded = 0u; r.ts_unit = 0xFFu;
        kernel_preserve_shape(r, v);
        return r;
    });
}

// INTEGER (INT32/INT16/INT8) → INT64: sign-extending widening.
VecResult draken_cast_integer_to_int64(void* ctx, const DrakenVector* v) {
    DRAKEN_KERNEL_TRY({
        if (!v) return draken_error_sentinel("Input vector is null");
        if (v->type != DRAKEN_INT32 && v->type != DRAKEN_INT16 && v->type != DRAKEN_INT8)
            return draken_error_sentinel_fmt("cast integer->int64: expected INT32/16/8, got %d", v->type);
        const uint32_t k = v->data_length;
        const DrakenType st = v->type;
        int64_t* out = static_cast<int64_t*>(draken_malloc((k > 0u ? k : 1u) * sizeof(int64_t)));
        if (!out) return draken_error_sentinel("Allocation failed");
        // No disposition read: a widening cannot fail, so TRY_CAST and CAST are
        // the same operation here.
        for (uint32_t j = 0u; j < k; ++j) {
            if (st == DRAKEN_INT32)
                out[j] = static_cast<int64_t>(static_cast<const int32_t*>(v->data)[j]);
            else if (st == DRAKEN_INT16)
                out[j] = static_cast<int64_t>(static_cast<const int16_t*>(v->data)[j]);
            else
                out[j] = static_cast<int64_t>(static_cast<const int8_t*>(v->data)[j]);
        }
        VecResult r;
        r.data = out; r.type = DRAKEN_INT64; r.validity_embedded = 0u; r.ts_unit = 0xFFu;
        kernel_preserve_shape(r, v);
        return r;
    });
}

// E33 — read the j-th physical value of ANY signed integer vector (INT8/16/32/64)
// as int64_t, sign-extending narrower widths (mirrors draken_cast_integer_to_int64's
// per-width read, generalized to include INT64 itself so one helper covers all
// four signed source widths for the unsigned-target casts below).
static inline int64_t cast_read_signed_i64(const DrakenVector* v, uint32_t j) {
    switch (v->type) {
        case DRAKEN_INT8:  return static_cast<int64_t>(static_cast<const int8_t* >(v->data)[j]);
        case DRAKEN_INT16: return static_cast<int64_t>(static_cast<const int16_t*>(v->data)[j]);
        case DRAKEN_INT32: return static_cast<int64_t>(static_cast<const int32_t*>(v->data)[j]);
        default:           return static_cast<const int64_t*>(v->data)[j];  // DRAKEN_INT64
    }
}

// E33 — signed integer (any of INT8/16/32/64) -> unsigned target, range-checked.
// Negative values and magnitudes exceeding the target width both raise
// std::overflow_error (fail loud — CLAUDE.md §1 — never silently truncate/wrap).
// Null rows are skipped (any garbage placeholder is fine; validity masks them).
#define DRAKEN_CAST_SIGNED_TO_UINT(fn_name, UT, TAG, UMAX)                              \
VecResult fn_name(void* ctx, const DrakenVector* v) {                                    \
    DRAKEN_KERNEL_TRY({                                                                 \
        if (!v) return draken_error_sentinel("Input vector is null");                   \
        if (v->type != DRAKEN_INT8 && v->type != DRAKEN_INT16 &&                        \
            v->type != DRAKEN_INT32 && v->type != DRAKEN_INT64)                          \
            return draken_error_sentinel_fmt(                                           \
                #fn_name ": expected a signed integer source, got %d", v->type);         \
        const uint32_t k = v->data_length;                                              \
        UT* out = static_cast<UT*>(draken_malloc((k > 0u ? k : 1u) * sizeof(UT)));       \
        if (!out) return draken_error_sentinel("Allocation failed");                    \
        const bool is_safe = kernel_cast_is_safe(ctx);                                  \
        std::vector<uint8_t> bad(k > 0u ? k : 1u, 0u);                                  \
        bool any_bad = false;                                                           \
        const bool has_nulls = (v->validity != nullptr);                                \
        for (uint32_t j = 0u; j < k; ++j) {                                             \
            if (has_nulls && !((v->validity[j >> 3] >> (j & 7u)) & 1u)) {                \
                out[j] = 0; continue;                                                    \
            }                                                                            \
            const int64_t val = cast_read_signed_i64(v, j);                              \
            if (val < 0 || static_cast<uint64_t>(val) > (UMAX)) {                        \
                if (!is_safe) {                                                          \
                    draken_free(out);                                                        \
                    return draken_error_sentinel_fmt(                                       \
                        #fn_name ": value %lld out of range for " #UT, (long long)val);      \
                }                                                                        \
                out[j] = 0; bad[j] = 1u; any_bad = true; continue;                       \
            }                                                                            \
            out[j] = static_cast<UT>(val);                                              \
        }                                                                                \
        VecResult r;                                                                    \
        r.data = out; r.type = TAG; r.validity_embedded = 0u; r.ts_unit = 0xFFu;         \
        kernel_preserve_shape(r, v);                                                    \
        if (any_bad) kernel_null_bad_rows(r, v, bad.data());                             \
        return r;                                                                        \
    });                                                                                  \
}

DRAKEN_CAST_SIGNED_TO_UINT(draken_cast_integer_to_uint8,  uint8_t,  DRAKEN_UINT8,  255ull)
DRAKEN_CAST_SIGNED_TO_UINT(draken_cast_integer_to_uint16, uint16_t, DRAKEN_UINT16, 65535ull)
DRAKEN_CAST_SIGNED_TO_UINT(draken_cast_integer_to_uint32, uint32_t, DRAKEN_UINT32, 4294967295ull)
DRAKEN_CAST_SIGNED_TO_UINT(draken_cast_integer_to_uint64, uint64_t, DRAKEN_UINT64, 0xFFFFFFFFFFFFFFFFull)

#undef DRAKEN_CAST_SIGNED_TO_UINT

// E33 — reverse direction: unsigned source (any of UINT8/16/32/64) -> INT64.
// UINT8/16/32 always fit int64_t's range losslessly (no check needed, unlike
// the other direction); UINT64 needs one range check (values > INT64_MAX raise
// rather than silently wrapping to negative).
VecResult draken_cast_uint_to_int64(void* ctx, const DrakenVector* v) {
    DRAKEN_KERNEL_TRY({
        if (!v) return draken_error_sentinel("Input vector is null");
        if (v->type != DRAKEN_UINT8 && v->type != DRAKEN_UINT16 &&
            v->type != DRAKEN_UINT32 && v->type != DRAKEN_UINT64)
            return draken_error_sentinel_fmt(
                "draken_cast_uint_to_int64: expected an unsigned integer source, got %d", v->type);
        const uint32_t k = v->data_length;
        const DrakenType st = v->type;
        int64_t* out = static_cast<int64_t*>(draken_malloc((k > 0u ? k : 1u) * sizeof(int64_t)));
        if (!out) return draken_error_sentinel("Allocation failed");
        const bool is_safe = kernel_cast_is_safe(ctx);
        std::vector<uint8_t> bad(k > 0u ? k : 1u, 0u);
        bool any_bad = false;
        for (uint32_t j = 0u; j < k; ++j) {
            if (st == DRAKEN_UINT8)
                out[j] = static_cast<int64_t>(static_cast<const uint8_t*>(v->data)[j]);
            else if (st == DRAKEN_UINT16)
                out[j] = static_cast<int64_t>(static_cast<const uint16_t*>(v->data)[j]);
            else if (st == DRAKEN_UINT32)
                out[j] = static_cast<int64_t>(static_cast<const uint32_t*>(v->data)[j]);
            else {
                const uint64_t uv = static_cast<const uint64_t*>(v->data)[j];
                if (uv > 0x7FFFFFFFFFFFFFFFull) {
                    if (!is_safe) {
                        draken_free(out);
                        return draken_error_sentinel_fmt(
                            "draken_cast_uint_to_int64: value %llu out of range for INT64",
                            (unsigned long long)uv);
                    }
                    out[j] = 0; bad[j] = 1u; any_bad = true; continue;
                }
                out[j] = static_cast<int64_t>(uv);
            }
        }
        VecResult r;
        r.data = out; r.type = DRAKEN_INT64; r.validity_embedded = 0u; r.ts_unit = 0xFFu;
        kernel_preserve_shape(r, v);
        if (any_bad) kernel_null_bad_rows(r, v, bad.data());
        return r;
    });
}

// E33 — FLOAT64/FLOAT32 -> unsigned target, range-checked. Negative values
// (including NaN, which fails every comparison) and magnitudes exceeding the
// target width both raise (fail loud — CLAUDE.md §1 — never silently
// truncate/wrap), mirroring DRAKEN_CAST_SIGNED_TO_UINT above. Truncates
// toward zero otherwise, matching draken_cast_float64_to_int64's convention.
#define DRAKEN_CAST_FLOAT_TO_UINT(fn_name, UT, TAG, UMAX)                                \
VecResult fn_name(void* ctx, const DrakenVector* v) {                                     \
    DRAKEN_KERNEL_TRY({                                                                  \
        if (!v) return draken_error_sentinel("Input vector is null");                    \
        if (v->type != DRAKEN_FLOAT64 && v->type != DRAKEN_FLOAT32)                       \
            return draken_error_sentinel_fmt(                                            \
                #fn_name ": expected FLOAT, got %d", v->type);                            \
        const uint32_t k = v->data_length;                                               \
        const bool is64 = (v->type == DRAKEN_FLOAT64);                                   \
        UT* out = static_cast<UT*>(draken_malloc((k > 0u ? k : 1u) * sizeof(UT)));        \
        if (!out) return draken_error_sentinel("Allocation failed");                     \
        const bool is_safe = kernel_cast_is_safe(ctx);                                  \
        std::vector<uint8_t> bad(k > 0u ? k : 1u, 0u);                                  \
        bool any_bad = false;                                                           \
        for (uint32_t j = 0u; j < k; ++j) {                                              \
            const double d = is64 ? static_cast<const double*>(v->data)[j]               \
                                  : static_cast<double>(static_cast<const float*>(v->data)[j]); \
            if (!(d >= 0.0) || d > static_cast<double>(UMAX)) {                          \
                if (!is_safe) {                                                          \
                    draken_free(out);                                                         \
                    return draken_error_sentinel_fmt(                                         \
                        #fn_name ": value %g out of range for " #UT, d);                       \
                }                                                                        \
                out[j] = 0; bad[j] = 1u; any_bad = true; continue;                       \
            }                                                                             \
            out[j] = static_cast<UT>(d);                                                  \
        }                                                                                 \
        VecResult r;                                                                     \
        r.data = out; r.type = TAG; r.validity_embedded = 0u; r.ts_unit = 0xFFu;          \
        kernel_preserve_shape(r, v);                                                      \
        if (any_bad) kernel_null_bad_rows(r, v, bad.data());                             \
        return r;                                                                          \
    });                                                                                    \
}

DRAKEN_CAST_FLOAT_TO_UINT(draken_cast_float_to_uint8,  uint8_t,  DRAKEN_UINT8,  255ull)
DRAKEN_CAST_FLOAT_TO_UINT(draken_cast_float_to_uint16, uint16_t, DRAKEN_UINT16, 65535ull)
DRAKEN_CAST_FLOAT_TO_UINT(draken_cast_float_to_uint32, uint32_t, DRAKEN_UINT32, 4294967295ull)
DRAKEN_CAST_FLOAT_TO_UINT(draken_cast_float_to_uint64, uint64_t, DRAKEN_UINT64, 0xFFFFFFFFFFFFFFFFull)

#undef DRAKEN_CAST_FLOAT_TO_UINT

// E33 — BOOL -> unsigned target. Always 0 or 1, so no range check is ever
// needed for any width (mirrors draken_cast_bool_to_int64).
#define DRAKEN_CAST_BOOL_TO_UINT(fn_name, UT, TAG)                                        \
VecResult fn_name(void* ctx, const DrakenVector* v) {                                     \
    DRAKEN_KERNEL_TRY({                                                                  \
        if (!v) return draken_error_sentinel("Input vector is null");                    \
        if (v->type != DRAKEN_BOOL)                                                      \
            return draken_error_sentinel_fmt(#fn_name ": expected BOOL, got %d", v->type); \
        const uint32_t k = v->data_length;                                               \
        const uint8_t* src = static_cast<const uint8_t*>(v->data);                        \
        UT* out = static_cast<UT*>(draken_malloc((k > 0u ? k : 1u) * sizeof(UT)));        \
        if (!out) return draken_error_sentinel("Allocation failed");                     \
        for (uint32_t j = 0u; j < k; ++j)                                                \
            out[j] = static_cast<UT>((src[j >> 3u] >> (j & 7u)) & 1u);                    \
        VecResult r;                                                                     \
        r.data = out; r.type = TAG; r.validity_embedded = 0u; r.ts_unit = 0xFFu;          \
        kernel_preserve_shape(r, v);                                                      \
        return r;                                                                          \
    });                                                                                    \
}

DRAKEN_CAST_BOOL_TO_UINT(draken_cast_bool_to_uint8,  uint8_t,  DRAKEN_UINT8)
DRAKEN_CAST_BOOL_TO_UINT(draken_cast_bool_to_uint16, uint16_t, DRAKEN_UINT16)
DRAKEN_CAST_BOOL_TO_UINT(draken_cast_bool_to_uint32, uint32_t, DRAKEN_UINT32)
DRAKEN_CAST_BOOL_TO_UINT(draken_cast_bool_to_uint64, uint64_t, DRAKEN_UINT64)

#undef DRAKEN_CAST_BOOL_TO_UINT

// FLOAT → DECIMAL (both tiers; SQL half-away-from-zero rounding at the target
// scale). ctx = binary_op_ctx: result_scale + result_precision (p>18 → int128
// tier). DENSE output (uniform selection gather — see the date32 cast lesson).
VecResult draken_cast_float_to_decimal(void* ctx, const DrakenVector* v) {
    DRAKEN_KERNEL_TRY({
        if (!v) return draken_error_sentinel("Input vector is null");
        if (v->type != DRAKEN_FLOAT64 && v->type != DRAKEN_FLOAT32)
            return draken_error_sentinel_fmt("cast float->decimal: expected FLOAT, got %d", v->type);
        if (!ctx) return draken_error_sentinel("cast float->decimal: missing ctx (scale)");
        const auto* c = static_cast<const binary_op_ctx*>(ctx);
        const int scale = c->result_scale;
        const bool wide = c->result_precision > 18;
        double mult = 1.0;
        for (int i = 0; i < scale; ++i) mult *= 10.0;
        const uint32_t n = v->length;
        const size_t es = wide ? 16u : 8u;
        uint8_t* out = static_cast<uint8_t*>(draken_malloc((n > 0u ? n : 1u) * es));
        if (!out) return draken_error_sentinel("Allocation failed");
        for (uint32_t j = 0u; j < n; ++j) {
            uint32_t phys = v->selection[j];
            double d = (v->type == DRAKEN_FLOAT32)
                ? static_cast<double>(static_cast<const float*>(v->data)[phys])
                : static_cast<const double*>(v->data)[phys];
            double scaled = d * mult;
            // half away from zero
            double rounded = scaled >= 0.0 ? std::floor(scaled + 0.5) : std::ceil(scaled - 0.5);
            if (wide) {
                __int128 w = static_cast<__int128>(rounded);
                std::memcpy(out + static_cast<size_t>(j) * 16u, &w, 16u);
            } else {
                int64_t w = static_cast<int64_t>(rounded);
                std::memcpy(out + static_cast<size_t>(j) * 8u, &w, 8u);
            }
        }
        VecResult r;
        r.data = out;
        r.type = wide ? DRAKEN_DECIMAL128 : DRAKEN_DECIMAL;
        r.validity_embedded = 0u;
        r.dec_precision = c->result_precision;
        r.dec_scale = static_cast<uint8_t>(scale);
        r.length = n; r.data_length = n;
        r.selection = draken_identity_sel(n);
        r.owns_selection = false;
        r.validity = kernel_copy_validity(v);
        r.flags = DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION;
        return r;
    });
}

// --- DECIMAL → DECIMAL (either tier, any scale) --------------------------------
//
// The unscaled payload is meaningless without its scale, and the DrakenVector does
// NOT carry one (§11/§14) — so BOTH scales ride in the binary_op_ctx the binder
// fills: `left_scale` is the SOURCE scale, `result_scale`/`result_precision` the
// TARGET (precision > 18 selects the int128 tier). Same mechanism the DECIMAL
// binop and `_to_string` cast kernels already use.
//
// Until this kernel existed there was NO decimal→decimal cast at all: the pair
// fell through `_c_native_cast` to the Python closure, which the native engine
// refuses ("outside the c-native kernel set"). That made every promoted-DECIMAL
// CASE/blend unrunnable once the promotion crossed the int64→int128 tier.
//
// Value policy mirrors _build_decimal_closure (casts.pyx) — a declared type is a
// contract, not a hint:
//   - widening (target scale > source): multiply by 10^delta, overflow FAILS LOUD.
//   - narrowing (target scale < source): exact only. Digits that would be DROPPED
//     fail loud; trailing zeros re-pad silently (DECIMAL(5,3) 1500 → DECIMAL(5,1) 15).
//   - a value outside the DECLARED precision fails loud rather than being stored
//     and silently mis-read later.
// Null rows are skipped entirely (placeholder 0, validity copied): their payload is
// undefined, and range-checking undefined bytes would raise on a row SQL never reads.
//
// DENSE output (uniform data[selection[j]] gather over logical rows), matching the
// float→decimal kernel above.

// 10^k as __int128; k is bounded by the DECIMAL(38) precision cap.
static __int128 dec_pow10(int k) noexcept {
    __int128 r = 1;
    while (k-- > 0) r *= 10;
    return r;
}

// One (source tier → destination tier) rescale loop. Written as a macro rather than
// a template because this translation unit is one extern "C" block (templates are
// not permitted there), and as four specialisations rather than one branchy loop so
// the per-row path carries no tier test (§3).
#define DRAKEN_DEC_RESCALE_LOOP(SRC_T, DST_T)                                          \
    for (uint32_t j = 0u; j < n; ++j) {                                                \
        DST_T* dst = reinterpret_cast<DST_T*>(out) + j;                                \
        if (val_in && ((val_in[j >> 3u] >> (j & 7u)) & 1u) == 0u) { *dst = 0; continue; } \
        __int128 x = static_cast<__int128>(                                            \
            static_cast<const SRC_T*>(v->data)[v->selection[j]]);                      \
        if (delta > 0) {                                                               \
            if (x > mul_lim || x < -mul_lim) {                                         \
                if (!is_safe) {                                                        \
                    draken_free(out);                                                  \
                    return draken_error_sentinel_fmt(                                  \
                        "cast decimal->decimal: value overflows DECIMAL(%d, %d)",      \
                        (int)c->result_precision, (int)c->result_scale);               \
                }                                                                      \
                *dst = 0; bad[j] = 1u; any_bad = true; continue;                       \
            }                                                                          \
            x *= factor;                                                               \
        } else if (delta < 0) {                                                        \
            if (x % factor != 0) {                                                     \
                if (!is_safe) {                                                        \
                    draken_free(out);                                                  \
                    return draken_error_sentinel_fmt(                                  \
                        "cast decimal->decimal: value has more decimal places than "   \
                        "the declared scale %d", (int)c->result_scale);                \
                }                                                                      \
                *dst = 0; bad[j] = 1u; any_bad = true; continue;                       \
            }                                                                          \
            x /= factor;                                                               \
        }                                                                              \
        if (x >= dec_lim || x <= -dec_lim) {                                           \
            if (!is_safe) {                                                            \
                draken_free(out);                                                      \
                return draken_error_sentinel_fmt(                                      \
                    "cast decimal->decimal: value overflows DECIMAL(%d, %d)",          \
                    (int)c->result_precision, (int)c->result_scale);                   \
            }                                                                          \
            *dst = 0; bad[j] = 1u; any_bad = true; continue;                           \
        }                                                                              \
        *dst = static_cast<DST_T>(x);                                                  \
    }

static VecResult decimal_to_decimal_core(void* ctx, const DrakenVector* v, bool src128) {
    if (!v) return draken_error_sentinel("Input vector is null");
    const DrakenType want = src128 ? DRAKEN_DECIMAL128 : DRAKEN_DECIMAL;
    if (v->type != want)
        return draken_error_sentinel_fmt("cast decimal->decimal: expected %d, got %d",
                                         (int)want, (int)v->type);
    if (!ctx) return draken_error_sentinel("cast decimal->decimal: missing ctx (scales)");
    const auto* c = static_cast<const binary_op_ctx*>(ctx);
    if (c->result_precision == 0u || c->result_precision > 38u)
        return draken_error_sentinel_fmt("cast decimal->decimal: bad target precision %d",
                                         (int)c->result_precision);

    const int delta = static_cast<int>(c->result_scale) - static_cast<int>(c->left_scale);
    const __int128 factor  = dec_pow10(delta >= 0 ? delta : -delta);
    const __int128 dec_lim = dec_pow10(static_cast<int>(c->result_precision));
    // Largest magnitude that can still be multiplied by `factor` without leaving the
    // declared precision — checked BEFORE the multiply, so it can never wrap.
    const __int128 mul_lim = (delta > 0) ? (dec_lim - 1) / factor : 0;

    const bool dst128 = c->result_precision > 18u;
    const uint32_t n = v->length;
    const size_t es = dst128 ? 16u : 8u;
    uint8_t* out = static_cast<uint8_t*>(draken_malloc((n > 0u ? n : 1u) * es));
    if (!out) return draken_error_sentinel("Allocation failed");
    const uint8_t* val_in = v->validity;
    const bool is_safe = kernel_cast_is_safe(ctx);
    std::vector<uint8_t> bad(n > 0u ? n : 1u, 0u);
    bool any_bad = false;

    if (src128) {
        if (dst128) { DRAKEN_DEC_RESCALE_LOOP(__int128, __int128) }
        else        { DRAKEN_DEC_RESCALE_LOOP(__int128, int64_t) }
    } else {
        if (dst128) { DRAKEN_DEC_RESCALE_LOOP(int64_t, __int128) }
        else        { DRAKEN_DEC_RESCALE_LOOP(int64_t, int64_t) }
    }

    VecResult r;
    r.data = out;
    r.type = dst128 ? DRAKEN_DECIMAL128 : DRAKEN_DECIMAL;
    r.validity_embedded = 0u;
    r.ts_unit = 0xFFu;
    r.dec_precision = c->result_precision;
    r.dec_scale = c->result_scale;
    r.length = n; r.data_length = n;
    r.selection = draken_identity_sel(n);
    r.owns_selection = false;
    r.validity = kernel_copy_validity(v);
    r.flags = DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION;
    if (any_bad) kernel_null_bad_rows(r, v, bad.data());
    return r;
}

#undef DRAKEN_DEC_RESCALE_LOOP

VecResult draken_cast_decimal_to_decimal(void* ctx, const DrakenVector* v) {
    DRAKEN_KERNEL_TRY({ return decimal_to_decimal_core(ctx, v, /*src128=*/false); });
}

VecResult draken_cast_decimal128_to_decimal(void* ctx, const DrakenVector* v) {
    DRAKEN_KERNEL_TRY({ return decimal_to_decimal_core(ctx, v, /*src128=*/true); });
}

// --- INTEGER → DECIMAL ---------------------------------------------------------
//
// An integer IS a decimal at scale 0, so the stored unscaled payload is simply
// value * 10^target_scale. ctx = binary_op_ctx: result_scale / result_precision
// (>18 selects the int128 tier). `left_scale` is unused — an integer source has no
// scale of its own.
//
// This closed the last hole in the promoted-DECIMAL blend: a CASE (or UNION leg)
// pairing an INTEGER COLUMN with a DECIMAL branch needs exactly this cast, and with
// no kernel for it the plan compiler refused the whole query ("outside the c-native
// kernel set"). Integer LITERALS never needed it — the binder retypes those in place.
//
// The multiply is range-checked against the DECLARED precision BEFORE it happens, so
// an out-of-range value fails loud instead of wrapping — same contract as the
// decimal→decimal rescale above. Null rows are skipped (undefined payload).

#define DRAKEN_INT_TO_DEC_LOOP(SRC_T, DST_T)                                           \
    for (uint32_t j = 0u; j < n; ++j) {                                                \
        DST_T* dst = reinterpret_cast<DST_T*>(out) + j;                                \
        if (val_in && ((val_in[j >> 3u] >> (j & 7u)) & 1u) == 0u) { *dst = 0; continue; } \
        __int128 x = static_cast<__int128>(                                            \
            static_cast<const SRC_T*>(v->data)[v->selection[j]]);                      \
        if (x > mul_lim || x < -mul_lim) {                                             \
            if (!is_safe) {                                                            \
                draken_free(out);                                                      \
                return draken_error_sentinel_fmt(                                      \
                    "cast integer->decimal: value overflows DECIMAL(%d, %d)",          \
                    (int)c->result_precision, (int)c->result_scale);                   \
            }                                                                          \
            *dst = 0; bad[j] = 1u; any_bad = true; continue;                           \
        }                                                                              \
        *dst = static_cast<DST_T>(x * factor);                                         \
    }

// Both destination tiers for one source width. `dst128` is loop-invariant, so this
// keeps the per-row path free of any tier test (§3).
#define DRAKEN_INT_TO_DEC_SRC(SRC_T)                                                   \
    if (dst128) { DRAKEN_INT_TO_DEC_LOOP(SRC_T, __int128) }                            \
    else        { DRAKEN_INT_TO_DEC_LOOP(SRC_T, int64_t) }

static VecResult int_to_decimal_core(void* ctx, const DrakenVector* v, const char* who) {
    if (!v) return draken_error_sentinel("Input vector is null");
    if (!ctx) return draken_error_sentinel_fmt("%s: missing ctx (scale)", who);
    const auto* c = static_cast<const binary_op_ctx*>(ctx);
    if (c->result_precision == 0u || c->result_precision > 38u)
        return draken_error_sentinel_fmt("%s: bad target precision %d",
                                         who, (int)c->result_precision);

    const __int128 factor  = dec_pow10(static_cast<int>(c->result_scale));
    const __int128 dec_lim = dec_pow10(static_cast<int>(c->result_precision));
    const __int128 mul_lim = (dec_lim - 1) / factor;

    const bool dst128 = c->result_precision > 18u;
    const uint32_t n = v->length;
    const size_t es = dst128 ? 16u : 8u;
    uint8_t* out = static_cast<uint8_t*>(draken_malloc((n > 0u ? n : 1u) * es));
    if (!out) return draken_error_sentinel("Allocation failed");
    const uint8_t* val_in = v->validity;
    const bool is_safe = kernel_cast_is_safe(ctx);
    std::vector<uint8_t> bad(n > 0u ? n : 1u, 0u);
    bool any_bad = false;

    switch (v->type) {
        case DRAKEN_INT8:   { DRAKEN_INT_TO_DEC_SRC(int8_t)   break; }
        case DRAKEN_INT16:  { DRAKEN_INT_TO_DEC_SRC(int16_t)  break; }
        case DRAKEN_INT32:  { DRAKEN_INT_TO_DEC_SRC(int32_t)  break; }
        case DRAKEN_INT64:  { DRAKEN_INT_TO_DEC_SRC(int64_t)  break; }
        case DRAKEN_UINT8:  { DRAKEN_INT_TO_DEC_SRC(uint8_t)  break; }
        case DRAKEN_UINT16: { DRAKEN_INT_TO_DEC_SRC(uint16_t) break; }
        case DRAKEN_UINT32: { DRAKEN_INT_TO_DEC_SRC(uint32_t) break; }
        case DRAKEN_UINT64: { DRAKEN_INT_TO_DEC_SRC(uint64_t) break; }
        default:
            draken_free(out);
            return draken_error_sentinel_fmt("%s: expected an integer source, got %d",
                                             who, (int)v->type);
    }

    VecResult r;
    r.data = out;
    r.type = dst128 ? DRAKEN_DECIMAL128 : DRAKEN_DECIMAL;
    r.validity_embedded = 0u;
    r.ts_unit = 0xFFu;
    r.dec_precision = c->result_precision;
    r.dec_scale = c->result_scale;
    r.length = n; r.data_length = n;
    r.selection = draken_identity_sel(n);
    r.owns_selection = false;
    r.validity = kernel_copy_validity(v);
    r.flags = DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION;
    if (any_bad) kernel_null_bad_rows(r, v, bad.data());
    return r;
}

#undef DRAKEN_INT_TO_DEC_SRC
#undef DRAKEN_INT_TO_DEC_LOOP

// Three entry points, one core — the names follow the established source-width
// convention (`_int64_`, `_integer_` for INT8/16/32, `_uint_` for every unsigned
// width) so the bind-time table in casts.pyx reads like its INT64/FLOAT64 siblings.
VecResult draken_cast_int64_to_decimal(void* ctx, const DrakenVector* v) {
    DRAKEN_KERNEL_TRY({ return int_to_decimal_core(ctx, v, "cast int64->decimal"); });
}

VecResult draken_cast_integer_to_decimal(void* ctx, const DrakenVector* v) {
    DRAKEN_KERNEL_TRY({ return int_to_decimal_core(ctx, v, "cast integer->decimal"); });
}

VecResult draken_cast_uint_to_decimal(void* ctx, const DrakenVector* v) {
    DRAKEN_KERNEL_TRY({ return int_to_decimal_core(ctx, v, "cast uint->decimal"); });
}

// --- BOOL → DECIMAL ------------------------------------------------------------
//
// true is the decimal 1, false the decimal 0 — the same integer promotion BOOL
// already has to every other numeric target (int64, float32/64, and the narrow
// signed and unsigned families). DECIMAL was the one hole in that row, which made
// `SELECT dec_col UNION ALL SELECT bool_expr` unrunnable: the union coerces the
// legs to DECIMAL (find_compatible_type), inserts a CAST, and the missing arm then
// failed the compiler's c-native admission gate.
//
// BOOL cannot join int_to_decimal_core's `switch (v->type)`: its payload is
// BIT-packed, one bit per PHYSICAL slot, not a fixed-width lane the SRC_T macro can
// index. So it gets its own loop, exactly as draken_cast_bool_to_int64 does.
//
// The range check is NOT vestigial. `true` is stored as 10^scale, which leaves the
// declared precision whenever scale >= precision — DECIMAL(1,1) spans -0.9..0.9 and
// genuinely cannot hold 1. Hoisted out of the loop (`one_fits`) because it depends
// only on the target type, never on the row (§3).
//
// DENSE output, matching every other → DECIMAL kernel in this file.
VecResult draken_cast_bool_to_decimal(void* ctx, const DrakenVector* v) {
    DRAKEN_KERNEL_TRY({
        if (!v) return draken_error_sentinel("Input vector is null");
        if (v->type != DRAKEN_BOOL)
            return draken_error_sentinel_fmt("cast bool->decimal: expected BOOL, got %d", v->type);
        if (!ctx) return draken_error_sentinel("cast bool->decimal: missing ctx (scale)");
        const auto* c = static_cast<const binary_op_ctx*>(ctx);
        if (c->result_precision == 0u || c->result_precision > 38u)
            return draken_error_sentinel_fmt("cast bool->decimal: bad target precision %d",
                                             (int)c->result_precision);

        const __int128 factor  = dec_pow10(static_cast<int>(c->result_scale));
        const __int128 dec_lim = dec_pow10(static_cast<int>(c->result_precision));
        const bool one_fits = factor < dec_lim;   // `true` is the only value that can overflow

        const bool dst128 = c->result_precision > 18u;
        const uint32_t n = v->length;
        const size_t es = dst128 ? 16u : 8u;
        uint8_t* out = static_cast<uint8_t*>(draken_malloc((n > 0u ? n : 1u) * es));
        if (!out) return draken_error_sentinel("Allocation failed");
        const uint8_t* src = static_cast<const uint8_t*>(v->data);
        const uint8_t* val_in = v->validity;
        const bool is_safe = kernel_cast_is_safe(ctx);
        std::vector<uint8_t> bad(n > 0u ? n : 1u, 0u);
        bool any_bad = false;

        for (uint32_t j = 0u; j < n; ++j) {
            uint8_t* dst = out + static_cast<size_t>(j) * es;
            // validity is indexed by LOGICAL row, the payload bit by PHYSICAL slot.
            if (val_in && ((val_in[j >> 3u] >> (j & 7u)) & 1u) == 0u) {
                std::memset(dst, 0, es); continue;
            }
            const uint32_t phys = v->selection[j];
            const unsigned bit = (src[phys >> 3u] >> (phys & 7u)) & 1u;
            if (bit && !one_fits) {
                if (!is_safe) {
                    draken_free(out);
                    return draken_error_sentinel_fmt(
                        "cast bool->decimal: value overflows DECIMAL(%d, %d)",
                        (int)c->result_precision, (int)c->result_scale);
                }
                std::memset(dst, 0, es); bad[j] = 1u; any_bad = true; continue;
            }
            if (dst128) {
                __int128 w = bit ? factor : static_cast<__int128>(0);
                std::memcpy(dst, &w, 16u);
            } else {
                int64_t w = bit ? static_cast<int64_t>(factor) : static_cast<int64_t>(0);
                std::memcpy(dst, &w, 8u);
            }
        }

        VecResult r;
        r.data = out;
        r.type = dst128 ? DRAKEN_DECIMAL128 : DRAKEN_DECIMAL;
        r.validity_embedded = 0u;
        r.ts_unit = 0xFFu;
        r.dec_precision = c->result_precision;
        r.dec_scale = c->result_scale;
        r.length = n; r.data_length = n;
        r.selection = draken_identity_sel(n);
        r.owns_selection = false;
        r.validity = kernel_copy_validity(v);
        r.flags = DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION;
        if (any_bad) kernel_null_bad_rows(r, v, bad.data());
        return r;
    });
}

// --- DECIMAL → INT64 / FLOAT64 -------------------------------------------------
//
// The reverse of the two casts above, and the pair that made DECIMAL a dead end:
// with no kernel, `CAST(dec_col AS INTEGER)` and `CAST(dec_col AS DOUBLE)` were
// refused at plan time even though every other numeric type converts freely.
//
// ctx = binary_op_ctx, and ONLY `left_scale` is read — the SOURCE scale, which
// lives on the bind-time ColumnType and NOT on the runtime vector, exactly as
// the decimal→string kernels take it. No target precision is involved: INT64 and
// FLOAT64 have none.
//
// INTEGER: the fractional digits are TRUNCATED TOWARD ZERO (__int128 division
// truncates toward zero, so -15 / 10 == -1), matching
// draken_cast_float64_to_int64's stated convention — an engine where
// `1.9::DOUBLE::INTEGER` and `1.9::DECIMAL(2,1)::INTEGER` disagreed would be
// indefensible. Magnitudes beyond INT64 fail loud rather than wrap. NOTE this is
// deliberately LOOSER than decimal→decimal's narrowing rescale, which refuses to
// drop non-zero digits: that cast declares a scale and must honour it, while a
// cast to INTEGER states outright that no fractional part is wanted.
//
// FLOAT64: value / 10^scale in double. Inexact by construction (that is what
// binary floating point IS); the caller asked for a float.
//
// DENSE output (uniform data[selection[j]] gather), matching the kernels above.

#define DRAKEN_DEC_TO_I64_LOOP(SRC_T)                                                  \
    for (uint32_t j = 0u; j < n; ++j) {                                                \
        if (val_in && ((val_in[j >> 3u] >> (j & 7u)) & 1u) == 0u) { out[j] = 0; continue; } \
        const __int128 x = static_cast<__int128>(                                      \
            static_cast<const SRC_T*>(v->data)[v->selection[j]]) / factor;             \
        if (x > INT64_LIM || x < -INT64_LIM - 1) {                                     \
            if (!is_safe) {                                                            \
                draken_free(out);                                                      \
                return draken_error_sentinel(                                          \
                    "cast decimal->int64: value out of range for INT64");              \
            }                                                                          \
            out[j] = 0; bad[j] = 1u; any_bad = true; continue;                         \
        }                                                                              \
        out[j] = static_cast<int64_t>(x);                                              \
    }

static VecResult decimal_to_int64_core(void* ctx, const DrakenVector* v, bool src128) {
    if (!v) return draken_error_sentinel("Input vector is null");
    const DrakenType want = src128 ? DRAKEN_DECIMAL128 : DRAKEN_DECIMAL;
    if (v->type != want)
        return draken_error_sentinel_fmt("cast decimal->int64: expected %d, got %d",
                                         (int)want, (int)v->type);
    if (!ctx) return draken_error_sentinel("cast decimal->int64: missing ctx (source scale)");
    const auto* c = static_cast<const binary_op_ctx*>(ctx);
    if (c->left_scale > 38u)
        return draken_error_sentinel_fmt("cast decimal->int64: bad source scale %d",
                                         (int)c->left_scale);

    const __int128 factor = dec_pow10(static_cast<int>(c->left_scale));
    const __int128 INT64_LIM = static_cast<__int128>(INT64_MAX);
    const uint32_t n = v->length;
    int64_t* out = static_cast<int64_t*>(draken_malloc((n > 0u ? n : 1u) * sizeof(int64_t)));
    if (!out) return draken_error_sentinel("Allocation failed");
    const uint8_t* val_in = v->validity;
    const bool is_safe = kernel_cast_is_safe(ctx);
    std::vector<uint8_t> bad(n > 0u ? n : 1u, 0u);
    bool any_bad = false;

    if (src128) { DRAKEN_DEC_TO_I64_LOOP(__int128) }
    else        { DRAKEN_DEC_TO_I64_LOOP(int64_t) }

    VecResult r;
    r.data = out;
    r.type = DRAKEN_INT64;
    r.validity_embedded = 0u;
    r.ts_unit = 0xFFu;
    r.length = n; r.data_length = n;
    r.selection = draken_identity_sel(n);
    r.owns_selection = false;
    r.validity = kernel_copy_validity(v);
    r.flags = DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION;
    if (any_bad) kernel_null_bad_rows(r, v, bad.data());
    return r;
}

#undef DRAKEN_DEC_TO_I64_LOOP

#define DRAKEN_DEC_TO_F64_LOOP(SRC_T)                                                  \
    for (uint32_t j = 0u; j < n; ++j) {                                                \
        if (val_in && ((val_in[j >> 3u] >> (j & 7u)) & 1u) == 0u) { out[j] = 0.0; continue; } \
        out[j] = static_cast<double>(                                                  \
            static_cast<const SRC_T*>(v->data)[v->selection[j]]) / divisor;            \
    }

static VecResult decimal_to_float64_core(void* ctx, const DrakenVector* v, bool src128) {
    if (!v) return draken_error_sentinel("Input vector is null");
    const DrakenType want = src128 ? DRAKEN_DECIMAL128 : DRAKEN_DECIMAL;
    if (v->type != want)
        return draken_error_sentinel_fmt("cast decimal->float64: expected %d, got %d",
                                         (int)want, (int)v->type);
    if (!ctx) return draken_error_sentinel("cast decimal->float64: missing ctx (source scale)");
    const auto* c = static_cast<const binary_op_ctx*>(ctx);
    if (c->left_scale > 38u)
        return draken_error_sentinel_fmt("cast decimal->float64: bad source scale %d",
                                         (int)c->left_scale);

    const double divisor = static_cast<double>(dec_pow10(static_cast<int>(c->left_scale)));
    const uint32_t n = v->length;
    double* out = static_cast<double*>(draken_malloc((n > 0u ? n : 1u) * sizeof(double)));
    if (!out) return draken_error_sentinel("Allocation failed");
    const uint8_t* val_in = v->validity;

    if (src128) { DRAKEN_DEC_TO_F64_LOOP(__int128) }
    else        { DRAKEN_DEC_TO_F64_LOOP(int64_t) }

    VecResult r;
    r.data = out;
    r.type = DRAKEN_FLOAT64;
    r.validity_embedded = 0u;
    r.ts_unit = 0xFFu;
    r.length = n; r.data_length = n;
    r.selection = draken_identity_sel(n);
    r.owns_selection = false;
    r.validity = kernel_copy_validity(v);
    r.flags = DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION;
    return r;
}

#undef DRAKEN_DEC_TO_F64_LOOP

VecResult draken_cast_decimal_to_int64(void* ctx, const DrakenVector* v) {
    DRAKEN_KERNEL_TRY({ return decimal_to_int64_core(ctx, v, /*src128=*/false); });
}

VecResult draken_cast_decimal128_to_int64(void* ctx, const DrakenVector* v) {
    DRAKEN_KERNEL_TRY({ return decimal_to_int64_core(ctx, v, /*src128=*/true); });
}

VecResult draken_cast_decimal_to_float64(void* ctx, const DrakenVector* v) {
    DRAKEN_KERNEL_TRY({ return decimal_to_float64_core(ctx, v, /*src128=*/false); });
}

VecResult draken_cast_decimal128_to_float64(void* ctx, const DrakenVector* v) {
    DRAKEN_KERNEL_TRY({ return decimal_to_float64_core(ctx, v, /*src128=*/true); });
}

// --- unsigned integer -> unsigned integer ---------------------------------------
//
// The unsigned counterpart of the DRAKEN_CAST_SIGNED_TO_UINT family above, which
// takes SIGNED sources only and rejects an unsigned one outright — so before this
// existed, an unsigned column could not change width at all (not even UINT8 →
// UINT64, a widening that cannot fail).
//
// Range-checked on the way down: a value exceeding the target width raises rather
// than truncating. Widenings and the same-width copy cannot fail, and the check
// compiles away to a constant-false compare for those pairs.
//
// The UINT32 member additionally serves CAST(<unsigned> AS IPV4) — an address IS
// a uint32.

// Read the j-th physical value of ANY unsigned integer vector as uint64_t
// (mirrors cast_read_signed_i64 for the unsigned widths).
static inline uint64_t cast_read_unsigned_u64(const DrakenVector* v, uint32_t j) {
    switch (v->type) {
        case DRAKEN_UINT8:  return static_cast<uint64_t>(static_cast<const uint8_t* >(v->data)[j]);
        case DRAKEN_UINT16: return static_cast<uint64_t>(static_cast<const uint16_t*>(v->data)[j]);
        case DRAKEN_UINT32: return static_cast<uint64_t>(static_cast<const uint32_t*>(v->data)[j]);
        default:            return static_cast<const uint64_t*>(v->data)[j];  // DRAKEN_UINT64
    }
}

#define DRAKEN_CAST_UINT_TO_UINT(fn_name, UT, TAG, UMAX)                                \
VecResult fn_name(void* ctx, const DrakenVector* v) {                                    \
    DRAKEN_KERNEL_TRY({                                                                 \
        if (!v) return draken_error_sentinel("Input vector is null");                   \
        if (v->type != DRAKEN_UINT8 && v->type != DRAKEN_UINT16 &&                      \
            v->type != DRAKEN_UINT32 && v->type != DRAKEN_UINT64)                       \
            return draken_error_sentinel_fmt(                                           \
                #fn_name ": expected an unsigned integer source, got %d", v->type);      \
        const uint32_t k = v->data_length;                                              \
        UT* out = static_cast<UT*>(draken_malloc((k > 0u ? k : 1u) * sizeof(UT)));       \
        if (!out) return draken_error_sentinel("Allocation failed");                    \
        const bool is_safe = kernel_cast_is_safe(ctx);                                  \
        std::vector<uint8_t> bad(k > 0u ? k : 1u, 0u);                                  \
        bool any_bad = false;                                                           \
        const bool has_nulls = (v->validity != nullptr);                                \
        for (uint32_t j = 0u; j < k; ++j) {                                             \
            if (has_nulls && !((v->validity[j >> 3] >> (j & 7u)) & 1u)) {                \
                out[j] = 0; continue;                                                    \
            }                                                                            \
            const uint64_t uv = cast_read_unsigned_u64(v, j);                            \
            if (uv > (UMAX)) {                                                           \
                if (!is_safe) {                                                          \
                    draken_free(out);                                                        \
                    return draken_error_sentinel_fmt(                                       \
                        #fn_name ": value %llu out of range for " #UT,                       \
                        (unsigned long long)uv);                                             \
                }                                                                        \
                out[j] = 0; bad[j] = 1u; any_bad = true; continue;                       \
            }                                                                            \
            out[j] = static_cast<UT>(uv);                                                \
        }                                                                                \
        VecResult r;                                                                    \
        r.data = out; r.type = TAG; r.validity_embedded = 0u; r.ts_unit = 0xFFu;         \
        kernel_preserve_shape(r, v);                                                    \
        if (any_bad) kernel_null_bad_rows(r, v, bad.data());                             \
        return r;                                                                        \
    });                                                                                  \
}

DRAKEN_CAST_UINT_TO_UINT(draken_cast_uint_to_uint8,  uint8_t,  DRAKEN_UINT8,  255ull)
DRAKEN_CAST_UINT_TO_UINT(draken_cast_uint_to_uint16, uint16_t, DRAKEN_UINT16, 65535ull)
DRAKEN_CAST_UINT_TO_UINT(draken_cast_uint_to_uint32, uint32_t, DRAKEN_UINT32, 4294967295ull)
DRAKEN_CAST_UINT_TO_UINT(draken_cast_uint_to_uint64, uint64_t, DRAKEN_UINT64, 0xFFFFFFFFFFFFFFFFull)

#undef DRAKEN_CAST_UINT_TO_UINT

// --- unsigned integer -> FLOAT64 ------------------------------------------------
//
// The hole this fills: an unsigned column could reach floating point only via
// INT64, which RAISES above 2^63-1 — so the top half of the UINT64 range had no
// route into float arithmetic at all.
//
// No range check: every uint64 is representable as a double. Values above 2^53
// lose low-order bits, which is what binary floating point IS, not an error —
// same contract as draken_cast_int64_to_float64 at the other end of the range.
VecResult draken_cast_uint_to_float64(void* ctx, const DrakenVector* v) {
    DRAKEN_KERNEL_TRY({
        if (!v) return draken_error_sentinel("Input vector is null");
        if (v->type != DRAKEN_UINT8 && v->type != DRAKEN_UINT16 &&
            v->type != DRAKEN_UINT32 && v->type != DRAKEN_UINT64)
            return draken_error_sentinel_fmt(
                "draken_cast_uint_to_float64: expected an unsigned integer source, got %d",
                v->type);
        const uint32_t k = v->data_length;
        double* out = static_cast<double*>(draken_malloc((k > 0u ? k : 1u) * sizeof(double)));
        if (!out) return draken_error_sentinel("Allocation failed");
        for (uint32_t j = 0u; j < k; ++j)
            out[j] = static_cast<double>(cast_read_unsigned_u64(v, j));
        VecResult r;
        r.data = out; r.type = DRAKEN_FLOAT64; r.validity_embedded = 0u; r.ts_unit = 0xFFu;
        kernel_preserve_shape(r, v);
        return r;
    });
}

// --- DECIMAL -> unsigned integer ------------------------------------------------
//
// The unsigned twin of decimal_to_int64_core: divide out the source scale
// (binary_op_ctx.left_scale — the vector does not carry it), TRUNCATING TOWARD
// ZERO exactly as the INTEGER target does, then range-check into the target
// width. A negative decimal is not an unsigned value and raises; so does a
// magnitude above the target width. Never wraps.
//
// DENSE output (uniform data[selection[j]] gather), matching the decimal kernels
// above.

#define DRAKEN_DEC_TO_UINT_LOOP(SRC_T, UT)                                             \
    for (uint32_t j = 0u; j < n; ++j) {                                                \
        if (val_in && ((val_in[j >> 3u] >> (j & 7u)) & 1u) == 0u) { out[j] = 0; continue; } \
        const __int128 x = static_cast<__int128>(                                      \
            static_cast<const SRC_T*>(v->data)[v->selection[j]]) / factor;             \
        if (x < 0 || x > static_cast<__int128>(umax)) {                                \
            if (!is_safe) {                                                            \
                draken_free(out);                                                      \
                return draken_error_sentinel_fmt(                                      \
                    "cast decimal->%s: value out of range", width_name);               \
            }                                                                          \
            out[j] = 0; bad[j] = 1u; any_bad = true; continue;                         \
        }                                                                              \
        out[j] = static_cast<UT>(x);                                                   \
    }

#define DRAKEN_CAST_DEC_TO_UINT(width_suffix, UT, TAG, UMAX, WIDTH_NAME)               \
static VecResult dec_to_##width_suffix##_core(void* ctx, const DrakenVector* v,        \
                                              bool src128) {                            \
    if (!v) return draken_error_sentinel("Input vector is null");                      \
    const DrakenType want = src128 ? DRAKEN_DECIMAL128 : DRAKEN_DECIMAL;               \
    const char* const width_name = WIDTH_NAME;                                         \
    if (v->type != want)                                                                \
        return draken_error_sentinel_fmt("cast decimal->%s: expected %d, got %d",       \
                                         width_name, (int)want, (int)v->type);          \
    if (!ctx)                                                                           \
        return draken_error_sentinel_fmt("cast decimal->%s: missing ctx (source scale)",\
                                         width_name);                                   \
    const auto* c = static_cast<const binary_op_ctx*>(ctx);                             \
    if (c->left_scale > 38u)                                                            \
        return draken_error_sentinel_fmt("cast decimal->%s: bad source scale %d",       \
                                         width_name, (int)c->left_scale);               \
    const __int128 factor = dec_pow10(static_cast<int>(c->left_scale));                 \
    const uint64_t umax = (UMAX);                                                       \
    const uint32_t n = v->length;                                                       \
    UT* out = static_cast<UT*>(draken_malloc((n > 0u ? n : 1u) * sizeof(UT)));           \
    if (!out) return draken_error_sentinel("Allocation failed");                        \
    const uint8_t* val_in = v->validity;                                                \
    const bool is_safe = kernel_cast_is_safe(ctx);                                      \
    std::vector<uint8_t> bad(n > 0u ? n : 1u, 0u);                                      \
    bool any_bad = false;                                                               \
    if (src128) { DRAKEN_DEC_TO_UINT_LOOP(__int128, UT) }                               \
    else        { DRAKEN_DEC_TO_UINT_LOOP(int64_t, UT) }                                \
    VecResult r;                                                                        \
    r.data = out;                                                                       \
    r.type = TAG;                                                                       \
    r.validity_embedded = 0u;                                                           \
    r.ts_unit = 0xFFu;                                                                  \
    r.length = n; r.data_length = n;                                                    \
    r.selection = draken_identity_sel(n);                                               \
    r.owns_selection = false;                                                           \
    r.validity = kernel_copy_validity(v);                                               \
    r.flags = DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION;                             \
    if (any_bad) kernel_null_bad_rows(r, v, bad.data());                                \
    return r;                                                                           \
}                                                                                       \
VecResult draken_cast_decimal_to_##width_suffix(void* ctx, const DrakenVector* v) {     \
    DRAKEN_KERNEL_TRY({ return dec_to_##width_suffix##_core(ctx, v, false); });          \
}                                                                                       \
VecResult draken_cast_decimal128_to_##width_suffix(void* ctx, const DrakenVector* v) {  \
    DRAKEN_KERNEL_TRY({ return dec_to_##width_suffix##_core(ctx, v, true); });           \
}

DRAKEN_CAST_DEC_TO_UINT(uint8,  uint8_t,  DRAKEN_UINT8,  255ull, "uint8")
DRAKEN_CAST_DEC_TO_UINT(uint16, uint16_t, DRAKEN_UINT16, 65535ull, "uint16")
DRAKEN_CAST_DEC_TO_UINT(uint32, uint32_t, DRAKEN_UINT32, 4294967295ull, "uint32")
DRAKEN_CAST_DEC_TO_UINT(uint64, uint64_t, DRAKEN_UINT64, 0xFFFFFFFFFFFFFFFFull, "uint64")

#undef DRAKEN_CAST_DEC_TO_UINT
#undef DRAKEN_DEC_TO_UINT_LOOP

// ================================================================================
// NARROW SIGNED (INT8/INT16/INT32) and FLOAT32 TARGETS
// ================================================================================
//
// These widths existed only as cast SOURCES: a Parquet file or a catalog schema
// could hand the engine an INT8 column, but no SQL could ask for one, because
// the target arm mapped the narrow names onto INT64-PRODUCING kernels — so
// accepting `CAST(x AS INT32)` without these would have declared INT32 and
// produced INT64, the declared-vs-actual divergence that is worse than the
// refusal it replaces.
//
// The source families mirror the unsigned family exactly — signed int, unsigned
// int, float, bool, string (in cast_string.cpp), decimal. Temporal sources are
// deliberately NOT here: the unsigned targets do not take them either, and a
// timestamp that needs an integer goes through INTEGER, which does.
//
// Every narrowing is RANGE-CHECKED and raises; nothing wraps, nothing saturates.

// --- signed integer -> narrow signed integer ------------------------------------
#define DRAKEN_CAST_SIGNED_TO_INT(fn_name, IT, TAG, IMIN, IMAX)                         \
VecResult fn_name(void* ctx, const DrakenVector* v) {                                    \
    DRAKEN_KERNEL_TRY({                                                                 \
        if (!v) return draken_error_sentinel("Input vector is null");                   \
        if (v->type != DRAKEN_INT8 && v->type != DRAKEN_INT16 &&                        \
            v->type != DRAKEN_INT32 && v->type != DRAKEN_INT64)                         \
            return draken_error_sentinel_fmt(                                           \
                #fn_name ": expected a signed integer source, got %d", v->type);         \
        const uint32_t k = v->data_length;                                              \
        IT* out = static_cast<IT*>(draken_malloc((k > 0u ? k : 1u) * sizeof(IT)));       \
        if (!out) return draken_error_sentinel("Allocation failed");                    \
        const bool is_safe = kernel_cast_is_safe(ctx);                                  \
        std::vector<uint8_t> bad(k > 0u ? k : 1u, 0u);                                  \
        bool any_bad = false;                                                           \
        const bool has_nulls = (v->validity != nullptr);                                \
        for (uint32_t j = 0u; j < k; ++j) {                                             \
            if (has_nulls && !((v->validity[j >> 3] >> (j & 7u)) & 1u)) {                \
                out[j] = 0; continue;                                                    \
            }                                                                            \
            const int64_t val = cast_read_signed_i64(v, j);                              \
            if (val < (IMIN) || val > (IMAX)) {                                          \
                if (!is_safe) {                                                          \
                    draken_free(out);                                                        \
                    return draken_error_sentinel_fmt(                                       \
                        #fn_name ": value %lld out of range for " #IT, (long long)val);      \
                }                                                                        \
                out[j] = 0; bad[j] = 1u; any_bad = true; continue;                       \
            }                                                                            \
            out[j] = static_cast<IT>(val);                                               \
        }                                                                                \
        VecResult r;                                                                    \
        r.data = out; r.type = TAG; r.validity_embedded = 0u; r.ts_unit = 0xFFu;         \
        kernel_preserve_shape(r, v);                                                    \
        if (any_bad) kernel_null_bad_rows(r, v, bad.data());                             \
        return r;                                                                        \
    });                                                                                  \
}

DRAKEN_CAST_SIGNED_TO_INT(draken_cast_integer_to_int8,  int8_t,  DRAKEN_INT8,  -128LL, 127LL)
DRAKEN_CAST_SIGNED_TO_INT(draken_cast_integer_to_int16, int16_t, DRAKEN_INT16, -32768LL, 32767LL)
DRAKEN_CAST_SIGNED_TO_INT(draken_cast_integer_to_int32, int32_t, DRAKEN_INT32, -2147483648LL, 2147483647LL)

// <signed integer> -> DATE32. A DATE32 *is* an int32 days-since-epoch, so this is
// the int32 narrowing above with the temporal tag — the integer is taken to
// already hold days-since-epoch, exactly the reading the planner's
// CAST(<int> AS DATE) predicate rewrite asserts (_try_normalize_cast_predicate in
// predicate_pushdown.py). The two must agree: if this kernel meant anything else,
// a pushed-down and a non-pushed-down `col::DATE >= <date>` would answer
// differently. Instantiated here, next to the int32 narrowing whose range check
// it reuses verbatim, rather than re-typed by hand in cast_temporal.cpp.
DRAKEN_CAST_SIGNED_TO_INT(draken_cast_integer_to_date32, int32_t, DRAKEN_DATE32, -2147483648LL, 2147483647LL)

#undef DRAKEN_CAST_SIGNED_TO_INT

// --- unsigned integer -> narrow signed integer ----------------------------------
#define DRAKEN_CAST_UINT_TO_INT(fn_name, IT, TAG, IMAX)                                 \
VecResult fn_name(void* ctx, const DrakenVector* v) {                                    \
    DRAKEN_KERNEL_TRY({                                                                 \
        if (!v) return draken_error_sentinel("Input vector is null");                   \
        if (v->type != DRAKEN_UINT8 && v->type != DRAKEN_UINT16 &&                      \
            v->type != DRAKEN_UINT32 && v->type != DRAKEN_UINT64)                       \
            return draken_error_sentinel_fmt(                                           \
                #fn_name ": expected an unsigned integer source, got %d", v->type);      \
        const uint32_t k = v->data_length;                                              \
        IT* out = static_cast<IT*>(draken_malloc((k > 0u ? k : 1u) * sizeof(IT)));       \
        if (!out) return draken_error_sentinel("Allocation failed");                    \
        const bool is_safe = kernel_cast_is_safe(ctx);                                  \
        std::vector<uint8_t> bad(k > 0u ? k : 1u, 0u);                                  \
        bool any_bad = false;                                                           \
        const bool has_nulls = (v->validity != nullptr);                                \
        for (uint32_t j = 0u; j < k; ++j) {                                             \
            if (has_nulls && !((v->validity[j >> 3] >> (j & 7u)) & 1u)) {                \
                out[j] = 0; continue;                                                    \
            }                                                                            \
            const uint64_t uv = cast_read_unsigned_u64(v, j);                            \
            if (uv > static_cast<uint64_t>(IMAX)) {                                      \
                if (!is_safe) {                                                          \
                    draken_free(out);                                                        \
                    return draken_error_sentinel_fmt(                                       \
                        #fn_name ": value %llu out of range for " #IT,                       \
                        (unsigned long long)uv);                                             \
                }                                                                        \
                out[j] = 0; bad[j] = 1u; any_bad = true; continue;                       \
            }                                                                            \
            out[j] = static_cast<IT>(uv);                                                \
        }                                                                                \
        VecResult r;                                                                    \
        r.data = out; r.type = TAG; r.validity_embedded = 0u; r.ts_unit = 0xFFu;         \
        kernel_preserve_shape(r, v);                                                    \
        if (any_bad) kernel_null_bad_rows(r, v, bad.data());                             \
        return r;                                                                        \
    });                                                                                  \
}

DRAKEN_CAST_UINT_TO_INT(draken_cast_uint_to_int8,  int8_t,  DRAKEN_INT8,  127LL)
DRAKEN_CAST_UINT_TO_INT(draken_cast_uint_to_int16, int16_t, DRAKEN_INT16, 32767LL)
DRAKEN_CAST_UINT_TO_INT(draken_cast_uint_to_int32, int32_t, DRAKEN_INT32, 2147483647LL)

// <unsigned integer> -> DATE32. The unsigned twin of draken_cast_integer_to_date32
// above; same days-since-epoch reading, same range check. This is the pair a
// UINT16 ClickBench EventDate arrives on.
DRAKEN_CAST_UINT_TO_INT(draken_cast_uint_to_date32, int32_t, DRAKEN_DATE32, 2147483647LL)

#undef DRAKEN_CAST_UINT_TO_INT

// --- float -> narrow signed integer ---------------------------------------------
// Truncates toward zero (draken_cast_float64_to_int64's convention), then range
// checks. The `!(d >= IMIN)` form is deliberate: it also rejects NaN, which
// compares false against everything and would otherwise become an arbitrary int.
#define DRAKEN_CAST_FLOAT_TO_INT(fn_name, IT, TAG, IMIN, IMAX)                          \
VecResult fn_name(void* ctx, const DrakenVector* v) {                                    \
    DRAKEN_KERNEL_TRY({                                                                 \
        if (!v) return draken_error_sentinel("Input vector is null");                   \
        if (v->type != DRAKEN_FLOAT64 && v->type != DRAKEN_FLOAT32)                     \
            return draken_error_sentinel_fmt(                                           \
                #fn_name ": expected FLOAT, got %d", v->type);                           \
        const uint32_t k = v->data_length;                                              \
        const bool is64 = (v->type == DRAKEN_FLOAT64);                                  \
        IT* out = static_cast<IT*>(draken_malloc((k > 0u ? k : 1u) * sizeof(IT)));       \
        if (!out) return draken_error_sentinel("Allocation failed");                    \
        const bool is_safe = kernel_cast_is_safe(ctx);                                  \
        std::vector<uint8_t> bad(k > 0u ? k : 1u, 0u);                                  \
        bool any_bad = false;                                                           \
        for (uint32_t j = 0u; j < k; ++j) {                                             \
            const double d = is64 ? static_cast<const double*>(v->data)[j]              \
                                  : static_cast<double>(static_cast<const float*>(v->data)[j]); \
            if (!(d >= static_cast<double>(IMIN)) || d > static_cast<double>(IMAX)) {    \
                if (!is_safe) {                                                          \
                    draken_free(out);                                                    \
                    return draken_error_sentinel_fmt(                                    \
                        #fn_name ": value %g out of range for " #IT, d);                  \
                }                                                                        \
                out[j] = 0; bad[j] = 1u; any_bad = true; continue;                       \
            }                                                                            \
            out[j] = static_cast<IT>(d);                                                \
        }                                                                                \
        VecResult r;                                                                    \
        r.data = out; r.type = TAG; r.validity_embedded = 0u; r.ts_unit = 0xFFu;         \
        kernel_preserve_shape(r, v);                                                    \
        if (any_bad) kernel_null_bad_rows(r, v, bad.data());                             \
        return r;                                                                        \
    });                                                                                  \
}

DRAKEN_CAST_FLOAT_TO_INT(draken_cast_float_to_int8,  int8_t,  DRAKEN_INT8,  -128LL, 127LL)
DRAKEN_CAST_FLOAT_TO_INT(draken_cast_float_to_int16, int16_t, DRAKEN_INT16, -32768LL, 32767LL)
DRAKEN_CAST_FLOAT_TO_INT(draken_cast_float_to_int32, int32_t, DRAKEN_INT32, -2147483648LL, 2147483647LL)

#undef DRAKEN_CAST_FLOAT_TO_INT

// --- BOOL -> narrow signed integer ----------------------------------------------
// Always 0 or 1 — no width can fail (mirrors the BOOL → unsigned family).
#define DRAKEN_CAST_BOOL_TO_INT(fn_name, IT, TAG)                                       \
VecResult fn_name(void* ctx, const DrakenVector* v) {                                    \
    DRAKEN_KERNEL_TRY({                                                                 \
        if (!v) return draken_error_sentinel("Input vector is null");                   \
        if (v->type != DRAKEN_BOOL)                                                     \
            return draken_error_sentinel_fmt(#fn_name ": expected BOOL, got %d", v->type); \
        const uint32_t k = v->data_length;                                              \
        const uint8_t* src = static_cast<const uint8_t*>(v->data);                       \
        IT* out = static_cast<IT*>(draken_malloc((k > 0u ? k : 1u) * sizeof(IT)));       \
        if (!out) return draken_error_sentinel("Allocation failed");                    \
        for (uint32_t j = 0u; j < k; ++j)                                               \
            out[j] = static_cast<IT>((src[j >> 3u] >> (j & 7u)) & 1u);                   \
        VecResult r;                                                                    \
        r.data = out; r.type = TAG; r.validity_embedded = 0u; r.ts_unit = 0xFFu;         \
        kernel_preserve_shape(r, v);                                                    \
        return r;                                                                        \
    });                                                                                  \
}

DRAKEN_CAST_BOOL_TO_INT(draken_cast_bool_to_int8,  int8_t,  DRAKEN_INT8)
DRAKEN_CAST_BOOL_TO_INT(draken_cast_bool_to_int16, int16_t, DRAKEN_INT16)
DRAKEN_CAST_BOOL_TO_INT(draken_cast_bool_to_int32, int32_t, DRAKEN_INT32)

#undef DRAKEN_CAST_BOOL_TO_INT

// --- DECIMAL -> narrow signed integer -------------------------------------------
// The signed twin of the DECIMAL → unsigned family: divide out the source scale
// (binary_op_ctx.left_scale), truncating toward zero, then range check.
#define DRAKEN_DEC_TO_INT_LOOP(SRC_T, IT)                                              \
    for (uint32_t j = 0u; j < n; ++j) {                                                \
        if (val_in && ((val_in[j >> 3u] >> (j & 7u)) & 1u) == 0u) { out[j] = 0; continue; } \
        const __int128 x = static_cast<__int128>(                                      \
            static_cast<const SRC_T*>(v->data)[v->selection[j]]) / factor;             \
        if (x < static_cast<__int128>(imin) || x > static_cast<__int128>(imax)) {       \
            if (!is_safe) {                                                            \
                draken_free(out);                                                      \
                return draken_error_sentinel_fmt(                                      \
                    "cast decimal->%s: value out of range", width_name);               \
            }                                                                          \
            out[j] = 0; bad[j] = 1u; any_bad = true; continue;                         \
        }                                                                              \
        out[j] = static_cast<IT>(x);                                                   \
    }

#define DRAKEN_CAST_DEC_TO_INT(width_suffix, IT, TAG, IMIN, IMAX, WIDTH_NAME)          \
static VecResult dec_to_##width_suffix##_core(void* ctx, const DrakenVector* v,        \
                                              bool src128) {                            \
    if (!v) return draken_error_sentinel("Input vector is null");                      \
    const DrakenType want = src128 ? DRAKEN_DECIMAL128 : DRAKEN_DECIMAL;               \
    const char* const width_name = WIDTH_NAME;                                         \
    if (v->type != want)                                                                \
        return draken_error_sentinel_fmt("cast decimal->%s: expected %d, got %d",       \
                                         width_name, (int)want, (int)v->type);          \
    if (!ctx)                                                                           \
        return draken_error_sentinel_fmt("cast decimal->%s: missing ctx (source scale)",\
                                         width_name);                                   \
    const auto* c = static_cast<const binary_op_ctx*>(ctx);                             \
    if (c->left_scale > 38u)                                                            \
        return draken_error_sentinel_fmt("cast decimal->%s: bad source scale %d",       \
                                         width_name, (int)c->left_scale);               \
    const __int128 factor = dec_pow10(static_cast<int>(c->left_scale));                 \
    const int64_t imin = (IMIN);                                                        \
    const int64_t imax = (IMAX);                                                        \
    const uint32_t n = v->length;                                                       \
    IT* out = static_cast<IT*>(draken_malloc((n > 0u ? n : 1u) * sizeof(IT)));           \
    if (!out) return draken_error_sentinel("Allocation failed");                        \
    const uint8_t* val_in = v->validity;                                                \
    const bool is_safe = kernel_cast_is_safe(ctx);                                      \
    std::vector<uint8_t> bad(n > 0u ? n : 1u, 0u);                                      \
    bool any_bad = false;                                                               \
    if (src128) { DRAKEN_DEC_TO_INT_LOOP(__int128, IT) }                                \
    else        { DRAKEN_DEC_TO_INT_LOOP(int64_t, IT) }                                 \
    VecResult r;                                                                        \
    r.data = out;                                                                       \
    r.type = TAG;                                                                       \
    r.validity_embedded = 0u;                                                           \
    r.ts_unit = 0xFFu;                                                                  \
    r.length = n; r.data_length = n;                                                    \
    r.selection = draken_identity_sel(n);                                               \
    r.owns_selection = false;                                                           \
    r.validity = kernel_copy_validity(v);                                               \
    r.flags = DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION;                             \
    if (any_bad) kernel_null_bad_rows(r, v, bad.data());                                \
    return r;                                                                           \
}                                                                                       \
VecResult draken_cast_decimal_to_##width_suffix(void* ctx, const DrakenVector* v) {     \
    DRAKEN_KERNEL_TRY({ return dec_to_##width_suffix##_core(ctx, v, false); });          \
}                                                                                       \
VecResult draken_cast_decimal128_to_##width_suffix(void* ctx, const DrakenVector* v) {  \
    DRAKEN_KERNEL_TRY({ return dec_to_##width_suffix##_core(ctx, v, true); });           \
}

DRAKEN_CAST_DEC_TO_INT(int8,  int8_t,  DRAKEN_INT8,  -128LL, 127LL, "int8")
DRAKEN_CAST_DEC_TO_INT(int16, int16_t, DRAKEN_INT16, -32768LL, 32767LL, "int16")
DRAKEN_CAST_DEC_TO_INT(int32, int32_t, DRAKEN_INT32, -2147483648LL, 2147483647LL, "int32")

#undef DRAKEN_CAST_DEC_TO_INT
#undef DRAKEN_DEC_TO_INT_LOOP

// --- everything -> FLOAT32 -------------------------------------------------------
//
// Precision loss is NOT an error here — a float32 has ~7 significant digits and
// the caller asked for one; that is the contract of the type, exactly as
// int64→float64 silently loses bits above 2^53. What IS an error is a finite
// value whose MAGNITUDE has no float32 representation at all: silently becoming
// ±Inf would turn a number into a non-number. An input that is ALREADY ±Inf or
// NaN passes through unchanged — it arrived that way.
static inline bool f32_representable(double d) noexcept {
    // 3.4028235e38 is FLT_MAX; the compare is skipped for non-finite inputs.
    return !std::isfinite(d) || (d <= 3.4028234663852886e38 && d >= -3.4028234663852886e38);
}

#define DRAKEN_CAST_INT_TO_F32_BODY(READ_FN, TYPE_GUARD, WHO)                           \
        if (!v) return draken_error_sentinel("Input vector is null");                   \
        if (!(TYPE_GUARD))                                                              \
            return draken_error_sentinel_fmt(WHO ": unexpected source type %d", v->type); \
        const uint32_t k = v->data_length;                                              \
        float* out = static_cast<float*>(draken_malloc((k > 0u ? k : 1u) * sizeof(float))); \
        if (!out) return draken_error_sentinel("Allocation failed");                    \
        for (uint32_t j = 0u; j < k; ++j)                                               \
            out[j] = static_cast<float>(READ_FN(v, j));                                 \
        VecResult r;                                                                    \
        r.data = out; r.type = DRAKEN_FLOAT32; r.validity_embedded = 0u; r.ts_unit = 0xFFu; \
        kernel_preserve_shape(r, v);                                                    \
        return r;

VecResult draken_cast_integer_to_float32(void* ctx, const DrakenVector* v) {
    DRAKEN_KERNEL_TRY({
        DRAKEN_CAST_INT_TO_F32_BODY(
            cast_read_signed_i64,
            v->type == DRAKEN_INT8 || v->type == DRAKEN_INT16 ||
                v->type == DRAKEN_INT32 || v->type == DRAKEN_INT64,
            "cast integer->float32")
    });
}

VecResult draken_cast_uint_to_float32(void* ctx, const DrakenVector* v) {
    DRAKEN_KERNEL_TRY({
        DRAKEN_CAST_INT_TO_F32_BODY(
            cast_read_unsigned_u64,
            v->type == DRAKEN_UINT8 || v->type == DRAKEN_UINT16 ||
                v->type == DRAKEN_UINT32 || v->type == DRAKEN_UINT64,
            "cast uint->float32")
    });
}

#undef DRAKEN_CAST_INT_TO_F32_BODY

VecResult draken_cast_bool_to_float32(void* ctx, const DrakenVector* v) {
    DRAKEN_KERNEL_TRY({
        if (!v) return draken_error_sentinel("Input vector is null");
        if (v->type != DRAKEN_BOOL)
            return draken_error_sentinel_fmt("cast bool->float32: expected BOOL, got %d",
                                             v->type);
        const uint32_t k = v->data_length;
        const uint8_t* src = static_cast<const uint8_t*>(v->data);
        float* out = static_cast<float*>(draken_malloc((k > 0u ? k : 1u) * sizeof(float)));
        if (!out) return draken_error_sentinel("Allocation failed");
        for (uint32_t j = 0u; j < k; ++j)
            out[j] = static_cast<float>((src[j >> 3u] >> (j & 7u)) & 1u);
        VecResult r;
        r.data = out; r.type = DRAKEN_FLOAT32; r.validity_embedded = 0u; r.ts_unit = 0xFFu;
        kernel_preserve_shape(r, v);
        return r;
    });
}

VecResult draken_cast_float_to_float32(void* ctx, const DrakenVector* v) {
    DRAKEN_KERNEL_TRY({
        if (!v) return draken_error_sentinel("Input vector is null");
        if (v->type != DRAKEN_FLOAT64 && v->type != DRAKEN_FLOAT32)
            return draken_error_sentinel_fmt("cast float->float32: expected FLOAT, got %d",
                                             v->type);
        const uint32_t k = v->data_length;
        const bool is64 = (v->type == DRAKEN_FLOAT64);
        float* out = static_cast<float*>(draken_malloc((k > 0u ? k : 1u) * sizeof(float)));
        if (!out) return draken_error_sentinel("Allocation failed");
        const bool is_safe = kernel_cast_is_safe(ctx);
        std::vector<uint8_t> bad(k > 0u ? k : 1u, 0u);
        bool any_bad = false;
        for (uint32_t j = 0u; j < k; ++j) {
            const double d = is64 ? static_cast<const double*>(v->data)[j]
                                  : static_cast<double>(static_cast<const float*>(v->data)[j]);
            if (!f32_representable(d)) {
                if (!is_safe) {
                    draken_free(out);
                    return draken_error_sentinel_fmt(
                        "cast float->float32: value %g out of range for FLOAT32", d);
                }
                out[j] = 0.0f; bad[j] = 1u; any_bad = true; continue;
            }
            out[j] = static_cast<float>(d);
        }
        VecResult r;
        r.data = out; r.type = DRAKEN_FLOAT32; r.validity_embedded = 0u; r.ts_unit = 0xFFu;
        kernel_preserve_shape(r, v);
        if (any_bad) kernel_null_bad_rows(r, v, bad.data());
        return r;
    });
}

// FLOAT32 -> FLOAT64 (and the FLOAT64 self-copy). A widening that cannot fail —
// every float is a double. It needs a KERNEL, not a retag: the two types have
// different payload widths, so "same bits, new tag" would leave a 4-byte-per-row
// buffer being read at an 8-byte stride. The bind-time tables treated this pair
// as an identity passthrough, which is why it was refused at the compiler gate
// rather than producing garbage — the refusal was hiding a wrong answer.
VecResult draken_cast_float_to_float64(void* ctx, const DrakenVector* v) {
    DRAKEN_KERNEL_TRY({
        if (!v) return draken_error_sentinel("Input vector is null");
        if (v->type != DRAKEN_FLOAT64 && v->type != DRAKEN_FLOAT32)
            return draken_error_sentinel_fmt("cast float->float64: expected FLOAT, got %d",
                                             v->type);
        const uint32_t k = v->data_length;
        const bool is64 = (v->type == DRAKEN_FLOAT64);
        double* out = static_cast<double*>(draken_malloc((k > 0u ? k : 1u) * sizeof(double)));
        if (!out) return draken_error_sentinel("Allocation failed");
        for (uint32_t j = 0u; j < k; ++j)
            out[j] = is64 ? static_cast<const double*>(v->data)[j]
                          : static_cast<double>(static_cast<const float*>(v->data)[j]);
        VecResult r;
        r.data = out; r.type = DRAKEN_FLOAT64; r.validity_embedded = 0u; r.ts_unit = 0xFFu;
        kernel_preserve_shape(r, v);
        return r;
    });
}

// DECIMAL → FLOAT32: same shape as the FLOAT64 twin (divide out the source
// scale), with the representability guard the narrower type needs.
#define DRAKEN_DEC_TO_F32_LOOP(SRC_T)                                                  \
    for (uint32_t j = 0u; j < n; ++j) {                                                \
        if (val_in && ((val_in[j >> 3u] >> (j & 7u)) & 1u) == 0u) { out[j] = 0.0f; continue; } \
        const double d = static_cast<double>(                                          \
            static_cast<const SRC_T*>(v->data)[v->selection[j]]) / divisor;            \
        if (!f32_representable(d)) {                                                   \
            if (!is_safe) {                                                            \
                draken_free(out);                                                      \
                return draken_error_sentinel_fmt(                                      \
                    "cast decimal->float32: value %g out of range for FLOAT32", d);    \
            }                                                                          \
            out[j] = 0.0f; bad[j] = 1u; any_bad = true; continue;                      \
        }                                                                              \
        out[j] = static_cast<float>(d);                                                \
    }

static VecResult decimal_to_float32_core(void* ctx, const DrakenVector* v, bool src128) {
    if (!v) return draken_error_sentinel("Input vector is null");
    const DrakenType want = src128 ? DRAKEN_DECIMAL128 : DRAKEN_DECIMAL;
    if (v->type != want)
        return draken_error_sentinel_fmt("cast decimal->float32: expected %d, got %d",
                                         (int)want, (int)v->type);
    if (!ctx) return draken_error_sentinel("cast decimal->float32: missing ctx (source scale)");
    const auto* c = static_cast<const binary_op_ctx*>(ctx);
    if (c->left_scale > 38u)
        return draken_error_sentinel_fmt("cast decimal->float32: bad source scale %d",
                                         (int)c->left_scale);

    const double divisor = static_cast<double>(dec_pow10(static_cast<int>(c->left_scale)));
    const uint32_t n = v->length;
    float* out = static_cast<float*>(draken_malloc((n > 0u ? n : 1u) * sizeof(float)));
    if (!out) return draken_error_sentinel("Allocation failed");
    const uint8_t* val_in = v->validity;
    const bool is_safe = kernel_cast_is_safe(ctx);
    std::vector<uint8_t> bad(n > 0u ? n : 1u, 0u);
    bool any_bad = false;

    if (src128) { DRAKEN_DEC_TO_F32_LOOP(__int128) }
    else        { DRAKEN_DEC_TO_F32_LOOP(int64_t) }

    VecResult r;
    r.data = out;
    r.type = DRAKEN_FLOAT32;
    r.validity_embedded = 0u;
    r.ts_unit = 0xFFu;
    r.length = n; r.data_length = n;
    r.selection = draken_identity_sel(n);
    r.owns_selection = false;
    r.validity = kernel_copy_validity(v);
    r.flags = DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION;
    if (any_bad) kernel_null_bad_rows(r, v, bad.data());
    return r;
}

#undef DRAKEN_DEC_TO_F32_LOOP

VecResult draken_cast_decimal_to_float32(void* ctx, const DrakenVector* v) {
    DRAKEN_KERNEL_TRY({ return decimal_to_float32_core(ctx, v, /*src128=*/false); });
}

VecResult draken_cast_decimal128_to_float32(void* ctx, const DrakenVector* v) {
    DRAKEN_KERNEL_TRY({ return decimal_to_float32_core(ctx, v, /*src128=*/true); });
}

}  // extern "C"

// ---- ARRAY → VARCHAR ---------------------------------------------------------------
// Renders each list row as a bracketed literal: ['Apollo 11', 'Apollo 12'].
// String elements are single-quoted with backslash escaping of ' and \; NULL
// elements render as null; a NULL row is a NULL output (validity preserved).
//
// TWO-vector signature: an ARRAY DrakenVector's data is the int32 offsets
// buffer (offsets[j] .. offsets[j+1] bound physical row j's element range);
// the ELEMENTS live in the child vector, which is owned by the VectorOwner —
// not reachable from the parent DrakenVector*. The VM resolves the child at
// morsel time (BC_C_NATIVE_CHILD) and passes both explicitly. This is NOT a
// cast_fn_t and is never dispatched through the one-vector cast table.

static void array_elem_append(std::vector<char>& out, const DrakenVector* child,
                              uint32_t e, char* tmp) {
    // NULL element
    if (child->validity != nullptr
            && ((child->validity[e >> 3] >> (e & 7)) & 1u) == 0u) {
        const char knull[] = {'n', 'u', 'l', 'l'};
        out.insert(out.end(), knull, knull + 4);
        return;
    }
    const uint32_t phys = child->selection[e];
    switch (child->type) {
        case DRAKEN_VARCHAR:
        case DRAKEN_NVARCHAR:
        case DRAKEN_VARBINARY: {
            const DrakenStringArena* sa =
                static_cast<const DrakenStringArena*>(child->data);
            const DrakenStringSlot* slot = &sa->slots[phys];
            const uint8_t* p = str_data(slot, sa->arena);
            const uint32_t len = str_length(slot);
            out.push_back('\'');
            for (uint32_t b = 0; b < len; ++b) {
                const char c = static_cast<char>(p[b]);
                if (c == '\'' || c == '\\') out.push_back('\\');
                out.push_back(c);
            }
            out.push_back('\'');
            return;
        }
        case DRAKEN_INT8: case DRAKEN_INT16: case DRAKEN_INT32: case DRAKEN_INT64: {
            const int64_t val = load_int_signed(child->data, phys, child->type);
            const int len = i64_to_ascii(val, tmp);
            out.insert(out.end(), tmp, tmp + len);
            return;
        }
        case DRAKEN_FLOAT64:
        case DRAKEN_FLOAT32: {
            const double d = (child->type == DRAKEN_FLOAT64)
                ? static_cast<const double*>(child->data)[phys]
                : static_cast<double>(static_cast<const float*>(child->data)[phys]);
            const size_t len = ryu_format_double(tmp, d, 6u);
            out.insert(out.end(), tmp, tmp + len);
            return;
        }
        case DRAKEN_BOOL: {
            const bool b =
                ((static_cast<const uint8_t*>(child->data)[phys >> 3]
                  >> (phys & 7)) & 1u) != 0u;
            if (b) out.insert(out.end(), kBoolTrue, kBoolTrue + 4);
            else out.insert(out.end(), kBoolFalse, kBoolFalse + 5);
            return;
        }
        default:
            throw std::runtime_error(
                "ARRAY->VARCHAR: unsupported element type — fail loud, never a "
                "silent wrong rendering");
    }
}

static VecResult array_to_varchar_core(const DrakenVector* parent,
                                       const DrakenVector* child) {
    if (!parent || !child)
        return draken_error_sentinel("ARRAY->VARCHAR: null input vector");
    if (parent->type != DRAKEN_ARRAY)
        return draken_error_sentinel_fmt(
            "ARRAY->VARCHAR: expected ARRAY parent, got %d", parent->type);
    const uint32_t k = parent->data_length;   // physical rows (offsets pairs)
    const int32_t* offsets = static_cast<const int32_t*>(parent->data);

    // Pass 1: render every physical row into one staging buffer.
    std::vector<char> stage;
    stage.reserve(static_cast<size_t>(k) * 24u);
    std::vector<uint32_t> rlen(k > 0u ? k : 1u, 0u);
    char tmp[40];
    size_t total_extern = 0u;
    for (uint32_t j = 0u; j < k; ++j) {
        const size_t before = stage.size();
        stage.push_back('[');
        const int32_t start = offsets[j];
        const int32_t end = offsets[j + 1];
        for (int32_t e = start; e < end; ++e) {
            if (e > start) {
                stage.push_back(',');
                stage.push_back(' ');
            }
            array_elem_append(stage, child, static_cast<uint32_t>(e), tmp);
        }
        stage.push_back(']');
        const size_t len = stage.size() - before;
        rlen[j] = static_cast<uint32_t>(len);
        if (len > STR_INLINE_MAX) total_extern += len;
    }

    // Pass 2: build the canonical consolidated string block.
    DrakenStringSlot* slots;
    uint8_t* arena;
    uint8_t* vunused;
    uint8_t* block = vecresult_string_block_alloc(k, total_extern, 0,
                                                  &slots, &arena, &vunused);
    if (!block) return draken_error_sentinel("Allocation failed");
    (void)vunused;
    const char* sbase = stage.data();
    size_t soff = 0u;
    size_t arena_used = 0u;
    for (uint32_t j = 0u; j < k; ++j) {
        const uint8_t* bytes = reinterpret_cast<const uint8_t*>(sbase + soff);
        const uint32_t len = rlen[j];
        soff += len;
        if (len > STR_INLINE_MAX) {
            std::memcpy(arena + arena_used, bytes, len);
            draken_build_string_slot(&slots[j], bytes, len,
                                     static_cast<uint32_t>(arena_used));
            arena_used += len;
        } else {
            draken_build_string_slot(&slots[j], bytes, len, 0u);
        }
    }
    // Non-ASCII child bytes (NVARCHAR) make the rendering UTF-8.
    const DrakenType out_t =
        (child->type == DRAKEN_NVARCHAR) ? DRAKEN_NVARCHAR : DRAKEN_VARCHAR;
    VecResult r = vecresult_from_string_block(block, k, total_extern, 0, out_t);
    kernel_preserve_shape(r, parent);
    return r;
}

VecResult draken_cast_array_to_varchar(void* ctx, const DrakenVector* parent,
                                       const DrakenVector* child) {
    (void)ctx;
    DRAKEN_KERNEL_TRY({ return array_to_varchar_core(parent, child); });
}
