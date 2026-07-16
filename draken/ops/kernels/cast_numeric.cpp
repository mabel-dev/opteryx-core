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

// Shared int64/uint64 → VARCHAR core. Compression-aware: format the data_length
// PHYSICAL values into a value block (1 for a constant, K for a dict, length for
// dense) and carry the input's selection + validity through — the output keeps the
// input's encoding (constant→constant string, dict→dict string). Two passes over
// the K values: size the arena, then fill. Int formatting never fails (no
// null-introduction), so validity is preserved 1:1.
static VecResult int_to_string_core(const DrakenVector* v, bool treat_as_unsigned) {
    if (!v) return draken_error_sentinel("Input vector is null");
    const DrakenType st = v->type;
    const bool is_int = (st == DRAKEN_INT8 || st == DRAKEN_INT16 ||
                         st == DRAKEN_INT32 || st == DRAKEN_INT64);
    if (!is_int)
        return draken_error_sentinel_fmt("cast-to-string: expected INT8/16/32/64, got %d", st);
    // Unsigned formatting is only meaningful for the full 64-bit width (the bit
    // pattern is reinterpreted). Narrow widths are always signed here.
    if (treat_as_unsigned && st != DRAKEN_INT64)
        return draken_error_sentinel_fmt("cast-to-string: unsigned requires INT64, got %d", st);

    const uint32_t k   = v->data_length;   // physical value count

    char tmp[21];
    size_t total_extern = 0u;
    for (uint32_t j = 0u; j < k; ++j) {
        const int64_t val = load_int_signed(v->data, j, st);
        int len = treat_as_unsigned ? u64_to_ascii(static_cast<uint64_t>(val), tmp)
                                     : i64_to_ascii(val, tmp);
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
        const int64_t val = load_int_signed(v->data, j, st);
        int len = treat_as_unsigned ? u64_to_ascii(static_cast<uint64_t>(val), tmp)
                                     : i64_to_ascii(val, tmp);
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
    DRAKEN_KERNEL_TRY({ return int_to_string_core(v, /*unsigned=*/false); });
}

// Narrow signed int (INT8/16/32) → VARCHAR. Single pass at the source's native
// stride — no widen-to-int64 detour; int_to_string_core reads the value at the
// correct width. (INT64 keeps its own entry point above.)
VecResult draken_cast_integer_to_string(void* ctx, const DrakenVector* v) {
    DRAKEN_KERNEL_TRY({ return int_to_string_core(v, /*unsigned=*/false); });
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
        const bool has_nulls = (v->validity != nullptr);                                \
        for (uint32_t j = 0u; j < k; ++j) {                                             \
            if (has_nulls && !((v->validity[j >> 3] >> (j & 7u)) & 1u)) {                \
                out[j] = 0; continue;                                                    \
            }                                                                            \
            const int64_t val = cast_read_signed_i64(v, j);                              \
            if (val < 0 || static_cast<uint64_t>(val) > (UMAX)) {                        \
                draken_free(out);                                                        \
                return draken_error_sentinel_fmt(                                       \
                    #fn_name ": value %lld out of range for " #UT, (long long)val);      \
            }                                                                            \
            out[j] = static_cast<UT>(val);                                              \
        }                                                                                \
        VecResult r;                                                                    \
        r.data = out; r.type = TAG; r.validity_embedded = 0u; r.ts_unit = 0xFFu;         \
        kernel_preserve_shape(r, v);                                                    \
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
                    draken_free(out);
                    return draken_error_sentinel_fmt(
                        "draken_cast_uint_to_int64: value %llu out of range for INT64",
                        (unsigned long long)uv);
                }
                out[j] = static_cast<int64_t>(uv);
            }
        }
        VecResult r;
        r.data = out; r.type = DRAKEN_INT64; r.validity_embedded = 0u; r.ts_unit = 0xFFu;
        kernel_preserve_shape(r, v);
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
        for (uint32_t j = 0u; j < k; ++j) {                                              \
            const double d = is64 ? static_cast<const double*>(v->data)[j]               \
                                  : static_cast<double>(static_cast<const float*>(v->data)[j]); \
            if (!(d >= 0.0) || d > static_cast<double>(UMAX)) {                          \
                draken_free(out);                                                         \
                return draken_error_sentinel_fmt(                                         \
                    #fn_name ": value %g out of range for " #UT, d);                       \
            }                                                                             \
            out[j] = static_cast<UT>(d);                                                  \
        }                                                                                 \
        VecResult r;                                                                     \
        r.data = out; r.type = TAG; r.validity_embedded = 0u; r.ts_unit = 0xFFu;          \
        kernel_preserve_shape(r, v);                                                      \
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
