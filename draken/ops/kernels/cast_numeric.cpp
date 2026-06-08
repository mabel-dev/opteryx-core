#include "ops/kernels/cast_kernels.h"
#include "ops/kernels/error_handling.h"
#include "ops/kernels/result_helpers.h"
#include "core/alloc.h"
#include "core/vector_alloc.h"
#include "core/string_slot.h"
#include "ryu.h"          // d2fixed_buffered_n / d2s_buffered_n (FLOAT64 → ASCII)
#include <cstring>
#include <cmath>          // std::isfinite / std::isnan
#include <vector>         // single-pass format staging (avoid double ryu)

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

// Shared int64/uint64 → VARCHAR core. Compression-aware: format the data_length
// PHYSICAL values into a value block (1 for a constant, K for a dict, length for
// dense) and carry the input's selection + validity through — the output keeps the
// input's encoding (constant→constant string, dict→dict string). Two passes over
// the K values: size the arena, then fill. Int formatting never fails (no
// null-introduction), so validity is preserved 1:1.
static VecResult int_to_string_core(const DrakenVector* v, bool treat_as_unsigned) {
    if (!v) return draken_error_sentinel("Input vector is null");
    if (v->type != DRAKEN_INT64)
        return draken_error_sentinel_fmt("cast-to-string: expected INT64, got %d", v->type);

    const int64_t* src = static_cast<const int64_t*>(v->data);
    const uint32_t k   = v->data_length;   // physical value count

    char tmp[21];
    size_t total_extern = 0u;
    for (uint32_t j = 0u; j < k; ++j) {
        int len = treat_as_unsigned ? u64_to_ascii(static_cast<uint64_t>(src[j]), tmp)
                                     : i64_to_ascii(src[j], tmp);
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
        int len = treat_as_unsigned ? u64_to_ascii(static_cast<uint64_t>(src[j]), tmp)
                                     : i64_to_ascii(src[j], tmp);
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

}  // extern "C"
