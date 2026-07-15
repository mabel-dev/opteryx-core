// draken/ops/kernels/string_humanize.cpp — Phase 9a-fn: HUMANIZE string kernel on
// the C ABI. Signature is the design's func_fn_t:
//
//     VecResult fn(void* ctx, const DrakenVector* const* args, uint32_t nargs)
//
// Dispatched DIRECTLY from the nogil DV* VM (evaluation.pyx's BC_FUNCTION C-native
// arm) — no Python, no nanobind, no GIL.
//
// SEMANTICS — ported byte-for-byte from the legacy Python implementation
// (opteryx/expression/functions/implementations/utility.pyx::humanize):
//   For |value| >= 900 of a threshold (trillion/billion/million/thousand, checked
//   largest first), emit "<value/threshold rounded to 1dp, comma-grouped> <label>".
//   Otherwise emit the value comma-grouped with 0 decimals for an integer-family
//   source column, 1 decimal for a float-family source column. Negative values
//   never abbreviate (division by a positive threshold never reaches +0.9), matching
//   the reference implementation.
//   NaN/Infinity are spelled "NaN" / "Infinity" / "-Infinity" — matching the
//   engine's numeric->string CAST kernel (cast_numeric.cpp), not the reference
//   Python implementation's "inf trillion" (an accident of round(x, 1) passing
//   non-finite floats through unchanged — not worth reproducing; see
//   hz_format_one).
//
// SHAPE-PRESERVING (the string-CAST pattern, cast_numeric.cpp's
// draken_cast_float64_to_string): the formatted string is a pure function of one
// physical value, so it is computed ONCE per data_length PHYSICAL unique value
// (dict K, constant 1, or dense `length`), then kernel_preserve_shape carries the
// input's selection + per-logical-row validity onto the result.

#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <cmath>
#include <vector>

#include "core/buffers.h"
#include "core/string_slot.h"
#include "core/alloc.h"
#include "ops/vec_result.h"
#include "ops/kernels/result_helpers.h"
#include "ops/kernels/error_handling.h"
#include "ryu.h"

namespace {

inline bool hz_is_signed_int(DrakenType t) {
    switch (t) {
        case DRAKEN_INT8: case DRAKEN_INT16: case DRAKEN_INT32: case DRAKEN_INT64:
            return true;
        default:
            return false;
    }
}

inline bool hz_is_unsigned_int(DrakenType t) {
    switch (t) {
        case DRAKEN_UINT8: case DRAKEN_UINT16: case DRAKEN_UINT32: case DRAKEN_UINT64:
            return true;
        default:
            return false;
    }
}

inline bool hz_is_float(DrakenType t) {
    return t == DRAKEN_FLOAT32 || t == DRAKEN_FLOAT64;
}

inline bool hz_is_numeric(DrakenType t) {
    // DECIMAL excluded deliberately: reading it correctly needs the bind-time
    // scale context (same precedent as draken_abs rejecting unscaled DECIMAL) —
    // out of scope for this cut. Fail loud rather than reading raw unscaled bits.
    return hz_is_signed_int(t) || hz_is_unsigned_int(t) || hz_is_float(t);
}

// Direct physical-slot read — j indexes data_length PHYSICAL values, not logical
// rows (mirrors draken_cast_float64_to_string's `data[j]` access; selection is
// re-applied afterwards by kernel_preserve_shape).
inline double hz_read_physical(const DrakenVector* v, uint32_t j) {
    switch (v->type) {
        case DRAKEN_INT8:    return static_cast<const int8_t*>(v->data)[j];
        case DRAKEN_INT16:   return static_cast<const int16_t*>(v->data)[j];
        case DRAKEN_INT32:   return static_cast<const int32_t*>(v->data)[j];
        case DRAKEN_INT64:   return static_cast<double>(static_cast<const int64_t*>(v->data)[j]);
        case DRAKEN_UINT8:   return static_cast<const uint8_t*>(v->data)[j];
        case DRAKEN_UINT16:  return static_cast<const uint16_t*>(v->data)[j];
        case DRAKEN_UINT32:  return static_cast<const uint32_t*>(v->data)[j];
        case DRAKEN_UINT64:  return static_cast<double>(static_cast<const uint64_t*>(v->data)[j]);
        case DRAKEN_FLOAT32: return static_cast<const float*>(v->data)[j];
        case DRAKEN_FLOAT64: return static_cast<const double*>(v->data)[j];
        default:             return 0.0;   // unreachable: caller gates on hz_is_numeric
    }
}

// Insert ',' every 3 digits into the integer part of a "-?ddd(.d+)?" ASCII string.
// `out` must be sized >= len + len/3 + 1.
size_t hz_group_thousands(const char* digits, size_t len, char* out) {
    const bool neg = len > 0u && digits[0] == '-';
    const char* p = digits + (neg ? 1 : 0);
    const size_t body_len = len - (neg ? 1u : 0u);
    const char* dot = static_cast<const char*>(std::memchr(p, '.', body_len));
    const size_t int_len = dot ? static_cast<size_t>(dot - p) : body_len;

    size_t o = 0u;
    if (neg) out[o++] = '-';
    for (size_t i = 0; i < int_len; ++i) {
        if (i > 0u && (int_len - i) % 3u == 0u) out[o++] = ',';
        out[o++] = p[i];
    }
    if (dot != nullptr) {
        const size_t frac_len = body_len - int_len;
        std::memcpy(out + o, dot, frac_len);
        o += frac_len;
    }
    return o;
}

// Ryu-format `value` to `decimals` fractional digits, comma-group the integer
// part. Matches Python's f"{value:,.{decimals}f}". `out` must be >= 48 bytes.
size_t hz_format_grouped(double value, uint32_t decimals, char* out) {
    char raw[40];
    int rlen = d2fixed_buffered_n(value, decimals, raw);
    return hz_group_thousands(raw, static_cast<size_t>(rlen), out);
}

struct HzThreshold { double value; const char* label; size_t label_len; };
const HzThreshold HZ_THRESHOLDS[4] = {
    {1e12, "trillion", 8u},
    {1e9,  "billion",  7u},
    {1e6,  "million",  7u},
    {1e3,  "thousand", 8u},
};

// One physical value -> formatted bytes into `out` (>= 48 bytes is ample: worst
// case is an INT64-range fallback like "-9,223,372,036,854,775,808", 27 bytes).
// Returns byte length written.
//
// NaN/Infinity: Ryu's d2fixed is undefined on non-finite input, so these bypass
// the threshold loop entirely. Spelled "NaN" / "Infinity" / "-Infinity" — the
// same spelling the engine's own numeric->string cast kernel uses
// (cast_numeric.cpp's ryu_format_double) — rather than reproducing the legacy
// Python implementation's accidental "inf trillion" (an artifact of round(x, 1)
// passing non-finite floats through unchanged, so inf >= 0.9 is True).
size_t hz_format_one(double value, bool src_is_int, char* out) {
    if (std::isnan(value)) { std::memcpy(out, "NaN", 3u); return 3u; }
    if (std::isinf(value)) {
        if (value > 0.0) { std::memcpy(out, "Infinity", 8u); return 8u; }
        std::memcpy(out, "-Infinity", 9u); return 9u;
    }

    for (const auto& th : HZ_THRESHOLDS) {
        const double scaled = value / th.value;
        char rbuf[40];
        int rlen = d2fixed_buffered_n(scaled, 1u, rbuf);
        // Correctly-rounded-to-1dp value, parsed back for the bucket test —
        // matches Python's `round(value / threshold, 1) >= 0.9`.
        const double rounded = std::strtod(rbuf, nullptr);
        if (rounded >= 0.9) {
            size_t n = hz_group_thousands(rbuf, static_cast<size_t>(rlen), out);
            out[n++] = ' ';
            std::memcpy(out + n, th.label, th.label_len);
            n += th.label_len;
            return n;
        }
    }
    return hz_format_grouped(value, src_is_int ? 0u : 1u, out);
}

}  // namespace

extern "C" VecResult draken_humanize(void* /*ctx*/, const DrakenVector* const* args, uint32_t nargs) {
    DRAKEN_KERNEL_TRY({
        if (nargs != 1u) return draken_error_sentinel("HUMANIZE: expected 1 argument");
        const DrakenVector* v = args[0];
        if (v == nullptr) return draken_error_sentinel("HUMANIZE: input vector is null");
        if (!hz_is_numeric(v->type))
            return draken_error_sentinel_fmt(
                "HUMANIZE: numeric input required, got type %d", static_cast<int>(v->type));

        const uint32_t k = v->data_length;
        const bool src_is_int = hz_is_signed_int(v->type) || hz_is_unsigned_int(v->type);

        // Pass 1: format each unique physical value ONCE into a staging buffer.
        std::vector<char> stage;
        stage.reserve(static_cast<size_t>(k) * 20u);
        std::vector<uint32_t> rlen(k > 0u ? k : 1u, 0u);
        char tmp[48];
        size_t total_extern = 0u;
        for (uint32_t j = 0u; j < k; ++j) {
            const double d = hz_read_physical(v, j);
            const size_t len = hz_format_one(d, src_is_int, tmp);
            rlen[j] = static_cast<uint32_t>(len);
            stage.insert(stage.end(), tmp, tmp + len);
            if (len > STR_INLINE_MAX) total_extern += len;
        }

        DrakenStringSlot* slots;
        uint8_t* arena;
        uint8_t* vunused;
        uint8_t* block = vecresult_string_block_alloc(k, total_extern, 0, &slots, &arena, &vunused);
        if (!block) return draken_error_sentinel("HUMANIZE: allocation failed");
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
