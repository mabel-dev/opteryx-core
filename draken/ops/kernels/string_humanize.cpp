// draken/ops/kernels/string_humanize.cpp — HUMANIZE string kernel on the C ABI.
// Signature is the design's func_fn_t:
//
//     VecResult fn(void* ctx, const DrakenVector* const* args, uint32_t nargs)
//
// Dispatched DIRECTLY from the nogil DV* VM (evaluation.pyx's BC_FUNCTION C-native
// arm) — no Python, no nanobind, no GIL.
//
// SEMANTICS — HUMANIZE(val [, mode]). `mode` is a bind-time string literal from a
// CLOSED set, lowered to an integer id in binary_op_ctx.op_code by
// compiled_expression.pyx (the same vehicle draken_date_trunc uses for its unit).
// An unknown mode is rejected at PLAN time; the `default:` arm here is defence in
// depth, not a user-facing path.
//
//   words   (default, no 2nd arg)  1.4 million        x1000, spelled out
//   compact                        1.4M               x1000, suffixed
//   bytes                          1.4 GiB            x1024, IEC labels
//   si                             1.4G / 1.4µ        x1000, BOTH directions
//   time                           2.4 hours          seconds base, mixed radix
//   clock                          01:23:45           seconds -> HH:MM:SS
//   percent                        42.1%              x100
//   odds                           1 in 1 million     1/x through the words ladder
//
// The bucket rule is shared by every ladder and unchanged from the original:
// walk thresholds largest-first, take the first whose value/threshold rounds to
// >= 0.9 at one decimal place. Below the smallest threshold each mode has its own
// fallback (see hz_format_one).
//
// NEGATIVES ABBREVIATE. The bucket test is on |rounded|, not `rounded` — the
// original tested the signed value, so a negative could never reach +0.9 and
// HUMANIZE(-2500000000) rendered "-2,500,000,000" instead of "-2.5 billion".
// The value handed to the formatter is still signed, so the sign is carried by
// the digits themselves (hz_group_thousands already passes a leading '-').
//
// UNREPRESENTABLE ROWS ARE NULL. `odds` is defined on (0, 1] only, and `clock`
// cannot render a second count beyond the uint64 hour split; those rows come back
// NULL (via kernel_null_bad_rows, the TRY_CAST vehicle) rather than being clamped
// to a wrong answer. This is the only null this kernel introduces.
//
// NaN/Infinity are spelled "NaN" / "Infinity" / "-Infinity" — matching the
// engine's numeric->string CAST kernel (cast_numeric.cpp), not the legacy Python
// implementation's "inf trillion" (an accident of round(x, 1) passing non-finite
// floats through unchanged). Under `odds` they are out of domain, so NULL.
//
// BUFFER SIZING IS LOAD-BEARING. d2fixed writes up to ~310 integer digits for a
// double near DBL_MAX, and the ladder divides by at most 1e12 before formatting —
// so a FLOAT64 of 1e300 formats ~290 digits. The original sized these buffers at
// 40 bytes and aborted the process (SIGABRT, stack smash) on
// HUMANIZE(CAST(1e300 AS FLOAT64)). HZ_RAW_MAX/HZ_OUT_MAX cover the true worst
// case; do not shrink them.
//
// DECIMAL (int64-backed, p<=18): the physical value is an unscaled int64, so
// reading it needs the operand's LogicalType scale — a bind-time detail the
// DrakenVector cannot carry. compiled_expression.pyx hands it over in the same
// binary_op_ctx (left_scale); a DECIMAL operand with `ctx == nullptr` fails loud
// rather than reading raw unscaled bits. DECIMAL128 stays unsupported (no int128
// reader in this file).
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
#include "ops/kernels/kernel_context.h"   // binary_op_ctx — mode id + DECIMAL scale
#include "ryu.h"

namespace {

// Mode ids. Kernel contract — mirrored by _HUMANIZE_MODES in
// opteryx/compiled/expression/compiled_expression.pyx. Ids are positional and
// must not be renumbered.
enum HzMode : uint32_t {
    HZ_WORDS   = 0u,
    HZ_COMPACT = 1u,
    HZ_BYTES   = 2u,
    HZ_SI      = 3u,
    HZ_TIME    = 4u,
    HZ_CLOCK   = 5u,
    HZ_PERCENT = 6u,
    HZ_ODDS    = 7u,
    HZ_MODE_MAX = HZ_ODDS,
};

// See "BUFFER SIZING IS LOAD-BEARING" above. Ryu's d2fixed emits at most
// 1 (sign) + 309 (integer digits) + 1 ('.') + decimals; comma-grouping then adds
// one byte per three integer digits, and a label adds at most 12.
constexpr size_t HZ_RAW_MAX = 384u;
constexpr size_t HZ_OUT_MAX = 576u;

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
    // DECIMAL (int64-backed, p<=18) is readable via the bind-time scale ctx — see
    // draken_humanize's ctx handling below, same vehicle draken_sqrt uses.
    // DECIMAL128 stays excluded: no int128 reader in this file (same precedent as
    // draken_sqrt/draken_abs's DECIMAL128 rejection) — out of scope for this cut.
    return hz_is_signed_int(t) || hz_is_unsigned_int(t) || hz_is_float(t)
        || t == DRAKEN_DECIMAL;
}

// Direct physical-slot read — j indexes data_length PHYSICAL values, not logical
// rows (mirrors draken_cast_float64_to_string's `data[j]` access; selection is
// re-applied afterwards by kernel_preserve_shape). `dec_unscale` is
// 10^-left_scale for a DECIMAL operand (1.0 otherwise) — see draken_humanize.
inline double hz_read_physical(const DrakenVector* v, uint32_t j, double dec_unscale) {
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
        case DRAKEN_DECIMAL:
            return static_cast<double>(static_cast<const int64_t*>(v->data)[j]) * dec_unscale;
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

// The canonical non-finite spellings, matching the engine's numeric->string cast
// kernel (cast_numeric.cpp's ryu_format_double).
size_t hz_write_nonfinite(double value, char* out) {
    if (std::isnan(value)) { std::memcpy(out, "NaN", 3u); return 3u; }
    if (value > 0.0) { std::memcpy(out, "Infinity", 8u); return 8u; }
    std::memcpy(out, "-Infinity", 9u); return 9u;
}

// Ryu-format `value` to `decimals` fractional digits, comma-group the integer
// part. Matches Python's f"{value:,.{decimals}f}". `out` must be >= HZ_OUT_MAX.
//
// The non-finite guard is load-bearing, not belt-and-braces: d2fixed spells
// infinity "Infinity", which hz_group_thousands would then comma-group into
// "In,fin,ity". Callers reach here with a non-finite value even though
// hz_format_one rejects one up front, because a mode's own arithmetic can
// overflow a finite input (percent's x100, odds' 1/x).
size_t hz_format_grouped(double value, uint32_t decimals, char* out) {
    if (!std::isfinite(value)) return hz_write_nonfinite(value, out);
    char raw[HZ_RAW_MAX];
    int rlen = d2fixed_buffered_n(value, decimals, raw);
    return hz_group_thousands(raw, static_cast<size_t>(rlen), out);
}

// One rung of a scale ladder. `one`/`many` differ only where the unit inflects
// (the `time` ladder); every other ladder passes the same pointer twice.
struct HzStep { double threshold; const char* one; const char* many; };

const HzStep HZ_WORDS_STEPS[] = {
    {1e12, "trillion", "trillion"},
    {1e9,  "billion",  "billion"},
    {1e6,  "million",  "million"},
    {1e3,  "thousand", "thousand"},
};

const HzStep HZ_COMPACT_STEPS[] = {
    {1e12, "T", "T"}, {1e9, "B", "B"}, {1e6, "M", "M"}, {1e3, "K", "K"},
};

// IEC binary multiples — 1024-based, KiB/MiB/... labels (ratified 2026-08-08).
const HzStep HZ_BYTES_STEPS[] = {
    {1152921504606846976.0, "EiB", "EiB"},   // 1024^6
    {1125899906842624.0,    "PiB", "PiB"},   // 1024^5
    {1099511627776.0,       "TiB", "TiB"},   // 1024^4
    {1073741824.0,          "GiB", "GiB"},   // 1024^3
    {1048576.0,             "MiB", "MiB"},   // 1024^2
    {1024.0,                "KiB", "KiB"},
};

// SI prefixes, both directions. The {1, ""} rung is what makes an in-range value
// render bare ("5.0") instead of falling through to the sub-unit prefixes.
const HzStep HZ_SI_STEPS[] = {
    {1e24, "Y", "Y"}, {1e21, "Z", "Z"}, {1e18, "E", "E"}, {1e15, "P", "P"},
    {1e12, "T", "T"}, {1e9,  "G", "G"}, {1e6,  "M", "M"}, {1e3,  "k", "k"},
    {1.0,  "",  ""},
    {1e-3, "m", "m"}, {1e-6, "\xc2\xb5", "\xc2\xb5"},      // U+00B5 MICRO SIGN
    {1e-9, "n", "n"}, {1e-12, "p", "p"}, {1e-15, "f", "f"}, {1e-18, "a", "a"},
};

// Seconds base. Years are 365.25 days (Julian) so that century/millennium are
// exact multiples of the year rung. Weeks are omitted deliberately (rarely the
// unit a reader wants) and months/quarters are omitted because they are not a
// fixed number of seconds — rendering them would be a lie.
const HzStep HZ_TIME_STEPS[] = {
    {31557600000.0, "millennium",  "millennia"},      // 1000 years
    {3155760000.0,  "century",     "centuries"},      //  100 years
    {31557600.0,    "year",        "years"},          // 365.25 days
    {86400.0,       "day",         "days"},
    {3600.0,        "hour",        "hours"},
    {60.0,          "minute",      "minutes"},
    {1.0,           "second",      "seconds"},
    {1e-3,          "millisecond", "milliseconds"},
    {1e-6,          "microsecond", "microseconds"},
    {1e-9,          "nanosecond",  "nanoseconds"},
};

// Walk a ladder largest-threshold-first; emit at the first rung whose scaled
// value rounds to >= 0.9 in magnitude. Returns bytes written, or 0 when no rung
// matched (the caller then applies its mode's fallback).
//
// `spaced` separates number and label with ' ' (words/bytes/time); the suffix
// styles (compact/si) butt them together. `drop_unit_fraction` strips an exact
// ".0" before grouping — `odds` only, so 1/1e-6 reads "1 in 1 million" rather
// than "1 in 1.0 million".
size_t hz_walk_ladder(double value, const HzStep* steps, size_t nsteps,
                      bool spaced, bool drop_unit_fraction, char* out) {
    for (size_t s = 0u; s < nsteps; ++s) {
        const double scaled = value / steps[s].threshold;
        // Unreachable for a finite `value`: rungs descend, so `scaled` only grows
        // as we walk, and a rung that overflowed would have been preceded by one
        // that already passed the 0.9 test. Guarded anyway — d2fixed is undefined
        // on non-finite input, and a new ladder must not be able to reintroduce
        // the "In,fin,ity" class of bug.
        if (!std::isfinite(scaled)) continue;
        char rbuf[HZ_RAW_MAX];
        int rlen = d2fixed_buffered_n(scaled, 1u, rbuf);
        // Correctly-rounded-to-1dp value, parsed back for the bucket test. The
        // test is on MAGNITUDE so negatives abbreviate (see file header).
        const double rounded = std::strtod(rbuf, nullptr);
        if (std::fabs(rounded) < 0.9) continue;

        size_t nlen = static_cast<size_t>(rlen);
        const bool unit_fraction =
            nlen >= 2u && rbuf[nlen - 2u] == '.' && rbuf[nlen - 1u] == '0';
        // Singular only when the rendered magnitude is exactly one.
        const bool singular = unit_fraction
            && (nlen == 3u ? std::memcmp(rbuf, "1.0", 3u) == 0
                           : (nlen == 4u && std::memcmp(rbuf, "-1.0", 4u) == 0));
        if (drop_unit_fraction && unit_fraction) nlen -= 2u;

        size_t o = hz_group_thousands(rbuf, nlen, out);
        const char* label = singular ? steps[s].one : steps[s].many;
        if (label[0] != '\0') {
            if (spaced) out[o++] = ' ';
            const size_t ll = std::strlen(label);
            std::memcpy(out + o, label, ll);
            o += ll;
        }
        return o;
    }
    return 0u;
}

// seconds -> "[-]HH:MM:SS", hours unbounded (so "277:46:40" is a valid rendering
// of 1e6 seconds). Fractional seconds truncate toward zero. Beyond the uint64
// second range there is no hour split to make, so the row is unrepresentable and
// comes back NULL — the same disposition as an out-of-domain `odds` row.
size_t hz_format_clock(double value, char* out, bool* out_null) {
    const double av = std::fabs(value);
    if (!(av < 18446744073709551616.0)) { *out_null = true; return 0u; }

    uint64_t total = static_cast<uint64_t>(av);
    size_t o = 0u;
    if (std::signbit(value) && total > 0u) out[o++] = '-';

    uint64_t hh = total / 3600u;
    const uint32_t mm = static_cast<uint32_t>((total % 3600u) / 60u);
    const uint32_t ss = static_cast<uint32_t>(total % 60u);

    char hbuf[24];
    int hl = 0;
    if (hh == 0u) {
        hbuf[hl++] = '0';
    } else {
        while (hh > 0u) { hbuf[hl++] = static_cast<char>('0' + (hh % 10u)); hh /= 10u; }
    }
    if (hl < 2) hbuf[hl++] = '0';               // zero-pad to at least HH
    while (hl > 0) out[o++] = hbuf[--hl];

    out[o++] = ':';
    out[o++] = static_cast<char>('0' + mm / 10u);
    out[o++] = static_cast<char>('0' + mm % 10u);
    out[o++] = ':';
    out[o++] = static_cast<char>('0' + ss / 10u);
    out[o++] = static_cast<char>('0' + ss % 10u);
    return o;
}

// One physical value -> formatted bytes into `out` (>= HZ_OUT_MAX). Returns byte
// length written; sets *out_null when the value has no representation in `mode`
// (odds outside (0,1], clock beyond the uint64 second range, and the non-finite
// values under odds).
size_t hz_format_one(double value, bool src_is_int, uint32_t mode,
                     char* out, bool* out_null) {
    *out_null = false;

    if (!std::isfinite(value)) {
        if (mode == HZ_ODDS || mode == HZ_CLOCK) { *out_null = true; return 0u; }
        return hz_write_nonfinite(value, out);
    }

    switch (mode) {
        case HZ_COMPACT: {
            const size_t n = hz_walk_ladder(value, HZ_COMPACT_STEPS, 4u, false, false, out);
            return n ? n : hz_format_grouped(value, src_is_int ? 0u : 1u, out);
        }
        case HZ_BYTES: {
            const size_t n = hz_walk_ladder(value, HZ_BYTES_STEPS, 6u, true, false, out);
            if (n) return n;
            // Byte counts are whole below 1 KiB regardless of the source type.
            size_t o = hz_format_grouped(value, 0u, out);
            out[o++] = ' ';
            out[o++] = 'B';
            return o;
        }
        case HZ_SI: {
            const size_t n = hz_walk_ladder(value, HZ_SI_STEPS, 15u, false, false, out);
            // Only |value| < 0.9e-18 (and zero) reaches here — no prefix applies.
            return n ? n : hz_format_grouped(value, 0u, out);
        }
        case HZ_TIME: {
            const size_t n = hz_walk_ladder(value, HZ_TIME_STEPS, 10u, true, false, out);
            if (n) return n;
            size_t o = hz_format_grouped(value, value == 0.0 ? 0u : 1u, out);
            std::memcpy(out + o, " seconds", 8u);
            return o + 8u;
        }
        case HZ_CLOCK:
            return hz_format_clock(value, out, out_null);
        case HZ_PERCENT: {
            // x100 overflows for |value| > ~1.8e306. There is no percentage to
            // render then, but the magnitude is real, so it takes the same
            // spelling every other non-finite result does — and no '%', which
            // would claim a number precedes it.
            const double pct = value * 100.0;
            if (!std::isfinite(pct)) return hz_write_nonfinite(pct, out);
            size_t o = hz_format_grouped(pct, 1u, out);
            out[o++] = '%';
            return o;
        }
        case HZ_ODDS: {
            // Defined on (0, 1] only; everything else is unrepresentable as odds.
            if (!(value > 0.0) || value > 1.0) { *out_null = true; return 0u; }
            // A subnormal value inverts to infinity — there is no "1 in N" for
            // it, so it is unrepresentable in exactly the sense the ruling means.
            const double n = 1.0 / value;
            if (!std::isfinite(n)) { *out_null = true; return 0u; }
            std::memcpy(out, "1 in ", 5u);
            const size_t w =
                hz_walk_ladder(n, HZ_WORDS_STEPS, 4u, true, true, out + 5u);
            // n < ~1111 here, so the fallback renders a plain whole count.
            return 5u + (w ? w : hz_format_grouped(n, 0u, out + 5u));
        }
        case HZ_WORDS:
        default: {
            const size_t n = hz_walk_ladder(value, HZ_WORDS_STEPS, 4u, true, false, out);
            return n ? n : hz_format_grouped(value, src_is_int ? 0u : 1u, out);
        }
    }
}

}  // namespace

extern "C" VecResult draken_humanize(void* ctx, const DrakenVector* const* args, uint32_t nargs) {
    DRAKEN_KERNEL_TRY({
        // The mode literal is consumed at BIND time into ctx->op_code and never
        // pushed, so this kernel still sees exactly one operand.
        if (nargs != 1u) return draken_error_sentinel("HUMANIZE: expected 1 argument");
        const DrakenVector* v = args[0];
        if (v == nullptr) return draken_error_sentinel("HUMANIZE: input vector is null");
        if (v->type == DRAKEN_DECIMAL128)
            return draken_error_sentinel(
                "HUMANIZE: DECIMAL128 operand (precision > 18) is not supported by this kernel");
        if (!hz_is_numeric(v->type))
            return draken_error_sentinel_fmt(
                "HUMANIZE: numeric input required, got type %d", static_cast<int>(v->type));

        const binary_op_ctx* bctx = static_cast<const binary_op_ctx*>(ctx);

        // No ctx at all is the plain HUMANIZE(val) over a non-DECIMAL operand.
        const uint32_t mode =
            bctx ? static_cast<uint32_t>(bctx->op_code) : static_cast<uint32_t>(HZ_WORDS);
        if (mode > HZ_MODE_MAX)
            return draken_error_sentinel_fmt(
                "HUMANIZE: unknown mode id %u (bind-time lowering is out of step with the kernel)",
                mode);

        double dec_unscale = 1.0;
        if (v->type == DRAKEN_DECIMAL) {
            if (bctx == nullptr) {
                return draken_error_sentinel(
                    "HUMANIZE: DECIMAL operand needs its bind-time scale context");
            }
            dec_unscale = std::pow(10.0, -static_cast<double>(bctx->left_scale));
        }

        const uint32_t k = v->data_length;
        // DECIMAL always carries fractional precision (that's the point of its
        // scale), so it formats like a float-family source (1dp), not like an
        // integer-family one (0dp).
        const bool src_is_int = hz_is_signed_int(v->type) || hz_is_unsigned_int(v->type);

        // Pass 1: format each unique physical value ONCE into a staging buffer.
        // `bad` is indexed by PHYSICAL value, matching kernel_null_bad_rows'
        // shape-preserving contract.
        std::vector<char> stage;
        stage.reserve(static_cast<size_t>(k) * 20u);
        std::vector<uint32_t> rlen(k > 0u ? k : 1u, 0u);
        std::vector<uint8_t> bad(k > 0u ? k : 1u, 0u);
        bool any_bad = false;
        char tmp[HZ_OUT_MAX];
        size_t total_extern = 0u;
        for (uint32_t j = 0u; j < k; ++j) {
            const double d = hz_read_physical(v, j, dec_unscale);
            bool is_null = false;
            const size_t len = hz_format_one(d, src_is_int, mode, tmp, &is_null);
            if (is_null) {
                // Null rows carry an empty slot; the validity bit is what a reader
                // consults, but leaving stale bytes behind would be a trap.
                bad[j] = 1u;
                any_bad = true;
                rlen[j] = 0u;
                continue;
            }
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
        // AFTER the shape finalizer — it may have to materialise a bitmap the
        // input did not have (result_helpers.h).
        if (any_bad) kernel_null_bad_rows(r, v, bad.data());
        return r;
    });
}
