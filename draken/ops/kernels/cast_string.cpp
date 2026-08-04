#include "ops/kernels/cast_kernels.h"
#include "core/ipv4.h"
#include "ops/kernels/error_handling.h"
#include "ops/kernels/kernel_context.h"
#include "ops/kernels/result_helpers.h"
#include "ops/sql_temporal_format.h"
#include "core/alloc.h"
#include "core/vector_alloc.h"
#include "core/string_slot.h"
#include "ops/float_ops.h"   // fp_canon (NaN/-0.0 canonicalization)
#include "fast_float.h"      // string → double parse (vendored)
#include "utf8.h"            // utf8nvalid (vendored) — CAST(... AS NVARCHAR) validation
#include <cstring>
#include <system_error>
#include <string>
#include <vector>            // liveness mask for compression-aware raising casts

/**
 * Cast kernels: string → numeric/bool (Phase 9c).
 *
 * Compute extracted from opteryx/compiled/nanobind/vector_casts.cpp. Parse
 * failures return an error sentinel with a descriptive message; the nanobind
 * shim re-raises it as ValueError to preserve the existing Python contract.
 */

// ASCII whitespace recognized when trimming numeric strings (matches the
// nanobind string→float64 path).
static inline bool cast_is_ascii_space(uint8_t c) noexcept {
    return c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f' || c == '\v';
}

extern "C" {

// STRING → FLOAT64. Parse failures map to NULL (matches the nanobind twin's
// TRY-style float semantics — distinct from string→int64, which raises). Leading
// '+' and surrounding ASCII whitespace are tolerated; results are fp_canon'd.
VecResult draken_cast_string_to_float64(void* ctx, const DrakenVector* v) {
    DRAKEN_KERNEL_TRY({
        if (!v) return draken_error_sentinel("Input vector is null");
        if (v->type != DRAKEN_VARCHAR && v->type != DRAKEN_NVARCHAR && v->type != DRAKEN_VARBINARY)
            return draken_error_sentinel_fmt("cast string->float64: expected string, got %d", v->type);

        // Compression-aware: parse the K physical values (a constant parses one, a
        // dict K). This cast is NULL-INTRODUCING — a value that fails to parse maps
        // EVERY logical row referencing it to null, so we track per-physical-value
        // failure and fold it into the preserved validity via the selection.
        const DrakenStringArena* sa = static_cast<const DrakenStringArena*>(v->data);
        const uint32_t k = v->data_length;
        const uint32_t n = v->length;
        double* out = static_cast<double*>(draken_malloc((k > 0u ? k : 1u) * sizeof(double)));
        if (!out) return draken_error_sentinel("Allocation failed");

        std::vector<uint8_t> bad(k > 0u ? k : 1u, 0u);
        bool any_bad = false;
        for (uint32_t j = 0u; j < k; ++j) {
            const DrakenStringSlot* slot = &sa->slots[j];
            const uint8_t* bytes = str_data(slot, sa->arena);
            uint32_t len   = str_length(slot);
            uint32_t start = 0u;
            uint32_t end   = len;
            while (start < end && cast_is_ascii_space(bytes[start])) ++start;
            while (end > start && cast_is_ascii_space(bytes[end - 1u])) --end;
            if (start < end && bytes[start] == '+') ++start;

            double value = 0.0;
            const char* first = reinterpret_cast<const char*>(bytes + start);
            const char* last  = reinterpret_cast<const char*>(bytes + end);
            fast_float::from_chars_result res = fast_float::from_chars(first, last, value);
            if (first == last || res.ec != std::errc() || res.ptr != last) {
                if (!kernel_cast_is_safe(ctx)) {
                    draken_free(out);
                    return draken_error_sentinel("Invalid number in string literal");
                }
                out[j] = 0.0; bad[j] = 1u; any_bad = true;
            } else {
                out[j] = draken::ops::fp_canon(value);
            }
        }

        VecResult r;
        r.data = out; r.type = DRAKEN_FLOAT64; r.validity_embedded = 0u; r.ts_unit = 0xFFu;
        kernel_preserve_shape(r, v);  // r.validity = input copy (or null)

        if (any_bad) kernel_null_bad_rows(r, v, bad.data());   // TRY_CAST rows -> NULL
        return r;
    });
}

VecResult draken_cast_string_to_int64(void* ctx, const DrakenVector* v) {
    DRAKEN_KERNEL_TRY({
        if (!v) return draken_error_sentinel("Input vector is null");
        if (v->type != DRAKEN_VARCHAR && v->type != DRAKEN_NVARCHAR && v->type != DRAKEN_VARBINARY)
            return draken_error_sentinel_fmt("cast string->int64: expected string, got %d", v->type);

        // Compression-aware: parse the data_length physical string values, preserve
        // the input's selection + validity. Only parse values referenced by a
        // non-null logical row (liveness) — a dict value used solely by null rows
        // must not trigger a spurious raise, and this also reproduces the dense
        // "skip null rows" behaviour.
        const DrakenStringArena* sa = static_cast<const DrakenStringArena*>(v->data);
        const uint32_t k = v->data_length;
        const uint32_t n = v->length;
        int64_t* out = static_cast<int64_t*>(draken_malloc((k > 0u ? k : 1u) * sizeof(int64_t)));
        if (!out) return draken_error_sentinel("Allocation failed");

        std::vector<uint8_t> live(k > 0u ? k : 1u, 0u);
        for (uint32_t i = 0u; i < n; ++i)
            if (!kernel_row_is_null(v, i)) live[v->selection[i]] = 1u;

        const bool is_safe = kernel_cast_is_safe(ctx);
        std::vector<uint8_t> bad(k > 0u ? k : 1u, 0u);
        bool any_bad = false;

        for (uint32_t j = 0u; j < k; ++j) {
            if (!live[j]) { out[j] = 0; continue; }
            const DrakenStringSlot* slot = &sa->slots[j];
            const uint8_t* sdata = str_data(slot, sa->arena);
            const uint32_t slen  = str_length(slot);

            int64_t value = 0;
            int64_t sign = 1;
            uint32_t p = 0;
            if (slen > 0 && sdata[0] == '-') { sign = -1; p = 1; }
            bool malformed = false;
            for (; p < slen; ++p) {
                const uint8_t c = sdata[p];
                if (c < '0' || c > '9') { malformed = true; break; }
                value = value * 10 + (c - '0');
            }
            if (malformed || slen == 0u) {
                if (!is_safe) {
                    draken_free(out);
                    return draken_error_sentinel("Invalid digit in integer literal");
                }
                out[j] = 0; bad[j] = 1u; any_bad = true; continue;
            }
            out[j] = sign * value;
        }

        VecResult r;
        r.data = out; r.type = DRAKEN_INT64; r.validity_embedded = 0u; r.ts_unit = 0xFFu;
        kernel_preserve_shape(r, v);
        if (any_bad) kernel_null_bad_rows(r, v, bad.data());   // TRY_CAST rows -> NULL
        return r;
    });
}

// E33 — STRING -> unsigned target (UINT8/16/32 ONLY — see draken_cast_string_to_uint64
// below for why UINT64 needs its own parser): parse via the proven
// draken_cast_string_to_int64 (same digit-validation/error-message contract for
// malformed input), then range-check-narrow the INT64 intermediate through the
// existing signed->unsigned kernel (draken_cast_integer_to_uintN), which itself
// calls kernel_preserve_shape. Safe here because UINT8/16/32's full range never
// exceeds INT64_MAX, so the intermediate int64 accumulation never overflows.
// No new parsing logic — composition, not duplication (CLAUDE.md §3/§11).
#define DRAKEN_CAST_STRING_VIA(fn_name, parse_fn, narrow_fn)                              \
VecResult fn_name(void* ctx, const DrakenVector* v) {                                     \
    DRAKEN_KERNEL_TRY({                                                                  \
        if (!v) return draken_error_sentinel("Input vector is null");                    \
        VecResult tmp = parse_fn(ctx, v);                                                \
        if (!tmp.data) return tmp;  /* propagate the error sentinel as-is */             \
        DrakenVector tmp_dv;                                                             \
        tmp_dv.data        = tmp.data;                                                   \
        tmp_dv.selection   = tmp.selection;                                              \
        tmp_dv.data_length = tmp.data_length;                                            \
        tmp_dv.length      = tmp.length;                                                 \
        tmp_dv.validity    = tmp.validity;                                                \
        tmp_dv.type        = tmp.type;                                                   \
        tmp_dv.flags       = tmp.flags;                                                  \
        VecResult r = narrow_fn(ctx, &tmp_dv);                                           \
        draken_free(tmp.data);                                                           \
        if (tmp.validity) draken_free(tmp.validity);                                     \
        if (tmp.owns_selection && tmp.selection)                                          \
            draken_free(const_cast<uint32_t*>(tmp.selection));                            \
        return r;                                                                          \
    });                                                                                    \
}

DRAKEN_CAST_STRING_VIA(draken_cast_string_to_uint8,  draken_cast_string_to_int64, draken_cast_integer_to_uint8)
DRAKEN_CAST_STRING_VIA(draken_cast_string_to_uint16, draken_cast_string_to_int64, draken_cast_integer_to_uint16)
DRAKEN_CAST_STRING_VIA(draken_cast_string_to_uint32, draken_cast_string_to_int64, draken_cast_integer_to_uint32)

// STRING -> narrow SIGNED target, by the same composition: the int64 parser (one
// digit-validation contract, one error message) then the range-checked narrowing.
// Every INT8/16/32 value fits int64 with room to spare, so the intermediate can
// never overflow — the reason the unsigned family needs a bespoke UINT64 parser
// does not arise here.
DRAKEN_CAST_STRING_VIA(draken_cast_string_to_int8,  draken_cast_string_to_int64, draken_cast_integer_to_int8)
DRAKEN_CAST_STRING_VIA(draken_cast_string_to_int16, draken_cast_string_to_int64, draken_cast_integer_to_int16)
DRAKEN_CAST_STRING_VIA(draken_cast_string_to_int32, draken_cast_string_to_int64, draken_cast_integer_to_int32)

// STRING -> FLOAT32: the FLOAT64 parser (one number-syntax contract, one error
// message) then the range-checked narrowing — the same composition, a different
// parser. This is why the macro takes the parse step as a parameter.
DRAKEN_CAST_STRING_VIA(draken_cast_string_to_float32, draken_cast_string_to_float64, draken_cast_float_to_float32)

#undef DRAKEN_CAST_STRING_VIA

// E33 — STRING -> UINT64: genuine native parser with a uint64_t accumulator,
// NOT composed through draken_cast_string_to_int64 like the narrower UINT8/16/32
// casts above. UINT64's range (up to ~1.8e19) exceeds INT64_MAX (~9.2e18) — a
// value in that gap (e.g. "9223372036854775808") would overflow the int64_t
// accumulator in draken_cast_string_to_int64, which is undefined behavior in
// C++, not merely a wrong answer. Overflow is checked digit-by-digit and raises
// (fail loud — CLAUDE.md §1), matching the existing "Invalid digit" raise for
// malformed input. No leading '-' is accepted (unsigned target).
VecResult draken_cast_string_to_uint64(void* ctx, const DrakenVector* v) {
    DRAKEN_KERNEL_TRY({
        if (!v) return draken_error_sentinel("Input vector is null");
        if (v->type != DRAKEN_VARCHAR && v->type != DRAKEN_NVARCHAR && v->type != DRAKEN_VARBINARY)
            return draken_error_sentinel_fmt("cast string->uint64: expected string, got %d", v->type);

        const DrakenStringArena* sa = static_cast<const DrakenStringArena*>(v->data);
        const uint32_t k = v->data_length;
        const uint32_t n = v->length;
        uint64_t* out = static_cast<uint64_t*>(draken_malloc((k > 0u ? k : 1u) * sizeof(uint64_t)));
        if (!out) return draken_error_sentinel("Allocation failed");

        std::vector<uint8_t> live(k > 0u ? k : 1u, 0u);
        for (uint32_t i = 0u; i < n; ++i)
            if (!kernel_row_is_null(v, i)) live[v->selection[i]] = 1u;

        const bool is_safe = kernel_cast_is_safe(ctx);
        std::vector<uint8_t> bad(k > 0u ? k : 1u, 0u);
        bool any_bad = false;

        for (uint32_t j = 0u; j < k; ++j) {
            if (!live[j]) { out[j] = 0; continue; }
            const DrakenStringSlot* slot = &sa->slots[j];
            const uint8_t* sdata = str_data(slot, sa->arena);
            const uint32_t slen  = str_length(slot);

            if (slen > 0 && sdata[0] == '-') {
                if (!is_safe) {
                    draken_free(out);
                    return draken_error_sentinel(
                        "cast string->uint64: negative value out of range for uint64_t");
                }
                out[j] = 0; bad[j] = 1u; any_bad = true; continue;
            }
            uint64_t value = 0u;
            bool rejected = false;
            for (uint32_t p = 0u; p < slen; ++p) {
                const uint8_t c = sdata[p];
                if (c < '0' || c > '9') {
                    if (!is_safe) {
                        draken_free(out);
                        return draken_error_sentinel("Invalid digit in integer literal");
                    }
                    rejected = true; break;
                }
                const uint64_t digit = static_cast<uint64_t>(c - '0');
                // Overflow check BEFORE the multiply/add: value*10+digit must not
                // exceed UINT64_MAX. (UINT64_MAX - digit) / 10 is the largest
                // `value` for which the next digit still fits.
                if (value > (0xFFFFFFFFFFFFFFFFull - digit) / 10ull) {
                    if (!is_safe) {
                        draken_free(out);
                        return draken_error_sentinel("cast string->uint64: value out of range for uint64_t");
                    }
                    rejected = true; break;
                }
                value = value * 10ull + digit;
            }
            if (rejected) { out[j] = 0; bad[j] = 1u; any_bad = true; continue; }
            out[j] = value;
        }

        VecResult r;
        r.data = out; r.type = DRAKEN_UINT64; r.validity_embedded = 0u; r.ts_unit = 0xFFu;
        kernel_preserve_shape(r, v);
        if (any_bad) kernel_null_bad_rows(r, v, bad.data());
        return r;
    });
}

VecResult draken_cast_string_to_bool(void* ctx, const DrakenVector* v) {
    DRAKEN_KERNEL_TRY({
        if (!v) return draken_error_sentinel("Input vector is null");
        if (v->type != DRAKEN_VARCHAR && v->type != DRAKEN_NVARCHAR && v->type != DRAKEN_VARBINARY)
            return draken_error_sentinel_fmt("cast string->bool: expected string, got %d", v->type);

        // Compression-aware with liveness (see string->int64): K-bit output bitmap.
        const DrakenStringArena* sa = static_cast<const DrakenStringArena*>(v->data);
        const uint32_t k = v->data_length;
        const uint32_t n = v->length;
        const size_t nbytes = (k > 0u ? (k + 7u) / 8u : 1u);
        uint8_t* out = static_cast<uint8_t*>(draken_malloc(nbytes));
        if (!out) return draken_error_sentinel("Allocation failed");
        std::memset(out, 0, nbytes);

        std::vector<uint8_t> live(k > 0u ? k : 1u, 0u);
        for (uint32_t i = 0u; i < n; ++i)
            if (!kernel_row_is_null(v, i)) live[v->selection[i]] = 1u;

        const bool is_safe = kernel_cast_is_safe(ctx);
        std::vector<uint8_t> bad(k > 0u ? k : 1u, 0u);
        bool any_bad = false;

        for (uint32_t j = 0u; j < k; ++j) {
            if (!live[j]) continue;
            const DrakenStringSlot* slot = &sa->slots[j];
            const uint8_t* s = str_data(slot, sa->arena);
            const uint32_t slen = str_length(slot);

            bool truth;
            if (slen == 4 && (s[0]|32u)=='t' && (s[1]|32u)=='r' && (s[2]|32u)=='u' && (s[3]|32u)=='e') {
                truth = true;
            } else if (slen == 5 && (s[0]|32u)=='f' && (s[1]|32u)=='a' && (s[2]|32u)=='l' && (s[3]|32u)=='s' && (s[4]|32u)=='e') {
                truth = false;
            } else if (slen == 1 && s[0]=='1') { truth = true;
            } else if (slen == 1 && s[0]=='0') { truth = false;
            } else if (slen == 3 && (s[0]|32u)=='y' && (s[1]|32u)=='e' && (s[2]|32u)=='s') { truth = true;
            } else if (slen == 2 && (s[0]|32u)=='n' && (s[1]|32u)=='o') { truth = false;
            } else if (slen == 2 && (s[0]|32u)=='o' && (s[1]|32u)=='n') { truth = true;
            } else if (slen == 3 && (s[0]|32u)=='o' && (s[1]|32u)=='f' && (s[2]|32u)=='f') { truth = false;
            } else {
                if (!is_safe) {
                    draken_free(out);
                    return draken_error_sentinel(
                        "Cannot cast string to BOOL: expected true/false/1/0/yes/no/on/off");
                }
                bad[j] = 1u; any_bad = true; continue;
            }
            if (truth) out[j >> 3u] |= static_cast<uint8_t>(1u << (j & 7u));
        }

        VecResult r;
        r.data = out; r.type = DRAKEN_BOOL; r.validity_embedded = 0u; r.ts_unit = 0xFFu;
        kernel_preserve_shape(r, v);
        if (any_bad) kernel_null_bad_rows(r, v, bad.data());
        return r;
    });
}

// ---------------------------------------------------------------------------
// STRING → DATE32
//
// Accepts ISO 8601 date strings with "-" separators: "YYYY-MM-DD", "YYYY-M-D",
// "YYYY-MM-D", "YYYY-M-DD".  All such strings fit inline (≤ 12 bytes) so the
// arena pointer is never chased.  Raises ValueError on any malformed row.
//
// Days-since-epoch uses Howard Hinnant's civil_to_days formula, which is exact
// for all proleptic Gregorian dates and has no UB for the year range [0, 9999].
// ---------------------------------------------------------------------------
static inline int32_t civil_to_days(int y, int m, int d) noexcept {
    y -= (m <= 2);
    const int era = (y >= 0 ? y : y - 399) / 400;
    const unsigned yoe = static_cast<unsigned>(y - era * 400);
    const unsigned doy = (153u * static_cast<unsigned>(m + (m > 2 ? -3 : 9)) + 2u) / 5u
                         + static_cast<unsigned>(d) - 1u;
    const unsigned doe = yoe * 365u + yoe / 4u - yoe / 100u + doy;
    return era * 146097 + static_cast<int>(doe) - 719468;
}

// Returns INT32_MIN on any parse error.
static inline int32_t parse_iso_date(const uint8_t* s, uint32_t len) noexcept {
    uint32_t k = 0;
    int year = 0, month = 0, day = 0;

    while (k < len && s[k] != '-') {
        if (s[k] < '0' || s[k] > '9') return INT32_MIN;
        year = year * 10 + (s[k] - '0');
        ++k;
    }
    if (k >= len || s[k] != '-') return INT32_MIN;
    ++k;

    while (k < len && s[k] != '-') {
        if (s[k] < '0' || s[k] > '9') return INT32_MIN;
        month = month * 10 + (s[k] - '0');
        ++k;
    }
    if (k >= len || s[k] != '-') return INT32_MIN;
    ++k;

    while (k < len) {
        if (s[k] < '0' || s[k] > '9') return INT32_MIN;
        day = day * 10 + (s[k] - '0');
        ++k;
    }

    if (year < 1 || month < 1 || month > 12 || day < 1 || day > 31)
        return INT32_MIN;
    return civil_to_days(year, month, day);
}

VecResult draken_cast_string_to_date32(void* ctx, const DrakenVector* v) {
    DRAKEN_KERNEL_TRY({
        if (!v) return draken_error_sentinel("Input vector is null");
        if (v->type != DRAKEN_VARCHAR && v->type != DRAKEN_NVARCHAR && v->type != DRAKEN_VARBINARY)
            return draken_error_sentinel_fmt("cast string->date32: expected string, got %d", v->type);

        const auto* c = static_cast<const format_ctx*>(ctx);
        const bool use_fmt = c != nullptr && c->fmt_len > 0;
        std::vector<SqlToken> prog;
        if (use_fmt) {
            const std::string fmt(format_ctx_fmt(c), static_cast<size_t>(c->fmt_len));
            size_t max_len = 0;
            const char* bad_run = nullptr;
            uint32_t bad_run_len = 0;
            if (!sql_compile(fmt.c_str(), fmt.size(), &prog, &max_len, &bad_run, &bad_run_len))
                return draken_error_sentinel_fmt(
                    "CAST ... FORMAT: unrecognized format token '%.*s'",
                    (int)(bad_run_len < 16u ? bad_run_len : 16u), bad_run);
        }

        // Compression-aware with liveness (see string->int64).
        const DrakenStringArena* sa = static_cast<const DrakenStringArena*>(v->data);
        const uint32_t k = v->data_length;
        const uint32_t n = v->length;
        int32_t* out = static_cast<int32_t*>(draken_malloc((k > 0u ? k : 1u) * sizeof(int32_t)));
        if (!out) return draken_error_sentinel("Allocation failed");

        std::vector<uint8_t> live(k > 0u ? k : 1u, 0u);
        for (uint32_t i = 0u; i < n; ++i)
            if (!kernel_row_is_null(v, i)) live[v->selection[i]] = 1u;

        // TRY_CAST rides format_ctx.safe here, not binary_op_ctx — this kernel
        // needs the format pattern, so it takes that ctx.
        const bool is_safe = (c != nullptr && c->safe != 0u);
        std::vector<uint8_t> bad(k > 0u ? k : 1u, 0u);
        bool any_bad = false;

        for (uint32_t j = 0u; j < k; ++j) {
            if (!live[j]) { out[j] = 0; continue; }
            const DrakenStringSlot* slot = &sa->slots[j];
            const uint8_t* s   = str_data(slot, sa ? sa->arena : nullptr);
            const uint32_t len = str_length(slot);

            int32_t days;
            if (use_fmt) {
                int year;
                int month;
                int day;
                int hour;
                int minute;
                int second;
                int usec;
                if (!sql_parse_exec(prog, reinterpret_cast<const char*>(s), len,
                                     &year, &month, &day, &hour, &minute, &second, &usec)) {
                    if (!is_safe) {
                        draken_free(out);
                        return draken_error_sentinel_fmt(
                            "Cannot cast string to DATE: got %.*s",
                            (int)(len < 20u ? len : 20u), s);
                    }
                    out[j] = 0; bad[j] = 1u; any_bad = true; continue;
                }
                days = civil_to_days(year, month, day);
            } else {
                days = parse_iso_date(s, len);
                if (days == INT32_MIN) {
                    if (!is_safe) {
                        draken_free(out);
                        return draken_error_sentinel_fmt(
                            "Cannot cast string to DATE: expected YYYY-MM-DD, got %.*s",
                            (int)(len < 20u ? len : 20u), s);
                    }
                    out[j] = 0; bad[j] = 1u; any_bad = true; continue;
                }
            }
            out[j] = days;
        }

        VecResult r;
        r.data = out; r.type = DRAKEN_DATE32; r.validity_embedded = 0u; r.ts_unit = 0xFFu;
        kernel_preserve_shape(r, v);
        if (any_bad) kernel_null_bad_rows(r, v, bad.data());
        return r;
    });
}

// CAST(<string> AS IPV4) — dotted-decimal text -> the uint32 the address IS.
//
// The result is DRAKEN_UINT32 with NO descriptor set here: a VecResult has no
// channel for LogicalKind::IPV4, and it does not need one. The IPv4-ness comes
// from the BOUND OUTPUT TYPE via add_expr_project's `logical` tuple, which is
// re-attached to the output column's owner (engine.hpp). Same mechanism the
// parquet scan uses for a catalog-declared IPv4 column.
//
// Parsing is delegated to draken::ipv4::parse, which is deliberately strict —
// no inet_aton shorthand, no leading zeros. An unparseable address is a hard
// error, per the ticket: silently yielding NULL or 0 would turn a typo in an
// ACL into a rule that matches 0.0.0.0.
VecResult draken_cast_string_to_ipv4(void* ctx, const DrakenVector* v) {
    DRAKEN_KERNEL_TRY({
        if (!v) return draken_error_sentinel("Input vector is null");
        if (v->type != DRAKEN_VARCHAR && v->type != DRAKEN_NVARCHAR
                && v->type != DRAKEN_VARBINARY)
            return draken_error_sentinel_fmt(
                "cast string->ipv4: expected string, got %d", v->type);

        // Compression-aware with liveness: convert each PHYSICAL slot once and
        // keep the selection, so a dictionary-encoded address column parses K
        // distinct values rather than N rows. Dead slots are never parsed — a
        // dictionary entry no longer referenced by any live row must not be able
        // to fail the cast for rows that do not use it.
        const DrakenStringArena* sa = static_cast<const DrakenStringArena*>(v->data);
        const uint32_t k = v->data_length;
        const uint32_t n = v->length;
        uint32_t* out = static_cast<uint32_t*>(
            draken_malloc((k > 0u ? k : 1u) * sizeof(uint32_t)));
        if (!out) return draken_error_sentinel("Allocation failed");

        std::vector<uint8_t> live(k > 0u ? k : 1u, 0u);
        for (uint32_t i = 0u; i < n; ++i)
            if (!kernel_row_is_null(v, i)) live[v->selection[i]] = 1u;

        const bool is_safe = kernel_cast_is_safe(ctx);
        std::vector<uint8_t> bad(k > 0u ? k : 1u, 0u);
        bool any_bad = false;

        for (uint32_t j = 0u; j < k; ++j) {
            if (!live[j]) { out[j] = 0u; continue; }
            const DrakenStringSlot* slot = &sa->slots[j];
            const uint8_t* s   = str_data(slot, sa ? sa->arena : nullptr);
            const uint32_t len = str_length(slot);
            uint32_t addr = 0u;
            if (!draken::ipv4::parse(s, len, &addr)) {
                // Plain CAST still refuses: a typo in an ACL silently becoming
                // 0.0.0.0 is the failure this kernel was written to prevent.
                // TRY_CAST is how a caller opts into NULLing those rows instead.
                if (!is_safe) {
                    draken_free(out);
                    return draken_error_sentinel_fmt(
                        "Cannot cast string to IPV4: expected A.B.C.D, got %.*s",
                        (int)(len < 32u ? len : 32u), s);
                }
                out[j] = 0u; bad[j] = 1u; any_bad = true; continue;
            }
            out[j] = addr;
        }

        VecResult r;
        r.data = out; r.type = DRAKEN_UINT32; r.validity_embedded = 0u; r.ts_unit = 0xFFu;
        kernel_preserve_shape(r, v);
        if (any_bad) kernel_null_bad_rows(r, v, bad.data());
        return r;
    });
}

// CAST(<ipv4> AS VARCHAR) — the uint32 an address IS -> dotted-decimal text.
//
// The INVERSE of draken_cast_string_to_ipv4, and the reason it lives beside it:
// both directions must route through draken/core/ipv4.h so parse strictness and
// render form cannot drift apart. Rendering is delegated to draken::ipv4::format,
// the same writer interop/value_format.hpp's fmt_ipv4 uses for the text writers
// and to_pylist — there is exactly one dotted-decimal writer in the codebase.
//
// A DrakenVector carries no descriptor, so this kernel CANNOT tell an IPv4
// column from a plain unsigned one — both are DRAKEN_UINT32. The discriminant is
// the BOUND SOURCE ColumnType's LogicalKind, applied at bind time in
// opteryx/expression/casts.pyx: a descriptor-less UINT32 routes to
// draken_cast_uint_to_string and renders '3232235777'. Picking the wrong kernel
// there is a silent wrong-answer bug, which is why neither name is reachable
// from the physical type alone.
//
// Compression-aware: renders the data_length PHYSICAL values (1 for a constant,
// K for a dict, length for dense) and carries the input's selection + validity
// through, so a dictionary-encoded address column formats K values, not N rows.
// Unlike the parse direction there is no liveness pass — formatting cannot fail,
// so a dead dictionary slot cannot poison rows that do not reference it; it
// costs at most MAX_TEXT_LENGTH bytes of work. Two passes over the K values:
// size the arena, then fill ("255.255.255.255" is 15 bytes, past STR_INLINE_MAX).
VecResult draken_cast_ipv4_to_string(void* ctx, const DrakenVector* v) {
    DRAKEN_KERNEL_TRY({
        if (!v) return draken_error_sentinel("Input vector is null");
        if (v->type != DRAKEN_UINT32)
            return draken_error_sentinel_fmt(
                "cast ipv4->string: expected UINT32, got %d", v->type);

        const uint32_t k = v->data_length;
        const uint32_t* src = static_cast<const uint32_t*>(v->data);

        char tmp[draken::ipv4::MAX_TEXT_LENGTH];
        size_t total_extern = 0u;
        for (uint32_t j = 0u; j < k; ++j) {
            const uint32_t len = draken::ipv4::format(src[j], tmp);
            if (len > STR_INLINE_MAX) total_extern += static_cast<size_t>(len);
        }

        DrakenStringSlot* slots;
        uint8_t* arena;
        uint8_t* vunused;
        uint8_t* block = vecresult_string_block_alloc(k, total_extern, 0, &slots, &arena, &vunused);
        if (!block) return draken_error_sentinel("Allocation failed");
        (void)vunused;

        size_t arena_used = 0u;
        for (uint32_t j = 0u; j < k; ++j) {
            const uint32_t len = draken::ipv4::format(src[j], tmp);
            const uint8_t* bytes = reinterpret_cast<const uint8_t*>(tmp);
            if (len > STR_INLINE_MAX) {
                const uint32_t off = static_cast<uint32_t>(arena_used);
                std::memcpy(arena + off, tmp, static_cast<size_t>(len));
                draken_build_string_slot(&slots[j], bytes, len, off);
                arena_used += static_cast<size_t>(len);
            } else {
                draken_build_string_slot(&slots[j], bytes, len, 0u);
            }
        }

        VecResult r = vecresult_from_string_block(block, k, total_extern, 0, DRAKEN_VARCHAR);
        kernel_preserve_shape(r, v);
        return r;
    });
}

// VARBINARY twin — identical bytes, different tag. Same rationale as the
// DRAKEN_CAST_TO_BLOB family in cast_numeric.cpp: routing a BLOB target at the
// `_to_string` kernel would silently hand back a VARCHAR-tagged result.
VecResult draken_cast_ipv4_to_blob(void* ctx, const DrakenVector* v) {
    VecResult r = draken_cast_ipv4_to_string(ctx, v);
    if (r.data != nullptr) r.type = DRAKEN_VARBINARY;
    return r;
}

// String-family retag core: VARCHAR/NVARCHAR/VARBINARY/VARIANT -> `target`
// (VARCHAR or VARBINARY only — casting TO NVARCHAR needs UTF-8 validation and is
// not this kernel's job). All four share the exact DrakenStringArena layout
// (buffers.h §11 / draken_type_is_string_storage) — VARIANT holds JSON text in
// that same layout — so this is a plain byte copy of the K physical slots +
// arena bytes into a fresh block with the new type tag: no reformatting, no
// validation. For a VARIANT source the result is the raw JSON text verbatim (a
// JSON string keeps its quotes) — matches `x::text` on Postgres jsonb, which is
// a different, not-interchangeable operation from `->>` (unwraps a JSON string
// scalar and drops the quotes).
static VecResult string_retag_core(const DrakenVector* v, DrakenType target) {
    if (!v) return draken_error_sentinel("Input vector is null");
    if (v->type != DRAKEN_VARCHAR && v->type != DRAKEN_NVARCHAR
            && v->type != DRAKEN_VARBINARY && v->type != DRAKEN_VARIANT)
        return draken_error_sentinel_fmt("cast string retag: expected string, got %d", v->type);

    const DrakenStringArena* sa = static_cast<const DrakenStringArena*>(v->data);
    const uint32_t k = v->data_length;
    const size_t arena_len = sa->arena_used;

    DrakenStringSlot* slots;
    uint8_t* arena;
    uint8_t* vunused;
    uint8_t* block = vecresult_string_block_alloc(k, arena_len, 0, &slots, &arena, &vunused);
    if (!block) return draken_error_sentinel("Allocation failed");
    (void)vunused;

    if (k > 0u) std::memcpy(slots, sa->slots, static_cast<size_t>(k) * sizeof(DrakenStringSlot));
    if (arena_len > 0u && sa->arena) std::memcpy(arena, sa->arena, arena_len);

    VecResult r = vecresult_from_string_block(block, k, arena_len, 0, target);
    kernel_preserve_shape(r, v);
    return r;
}

VecResult draken_cast_string_to_varchar(void* ctx, const DrakenVector* v) {
    DRAKEN_KERNEL_TRY({ return string_retag_core(v, DRAKEN_VARCHAR); });
}

VecResult draken_cast_string_to_blob(void* ctx, const DrakenVector* v) {
    DRAKEN_KERNEL_TRY({ return string_retag_core(v, DRAKEN_VARBINARY); });
}

// VARCHAR/VARBINARY/VARIANT -> NVARCHAR: validate UTF-8 per row, then retag.
// NVARCHAR source is already-valid UTF-8 (invariant of the type), so it skips
// straight to string_retag_core — no re-validation. VARIANT source is JSON text,
// which the JSON spec requires to be valid Unicode, so it carries the same
// already-valid guarantee and also skips straight to the retag. VARCHAR/
// VARBINARY sources are validated: only the K PHYSICAL values referenced by a
// non-null logical row are checked (a dict value used solely by null rows must
// not raise). This is a plain CAST kernel — RAISES on the first invalid row;
// TRY_CAST never reaches here (`_c_native_cast` returns None for safe=True and
// keeps the Python closure, matching every other cast kernel's TRY_CAST posture).
VecResult draken_cast_string_to_nvarchar(void* ctx, const DrakenVector* v) {
    DRAKEN_KERNEL_TRY({
        if (!v) return draken_error_sentinel("Input vector is null");
        if (v->type != DRAKEN_VARCHAR && v->type != DRAKEN_NVARCHAR
                && v->type != DRAKEN_VARBINARY && v->type != DRAKEN_VARIANT)
            return draken_error_sentinel_fmt("cast string->nvarchar: expected string, got %d", v->type);
        if (v->type == DRAKEN_NVARCHAR || v->type == DRAKEN_VARIANT)
            return string_retag_core(v, DRAKEN_NVARCHAR);

        const DrakenStringArena* sa = static_cast<const DrakenStringArena*>(v->data);
        const uint32_t k = v->data_length;
        const uint32_t n = v->length;

        std::vector<uint8_t> live(k > 0u ? k : 1u, 0u);
        for (uint32_t i = 0u; i < n; ++i)
            if (!kernel_row_is_null(v, i)) live[v->selection[i]] = 1u;

        for (uint32_t j = 0u; j < k; ++j) {
            if (!live[j]) continue;
            const DrakenStringSlot* slot = &sa->slots[j];
            const uint32_t len = str_length(slot);
            const uint8_t* bytes = str_data(slot, sa->arena);
            if (len != 0u &&
                    utf8nvalid(reinterpret_cast<const utf8_int8_t*>(bytes), len) != nullptr) {
                return draken_error_sentinel("CAST to NVARCHAR: input is not valid UTF-8");
            }
        }
        return string_retag_core(v, DRAKEN_NVARCHAR);
    });
}

}  // extern "C"
