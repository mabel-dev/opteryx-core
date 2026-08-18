#include "ops/kernels/cast_kernels.h"
#include "core/ipv4.h"
#include "core/iso_datetime.h"
#include "core/decimal_text.h"
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

            int64_t sign = 1;
            uint32_t p = 0;
            if (slen > 0 && sdata[0] == '-') { sign = -1; p = 1; }
            // Accumulate the magnitude unsigned against the exact signed bound —
            // a 20-digit input must fail loud (or NULL under TRY_CAST), never
            // silently wrap (fail-loud contract, cast_numeric.cpp E33 note).
            const uint64_t limit = (sign < 0) ? 9223372036854775808ULL
                                              : 9223372036854775807ULL;
            uint64_t mag = 0u;
            bool malformed = false;
            bool overflow = false;
            for (; p < slen; ++p) {
                const uint8_t c = sdata[p];
                if (c < '0' || c > '9') { malformed = true; break; }
                const uint64_t d = (uint64_t)(c - '0');
                if (mag > (limit - d) / 10u) { overflow = true; break; }
                mag = mag * 10u + d;
            }
            if (malformed || overflow || slen == 0u || (slen == 1u && sign < 0)) {
                if (!is_safe) {
                    draken_free(out);
                    if (overflow)
                        return draken_error_sentinel_fmt(
                            "Integer literal out of range for INT64: '%.*s'",
                            (int)(slen > 64u ? 64u : slen), (const char*)sdata);
                    return draken_error_sentinel("Invalid digit in integer literal");
                }
                out[j] = 0; bad[j] = 1u; any_bad = true; continue;
            }
            out[j] = (sign < 0) ? (int64_t)(0u - mag) : (int64_t)mag;
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
// Days-since-epoch and the ISO parse both live in core/iso_datetime.h — the
// single source shared with the string->TIMESTAMP kernel (which carried its own
// copy of civil_to_days) and with rugo's declared-schema readers.
// ---------------------------------------------------------------------------
using draken::iso_datetime::parse_iso_date;
using draken::iso_datetime::civil_to_days;

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
                days = static_cast<int32_t>(civil_to_days(year, month, day));
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

        // A dense vector with no validity mask references every physical slot
        // through selection[i] == i, so the liveness pass would mark all k slots
        // live and can be skipped along with its zeroed allocation. This is a
        // provable shortcut, NOT a shape-dependent answer: both paths treat
        // exactly the same set of slots as live, so a missing layout hint (flags
        // == 0 means "don't know") costs a pass and never a wrong result.
        const bool all_live = (v->validity == nullptr)
                           && ((v->flags & DRAKEN_SEL_IDENTITY) != 0u)
                           && (k == n);
        std::vector<uint8_t> live;
        if (!all_live) {
            live.assign(k > 0u ? k : 1u, 0u);
            for (uint32_t i = 0u; i < n; ++i)
                if (!kernel_row_is_null(v, i)) live[v->selection[i]] = 1u;
        }
        const uint8_t* live_map = all_live ? nullptr : live.data();

        // `bad` records rows to NULL afterwards, which only TRY_CAST ever does —
        // a plain CAST returns on the first unparseable value, so it can never
        // record one. Allocating it unconditionally zeroed k bytes that the
        // common path never reads.
        const bool is_safe = kernel_cast_is_safe(ctx);
        std::vector<uint8_t> bad;
        if (is_safe) bad.assign(k > 0u ? k : 1u, 0u);
        bool any_bad = false;

        for (uint32_t j = 0u; j < k; ++j) {
            if (live_map != nullptr && !live_map[j]) { out[j] = 0u; continue; }
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
// The sizing pass calls ipv4::text_length rather than rendering into a scratch
// buffer and measuring — the width of an address is four threshold tests, so
// there is no reason to format every value twice just to learn how big the
// arena must be.
VecResult draken_cast_ipv4_to_string(void* ctx, const DrakenVector* v) {
    DRAKEN_KERNEL_TRY({
        if (!v) return draken_error_sentinel("Input vector is null");
        if (v->type != DRAKEN_UINT32)
            return draken_error_sentinel_fmt(
                "cast ipv4->string: expected UINT32, got %d", v->type);

        const uint32_t k = v->data_length;
        const uint32_t* src = static_cast<const uint32_t*>(v->data);

        size_t total_extern = 0u;
        for (uint32_t j = 0u; j < k; ++j) {
            const uint32_t len = draken::ipv4::text_length(src[j]);
            if (len > STR_INLINE_MAX) total_extern += static_cast<size_t>(len);
        }

        char tmp[draken::ipv4::MAX_TEXT_LENGTH];

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

// --- STRING → DECIMAL ----------------------------------------------------------
//
// EXACT text → fixed-point. Deliberately NOT composed from
// draken_cast_string_to_float64 the way the uint/narrow-int/float32 targets above
// compose from their parsers: a double cannot hold 18 significant digits, so
// routing through one would silently corrupt the low digits of exactly the values
// DECIMAL exists to keep. The parse is therefore its own integer accumulation.
//
// Accepted syntax mirrors the literal path (`decimal.Decimal(text.strip())` in
// _parse_decimal / _build_decimal_closure, casts.pyx) so a cast over a COLUMN and
// the same cast over a LITERAL cannot disagree:
//   [ws] [+|-] digits [ . digits] [ (e|E) [+|-] digits ] [ws]
// with at least one mantissa digit ('1.' and '.5' are both accepted). Infinity and
// NaN are rejected — neither has a fixed-point representation, and the literal path
// rejects them too (at quantize).
//
// Value policy is the one every → DECIMAL kernel in this tree enforces: a declared
// type is a contract, not a hint.
//   - fractional digits beyond the declared scale FAIL LOUD when they would be
//     dropped; trailing zeros re-pad silently ('1.250' → DECIMAL(10,2) is 1.25).
//   - a magnitude outside the declared precision FAILS LOUD, never wraps.
//   - malformed text FAILS LOUD.
// Under TRY_CAST each of those maps the row to NULL instead (kernel_cast_is_safe).
//
// LIMIT, deliberate and loud: a mantissa carrying more than 38 significant digits
// is refused as an overflow even when a negative exponent would bring it back into
// range ('1e39' * 1e-30). 38 digits IS the DECIMAL precision ceiling, so no
// representable target loses reachable values; the alternative is arbitrary-
// precision accumulation for inputs no DECIMAL column can store.
//
// SHAPE-PRESERVING (parse the data_length PHYSICAL values, keep the input's
// selection), matching every other string→numeric kernel in this file rather than
// the dense → DECIMAL kernels: parsing is the expensive step here, and a
// dict-encoded string column would otherwise re-parse every repeat. Liveness-masked
// like draken_cast_string_to_int64, so a dictionary value referenced only by NULL
// rows can never raise on text SQL does not read.
// The parse itself lives in core/decimal_text.h so a declared-schema reader
// (rugo) applies byte-for-byte the same syntax and value policy as this CAST.
// Its status codes are file-scope there for the same reason they were file-scope
// here: an enum brace list inside DRAKEN_KERNEL_TRY would have its commas eaten
// as macro argument separators.

VecResult draken_cast_string_to_decimal(void* ctx, const DrakenVector* v) {
    DRAKEN_KERNEL_TRY({
        if (!v) return draken_error_sentinel("Input vector is null");
        if (v->type != DRAKEN_VARCHAR && v->type != DRAKEN_NVARCHAR && v->type != DRAKEN_VARBINARY)
            return draken_error_sentinel_fmt("cast string->decimal: expected string, got %d", v->type);
        if (!ctx) return draken_error_sentinel("cast string->decimal: missing ctx (precision/scale)");
        const auto* c = static_cast<const binary_op_ctx*>(ctx);
        if (c->result_precision == 0u || c->result_precision > 38u)
            return draken_error_sentinel_fmt("cast string->decimal: bad target precision %d",
                                             (int)c->result_precision);

        const DrakenStringArena* sa = static_cast<const DrakenStringArena*>(v->data);
        const uint32_t k = v->data_length;
        const uint32_t n = v->length;
        const bool dst128 = c->result_precision > 18u;
        const size_t es = dst128 ? 16u : 8u;
        uint8_t* out = static_cast<uint8_t*>(draken_malloc((k > 0u ? k : 1u) * es));
        if (!out) return draken_error_sentinel("Allocation failed");

        std::vector<uint8_t> live(k > 0u ? k : 1u, 0u);
        for (uint32_t i = 0u; i < n; ++i)
            if (!kernel_row_is_null(v, i)) live[v->selection[i]] = 1u;

        const bool is_safe = kernel_cast_is_safe(ctx);
        std::vector<uint8_t> bad(k > 0u ? k : 1u, 0u);
        bool any_bad = false;

        for (uint32_t j = 0u; j < k; ++j) {
            uint8_t* dst = out + static_cast<size_t>(j) * es;
            if (!live[j]) { std::memset(dst, 0, es); continue; }

            const DrakenStringSlot* slot = &sa->slots[j];
            const uint8_t* sdata = str_data(slot, sa->arena);
            const uint32_t slen  = str_length(slot);

            __int128 unscaled = 0;
            const int status = draken::decimal_text::parse(
                sdata, slen, c->result_precision, c->result_scale, &unscaled);

            if (status != draken::decimal_text::OK) {
                if (!is_safe) {
                    draken_free(out);
                    if (status == draken::decimal_text::OVERFLOW_)
                        return draken_error_sentinel_fmt(
                            "cast string->decimal: value overflows DECIMAL(%d, %d)",
                            (int)c->result_precision, (int)c->result_scale);
                    if (status == draken::decimal_text::SCALE)
                        return draken_error_sentinel_fmt(
                            "cast string->decimal: value has more decimal places than "
                            "the declared scale %d", (int)c->result_scale);
                    return draken_error_sentinel("Invalid number in string literal");
                }
                std::memset(dst, 0, es); bad[j] = 1u; any_bad = true; continue;
            }

            if (dst128) {
                std::memcpy(dst, &unscaled, 16u);
            } else {
                int64_t w = static_cast<int64_t>(unscaled);
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
        kernel_preserve_shape(r, v);
        if (any_bad) kernel_null_bad_rows(r, v, bad.data());
        return r;
    });
}

}  // extern "C"
