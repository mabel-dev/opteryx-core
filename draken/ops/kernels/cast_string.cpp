#include "ops/kernels/cast_kernels.h"
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
                out[j] = 0.0; bad[j] = 1u; any_bad = true;
            } else {
                out[j] = draken::ops::fp_canon(value);
            }
        }

        VecResult r;
        r.data = out; r.type = DRAKEN_FLOAT64; r.validity_embedded = 0u; r.ts_unit = 0xFFu;
        kernel_preserve_shape(r, v);  // r.validity = input copy (or null)

        if (any_bad) {
            if (!r.validity) {   // input was all-valid — materialise an all-valid bitmap
                const uint32_t bmn    = (n + 7u) >> 3;
                const uint32_t padded = (bmn + 7u) & ~7u;
                const size_t   vbytes = padded > 0u ? padded : 8u;
                uint8_t* nv = static_cast<uint8_t*>(draken_malloc(vbytes));
                if (!nv) { draken_free(out); return draken_error_sentinel("Allocation failed"); }
                std::memset(nv, 0xFF, vbytes);
                r.validity = nv;
            }
            for (uint32_t i = 0u; i < n; ++i)
                if (bad[v->selection[i]])
                    r.validity[i >> 3] &= static_cast<uint8_t>(~(1u << (i & 7u)));
        }
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

        for (uint32_t j = 0u; j < k; ++j) {
            if (!live[j]) { out[j] = 0; continue; }
            const DrakenStringSlot* slot = &sa->slots[j];
            const uint8_t* sdata = str_data(slot, sa->arena);
            const uint32_t slen  = str_length(slot);

            int64_t value = 0;
            int64_t sign = 1;
            uint32_t p = 0;
            if (slen > 0 && sdata[0] == '-') { sign = -1; p = 1; }
            for (; p < slen; ++p) {
                const uint8_t c = sdata[p];
                if (c < '0' || c > '9') {
                    draken_free(out);
                    return draken_error_sentinel("Invalid digit in integer literal");
                }
                value = value * 10 + (c - '0');
            }
            out[j] = sign * value;
        }

        VecResult r;
        r.data = out; r.type = DRAKEN_INT64; r.validity_embedded = 0u; r.ts_unit = 0xFFu;
        kernel_preserve_shape(r, v);
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
#define DRAKEN_CAST_STRING_TO_UINT(fn_name, narrow_fn)                                    \
VecResult fn_name(void* ctx, const DrakenVector* v) {                                     \
    DRAKEN_KERNEL_TRY({                                                                  \
        if (!v) return draken_error_sentinel("Input vector is null");                    \
        VecResult tmp = draken_cast_string_to_int64(ctx, v);                             \
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

DRAKEN_CAST_STRING_TO_UINT(draken_cast_string_to_uint8,  draken_cast_integer_to_uint8)
DRAKEN_CAST_STRING_TO_UINT(draken_cast_string_to_uint16, draken_cast_integer_to_uint16)
DRAKEN_CAST_STRING_TO_UINT(draken_cast_string_to_uint32, draken_cast_integer_to_uint32)

#undef DRAKEN_CAST_STRING_TO_UINT

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

        for (uint32_t j = 0u; j < k; ++j) {
            if (!live[j]) { out[j] = 0; continue; }
            const DrakenStringSlot* slot = &sa->slots[j];
            const uint8_t* sdata = str_data(slot, sa->arena);
            const uint32_t slen  = str_length(slot);

            if (slen > 0 && sdata[0] == '-') {
                draken_free(out);
                return draken_error_sentinel(
                    "cast string->uint64: negative value out of range for uint64_t");
            }
            uint64_t value = 0u;
            for (uint32_t p = 0u; p < slen; ++p) {
                const uint8_t c = sdata[p];
                if (c < '0' || c > '9') {
                    draken_free(out);
                    return draken_error_sentinel("Invalid digit in integer literal");
                }
                const uint64_t digit = static_cast<uint64_t>(c - '0');
                // Overflow check BEFORE the multiply/add: value*10+digit must not
                // exceed UINT64_MAX. (UINT64_MAX - digit) / 10 is the largest
                // `value` for which the next digit still fits.
                if (value > (0xFFFFFFFFFFFFFFFFull - digit) / 10ull) {
                    draken_free(out);
                    return draken_error_sentinel("cast string->uint64: value out of range for uint64_t");
                }
                value = value * 10ull + digit;
            }
            out[j] = value;
        }

        VecResult r;
        r.data = out; r.type = DRAKEN_UINT64; r.validity_embedded = 0u; r.ts_unit = 0xFFu;
        kernel_preserve_shape(r, v);
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
                draken_free(out);
                return draken_error_sentinel(
                    "Cannot cast string to BOOL: expected true/false/1/0/yes/no/on/off");
            }
            if (truth) out[j >> 3u] |= static_cast<uint8_t>(1u << (j & 7u));
        }

        VecResult r;
        r.data = out; r.type = DRAKEN_BOOL; r.validity_embedded = 0u; r.ts_unit = 0xFFu;
        kernel_preserve_shape(r, v);
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

        for (uint32_t j = 0u; j < k; ++j) {
            if (!live[j]) { out[j] = 0; continue; }
            const DrakenStringSlot* slot = &sa->slots[j];
            const uint8_t* s   = str_data(slot, sa ? sa->arena : nullptr);
            const uint32_t len = str_length(slot);

            int32_t days;
            if (use_fmt) {
                int year, month, day, hour, minute, second, usec;
                if (!sql_parse_exec(prog, reinterpret_cast<const char*>(s), len,
                                     &year, &month, &day, &hour, &minute, &second, &usec)) {
                    draken_free(out);
                    return draken_error_sentinel_fmt(
                        "Cannot cast string to DATE: got %.*s",
                        (int)(len < 20u ? len : 20u), s);
                }
                days = civil_to_days(year, month, day);
            } else {
                days = parse_iso_date(s, len);
                if (days == INT32_MIN) {
                    draken_free(out);
                    return draken_error_sentinel_fmt(
                        "Cannot cast string to DATE: expected YYYY-MM-DD, got %.*s",
                        (int)(len < 20u ? len : 20u), s);
                }
            }
            out[j] = days;
        }

        VecResult r;
        r.data = out; r.type = DRAKEN_DATE32; r.validity_embedded = 0u; r.ts_unit = 0xFFu;
        kernel_preserve_shape(r, v);
        return r;
    });
}

// String-family retag core: VARCHAR/NVARCHAR/VARBINARY -> `target` (VARCHAR or
// VARBINARY only — casting TO NVARCHAR needs UTF-8 validation and is not this
// kernel's job). All three share the exact DrakenStringArena layout (buffers.h
// §11), so this is a plain byte copy of the K physical slots + arena bytes into
// a fresh block with the new type tag — no reformatting, no validation.
static VecResult string_retag_core(const DrakenVector* v, DrakenType target) {
    if (!v) return draken_error_sentinel("Input vector is null");
    if (v->type != DRAKEN_VARCHAR && v->type != DRAKEN_NVARCHAR && v->type != DRAKEN_VARBINARY)
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

// VARCHAR/VARBINARY -> NVARCHAR: validate UTF-8 per row, then retag. NVARCHAR
// source is already-valid UTF-8 (invariant of the type), so it skips straight
// to string_retag_core — no re-validation. VARCHAR/VARBINARY sources are
// validated: only the K PHYSICAL values referenced by a non-null logical row
// are checked (a dict value used solely by null rows must not raise). This is
// a plain CAST kernel — RAISES on the first invalid row; TRY_CAST never
// reaches here (`_c_native_cast` returns None for safe=True and keeps the
// Python closure, matching every other cast kernel's TRY_CAST posture).
VecResult draken_cast_string_to_nvarchar(void* ctx, const DrakenVector* v) {
    DRAKEN_KERNEL_TRY({
        if (!v) return draken_error_sentinel("Input vector is null");
        if (v->type != DRAKEN_VARCHAR && v->type != DRAKEN_NVARCHAR && v->type != DRAKEN_VARBINARY)
            return draken_error_sentinel_fmt("cast string->nvarchar: expected string, got %d", v->type);
        if (v->type == DRAKEN_NVARCHAR)
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
