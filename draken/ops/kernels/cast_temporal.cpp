#include "ops/kernels/cast_kernels.h"
#include "ops/kernels/error_handling.h"
#include "ops/kernels/kernel_context.h"
#include "ops/kernels/result_helpers.h"
#include "ops/sql_temporal_format.h"
#include "ops/temporal_arith.h"
#include "core/alloc.h"
#include "core/vector_alloc.h"
#include "core/string_slot.h"
#include "core/interval_slot.h"
#include "logical_type.h"
#include <cstring>
#include <vector>

/**
 * Cast kernels: temporal conversions (Phase 9c).
 *
 * Compute extracted from opteryx/compiled/nanobind/vector_casts.cpp.
 * int64->timestamp carries its unit via VecResult.ts_unit (see vec_result.h);
 * vecresult_to_owner interns it into the VectorOwner's LogicalType.
 */

// Shared "compile the FORMAT pattern, walk it per physical row, build a
// variable-width string result" helper for the FORMAT-bearing branch of the
// DATE32/TIMESTAMP64/INTERVAL -> VARCHAR kernels below. Mirrors draken_date_format's
// arena-growth Guard pattern (function_temporal.cpp) — not reused directly
// since that kernel speaks strftime tokens, this speaks SQL tokens. A template
// (not extern "C" — templates cannot have C linkage), called only from the
// extern "C" kernels below.
template <typename RowFieldsFn>
static VecResult sql_format_to_string(const DrakenVector* v, const format_ctx* c, RowFieldsFn row_fields) {
    const uint32_t k = v->data_length;
    const std::string fmt(format_ctx_fmt(c), static_cast<size_t>(c->fmt_len));

    std::vector<SqlToken> prog;
    size_t max_row_len = 0;
    const char* bad_run = nullptr;
    uint32_t bad_run_len = 0;
    if (!sql_compile(fmt.c_str(), fmt.size(), &prog, &max_row_len, &bad_run, &bad_run_len))
        return draken_error_sentinel_fmt(
            "CAST ... FORMAT: unrecognized format token '%.*s'",
            (int)(bad_run_len < 16u ? bad_run_len : 16u), bad_run);

    std::vector<char> row_vec(max_row_len);
    char* const row_buf = row_vec.data();

    const size_t slots_sz = (k > 0u ? k : 1u) * sizeof(DrakenStringSlot);
    auto* slots = static_cast<DrakenStringSlot*>(draken_malloc(slots_sz));
    if (!slots) return draken_error_sentinel("Allocation failed");
    std::memset(slots, 0, slots_sz);

    size_t arena_cap = (k > 0u ? static_cast<size_t>(k) * 32u : 32u);
    auto* arena = static_cast<uint8_t*>(draken_malloc(arena_cap));
    if (!arena) { draken_free(slots); return draken_error_sentinel("Allocation failed"); }

    struct Guard {
        DrakenStringSlot* s; uint8_t* a;
        ~Guard() { if (s) draken_free(s); if (a) draken_free(a); }
    } g{slots, arena};

    size_t arena_used = 0u;
    for (uint32_t j = 0u; j < k; ++j) {
        const SqlFields f = row_fields(j);
        char* p = sql_emit(row_buf, prog, f);
        const uint32_t slen = static_cast<uint32_t>(p - row_buf);
        if (slen <= STR_INLINE_MAX) {
            str_init_inline(&slots[j], reinterpret_cast<const uint8_t*>(row_buf), slen);
        } else {
            if (arena_used + slen > arena_cap) {
                arena_cap = (arena_used + slen) * 2u;
                auto* new_arena = static_cast<uint8_t*>(draken_malloc(arena_cap));
                if (!new_arena) throw std::bad_alloc();
                std::memcpy(new_arena, g.a, arena_used);
                draken_free(g.a);
                g.a = new_arena;
            }
            const uint32_t arena_off = static_cast<uint32_t>(arena_used);
            std::memcpy(g.a + arena_off, row_buf, slen);
            draken_build_string_slot(&slots[j], reinterpret_cast<const uint8_t*>(row_buf), slen, arena_off);
            arena_used += slen;
        }
    }

    DrakenStringSlot* out_slots = g.s;
    uint8_t*          out_arena = g.a;
    g.s = nullptr; g.a = nullptr;

    return vecresult_from_string_buffers(out_slots, out_arena, arena_used, nullptr, k, DRAKEN_VARCHAR);
}

extern "C" {

// Gregorian calendar: days-since-epoch (1970-01-01) → (year, month, day).
// Howard Hinnant's algorithm.
static void days_to_ymd(int32_t days, int32_t& y, int32_t& m, int32_t& d) noexcept {
    const int32_t z   = days + 719468;
    const int32_t era = (z >= 0 ? z : z - 146096) / 146097;
    const int32_t doe = z - era * 146097;
    const int32_t yoe = (doe - doe/1460 + doe/36524 - doe/146096) / 365;
    const int32_t yr  = yoe + era * 400;
    const int32_t doy = doe - (365*yoe + yoe/4 - yoe/100);
    const int32_t mp  = (5*doy + 2) / 153;
    d = doy - (153*mp + 2)/5 + 1;
    m = mp < 10 ? mp + 3 : mp - 9;
    y = yr + (m <= 2 ? 1 : 0);
}

VecResult draken_cast_date32_to_int64(void* ctx, const DrakenVector* v) {
    DRAKEN_KERNEL_TRY({
        if (!v) return draken_error_sentinel("Input vector is null");
        if (v->type != DRAKEN_DATE32)
            return draken_error_sentinel_fmt("cast date32->int64: expected DATE32, got %d", v->type);
        const uint32_t k = v->data_length;
        const int32_t* src = static_cast<const int32_t*>(v->data);
        int64_t* out = static_cast<int64_t*>(draken_malloc((k > 0u ? k : 1u) * sizeof(int64_t)));
        if (!out) return draken_error_sentinel("Allocation failed");
        for (uint32_t j = 0u; j < k; ++j) out[j] = static_cast<int64_t>(src[j]);
        VecResult r;
        r.data = out; r.type = DRAKEN_INT64; r.validity_embedded = 0u; r.ts_unit = 0xFFu;
        kernel_preserve_shape(r, v);
        return r;
    });
}

VecResult draken_cast_timestamp_to_int64(void* ctx, const DrakenVector* v) {
    DRAKEN_KERNEL_TRY({
        if (!v) return draken_error_sentinel("Input vector is null");
        if (v->type != DRAKEN_TIMESTAMP64)
            return draken_error_sentinel_fmt("cast timestamp->int64: expected TIMESTAMP64, got %d", v->type);
        const uint32_t k = v->data_length;
        const int64_t* src = static_cast<const int64_t*>(v->data);
        int64_t* out = static_cast<int64_t*>(draken_malloc((k > 0u ? k : 1u) * sizeof(int64_t)));
        if (!out) return draken_error_sentinel("Allocation failed");
        for (uint32_t j = 0u; j < k; ++j) out[j] = src[j];
        VecResult r;
        r.data = out; r.type = DRAKEN_INT64; r.validity_embedded = 0u; r.ts_unit = 0xFFu;
        kernel_preserve_shape(r, v);
        return r;
    });
}

// INT64 → TIMESTAMP64. Unit from cast_timestamp_ctx (0/none=us default).
// ctx codes: 1=ns,2=us,3=ms,4=s,5=days. "days" scales epoch-days → epoch-µs.
VecResult draken_cast_int64_to_timestamp(void* ctx, const DrakenVector* v) {
    DRAKEN_KERNEL_TRY({
        if (!v) return draken_error_sentinel("Input vector is null");
        if (v->type != DRAKEN_INT64)
            return draken_error_sentinel_fmt("cast int64->timestamp: expected INT64, got %d", v->type);

        int unit_code = 2;  // default microseconds
        if (ctx) unit_code = static_cast<const cast_timestamp_ctx*>(ctx)->unit;
        if (unit_code == 0) unit_code = 2;

        TimestampUnit ts_unit;
        bool scale_days = false;
        switch (unit_code) {
            case 1: ts_unit = TimestampUnit::NANOSECONDS;  break;
            case 2: ts_unit = TimestampUnit::MICROSECONDS; break;
            case 3: ts_unit = TimestampUnit::MILLISECONDS; break;
            case 4: ts_unit = TimestampUnit::SECONDS;      break;
            case 5: ts_unit = TimestampUnit::MICROSECONDS; scale_days = true; break;
            default: return draken_error_sentinel_fmt("cast int64->timestamp: bad unit %d", unit_code);
        }

        // DENSE emit (gather through selection, identity-selection output) — a
        // physical-domain (data_length-indexed) shape-preserving emit here
        // caused a silent wrong-answer bug in the sibling DATE32->TIMESTAMP
        // kernel on dict-shaped columns (see draken_cast_date32_to_timestamp).
        const uint32_t n = v->length;
        const int64_t* src = static_cast<const int64_t*>(v->data);
        int64_t* out = static_cast<int64_t*>(draken_malloc((n > 0u ? n : 1u) * sizeof(int64_t)));
        if (!out) return draken_error_sentinel("Allocation failed");
        for (uint32_t j = 0u; j < n; ++j) {
            int64_t raw = src[v->selection[j]];
            out[j] = scale_days ? raw * 86'400'000'000LL : raw;
        }
        VecResult r;
        r.data = out; r.type = DRAKEN_TIMESTAMP64; r.validity_embedded = 0u;
        r.ts_unit = static_cast<uint8_t>(ts_unit);
        r.length = n; r.data_length = n;
        r.selection = draken_identity_sel(n);
        r.owns_selection = false;
        r.validity = kernel_copy_validity(v);
        r.flags = DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION;
        return r;
    });
}

// DATE32 → TIMESTAMP64: midnight of the date, in the target unit. DATE32 is int32
// days-since-epoch; result = days * 86400 * (ticks per second for the unit).
VecResult draken_cast_date32_to_timestamp(void* ctx, const DrakenVector* v) {
    DRAKEN_KERNEL_TRY({
        if (!v) return draken_error_sentinel("Input vector is null");
        if (v->type != DRAKEN_DATE32)
            return draken_error_sentinel_fmt("cast date32->timestamp: expected DATE32, got %d", v->type);
        int unit_code = 2;   // default microseconds
        if (ctx) unit_code = static_cast<const cast_timestamp_ctx*>(ctx)->unit;
        if (unit_code == 0 || unit_code == 5) unit_code = 2;   // 5=days-scaled→us here
        TimestampUnit ts_unit;
        int64_t per_sec;
        switch (unit_code) {
            case 1: ts_unit = TimestampUnit::NANOSECONDS;  per_sec = 1000000000LL; break;
            case 2: ts_unit = TimestampUnit::MICROSECONDS; per_sec = 1000000LL;    break;
            case 3: ts_unit = TimestampUnit::MILLISECONDS; per_sec = 1000LL;       break;
            case 4: ts_unit = TimestampUnit::SECONDS;      per_sec = 1LL;          break;
            default: return draken_error_sentinel_fmt("cast date32->timestamp: bad unit %d", unit_code);
        }
        const int64_t mult = 86400LL * per_sec;
        // DENSE output: gather through selection (uniform contract) and emit an
        // identity-selection result — unambiguous for every downstream consumer.
        const uint32_t n = v->length;
        const int32_t* src = static_cast<const int32_t*>(v->data);
        int64_t* out = static_cast<int64_t*>(draken_malloc((n > 0u ? n : 1u) * sizeof(int64_t)));
        if (!out) return draken_error_sentinel("Allocation failed");
        for (uint32_t j = 0u; j < n; ++j)
            out[j] = static_cast<int64_t>(src[v->selection[j]]) * mult;
        VecResult r;
        r.data = out; r.type = DRAKEN_TIMESTAMP64; r.validity_embedded = 0u;
        r.ts_unit = static_cast<uint8_t>(ts_unit);
        r.length = n; r.data_length = n;
        r.selection = draken_identity_sel(n);
        r.owns_selection = false;
        r.validity = kernel_copy_validity(v);
        r.flags = DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION;
        return r;
    });
}

// DATE32 → VARCHAR: default "YYYY-MM-DD" (10 chars, always inline, fixed-width
// fast path — every output is 10 bytes so the block size is known up front).
// ctx (format_ctx*) may be null (fixed default) or carry a FORMAT pattern
// (variable-width, via sql_format_to_string).
VecResult draken_cast_date_to_string(void* ctx, const DrakenVector* v) {
    DRAKEN_KERNEL_TRY({
        if (!v) return draken_error_sentinel("Input vector is null");
        if (v->type != DRAKEN_DATE32)
            return draken_error_sentinel_fmt("cast date->string: expected DATE32, got %d", v->type);

        const auto* c = static_cast<const format_ctx*>(ctx);
        if (c != nullptr && c->fmt_len > 0) {
            const int32_t* src = static_cast<const int32_t*>(v->data);
            VecResult r = sql_format_to_string(v, c, [&](uint32_t j) {
                return sql_calendar_fields(static_cast<int64_t>(src[j]), 0);
            });
            kernel_preserve_shape(r, v);
            return r;
        }

        const uint32_t k = v->data_length;
        const int32_t* src = static_cast<const int32_t*>(v->data);

        DrakenStringSlot* slots;
        uint8_t* arena;
        uint8_t* vunused;
        uint8_t* block = vecresult_string_block_alloc(k, 0u, 0, &slots, &arena, &vunused);
        if (!block) return draken_error_sentinel("Allocation failed");
        (void)arena; (void)vunused;  // all-inline: no arena; validity preserved separately

        uint8_t buf[10];
        for (uint32_t j = 0u; j < k; ++j) {
            int32_t y; int32_t m; int32_t d;
            days_to_ymd(src[j], y, m, d);
            buf[0] = static_cast<uint8_t>('0' + (y / 1000) % 10);
            buf[1] = static_cast<uint8_t>('0' + (y / 100)  % 10);
            buf[2] = static_cast<uint8_t>('0' + (y / 10)   % 10);
            buf[3] = static_cast<uint8_t>('0' + y % 10);
            buf[4] = '-';
            buf[5] = static_cast<uint8_t>('0' + m / 10);
            buf[6] = static_cast<uint8_t>('0' + m % 10);
            buf[7] = '-';
            buf[8] = static_cast<uint8_t>('0' + d / 10);
            buf[9] = static_cast<uint8_t>('0' + d % 10);
            draken_build_string_slot(&slots[j], buf, 10u, 0u);
        }
        VecResult r = vecresult_from_string_block(block, k, 0u, 0, DRAKEN_VARCHAR);
        kernel_preserve_shape(r, v);
        return r;
    });
}

// TIMESTAMP64 → VARCHAR: default "YYYY-MM-DDTHH:MM:SS.ffffff" (26 chars, extern,
// true ISO-8601 — no separator/offset fabrication; naive timestamps carry no
// offset to report). ctx (format_ctx*) carries the operand's TimestampUnit
// (0=s,1=ms,2=us,3=ns) — REQUIRED even for the no-FORMAT default, since the raw
// int64 payload's scale depends on it (a prior version of this kernel always
// treated the payload as microseconds, which was silently wrong for non-us
// TIMESTAMP64 columns). ctx == nullptr falls back to microseconds (defensive
// only — the compiler always allocates a ctx for this kernel).
// Compression-aware: format the K physical values into a K-slot value block, then
// preserve the input's selection + validity (dict timestamp → dict string).
VecResult draken_cast_timestamp_to_string(void* ctx, const DrakenVector* v) {
    DRAKEN_KERNEL_TRY({
        if (!v) return draken_error_sentinel("Input vector is null");
        if (v->type != DRAKEN_TIMESTAMP64)
            return draken_error_sentinel_fmt("cast timestamp->string: expected TIMESTAMP64, got %d", v->type);

        const auto* c = static_cast<const format_ctx*>(ctx);
        const int unit_code = c ? static_cast<int>(c->ts_unit) : 2;
        const int64_t tps = ta_ticks_per_second(unit_code);
        const int64_t* src0 = static_cast<const int64_t*>(v->data);

        if (c != nullptr && c->fmt_len > 0) {
            VecResult r = sql_format_to_string(v, c, [&](uint32_t j) {
                const int64_t sec = ta_floor_div(src0[j], tps);
                const int64_t days = ta_floor_div(sec, 86400LL);
                const int64_t tod  = sec - days * 86400LL;
                const int64_t frac_ticks = src0[j] - sec * tps;   // remainder in source ticks
                const int64_t usec = ta_floor_div(frac_ticks * 1000000LL, tps);
                SqlFields f = sql_calendar_fields(days, tod * 1000000LL + usec);
                return f;
            });
            kernel_preserve_shape(r, v);
            return r;
        }

        const uint32_t k = v->data_length;
        const int64_t* src = src0;

        // Every physical value formats to exactly 26 bytes (extern), so the K-slot
        // value block size is known up front: K slots + K*26 arena, no embedded validity.
        const size_t total_arena = static_cast<size_t>(k) * 26u;
        DrakenStringSlot* slots;
        uint8_t* arena;
        uint8_t* vunused;
        uint8_t* block = vecresult_string_block_alloc(k, total_arena, 0, &slots, &arena, &vunused);
        if (!block) return draken_error_sentinel("Allocation failed");
        (void)vunused;

        size_t arena_used = 0u;
        uint8_t buf[26];
        for (uint32_t j = 0u; j < k; ++j) {
            const int64_t raw = src[j];
            const int64_t sec_ticks = ta_floor_div(raw, tps);
            const int64_t frac_ticks = raw - sec_ticks * tps;
            const int32_t usec = static_cast<int32_t>(ta_floor_div(frac_ticks * 1000000LL, tps));
            int64_t days64 = ta_floor_div(sec_ticks, 86400LL);
            int32_t tod = static_cast<int32_t>(sec_ticks - days64 * 86400LL);
            int32_t y; int32_t m; int32_t d;
            days_to_ymd(static_cast<int32_t>(days64), y, m, d);
            const int32_t hh = tod / 3600;
            const int32_t mm = (tod % 3600) / 60;
            const int32_t ss = tod % 60;
            buf[0]=static_cast<uint8_t>('0'+(y/1000)%10); buf[1]=static_cast<uint8_t>('0'+(y/100)%10);
            buf[2]=static_cast<uint8_t>('0'+(y/10)%10);   buf[3]=static_cast<uint8_t>('0'+y%10);
            buf[4]='-'; buf[5]=static_cast<uint8_t>('0'+m/10); buf[6]=static_cast<uint8_t>('0'+m%10);
            buf[7]='-'; buf[8]=static_cast<uint8_t>('0'+d/10); buf[9]=static_cast<uint8_t>('0'+d%10);
            buf[10]='T'; buf[11]=static_cast<uint8_t>('0'+hh/10); buf[12]=static_cast<uint8_t>('0'+hh%10);
            buf[13]=':'; buf[14]=static_cast<uint8_t>('0'+mm/10); buf[15]=static_cast<uint8_t>('0'+mm%10);
            buf[16]=':'; buf[17]=static_cast<uint8_t>('0'+ss/10); buf[18]=static_cast<uint8_t>('0'+ss%10);
            buf[19]='.';
            buf[20]=static_cast<uint8_t>('0'+(usec/100000)%10); buf[21]=static_cast<uint8_t>('0'+(usec/10000)%10);
            buf[22]=static_cast<uint8_t>('0'+(usec/1000)%10);   buf[23]=static_cast<uint8_t>('0'+(usec/100)%10);
            buf[24]=static_cast<uint8_t>('0'+(usec/10)%10);     buf[25]=static_cast<uint8_t>('0'+usec%10);
            std::memcpy(arena + arena_used, buf, 26u);
            draken_build_string_slot(&slots[j], buf, 26u, static_cast<uint32_t>(arena_used));
            arena_used += 26u;
        }
        VecResult r = vecresult_from_string_block(block, k, total_arena, 0, DRAKEN_VARCHAR);
        kernel_preserve_shape(r, v);
        return r;
    });
}

// Howard Hinnant civil-days, local copy for TIMESTAMP parsing (same algorithm as
// cast_string.cpp's civil_to_days, kept as an internal-linkage duplicate per this
// file's own days_to_ymd precedent above — no cross-TU header exists for it yet).
static int64_t civil_to_days(int y, int m, int d) noexcept {
    y -= (m <= 2);
    const int64_t era = (y >= 0 ? y : y - 399) / 400;
    const int64_t yoe = static_cast<int64_t>(y) - era * 400;
    const int64_t doy = (153 * static_cast<int64_t>(m + (m > 2 ? -3 : 9)) + 2) / 5 + d - 1;
    const int64_t doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    return era * 146097 + doe - 719468;
}

// VARCHAR/NVARCHAR/VARBINARY → TIMESTAMP64. ctx (format_ctx*) null or fmt_len==0
// -> strict ISO-8601 parse ("YYYY-MM-DDTHH:MM:SS[.ffffff]" or with a space
// separator; no timezone offset — Opteryx timestamps are always naive, so an
// offset suffix is a parse error, matching the plan-time literal parser's
// intent rather than silently discarding it). ctx->fmt_len > 0 -> FORMAT-driven
// parse via sql_parse_exec. Always produces microsecond-unit TIMESTAMP64.
static bool parse_iso_timestamp(const uint8_t* s, uint32_t len,
                                 int* year, int* month, int* day,
                                 int* hour, int* minute, int* second, int* usec) {
    *hour = 0; *minute = 0; *second = 0; *usec = 0;
    uint32_t k = 0;
    int y = 0, m = 0, d = 0;
    while (k < len && s[k] != '-') {
        if (s[k] < '0' || s[k] > '9') return false;
        y = y * 10 + (s[k] - '0'); ++k;
    }
    if (k >= len || s[k] != '-') return false;
    ++k;
    while (k < len && s[k] != '-') {
        if (s[k] < '0' || s[k] > '9') return false;
        m = m * 10 + (s[k] - '0'); ++k;
    }
    if (k >= len || s[k] != '-') return false;
    ++k;
    while (k < len && (s[k] >= '0' && s[k] <= '9')) {
        d = d * 10 + (s[k] - '0'); ++k;
    }
    *year = y; *month = m; *day = d;
    if (k == len) return (m >= 1 && m <= 12 && d >= 1 && d <= 31);
    if (s[k] != 'T' && s[k] != ' ') return false;
    ++k;

    int hh = 0, mi = 0, ss = 0, us = 0;
    uint32_t hstart = k;
    while (k < len && s[k] != ':') {
        if (s[k] < '0' || s[k] > '9') return false;
        hh = hh * 10 + (s[k] - '0'); ++k;
    }
    if (k == hstart || k >= len || s[k] != ':') return false;
    ++k;
    uint32_t mstart = k;
    while (k < len && s[k] != ':') {
        if (s[k] < '0' || s[k] > '9') return false;
        mi = mi * 10 + (s[k] - '0'); ++k;
    }
    if (k == mstart) return false;
    if (k < len && s[k] == ':') {
        ++k;
        uint32_t sstart = k;
        while (k < len && s[k] != '.') {
            if (s[k] < '0' || s[k] > '9') return false;
            ss = ss * 10 + (s[k] - '0'); ++k;
        }
        if (k == sstart) return false;
        if (k < len && s[k] == '.') {
            ++k;
            int ndigits = 0;
            while (k < len) {
                if (s[k] < '0' || s[k] > '9') return false;
                if (ndigits >= 6) return false;
                us = us * 10 + (s[k] - '0'); ++ndigits; ++k;
            }
            if (ndigits == 0) return false;
            for (int p = ndigits; p < 6; ++p) us *= 10;
        }
    }
    if (k != len) return false;  // trailing offset/'Z' etc. -> not supported
    if (hh > 23 || mi > 59 || ss > 59) return false;
    *hour = hh; *minute = mi; *second = ss; *usec = us;
    return m >= 1 && m <= 12 && d >= 1 && d <= 31;
}

VecResult draken_cast_string_to_timestamp(void* ctx, const DrakenVector* v) {
    DRAKEN_KERNEL_TRY({
        if (!v) return draken_error_sentinel("Input vector is null");
        if (v->type != DRAKEN_VARCHAR && v->type != DRAKEN_NVARCHAR && v->type != DRAKEN_VARBINARY)
            return draken_error_sentinel_fmt("cast string->timestamp: expected string, got %d", v->type);

        const auto* c = static_cast<const format_ctx*>(ctx);
        std::vector<SqlToken> prog;
        bool use_fmt = c != nullptr && c->fmt_len > 0;
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

        const DrakenStringArena* sa = static_cast<const DrakenStringArena*>(v->data);
        const uint32_t k = v->data_length;
        const uint32_t n = v->length;
        int64_t* out = static_cast<int64_t*>(draken_malloc((k > 0u ? k : 1u) * sizeof(int64_t)));
        if (!out) return draken_error_sentinel("Allocation failed");

        std::vector<uint8_t> live(k > 0u ? k : 1u, 0u);
        for (uint32_t i = 0u; i < n; ++i)
            if (!kernel_row_is_null(v, i)) live[v->selection[i]] = 1u;

        // TRY_CAST rides format_ctx.safe (this kernel takes that ctx for the
        // pattern), the same field the string->DATE twin reads.
        const bool is_safe = (c != nullptr && c->safe != 0u);
        std::vector<uint8_t> bad(k > 0u ? k : 1u, 0u);
        bool any_bad = false;

        for (uint32_t j = 0u; j < k; ++j) {
            if (!live[j]) { out[j] = 0; continue; }
            const DrakenStringSlot* slot = &sa->slots[j];
            const uint8_t* s = str_data(slot, sa->arena);
            const uint32_t len = str_length(slot);

            int year;
            int month;
            int day;
            int hour;
            int minute;
            int second;
            int usec;
            bool ok;
            if (use_fmt) {
                ok = sql_parse_exec(prog, reinterpret_cast<const char*>(s), len,
                                     &year, &month, &day, &hour, &minute, &second, &usec);
            } else {
                ok = parse_iso_timestamp(s, len, &year, &month, &day, &hour, &minute, &second, &usec);
            }
            if (!ok) {
                if (!is_safe) {
                    draken_free(out);
                    return draken_error_sentinel_fmt(
                        "Cannot cast string to TIMESTAMP: got %.*s",
                        (int)(len < 32u ? len : 32u), s);
                }
                out[j] = 0; bad[j] = 1u; any_bad = true; continue;
            }
            const int64_t days = civil_to_days(year, month, day);
            out[j] = days * 86400000000LL
                   + static_cast<int64_t>(hour) * 3600000000LL
                   + static_cast<int64_t>(minute) * 60000000LL
                   + static_cast<int64_t>(second) * 1000000LL
                   + usec;
        }

        VecResult r;
        r.data = out; r.type = DRAKEN_TIMESTAMP64; r.validity_embedded = 0u;
        r.ts_unit = static_cast<uint8_t>(TimestampUnit::MICROSECONDS);
        kernel_preserve_shape(r, v);
        if (any_bad) kernel_null_bad_rows(r, v, bad.data());
        return r;
    });
}

// INTERVAL → VARCHAR: default ISO-8601 duration ("P1DT2H30M"). ctx (format_ctx*)
// null or fmt_len==0 -> ISO-8601 default (iso8601_duration_emit). ctx->fmt_len>0
// -> FORMAT tokens reinterpreted as duration magnitudes (interval_to_sql_fields) —
// there is no INTERVAL parse direction (CAST FROM another type into INTERVAL
// stays rejected — see logical_planner_builders.py's _normalize_cast_type).
VecResult draken_cast_interval_to_string(void* ctx, const DrakenVector* v) {
    DRAKEN_KERNEL_TRY({
        if (!v) return draken_error_sentinel("Input vector is null");
        if (v->type != DRAKEN_INTERVAL)
            return draken_error_sentinel_fmt("cast interval->string: expected INTERVAL, got %d", v->type);

        const auto* c = static_cast<const format_ctx*>(ctx);
        const uint32_t k = v->data_length;
        const auto* src = static_cast<const DrakenIntervalSlot*>(v->data);

        if (c != nullptr && c->fmt_len > 0) {
            VecResult r = sql_format_to_string(v, c, [&](uint32_t j) {
                return interval_to_sql_fields(src[j].months, src[j].us);
            });
            kernel_preserve_shape(r, v);
            return r;
        }

        char buf[96];
        // Worst case per row is small but not fixed-width; use an arena-growth
        // shape (separately-allocated then consolidated) rather than a fixed-width
        // block since duration text length varies by magnitude. Guard uses default
        // member initializers + post-construction assignment (not a brace-init with
        // a top-level comma) — DRAKEN_KERNEL_TRY's argument scanning tracks ()
        // nesting, not {}, so a `T g{a, b};` here would be misread as two macro
        // arguments (see the fk_ts_ticks_per_sec comment above for the same trap).
        struct Guard {
            DrakenStringSlot* s = nullptr;
            uint8_t* a = nullptr;
            ~Guard() { if (s) draken_free(s); if (a) draken_free(a); }
        } g;
        const size_t slots_sz = (k > 0u ? k : 1u) * sizeof(DrakenStringSlot);
        g.s = static_cast<DrakenStringSlot*>(draken_malloc(slots_sz));
        if (!g.s) return draken_error_sentinel("Allocation failed");
        std::memset(g.s, 0, slots_sz);
        size_t arena_cap = (k > 0u ? static_cast<size_t>(k) * 24u : 24u);
        g.a = static_cast<uint8_t*>(draken_malloc(arena_cap));
        if (!g.a) return draken_error_sentinel("Allocation failed");
        DrakenStringSlot* const slots = g.s;

        size_t arena_used = 0u;
        for (uint32_t j = 0u; j < k; ++j) {
            const uint32_t slen = iso8601_duration_emit(buf, src[j].months, src[j].us);
            if (slen <= STR_INLINE_MAX) {
                str_init_inline(&slots[j], reinterpret_cast<const uint8_t*>(buf), slen);
            } else {
                if (arena_used + slen > arena_cap) {
                    arena_cap = (arena_used + slen) * 2u;
                    auto* new_arena = static_cast<uint8_t*>(draken_malloc(arena_cap));
                    if (!new_arena) throw std::bad_alloc();
                    std::memcpy(new_arena, g.a, arena_used);
                    draken_free(g.a);
                    g.a = new_arena;
                }
                const uint32_t arena_off = static_cast<uint32_t>(arena_used);
                std::memcpy(g.a + arena_off, buf, slen);
                draken_build_string_slot(&slots[j], reinterpret_cast<const uint8_t*>(buf), slen, arena_off);
                arena_used += slen;
            }
        }
        DrakenStringSlot* out_slots = g.s;
        uint8_t*          out_arena = g.a;
        g.s = nullptr;
        g.a = nullptr;

        VecResult r = vecresult_from_string_buffers(out_slots, out_arena, arena_used, nullptr, k, DRAKEN_VARCHAR);
        kernel_preserve_shape(r, v);
        return r;
    });
}

VecResult draken_cast_interval_to_blob(void* ctx, const DrakenVector* v) {
    VecResult r = draken_cast_interval_to_string(ctx, v);
    if (r.data != nullptr) r.type = DRAKEN_VARBINARY;
    return r;
}

// ---------------------------------------------------------------------------
// TIME64 (int64 microseconds-since-midnight) casts.
//
// Parses/formats "HH:MM:SS[.ffffff]" — no date, no timezone. Fixed-width
// output (15 bytes, always includes the fractional part) mirrors the
// TIMESTAMP64 -> VARCHAR kernel above; parsing mirrors parse_iso_date.
// Returns INT64_MIN on any parse error (never a valid microseconds-since-
// midnight value, which is in [0, 86_400_000_000)).
// ---------------------------------------------------------------------------
static inline int64_t parse_iso_time(const uint8_t* s, uint32_t len) noexcept {
    uint32_t k = 0;
    int hour = 0, minute = 0, second = 0;

    uint32_t hstart = k;
    while (k < len && s[k] != ':') {
        if (s[k] < '0' || s[k] > '9') return INT64_MIN;
        hour = hour * 10 + (s[k] - '0');
        ++k;
    }
    if (k == hstart || k - hstart > 2 || k >= len || s[k] != ':') return INT64_MIN;
    ++k;

    uint32_t mstart = k;
    while (k < len && s[k] != ':') {
        if (s[k] < '0' || s[k] > '9') return INT64_MIN;
        minute = minute * 10 + (s[k] - '0');
        ++k;
    }
    if (k == mstart || k - mstart > 2) return INT64_MIN;

    int frac_us = 0;
    if (k < len && s[k] == ':') {
        ++k;
        uint32_t sstart = k;
        while (k < len && s[k] != '.') {
            if (s[k] < '0' || s[k] > '9') return INT64_MIN;
            second = second * 10 + (s[k] - '0');
            ++k;
        }
        if (k == sstart || k - sstart > 2) return INT64_MIN;

        if (k < len && s[k] == '.') {
            ++k;
            uint32_t fstart = k;
            int ndigits = 0;
            while (k < len) {
                if (s[k] < '0' || s[k] > '9') return INT64_MIN;
                if (ndigits >= 6) return INT64_MIN;  // >6 fractional digits: fail loud
                frac_us = frac_us * 10 + (s[k] - '0');
                ++ndigits;
                ++k;
            }
            if (ndigits == 0) return INT64_MIN;
            for (int p = ndigits; p < 6; ++p) frac_us *= 10;
            (void)fstart;
        }
    }
    if (k != len) return INT64_MIN;
    if (hour > 23 || minute > 59 || second > 59) return INT64_MIN;

    return (static_cast<int64_t>(hour) * 3600LL
            + static_cast<int64_t>(minute) * 60LL
            + static_cast<int64_t>(second)) * 1000000LL
           + frac_us;
}

VecResult draken_cast_string_to_time64(void* ctx, const DrakenVector* v) {
    DRAKEN_KERNEL_TRY({
        if (!v) return draken_error_sentinel("Input vector is null");
        if (v->type != DRAKEN_VARCHAR && v->type != DRAKEN_NVARCHAR && v->type != DRAKEN_VARBINARY)
            return draken_error_sentinel_fmt("cast string->time: expected string, got %d", v->type);

        // Compression-aware with liveness (see draken_cast_string_to_date32).
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
            const uint8_t* s   = str_data(slot, sa ? sa->arena : nullptr);
            const uint32_t len = str_length(slot);

            const int64_t us = parse_iso_time(s, len);
            if (us == INT64_MIN) {
                draken_free(out);
                return draken_error_sentinel_fmt(
                    "Cannot cast string to TIME: expected HH:MM:SS[.ffffff], got %.*s",
                    (int)(len < 20u ? len : 20u), s);
            }
            out[j] = us;
        }

        VecResult r;
        r.data = out; r.type = DRAKEN_TIME64; r.validity_embedded = 0u;
        r.ts_unit = static_cast<uint8_t>(TimestampUnit::MICROSECONDS);
        kernel_preserve_shape(r, v);
        return r;
    });
}

// TIME64 -> VARCHAR: "HH:MM:SS.ffffff" (15 chars, extern — over the 12-byte
// inline threshold, so this needs an arena, like the TIMESTAMP64 formatter
// above). Values are int64 microseconds-since-midnight; the source unit is
// always microseconds (draken_cast_string_to_time64 is TIME64's only
// producer reachable from SQL CAST — TIME32 has no cast path).
VecResult draken_cast_time_to_string(void* ctx, const DrakenVector* v) {
    DRAKEN_KERNEL_TRY({
        if (!v) return draken_error_sentinel("Input vector is null");
        if (v->type != DRAKEN_TIME64)
            return draken_error_sentinel_fmt("cast time->string: expected TIME64, got %d", v->type);
        const uint32_t k = v->data_length;
        const int64_t* src = static_cast<const int64_t*>(v->data);

        const size_t total_arena = static_cast<size_t>(k) * 15u;
        DrakenStringSlot* slots;
        uint8_t* arena;
        uint8_t* vunused;
        uint8_t* block = vecresult_string_block_alloc(k, total_arena, 0, &slots, &arena, &vunused);
        if (!block) return draken_error_sentinel("Allocation failed");
        (void)vunused;

        size_t arena_used = 0u;
        uint8_t buf[15];
        for (uint32_t j = 0u; j < k; ++j) {
            int64_t us = src[j];
            int64_t sec = us / 1000000LL;
            int32_t usec = static_cast<int32_t>(us % 1000000LL);
            if (usec < 0) { sec -= 1; usec += 1000000; }
            int32_t tod = static_cast<int32_t>(sec % 86400LL);
            if (tod < 0) tod += 86400;
            const int32_t hh = tod / 3600;
            const int32_t mm = (tod % 3600) / 60;
            const int32_t ss = tod % 60;
            buf[0]=static_cast<uint8_t>('0'+hh/10); buf[1]=static_cast<uint8_t>('0'+hh%10);
            buf[2]=':'; buf[3]=static_cast<uint8_t>('0'+mm/10); buf[4]=static_cast<uint8_t>('0'+mm%10);
            buf[5]=':'; buf[6]=static_cast<uint8_t>('0'+ss/10); buf[7]=static_cast<uint8_t>('0'+ss%10);
            buf[8]='.';
            buf[9]=static_cast<uint8_t>('0'+(usec/100000)%10); buf[10]=static_cast<uint8_t>('0'+(usec/10000)%10);
            buf[11]=static_cast<uint8_t>('0'+(usec/1000)%10);  buf[12]=static_cast<uint8_t>('0'+(usec/100)%10);
            buf[13]=static_cast<uint8_t>('0'+(usec/10)%10);    buf[14]=static_cast<uint8_t>('0'+usec%10);
            std::memcpy(arena + arena_used, buf, 15u);
            draken_build_string_slot(&slots[j], buf, 15u, static_cast<uint32_t>(arena_used));
            arena_used += 15u;
        }
        VecResult r = vecresult_from_string_block(block, k, total_arena, 0, DRAKEN_VARCHAR);
        kernel_preserve_shape(r, v);
        return r;
    });
}

// → VARBINARY (BLOB) thin wrappers — see the matching comment in cast_numeric.cpp
// (DRAKEN_CAST_TO_BLOB): DATE32/TIMESTAMP64 format to the identical ASCII bytes
// for VARCHAR and VARBINARY targets, so these just retag the `_to_string`
// kernel's result rather than reformatting.
VecResult draken_cast_date_to_blob(void* ctx, const DrakenVector* v) {
    VecResult r = draken_cast_date_to_string(ctx, v);
    if (r.data != nullptr) r.type = DRAKEN_VARBINARY;
    return r;
}

VecResult draken_cast_timestamp_to_blob(void* ctx, const DrakenVector* v) {
    VecResult r = draken_cast_timestamp_to_string(ctx, v);
    if (r.data != nullptr) r.type = DRAKEN_VARBINARY;
    return r;
}

namespace {
// Kept OUTSIDE the DRAKEN_KERNEL_TRY macro body below: an in-body brace-init
// array literal's commas are seen as macro-argument separators (the
// preprocessor tracks () nesting, not {}), which breaks the macro call with
// "too many arguments". A plain function call has none of that ambiguity.
inline int64_t fk_ts_ticks_per_sec(unsigned unit) {
    switch (unit & 3) {
        case 1: return 1000;
        case 2: return 1000000;
        case 3: return 1000000000;
        default: return 1;   // 0 = seconds
    }
}
}  // namespace

// TIMESTAMP64 → TIMESTAMP64 unit rescale (e.g. `EventTime::TIMESTAMP[s]` where
// EventTime is already TIMESTAMP64 at a different unit). ctx = binary_op_ctx:
// left_unit = source TimestampUnit, right_unit = target TimestampUnit (both
// 0=s,1=ms,2=us,3=ns — the SAME convention draken_date_part/draken_date_trunc
// use, NOT cast_timestamp_ctx's older 1=ns..5=days numbering). Narrowing
// (source finer than target, e.g. us->s) floors toward -inf for correctness on
// pre-epoch timestamps; widening (source coarser, e.g. s->us) is an exact
// multiply. Identity (src==dst) degrades to a copy. DENSE emit (gather through
// selection) per the date32-cast lesson: shape-preserving output caused a
// silent wrong-answer bug there.
VecResult draken_cast_timestamp_rescale(void* ctx, const DrakenVector* v) {
    DRAKEN_KERNEL_TRY({
        if (!v) return draken_error_sentinel("Input vector is null");
        // INT64 accepted alongside TIMESTAMP64: same 8-byte payload — an INT64
        // source is "a timestamp at the ctx-declared source unit" (the SQL
        // suffix's interpretation), rescaled to the declared result unit.
        if (v->type != DRAKEN_TIMESTAMP64 && v->type != DRAKEN_INT64)
            return draken_error_sentinel_fmt("cast timestamp rescale: expected TIMESTAMP64/INT64, got %d", v->type);
        if (!ctx) return draken_error_sentinel("cast timestamp rescale: missing ctx (units)");
        const auto* c = static_cast<const binary_op_ctx*>(ctx);
        const int64_t src_ticks = fk_ts_ticks_per_sec(c->left_unit);
        const int64_t dst_ticks = fk_ts_ticks_per_sec(c->right_unit);
        const TimestampUnit dst_unit = static_cast<TimestampUnit>(c->right_unit & 3);

        const uint32_t n = v->length;
        const int64_t* src = static_cast<const int64_t*>(v->data);
        int64_t* out = static_cast<int64_t*>(draken_malloc((n > 0u ? n : 1u) * sizeof(int64_t)));
        if (!out) return draken_error_sentinel("Allocation failed");
        for (uint32_t j = 0u; j < n; ++j) {
            int64_t raw = src[v->selection[j]];
            if (src_ticks >= dst_ticks) {
                int64_t factor = src_ticks / dst_ticks;
                if (factor == 1) {
                    out[j] = raw;
                } else {
                    int64_t q = raw / factor;
                    int64_t r = raw % factor;
                    out[j] = (r != 0 && ((r < 0) != (factor < 0))) ? q - 1 : q;
                }
            } else {
                out[j] = raw * (dst_ticks / src_ticks);
            }
        }
        VecResult r;
        r.data = out; r.type = DRAKEN_TIMESTAMP64; r.validity_embedded = 0u;
        r.ts_unit = static_cast<uint8_t>(dst_unit);
        r.length = n; r.data_length = n;
        r.selection = draken_identity_sel(n);
        r.owns_selection = false;
        r.validity = kernel_copy_validity(v);
        r.flags = DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION;
        return r;
    });
}

// TIMESTAMP64 → DATE32: truncates the time component (floor-divide by the
// source unit's ticks-per-day), matching DATE32's own days-since-epoch
// storage. ctx = binary_op_ctx: left_unit = source TimestampUnit (0=s,1=ms,
// 2=us,3=ns) — same convention as draken_cast_timestamp_rescale and
// draken_date_trunc; right_unit is unused (DATE32 carries no unit). Floors
// toward -inf via ta_floor_div for correctness on pre-epoch timestamps. No
// range check: the widest unit (ns) still floors to well under INT32_MAX
// days, so unlike the integer->DATE32 narrowings this cast cannot overflow.
// DENSE emit (gather through selection, identity-selection output) — the same
// dict-shape lesson as draken_cast_date32_to_timestamp above.
VecResult draken_cast_timestamp_to_date32(void* ctx, const DrakenVector* v) {
    DRAKEN_KERNEL_TRY({
        if (!v) return draken_error_sentinel("Input vector is null");
        if (v->type != DRAKEN_TIMESTAMP64)
            return draken_error_sentinel_fmt("cast timestamp->date32: expected TIMESTAMP64, got %d", v->type);
        int unit_code = 2;  // default microseconds
        if (ctx) unit_code = static_cast<const binary_op_ctx*>(ctx)->left_unit;
        const int64_t tpd = ta_ticks_per_day(unit_code);

        const uint32_t n = v->length;
        const int64_t* src = static_cast<const int64_t*>(v->data);
        int32_t* out = static_cast<int32_t*>(draken_malloc((n > 0u ? n : 1u) * sizeof(int32_t)));
        if (!out) return draken_error_sentinel("Allocation failed");
        for (uint32_t j = 0u; j < n; ++j)
            out[j] = static_cast<int32_t>(ta_floor_div(src[v->selection[j]], tpd));
        VecResult r;
        r.data = out; r.type = DRAKEN_DATE32; r.validity_embedded = 0u; r.ts_unit = 0xFFu;
        r.length = n; r.data_length = n;
        r.selection = draken_identity_sel(n);
        r.owns_selection = false;
        r.validity = kernel_copy_validity(v);
        r.flags = DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION;
        return r;
    });
}

}  // extern "C"
