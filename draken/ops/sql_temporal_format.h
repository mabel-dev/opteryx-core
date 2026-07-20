#pragma once
// draken/ops/sql_temporal_format.h — compiled token-program SQL-style
// (BigQuery/Snowflake/Oracle "format element") formatter + parser, used by
// CAST(... AS TIMESTAMP/DATE/VARCHAR FORMAT '...').
//
// This is a SIBLING to temporal_format.h, not a variant of it: temporal_format.h
// speaks strftime tokens (`%Y-%m-%d`, `%`-escaped, single-char specifiers) for
// DATE_FORMAT(); this header speaks SQL format elements (`YYYY-MM-DD`, no escape
// character, multi-char keyword tokens) for CAST ... FORMAT — the two vocabularies
// are unrelated and must not be conflated (CLAUDE.md §11: a shape/vocabulary
// discriminant must not silently change under the other's rules). Calendar math
// (day <-> ymd) and the digit-writer primitives are reused from temporal_arith.h /
// temporal_format.h rather than re-implemented — see §3/§11 (no duplicated logic).
//
// Token vocabulary (case-sensitive, uppercase only — lowercase text is always
// literal passthrough, so it never collides with a token):
//   YYYY   4-digit zero-padded year
//   YY     2-digit zero-padded year (year % 100)
//   MM     2-digit zero-padded month (01-12)
//   DD     2-digit zero-padded day (01-31)
//   HH24   2-digit zero-padded hour, 24h (00-23)
//   HH12   2-digit zero-padded hour, 12h (01-12)
//   HH     alias for HH12
//   MI     2-digit zero-padded minute (00-59)
//   SS     2-digit zero-padded second (00-59)
//   FF     6-digit zero-padded fractional seconds (microseconds)
// Any character not part of one of the above keywords is literal passthrough.
// A run starting with a reserved letter (Y/M/D/H/I/S/F) that does not form one
// of the exact keywords above is a compile error (fail loud on a likely typo,
// e.g. "HHH" or "YYY") rather than silently falling back to literal text.
//
// The SAME token program is reused, with a different field source, to format
// INTERVAL values: tokens are reinterpreted as duration MAGNITUDES (years,
// months-remainder, days, hours, minutes, seconds, microseconds) rather than
// calendar fields — see SqlFields' dual construction below.

#include <cstdint>
#include <cstring>
#include <vector>
#include <string>

#include "ops/temporal_arith.h"   // ta_days_to_ymd, ta_ymd_to_days

enum SqlTok : uint8_t {
    SQL_TOK_LIT = 0,
    SQL_TOK_YYYY, SQL_TOK_YY, SQL_TOK_MM, SQL_TOK_DD,
    SQL_TOK_HH24, SQL_TOK_HH12, SQL_TOK_HH, SQL_TOK_MI, SQL_TOK_SS, SQL_TOK_FF,
};

struct SqlToken {
    const char* lit;      // literal bytes (points into the fmt buffer); unused when tok != LIT
    uint32_t    lit_len;
    SqlTok      tok;
};

// Broken-down fields consumed by sql_emit. For calendar (TIMESTAMP/DATE) use,
// these are true calendar fields; for INTERVAL use, `negative` carries the
// duration's overall sign and every field is a non-negative magnitude (years =
// months/12, month = months%12, day/hour/minute/second/usec decomposed from the
// microsecond remainder) — see interval_to_sql_fields below.
struct SqlFields {
    int64_t year;    // may be > 4 digits for very large intervals; not clamped
    int     month;    // 0-11 (interval) or 1-12 (calendar) — caller-dependent, see notes below
    int64_t day;
    int     hour;
    int     minute;
    int     second;
    int     usec;
    bool    negative;  // interval sign; always false for calendar fields
};

static const struct { const char* kw; uint8_t len; SqlTok tok; } SQL_TOK_KEYWORDS[] = {
    {"HH24", 4, SQL_TOK_HH24},
    {"HH12", 4, SQL_TOK_HH12},
    {"YYYY", 4, SQL_TOK_YYYY},
    {"MM",   2, SQL_TOK_MM},
    {"DD",   2, SQL_TOK_DD},
    {"HH",   2, SQL_TOK_HH},
    {"MI",   2, SQL_TOK_MI},
    {"SS",   2, SQL_TOK_SS},
    {"FF",   2, SQL_TOK_FF},
    {"YY",   2, SQL_TOK_YY},
};

static inline bool sql_is_reserved_start(char c) noexcept {
    return c == 'Y' || c == 'M' || c == 'D' || c == 'H' || c == 'I' || c == 'S' || c == 'F';
}

static inline uint32_t sql_tok_width(SqlTok t) noexcept {
    switch (t) {
        case SQL_TOK_YYYY: return 4;
        case SQL_TOK_FF:   return 6;
        default:            return 2;  // YY, MM, DD, HH24, HH12, HH, MI, SS
    }
}

// Compiles `fmt` (fmt_len bytes, NOT necessarily NUL-terminated) into a token
// program. On success returns true and fills *prog / *max_len (worst-case output
// width, for sizing a per-row buffer). On failure (unrecognized reserved-letter
// run) returns false and fills *bad_run/*bad_run_len with the offending text.
static inline bool sql_compile(const char* fmt, size_t fmt_len,
                                std::vector<SqlToken>* prog, size_t* max_len,
                                const char** bad_run, uint32_t* bad_run_len) {
    prog->clear();
    size_t width = 1;  // slack
    const char* end = fmt + fmt_len;
    const char* run = fmt;
    const char* p = fmt;

    auto flush_literal = [&](const char* upto) {
        if (upto > run) {
            prog->push_back({run, static_cast<uint32_t>(upto - run), SQL_TOK_LIT});
            width += static_cast<size_t>(upto - run);
        }
    };

    while (p < end) {
        bool matched = false;
        for (const auto& kw : SQL_TOK_KEYWORDS) {
            if (static_cast<size_t>(end - p) >= kw.len &&
                std::memcmp(p, kw.kw, kw.len) == 0) {
                flush_literal(p);
                prog->push_back({nullptr, 0, kw.tok});
                width += sql_tok_width(kw.tok);
                p += kw.len;
                run = p;
                matched = true;
                break;
            }
        }
        if (matched) continue;

        if (sql_is_reserved_start(*p)) {
            // Maximal run of reserved-letter chars that failed to match a keyword —
            // report it as the bad token (fail loud rather than silently literal).
            const char* q = p;
            while (q < end && sql_is_reserved_start(*q)) ++q;
            *bad_run = p;
            *bad_run_len = static_cast<uint32_t>(q - p);
            return false;
        }
        ++p;
    }
    flush_literal(end);
    *max_len = width;
    return true;
}

// --- digit writers (unpadded / zero-padded, no snprintf) --------------------
static inline char* sql_put_padded(char* p, int64_t v, int width) {
    char tmp[24];
    int k = 0;
    bool neg = v < 0;
    uint64_t uv = neg ? static_cast<uint64_t>(-v) : static_cast<uint64_t>(v);
    if (uv == 0) tmp[k++] = '0';
    while (uv) { tmp[k++] = static_cast<char>('0' + uv % 10); uv /= 10; }
    while (k < width) tmp[k++] = '0';
    if (neg) *p++ = '-';
    while (k) *p++ = tmp[--k];
    return p;
}

// Emit one row's worth of bytes for the compiled program; returns the end pointer.
static inline char* sql_emit(char* p, const std::vector<SqlToken>& prog, const SqlFields& f) {
    for (const SqlToken& tok : prog) {
        switch (tok.tok) {
            case SQL_TOK_LIT:  std::memcpy(p, tok.lit, tok.lit_len); p += tok.lit_len; break;
            case SQL_TOK_YYYY: p = sql_put_padded(p, f.year, 4); break;
            case SQL_TOK_YY:   p = sql_put_padded(p, ((f.year % 100) + 100) % 100, 2); break;
            case SQL_TOK_MM:   p = sql_put_padded(p, f.month, 2); break;
            case SQL_TOK_DD:   p = sql_put_padded(p, f.day, 2); break;
            case SQL_TOK_HH24: p = sql_put_padded(p, f.hour, 2); break;
            case SQL_TOK_HH12:
            case SQL_TOK_HH:   p = sql_put_padded(p, ((f.hour + 11) % 12) + 1, 2); break;
            case SQL_TOK_MI:   p = sql_put_padded(p, f.minute, 2); break;
            case SQL_TOK_SS:   p = sql_put_padded(p, f.second, 2); break;
            case SQL_TOK_FF:   p = sql_put_padded(p, f.usec, 6); break;
        }
    }
    return p;
}

// Calendar fields (TIMESTAMP64/DATE32) from days-since-epoch + microseconds-in-day.
static inline SqlFields sql_calendar_fields(int64_t days, int64_t us_in_day) {
    SqlFields f{};
    int y, m, d;
    ta_days_to_ymd(days, &y, &m, &d);
    f.year = y; f.month = m; f.day = d;
    const int64_t sec = us_in_day / 1000000LL;
    f.hour   = static_cast<int>(sec / 3600LL);
    f.minute = static_cast<int>((sec % 3600LL) / 60LL);
    f.second = static_cast<int>(sec % 60LL);
    f.usec   = static_cast<int>(us_in_day % 1000000LL);
    f.negative = false;
    return f;
}

// Duration-magnitude fields (INTERVAL) from the internal (months, microseconds)
// slot. Fields are non-negative magnitudes; `negative` carries the overall sign
// (either component negative -> the whole duration is treated as negative, matching
// ISO-8601's single leading '-' convention — Opteryx never mixes signs between the
// months and microseconds components of one interval value in practice).
static inline SqlFields interval_to_sql_fields(int64_t months, int64_t us) {
    SqlFields f{};
    f.negative = (months < 0) || (us < 0);
    const int64_t am = months < 0 ? -months : months;
    const int64_t au = us < 0 ? -us : us;
    f.year  = am / 12;
    f.month = static_cast<int>(am % 12);
    f.day   = au / 86400000000LL;
    int64_t rem = au % 86400000000LL;
    f.hour   = static_cast<int>(rem / 3600000000LL); rem %= 3600000000LL;
    f.minute = static_cast<int>(rem / 60000000LL);   rem %= 60000000LL;
    f.second = static_cast<int>(rem / 1000000LL);
    f.usec   = static_cast<int>(rem % 1000000LL);
    return f;
}

// ISO-8601 duration default (no FORMAT given): P[n]Y[n]M[n]DT[n]H[n]M[n]S, omitting
// zero components; "PT0S" when the whole duration is zero. `buf` must be at least
// 96 bytes (comfortably covers int64-magnitude years/days plus all separators).
// Returns the number of bytes written.
static inline uint32_t iso8601_duration_emit(char* buf, int64_t months, int64_t us) {
    const SqlFields f = interval_to_sql_fields(months, us);
    char* p = buf;
    if (f.negative) *p++ = '-';
    *p++ = 'P';
    bool any = false;
    if (f.year > 0)  { p = sql_put_padded(p, f.year, 1);  *p++ = 'Y'; any = true; }
    if (f.month > 0) { p = sql_put_padded(p, f.month, 1); *p++ = 'M'; any = true; }
    if (f.day > 0)   { p = sql_put_padded(p, f.day, 1);   *p++ = 'D'; any = true; }
    const bool has_time = f.hour > 0 || f.minute > 0 || f.second > 0 || f.usec > 0;
    if (has_time) {
        *p++ = 'T';
        if (f.hour > 0)   { p = sql_put_padded(p, f.hour, 1);   *p++ = 'H'; any = true; }
        if (f.minute > 0) { p = sql_put_padded(p, f.minute, 1); *p++ = 'M'; any = true; }
        if (f.second > 0 || f.usec > 0) {
            p = sql_put_padded(p, f.second, 1);
            if (f.usec > 0) { *p++ = '.'; p = sql_put_padded(p, f.usec, 6); }
            *p++ = 'S';
            any = true;
        }
    }
    if (!any) { *p++ = 'T'; *p++ = '0'; *p++ = 'S'; }
    return static_cast<uint32_t>(p - buf);
}

// ---------------------------------------------------------------------------
// Parsing (VARCHAR -> TIMESTAMP64/DATE32 only; INTERVAL has no FORMAT-parse
// direction — see cast_temporal.cpp). Walks the compiled program against the
// input bytes; literal tokens must match verbatim, numeric tokens consume
// exactly `sql_tok_width` digits (fixed-width, matching the formatter's output).
// Returns false on any mismatch (caller raises with the offending input).
// ---------------------------------------------------------------------------
static inline bool sql_parse_digits(const char*& s, const char* end, uint32_t width, int64_t* out) {
    if (static_cast<size_t>(end - s) < width) return false;
    int64_t v = 0;
    for (uint32_t i = 0; i < width; ++i) {
        if (s[i] < '0' || s[i] > '9') return false;
        v = v * 10 + (s[i] - '0');
    }
    s += width;
    *out = v;
    return true;
}

static inline bool sql_parse_exec(const std::vector<SqlToken>& prog, const char* s, size_t len,
                                   int* year, int* month, int* day,
                                   int* hour, int* minute, int* second, int* usec) {
    *year = 1970; *month = 1; *day = 1; *hour = 0; *minute = 0; *second = 0; *usec = 0;
    const char* p = s;
    const char* end = s + len;
    for (const SqlToken& tok : prog) {
        int64_t v;
        switch (tok.tok) {
            case SQL_TOK_LIT:
                if (static_cast<size_t>(end - p) < tok.lit_len) return false;
                if (std::memcmp(p, tok.lit, tok.lit_len) != 0) return false;
                p += tok.lit_len;
                break;
            case SQL_TOK_YYYY: if (!sql_parse_digits(p, end, 4, &v)) return false; *year = static_cast<int>(v); break;
            case SQL_TOK_YY:   if (!sql_parse_digits(p, end, 2, &v)) return false; *year = 2000 + static_cast<int>(v); break;
            case SQL_TOK_MM:   if (!sql_parse_digits(p, end, 2, &v)) return false; *month = static_cast<int>(v); break;
            case SQL_TOK_DD:   if (!sql_parse_digits(p, end, 2, &v)) return false; *day = static_cast<int>(v); break;
            case SQL_TOK_HH24: if (!sql_parse_digits(p, end, 2, &v)) return false; *hour = static_cast<int>(v); break;
            case SQL_TOK_HH12:
            case SQL_TOK_HH:   if (!sql_parse_digits(p, end, 2, &v)) return false; *hour = static_cast<int>(v) % 12; break;
            case SQL_TOK_MI:   if (!sql_parse_digits(p, end, 2, &v)) return false; *minute = static_cast<int>(v); break;
            case SQL_TOK_SS:   if (!sql_parse_digits(p, end, 2, &v)) return false; *second = static_cast<int>(v); break;
            case SQL_TOK_FF:   if (!sql_parse_digits(p, end, 6, &v)) return false; *usec = static_cast<int>(v); break;
        }
    }
    if (p != end) return false;
    if (*month < 1 || *month > 12 || *day < 1 || *day > 31 ||
        *hour < 0 || *hour > 23 || *minute < 0 || *minute > 59 || *second < 0 || *second > 59)
        return false;
    return true;
}
