#pragma once
// draken/ops/temporal_format.h — compiled token-program DATE_FORMAT formatter.
//
// Extracted from opteryx/compiled/nanobind/vector_temporal_arith.cpp (Milestone
// E.13, Phase 12) so both the nanobind DATE_FORMAT path and the C-ABI
// draken_date_format kernel (draken/ops/kernels/function_temporal.cpp) share ONE
// formatter — per the "no duplicated logic between paths" rule, this is the sole
// definition; do not re-implement it in either caller.
//
// strftime() re-parses the format string and walks the locale tables on every
// call, and gmtime_r() does a full calendar conversion per call. Both costs are
// constant-per-row work paid once for every row. Instead the (constant) format
// string is compiled ONCE into a token program, each timestamp is decomposed with
// the in-tree branchless ta_days_to_ymd (draken/ops/temporal_arith.h — same path
// DATE_PART/DATE_TRUNC use, no gmtime_r), and bytes are emitted directly.
//
// The engine always operates in UTC under the C locale, so every whitelisted
// specifier has a fixed, locale-independent definition. Deliberate pins:
//   %z -> "+0000", %Z -> "GMT"  (UTC — the only timezone-coupled specifiers)
//   %q -> quarter (1-4)          (Opteryx extension; %q is not a real strftime spec)

#include <cstdint>
#include <cstring>
#include <cctype>
#include <vector>

#include "ops/temporal_arith.h"   // ta_days_to_ymd, ta_ymd_to_days

// C-locale English names, UTC. Index by wday (0=Sunday) / month-1.
static const char* const TF_WDAY_ABBR[7] =
    {"Sun","Mon","Tue","Wed","Thu","Fri","Sat"};
static const char* const TF_WDAY_FULL[7] =
    {"Sunday","Monday","Tuesday","Wednesday","Thursday","Friday","Saturday"};
static const char* const TF_MON_ABBR[12] =
    {"Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"};
static const char* const TF_MON_FULL[12] =
    {"January","February","March","April","May","June",
     "July","August","September","October","November","December"};
static const char TF_NL  = '\n';
static const char TF_TAB = '\t';

// Broken-down UTC fields for one row.
struct TfFields {
    int64_t days;     // days since 1970-01-01
    int year, month, day;
    int hour, minute, second;
    int yday0;        // 0-based day of year
    int wday;         // 0=Sunday … 6=Saturday
};

// A compiled token: a literal run (spec==0) or a single specifier.
struct TfToken {
    const char* lit;      // literal bytes (point into fmt buffer or a static char)
    uint32_t    lit_len;
    char        spec;     // 0 → literal; else the specifier character
};

// --- direct digit writers (no snprintf) ------------------------------------
static inline char* tf_put2(char* p, int v) {
    *p++ = static_cast<char>('0' + (v / 10) % 10);
    *p++ = static_cast<char>('0' + v % 10);
    return p;
}
static inline char* tf_put2sp(char* p, int v) {  // space-padded 2-wide
    const int t = v / 10;
    *p++ = t ? static_cast<char>('0' + t) : ' ';
    *p++ = static_cast<char>('0' + v % 10);
    return p;
}
static inline char* tf_put3(char* p, int v) {    // zero-padded 3-wide
    *p++ = static_cast<char>('0' + (v / 100) % 10);
    *p++ = static_cast<char>('0' + (v / 10) % 10);
    *p++ = static_cast<char>('0' + v % 10);
    return p;
}
// Year, sign for negatives, zero-padded to at least minw (glibc pads %Y to 4).
static inline char* tf_put_year(char* p, int y, int minw) {
    if (y < 0) { *p++ = '-'; y = -y; }
    char tmp[16];
    int  k = 0;
    if (y == 0) tmp[k++] = '0';
    while (y) { tmp[k++] = static_cast<char>('0' + y % 10); y /= 10; }
    while (k < minw) tmp[k++] = '0';
    while (k) *p++ = tmp[--k];
    return p;
}
static inline char* tf_puts(char* p, const char* s) {
    while (*s) *p++ = *s++;
    return p;
}

// ISO-8601 week date: the week's Thursday determines the ISO year.
static inline void tf_iso_week(int64_t days, int wday, int* iso_year, int* iso_week) {
    const int iso_dow = (wday == 0) ? 7 : wday;        // Mon=1 … Sun=7
    const int64_t thu = days - (iso_dow - 4);
    int y, m, d; ta_days_to_ymd(thu, &y, &m, &d);
    *iso_year = y;
    *iso_week = static_cast<int>((thu - ta_ymd_to_days(y, 1, 1)) / 7) + 1;
}

// MySQL %X: year for the week where Sunday is the first day (mode 2).
static inline int tf_sunday_week_year(int64_t days, int wday) {
    const int64_t wed = days + 3 - static_cast<int64_t>(wday);
    int y, m, d; ta_days_to_ymd(wed, &y, &m, &d);
    return y;
}

// Emit one specifier; returns the advanced write pointer.
static inline char* tf_emit_spec(char* p, char c, const TfFields& f) {
    switch (c) {
        case 'Y': return tf_put_year(p, f.year, 4);
        case 'y': return tf_put2(p, ((f.year % 100) + 100) % 100);
        case 'm': return tf_put2(p, f.month);
        case 'd': return tf_put2(p, f.day);
        case 'e': return tf_put2sp(p, f.day);
        case 'H': return tf_put2(p, f.hour);
        case 'k': return tf_put2sp(p, f.hour);
        case 'I': return tf_put2(p, ((f.hour + 11) % 12) + 1);
        case 'l': return tf_put2sp(p, ((f.hour + 11) % 12) + 1);
        case 'M': return tf_puts(p, TF_MON_FULL[f.month - 1]);   // MySQL: month name
        case 'i': return tf_put2(p, f.minute);                   // MySQL: minutes 00-59
        case 'S': return tf_put2(p, f.second);
        case 'p': return tf_puts(p, f.hour < 12 ? "AM" : "PM");
        case 'P': return tf_puts(p, f.hour < 12 ? "am" : "pm");
        case 'j': return tf_put3(p, f.yday0 + 1);
        case 'a': return tf_puts(p, TF_WDAY_ABBR[f.wday]);
        case 'A': return tf_puts(p, TF_WDAY_FULL[f.wday]);
        case 'b': return tf_puts(p, TF_MON_ABBR[f.month - 1]);
        case 'B': return tf_puts(p, TF_MON_FULL[f.month - 1]);
        case 'w': *p++ = static_cast<char>('0' + f.wday); return p;
        case 'u': return tf_put2(p, (f.yday0 - (f.wday + 6) % 7 + 7) / 7); // MySQL: week 00-53, Monday-first
        case 'q': *p++ = static_cast<char>('0' + (f.month - 1) / 3 + 1); return p;
        case 'Z': return tf_puts(p, "GMT");
        case 'z': return tf_puts(p, "+0000");
        case 'U': return tf_put2(p, (f.yday0 - f.wday + 7) / 7);
        case 'W': return tf_puts(p, TF_WDAY_FULL[f.wday]);        // MySQL: weekday name
        case 'V': { int iy, iw; tf_iso_week(f.days, f.wday, &iy, &iw);
                    return tf_put2(p, iw); }
        case 'G': { int iy, iw; tf_iso_week(f.days, f.wday, &iy, &iw);
                    return tf_put_year(p, iy, 4); }
        case 'g': { int iy, iw; tf_iso_week(f.days, f.wday, &iy, &iw);
                    return tf_put2(p, ((iy % 100) + 100) % 100); }
        // Composite specifiers (C locale).
        case 'F': p = tf_put_year(p, f.year, 4); *p++ = '-';
                  p = tf_put2(p, f.month); *p++ = '-';
                  return tf_put2(p, f.day);
        case 'T': p = tf_put2(p, f.hour); *p++ = ':';
                  p = tf_put2(p, f.minute); *p++ = ':';
                  return tf_put2(p, f.second);
        case 'R': p = tf_put2(p, f.hour); *p++ = ':';
                  return tf_put2(p, f.minute);
        case 'r': p = tf_put2(p, ((f.hour + 11) % 12) + 1); *p++ = ':';
                  p = tf_put2(p, f.minute); *p++ = ':';
                  p = tf_put2(p, f.second); *p++ = ' ';
                  return tf_puts(p, f.hour < 12 ? "AM" : "PM");
        case 'x': { int iy, iw; tf_iso_week(f.days, f.wday, &iy, &iw);  // MySQL: year for Monday-first week (= %G)
                    return tf_put_year(p, iy, 4); }
        case 'X': return tf_put_year(p, tf_sunday_week_year(f.days, f.wday), 4); // MySQL: year for Sunday-first week
        case 'c': {                                                       // MySQL: month number 0-12 (no zero-pad)
                    const int mo = f.month;
                    if (mo >= 10) *p++ = static_cast<char>('0' + mo / 10);
                    *p++ = static_cast<char>('0' + mo % 10);
                    return p; }
        default:  *p++ = c; return p;   // unreachable after validation
    }
}

// Worst-case bytes a single specifier can emit (used to size the row buffer).
static inline uint32_t tf_spec_max_width(char c) {
    switch (c) {
        case 'Y': case 'G': case 'X': case 'x': return 12;  // sign + large year
        case 'A': case 'B': case 'M': case 'W': return 9;   // "Wednesday" / "September"
        case 'F': return 20;       // year(≤12) + "-MM-DD"
        case 'r': return 12;
        case 'T': return 10;
        case 'z': return 5;
        case 'a': case 'b': case 'Z': case 'j': return 3;
        case 'R': return 5;
        case 'c': case 'w': case 'q': return 2;  // %c = month num 1-12, %w/%q = 1 digit (2 is safe)
        default:  return 2;                // numeric 2-wide fields
    }
}

// Compile the (validated) format string into a token program; returns the
// worst-case output length via *max_len.
static inline std::vector<TfToken> tf_compile(const char* fmt, size_t* max_len) {
    std::vector<TfToken> prog;
    size_t width = 1;  // slack
    const char* run = fmt;
    for (const char* p = fmt; *p; ) {
        if (*p != '%') { ++p; continue; }
        if (p > run) {
            prog.push_back({run, static_cast<uint32_t>(p - run), 0});
            width += static_cast<size_t>(p - run);
        }
        const char spec = p[1];
        if (spec == '%') {
            prog.push_back({p + 1, 1, 0}); width += 1;       // literal '%'
        } else if (spec == 'n') {
            prog.push_back({&TF_NL, 1, 0}); width += 1;
        } else if (spec == 't') {
            prog.push_back({&TF_TAB, 1, 0}); width += 1;
        } else {
            prog.push_back({nullptr, 0, spec});
            width += tf_spec_max_width(spec);
        }
        p += 2;
        run = p;
    }
    if (*run) {  // trailing literal run
        const uint32_t len = static_cast<uint32_t>(std::strlen(run));
        prog.push_back({run, len, 0});
        width += len;
    }
    *max_len = width;
    return prog;
}

// Whitelists all POSIX/C99 strftime specifiers used by tf_emit_spec. Returns the
// offending specifier char via *bad_spec (0 if fmt is valid) instead of raising —
// callers own the fail-loud policy (Python ValueError vs. a draken_error_sentinel).
static inline bool tf_validate(const char* fmt, char* bad_spec) {
    // MySQL DATE_FORMAT specifiers (superset of the POSIX tokens exposed).
    // %i = minutes (MySQL); the rest are POSIX/C99 aliases with MySQL semantics.
    static const char* known = "YymdHIMSpjAaBbZenwtuVUWcxXFTRzGgklPqri";
    for (const char* p = fmt; *p; ++p) {
        if (*p != '%') continue;
        ++p;
        if (!*p) { *bad_spec = '\0'; return false; }   // trailing '%'
        if (*p == '%' || *p == 'n' || *p == 't') continue;  // %% %n %t always ok
        if (!std::strchr(known, *p)) { *bad_spec = *p; return false; }
    }
    return true;
}

// Decompose one row's tick value (already in `unit_code`'s ticks) into UTC
// calendar fields — branchless, no gmtime_r (same math DATE_PART uses).
static inline TfFields tf_decompose(int64_t days, int64_t secs_in_day) {
    TfFields f;
    ta_days_to_ymd(days, &f.year, &f.month, &f.day);
    f.days   = days;
    f.hour   = static_cast<int>(secs_in_day / 3600LL);
    f.minute = static_cast<int>((secs_in_day % 3600LL) / 60LL);
    f.second = static_cast<int>(secs_in_day % 60LL);
    f.yday0  = static_cast<int>(days - ta_ymd_to_days(f.year, 1, 1));
    f.wday   = static_cast<int>(((days % 7) + 7 + 4) % 7);  // 1970-01-01 = Thu
    return f;
}
