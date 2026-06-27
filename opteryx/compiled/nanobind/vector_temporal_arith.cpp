// opteryx/compiled/nanobind/vector_temporal_arith.cpp — Milestone E.13, Phase 12, C′.
//
// C′ pattern: pure nanobind C++, zero Cython.  One NB_MODULE, four functions:
//
//   vector_date_part(v, part)            — DATE32|TIMESTAMP64 → DRAKEN_INT64 per row.
//   vector_date_diff(start, end, part)   — TIMESTAMP64+TIMESTAMP64 → DRAKEN_INT64.
//   vector_date_trunc(v, unit)           — DATE32|TIMESTAMP64 → DRAKEN_TIMESTAMP64.
//   vector_date_format(v, fmt)           — DATE32|TIMESTAMP64 → DRAKEN_VARCHAR.
//
// Calendar math via draken/ops/temporal_arith.h (Howard Hinnant proleptic Gregorian).
//
// date_part supported parts:
//   year, month, day, quarter, dayofyear, dayofweek (0=Monday), hour, minute, second
//   (case-insensitive; "dow" alias for dayofweek, "doy" alias for dayofyear)
//
// date_diff supported units (plural form):
//   microseconds, milliseconds, seconds, minutes, hours, days, weeks, months, quarters, years
//   month/quarter/year use approximate arithmetic (days÷30/91/365) matching old .pyx.
//
// date_trunc supported units (case-insensitive):
//   year, quarter, month, week, day, hour, minute, second
//   DATE32 input → TIMESTAMP64 output in microseconds (matches old vector_date_trunc.pyx).
//
// date_format:
//   Format string is validated at the edge; unsupported %X tokens raise ValueError.
//   Produces DRAKEN_VARCHAR via draken_vector_own_string.
//   Output uses gmtime (UTC). Sub-second components are truncated before formatting.
//
// Null TVL: null input row → null output row; validity bitmap copied from input.
// Fail loud: TypeError on non-Vector or wrong DrakenType; ValueError on bad part/unit/fmt.
//
// Replaces: opteryx/compiled/vector_ops/vector_date_part.pyx
//           opteryx/compiled/vector_ops/vector_date_diff.pyx
//           opteryx/compiled/vector_ops/vector_date_trunc.pyx
//           opteryx/compiled/vector_ops/vector_date_format.pyx

#include <Python.h>
#include <nanobind/nanobind.h>
#include <cstdint>
#include <cstring>
#include <cctype>
#include <cstdio>
#include <ctime>
#include <stdexcept>
#include <vector>

#include "core/buffers.h"
#include "core/alloc.h"
#include "core/draken_bridge.h"
#include "core/string_slot.h"
#include "logical_type.h"   // TimestampUnit
#include "ops/temporal_arith.h"

namespace nb = nanobind;

// ---------------------------------------------------------------------------
// Shared helpers (null / validity / unit / scalar extraction)
// ---------------------------------------------------------------------------

static inline bool row_is_null(const DrakenVector* dv, uint32_t i) noexcept {
    if (!dv->validity) return false;
    return !((dv->validity[i >> 3] >> (i & 7u)) & 1u);
}

// Extract scalar string from a Python sequence-like object (vector or list).
// Returns an OWNING std::string: the source is `seq[0]`, a temporary Python
// object whose UTF-8 buffer is freed when it goes out of scope — returning a
// borrowed `const char*` into it is a use-after-free. Callers keep the
// std::string alive and pass `.c_str()`.
static std::string extract_scalar_string(nb::object seq) {
    try {
        nb::object first = seq[0];
        if (PyUnicode_Check(first.ptr())) {
            const char* s = PyUnicode_AsUTF8(first.ptr());
            if (!s) throw nb::python_error();
            return std::string(s);
        } else if (PyBytes_Check(first.ptr())) {
            const char* s = PyBytes_AS_STRING(first.ptr());
            if (!s) throw nb::python_error();
            return std::string(s);
        } else {
            // Try str() conversion
            nb::object str_obj = nb::cast<nb::object>(nb::str(first));
            const char* s = PyUnicode_AsUTF8(str_obj.ptr());
            if (!s) throw nb::python_error();
            return std::string(s);
        }
    } catch (const std::exception& e) {
        PyErr_SetString(PyExc_TypeError, "Failed to extract scalar from constant vector");
        throw nb::python_error();
    }
}

static uint8_t* copy_validity(const DrakenVector* dv) {
    if (!dv->validity) return nullptr;
    const uint32_t bm     = (dv->length + 7u) >> 3;
    const uint32_t padded = (bm + 7u) & ~7u;
    const size_t   vbytes = padded > 0u ? padded : 8u;
    uint8_t* out = static_cast<uint8_t*>(draken_malloc(vbytes));
    if (!out) throw std::bad_alloc();
    std::memcpy(out, dv->validity, vbytes);
    return out;
}

// Read the timestamp unit as a code (0-3) from the logical_type_unit attribute.
static int get_ts_unit_code(nb::object obj) {
    PyObject* raw = PyObject_GetAttrString(obj.ptr(), "logical_type_unit");
    if (!raw) throw nb::python_error();
    nb::object unit_obj = nb::steal<nb::object>(raw);
    if (unit_obj.is_none())
        throw nb::type_error(
            "TIMESTAMP64 vector is missing mandatory logical_type_unit descriptor");
    const char* s = PyUnicode_AsUTF8(unit_obj.ptr());
    if (!s) throw nb::python_error();
    if (std::strcmp(s, "s")  == 0) return 0;
    if (std::strcmp(s, "ms") == 0) return 1;
    if (std::strcmp(s, "us") == 0) return 2;
    if (std::strcmp(s, "ns") == 0) return 3;
    PyErr_Format(PyExc_ValueError,
        "vector_temporal_arith: unknown timestamp unit '%s'; expected 's', 'ms', 'us', 'ns'", s);
    throw nb::python_error();
}

static const char* unit_code_to_str(int uc) noexcept {
    switch (uc) {
        case 0: return "s";
        case 1: return "ms";
        case 2: return "us";
        case 3: return "ns";
        default: return "us";
    }
}

// Case-insensitive ASCII comparison.
static bool ci_eq(const char* a, const char* b) noexcept {
    while (*a && *b) {
        if (std::tolower(static_cast<unsigned char>(*a)) !=
            std::tolower(static_cast<unsigned char>(*b))) return false;
        ++a; ++b;
    }
    return *a == '\0' && *b == '\0';
}

// ---------------------------------------------------------------------------
// date_part — part kind mapping
// part_kind encoding (internal):
//   0=year  1=month  2=day  3=quarter  4=dayofyear  5=dayofweek
//   6=hour  7=minute 8=second
// ---------------------------------------------------------------------------

static int parse_part_kind(const char* part) {
    if (ci_eq(part, "year"))                           return 0;
    if (ci_eq(part, "month"))                          return 1;
    if (ci_eq(part, "day"))                            return 2;
    if (ci_eq(part, "quarter"))                        return 3;
    if (ci_eq(part, "dayofyear") || ci_eq(part, "doy")) return 4;
    if (ci_eq(part, "dayofweek") || ci_eq(part, "dow")) return 5;
    if (ci_eq(part, "hour"))                           return 6;
    if (ci_eq(part, "minute"))                         return 7;
    if (ci_eq(part, "second") || ci_eq(part, "seconds")) return 8;
    return -1;
}

// ---------------------------------------------------------------------------
// date_trunc — trunc kind mapping
// trunc_kind encoding: 0=year 1=month 2=quarter 3=week 4=day 5=hour 6=minute 7=second
// ---------------------------------------------------------------------------

static int parse_trunc_kind(const char* unit) {
    if (ci_eq(unit, "year"))    return 0;
    if (ci_eq(unit, "month"))   return 1;
    if (ci_eq(unit, "quarter")) return 2;
    if (ci_eq(unit, "week"))    return 3;
    if (ci_eq(unit, "day"))     return 4;
    if (ci_eq(unit, "hour"))    return 5;
    if (ci_eq(unit, "minute"))  return 6;
    if (ci_eq(unit, "second"))  return 7;
    return -1;
}

// ---------------------------------------------------------------------------
// date_diff — diff kind mapping (plural form)
// ---------------------------------------------------------------------------

static int parse_diff_kind(const char* part) {
    if (ci_eq(part, "microseconds") || ci_eq(part, "microsecond")) return 0;
    if (ci_eq(part, "milliseconds") || ci_eq(part, "millisecond")) return 1;
    if (ci_eq(part, "seconds")      || ci_eq(part, "second"))      return 2;
    if (ci_eq(part, "minutes")      || ci_eq(part, "minute"))      return 3;
    if (ci_eq(part, "hours")        || ci_eq(part, "hour"))        return 4;
    if (ci_eq(part, "days")         || ci_eq(part, "day"))         return 5;
    if (ci_eq(part, "weeks")        || ci_eq(part, "week"))        return 6;
    if (ci_eq(part, "months")       || ci_eq(part, "month"))       return 7;
    if (ci_eq(part, "quarters")     || ci_eq(part, "quarter"))     return 8;
    if (ci_eq(part, "years")        || ci_eq(part, "year"))        return 9;
    return -1;
}

// ---------------------------------------------------------------------------
// date_format — format string validation
// Whitelists all POSIX/C99 strftime specifiers. Raises on unknown %X tokens.
// ---------------------------------------------------------------------------

static void validate_date_format(const char* fmt) {
    // All standard POSIX/C99 strftime single-character specifiers.
    static const char* known = "YymdHIMSpjAaBbZenwtuVUWcxXFTRzGgklPqr";
    for (const char* p = fmt; *p; ++p) {
        if (*p != '%') continue;
        ++p;
        if (!*p) {
            PyErr_SetString(PyExc_ValueError,
                "vector_date_format: trailing '%' in format string");
            throw nb::python_error();
        }
        if (*p == '%' || *p == 'n' || *p == 't') continue;  // %% %n %t always ok
        if (!std::strchr(known, *p)) {
            PyErr_Format(PyExc_ValueError,
                "vector_date_format: unsupported format token '%%%c'; "
                "use standard strftime specifiers (%%Y, %%m, %%d, %%H, %%M, %%S, etc.)",
                (unsigned char)*p);
            throw nb::python_error();
        }
    }
}

// ---------------------------------------------------------------------------
// impl_date_part
// ---------------------------------------------------------------------------

static nb::object impl_date_part(nb::object v_obj, const char* part_str) {
    const int part_kind = parse_part_kind(part_str);
    if (part_kind < 0) {
        PyErr_Format(PyExc_ValueError,
            "vector_date_part: unsupported part '%s'; "
            "supported: year, month, day, quarter, dayofyear, dayofweek, hour, minute, second",
            part_str);
        throw nb::python_error();
    }

    const DrakenVector* dv = draken_vector_unwrap(v_obj.ptr());
    if (!dv) throw nb::python_error();

    if (dv->type != DRAKEN_TIMESTAMP64 && dv->type != DRAKEN_DATE32) {
        PyErr_Format(PyExc_TypeError,
            "vector_date_part: expected DRAKEN_TIMESTAMP64 or DRAKEN_DATE32 Vector, got type %d",
            static_cast<int>(dv->type));
        throw nb::python_error();
    }

    const bool is_date32 = (dv->type == DRAKEN_DATE32);
    const int  unit_code = is_date32 ? 2 : get_ts_unit_code(v_obj);
    const uint32_t n     = dv->length;

    const size_t data_sz = (n > 0u ? n : 1u) * sizeof(int64_t);
    int64_t* out = static_cast<int64_t*>(draken_malloc(data_sz));
    if (!out) throw std::bad_alloc();

    struct Guard { int64_t* d; uint8_t* v;
        ~Guard() { if (d) draken_free(d); if (v) draken_free(v); } } g{out, nullptr};

    g.v = copy_validity(dv);

    const int64_t tps = ta_ticks_per_second(unit_code);

    for (uint32_t i = 0u; i < n; ++i) {
        if (row_is_null(dv, i)) { out[i] = 0; continue; }

        int64_t days, secs_in_day;
        if (is_date32) {
            const int32_t* src = static_cast<const int32_t*>(dv->data);
            days = static_cast<int64_t>(src[dv->selection[i]]);
            secs_in_day = 0;
        } else {
            const int64_t* src = static_cast<const int64_t*>(dv->data);
            const int64_t  v   = src[dv->selection[i]];
            const int64_t  sec = ta_floor_div(v, tps);
            days        = ta_floor_div(sec, 86400LL);
            secs_in_day = sec - days * 86400LL;  // always [0, 86400)
        }

        int64_t result;
        switch (part_kind) {
            case 0: { // year
                int yr, mo, dy; ta_days_to_ymd(days, &yr, &mo, &dy);
                result = yr; break;
            }
            case 1: { // month
                int yr, mo, dy; ta_days_to_ymd(days, &yr, &mo, &dy);
                result = mo; break;
            }
            case 2: { // day
                int yr, mo, dy; ta_days_to_ymd(days, &yr, &mo, &dy);
                result = dy; break;
            }
            case 3: { // quarter
                int yr, mo, dy; ta_days_to_ymd(days, &yr, &mo, &dy);
                result = (mo - 1) / 3 + 1; break;
            }
            case 4: { // dayofyear (1-indexed)
                int yr, mo, dy; ta_days_to_ymd(days, &yr, &mo, &dy);
                result = days - ta_ymd_to_days(yr, 1, 1) + 1; break;
            }
            case 5: { // dayofweek: 0=Monday…6=Sunday; epoch (1970-01-01) was Thursday=3
                result = (((days + 3LL) % 7 + 7) % 7);
                break;
            }
            case 6: result = secs_in_day / 3600LL;          break; // hour
            case 7: result = (secs_in_day % 3600LL) / 60LL; break; // minute
            case 8: result = secs_in_day % 60LL;             break; // second
            default: result = 0; break;
        }
        out[i] = result;
    }

    uint8_t* validity = g.v;
    g.d = nullptr; g.v = nullptr;
    PyObject* result = draken_vector_own_raw(out, validity, n, DRAKEN_INT64);
    if (!result) throw nb::python_error();
    return nb::steal<nb::object>(result);
}

// ---------------------------------------------------------------------------
// impl_date_diff
// ---------------------------------------------------------------------------

static nb::object impl_date_diff(nb::object start_obj, nb::object end_obj,
                                  const char* part_str) {
    const int diff_kind = parse_diff_kind(part_str);
    if (diff_kind < 0) {
        PyErr_Format(PyExc_ValueError,
            "vector_date_diff: unsupported part '%s'; "
            "supported: microseconds, milliseconds, seconds, minutes, hours, days, "
            "weeks, months, quarters, years",
            part_str);
        throw nb::python_error();
    }

    const DrakenVector* ds = draken_vector_unwrap(start_obj.ptr());
    const DrakenVector* de = draken_vector_unwrap(end_obj.ptr());
    if (!ds || !de) throw nb::python_error();
    if (ds->type != DRAKEN_TIMESTAMP64) {
        throw nb::type_error("vector_date_diff: start must be DRAKEN_TIMESTAMP64");
    }
    if (de->type != DRAKEN_TIMESTAMP64) {
        throw nb::type_error("vector_date_diff: end must be DRAKEN_TIMESTAMP64");
    }
    if (ds->length != de->length) {
        PyErr_Format(PyExc_ValueError,
            "vector_date_diff: start length %u != end length %u", ds->length, de->length);
        throw nb::python_error();
    }

    const int      ucs = get_ts_unit_code(start_obj);
    const int      uce = get_ts_unit_code(end_obj);
    const uint32_t n   = ds->length;

    const size_t data_sz = (n > 0u ? n : 1u) * sizeof(int64_t);
    int64_t* out = static_cast<int64_t*>(draken_malloc(data_sz));
    if (!out) throw std::bad_alloc();

    struct Guard { int64_t* d; uint8_t* v;
        ~Guard() { if (d) draken_free(d); if (v) draken_free(v); } } g{out, nullptr};

    // Validity: row is null if either input is null.
    const bool has_null_s = (ds->validity != nullptr);
    const bool has_null_e = (de->validity != nullptr);
    uint8_t* out_validity = nullptr;
    if (has_null_s || has_null_e) {
        const uint32_t bm     = (n + 7u) >> 3;
        const uint32_t padded = (bm + 7u) & ~7u;
        const size_t   vbytes = padded > 0u ? padded : 8u;
        out_validity = static_cast<uint8_t*>(draken_malloc(vbytes));
        if (!out_validity) throw std::bad_alloc();
        std::memset(out_validity, 0xFFu, vbytes);
        g.v = out_validity;

        for (uint32_t i = 0u; i < n; ++i) {
            const bool ns = has_null_s && !((ds->validity[i >> 3] >> (i & 7u)) & 1u);
            const bool ne = has_null_e && !((de->validity[i >> 3] >> (i & 7u)) & 1u);
            if (ns || ne) out_validity[i >> 3] &= ~(1u << (i & 7u));
        }
    }

    // Gather start/end values into flat arrays for the batch kernel.
    const int64_t* s_data = static_cast<const int64_t*>(ds->data);
    const int64_t* e_data = static_cast<const int64_t*>(de->data);

    const size_t flat_sz = (n > 0u ? n : 1u) * sizeof(int64_t);
    int64_t* s_flat = static_cast<int64_t*>(draken_malloc(flat_sz));
    int64_t* e_flat = static_cast<int64_t*>(draken_malloc(flat_sz));
    if (!s_flat || !e_flat) { draken_free(s_flat); draken_free(e_flat); throw std::bad_alloc(); }

    struct FlatGuard { int64_t* s; int64_t* e;
        ~FlatGuard() { if (s) draken_free(s); if (e) draken_free(e); } } fg{s_flat, e_flat};

    for (uint32_t i = 0u; i < n; ++i) {
        s_flat[i] = row_is_null(ds, i) ? 0 : s_data[ds->selection[i]];
        e_flat[i] = row_is_null(de, i) ? 0 : e_data[de->selection[i]];
    }

    date_diff_batch(s_flat, e_flat, n, ucs, uce, diff_kind, out);

    fg.s = nullptr; fg.e = nullptr;
    uint8_t* validity = g.v;
    g.d = nullptr; g.v = nullptr;
    PyObject* result = draken_vector_own_raw(out, validity, n, DRAKEN_INT64);
    if (!result) throw nb::python_error();
    return nb::steal<nb::object>(result);
}

// ---------------------------------------------------------------------------
// impl_date_trunc
// ---------------------------------------------------------------------------

static nb::object impl_date_trunc(nb::object v_obj, const char* unit_str) {
    const int trunc_kind = parse_trunc_kind(unit_str);
    if (trunc_kind < 0) {
        PyErr_Format(PyExc_ValueError,
            "vector_date_trunc: unsupported unit '%s'; "
            "supported: year, quarter, month, week, day, hour, minute, second",
            unit_str);
        throw nb::python_error();
    }

    const DrakenVector* dv = draken_vector_unwrap(v_obj.ptr());
    if (!dv) throw nb::python_error();

    if (dv->type != DRAKEN_TIMESTAMP64 && dv->type != DRAKEN_DATE32) {
        PyErr_Format(PyExc_TypeError,
            "vector_date_trunc: expected DRAKEN_TIMESTAMP64 or DRAKEN_DATE32 Vector, got type %d",
            static_cast<int>(dv->type));
        throw nb::python_error();
    }

    const bool is_date32 = (dv->type == DRAKEN_DATE32);
    // DATE32 → output as microseconds (matches old vector_date_trunc.pyx behaviour).
    const int  unit_code = is_date32 ? 2 : get_ts_unit_code(v_obj);
    const uint32_t n     = dv->length;

    const size_t data_sz = (n > 0u ? n : 1u) * sizeof(int64_t);
    int64_t* out = static_cast<int64_t*>(draken_malloc(data_sz));
    if (!out) throw std::bad_alloc();

    struct Guard { int64_t* d; uint8_t* v;
        ~Guard() { if (d) draken_free(d); if (v) draken_free(v); } } g{out, nullptr};

    g.v = copy_validity(dv);

    // Gather into a flat buffer, converting DATE32→int64 ticks (microseconds).
    const size_t flat_sz = (n > 0u ? n : 1u) * sizeof(int64_t);
    int64_t* src_flat = static_cast<int64_t*>(draken_malloc(flat_sz));
    if (!src_flat) throw std::bad_alloc();

    struct FlatGuard { int64_t* p; ~FlatGuard() { if (p) draken_free(p); } } fg{src_flat};

    if (is_date32) {
        const int32_t* src = static_cast<const int32_t*>(dv->data);
        const int64_t  tpd = ta_ticks_per_day(2);  // microseconds per day
        for (uint32_t i = 0u; i < n; ++i) {
            if (row_is_null(dv, i)) { src_flat[i] = 0; continue; }
            src_flat[i] = static_cast<int64_t>(src[dv->selection[i]]) * tpd;
        }
    } else {
        const int64_t* src = static_cast<const int64_t*>(dv->data);
        for (uint32_t i = 0u; i < n; ++i) {
            if (row_is_null(dv, i)) { src_flat[i] = 0; continue; }
            src_flat[i] = src[dv->selection[i]];
        }
    }

    date_trunc_batch(src_flat, n, unit_code, trunc_kind, out);

    fg.p = nullptr;
    uint8_t* validity = g.v;
    g.d = nullptr; g.v = nullptr;
    PyObject* result = draken_vector_own_timestamp(out, validity, n, unit_code_to_str(unit_code));
    if (!result) throw nb::python_error();
    return nb::steal<nb::object>(result);
}

// ---------------------------------------------------------------------------
// date_format — compiled formatter
//
// strftime() re-parses the format string and walks the locale tables on every
// call, and gmtime_r() does a full calendar conversion per call.  Both costs
// are constant-per-row work paid once for every row.  Instead we compile the
// (constant) format string once into a token program, decompose each timestamp
// with the in-tree branchless ta_days_to_ymd (same path DATE_PART uses, no
// gmtime_r), and emit bytes directly.
//
// The engine always operates in UTC (gmtime_r) under the C locale, so every
// whitelisted specifier has a fixed, locale-independent definition.  Output is
// byte-identical to the old strftime(gmtime_r) path; see the parity test in
// tests/.../test_date_format_parity.py.  Deliberate pins:
//   %z → "+0000", %Z → "GMT"  (UTC — the only timezone-coupled specifiers)
//   %q → quarter (1-4)         (Opteryx extension; %q is not a real strftime spec)
// ---------------------------------------------------------------------------

namespace {

// C-locale English names, UTC.  Index by wday (0=Sunday) / month-1.
static const char* const DF_WDAY_ABBR[7] =
    {"Sun","Mon","Tue","Wed","Thu","Fri","Sat"};
static const char* const DF_WDAY_FULL[7] =
    {"Sunday","Monday","Tuesday","Wednesday","Thursday","Friday","Saturday"};
static const char* const DF_MON_ABBR[12] =
    {"Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"};
static const char* const DF_MON_FULL[12] =
    {"January","February","March","April","May","June",
     "July","August","September","October","November","December"};
static const char DF_NL  = '\n';
static const char DF_TAB = '\t';

// Broken-down UTC fields for one row.
struct DfFields {
    int64_t days;     // days since 1970-01-01
    int year, month, day;
    int hour, minute, second;
    int yday0;        // 0-based day of year
    int wday;         // 0=Sunday … 6=Saturday
};

// A compiled token: a literal run (spec==0) or a single specifier.
struct DfToken {
    const char* lit;      // literal bytes (point into fmt buffer or a static char)
    uint32_t    lit_len;
    char        spec;     // 0 → literal; else the specifier character
};

// --- direct digit writers (no snprintf) ------------------------------------
static inline char* df_put2(char* p, int v) {
    *p++ = static_cast<char>('0' + (v / 10) % 10);
    *p++ = static_cast<char>('0' + v % 10);
    return p;
}
static inline char* df_put2sp(char* p, int v) {  // space-padded 2-wide
    const int t = v / 10;
    *p++ = t ? static_cast<char>('0' + t) : ' ';
    *p++ = static_cast<char>('0' + v % 10);
    return p;
}
static inline char* df_put3(char* p, int v) {    // zero-padded 3-wide
    *p++ = static_cast<char>('0' + (v / 100) % 10);
    *p++ = static_cast<char>('0' + (v / 10) % 10);
    *p++ = static_cast<char>('0' + v % 10);
    return p;
}
// Year, sign for negatives, zero-padded to at least minw (glibc pads %Y to 4).
static char* df_put_year(char* p, int y, int minw) {
    if (y < 0) { *p++ = '-'; y = -y; }
    char tmp[16];
    int  k = 0;
    if (y == 0) tmp[k++] = '0';
    while (y) { tmp[k++] = static_cast<char>('0' + y % 10); y /= 10; }
    while (k < minw) tmp[k++] = '0';
    while (k) *p++ = tmp[--k];
    return p;
}
static inline char* df_puts(char* p, const char* s) {
    while (*s) *p++ = *s++;
    return p;
}

// ISO-8601 week date: the week's Thursday determines the ISO year.
static void df_iso_week(int64_t days, int wday, int* iso_year, int* iso_week) {
    const int iso_dow = (wday == 0) ? 7 : wday;        // Mon=1 … Sun=7
    const int64_t thu = days - (iso_dow - 4);
    int y, m, d; ta_days_to_ymd(thu, &y, &m, &d);
    *iso_year = y;
    *iso_week = static_cast<int>((thu - ta_ymd_to_days(y, 1, 1)) / 7) + 1;
}

// Emit one specifier; returns the advanced write pointer.
static char* df_emit_spec(char* p, char c, const DfFields& f) {
    switch (c) {
        case 'Y': return df_put_year(p, f.year, 4);
        case 'y': return df_put2(p, ((f.year % 100) + 100) % 100);
        case 'm': return df_put2(p, f.month);
        case 'd': return df_put2(p, f.day);
        case 'e': return df_put2sp(p, f.day);
        case 'H': return df_put2(p, f.hour);
        case 'k': return df_put2sp(p, f.hour);
        case 'I': return df_put2(p, ((f.hour + 11) % 12) + 1);
        case 'l': return df_put2sp(p, ((f.hour + 11) % 12) + 1);
        case 'M': return df_put2(p, f.minute);
        case 'S': return df_put2(p, f.second);
        case 'p': return df_puts(p, f.hour < 12 ? "AM" : "PM");
        case 'P': return df_puts(p, f.hour < 12 ? "am" : "pm");
        case 'j': return df_put3(p, f.yday0 + 1);
        case 'a': return df_puts(p, DF_WDAY_ABBR[f.wday]);
        case 'A': return df_puts(p, DF_WDAY_FULL[f.wday]);
        case 'b': return df_puts(p, DF_MON_ABBR[f.month - 1]);
        case 'B': return df_puts(p, DF_MON_FULL[f.month - 1]);
        case 'w': *p++ = static_cast<char>('0' + f.wday); return p;
        case 'u': *p++ = static_cast<char>('0' + (f.wday == 0 ? 7 : f.wday)); return p;
        case 'q': *p++ = static_cast<char>('0' + (f.month - 1) / 3 + 1); return p;
        case 'Z': return df_puts(p, "GMT");
        case 'z': return df_puts(p, "+0000");
        case 'U': return df_put2(p, (f.yday0 - f.wday + 7) / 7);
        case 'W': return df_put2(p, (f.yday0 - ((f.wday + 6) % 7) + 7) / 7);
        case 'V': { int iy, iw; df_iso_week(f.days, f.wday, &iy, &iw);
                    return df_put2(p, iw); }
        case 'G': { int iy, iw; df_iso_week(f.days, f.wday, &iy, &iw);
                    return df_put_year(p, iy, 4); }
        case 'g': { int iy, iw; df_iso_week(f.days, f.wday, &iy, &iw);
                    return df_put2(p, ((iy % 100) + 100) % 100); }
        // Composite specifiers (C locale).
        case 'F': p = df_put_year(p, f.year, 4); *p++ = '-';
                  p = df_put2(p, f.month); *p++ = '-';
                  return df_put2(p, f.day);
        case 'T': p = df_put2(p, f.hour); *p++ = ':';
                  p = df_put2(p, f.minute); *p++ = ':';
                  return df_put2(p, f.second);
        case 'R': p = df_put2(p, f.hour); *p++ = ':';
                  return df_put2(p, f.minute);
        case 'r': p = df_put2(p, ((f.hour + 11) % 12) + 1); *p++ = ':';
                  p = df_put2(p, f.minute); *p++ = ':';
                  p = df_put2(p, f.second); *p++ = ' ';
                  return df_puts(p, f.hour < 12 ? "AM" : "PM");
        case 'x': p = df_put2(p, f.month); *p++ = '/';
                  p = df_put2(p, f.day); *p++ = '/';
                  return df_put2(p, ((f.year % 100) + 100) % 100);
        case 'X': p = df_put2(p, f.hour); *p++ = ':';
                  p = df_put2(p, f.minute); *p++ = ':';
                  return df_put2(p, f.second);
        case 'c': p = df_puts(p, DF_WDAY_ABBR[f.wday]); *p++ = ' ';
                  p = df_puts(p, DF_MON_ABBR[f.month - 1]); *p++ = ' ';
                  p = df_put2sp(p, f.day); *p++ = ' ';
                  p = df_put2(p, f.hour); *p++ = ':';
                  p = df_put2(p, f.minute); *p++ = ':';
                  p = df_put2(p, f.second); *p++ = ' ';
                  return df_put_year(p, f.year, 4);
        default:  *p++ = c; return p;   // unreachable after validation
    }
}

// Worst-case bytes a single specifier can emit (used to size the row buffer).
static uint32_t df_spec_max_width(char c) {
    switch (c) {
        case 'Y': case 'G': return 12;     // sign + large year
        case 'A': case 'B': return 9;      // "Wednesday" / "September"
        case 'c': return 36;
        case 'F': return 20;       // year(≤12) + "-MM-DD"
        case 'r': return 12;
        case 'x': case 'X': case 'T': return 10;
        case 'z': return 5;
        case 'a': case 'b': case 'Z': case 'j': return 3;
        case 'R': return 5;
        case 'w': case 'u': case 'q': return 1;
        default:  return 2;                // numeric 2-wide fields
    }
}

// Compile the (validated) format string into a token program; returns the
// worst-case output length via *max_len.
static std::vector<DfToken> df_compile(const char* fmt, size_t* max_len) {
    std::vector<DfToken> prog;
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
            prog.push_back({&DF_NL, 1, 0}); width += 1;
        } else if (spec == 't') {
            prog.push_back({&DF_TAB, 1, 0}); width += 1;
        } else {
            prog.push_back({nullptr, 0, spec});
            width += df_spec_max_width(spec);
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

}  // namespace

// ---------------------------------------------------------------------------
// impl_date_format
// ---------------------------------------------------------------------------

static nb::object impl_date_format(nb::object v_obj, const char* fmt) {
    validate_date_format(fmt);

    const DrakenVector* dv = draken_vector_unwrap(v_obj.ptr());
    if (!dv) throw nb::python_error();

    if (dv->type != DRAKEN_TIMESTAMP64 && dv->type != DRAKEN_DATE32) {
        PyErr_Format(PyExc_TypeError,
            "vector_date_format: expected DRAKEN_TIMESTAMP64 or DRAKEN_DATE32 Vector, got type %d",
            static_cast<int>(dv->type));
        throw nb::python_error();
    }

    const bool is_date32 = (dv->type == DRAKEN_DATE32);
    const int  unit_code = is_date32 ? 2 : get_ts_unit_code(v_obj);
    const int64_t tps    = ta_ticks_per_second(unit_code);
    const uint32_t n     = dv->length;

    // Compile the constant format string once into a token program; size the
    // per-row scratch buffer to its worst-case output.
    size_t max_row_len = 0;
    const std::vector<DfToken> prog = df_compile(fmt, &max_row_len);
    std::vector<char> row_vec(max_row_len);
    char* const row_buf = row_vec.data();

    // Allocate slot array and an arena.  Arena worst case: n × 64 bytes.
    // Strings that fit inline (≤12 bytes) do not consume arena space.
    const size_t slots_sz = (n > 0u ? n : 1u) * sizeof(DrakenStringSlot);
    auto* slots = static_cast<DrakenStringSlot*>(draken_malloc(slots_sz));
    if (!slots) throw std::bad_alloc();
    std::memset(slots, 0, slots_sz);

    // Initial arena estimate; will grow dynamically if needed.
    size_t arena_cap = (n > 0u ? static_cast<size_t>(n) * 32u : 32u);
    uint8_t* arena   = static_cast<uint8_t*>(draken_malloc(arena_cap));
    if (!arena) { draken_free(slots); throw std::bad_alloc(); }

    struct Guard {
        DrakenStringSlot* s; uint8_t* a; uint8_t* v;
        ~Guard() { if (s) draken_free(s); if (a) draken_free(a); if (v) draken_free(v); }
    } g{slots, arena, nullptr};

    g.v = copy_validity(dv);
    size_t arena_used = 0u;

    for (uint32_t i = 0u; i < n; ++i) {
        if (row_is_null(dv, i)) {
            str_init_null(&slots[i]);
            continue;
        }

        // Decompose this row's timestamp into UTC calendar fields — branchless,
        // no gmtime_r (same math DATE_PART uses).
        int64_t days, secs_in_day;
        if (is_date32) {
            const int32_t* src = static_cast<const int32_t*>(dv->data);
            days        = static_cast<int64_t>(src[dv->selection[i]]);
            secs_in_day = 0;
        } else {
            const int64_t* src = static_cast<const int64_t*>(dv->data);
            const int64_t  sec = ta_floor_div(src[dv->selection[i]], tps);
            days        = ta_floor_div(sec, 86400LL);
            secs_in_day = sec - days * 86400LL;  // always [0, 86400)
        }

        DfFields f;
        ta_days_to_ymd(days, &f.year, &f.month, &f.day);
        f.days   = days;
        f.hour   = static_cast<int>(secs_in_day / 3600LL);
        f.minute = static_cast<int>((secs_in_day % 3600LL) / 60LL);
        f.second = static_cast<int>(secs_in_day % 60LL);
        f.yday0  = static_cast<int>(days - ta_ymd_to_days(f.year, 1, 1));
        f.wday   = static_cast<int>(((days % 7) + 7 + 4) % 7);  // 1970-01-01 = Thu

        char* p = row_buf;
        for (const DfToken& tok : prog) {
            if (tok.spec == 0) {
                std::memcpy(p, tok.lit, tok.lit_len);
                p += tok.lit_len;
            } else {
                p = df_emit_spec(p, tok.spec, f);
            }
        }
        const uint32_t slen = static_cast<uint32_t>(p - row_buf);

        if (slen <= 12u) {
            str_init_inline(&slots[i], reinterpret_cast<const uint8_t*>(row_buf), slen);
        } else {
            // Ensure arena has capacity.
            if (arena_used + slen > arena_cap) {
                arena_cap = (arena_used + slen) * 2u;
                uint8_t* new_arena = static_cast<uint8_t*>(draken_malloc(arena_cap));
                if (!new_arena) throw std::bad_alloc();
                std::memcpy(new_arena, g.a, arena_used);
                draken_free(g.a);
                g.a = new_arena;
                arena = new_arena;
            }
            const uint32_t arena_off = static_cast<uint32_t>(arena_used);
            std::memcpy(arena + arena_off, row_buf, slen);
            draken_build_string_slot(&slots[i], reinterpret_cast<const uint8_t*>(row_buf),
                                     slen, arena_off);
            arena_used += slen;
        }
    }

    uint8_t* validity = g.v;
    DrakenStringSlot* out_slots = g.s;
    uint8_t*          out_arena = g.a;
    g.s = nullptr; g.a = nullptr; g.v = nullptr;

    PyObject* result = draken_vector_own_string(
        out_slots, out_arena, arena_used, validity, n, DRAKEN_VARCHAR);
    if (!result) throw nb::python_error();
    return nb::steal<nb::object>(result);
}

// ---------------------------------------------------------------------------
// Dispatch wrappers (constant vector unwrapping)
// These are called from the Python dispatch layer and handle scalar extraction.
// ---------------------------------------------------------------------------

static nb::object dispatch_date_part(nb::object part_seq, nb::object arr) {
    const std::string part = extract_scalar_string(part_seq);
    return impl_date_part(arr, part.c_str());
}

static nb::object dispatch_trunc_date(nb::object arr, nb::object part_seq) {
    const std::string unit = extract_scalar_string(part_seq);
    return impl_date_trunc(arr, unit.c_str());
}

static nb::object dispatch_trunc_timestamp(nb::object arr, nb::object part_seq) {
    const std::string unit = extract_scalar_string(part_seq);
    return impl_date_trunc(arr, unit.c_str());
}

static nb::object dispatch_date_diff(nb::object part_seq, nb::object start, nb::object end) {
    const std::string part = extract_scalar_string(part_seq);
    return impl_date_diff(start, end, part.c_str());
}

static nb::object dispatch_time_diff(nb::object time1, nb::object time2) {
    // time_diff hardcodes 'hours' as the unit
    return impl_date_diff(time1, time2, "hours");
}

static nb::object dispatch_date_format(nb::object dates, nb::object pattern_seq) {
    const std::string fmt = extract_scalar_string(pattern_seq);
    return impl_date_format(dates, fmt.c_str());
}

// ---------------------------------------------------------------------------
// NB_MODULE
// ---------------------------------------------------------------------------

void register_vector_temporal_arith(nb::module_ &m) {

    m.def("vector_date_part",
        [](nb::object v, nb::object part_obj) -> nb::object {
            if (!PyUnicode_Check(part_obj.ptr()))
                throw nb::type_error("part must be a str");
            const char* part = PyUnicode_AsUTF8(part_obj.ptr());
            if (!part) throw nb::python_error();
            return impl_date_part(v, part);
        },
        nb::arg("v"), nb::arg("part"),
        "DATE32|TIMESTAMP64 → DRAKEN_INT64. "
        "Extract one calendar component per row. "
        "part: 'year', 'month', 'day', 'quarter', 'dayofyear'/'doy', "
        "'dayofweek'/'dow' (0=Monday), 'hour', 'minute', 'second' (case-insensitive). "
        "DATE32 input: hour/minute/second always 0. "
        "Null rows propagate as null. Raises ValueError on unknown part.");

    m.def("vector_date_diff",
        [](nb::object start, nb::object end, nb::object part_obj) -> nb::object {
            if (!PyUnicode_Check(part_obj.ptr()))
                throw nb::type_error("part must be a str");
            const char* part = PyUnicode_AsUTF8(part_obj.ptr());
            if (!part) throw nb::python_error();
            return impl_date_diff(start, end, part);
        },
        nb::arg("start"), nb::arg("end"), nb::arg("part"),
        "TIMESTAMP64+TIMESTAMP64 → DRAKEN_INT64. "
        "Signed difference (end − start) in the requested unit. "
        "part (singular or plural, case-insensitive): microseconds, milliseconds, seconds, "
        "minutes, hours, days, weeks, months, quarters, years. "
        "month/quarter/year use approximate arithmetic (days÷30/91/365). "
        "Null rows propagate as null. Raises TypeError on non-TIMESTAMP64, "
        "ValueError on unsupported part, ValueError on mismatched lengths.");

    m.def("vector_date_trunc",
        [](nb::object v, nb::object unit_obj) -> nb::object {
            if (!PyUnicode_Check(unit_obj.ptr()))
                throw nb::type_error("unit must be a str");
            const char* unit = PyUnicode_AsUTF8(unit_obj.ptr());
            if (!unit) throw nb::python_error();
            return impl_date_trunc(v, unit);
        },
        nb::arg("v"), nb::arg("unit"),
        "DATE32|TIMESTAMP64 → DRAKEN_TIMESTAMP64. "
        "Calendar-aware floor to boundary. "
        "unit (case-insensitive): year, quarter, month, week, day, hour, minute, second. "
        "DATE32 input → TIMESTAMP64 output in microseconds (same as old vector_date_trunc.pyx). "
        "TIMESTAMP64 output carries the same unit descriptor as the input (D.8 invariant). "
        "Null rows propagate as null. Raises ValueError on unsupported unit.");

    m.def("vector_date_format",
        [](nb::object v, nb::object fmt_obj) -> nb::object {
            const char* fmt = nullptr;
            if (PyUnicode_Check(fmt_obj.ptr())) {
                fmt = PyUnicode_AsUTF8(fmt_obj.ptr());
            } else if (PyBytes_Check(fmt_obj.ptr())) {
                fmt = PyBytes_AS_STRING(fmt_obj.ptr());
            } else {
                throw nb::type_error("fmt must be str or bytes");
            }
            if (!fmt) throw nb::python_error();
            return impl_date_format(v, fmt);
        },
        nb::arg("v"), nb::arg("fmt"),
        "DATE32|TIMESTAMP64 → DRAKEN_VARCHAR. "
        "Format each temporal value using a strftime pattern. "
        "fmt: str or bytes. Validated at call time; unsupported %%X tokens raise ValueError. "
        "Supported: standard POSIX/C99 strftime specifiers (%%Y, %%m, %%d, %%H, %%M, %%S, etc.). "
        "Time is formatted as UTC. Sub-second components are truncated. "
        "Null input rows → null output rows. "
        "Raises TypeError on non-DATE32/TIMESTAMP64 input, ValueError on invalid format.");

    // Dispatch wrappers that handle constant vector unwrapping (called from Python layer).
    m.def("date_part", &dispatch_date_part, nb::arg("part"), nb::arg("arr"),
        "Extract datepart from vector (dispatcher for constant-wrapped part).");

    m.def("trunc_date", &dispatch_trunc_date, nb::arg("arr"), nb::arg("part"),
        "Truncate date to unit (dispatcher for constant-wrapped unit).");

    m.def("trunc_timestamp", &dispatch_trunc_timestamp, nb::arg("arr"), nb::arg("part"),
        "Truncate timestamp to unit (dispatcher for constant-wrapped unit).");

    m.def("date_diff", &dispatch_date_diff, nb::arg("part"), nb::arg("start"), nb::arg("end"),
        "Difference between timestamps (dispatcher for constant-wrapped part).");

    m.def("time_diff", &dispatch_time_diff, nb::arg("time1"), nb::arg("time2"),
        "Time difference in hours (dispatcher wrapper).");

    m.def("date_format", &dispatch_date_format, nb::arg("dates"), nb::arg("pattern"),
        "Format dates using pattern (dispatcher for constant-wrapped pattern).");
}
