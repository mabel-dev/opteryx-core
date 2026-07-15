#pragma once
// Shared value -> text formatting for the draken Vector JSON serializer and the
// rugo CSV / JSON(L) morsel writers. Lives in draken because it is the shared,
// pure renderer both sides depend on (rugo -> draken; never the reverse).
// Pure C++; no Python. Doubles use ryu (shortest round-trip); dates/timestamps
// render ISO-8601; decimals render with their scale.

#include <charconv>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <string>

#include "core/buffers.h"
#include "core/string_slot.h"
#include "ryu.h"  // third_party/ulfjack/ryu is on every consuming extension's include path

namespace rugo_text {

// Time unit codes (match draken TimestampUnit: 0=s,1=ms,2=us,3=ns).
enum { U_S = 0, U_MS = 1, U_US = 2, U_NS = 3 };

inline void fmt_int64(std::string &out, int64_t v) {
  char buf[24];
  std::to_chars_result r = std::to_chars(buf, buf + sizeof(buf), v);
  out.append(buf, r.ptr - buf);
}

// Fixed-width integer writers backed by a two-digit lookup table — the temporal
// formatters below emit only zero-padded fields of known width, so this replaces
// snprintf's format-string parsing / locale plumbing (~100-200ns/value) with a
// handful of table loads. Callers guarantee the value is in the field's range.
static constexpr char kTwoDigits[201] =
    "0001020304050607080910111213141516171819"
    "2021222324252627282930313233343536373839"
    "4041424344454647484950515253545556575859"
    "6061626364656667686970717273747576777879"
    "8081828384858687888990919293949596979899";
inline char *put2(char *p, int v) { // v in [0,99]
  p[0] = kTwoDigits[2 * v];
  p[1] = kTwoDigits[2 * v + 1];
  return p + 2;
}
inline char *put4(char *p, int v) { // v in [0,9999]
  put2(p, v / 100);
  put2(p + 2, v % 100);
  return p + 4;
}
inline char *put6(char *p, int v) { // v in [0,999999] (microseconds)
  put2(p, v / 10000);
  put2(p + 2, (v / 100) % 100);
  put2(p + 4, v % 100);
  return p + 6;
}

inline bool double_is_nan_or_inf(double v) { return std::isnan(v) || std::isinf(v); }

// Render a finite double as Python-style decimal text: plain fixed-point for
// ordinary magnitudes, scientific (lowercase e, signed 2+ digit exponent)
// only outside that range -- the same threshold CPython's repr()/json.dumps
// use (fixed for -4 <= exp < 16, scientific otherwise). ryu's d2s gives the
// shortest round-trippable digits but always in scientific form ("5.5E0");
// this reshapes those digits into the OData-compatible style.
inline void fmt_double(std::string &out, double v) {
  char sci[32];
  int n = d2s_buffered_n(v, sci);
  sci[n] = '\0'; // d2s_buffered_n does not null-terminate
  bool neg = sci[0] == '-';
  int p = neg ? 1 : 0;
  std::string digits;
  digits.push_back(sci[p++]);
  if (p < n && sci[p] == '.') {
    p++;
    while (sci[p] != 'E') digits.push_back(sci[p++]);
  }
  p++; // skip 'E'
  int exp = std::atoi(sci + p);

  if (exp >= -4 && exp < 16) {
    if (neg) out.push_back('-');
    if (exp < 0) {
      out.append("0.");
      out.append((size_t)(-exp - 1), '0');
      out.append(digits);
    } else {
      size_t intDigits = (size_t)exp + 1;
      if (digits.size() <= intDigits) {
        out.append(digits);
        out.append(intDigits - digits.size(), '0');
        out.append(".0");
      } else {
        out.append(digits, 0, intDigits);
        out.push_back('.');
        out.append(digits, intDigits, std::string::npos);
      }
    }
  } else {
    if (neg) out.push_back('-');
    out.push_back(digits[0]);
    if (digits.size() > 1) { out.push_back('.'); out.append(digits, 1, std::string::npos); }
    out.push_back('e');
    out.push_back(exp < 0 ? '-' : '+');
    int aexp = exp < 0 ? -exp : exp;
    char ebuf[8];
    int en = std::snprintf(ebuf, sizeof(ebuf), "%02d", aexp);
    out.append(ebuf, en);
  }
}

// Howard Hinnant's civil-from-days (days since 1970-01-01).
inline void civil_from_days(int64_t z, int &y, int &m, int &d) {
  z += 719468;
  int64_t era = (z >= 0 ? z : z - 146096) / 146097;
  int64_t doe = z - era * 146097;
  int64_t yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
  int64_t doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
  int64_t mp = (5 * doy + 2) / 153;
  d = (int)(doy - (153 * mp + 2) / 5 + 1);
  m = (int)(mp < 10 ? mp + 3 : mp - 9);
  y = (int)(yoe + era * 400 + (m <= 2));
}

inline void fmt_date(std::string &out, int32_t days) {
  int y, m, d;
  civil_from_days(days, y, m, d);
  if (y >= 0 && y <= 9999) { // fast path covers every representable calendar date
    char buf[10];
    char *p = put4(buf, y);
    *p++ = '-'; p = put2(p, m);
    *p++ = '-'; p = put2(p, d);
    out.append(buf, p - buf);
    return;
  }
  char buf[16]; // years outside [0,9999]: keep snprintf's %04d/sign behaviour
  int n = std::snprintf(buf, sizeof(buf), "%04d-%02d-%02d", y, m, d);
  out.append(buf, n);
}

inline int64_t to_micros(int64_t v, int unit) {
  switch (unit) {
  case U_S:  return v * 1000000LL;
  case U_MS: return v * 1000LL;
  case U_NS: return v / 1000LL;
  default:   return v; // U_US
  }
}

inline void fmt_timestamp(std::string &out, int64_t v, int unit) {
  int64_t us = to_micros(v, unit);
  int64_t day = 86400000000LL;
  int64_t days = us >= 0 ? us / day : -((-us + day - 1) / day); // floor div
  int64_t tod = us - days * day;                                // [0, day)
  int y, mo, d;
  civil_from_days(days, y, mo, d);
  int h = (int)(tod / 3600000000LL);
  int mi = (int)((tod / 60000000LL) % 60);
  int s = (int)((tod / 1000000LL) % 60);
  int frac = (int)(tod % 1000000LL);
  // RFC 3339 / ISO 8601 extended: 'T' separator, '+00:00' (UTC) zone offset
  // -- matches the OData sanitizer's prior output format.
  if (y >= 0 && y <= 9999) { // table-driven fast path (see put2/put4)
    char buf[40];
    char *p = put4(buf, y);
    *p++ = '-'; p = put2(p, mo);
    *p++ = '-'; p = put2(p, d);
    *p++ = 'T'; p = put2(p, h);
    *p++ = ':'; p = put2(p, mi);
    *p++ = ':'; p = put2(p, s);
    if (frac) { *p++ = '.'; p = put6(p, frac); }
    *p++ = '+'; *p++ = '0'; *p++ = '0'; *p++ = ':'; *p++ = '0'; *p++ = '0';
    out.append(buf, p - buf);
    return;
  }
  char buf[40];
  int n;
  if (frac)
    n = std::snprintf(buf, sizeof(buf), "%04d-%02d-%02dT%02d:%02d:%02d.%06d+00:00",
                      y, mo, d, h, mi, s, frac);
  else
    n = std::snprintf(buf, sizeof(buf), "%04d-%02d-%02dT%02d:%02d:%02d+00:00",
                      y, mo, d, h, mi, s);
  out.append(buf, n);
}

inline void fmt_time(std::string &out, int64_t v, int unit) {
  int64_t us = to_micros(v, unit);
  int h = (int)(us / 3600000000LL);
  int mi = (int)((us / 60000000LL) % 60);
  int s = (int)((us / 1000000LL) % 60);
  int frac = (int)(us % 1000000LL);
  if (h >= 0 && h <= 99) { // TIME-of-day is [0,23]; guard covers the field width
    char buf[24];
    char *p = put2(buf, h);
    *p++ = ':'; p = put2(p, mi);
    *p++ = ':'; p = put2(p, s);
    if (frac) { *p++ = '.'; p = put6(p, frac); }
    out.append(buf, p - buf);
    return;
  }
  char buf[24];
  int n;
  if (frac)
    n = std::snprintf(buf, sizeof(buf), "%02d:%02d:%02d.%06d", h, mi, s, frac);
  else
    n = std::snprintf(buf, sizeof(buf), "%02d:%02d:%02d", h, mi, s);
  out.append(buf, n);
}

// Unsigned __int128 -> decimal digits appended; returns nothing.
inline void append_u128_digits(std::string &out, unsigned __int128 v) {
  char tmp[40];
  int i = 0;
  if (v == 0) { out.push_back('0'); return; }
  while (v > 0) { tmp[i++] = (char)('0' + (int)(v % 10)); v /= 10; }
  while (i > 0) out.push_back(tmp[--i]);
}

// Format a scaled integer (unscaled value, scale s) as a decimal string.
inline void fmt_decimal(std::string &out, __int128 unscaled, int s) {
  bool neg = unscaled < 0;
  unsigned __int128 mag = neg ? (unsigned __int128)(-unscaled)
                              : (unsigned __int128)unscaled;
  std::string digits;
  append_u128_digits(digits, mag);
  if (s <= 0) {
    if (neg) out.push_back('-');
    out.append(digits);
    return;
  }
  // ensure at least s+1 digits (leading zeros for the integer part)
  while ((int)digits.size() <= s)
    digits.insert(digits.begin(), '0');
  if (neg) out.push_back('-');
  size_t point = digits.size() - (size_t)s;
  out.append(digits, 0, point);
  out.push_back('.');
  out.append(digits, point, std::string::npos);
}

// Cython-friendly entry points (no __int128 in the Cython type system).
inline void fmt_decimal_i64(std::string &out, int64_t v, int s) {
  fmt_decimal(out, (__int128)v, s);
}
inline void fmt_decimal_ptr128(std::string &out, const void *p, int s) {
  __int128 v;
  std::memcpy(&v, p, 16);
  fmt_decimal(out, v, s);
}

// ---- escaping ----

// Append a CSV field, quoting per RFC 4180 if it contains the delimiter, a
// quote, CR or LF. Quotes are doubled.
inline void csv_field(std::string &out, const char *s, size_t n, char delim) {
  bool quote = false;
  for (size_t i = 0; i < n; i++) {
    char c = s[i];
    if (c == delim || c == '"' || c == '\n' || c == '\r') { quote = true; break; }
  }
  if (!quote) { out.append(s, n); return; }
  out.push_back('"');
  for (size_t i = 0; i < n; i++) {
    if (s[i] == '"') out.push_back('"');
    out.push_back(s[i]);
  }
  out.push_back('"');
}

// Append a JSON string (quoted + escaped).
inline void json_string(std::string &out, const char *s, size_t n) {
  static const char *HEX = "0123456789abcdef";
  out.push_back('"');
  size_t i = 0;
  for (; i < n; i++) {
    unsigned char c = (unsigned char)s[i];
    if (c == '"' || c == '\\' || c < 0x20) break;
  }
  if (i == n) { out.append(s, n); out.push_back('"'); return; }
  out.append(s, i);
  for (; i < n; i++) {
    unsigned char c = (unsigned char)s[i];
    switch (c) {
    case '"':  out.append("\\\""); break;
    case '\\': out.append("\\\\"); break;
    case '\n': out.append("\\n"); break;
    case '\r': out.append("\\r"); break;
    case '\t': out.append("\\t"); break;
    case '\b': out.append("\\b"); break;
    case '\f': out.append("\\f"); break;
    default:
      if (c < 0x20) {
        out.append("\\u00");
        out.push_back(HEX[c >> 4]);
        out.push_back(HEX[c & 0xF]);
      } else {
        out.push_back((char)c);
      }
    }
  }
  out.push_back('"');
}

// ---- value rendering (reads DrakenVector directly) ----

inline bool row_valid(const uint8_t *validity, size_t i) {
  return validity == nullptr || ((validity[i >> 3] >> (i & 7)) & 1) != 0;
}

// Append the JSON representation of the scalar at logical row i of `dv`.
// (Not for ARRAY columns — see render_json_value.)
inline void render_json_scalar(std::string &out, const DrakenVector *dv,
                               size_t i, int unit, int scale) {
  if (!row_valid(dv->validity, i)) { out.append("null"); return; }
  uint32_t p = dv->selection[i];
  switch (dv->type) {
  case DRAKEN_INT64:  fmt_int64(out, ((const int64_t *)dv->data)[p]); break;
  case DRAKEN_INT32:  fmt_int64(out, ((const int32_t *)dv->data)[p]); break;
  case DRAKEN_INT16:  fmt_int64(out, ((const int16_t *)dv->data)[p]); break;
  case DRAKEN_INT8:   fmt_int64(out, ((const int8_t *)dv->data)[p]); break;
  case DRAKEN_FLOAT64: {
    double d = ((const double *)dv->data)[p];
    if (double_is_nan_or_inf(d)) out.append("null"); else fmt_double(out, d);
    break;
  }
  case DRAKEN_FLOAT32: {
    double d = ((const float *)dv->data)[p];
    if (double_is_nan_or_inf(d)) out.append("null"); else fmt_double(out, d);
    break;
  }
  case DRAKEN_BOOL:
    out.append((((const uint8_t *)dv->data)[p >> 3] >> (p & 7)) & 1 ? "true" : "false");
    break;
  case DRAKEN_DATE32:
    out.push_back('"'); fmt_date(out, ((const int32_t *)dv->data)[p]); out.push_back('"');
    break;
  case DRAKEN_TIMESTAMP64:
    out.push_back('"'); fmt_timestamp(out, ((const int64_t *)dv->data)[p], unit); out.push_back('"');
    break;
  case DRAKEN_TIME64:
    out.push_back('"'); fmt_time(out, ((const int64_t *)dv->data)[p], unit); out.push_back('"');
    break;
  case DRAKEN_TIME32:
    out.push_back('"'); fmt_time(out, ((const int32_t *)dv->data)[p], unit); out.push_back('"');
    break;
  case DRAKEN_DECIMAL:    fmt_decimal(out, (__int128)((const int64_t *)dv->data)[p], scale); break;
  case DRAKEN_DECIMAL128: { __int128 v; std::memcpy(&v, (const uint8_t *)dv->data + (size_t)p * 16, 16); fmt_decimal(out, v, scale); break; }
  case DRAKEN_VARCHAR:
  case DRAKEN_NVARCHAR:
  case DRAKEN_VARBINARY:
  case DRAKEN_VARIANT: {
    const DrakenStringArena *sa = (const DrakenStringArena *)dv->data;
    const DrakenStringSlot *slot = &sa->slots[p];
    json_string(out, (const char *)str_data(slot, sa->arena), str_length(slot));
    break;
  }
  default: out.append("null");
  }
}

// render_json_scalar (above) is reused for ARRAY elements by the fast writer
// in _text_render.hpp; top-level cell rendering lives there.

// Append the JSON array  [v0,v1,…,v(nrows-1)]  for every logical row of `dv`.
// This is the column-oriented analogue of the row-oriented morsel writers in
// _text_render.hpp; it backs draken's Vector._to_json() so a single column can
// serialize itself to JSON bytes with the SAME per-value rendering the rugo
// JSONL writer uses (matching /download output). `child`, `cunit`, `cscale`
// are consulted only when `dv->type == DRAKEN_ARRAY` (they describe the array's
// element vector, mirroring _text_render.hpp::ej_array).
inline void render_json_column(std::string &out, const DrakenVector *dv,
                               const DrakenVector *child, int unit, int scale,
                               int cunit, int cscale, size_t nrows) {
  out.push_back('[');
  for (size_t i = 0; i < nrows; i++) {
    if (i) out.push_back(',');
    if (dv->type == DRAKEN_ARRAY) {
      if (!row_valid(dv->validity, i)) { out.append("null"); continue; }
      const int32_t *offs = (const int32_t *)dv->data;
      uint32_t p = dv->selection[i];
      int32_t s = offs[p], e = offs[p + 1];
      out.push_back('[');
      for (int32_t k = s; k < e; k++) {
        if (k > s) out.push_back(',');
        render_json_scalar(out, child, (size_t)k, cunit, cscale);
      }
      out.push_back(']');
    } else {
      render_json_scalar(out, dv, i, unit, scale);
    }
  }
  out.push_back(']');
}

} // namespace rugo_text
