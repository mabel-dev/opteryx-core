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
#include <vector>

#include "core/buffers.h"
#include "core/ipv4.h"
#include "core/string_slot.h"
#include "logical_type.h"  // LogicalKind — the kind vocabulary is draken's, never copied
#include "ryu.h"  // third_party/ulfjack/ryu is on every consuming extension's include path

namespace rugo_text {

// Time unit codes (match draken TimestampUnit: 0=s,1=ms,2=us,3=ns).
enum { U_S = 0, U_MS = 1, U_US = 2, U_NS = 3 };

// ---------------------------------------------------------------------------
// Per-column render descriptor.
//
// Everything the renderers need to know about a column's LOGICAL type travels
// as one value. The physical DrakenType is on the vector; this carries what the
// physical tag cannot answer:
//   kind  — which logical type this is. Mandatory for IPv4: a UINT32 address
//           column and a plain UINT32 column share the same physical tag, so
//           the kind is the ONLY signal that distinguishes them.
//   unit  — TIMESTAMP / TIME resolution (U_S..U_NS).
//   scale — DECIMAL digits to the right of the point.
//   dim   — VECTOR_FP16 values per row.
//
// Read once per column at resolve time, never inside a row loop.
// ---------------------------------------------------------------------------
struct LogicalDesc {
  LogicalKind kind  = LogicalKind::NONE;
  int         unit  = U_S;
  int         scale = 0;
  int         dim   = 0;
};

// One level of a nested-ARRAY descent below the top-level column: the
// element vector at that depth, and its logical descriptor. Consulted only
// when a shallower level's vector is itself DRAKEN_ARRAY (see
// render_json_value below). Default-constructs to vec=nullptr / desc=NONE,
// same convention as ColumnDesc.
struct ArrayLevel {
  const DrakenVector *vec = nullptr;
  LogicalDesc          desc;
};

// A column and, for an ARRAY column, the chain of element vectors beneath it
// — one entry per nesting level, to whatever depth the data actually has
// (levels[0] is the column's own element vector, levels[1] that vector's
// own element vector if IT is also ARRAY, and so on for ARRAY<ARRAY<...>>).
// `levels` stays empty for every non-ARRAY column.
//
// Default-constructing a ColumnDesc yields exactly these defaults (kind
// NONE, empty levels), which is what the Cython writers rely on when they
// build one per column — see draken_native.cpp's row_array_to_pylist /
// child_elem_to_py for the same recursive descent on the to_pylist() path.
struct ColumnDesc {
  LogicalDesc column;
  std::vector<ArrayLevel> levels;
};

// Render a UINT32 carrying LogicalKind::IPV4 as dotted-decimal. Delegates to
// draken/core/ipv4.h so the writers, the cast kernels and the CIDR/IP_TRUNC
// kernels cannot disagree about the octet-to-bit mapping.
//
// The caller decides WHETHER a uint32 is an address; this only knows how to
// render one. A UINT32 with no descriptor renders through fmt_uint64 as the
// plain integer it is.
inline void fmt_ipv4(std::string &out, uint32_t v) {
  char buf[draken::ipv4::MAX_TEXT_LENGTH];
  out.append(buf, draken::ipv4::format(v, buf));
}

// JSON form: quotes baked into the same stack buffer, one append per cell.
inline void fmt_ipv4_quoted(std::string &out, uint32_t v) {
  char buf[draken::ipv4::MAX_TEXT_LENGTH + 2];
  buf[0] = '"';
  uint32_t n = draken::ipv4::format(v, buf + 1);
  buf[n + 1] = '"';
  out.append(buf, n + 2);
}

// Append `"` + s + `"` for a payload that is ALREADY valid JSON string content
// (no escaping needed) — the cast-kernel text for dates and timestamps. Short
// payloads go through a stack buffer so the cell costs one append instead of
// three calls; longer ones are not worth the extra copy.
inline void append_quoted_raw(std::string &out, const char *s, size_t n) {
  if (n <= 62) {
    char buf[64];
    buf[0] = '"';
    std::memcpy(buf + 1, s, n);
    buf[n + 1] = '"';
    out.append(buf, n + 2);
    return;
  }
  out.push_back('"');
  out.append(s, n);
  out.push_back('"');
}

inline void fmt_int64(std::string &out, int64_t v) {
  char buf[24];
  std::to_chars_result r = std::to_chars(buf, buf + sizeof(buf), v);
  out.append(buf, r.ptr - buf);
}

// Unsigned counterpart. UINT64 values above INT64_MAX are not representable as
// int64_t, so the unsigned family must NOT be funnelled through fmt_int64 —
// 2^63 would render as -9223372036854775808.
inline void fmt_uint64(std::string &out, uint64_t v) {
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

// Reshape already-parsed shortest-round-trip digits into Python-style decimal
// text: plain fixed-point for ordinary magnitudes, scientific (lowercase e,
// signed 2+ digit exponent) only outside that range -- the same threshold
// CPython's repr()/json.dumps use (fixed for -4 <= exp < 16, scientific
// otherwise). Pure text layout: does not care whether `digits` came from
// ryu's double or float algorithm, so fmt_double and fmt_float share it.
inline void fmt_shortest_digits(std::string &out, bool neg, const char *digits, int nd, int exp) {
  // Reshape into a stack buffer, appended once. Worst case is "0." plus three
  // leading zeros plus 17 digits (23 bytes incl. sign) — 40 leaves headroom.
  char buf[40];
  char *q = buf;
  if (neg) *q++ = '-';
  if (exp >= -4 && exp < 16) {
    if (exp < 0) {
      *q++ = '0'; *q++ = '.';
      for (int k = 0; k < -exp - 1; k++) *q++ = '0';
      std::memcpy(q, digits, nd); q += nd;
    } else {
      int intDigits = exp + 1;
      if (nd <= intDigits) {
        std::memcpy(q, digits, nd); q += nd;
        for (int k = nd; k < intDigits; k++) *q++ = '0';
        *q++ = '.'; *q++ = '0';
      } else {
        std::memcpy(q, digits, intDigits); q += intDigits;
        *q++ = '.';
        std::memcpy(q, digits + intDigits, nd - intDigits); q += nd - intDigits;
      }
    }
  } else {
    *q++ = digits[0];
    if (nd > 1) { *q++ = '.'; std::memcpy(q, digits + 1, nd - 1); q += nd - 1; }
    *q++ = 'e';
    *q++ = exp < 0 ? '-' : '+';
    int aexp = exp < 0 ? -exp : exp;
    if (aexp >= 100) { *q++ = (char)('0' + aexp / 100); aexp %= 100; }
    q = put2(q, aexp); // %02d: two digits minimum
  }
  out.append(buf, q - buf);
}

// Parse ryu's "D[.DDDD]E[-]DD" scientific text (as returned by d2s_buffered_n
// / f2s_buffered_n — same shape for both) into sign + digit run + exponent,
// then reshape. `digits` is sized for the double case (<= 17 significant
// digits); float's <= 9 fits with room to spare.
inline void fmt_shortest(std::string &out, const char *sci, int n) {
  bool neg = sci[0] == '-';
  int p = neg ? 1 : 0;
  char digits[24];
  int nd = 0;
  digits[nd++] = sci[p++];
  if (p < n && sci[p] == '.') {
    p++;
    while (sci[p] != 'E') digits[nd++] = sci[p++];
  }
  p++; // skip 'E'
  bool eneg = sci[p] == '-';
  if (eneg) p++;
  int exp = 0;
  while (p < n) exp = exp * 10 + (sci[p++] - '0');
  if (eneg) exp = -exp;
  fmt_shortest_digits(out, neg, digits, nd, exp);
}

inline void fmt_double(std::string &out, double v) {
  char sci[32];
  int n = d2s_buffered_n(v, sci);
  fmt_shortest(out, sci, n);
}

// FLOAT32 counterpart. MUST NOT be reached by promoting a float to double
// first: d2s_buffered_n on the widened value finds the shortest string that
// round-trips THAT double, which is a different (typically much longer)
// value than the shortest string that round-trips the original 32-bit float
// -- widening is exact but not distance-preserving among neighbouring
// doubles. f2s_buffered_n runs ryu's algorithm on the actual 24-bit mantissa.
inline void fmt_float(std::string &out, float v) {
  char sci[32];
  int n = f2s_buffered_n(v, sci);
  fmt_shortest(out, sci, n);
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

// ---- buffer-writing cores ----
//
// Each wr_* writes the rendering at `p` and returns one past the last byte. The
// caller owns the buffer, so it can bake the surrounding JSON quotes into the
// same buffer and hand the whole cell to std::string::append in ONE call — the
// row loop's push_back('"') pairs were the second-hottest frame in the writer
// profile, and every one of them was an out-of-line libc++ call.
//
// Buffer sizes below are the caller's contract; kDateText / kTimestampText /
// kTimeText are the maxima these can emit, quotes excluded.
static constexpr size_t kDateText = 24;
static constexpr size_t kTimestampText = 48;
static constexpr size_t kTimeText = 24;

inline char *wr_date(char *p, int32_t days) {
  int y, m, d;
  civil_from_days(days, y, m, d);
  if (y >= 0 && y <= 9999) { // fast path covers every representable calendar date
    char *q = put4(p, y);
    *q++ = '-'; q = put2(q, m);
    *q++ = '-'; q = put2(q, d);
    return q;
  }
  // years outside [0,9999]: keep snprintf's %04d/sign behaviour
  return p + std::snprintf(p, kDateText, "%04d-%02d-%02d", y, m, d);
}

inline void fmt_date(std::string &out, int32_t days) {
  char buf[kDateText];
  out.append(buf, wr_date(buf, days) - buf);
}

// JSON form: quotes written into the same buffer, one append for the cell.
inline void fmt_date_quoted(std::string &out, int32_t days) {
  char buf[kDateText + 2];
  buf[0] = '"';
  char *p = wr_date(buf + 1, days);
  *p++ = '"';
  out.append(buf, p - buf);
}

inline int64_t to_micros(int64_t v, int unit) {
  switch (unit) {
  case U_S:  return v * 1000000LL;
  case U_MS: return v * 1000LL;
  case U_NS: return v / 1000LL;
  default:   return v; // U_US
  }
}

inline char *wr_timestamp(char *p, int64_t v, int unit) {
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
    char *q = put4(p, y);
    *q++ = '-'; q = put2(q, mo);
    *q++ = '-'; q = put2(q, d);
    *q++ = 'T'; q = put2(q, h);
    *q++ = ':'; q = put2(q, mi);
    *q++ = ':'; q = put2(q, s);
    if (frac) { *q++ = '.'; q = put6(q, frac); }
    *q++ = '+'; *q++ = '0'; *q++ = '0'; *q++ = ':'; *q++ = '0'; *q++ = '0';
    return q;
  }
  if (frac)
    return p + std::snprintf(p, kTimestampText,
                             "%04d-%02d-%02dT%02d:%02d:%02d.%06d+00:00",
                             y, mo, d, h, mi, s, frac);
  return p + std::snprintf(p, kTimestampText, "%04d-%02d-%02dT%02d:%02d:%02d+00:00",
                           y, mo, d, h, mi, s);
}

inline void fmt_timestamp(std::string &out, int64_t v, int unit) {
  char buf[kTimestampText];
  out.append(buf, wr_timestamp(buf, v, unit) - buf);
}

inline void fmt_timestamp_quoted(std::string &out, int64_t v, int unit) {
  char buf[kTimestampText + 2];
  buf[0] = '"';
  char *p = wr_timestamp(buf + 1, v, unit);
  *p++ = '"';
  out.append(buf, p - buf);
}

inline char *wr_time(char *p, int64_t v, int unit) {
  int64_t us = to_micros(v, unit);
  int h = (int)(us / 3600000000LL);
  int mi = (int)((us / 60000000LL) % 60);
  int s = (int)((us / 1000000LL) % 60);
  int frac = (int)(us % 1000000LL);
  if (h >= 0 && h <= 99) { // TIME-of-day is [0,23]; guard covers the field width
    char *q = put2(p, h);
    *q++ = ':'; q = put2(q, mi);
    *q++ = ':'; q = put2(q, s);
    if (frac) { *q++ = '.'; q = put6(q, frac); }
    return q;
  }
  if (frac)
    return p + std::snprintf(p, kTimeText, "%02d:%02d:%02d.%06d", h, mi, s, frac);
  return p + std::snprintf(p, kTimeText, "%02d:%02d:%02d", h, mi, s);
}

inline void fmt_time(std::string &out, int64_t v, int unit) {
  char buf[kTimeText];
  out.append(buf, wr_time(buf, v, unit) - buf);
}

inline void fmt_time_quoted(std::string &out, int64_t v, int unit) {
  char buf[kTimeText + 2];
  buf[0] = '"';
  char *p = wr_time(buf + 1, v, unit);
  *p++ = '"';
  out.append(buf, p - buf);
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

// Byte offset of the first byte in [i, n) that forces RFC 4180 quoting
// (`delim`, '"', '\n' or '\r'), or n if none. Same SWAR haszero technique as
// json_scan_clean below, generalized to a runtime delimiter byte broadcast
// across the word.
inline size_t csv_scan_clean(const char *s, size_t i, size_t n, char delim) {
  constexpr uint64_t ONES = 0x0101010101010101ULL, HIGH = 0x8080808080808080ULL;
  const uint64_t DELIM = (uint64_t)(unsigned char)delim * ONES;
  while (i + 8 <= n) {
    uint64_t w;
    std::memcpy(&w, s + i, 8);
    uint64_t dd = w ^ DELIM;
    uint64_t q  = w ^ 0x2222222222222222ULL; // '"'
    uint64_t nl = w ^ 0x0A0A0A0A0A0A0A0AULL; // '\n'
    uint64_t cr = w ^ 0x0D0D0D0D0D0D0D0DULL; // '\r'
    uint64_t hit = (((dd - ONES) & ~dd) | ((q - ONES) & ~q) |
                    ((nl - ONES) & ~nl) | ((cr - ONES) & ~cr)) & HIGH;
    if (hit) return i + ((size_t)__builtin_ctzll(hit) >> 3);
    i += 8;
  }
  for (; i < n; i++) {
    char c = s[i];
    if (c == delim || c == '"' || c == '\n' || c == '\r') break;
  }
  return i;
}

// Byte offset of the first '"' in [i, n), or n if none — the only byte that
// needs escaping once a field is already inside quotes.
inline size_t csv_scan_dquote(const char *s, size_t i, size_t n) {
  constexpr uint64_t ONES = 0x0101010101010101ULL, HIGH = 0x8080808080808080ULL;
  while (i + 8 <= n) {
    uint64_t w;
    std::memcpy(&w, s + i, 8);
    uint64_t q = w ^ 0x2222222222222222ULL;
    uint64_t hit = ((q - ONES) & ~q) & HIGH;
    if (hit) return i + ((size_t)__builtin_ctzll(hit) >> 3);
    i += 8;
  }
  for (; i < n; i++)
    if (s[i] == '"') break;
  return i;
}

// Append a CSV field, quoting per RFC 4180 if it contains the delimiter, a
// quote, CR or LF. A zero-length field is also quoted, so an empty string
// ("") is distinguishable from a NULL cell (which never calls this
// function at all). Quotes are doubled. Clean runs (the common case, and the
// whole field the overwhelming majority of the time) are appended in bulk —
// see json_string below for the same pattern.
inline void csv_field(std::string &out, const char *s, size_t n, char delim) {
  if (n != 0 && csv_scan_clean(s, 0, n, delim) == n) { out.append(s, n); return; }
  out.push_back('"');
  size_t i = csv_scan_dquote(s, 0, n);
  out.append(s, i);
  while (i < n) {
    out.append("\"\"", 2); // the '"' itself, doubled
    size_t run = ++i;
    i = csv_scan_dquote(s, i, n);
    if (i > run) out.append(s + run, i - run);
  }
  out.push_back('"');
}

// Byte offset of the first byte in [s+i, s+n) needing a JSON escape ('"', '\\'
// or a control byte < 0x20), or n if none. SWAR, 8 bytes per step: haszero(v)
// = (v-0x01..01) & ~v & 0x80..80 flags every zero byte exactly, plus possible
// false positives only ABOVE (more significant than) a true zero — so on a
// little-endian OR of the three predicates, ctz still lands on the first true
// hit. UTF-8 continuation/lead bytes (>= 0x80) are never flagged.
inline size_t json_scan_clean(const char *s, size_t i, size_t n) {
  constexpr uint64_t ONES = 0x0101010101010101ULL, HIGH = 0x8080808080808080ULL;
  while (i + 8 <= n) {
    uint64_t w;
    std::memcpy(&w, s + i, 8);
    uint64_t q = w ^ 0x2222222222222222ULL;           // '"'
    uint64_t b = w ^ 0x5C5C5C5C5C5C5C5CULL;           // '\\'
    uint64_t c = w & 0xE0E0E0E0E0E0E0E0ULL;           // zero byte iff < 0x20
    uint64_t hit = (((q - ONES) & ~q) | ((b - ONES) & ~b) | ((c - ONES) & ~c)) & HIGH;
    if (hit) return i + ((size_t)__builtin_ctzll(hit) >> 3);
    i += 8;
  }
  for (; i < n; i++) {
    unsigned char ch = (unsigned char)s[i];
    if (ch == '"' || ch == '\\' || ch < 0x20) break;
  }
  return i;
}

// Append a JSON string (quoted + escaped). Clean runs between escapes are
// appended in bulk, never byte-at-a-time.
inline void json_string(std::string &out, const char *s, size_t n) {
  static const char *HEX = "0123456789abcdef";
  // Clean string (the overwhelming majority) is a whole cell on its own, so it
  // goes out as one append rather than push_back / append / push_back.
  size_t i = json_scan_clean(s, 0, n);
  if (i == n) { append_quoted_raw(out, s, n); return; }
  out.push_back('"');
  out.append(s, i);
  while (i < n) {
    unsigned char c = (unsigned char)s[i];
    switch (c) {
    case '"':  out.append("\\\"", 2); break;
    case '\\': out.append("\\\\", 2); break;
    case '\n': out.append("\\n", 2); break;
    case '\r': out.append("\\r", 2); break;
    case '\t': out.append("\\t", 2); break;
    case '\b': out.append("\\b", 2); break;
    case '\f': out.append("\\f", 2); break;
    default: // remaining control bytes
      out.append("\\u00", 4);
      out.push_back(HEX[c >> 4]);
      out.push_back(HEX[c & 0xF]);
    }
    size_t run = ++i;
    i = json_scan_clean(s, i, n);
    if (i > run) out.append(s + run, i - run);
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
                               size_t i, const LogicalDesc &d) {
  if (!row_valid(dv->validity, i)) { out.append("null"); return; }
  uint32_t p = dv->selection[i];
  switch (dv->type) {
  case DRAKEN_INT64:  fmt_int64(out, ((const int64_t *)dv->data)[p]); break;
  case DRAKEN_INT32:  fmt_int64(out, ((const int32_t *)dv->data)[p]); break;
  case DRAKEN_INT16:  fmt_int64(out, ((const int16_t *)dv->data)[p]); break;
  case DRAKEN_INT8:   fmt_int64(out, ((const int8_t *)dv->data)[p]); break;
  case DRAKEN_UINT64: fmt_uint64(out, ((const uint64_t *)dv->data)[p]); break;
  // A UINT32 carrying LogicalKind::IPV4 is the same 32 bits with a narrower
  // meaning, so the descriptor kind is the only thing that can tell them apart.
  // It renders as a quoted dotted-decimal string, matching what to_pylist()
  // hands back for an IPv4 column; with no descriptor it stays a plain number.
  case DRAKEN_UINT32:
    if (d.kind == LogicalKind::IPV4) {
      fmt_ipv4_quoted(out, ((const uint32_t *)dv->data)[p]);
    } else {
      fmt_uint64(out, ((const uint32_t *)dv->data)[p]);
    }
    break;
  case DRAKEN_UINT16: fmt_uint64(out, ((const uint16_t *)dv->data)[p]); break;
  case DRAKEN_UINT8:  fmt_uint64(out, ((const uint8_t *)dv->data)[p]); break;
  case DRAKEN_FLOAT64: {
    double d = ((const double *)dv->data)[p];
    if (double_is_nan_or_inf(d)) out.append("null"); else fmt_double(out, d);
    break;
  }
  case DRAKEN_FLOAT32: {
    float f = ((const float *)dv->data)[p];
    if (double_is_nan_or_inf((double)f)) out.append("null"); else fmt_float(out, f);
    break;
  }
  case DRAKEN_BOOL:
    out.append((((const uint8_t *)dv->data)[p >> 3] >> (p & 7)) & 1 ? "true" : "false");
    break;
  case DRAKEN_DATE32:
    fmt_date_quoted(out, ((const int32_t *)dv->data)[p]);
    break;
  case DRAKEN_TIMESTAMP64:
    fmt_timestamp_quoted(out, ((const int64_t *)dv->data)[p], d.unit);
    break;
  case DRAKEN_TIME64:
    fmt_time_quoted(out, ((const int64_t *)dv->data)[p], d.unit);
    break;
  case DRAKEN_TIME32:
    fmt_time_quoted(out, ((const int32_t *)dv->data)[p], d.unit);
    break;
  case DRAKEN_DECIMAL:    fmt_decimal(out, (__int128)((const int64_t *)dv->data)[p], d.scale); break;
  case DRAKEN_DECIMAL128: { __int128 v; std::memcpy(&v, (const uint8_t *)dv->data + (size_t)p * 16, 16); fmt_decimal(out, v, d.scale); break; }
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

// render_json_scalar (above) is reused for leaf ARRAY elements by
// render_json_value below and by the fast writer in _text_render.hpp.

// Append the JSON representation of logical row `row_idx` of `vec`. `own_desc`
// is `vec`'s own descriptor and is used only when `vec` is a scalar (kind /
// unit / scale for `vec` itself). When `vec` is DRAKEN_ARRAY, `levels[depth]`
// gives the element vector one nesting level down — `depth` starts at 0 for
// the top-level column and increases by one each time the recursion steps
// into an ARRAY-of-ARRAY element, so a column nested to depth N is handled by
// N recursive calls, each consuming the next `levels` entry. This mirrors
// row_array_to_pylist's descent through VectorOwner::child_owner in
// draken_native.cpp, which is the reference for correct nested-array
// handling — see CLAUDE.md and the caller's comment for how `levels` is
// built (walking array_child/array_child_type to whatever depth the data
// actually has).
inline void render_json_value(std::string &out, const DrakenVector *vec,
                              size_t row_idx, const LogicalDesc &own_desc,
                              const std::vector<ArrayLevel> &levels, size_t depth) {
  if (vec->type != DRAKEN_ARRAY) {
    render_json_scalar(out, vec, row_idx, own_desc);
    return;
  }
  if (!row_valid(vec->validity, row_idx)) { out.append("null"); return; }
  const int32_t *offs = (const int32_t *)vec->data;
  uint32_t p = vec->selection[row_idx];
  int32_t s = offs[p], e = offs[p + 1];
  out.push_back('[');
  const ArrayLevel *next = depth < levels.size() ? &levels[depth] : nullptr;
  for (int32_t k = s; k < e; k++) {
    if (k > s) out.push_back(',');
    // `next` is only null if the caller's chain is shorter than the data's
    // actual nesting, which cannot happen when `levels` was built by walking
    // this same vector's array_child chain — an ARRAY row with elements
    // (s < e) always has a populated child vector at that depth.
    render_json_value(out, next->vec, (size_t)k, next->desc, levels, depth + 1);
  }
  out.push_back(']');
}

// Append the JSON array  [v0,v1,…,v(nrows-1)]  for every logical row of `dv`.
// This is the column-oriented analogue of the row-oriented morsel writers in
// _text_render.hpp; it backs draken's Vector._to_json() so a single column can
// serialize itself to JSON bytes with the SAME per-value rendering the rugo
// JSONL writer uses (matching /download output). `desc.levels` is consulted
// only when `dv->type == DRAKEN_ARRAY` (see render_json_value above).
inline void render_json_column(std::string &out, const DrakenVector *dv,
                               const ColumnDesc &desc, size_t nrows) {
  out.push_back('[');
  for (size_t i = 0; i < nrows; i++) {
    if (i) out.push_back(',');
    render_json_value(out, dv, i, desc.column, desc.levels, 0);
  }
  out.push_back(']');
}

} // namespace rugo_text
