#pragma once

#include <cstdint>
#include <cstring>

#include "fast_parsers.hpp"  // fast_float, draken::ops::fp_canon

// Strict JSON array walker for the JSONL ingest path.
//
// The bulk read path (interpreter.cpp / structural_scan.hpp) bounds a container value
// but never looks inside it: scan_container_markers only balances brackets, so the text
// reaching here is `[` .. matching `]` and nothing more is known about it. This walker
// is the value decoder for that span — it validates the array and reports one element at
// a time, so an array column can be materialised without a general JSON library.
//
// ACCEPTANCE IS DELIBERATELY RFC 8259 STRICT, and matches yyjson's default (flags == 0)
// accept/reject set exactly, because that is what this replaced: a column whose array
// text fails to parse falls back to raw VARCHAR text, so accepting one byte more or less
// than before silently changes a column's TYPE. Specifically rejected: leading zeros
// (`01`), a leading `+`, `.5`, `1.`, `1e`, trailing commas, `NaN`/`Infinity`, single
// quotes, comments, unescaped control bytes in strings, unknown escapes, lone
// surrogates, invalid UTF-8, and any trailing content after the closing bracket.
//
// String bodies are UTF-8 validated because yyjson validates them by default, and a
// string column's bytes decide whether it materialises as ARRAY or falls back to text.
// utf8_seq_len() below is transcribed from yyjson's is_utf8_seq2/3/4 mask-pattern-require
// tables rather than written from the spec, so the two cannot drift: it rejects overlong
// forms, UTF-8-encoded surrogate halves, and anything past U+10FFFF.
//
// Nested containers are NOT traversed. The only caller bails the whole column out of
// scope the moment it sees one, so validating the rest would be work whose result is
// discarded — the walker reports Nested and stops. Either way the column falls back to
// VARCHAR, so the observable outcome is unchanged.

namespace rugo::_jsonl {

enum class JsonElemKind : uint8_t {
    Null   = 0,
    Bool   = 1,
    Int    = 2,  // integer that fits int64 (may be negative)
    Uint   = 3,  // integer in (INT64_MAX, UINT64_MAX] — does NOT fit int64
    Real   = 4,  // had a fraction/exponent, or an integer magnitude past uint64
    String = 5,
    Nested = 6,  // '[' or '{' in element position; the walk stops here
};

struct JsonArrayElement {
    JsonElemKind kind = JsonElemKind::Null;

    bool     bool_value = false;
    int64_t  int_value  = 0;
    uint64_t uint_value = 0;
    double   real_value = 0.0;

    // String elements point at the RAW (still-escaped) bytes between the quotes, in the
    // source buffer. decoded_len is the length after unescaping — computed during
    // validation, so sizing an arena costs no extra pass. When !escaped the raw bytes
    // ARE the decoded bytes and can be memcpy'd directly.
    const uint8_t* str_raw     = nullptr;
    uint32_t       str_raw_len = 0;
    uint32_t       str_decoded_len = 0;
    bool           str_escaped = false;
};

namespace jsonarr {

inline bool is_ws(uint8_t c) noexcept {
    return c == ' ' || c == '\t' || c == '\n' || c == '\r';
}

inline bool is_digit(uint8_t c) noexcept { return c >= '0' && c <= '9'; }

// Length of the valid UTF-8 sequence at p, or 0 if invalid. Mirrors yyjson's
// is_utf8_seq2/3/4 mask-pattern-require tables (see the note at the top of this file).
inline uint32_t utf8_seq_len(const uint8_t* p, const uint8_t* end) noexcept {
    const uint8_t b0 = p[0];
    if (b0 < 0x80) return 1;

    if ((b0 & 0xE0) == 0xC0) {
        if (end - p < 2) return 0;
        if ((p[1] & 0xC0) != 0x80) return 0;
        if ((b0 & 0x1E) == 0) return 0;  // overlong (C0/C1)
        return 2;
    }
    if ((b0 & 0xF0) == 0xE0) {
        if (end - p < 3) return 0;
        if ((p[1] & 0xC0) != 0x80 || (p[2] & 0xC0) != 0x80) return 0;
        if (((b0 & 0x0F) | (p[1] & 0x20)) == 0) return 0;              // overlong
        if ((b0 & 0x0F) == 0x0D && (p[1] & 0x20) == 0x20) return 0;    // surrogate half
        return 3;
    }
    if ((b0 & 0xF8) == 0xF0) {
        if (end - p < 4) return 0;
        if ((p[1] & 0xC0) != 0x80 || (p[2] & 0xC0) != 0x80 || (p[3] & 0xC0) != 0x80)
            return 0;
        const uint8_t r0 = b0 & 0x07, r1 = p[1] & 0x30;
        if ((r0 | r1) == 0) return 0;                                  // overlong
        if ((r0 & 0x04) != 0 && (((r0 & 0x03) | r1) != 0)) return 0;   // past U+10FFFF
        return 4;
    }
    return 0;
}

inline bool hex4(const uint8_t* p, uint32_t& out) noexcept {
    uint32_t v = 0;
    for (int k = 0; k < 4; ++k) {
        const uint8_t c = p[k];
        uint32_t d;
        if (c >= '0' && c <= '9')      d = c - '0';
        else if (c >= 'a' && c <= 'f') d = c - 'a' + 10;
        else if (c >= 'A' && c <= 'F') d = c - 'A' + 10;
        else return false;
        v = (v << 4) | d;
    }
    out = v;
    return true;
}

inline uint32_t utf8_width(uint32_t cp) noexcept {
    if (cp <= 0x7F)   return 1;
    if (cp <= 0x7FF)  return 2;
    if (cp <= 0xFFFF) return 3;
    return 4;
}

inline void utf8_write(uint32_t cp, uint8_t*& dst) noexcept {
    if (cp <= 0x7F) {
        *dst++ = static_cast<uint8_t>(cp);
    } else if (cp <= 0x7FF) {
        *dst++ = static_cast<uint8_t>(0xC0 | (cp >> 6));
        *dst++ = static_cast<uint8_t>(0x80 | (cp & 0x3F));
    } else if (cp <= 0xFFFF) {
        *dst++ = static_cast<uint8_t>(0xE0 | (cp >> 12));
        *dst++ = static_cast<uint8_t>(0x80 | ((cp >> 6) & 0x3F));
        *dst++ = static_cast<uint8_t>(0x80 | (cp & 0x3F));
    } else {
        *dst++ = static_cast<uint8_t>(0xF0 | (cp >> 18));
        *dst++ = static_cast<uint8_t>(0x80 | ((cp >> 12) & 0x3F));
        *dst++ = static_cast<uint8_t>(0x80 | ((cp >> 6) & 0x3F));
        *dst++ = static_cast<uint8_t>(0x80 | (cp & 0x3F));
    }
}

// Validate the string body starting at `cur` (just past the opening quote). On success
// advances `cur` past the CLOSING quote and reports the body's raw extent plus its
// decoded length. Rejects unescaped control bytes, unknown escapes, malformed \u,
// unpaired surrogates and invalid UTF-8 — all of which yyjson also rejects with flags 0.
inline bool scan_string(const uint8_t*& cur, const uint8_t* end, JsonArrayElement& out) noexcept {
    const uint8_t* const body = cur;
    uint32_t decoded = 0;
    bool escaped = false;

    while (true) {
        if (cur >= end) return false;  // unterminated
        const uint8_t c = *cur;

        if (c == '"') {
            out.str_raw         = body;
            out.str_raw_len     = static_cast<uint32_t>(cur - body);
            out.str_decoded_len = decoded;
            out.str_escaped     = escaped;
            ++cur;  // consume the closing quote
            return true;
        }
        if (c < 0x20) return false;  // unescaped control character

        if (c == '\\') {
            escaped = true;
            if (cur + 1 >= end) return false;
            const uint8_t e = cur[1];
            switch (e) {
                case '"': case '\\': case '/':
                case 'b': case 'f': case 'n': case 'r': case 't':
                    decoded += 1;
                    cur += 2;
                    break;
                case 'u': {
                    uint32_t hi;
                    if (cur + 6 > end || !hex4(cur + 2, hi)) return false;
                    cur += 6;
                    if (hi >= 0xD800 && hi <= 0xDBFF) {
                        // High surrogate: a low surrogate MUST follow, as \uXXXX.
                        uint32_t lo;
                        if (cur + 6 > end || cur[0] != '\\' || cur[1] != 'u') return false;
                        if (!hex4(cur + 2, lo)) return false;
                        if (lo < 0xDC00 || lo > 0xDFFF) return false;
                        cur += 6;
                        decoded += 4;  // combined codepoint is always 4-byte UTF-8
                    } else if (hi >= 0xDC00 && hi <= 0xDFFF) {
                        return false;  // lone low surrogate
                    } else {
                        decoded += utf8_width(hi);
                    }
                    break;
                }
                default:
                    return false;  // unknown escape
            }
            continue;
        }

        if (c < 0x80) {
            decoded += 1;
            ++cur;
            continue;
        }

        const uint32_t w = utf8_seq_len(cur, end);
        if (w == 0) return false;
        decoded += w;
        cur += w;
    }
}

// Decode a validated string body into `dst`, which must have room for decoded_len bytes.
// Only call with a span scan_string() accepted — it assumes well-formedness.
inline void decode_string(const uint8_t* raw, uint32_t raw_len, uint8_t* dst) noexcept {
    const uint8_t* cur = raw;
    const uint8_t* const end = raw + raw_len;
    while (cur < end) {
        if (*cur != '\\') {
            const uint8_t* run = cur;
            while (run < end && *run != '\\') ++run;
            const size_t n = static_cast<size_t>(run - cur);
            std::memcpy(dst, cur, n);
            dst += n;
            cur = run;
            continue;
        }
        const uint8_t e = cur[1];
        switch (e) {
            case '"':  *dst++ = '"';  cur += 2; break;
            case '\\': *dst++ = '\\'; cur += 2; break;
            case '/':  *dst++ = '/';  cur += 2; break;
            case 'b':  *dst++ = 0x08; cur += 2; break;
            case 'f':  *dst++ = 0x0C; cur += 2; break;
            case 'n':  *dst++ = 0x0A; cur += 2; break;
            case 'r':  *dst++ = 0x0D; cur += 2; break;
            case 't':  *dst++ = 0x09; cur += 2; break;
            default: {  // 'u'
                uint32_t cp = 0;
                hex4(cur + 2, cp);
                cur += 6;
                if (cp >= 0xD800 && cp <= 0xDBFF) {
                    uint32_t lo = 0;
                    hex4(cur + 2, lo);
                    cur += 6;
                    cp = 0x10000u + ((cp - 0xD800u) << 10) + (lo - 0xDC00u);
                }
                utf8_write(cp, dst);
                break;
            }
        }
    }
}

// Validate and classify a number token starting at `cur`, advancing past it.
//
// Integer magnitudes are accumulated exactly and only widen to double when they leave
// uint64 — matching yyjson, which keeps every integer up to UINT64_MAX exact and falls
// to a correctly-rounded double beyond. Reals go through fast_parse_float64's canonical
// form (fp_canon: -0.0 -> +0.0), so an array element and a scalar column agree on what
// the same literal means (ingestion canonicalisation, architect-locked 2026-05-22).
inline bool scan_number(const uint8_t*& cur, const uint8_t* end, JsonArrayElement& out) noexcept {
    const uint8_t* const tok = cur;

    const bool negative = (*cur == '-');
    if (negative && ++cur >= end) return false;

    // int part: '0' alone, or [1-9][0-9]*
    if (!is_digit(*cur)) return false;
    uint64_t mag = 0;
    bool mag_overflow = false;
    if (*cur == '0') {
        ++cur;
        if (cur < end && is_digit(*cur)) return false;  // leading zero
    } else {
        while (cur < end && is_digit(*cur)) {
            const uint64_t d = static_cast<uint64_t>(*cur - '0');
            if (mag > (UINT64_MAX - d) / 10u) mag_overflow = true;
            else mag = mag * 10u + d;
            ++cur;
        }
    }

    bool is_real = false;
    if (cur < end && *cur == '.') {
        is_real = true;
        ++cur;
        if (cur >= end || !is_digit(*cur)) return false;  // "1." / "1.e5"
        while (cur < end && is_digit(*cur)) ++cur;
    }
    if (cur < end && (*cur == 'e' || *cur == 'E')) {
        is_real = true;
        ++cur;
        if (cur < end && (*cur == '+' || *cur == '-')) ++cur;
        if (cur >= end || !is_digit(*cur)) return false;  // "1e" / "1e+"
        while (cur < end && is_digit(*cur)) ++cur;
    }

    if (!is_real && !mag_overflow) {
        if (negative) {
            if (mag <= static_cast<uint64_t>(INT64_MAX) + 1u) {
                out.kind = JsonElemKind::Int;
                out.int_value = (mag == static_cast<uint64_t>(INT64_MAX) + 1u)
                    ? INT64_MIN
                    : -static_cast<int64_t>(mag);
                return true;
            }
        } else if (mag <= static_cast<uint64_t>(INT64_MAX)) {
            out.kind = JsonElemKind::Int;
            out.int_value = static_cast<int64_t>(mag);
            return true;
        } else {
            out.kind = JsonElemKind::Uint;
            out.uint_value = mag;
            return true;
        }
    }

    // Real, or an integer magnitude past what uint64 holds.
    double v = 0.0;
    const char* first = reinterpret_cast<const char*>(tok);
    const char* last  = reinterpret_cast<const char*>(cur);
    const auto answer = fast_float::from_chars(first, last, v);
    if (answer.ptr != last) return false;
    if (answer.ec == std::errc::result_out_of_range) {
        // fast_float reports overflow and underflow with one code; yyjson distinguishes
        // them — a magnitude past DBL_MAX is a parse error ("number is infinity when
        // parsed as double"), while an underflow silently yields zero.
        if (v != 0.0) return false;
    } else if (answer.ec != std::errc()) {
        return false;
    }
    out.kind = JsonElemKind::Real;
    out.real_value = draken::ops::fp_canon(v);
    return true;
}

}  // namespace jsonarr

// Walk a JSON array's text, invoking on_element(const JsonArrayElement&) per element.
// on_element returns false to stop the walk early (the remainder is then NOT validated).
//
// Returns false if `text` is not a well-formed JSON array — including when it is valid
// JSON that simply is not an array, and when anything but whitespace trails the closing
// bracket. Callers treat false as "this column is not a materialisable array".
template <class F>
inline bool walk_json_array(const uint8_t* text, uint32_t len, F&& on_element) {
    const uint8_t* cur = text;
    const uint8_t* const end = text + len;

    while (cur < end && jsonarr::is_ws(*cur)) ++cur;
    if (cur >= end || *cur != '[') return false;
    ++cur;

    while (cur < end && jsonarr::is_ws(*cur)) ++cur;
    if (cur < end && *cur == ']') {
        ++cur;  // empty array
    } else {
        while (true) {
            if (cur >= end) return false;

            JsonArrayElement elem;
            const uint8_t c = *cur;
            if (c == '"') {
                ++cur;
                if (!jsonarr::scan_string(cur, end, elem)) return false;
                elem.kind = JsonElemKind::String;
            } else if (c == '[' || c == '{') {
                elem.kind = JsonElemKind::Nested;
                on_element(elem);
                return true;  // nested containers are never traversed — see the file note
            } else if (c == 't') {
                if (end - cur < 4 || std::memcmp(cur, "true", 4) != 0) return false;
                cur += 4;
                elem.kind = JsonElemKind::Bool;
                elem.bool_value = true;
            } else if (c == 'f') {
                if (end - cur < 5 || std::memcmp(cur, "false", 5) != 0) return false;
                cur += 5;
                elem.kind = JsonElemKind::Bool;
                elem.bool_value = false;
            } else if (c == 'n') {
                if (end - cur < 4 || std::memcmp(cur, "null", 4) != 0) return false;
                cur += 4;
                elem.kind = JsonElemKind::Null;
            } else if (c == '-' || jsonarr::is_digit(c)) {
                if (!jsonarr::scan_number(cur, end, elem)) return false;
            } else {
                return false;
            }

            if (!on_element(elem)) return true;

            while (cur < end && jsonarr::is_ws(*cur)) ++cur;
            if (cur >= end) return false;
            if (*cur == ']') { ++cur; break; }
            if (*cur != ',') return false;
            ++cur;
            while (cur < end && jsonarr::is_ws(*cur)) ++cur;
            if (cur < end && *cur == ']') return false;  // trailing comma
        }
    }

    while (cur < end && jsonarr::is_ws(*cur)) ++cur;
    return cur == end;  // trailing content is a parse failure
}

}  // namespace rugo::_jsonl
