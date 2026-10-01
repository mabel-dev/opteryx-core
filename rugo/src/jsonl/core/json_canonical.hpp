#pragma once

#include <cstdint>
#include <cstring>
#include <vector>

#include "json_array_walker.hpp"  // jsonarr::scan_string / decode_string / is_ws / is_digit
#include "markers.hpp"            // FieldSpan, ValueType

// Canonical JSON text for a nested column (`key->'sub'` / `key->>'sub'`, nested_column.hpp)
// — byte-identical to what draken's `->`/`->>` produce (draken/ops/json_extract.h), which
// parse with yyjson_read(YYJSON_READ_STOP_WHEN_DONE | YYJSON_READ_NUMBER_AS_RAW) and write
// with yyjson_val_write(val, 0).
//
// TRANSCRIBED from the vendored yyjson (third_party/yyjson/src/yyjson.c), not linked:
// rugo stands alone (architect ruling 2026-10-01). Same discipline as json_array_walker.hpp,
// whose strict string scanner this reuses, so the accept/reject set is yyjson's default.
// What yyjson_val_write(flags 0) does, and so what this does:
//   * containers are written minified — no whitespace between tokens, members in source
//     order, duplicate keys kept;
//   * strings (keys and values) are decoded, then re-escaped with enc_table_cpy: `"` and
//     `\` as \" \\, U+0008/09/0A/0C/0D as \b \t \n \f \r, every other byte < 0x20 as
//     \u00XX with UPPERCASE hex (esc_hex_char_table), '/' and all valid UTF-8 verbatim;
//   * numbers are copied as their source token (NUMBER_AS_RAW): grammar-checked, never
//     re-formatted, no range check;
//   * true / false / null verbatim.
// Anything yyjson's read would refuse makes these return false.

namespace rugo::_jsonl::jsoncanon {

// A JSON string token for `s` (already decoded UTF-8), escaped as yyjson writes it.
inline void write_string(const uint8_t* s, size_t n, std::vector<uint8_t>& out) {
    static const char kHex[] = "0123456789ABCDEF";
    out.push_back('"');
    size_t run = 0;
    for (size_t i = 0; i < n; ++i) {
        const uint8_t c = s[i];
        if (c >= 0x20 && c != '"' && c != '\\') continue;
        out.insert(out.end(), s + run, s + i);
        run = i + 1;
        out.push_back('\\');
        switch (c) {
            case '"':  out.push_back('"');  break;
            case '\\': out.push_back('\\'); break;
            case 0x08: out.push_back('b');  break;
            case 0x09: out.push_back('t');  break;
            case 0x0A: out.push_back('n');  break;
            case 0x0C: out.push_back('f');  break;
            case 0x0D: out.push_back('r');  break;
            default:
                out.push_back('u'); out.push_back('0'); out.push_back('0');
                out.push_back(static_cast<uint8_t>(kHex[c >> 4]));
                out.push_back(static_cast<uint8_t>(kHex[c & 0x0F]));
        }
    }
    out.insert(out.end(), s + run, s + n);
    out.push_back('"');
}

// The decoded bytes of a string body jsonarr::scan_string accepted: the raw body itself
// when it held no escapes, else decoded into `scratch`.
inline const uint8_t* decoded(const JsonArrayElement& e, std::vector<uint8_t>& scratch) {
    if (!e.str_escaped) return e.str_raw;
    scratch.resize(e.str_decoded_len);
    jsonarr::decode_string(e.str_raw, e.str_raw_len, scratch.data());
    return scratch.data();
}

// A JSON number token at `cur`, by RFC 8259 grammar only — NUMBER_AS_RAW keeps the source
// bytes, so yyjson checks the spelling but never the range. Advances past it.
inline bool number_token(const uint8_t*& cur, const uint8_t* end) noexcept {
    const uint8_t* p = cur;
    if (p < end && *p == '-') ++p;
    if (p >= end || !jsonarr::is_digit(*p)) return false;
    if (*p == '0') {
        ++p;
        if (p < end && jsonarr::is_digit(*p)) return false;  // leading zero
    } else {
        while (p < end && jsonarr::is_digit(*p)) ++p;
    }
    if (p < end && *p == '.') {
        ++p;
        if (p >= end || !jsonarr::is_digit(*p)) return false;
        while (p < end && jsonarr::is_digit(*p)) ++p;
    }
    if (p < end && (*p == 'e' || *p == 'E')) {
        ++p;
        if (p < end && (*p == '+' || *p == '-')) ++p;
        if (p >= end || !jsonarr::is_digit(*p)) return false;
        while (p < end && jsonarr::is_digit(*p)) ++p;
    }
    cur = p;
    return true;
}

inline bool literal(const uint8_t*& cur, const uint8_t* end, const char* word, size_t n) noexcept {
    if (static_cast<size_t>(end - cur) < n || std::memcmp(cur, word, n) != 0) return false;
    cur += n;
    return true;
}

// Append the canonical text of the single JSON value in [text, text+len) (surrounding
// whitespace allowed) to `out`. False — with `out` holding partial output the caller
// discards — when it is not exactly one well-formed value. Iterative, so no input can
// exhaust the stack; `stack` and `scratch` are caller-owned reusable scratch.
inline bool write_value(const uint8_t* text, size_t len, std::vector<uint8_t>& out,
                        std::vector<uint8_t>& stack, std::vector<uint8_t>& scratch) {
    const uint8_t* cur = text;
    const uint8_t* const end = text + len;
    auto skip_ws = [&]() { while (cur < end && jsonarr::is_ws(*cur)) ++cur; };
    auto string_token = [&]() -> bool {  // at the opening quote
        ++cur;
        JsonArrayElement e;
        if (!jsonarr::scan_string(cur, end, e)) return false;
        const uint8_t* d = decoded(e, scratch);
        write_string(d, e.str_escaped ? e.str_decoded_len : e.str_raw_len, out);
        return true;
    };

    enum class Want : uint8_t { Value, Key, After };
    Want want = Want::Value;
    stack.clear();
    skip_ws();
    while (true) {
        if (want == Want::Value) {
            if (cur >= end) return false;
            const uint8_t c = *cur;
            if (c == '{' || c == '[') {
                out.push_back(c);
                ++cur;
                skip_ws();
                const uint8_t close = (c == '{') ? '}' : ']';
                if (cur < end && *cur == close) { out.push_back(close); ++cur; want = Want::After; continue; }
                stack.push_back(c);
                want = (c == '{') ? Want::Key : Want::Value;
                continue;
            }
            const uint8_t* start = cur;
            bool ok;
            if (c == '"')      ok = string_token();
            else if (c == 't') ok = literal(cur, end, "true", 4);
            else if (c == 'f') ok = literal(cur, end, "false", 5);
            else if (c == 'n') ok = literal(cur, end, "null", 4);
            else               ok = number_token(cur, end);
            if (!ok) return false;
            if (c != '"') out.insert(out.end(), start, cur);
            want = Want::After;
        } else if (want == Want::Key) {
            skip_ws();
            if (cur >= end || *cur != '"' || !string_token()) return false;
            skip_ws();
            if (cur >= end || *cur != ':') return false;
            out.push_back(':');
            ++cur;
            skip_ws();
            want = Want::Value;
        } else {  // After a value
            skip_ws();
            if (stack.empty()) return cur == end;
            if (cur >= end) return false;
            const uint8_t c = *cur;
            if (c == ',') {
                out.push_back(',');
                ++cur;
                skip_ws();
                want = (stack.back() == '{') ? Want::Key : Want::Value;
            } else if ((c == '}' && stack.back() == '{') || (c == ']' && stack.back() == '[')) {
                out.push_back(c);
                ++cur;
                stack.pop_back();
            } else {
                return false;
            }
        }
    }
}

// Render one nested sub-value span (find_nested_field in interpreter.cpp) exactly as
// draken's `->` (as_json) / `->>` render the same path, appending to `out`:
//   string  `->>`: its decoded UTF-8        `->`: re-escaped JSON string
//   object / array: canonical minified JSON (both operators)
//   number: the source token                true / false: verbatim
// A string span is the body BETWEEN the quotes (the closing quote follows it). False when
// the value is not valid JSON — the caller fails loud. The ONE renderer for both the
// column builder and nested predicate evaluation, so a pushed filter can never disagree
// with the column it filters.
inline bool render_nested(const uint8_t* buffer, const FieldSpan& f, bool as_json,
                          std::vector<uint8_t>& out,
                          std::vector<uint8_t>& stack, std::vector<uint8_t>& scratch) {
    const uint8_t* v = buffer + f.value_start;
    const uint32_t len = f.value_width;
    switch (static_cast<ValueType>(f.type)) {
        case ValueType::String: {
            const uint8_t* cur = v;
            JsonArrayElement e;
            if (!jsonarr::scan_string(cur, v + len + 1, e) || cur != v + len + 1) return false;
            const uint8_t* d = decoded(e, scratch);
            const size_t dn = e.str_escaped ? e.str_decoded_len : e.str_raw_len;
            if (as_json) write_string(d, dn, out);
            else         out.insert(out.end(), d, d + dn);
            return true;
        }
        case ValueType::Object:
        case ValueType::Array:
            return write_value(v, len, out, stack, scratch);
        default: {
            // Number or literal: the whole trimmed slice must be exactly one token.
            const uint8_t* cur = v;
            const uint8_t* const end = v + len;
            const bool ok = (literal(cur, end, "true", 4) || literal(cur, end, "false", 5) ||
                             number_token(cur, end)) && cur == end;
            if (ok) out.insert(out.end(), v, end);
            return ok;
        }
    }
}

}  // namespace rugo::_jsonl::jsoncanon
