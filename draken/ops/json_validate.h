// SQL:2016 `IS [NOT] JSON` — well-formedness over JSON TEXT.
//
// The answer is yyjson's: a row IS JSON exactly when yyjson reads it with
// kIsJsonReadFlags. That makes `IS JSON` and the engine's JSON functions agree —
// a row that passes is a row `->`, `->>` and the JSONB functions can parse.
//
// Grammar is RFC 8259, strict. Rejected, in particular: trailing content after the
// root value, trailing commas, leading zeroes (`01`), a bare `-` or `.5`,
// single-quoted strings, comments, NaN/Infinity, unescaped control bytes inside a
// string, unterminated strings, and `\u` escapes without four hex digits. An empty
// or whitespace-only input has no root value and is therefore not well-formed.
//
// Also rejected, because yyjson rejects them: invalid UTF-8 anywhere (overlong
// forms, UTF-8-encoded surrogates, past U+10FFFF — this applies to VARBINARY too),
// and unpaired `\u` surrogate escapes. There is NO nesting limit. Numbers are read
// raw (YYJSON_READ_NUMBER_AS_RAW), so `1e999` is well-formed: its syntax is valid
// and nothing here converts it.
//
// Measured with json_validate_bench (2026-09-22, 200k JSONBench rows): this is ~33%
// faster per row than the hand-written single-pass walker it replaced, on valid
// and truncated input alike.

#pragma once

#include <cstddef>
#include <cstdint>

#include "yyjson.h"

#include "ops/json_extract.h"  // BasicReadPool, max_slot_length

namespace draken {
namespace ops {

// Top-level shape constraint. VALUE accepts any root node; the other three
// additionally pin what the root must be. Values are wire-stable — the plan
// compiler passes them straight through as the kernel's bind-time argument.
enum JsonShape : uint8_t {
    JSON_SHAPE_VALUE = 0,
    JSON_SHAPE_SCALAR = 1,
    JSON_SHAPE_ARRAY = 2,
    JSON_SHAPE_OBJECT = 3,
};

// NOT YYJSON_READ_STOP_WHEN_DONE: `->` stops at the end of the root value, but
// trailing content must make a document fail IS JSON.
static constexpr yyjson_read_flag kIsJsonReadFlags = YYJSON_READ_NUMBER_AS_RAW;

using IsJsonReadPool = BasicReadPool<kIsJsonReadFlags>;

namespace jv {

inline bool is_ws(uint8_t c) noexcept {
    return c == ' ' || c == '\t' || c == '\n' || c == '\r';
}

// Cheap rejections taken before the parse. They can only REJECT — a row that
// passes still goes to yyjson. false = the row cannot be well-formed JSON of this
// shape.
//   * empty / whitespace-only input has no root value;
//   * the first significant byte fixes the root node kind, so a row that cannot
//     be the asked-for shape is never parsed;
//   * a root container must END with its matching close, so an unclosed one —
//     truncation is the realistic case — is never parsed. Measured: no cost on
//     valid input; without it a truncated row pays for a partial parse.
inline bool passes_gates(const uint8_t* data, uint32_t len, uint8_t shape) noexcept {
    const uint8_t* p = data;
    const uint8_t* const end = data + len;
    while (p < end && is_ws(*p)) ++p;
    if (p >= end) return false;

    const uint8_t first = *p;
    switch (shape) {
        case JSON_SHAPE_OBJECT:
            if (first != '{') return false;
            break;
        case JSON_SHAPE_ARRAY:
            if (first != '[') return false;
            break;
        case JSON_SHAPE_SCALAR:
            if (first == '{' || first == '[') return false;
            break;
        default:
            break;  // JSON_SHAPE_VALUE accepts any root
    }

    if (first == '{' || first == '[') {
        const uint8_t* last = end;
        while (is_ws(last[-1])) --last;  // stops at `first` at the latest
        if (last[-1] != (first == '{' ? '}' : ']')) return false;
    }
    return true;
}

inline bool root_matches_shape(yyjson_val* root, uint8_t shape) noexcept {
    switch (shape) {
        case JSON_SHAPE_OBJECT:
            return yyjson_is_obj(root);
        case JSON_SHAPE_ARRAY:
            return yyjson_is_arr(root);
        case JSON_SHAPE_SCALAR:
            return !yyjson_is_obj(root) && !yyjson_is_arr(root);
        default:
            return true;
    }
}

}  // namespace jv

// Is `[data, data+len)` a well-formed JSON document whose root satisfies `shape`?
// `pool` must have been sized for at least `len` (see max_slot_length).
inline bool json_is_wellformed(const uint8_t* data, uint32_t len, uint8_t shape,
                               IsJsonReadPool& pool) noexcept {
    if (!jv::passes_gates(data, len, shape)) return false;
    yyjson_doc* doc = pool.read(reinterpret_cast<const char*>(data), len, nullptr);
    if (doc == nullptr) return false;
    const bool ok = jv::root_matches_shape(yyjson_doc_get_root(doc), shape);
    yyjson_doc_free(doc);
    return ok;
}

}  // namespace ops
}  // namespace draken
