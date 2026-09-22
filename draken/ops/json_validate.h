// SQL:2016 `IS [NOT] JSON` — well-formedness over JSON TEXT.
//
// This is a VALIDATOR, not a parser. It walks the bytes once, keeps no value,
// and allocates nothing: the only state is a caller-owned container-kind
// bitstack, hoisted out of the per-row call so a column of N documents pays
// for it once. yyjson (draken/ops/json_extract.h) is deliberately NOT used —
// `yyjson_read` materialises a whole document, which is strictly more work
// than the predicate needs and puts an allocator on a path that has no reason
// to touch one.
//
// Grammar is RFC 8259. Rejected, in particular: trailing content after the
// root value, trailing commas, leading zeroes (`01`), a bare `-` or `.5`,
// single-quoted strings, unescaped control bytes inside a string, unterminated
// strings, and `\u` escapes without four hex digits. An empty or
// whitespace-only input has no root value and is therefore not well-formed.
//
// UTF-8 is NOT validated. The operand types this predicate admits include
// VARBINARY, whose bytes carry no encoding promise, and RFC 8259
// well-formedness is a grammar property. `IS JSON` answers "does this parse",
// not "is this valid UTF-8".

#pragma once

#include <cstddef>
#include <cstdint>

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

// Maximum container nesting. A document nested deeper than this is reported NOT
// well-formed rather than overflowing the bitstack — the failure is closed, and
// it is the answer the predicate exists to give. 1024 is far past any realistic
// ingest payload.
constexpr int JSON_MAX_DEPTH = 1024;

// One bit per open container: set = object, clear = array. Caller-owned so the
// per-row validator neither allocates nor initialises it; every bit is written
// (on push) before it is read (on the matching close).
struct JsonDepthStack {
    uint64_t kinds[JSON_MAX_DEPTH / 64];
};

namespace jv {

inline bool is_ws(uint8_t c) noexcept {
    return c == ' ' || c == '\t' || c == '\n' || c == '\r';
}

inline void skip_ws(const uint8_t*& p, const uint8_t* end) noexcept {
    while (p < end && is_ws(*p)) ++p;
}

inline bool hex4(const uint8_t*& p, const uint8_t* end) noexcept {
    if (end - p < 4) return false;
    for (int i = 0; i < 4; ++i) {
        const uint8_t c = p[i];
        const bool ok =
            (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F');
        if (!ok) return false;
    }
    p += 4;
    return true;
}

// `p` is at the opening quote on entry; one past the closing quote on success.
inline bool string(const uint8_t*& p, const uint8_t* end) noexcept {
    ++p;  // opening quote
    while (p < end) {
        const uint8_t c = *p;
        if (c == '"') {
            ++p;
            return true;
        }
        if (c == '\\') {
            ++p;
            if (p >= end) return false;
            switch (*p) {
                case '"':
                case '\\':
                case '/':
                case 'b':
                case 'f':
                case 'n':
                case 'r':
                case 't':
                    ++p;
                    break;
                case 'u':
                    ++p;
                    if (!hex4(p, end)) return false;
                    break;
                default:
                    return false;  // undefined escape
            }
            continue;
        }
        if (c < 0x20u) return false;  // unescaped control byte
        ++p;
    }
    return false;  // unterminated
}

inline bool digits(const uint8_t*& p, const uint8_t* end) noexcept {
    const uint8_t* const start = p;
    while (p < end && *p >= '0' && *p <= '9') ++p;
    return p != start;
}

// `p` is at the first byte of the number on entry. A leading zero is consumed
// alone, so `01` leaves `p` on the `1` and the caller's separator check rejects
// it — the same mechanism that rejects `1 2` at the root.
inline bool number(const uint8_t*& p, const uint8_t* end) noexcept {
    if (p < end && *p == '-') ++p;
    if (p >= end) return false;
    if (*p == '0') {
        ++p;
    } else if (!digits(p, end)) {
        return false;  // bare '-', a leading '+', or '.5'
    }
    if (p < end && *p == '.') {
        ++p;
        if (!digits(p, end)) return false;
    }
    if (p < end && (*p == 'e' || *p == 'E')) {
        ++p;
        if (p < end && (*p == '+' || *p == '-')) ++p;
        if (!digits(p, end)) return false;
    }
    return true;
}

inline bool keyword(const uint8_t*& p, const uint8_t* end, const char* lit,
                    size_t n) noexcept {
    if (static_cast<size_t>(end - p) < n) return false;
    for (size_t i = 0; i < n; ++i)
        if (p[i] != static_cast<uint8_t>(lit[i])) return false;
    p += n;
    return true;
}

}  // namespace jv

// Is `[data, data+len)` a well-formed JSON document whose root satisfies
// `shape`? `stack` is scratch — its contents on entry and exit are meaningless.
inline bool json_is_wellformed(const uint8_t* data, uint32_t len, uint8_t shape,
                               JsonDepthStack& stack) noexcept {
    const uint8_t* p = data;
    const uint8_t* const end = data + len;

    jv::skip_ws(p, end);
    if (p >= end) return false;  // empty or whitespace-only: no root value

    // The first significant byte fixes the root node kind, so the shape gate is
    // one branch taken before any walking — a malformed document that could not
    // have matched the asked-for shape is rejected without being parsed.
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

    uint64_t* const kinds = stack.kinds;
    int depth = 0;

value:
    jv::skip_ws(p, end);
    if (p >= end) return false;
    switch (*p) {
        case '{':
            if (depth >= JSON_MAX_DEPTH) return false;
            kinds[depth >> 6] |= (static_cast<uint64_t>(1) << (depth & 63));
            ++depth;
            ++p;
            jv::skip_ws(p, end);
            if (p >= end) return false;
            if (*p == '}') {
                ++p;
                --depth;
                goto after_value;
            }
            goto key;
        case '[':
            if (depth >= JSON_MAX_DEPTH) return false;
            kinds[depth >> 6] &= ~(static_cast<uint64_t>(1) << (depth & 63));
            ++depth;
            ++p;
            jv::skip_ws(p, end);
            if (p >= end) return false;
            if (*p == ']') {
                ++p;
                --depth;
                goto after_value;
            }
            goto value;
        case '"':
            if (!jv::string(p, end)) return false;
            goto after_value;
        case 't':
            if (!jv::keyword(p, end, "true", 4)) return false;
            goto after_value;
        case 'f':
            if (!jv::keyword(p, end, "false", 5)) return false;
            goto after_value;
        case 'n':
            if (!jv::keyword(p, end, "null", 4)) return false;
            goto after_value;
        default:
            if (!jv::number(p, end)) return false;
            goto after_value;
    }

key:
    // `p` is at the first non-whitespace byte of a member name.
    if (*p != '"') return false;
    if (!jv::string(p, end)) return false;
    jv::skip_ws(p, end);
    if (p >= end || *p != ':') return false;
    ++p;
    goto value;

after_value:
    jv::skip_ws(p, end);
    if (depth == 0) return p == end;  // exactly one root value, nothing trailing
    if (p >= end) return false;       // unclosed container
    {
        const int d = depth - 1;
        const bool in_object = ((kinds[d >> 6] >> (d & 63)) & 1u) != 0u;
        const uint8_t c = *p;
        if (c == ',') {
            ++p;
            if (in_object) {
                jv::skip_ws(p, end);
                if (p >= end) return false;
                goto key;  // rejects a trailing comma: `}` is not a member name
            }
            goto value;  // rejects a trailing comma: `]` is not a value
        }
        if (in_object ? (c == '}') : (c == ']')) {
            ++p;
            --depth;
            goto after_value;
        }
        return false;  // a value must be followed by a separator or a close
    }
}

}  // namespace ops
}  // namespace draken
