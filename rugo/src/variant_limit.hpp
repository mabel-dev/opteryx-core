#pragma once
// rugo/src/variant_limit.hpp — the deepest a VARIANT value may nest. Shared so the JSONL
// reader (which refuses a deeper record it reads as a VARIANT), the Variant encoder and the
// Variant renderer cannot disagree about it.
//
// Depth counts containers: a record's top-level value is at depth 0 and a value directly
// inside it at depth 1. A value at a depth greater than kVariantMaxDepth — inside more than
// kVariantMaxDepth objects or arrays — cannot be held.

#include <cstddef>
#include <cstdint>

namespace rugo {

inline constexpr int kVariantMaxDepth = 1024;

// Does the JSON text [p, p + n) hold a value deeper than kVariantMaxDepth? The text is a
// record the reader already accepted; only brackets outside strings are counted. A value at
// depth kVariantMaxDepth + 1 sits inside a container opened at that depth, so the record is
// too deep exactly when such a container is not empty. That takes kVariantMaxDepth + 1
// opening and closing brackets around at least one byte, so shorter text is never read.
inline bool variant_too_deep(const uint8_t* p, size_t n) {
    if (n <= 2 * static_cast<size_t>(kVariantMaxDepth)) return false;
    int depth = 0;
    for (size_t i = 0; i < n; ++i) {
        const uint8_t c = p[i];
        if (c == '"') {
            for (++i; i < n && p[i] != '"'; ++i)
                if (p[i] == '\\') ++i;
        } else if (c == '{' || c == '[') {
            if (++depth == kVariantMaxDepth + 1) {
                size_t j = i + 1;
                while (j < n && (p[j] == ' ' || p[j] == '\t' || p[j] == '\r' || p[j] == '\n')) ++j;
                if (j < n && p[j] != '}' && p[j] != ']') return true;
            }
        } else if (c == '}' || c == ']') {
            --depth;
        }
    }
    return false;
}

}  // namespace rugo
