#pragma once
// draken/core/uuid_text.h — the ONE text form of a UUID (docs/UUID_TYPE_DESIGN.md).
//
// Every renderer (casts, CSV/JSONL writers, nested JSON) and every parser (casts,
// literals, readers) goes through these two functions, so parse strictness and
// render form cannot drift apart. Dependency-free on purpose: the text writers
// include it without pulling in the kernel headers.

#include <stddef.h>
#include <stdint.h>

namespace draken { namespace ops {

// ── text form ──────────────────────────────────────────────────────────────────

// Canonical text: 36 chars, lowercase, hyphenated 8-4-4-4-12. `out` holds 36 bytes.
static inline void uuid_format(const uint8_t* p, char* out) noexcept {
    static const char hex[] = "0123456789abcdef";
    int o = 0;
    for (int i = 0; i < 16; ++i) {
        if (i == 4 || i == 6 || i == 8 || i == 10) out[o++] = '-';
        out[o++] = hex[p[i] >> 4];
        out[o++] = hex[p[i] & 0x0F];
    }
}

static inline int uuid_hex_nibble(char c) noexcept {
    if (c >= '0' && c <= '9') return c - '0';
    if (c >= 'a' && c <= 'f') return c - 'a' + 10;
    if (c >= 'A' && c <= 'F') return c - 'A' + 10;
    return -1;
}

// Strict parse (ruling U5): exactly 36 chars, hyphens at 8/13/18/23, hex digits of
// either case elsewhere. Returns false on anything else; `out` is then unspecified.
static inline bool uuid_parse(const char* s, size_t len, uint8_t* out) noexcept {
    if (len != 36) return false;
    int b = 0;
    for (size_t i = 0; i < 36;) {
        if (i == 8 || i == 13 || i == 18 || i == 23) {
            if (s[i] != '-') return false;
            ++i;
            continue;
        }
        const int h = uuid_hex_nibble(s[i]);
        const int l = uuid_hex_nibble(s[i + 1]);
        if (h < 0 || l < 0) return false;
        out[b++] = static_cast<uint8_t>((h << 4) | l);
        i += 2;
    }
    return true;
}

}} // namespace draken::ops
