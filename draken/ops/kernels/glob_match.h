#pragma once
// Shared byte-level SQL LIKE glob matcher, used by draken_like
// (function_kernels.cpp) and draken_like_any (function_like_any.cpp) — one
// implementation, no duplication (.claude/CLAUDE.md §2). `%` matches any run
// (byte-safe on UTF-8: whole-suffix only), `_` matches exactly ONE BYTE
// (callers must reject `_` for NVARCHAR/UTF-8), NO backslash escaping.

#include <cstdint>

namespace draken_glob {

inline uint8_t ascii_lower(uint8_t c) {
    return (c >= 'A' && c <= 'Z') ? static_cast<uint8_t>(c + 32) : c;
}

// Iterative glob match over bytes with %-backtracking. ci = ASCII case-fold.
inline bool like_match(const uint8_t* s, uint32_t sn,
                       const uint8_t* p, uint32_t pn, bool ci) {
    uint32_t si = 0, pi = 0, star_p = UINT32_MAX, star_s = 0;
    while (si < sn) {
        if (pi < pn && p[pi] == '%') { star_p = ++pi; star_s = si; continue; }
        if (pi < pn && (p[pi] == '_' ||
                        (ci ? ascii_lower(p[pi]) == ascii_lower(s[si])
                            : p[pi] == s[si]))) { ++pi; ++si; continue; }
        if (star_p != UINT32_MAX) { pi = star_p; si = ++star_s; continue; }
        return false;
    }
    while (pi < pn && p[pi] == '%') ++pi;
    return pi == pn;
}

}  // namespace draken_glob
