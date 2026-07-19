#pragma once
// Unicode simple-casefold matching for NVARCHAR (UTF-8) haystacks — the ci
// (ILIKE) counterpart to draken_glob::like_match, which is ASCII/byte-only
// (glob_match.h) and is explicitly not valid on non-ASCII UTF-8: folded
// codepoints do not preserve byte offsets 1:1, so byte-position comparison
// is wrong, not just slow, once the codepoint range goes non-ASCII.
//
// Built on the vendored sheredom/utf8.h codepoint tables
// (third_party/utf8h/utf8.h — see draken/docs/design/E26_utf8h_lowercase_pilot.md).
// Iteration here is length-bounded, NOT NUL-terminated: utf8codepoint()/
// utf8lwrcodepoint() are pure functions of the leading byte pattern /
// codepoint value and never scan for a NUL, so no scratch-buffer copy is
// needed — callers pass the arena slice directly (§3 zero-copy).

#include <cstdint>

#include "utf8.h"

namespace draken_utf8ci {

// Decode the codepoint at *p (caller guarantees p < end), fold it to
// lowercase, and advance p past it. Returns the folded codepoint.
inline int32_t next_folded(const uint8_t*& p, const uint8_t* end) {
    (void)end;  // bound already checked by every call site
    utf8_int32_t cp;
    p = reinterpret_cast<const uint8_t*>(
        utf8codepoint(reinterpret_cast<const utf8_int8_t*>(p), &cp));
    return utf8lwrcodepoint(cp);
}

// Step back to the start of the codepoint immediately preceding `p`.
// Never steps at or before `lo`.
inline const uint8_t* prev_start(const uint8_t* p, const uint8_t* lo) {
    do { --p; } while (p > lo && (*p & 0xC0) == 0x80);
    return p;
}

// Simple-casefold a UTF-8 buffer in place: fold each codepoint to lowercase and
// write it back over the same span. utf8lwrcodepoint is byte-width-preserving
// across the WHOLE Unicode range (verified exhaustively — no fold changes a
// codepoint's UTF-8 byte length), so the output is byte-for-byte the same length
// as the input and re-encoding never overruns the source span. `src`/`dst` may
// alias (decode reads the whole codepoint before the write, widths match).
//
// This is the SINGLE fold used on BOTH sides of the LIKE-ANY split: the plan-time
// compiler (opteryx.compiled.vector_ops.compile_like_any) folds each needle with
// it before bucketing/AC construction, and draken_like_any folds each subject row
// with it at match time — one implementation, so the automaton alphabet and the
// subject bytes are guaranteed identical (no Python/C++ fold drift). Callers with
// ASCII-only data should keep using draken_glob::ascii_lower — this pays the
// codepoint-decode cost only NVARCHAR needs.
inline void casefold(const uint8_t* src, uint32_t n, uint8_t* dst) {
    const uint8_t* s = src;
    const uint8_t* end = src + n;
    uint8_t* d = dst;
    while (s < end) {
        const size_t avail = static_cast<size_t>(end - s);
        const size_t w = utf8codepointcalcsize(reinterpret_cast<const utf8_int8_t*>(s));
        if (w > avail) {
            // Truncated/malformed multibyte tail — never decode past the buffer;
            // copy the remaining bytes verbatim. (NVARCHAR subjects are validated
            // so this only guards malformed needle-literal bytes.)
            while (s < end) *d++ = *s++;
            break;
        }
        utf8_int32_t cp;
        utf8codepoint(reinterpret_cast<const utf8_int8_t*>(s), &cp);  // full cp in buffer
        const utf8_int32_t lo = utf8lwrcodepoint(cp);
        if (utf8codepointsize(lo) == w) {
            utf8catcodepoint(reinterpret_cast<utf8_int8_t*>(d), lo, w);
        } else {
            // Width would change — impossible for valid UTF-8 (fold table is
            // width-preserving, verified), so this is malformed input; pass raw.
            for (size_t k = 0; k < w; ++k) d[k] = s[k];
        }
        d += w;
        s += w;
    }
}

// Unicode-casefolded substring search (LIKE '%needle%' → InStr rewrite).
inline bool contains(const uint8_t* hay, uint32_t hlen,
                     const uint8_t* ndl, uint32_t nlen) {
    if (nlen == 0) return true;
    if (nlen > hlen) return false;
    const uint8_t* hend = hay + hlen;
    const uint8_t* nend = ndl + nlen;
    for (const uint8_t* s = hay; s < hend; ) {
        const uint8_t* hp = s;
        const uint8_t* np = ndl;
        bool match = true;
        while (np < nend) {
            if (hp >= hend) { match = false; break; }
            const int32_t hc = next_folded(hp, hend);
            const int32_t nc = next_folded(np, nend);
            if (hc != nc) { match = false; break; }
        }
        if (match) return true;
        next_folded(s, hend);  // advance the scan by one haystack codepoint
    }
    return false;
}

// Unicode-casefolded prefix test (ILIKE 'needle%' → _CI_STARTS_WITH).
inline bool starts_with(const uint8_t* hay, uint32_t hlen,
                        const uint8_t* ndl, uint32_t nlen) {
    if (nlen > hlen) return false;
    const uint8_t* hp = hay;
    const uint8_t* hend = hay + hlen;
    const uint8_t* np = ndl;
    const uint8_t* nend = ndl + nlen;
    while (np < nend) {
        if (hp >= hend) return false;
        const int32_t hc = next_folded(hp, hend);
        const int32_t nc = next_folded(np, nend);
        if (hc != nc) return false;
    }
    return true;
}

// Unicode-casefolded suffix test (ILIKE '%needle' → _CI_ENDS_WITH).
inline bool ends_with(const uint8_t* hay, uint32_t hlen,
                      const uint8_t* ndl, uint32_t nlen) {
    if (nlen > hlen) return false;
    const uint8_t* nend = ndl + nlen;
    // Walk back from hend by exactly as many codepoints as ndl has, to find
    // the candidate suffix start (folding never changes codepoint count).
    const uint8_t* base = hay + hlen;
    {
        const uint8_t* np = ndl;
        while (np < nend) {
            if (base <= hay) return false;
            base = prev_start(base, hay);
            utf8_int32_t cp;
            np = reinterpret_cast<const uint8_t*>(
                utf8codepoint(reinterpret_cast<const utf8_int8_t*>(np), &cp));
        }
    }
    const uint8_t* hp = base;
    const uint8_t* hend = hay + hlen;
    const uint8_t* np = ndl;
    while (np < nend) {
        const int32_t hc = next_folded(hp, hend);
        const int32_t nc = next_folded(np, nend);
        if (hc != nc) return false;
    }
    return true;
}

// Unicode-casefolded glob match — '%' (any run) only, same backtracking
// shape as draken_glob::like_match but codepoint-compared. Callers must
// reject '_' against NVARCHAR before calling this (one BYTE is not one
// CODEPOINT — see draken_like's existing pre-check; unchanged by this file).
inline bool like_match(const uint8_t* s, uint32_t sn,
                       const uint8_t* p, uint32_t pn) {
    const uint8_t* send = s + sn;
    const uint8_t* pend = p + pn;
    const uint8_t* sp = s;
    const uint8_t* pp = p;
    const uint8_t* star_pp = nullptr;
    const uint8_t* star_sp = nullptr;
    while (sp < send) {
        if (pp < pend && *pp == '%') {
            ++pp;
            star_pp = pp;
            star_sp = sp;
            continue;
        }
        if (pp < pend) {
            const uint8_t* sp2 = sp;
            const uint8_t* pp2 = pp;
            const int32_t sc = next_folded(sp2, send);
            const int32_t pc = next_folded(pp2, pend);
            if (sc == pc) { sp = sp2; pp = pp2; continue; }
        }
        if (star_pp != nullptr) {
            pp = star_pp;
            const uint8_t* ssp = star_sp;
            next_folded(ssp, send);
            star_sp = ssp;
            sp = star_sp;
            continue;
        }
        return false;
    }
    while (pp < pend && *pp == '%') ++pp;
    return pp == pend;
}

}  // namespace draken_utf8ci
