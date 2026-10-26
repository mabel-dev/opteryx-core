#pragma once
// Shared byte-DFA walk over a compiled blob (vector_dfa_compile.pyx, blob version 3).
// Used by draken_rlike (function_rlike.cpp), the length-adaptive LIKE path
// (function_kernels.cpp) and vector_rlike — one implementation, no duplication
// (.claude/CLAUDE.md §2).
//
// Two-stage like like_program.h: decode() parses the (per-morsel constant) blob
// ONCE into a DfaProgram; match() walks the decoded program per row.
//
// Blob format (little-endian), produced by _subset_and_serialize:
//   u8  version (=3)
//   u8  flags   bit0 anchored_start, bit1 anchored_end, bit2 start state accelerated
//   u16 num_states
//   u8  lit_len (0 = no prefilter)
//   u8  pad
//   lit_len bytes: a literal every match contains (the required-literal prefilter)
//   pad to even
//   num_states * 4 bytes accel entries: [escape_count (0..3), e0, e1, e2]
//   accept_bitmap: ceil(num_states / 8) bytes, padded to even
//   num_states * 256 u16 transitions. Low 14 bits = target state; flag bits:
//     0x8000 target accepts AND the pattern is not end-anchored (early accept)
//     0x4000 target is accelerated (it loops to itself on every byte except
//            its escape bytes, so the walk jumps straight to the next escape)
//     0xFFFF dead (no match possible)
//   Every entry >= 0x4000 is special, so the hot loop pays ONE compare per byte.
//
// Start state is state 0.

#include <cstdint>
#include <cstddef>
#include <cstring>

#include "ops/kernels/like_program.h"   // draken_like_prog::search_literal (prefilter)

#if defined(__ARM_NEON) || defined(__ARM_NEON__)
#include <arm_neon.h>
#define DRAKEN_DFA_NEON 1
#elif defined(__SSE2__)
#include <emmintrin.h>
#define DRAKEN_DFA_SSE2 1
#endif

namespace draken_dfa {

constexpr uint16_t kDead = 0xFFFFu;
constexpr uint16_t kSpecial = 0x4000u;
constexpr uint16_t kAccept = 0x8000u;
constexpr uint16_t kAccel = 0x4000u;
constexpr uint16_t kStateMask = 0x3FFFu;

struct DfaProgram {
    const uint16_t* table;
    const uint8_t* accept;
    const uint8_t* accel;     // num_states * 4
    const uint8_t* lit;
    uint32_t lit_len;
    uint16_t num_states;
    bool has_end;
    bool start_accel;
};

// Parse the blob once. Returns 1 on success, 0 if malformed (compiler/kernel
// format drift — the blob is entirely plan-time-produced, so callers fail loud).
inline int decode(const uint8_t* blob, size_t blob_len, DfaProgram* p) {
    if (blob_len < 6) return 0;
    if (blob[0] != 3) return 0;
    const uint8_t flags = blob[1];
    const uint16_t num_states =
        static_cast<uint16_t>(blob[2]) | (static_cast<uint16_t>(blob[3]) << 8);
    if (num_states == 0 || num_states > kStateMask) return 0;
    const uint32_t lit_len = blob[4];
    size_t off = 6 + lit_len;
    off += off & 1u;
    const size_t accel_off = off;
    off += static_cast<size_t>(num_states) * 4;
    const size_t accept_off = off;
    size_t accept_len = (static_cast<size_t>(num_states) + 7) / 8;
    accept_len += accept_len & 1u;
    off += accept_len;
    const size_t table_off = off;
    off += static_cast<size_t>(num_states) * 256 * 2;
    if (off != blob_len) return 0;

    p->lit = blob + 6;
    p->lit_len = lit_len;
    p->accel = blob + accel_off;
    p->accept = blob + accept_off;
    p->table = reinterpret_cast<const uint16_t*>(blob + table_off);
    p->num_states = num_states;
    p->has_end = (flags & 0x02) != 0;
    p->start_accel = (flags & 0x04) != 0;
    return 1;
}

inline bool accepts(const DfaProgram* p, uint32_t state) {
    return ((p->accept[state >> 3] >> (state & 7)) & 1u) != 0;
}

// First byte in [s, end) equal to one of the n (0..3) escape bytes, or nullptr.
// n == 0 means the state loops on every byte: nothing escapes it.
inline const uint8_t* find_escape(const uint8_t* s, const uint8_t* end,
                                  const uint8_t* e, uint32_t n) {
    if (n == 0 || s >= end) return nullptr;
    if (n == 1) return static_cast<const uint8_t*>(std::memchr(s, e[0], static_cast<size_t>(end - s)));
    const uint8_t e0 = e[0], e1 = e[1], e2 = (n == 3) ? e[2] : e[1];
#if defined(DRAKEN_DFA_NEON)
    const uint8x16_t v0 = vdupq_n_u8(e0), v1 = vdupq_n_u8(e1), v2 = vdupq_n_u8(e2);
    while (end - s >= 16) {
        const uint8x16_t x = vld1q_u8(s);
        const uint8x16_t m = vorrq_u8(vorrq_u8(vceqq_u8(x, v0), vceqq_u8(x, v1)), vceqq_u8(x, v2));
        const uint64_t bits = vget_lane_u64(vreinterpret_u64_u8(
            vshrn_n_u16(vreinterpretq_u16_u8(m), 4)), 0);
        if (bits != 0) return s + (__builtin_ctzll(bits) >> 2);
        s += 16;
    }
#elif defined(DRAKEN_DFA_SSE2)
    const __m128i v0 = _mm_set1_epi8(static_cast<char>(e0));
    const __m128i v1 = _mm_set1_epi8(static_cast<char>(e1));
    const __m128i v2 = _mm_set1_epi8(static_cast<char>(e2));
    while (end - s >= 16) {
        const __m128i x = _mm_loadu_si128(reinterpret_cast<const __m128i*>(s));
        const __m128i m = _mm_or_si128(_mm_or_si128(_mm_cmpeq_epi8(x, v0), _mm_cmpeq_epi8(x, v1)),
                                       _mm_cmpeq_epi8(x, v2));
        const int bits = _mm_movemask_epi8(m);
        if (bits != 0) return s + __builtin_ctz(static_cast<unsigned>(bits));
        s += 16;
    }
#endif
    for (; s < end; ++s)
        if (*s == e0 || *s == e1 || *s == e2) return s;
    return nullptr;
}

// Walk a decoded program over one subject. Returns 1/0 for match/no-match.
inline int match(const DfaProgram* p, const uint8_t* s, uint32_t slen) {
    if (!p->has_end && accepts(p, 0)) return 1;
    const uint8_t* const end = s + slen;
    if (p->lit_len != 0 &&
        (p->lit_len > slen ||
         draken_like_prog::search_literal(s, end - p->lit_len + 1, p->lit, p->lit_len) == nullptr))
        return 0;

    const uint16_t* const table = p->table;
    uint32_t state = 0;
    const uint8_t* c = s;
    if (p->start_accel) {
        c = find_escape(c, end, p->accel + 1, p->accel[0]);
        if (c == nullptr) c = end;
    }
    while (c < end) {
        const uint32_t nx = table[(state << 8) + *c++];
        if (nx < kSpecial) { state = nx; continue; }
        if (nx == kDead) return 0;
        if (nx & kAccept) return 1;
        state = nx & kStateMask;
        // Accelerated: every non-escape byte maps this state to itself.
        const uint8_t* a = p->accel + (static_cast<size_t>(state) << 2);
        c = find_escape(c, end, a + 1, a[0]);
        if (c == nullptr) break;
    }
    if (p->has_end) return accepts(p, state) ? 1 : 0;
    return 0;
}

// One-shot decode + match. Returns 1/0, or -1 for a malformed blob.
inline int match_blob(const uint8_t* blob, size_t blob_len,
                      const uint8_t* s, uint32_t slen) {
    DfaProgram p;
    if (!decode(blob, blob_len, &p)) return -1;
    return match(&p, s, slen);
}

}  // namespace draken_dfa
