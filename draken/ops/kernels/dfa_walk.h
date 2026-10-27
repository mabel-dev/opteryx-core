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
    bool interleave;          // match_all walks rows through match_rows<kLanes>
};

// Lanes for match_rows. Measured on ARM (M-series, 2026-10-26): 8 beats 4/6/12/16.
constexpr uint32_t kLanes = 8;

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
    // Interleave only programs that walk most of every row byte by byte: an
    // accelerated state hands its bytes to find_escape, and an anchored start
    // dies within a few bytes — both break the lockstep so often that the
    // per-row walk is faster (measured: 1.1-2.0x slower interleaved).
    bool any_accel = false;
    for (uint32_t k = 0; k < num_states; ++k) any_accel |= p->accel[static_cast<size_t>(k) << 2] != 0;
    p->interleave = (flags & 0x01) == 0 && !any_accel;
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

// Row prologue: the start-state accept, the required-literal prefilter and the
// start-state skip. Returns the verdict (0/1) when one is already known, else -1
// with *cp = where the walk resumes (always < end, in state 0).
inline int begin(const DfaProgram* p, const uint8_t* s, const uint8_t* end, const uint8_t** cp) {
    if (!p->has_end && accepts(p, 0)) return 1;
    const uint32_t slen = static_cast<uint32_t>(end - s);
    if (p->lit_len != 0 &&
        (p->lit_len > slen ||
         draken_like_prog::search_literal(s, end - p->lit_len + 1, p->lit, p->lit_len) == nullptr))
        return 0;
    const uint8_t* c = s;
    if (p->start_accel) {
        c = find_escape(c, end, p->accel + 1, p->accel[0]);
        if (c == nullptr) c = end;
    }
    if (c == end) return (p->has_end && accepts(p, 0)) ? 1 : 0;
    *cp = c;
    return -1;
}

// Resolve a special transition (>= kSpecial) taken at c. Returns the verdict
// (0/1), or -1 with *state set and *cp advanced to the next escape byte (or end).
inline int special(const DfaProgram* p, uint32_t nx, const uint8_t** cp, const uint8_t* end,
                   uint32_t* state) {
    if (nx == kDead) return 0;
    if (nx & kAccept) return 1;
    *state = nx & kStateMask;
    // Accelerated: every non-escape byte maps this state to itself.
    const uint8_t* a = p->accel + (static_cast<size_t>(*state) << 2);
    const uint8_t* c = find_escape(*cp, end, a + 1, a[0]);
    *cp = (c == nullptr) ? end : c;
    return -1;
}

// Walk [c, end) from `state` to a verdict.
inline int walk(const DfaProgram* p, const uint8_t* c, const uint8_t* end, uint32_t state) {
    const uint16_t* const table = p->table;
    while (c < end) {
        const uint32_t nx = table[(state << 8) + *c++];
        if (nx < kSpecial) { state = nx; continue; }
        const int v = special(p, nx, &c, end, &state);
        if (v >= 0) return v;
    }
    return (p->has_end && accepts(p, state)) ? 1 : 0;
}

// Walk a decoded program over one subject. Returns 1/0 for match/no-match.
inline int match(const DfaProgram* p, const uint8_t* s, uint32_t slen) {
    const uint8_t* const end = s + slen;
    const uint8_t* c = nullptr;
    const int v = begin(p, s, end, &c);
    return v >= 0 ? v : walk(p, c, end, 0);
}

// Batch walk: rows [0, n) through L interleaved lanes. One row's walk is a serial
// load -> index -> load chain (~4-5 cycles/byte); L independent rows step in
// lockstep so their chains overlap. Each lockstep run lasts the shortest live
// lane's remaining bytes, so the inner loop has no per-byte end test, and it
// breaks on the first special transition in any lane (one OR + one test per
// step: every special entry has bit 14 or 15 set). Finished lanes refill from
// the next row; when rows run out the live lanes finish with walk().
//   row(i, &s, &len) -> false to skip row i (e.g. NULL);  emit(i, verdict 0/1).
// Verdicts are emitted out of row order. Same verdict as match() per row.
template <uint32_t L, class Row, class Emit>
inline void match_rows(const DfaProgram* p, uint32_t n, Row&& row, Emit&& emit) {
    const uint16_t* const table = p->table;
    const uint8_t* c[L];
    const uint8_t* e[L];
    uint32_t st[L];
    uint32_t rid[L];
    uint32_t next = 0;
    // Load the next row that needs a walk into lane k; false when rows run out.
    auto admit = [&](uint32_t k) -> bool {
        while (next < n) {
            const uint32_t i = next++;
            const uint8_t* s;
            uint32_t len;
            if (!row(i, &s, &len)) continue;
            const uint8_t* cc = nullptr;
            const int v = begin(p, s, s + len, &cc);
            if (v >= 0) { emit(i, v); continue; }
            c[k] = cc; e[k] = s + len; st[k] = 0; rid[k] = i;
            return true;
        }
        return false;
    };
    uint32_t live = 0;
    while (live < L && admit(live)) ++live;
    bool drained = live < L;
    while (!drained) {
        size_t m = static_cast<size_t>(e[0] - c[0]);
        for (uint32_t k = 1; k < L; ++k) {
            const size_t r = static_cast<size_t>(e[k] - c[k]);
            m = r < m ? r : m;
        }
        // Lockstep run on register copies (no address escapes the block).
        {
            const uint8_t* cc[L];
            uint32_t ss[L];
            for (uint32_t k = 0; k < L; ++k) { cc[k] = c[k]; ss[k] = st[k]; }
            uint32_t any;
            do {
                any = 0;
                for (uint32_t k = 0; k < L; ++k) {
                    const uint32_t nx = table[(ss[k] << 8) + *cc[k]++];
                    ss[k] = nx;
                    any |= nx;
                }
            } while (--m != 0 && (any & 0xC000u) == 0);
            for (uint32_t k = 0; k < L; ++k) { c[k] = cc[k]; st[k] = ss[k]; }
        }
        for (uint32_t k = 0; k < L; ++k) {
            int v = -1;
            if (st[k] >= kSpecial) v = special(p, st[k], &c[k], e[k], &st[k]);
            if (v < 0 && c[k] == e[k]) v = (p->has_end && accepts(p, st[k])) ? 1 : 0;
            if (v < 0) continue;
            emit(rid[k], v);
            if (!admit(k)) { e[k] = nullptr; drained = true; }
        }
    }
    for (uint32_t k = 0; k < live; ++k)
        if (e[k] != nullptr) emit(rid[k], walk(p, c[k], e[k], st[k]));
}

// Every row through the walk the program suits: match_rows<kLanes> when
// p->interleave, else one row at a time. Same row/emit contract as match_rows.
template <class Row, class Emit>
inline void match_all(const DfaProgram* p, uint32_t n, Row&& row, Emit&& emit) {
    if (p->interleave) {
        match_rows<kLanes>(p, n, row, emit);
        return;
    }
    for (uint32_t i = 0; i < n; ++i) {
        const uint8_t* s;
        uint32_t len;
        if (!row(i, &s, &len)) continue;
        emit(i, match(p, s, len));
    }
}

// One-shot decode + match. Returns 1/0, or -1 for a malformed blob.
inline int match_blob(const uint8_t* blob, size_t blob_len,
                      const uint8_t* s, uint32_t slen) {
    DfaProgram p;
    if (!decode(blob, blob_len, &p)) return -1;
    return match(&p, s, slen);
}

}  // namespace draken_dfa
