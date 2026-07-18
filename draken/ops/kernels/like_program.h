#pragma once
// Boolean LIKE/RLIKE matcher over a compiled SIMD op-program (blob version 2).
//
// This is the op-program counterpart to the scalar byte-transition-table DFA
// in dfa_walk.h. Where dfa_walk.h does one table[state*256+byte] L1 load PER
// BYTE, this walks a decoded program of at most 8 ops and does O(segments)
// scans instead of O(bytes) scalar lookups. A pattern like `un%believable`
// decodes to LIT("un") + SUFFIX("believable"); `a%b%c` to LIT("a") +
// SEARCH("b") + SUFFIX("c").
//
// Two-stage like _dfa_extract: decode() parses the (per-morsel constant) blob
// ONCE into a LikeProgram stack struct; match() then walks that decoded struct
// per row with zero blob re-parsing. Each SEARCH is a memchr-anchored substring
// scan (memchr the first byte, memcmp the rest) — the same shape as
// fk_contains_hit. libc memchr is SIMD-optimised, so long haystacks stay fast,
// yet it has near-zero fixed cost, so short haystacks don't pay SIMD-dispatch
// setup: measured to beat both the scalar glob and the transition-table DFA on
// short AND long real columns (single-byte segments — the common `%x%y%` shape
// — are just the rem==0 case). The anchored-prefix LIT is hoisted out of the op
// loop with an 8-byte masked word compare so the "doesn't start with the
// prefix" fast-reject is as tight as glob's.
//
// The op-program only expresses the DECOMPOSABLE subset (anchored fixed prefix
// + %-separated floating literals + optional anchored suffix). The plan-time
// compiler (compile_like_program in vector_dfa_compile.pyx) returns None for
// anything outside it, and the caller keeps the transition-table DFA
// (dfa_walk.h) as the correct fallback — no pattern loses coverage.
//
// Correctness rests on the standard greedy two-star result: for
//   [prefix] % s1 % s2 % ... % sk % [suffix]
// with unbounded `%` gaps, matching == anchor the prefix at the start, anchor
// the suffix at the true END (a suffix compare, NOT a forward search — the
// naive "find s_k then require end" would reject "…believable…believable"),
// and greedily leftmost-search the middle literals. Each middle SEARCH is
// bounded to exclude the reserved suffix tail (tail_reserve), so a middle
// literal can never be matched inside the suffix's region.
//
// SQL LIKE `%` is dotall (matches newline) — the substring search is byte-exact
// for it. RLIKE `.*` is non-dotall and needs the newline-bounded SEARCH_NONL op
// (reserved below, not yet implemented); until then RLIKE keeps the
// transition-table DFA for `.*`-gap patterns.
//
// Blob format (little-endian):
//   u8  version (=2)
//   u8  op_count (1..8)
//   u16 tail_reserve (bytes reserved at end for a terminal SUFFIX; else 0)
//   op_count ops, each:
//     u8 op_type
//     LMOP_LIT / LMOP_SEARCH / LMOP_SEARCH_NONL / LMOP_SUFFIX: u32 len, len bytes
//     LMOP_SKIP: u32 n
//     LMOP_END / LMOP_ACCEPT / LMOP_TAIL_NO_NEWLINE: (no payload)
//
// match() returns 1/0 for match/no-match; decode() returns 1/0 for ok/malformed
// (a malformed blob is compiler/kernel drift, since the blob is entirely
// plan-time-produced — the caller fails loud rather than guessing).

#include <cstdint>
#include <cstddef>
#include <cstring>

namespace draken_like_prog {

enum LikeOp : uint8_t {
    LMOP_LIT = 1,      // anchored literal at cursor
    LMOP_SKIP = 2,     // advance n bytes (`_` run, or `.`/`.+` fixed skip)
    LMOP_SEARCH = 3,   // gap + literal: forward substring search (within window)
    LMOP_SUFFIX = 5,   // trailing literal anchored at window end: compare at end (terminal)
    LMOP_END = 6,      // require cursor == window end (terminal)
    LMOP_ACCEPT = 7,   // accept remainder (terminal)
    LMOP_TAIL_NO_NEWLINE = 8,  // RLIKE trailing `.*$` (non-dotall, nothing after
                               // the last gap): accept iff [cursor, end) has no
                               // '\n' (terminal). One memchr, no forward search.
};

// Program flags (blob header byte 2). The anchor bits and line-driver bit are
// consumed only by the RLIKE line-window driver; LIKE blobs set flags == 0 and
// run the plain whole-string path (their own anchoring rides in the ops:
// LIT-prefix / SUFFIX / END).
enum LikeFlag : uint8_t {
    LFLAG_ANCHOR_START = 1,  // RLIKE `^`: segment 0 pinned to text start (line 0)
    LFLAG_ANCHOR_END = 2,    // RLIKE `$`: last segment pinned to text end (last line)
    LFLAG_LINE_DRIVER = 4,   // RLIKE non-dotall `.*`/`.+` gaps: match per newline-free window
};

struct Op {
    uint8_t type;
    uint32_t len;         // LIT/SEARCH/SUFFIX byte length, or SKIP count
    const uint8_t* lit;   // points into the blob (LIT/SEARCH/SUFFIX only)
    // For LIT literals <= 8 bytes: the literal packed into a uint64 (native-
    // endian, zero-padded) plus a byte mask, so the anchored compare is a
    // single masked register op instead of an out-of-line memcmp — the same
    // literal fusion the _DFA_EXTRACT engine uses. Valid only when len <= 8.
    uint64_t packed;
    uint64_t mask;
};

struct LikeProgram {
    Op ops[8];
    uint8_t op_count;
    uint8_t flags;          // LikeFlag bits (RLIKE anchoring + line driver)
    uint32_t tail_reserve;
    // Anchored prefix hoist: when ops[0] is a LIT it is the anchored-at-start
    // literal, and for the (very common) rows that don't start with it the
    // whole match rejects on the first bytes. Hoisting that literal out of the
    // op loop makes the reject path as tight as glob's — one field load + a
    // masked word compare — instead of paying the loop/struct-index overhead
    // first. prefix_len == 0 means "no anchored-LIT prefix; run from op 0".
    uint32_t prefix_len;
    uint64_t prefix_packed;
    uint64_t prefix_mask;   // 0 when prefix_len > 8 (memcmp path)
    const uint8_t* prefix_lit;
    uint8_t first_op;       // index of the first op the loop runs (1 if hoisted)
};

inline uint32_t read_u32(const uint8_t* p) {
    return static_cast<uint32_t>(p[0]) | (static_cast<uint32_t>(p[1]) << 8) |
           (static_cast<uint32_t>(p[2]) << 16) | (static_cast<uint32_t>(p[3]) << 24);
}

// Parse the constant blob once into `prog`. Returns 1 on success, 0 if the blob
// is malformed for this executor (bad version, truncation, unknown op, or an
// op count outside 1..8).
// Blob header (6 bytes): u8 version(=2), u8 op_count, u8 flags, u8 pad,
// u16 tail_reserve, then the ops.
inline int decode(const uint8_t* blob, size_t blob_len, LikeProgram* prog) {
    if (blob_len < 6) return 0;
    if (blob[0] != 2) return 0;
    const uint8_t op_count = blob[1];
    if (op_count == 0 || op_count > 8) return 0;
    prog->op_count = op_count;
    prog->flags = blob[2];
    prog->tail_reserve =
        static_cast<uint32_t>(blob[4]) | (static_cast<uint32_t>(blob[5]) << 8);

    const uint8_t* p = blob + 6;
    const uint8_t* bend = blob + blob_len;
    for (uint8_t i = 0; i < op_count; ++i) {
        if (p >= bend) return 0;
        const uint8_t op = *p++;
        Op& o = prog->ops[i];
        o.type = op;
        o.len = 0;
        o.lit = nullptr;
        o.packed = 0;
        o.mask = 0;
        if (op == LMOP_LIT || op == LMOP_SEARCH || op == LMOP_SUFFIX) {
            if (bend - p < 4) return 0;
            const uint32_t len = read_u32(p); p += 4;
            if (len == 0) return 0;
            if (static_cast<size_t>(bend - p) < len) return 0;
            o.len = len;
            o.lit = p;
            if (op == LMOP_LIT && len <= 8) {
                std::memcpy(&o.packed, p, len);
                o.mask = (len == 8) ? ~static_cast<uint64_t>(0)
                                    : ((static_cast<uint64_t>(1) << (8 * len)) - 1);
            }
            p += len;
        } else if (op == LMOP_SKIP) {
            if (bend - p < 4) return 0;
            o.len = read_u32(p); p += 4;
        } else if (op == LMOP_END || op == LMOP_ACCEPT || op == LMOP_TAIL_NO_NEWLINE) {
            // no payload
        } else {
            // LMOP_SEARCH_NONL (RLIKE, phase 2) and anything unknown.
            return 0;
        }
    }
    if (p != bend) return 0;

    // Hoist an anchored-LIT prefix (ops[0]) for a tight fast-reject path.
    prog->prefix_len = 0;
    prog->prefix_packed = 0;
    prog->prefix_mask = 0;
    prog->prefix_lit = nullptr;
    prog->first_op = 0;
    if (op_count >= 1 && prog->ops[0].type == LMOP_LIT) {
        prog->prefix_len = prog->ops[0].len;
        prog->prefix_packed = prog->ops[0].packed;
        prog->prefix_mask = prog->ops[0].mask;
        prog->prefix_lit = prog->ops[0].lit;
        prog->first_op = 1;
    }
    return 1;
}

// Walk a decoded program over the window [base, end) of subject `s`. Returns
// 1/0 for match. LIT anchors at `base`; SUFFIX/END anchor at `end`; SEARCH scans
// within the window. For LIKE the window is the whole string [0, slen); the
// RLIKE driver calls this once per newline-free window (a `%`/`.*` gap inside a
// window is dotall because a window never contains a newline).
__attribute__((always_inline)) inline int match_window(
        const LikeProgram* prog, const uint8_t* s, uint32_t base, uint32_t end) {
    // Anchored-prefix fast reject (hoisted ops[0] LIT): the tightest possible
    // path for the common "doesn't start with the prefix" case.
    const uint32_t win = end - base;   // base <= end always (driver guarantees)
    const uint32_t prefix_len = prog->prefix_len;
    if (prefix_len) {
        if (prefix_len > win) return 0;
        if (prog->prefix_mask != 0 && win >= 8) {
            uint64_t loaded;
            std::memcpy(&loaded, s + base, 8);
            if ((loaded & prog->prefix_mask) != prog->prefix_packed) return 0;
        } else if (std::memcmp(s + base, prog->prefix_lit, prefix_len) != 0) {
            return 0;
        }
    }

    uint32_t cursor = base + prefix_len;
    const uint32_t tail_reserve = prog->tail_reserve;
    const uint8_t count = prog->op_count;

    for (uint8_t oi = prog->first_op; oi < count; ++oi) {
        const Op& o = prog->ops[oi];

        if (o.type == LMOP_LIT) {
            if (static_cast<uint64_t>(cursor) + o.len > end) return 0;
            if (o.mask != 0 && static_cast<uint64_t>(cursor) + 8 <= end) {
                // Fast path: one unaligned 8-byte load + masked register compare
                // (literal <= 8 bytes, >= 8 bytes remain). The rare short tail
                // falls through to memcmp.
                uint64_t loaded;
                std::memcpy(&loaded, s + cursor, 8);
                if ((loaded & o.mask) != o.packed) return 0;
            } else if (std::memcmp(s + cursor, o.lit, o.len) != 0) {
                return 0;
            }
            cursor += o.len;

        } else if (o.type == LMOP_SKIP) {
            if (static_cast<uint64_t>(cursor) + o.len > end) return 0;
            cursor += o.len;

        } else if (o.type == LMOP_SEARCH) {
            // Search region is [cursor, end - tail_reserve): the reserved suffix
            // tail is off-limits so a middle literal can't land inside it.
            if (static_cast<uint64_t>(cursor) + tail_reserve + o.len > end) return 0;
            // memchr-anchored substring search (same shape as fk_contains_hit):
            // libc memchr is SIMD-optimised (so long haystacks stay fast) with
            // near-zero fixed cost (so short haystacks don't pay SIMD setup). A
            // single-byte segment (the common `%x%y%` shape) is just rem == 0.
            const uint8_t* bp = s + cursor;
            const uint8_t* limit = s + (end - tail_reserve) - o.len + 1;  // last start
            const uint32_t rem = o.len - 1;
            const uint8_t first = o.lit[0];
            for (;;) {
                const void* hit = std::memchr(bp, first,
                                              static_cast<size_t>(limit - bp));
                if (hit == nullptr) return 0;
                const uint8_t* h = static_cast<const uint8_t*>(hit);
                if (rem == 0 || std::memcmp(h + 1, o.lit + 1, rem) == 0) {
                    cursor = static_cast<uint32_t>(h - s) + o.len;
                    break;
                }
                bp = h + 1;
            }

        } else if (o.type == LMOP_SUFFIX) {
            // Terminal: the last `len` bytes of the window must equal the
            // literal, and the consumed prefix must not overlap the suffix.
            if (o.len > win) return 0;
            if (cursor > end - o.len) return 0;
            return std::memcmp(s + (end - o.len), o.lit, o.len) == 0 ? 1 : 0;

        } else if (o.type == LMOP_END) {
            return cursor == end ? 1 : 0;

        } else if (o.type == LMOP_TAIL_NO_NEWLINE) {
            // Trailing `.*$` with nothing after it: accept iff no '\n' remains
            // between cursor and window end. One memchr, no forward search.
            return std::memchr(s + cursor, '\n', end - cursor) == nullptr ? 1 : 0;

        } else {  // LMOP_ACCEPT
            return 1;
        }
    }
    // A well-formed program always ends in a terminal op.
    return 0;
}

// RLIKE line-window driver. Non-dotall `.*`/`.+` gaps confine the whole match to
// a single line, so the match reduces to running the (dotall-within-a-line)
// program over the right newline-free window(s), selected by the anchors:
//   `^…$`  one line only — a `\n` means a gap can't span it, so no match
//   `^…`   line 0 (segment 0 pinned to text start)
//   `…$`   the last line (last segment pinned to text end)
//   else   any line — try each in turn
// Anchored-prefix fast reject at text start — cheap O(prefix) check used before
// the O(n) newline scan so an anchored-start pattern rejects the common
// "doesn't start with the prefix" rows as fast as the DFA (first byte), instead
// of scanning the whole row for a newline it will never use.
__attribute__((always_inline)) inline bool prefix_rejects(
        const LikeProgram* prog, const uint8_t* s, uint32_t slen) {
    const uint32_t pl = prog->prefix_len;
    if (!pl) return false;
    if (pl > slen) return true;
    if (prog->prefix_mask != 0 && slen >= 8) {
        uint64_t loaded;
        std::memcpy(&loaded, s, 8);
        return (loaded & prog->prefix_mask) != prog->prefix_packed;
    }
    return std::memcmp(s, prog->prefix_lit, pl) != 0;
}

inline int match_rlike(const LikeProgram* prog, const uint8_t* s, uint32_t slen) {
    const bool as = (prog->flags & LFLAG_ANCHOR_START) != 0;
    const bool ae = (prog->flags & LFLAG_ANCHOR_END) != 0;

    if (as && ae) {
        if (prefix_rejects(prog, s, slen)) return 0;
        if (std::memchr(s, '\n', slen) != nullptr) return 0;
        return match_window(prog, s, 0, slen);
    }
    if (as) {
        if (prefix_rejects(prog, s, slen)) return 0;
        const void* nl = std::memchr(s, '\n', slen);
        const uint32_t hi = nl ? static_cast<uint32_t>(static_cast<const uint8_t*>(nl) - s)
                               : slen;
        return match_window(prog, s, 0, hi);
    }
    if (ae) {
        uint32_t lo = 0;
        for (uint32_t i = slen; i-- > 0;) {
            if (s[i] == '\n') { lo = i + 1; break; }
        }
        return match_window(prog, s, lo, slen);
    }
    uint32_t lo = 0;
    for (;;) {
        const void* nl = std::memchr(s + lo, '\n', slen - lo);
        const uint32_t hi = nl ? static_cast<uint32_t>(static_cast<const uint8_t*>(nl) - s)
                               : slen;
        if (match_window(prog, s, lo, hi)) return 1;
        if (nl == nullptr) break;
        lo = hi + 1;
    }
    return 0;
}

// Dispatch: LIKE (flags == 0) runs the whole-string window; RLIKE with
// non-dotall gaps (LFLAG_LINE_DRIVER) runs the per-line driver.
__attribute__((always_inline)) inline int match(
        const LikeProgram* prog, const uint8_t* s, uint32_t slen) {
    if (prog->flags & LFLAG_LINE_DRIVER) return match_rlike(prog, s, slen);
    return match_window(prog, s, 0, slen);
}

// Convenience one-shot (decode + match). Returns 1/0/-1 (-1 = malformed blob).
// Used for the kernel's one-time format validation; the hot path decodes once
// and calls match() per row.
inline int match_blob(const uint8_t* blob, size_t blob_len,
                      const uint8_t* s, uint32_t slen) {
    LikeProgram prog;
    if (!decode(blob, blob_len, &prog)) return -1;
    return match(&prog, s, slen);
}

}  // namespace draken_like_prog
