#pragma once
// draken/core/validity_word.h — word-wide (64 logical row) validity classification.
//
// WHY THIS EXISTS
// ---------------
// Kernels historically branched exactly once, on `validity == nullptr`, and
// otherwise ran the whole column on a validity-aware path. The common real
// shape is neither "no nulls" nor "many nulls": it is a column that is 99.9%
// non-null but still carries a bitmap because of one row. Classifying 64
// logical rows at a time lets a kernel
//   * run the cheap no-mask loop over an ALL_VALID word, and
//   * SKIP THE COMPUTATION ENTIRELY over an ALL_NULL word.
// The second is the structural win: it removes work, not just masking.
//
// WHAT THIS IS NOT
// ----------------
// This discriminates on the NULL MASK, not on encoding shape. It is not §11
// shape dispatch and needs no shape ruling. Validity is indexed by LOGICAL ROW
// i; data is indexed by selection[i]. A word of 64 logical rows does NOT
// correspond to any contiguous run of `data` when the vector is dict-shaped —
// never use a word boundary to justify a contiguous data load. Only the
// Identity hint (DRAKEN_SEL_IDENTITY) can do that, and it does so independently.
//
// CORRECTNESS CONTRACT
// --------------------
// The uniform access contract is data[selection[i]] for i in [0, length). A
// word classification must never change the answer or skip a live row:
//   ALL_VALID  — every meaningful bit set    -> result identical to masking by 0xFF..
//   ALL_NULL   — every meaningful bit clear  -> every one of those rows is NULL, so
//                the result bit is 0 and the output validity bit is 0. Kernels
//                writing into a zero-initialised buffer may skip the word whole.
//   MIXED      — anything else               -> take the per-row/masked path.
//
// PARTIAL TAIL WORD
// -----------------
// draken's convention is that bits beyond the logical row count are NOT
// meaningful (cf. cmp_copy_validity in draken/ops/int64_compare.h, which masks
// the last byte on copy). This header masks them off before classifying, so a
// tail word whose meaningful bits are all set but whose padding bits are
// garbage classifies as ALL_VALID, not MIXED. It also never reads past
// ceil(n/8) bytes of the source, so an unpadded validity buffer is safe.
//
// This header deliberately depends on nothing but <stdint.h>/<string.h> so any
// kernel can include it. In particular it does NOT include core/bitmap_ops.h.

#include <stdint.h>
#include <stddef.h>
#include <string.h>

#if defined(__BYTE_ORDER__) && (__BYTE_ORDER__ != __ORDER_LITTLE_ENDIAN__)
#error "draken validity bitmaps are LSB-first within a byte; the word view assumes little-endian byte order"
#endif

// Classification of one 64-logical-row window of a validity bitmap.
typedef enum {
    DRAKEN_VW_ALL_NULL  = 0,   // no meaningful bit set   — every row NULL
    DRAKEN_VW_MIXED     = 1,   // some set, some clear
    DRAKEN_VW_ALL_VALID = 2    // every meaningful bit set — no row NULL
} DrakenValidityWord;

// Number of 64-row words spanning `n` logical rows (the last may be partial).
static inline uint32_t draken_validity_word_count(uint32_t n) {
    return (n + 63u) >> 6;
}

// Meaningful-bit mask for word `word_idx` of an `n`-row column.
// Full words -> ~0; the tail word -> low (n & 63) bits; 0 when the word is
// entirely past the end.
static inline uint64_t draken_validity_word_mask(uint32_t word_idx, uint32_t n) {
    const uint64_t first = (uint64_t)word_idx << 6;
    if (first >= (uint64_t)n) return 0u;
    const uint64_t rows = (uint64_t)n - first;
    if (rows >= 64u) return ~(uint64_t)0;
    return ((uint64_t)1 << rows) - (uint64_t)1;
}

// Load the meaningful validity bits for word `word_idx` (logical rows
// [word_idx*64, min(n, word_idx*64+64))). Bit k of the result is the validity
// of logical row word_idx*64 + k; bits beyond the row count are 0.
//
// `validity` must be non-NULL (the all-valid case is the caller's single
// up-front `validity == nullptr` branch, which is strictly cheaper than this).
// Reads at most ceil(n/8) bytes — safe on an unpadded bitmap.
static inline uint64_t draken_validity_bits(
    const uint8_t* validity, uint32_t word_idx, uint32_t n)
{
    const uint64_t mask = draken_validity_word_mask(word_idx, n);
    if (mask == 0u) return 0u;

    const uint32_t byte_off  = word_idx << 3;               // 8 bytes per word
    const uint32_t nbytes    = (n + 7u) >> 3;               // meaningful bytes in total
    const uint32_t avail     = nbytes - byte_off;           // >= 1 because mask != 0
    const uint32_t take      = avail < 8u ? avail : 8u;

    uint64_t w = 0u;
    memcpy(&w, validity + byte_off, take);                  // LE: byte k -> bits 8k..8k+7
    return w & mask;
}

// FULL-WORD FAST LOAD — for word_idx < (n >> 6) only.
//
// Every bit of such a word is meaningful, so the tail mask is unconditionally
// ~0 and can be skipped entirely: classification collapses to one 8-byte load
// and two compares (== ~0 -> ALL_VALID, == 0 -> ALL_NULL, else MIXED).
//
// This matters more than it looks. A bit-packed compare kernel runs at roughly
// half a cycle per row, i.e. ~70 cycles per 64-row word; routing every word
// through draken_validity_word_mask's branches measured a 22% REGRESSION on the
// motivating almost-all-valid column — the classification cost more than the
// masking it removed. Hot loops must use this helper for the full words and
// leave the general form to the tail.
//
// Caller MUST guarantee word_idx < (n >> 6); this reads 8 bytes unconditionally.
static inline uint64_t draken_validity_full_bits(
    const uint8_t* validity, uint32_t word_idx)
{
    uint64_t w;
    memcpy(&w, validity + (word_idx << 3), 8);   // LE: byte k -> bits 8k..8k+7
    return w;
}

// Classify one word. `bits` must be the value returned by draken_validity_bits
// for the same (word_idx, n) — i.e. already tail-masked.
static inline DrakenValidityWord draken_validity_classify(
    uint64_t bits, uint32_t word_idx, uint32_t n)
{
    const uint64_t mask = draken_validity_word_mask(word_idx, n);
    if (bits == mask) return DRAKEN_VW_ALL_VALID;   // includes the empty-mask case
    if (bits == 0u)   return DRAKEN_VW_ALL_NULL;
    return DRAKEN_VW_MIXED;
}

// Convenience: load + classify in one call, handing back the bits.
static inline DrakenValidityWord draken_validity_word(
    const uint8_t* validity, uint32_t word_idx, uint32_t n, uint64_t* out_bits)
{
    const uint64_t bits = draken_validity_bits(validity, word_idx, n);
    if (out_bits) *out_bits = bits;
    return draken_validity_classify(bits, word_idx, n);
}

// ---------------------------------------------------------------------------
// SET-BIT ITERATION (for kernels that SKIP null rows rather than mask them)
// ---------------------------------------------------------------------------
// The skeleton below suits kernels that COMPUTE every row and mask the result.
// A kernel whose per-row body is expensive typically does the opposite — it
// already writes `if (!valid) continue;` and skips. Such a kernel has ALREADY
// captured most of the ALL_NULL win, so word classification buys it much less
// than it buys a masking kernel. Do not assume otherwise; measure.
//
// What it DOES buy is removal of the per-row branch, which matters because that
// branch is data-dependent: on a column with ~50% scattered nulls it mispredicts
// about half the time. Measured on str_starts_with, a uniformly-random 50%-null
// column ran 2.12x SLOWER than the same column with NO validity bitmap at all —
// the misprediction cost more than double the actual string work.
//
// The fix is to iterate SET BITS instead of testing rows: no per-row branch, and
// the loop trip count is the number of valid rows.
//
//   while (bits) {
//       const uint32_t i = base + (uint32_t)__builtin_ctzll(bits);
//       bits &= bits - 1;                    // clear lowest set bit
//       body(i);
//   }
//
// ALL_VALID words should still take a plain 64-iteration loop — ctz-hopping a
// full word is strictly more work than walking it.

// ---------------------------------------------------------------------------
// DRAKEN_FOR_EACH_VALID_ROW — traverse every NON-NULL logical row.
//
// For kernels whose per-row body is expensive enough that they already SKIP
// null rows (`if (!valid) continue;`) rather than compute-and-mask. Such a
// kernel has already captured most of the ALL_NULL win; what this buys it is
// removal of the per-row, data-dependent validity BRANCH.
//
//   ALL_NULL  word -> skipped outright, zero per-row work
//   ALL_VALID word -> walked via draken_identity64, no validity test
//   MIXED     word -> set bits COMPACTED into a scratch array, then walked:
//                     no per-row branch, trip count = number of valid rows
//
// ⚠ WHY A MACRO AND NOT A TEMPLATE + LAMBDA.
// The obvious C++ form — a `template<typename Fn>` helper taking the body as a
// lambda — was implemented, measured, and REJECTED. Routing a large body
// through the callable stopped it inlining: str_contains (body inlines a SIMD
// substring search) measured **+40% to +50%** against its own baseline, on
// EVERY null density including a column with no validity bitmap at all, whose
// path is a byte-identical loop. always_inline did not fix it; by-value capture
// did not fix it. The same loop written inline at the call site measured
// -5% to -95%. Cheap bodies (str_starts_with, a short memcmp) were unaffected
// either way, so a lambda helper benchmarked on a cheap kernel alone looks
// fine and silently penalises the expensive ones it was introduced for.
//
// The body is expanded at exactly ONE site. An earlier version had a separate
// `validity == NULL` fast loop, expanding the body twice; that cost **+6.2%**
// on a column with NO nulls — the most common shape there is — purely from the
// larger function. A NULL validity is instead fed through the same word loop
// with bits synthesised from the mask, so every row density shares one body.
//
// ⚠ `continue` / `break` inside the body bind to this macro's INNER row loop,
// not to any loop in the caller — `continue` happens to mean "next valid row",
// which is usually what was meant, but `break` silently exits only the current
// 64-row word. Do not use `break` in a body; use a flag.
//
// `iv` is the caller-chosen name bound to the LOGICAL ROW index. Validity is
// indexed by logical row; the body is responsible for the uniform
// data[selection[iv]] access. The body is variadic so it may contain commas.
// ---------------------------------------------------------------------------
static const uint32_t draken_identity64[64] = {
     0, 1, 2, 3, 4, 5, 6, 7, 8, 9,10,11,12,13,14,15,
    16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,
    32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,
    48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63
};

#define DRAKEN_FOR_EACH_VALID_ROW(validity_, n_, iv, ...)                      \
    do {                                                                       \
        const uint8_t* const _dvw_v = (validity_);                             \
        const uint32_t       _dvw_n = (n_);                                    \
        const uint32_t       _dvw_nw  = draken_validity_word_count(_dvw_n);    \
        const uint32_t       _dvw_nfw = _dvw_n >> 6;                           \
        uint32_t _dvw_sc[64];                                                  \
        for (uint32_t _dvw_w = 0; _dvw_w < _dvw_nw; ++_dvw_w) {                \
            const uint64_t _dvw_m = draken_validity_word_mask(_dvw_w, _dvw_n); \
            uint64_t _dvw_b =                                                  \
                (_dvw_v == NULL) ? _dvw_m                                      \
              : (_dvw_w < _dvw_nfw)                                            \
                    ? draken_validity_full_bits(_dvw_v, _dvw_w)                \
                    : draken_validity_bits(_dvw_v, _dvw_w, _dvw_n);            \
            if (_dvw_b == (uint64_t)0) continue;                               \
            const uint32_t* _dvw_ar;                                           \
            uint32_t        _dvw_cn;                                           \
            if (_dvw_b == _dvw_m) {                                            \
                /* ALL_VALID. The mask is always a contiguous low-bit run, so  \
                 * its popcount is the row count and identity64 indexes it. */ \
                _dvw_ar = draken_identity64;                                   \
                _dvw_cn = (uint32_t)__builtin_popcountll(_dvw_m);              \
            } else {                                                           \
                _dvw_cn = 0u;                                                  \
                while (_dvw_b) {                                               \
                    _dvw_sc[_dvw_cn++] = (uint32_t)__builtin_ctzll(_dvw_b);    \
                    _dvw_b &= _dvw_b - 1u;                                     \
                }                                                              \
                _dvw_ar = _dvw_sc;                                             \
            }                                                                  \
            const uint32_t _dvw_base = _dvw_w << 6;                            \
            for (uint32_t _dvw_k = 0; _dvw_k < _dvw_cn; ++_dvw_k) {            \
                const uint32_t iv = _dvw_base + _dvw_ar[_dvw_k];               \
                __VA_ARGS__                                                    \
            }                                                                  \
        }                                                                      \
    } while (0)

// ---------------------------------------------------------------------------
// DRAKEN_FOR_EACH_LIVE_BYTE — traverse output BYTES whose 64-row word has at
// least one valid row. The companion to DRAKEN_FOR_EACH_VALID_ROW, for
// MASK-style kernels: ones that pack 8 results into an output byte and AND the
// validity byte, rather than skipping null rows.
//
// ⚠ DO NOT "upgrade" a mask-style kernel to DRAKEN_FOR_EACH_VALID_ROW.
// It was tried on all six string_compare.h kernels and measured up to
// **+96%** — str_compare_scalar(gt) on a 1%-null column went 1.89 -> 3.71
// ns/row, and the no-null column regressed +80%. The 8-way byte pack has NO
// read-modify-write dependency on dst, so the eight string comparisons issue
// independently; a per-row `dst[i>>3] |= ...` serialises them behind a
// loop-carried dependency on one byte. The packing is load-bearing here even
// though it buys nothing in the (already per-row) string_search.h kernels.
//
// So this macro changes ONE thing: a word with no valid rows at all is skipped,
// which is where the wasted work actually is. The body — the pack and store —
// is untouched and still appears at exactly one site. ALL_VALID and MIXED words
// both run it; distinguishing them buys nothing because `m & validity[b]` is
// already a single cheap AND.
//
// Consecutive LIVE words are coalesced into ONE flat byte loop. Without that,
// the inner loop's trip count is capped at 8 and the nested form measured a
// flat **+8%** on str_compare_scalar(eq) for every density that is not mostly
// all-null — including a column with no validity bitmap, where the macro does
// no bitmap work at all. A short, runtime-bounded inner loop simply does not
// pipeline the way the original single flat `for (b < whole_bytes)` did. With
// coalescing a no-null column is one run, i.e. exactly the original loop.
//
// `bv` is the caller-chosen name bound to the OUTPUT BYTE index, covering only
// whole bytes [0, n>>3). The caller keeps its own partial-byte tail loop for
// the trailing n & 7 rows.
// ---------------------------------------------------------------------------
// Does 64-row word `w` contain at least one valid row? A NULL validity means
// all-valid, so every word is live. Internal to DRAKEN_FOR_EACH_LIVE_BYTE.
#define DRAKEN_VW_WORD_LIVE(v_, w_, n_, nfw_)                                  \
    ((v_) == NULL ||                                                           \
     (((w_) < (nfw_)) ? draken_validity_full_bits((v_), (w_))                  \
                      : draken_validity_bits((v_), (w_), (n_))) != (uint64_t)0)

#define DRAKEN_FOR_EACH_LIVE_BYTE(validity_, n_, bv, ...)                      \
    do {                                                                       \
        const uint8_t* const _dlb_v = (validity_);                             \
        const uint32_t _dlb_n   = (n_);                                        \
        const uint32_t _dlb_wb  = _dlb_n >> 3;                                 \
        const uint32_t _dlb_nw  = draken_validity_word_count(_dlb_n);          \
        const uint32_t _dlb_nfw = _dlb_n >> 6;                                 \
        uint32_t _dlb_w = 0u;                                                  \
        while (_dlb_w < _dlb_nw) {                                             \
            if (!DRAKEN_VW_WORD_LIVE(_dlb_v, _dlb_w, _dlb_n, _dlb_nfw)) {      \
                ++_dlb_w; continue;                                            \
            }                                                                  \
            uint32_t _dlb_we = _dlb_w + 1u;                                    \
            while (_dlb_we < _dlb_nw &&                                        \
                   DRAKEN_VW_WORD_LIVE(_dlb_v, _dlb_we, _dlb_n, _dlb_nfw))     \
                ++_dlb_we;                                                     \
            uint32_t _dlb_hi = _dlb_we << 3;                                   \
            if (_dlb_hi > _dlb_wb) _dlb_hi = _dlb_wb;                          \
            for (uint32_t bv = (_dlb_w << 3); bv < _dlb_hi; ++bv) {            \
                __VA_ARGS__                                                    \
            }                                                                  \
            _dlb_w = _dlb_we;                                                  \
        }                                                                      \
    } while (0)

// ---------------------------------------------------------------------------
// ADOPTION SKELETON
// ---------------------------------------------------------------------------
// A kernel writing a bit-packed result into a ZERO-INITIALISED destination
// buffer adopts this shape. `n_full_words = n >> 6` covers only rows whose
// bytes are whole; the remaining n & 63 rows keep the kernel's existing
// byte-loop + scalar tail, which already handles partial bytes correctly.
//
//   if (validity == nullptr) {
//       run_no_mask(0, n);                   // unchanged all-valid path
//   } else {
//       const uint32_t nfw = n >> 6;
//       uint32_t w = 0;
//       while (w < nfw) {
//           const uint64_t bits = draken_validity_full_bits(validity, w);
//           uint32_t we = w + 1;
//           // Coalesce the RUN of same-class words, then emit ONE flat loop over
//           // it. A per-word 8-byte inner loop has too short a trip count for the
//           // vectoriser and measured slower than doing no classification at all.
//           if (bits == ~(uint64_t)0) {
//               while (we < nfw && draken_validity_full_bits(validity, we) == ~(uint64_t)0) ++we;
//               run_no_mask(w << 6, (we - w) << 6);     // no per-row masking
//           } else if (bits == 0) {
//               while (we < nfw && draken_validity_full_bits(validity, we) == 0) ++we;
//               /* skip: dst already 0 — (we-w)*64 rows of work removed */
//           } else {
//               while (we < nfw) { const uint64_t x = draken_validity_full_bits(validity, we);
//                                  if (x == 0 || x == ~(uint64_t)0) break; ++we; }
//               run_masked(w << 6, (we - w) << 6);
//           }
//           w = we;
//       }
//       run_existing_tail(nfw << 6, n);      // untouched partial-byte handling
//   }
//
// The ALL_NULL arm must only be taken when the destination bits AND the output
// validity bits for those rows are already correct at zero. If a kernel writes
// a non-zero identity into null rows, it must not skip — classify and mask.
