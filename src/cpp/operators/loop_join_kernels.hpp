// loop_join_kernels.hpp
//
// C++ kernels for the two loop-join operators:
//   * nested_loop_match  — uint64 hash equality double loop with SIMD probe
//   * non_equi_emit      — bit-extraction from a BoolVector mask, emitting
//                          (i, j) index pairs for one left row at a time
//
// Header-only so we can include it directly from the consolidated
// opteryx.operators._operators Cython extension without adding a new
// translation unit. Both kernels are noexcept and release no Python state.
//
// Targets ARM NEON (dev) and AVX2 (prod). Falls back to scalar where
// neither is available. The scalar path is always correct; SIMD is opt-in
// via compiler-defined macros.

#pragma once

#include <cstdint>
#include <cstddef>
#include <vector>

#if defined(__ARM_NEON) || defined(__ARM_NEON__)
  #include <arm_neon.h>
  #define OPTERYX_LOOPJOIN_NEON 1
#endif

#if defined(__AVX2__)
  #include <immintrin.h>
  #define OPTERYX_LOOPJOIN_AVX2 1
#endif

namespace opteryx { namespace operators {

// ---------------------------------------------------------------------------
// nested_loop_match
//
// For every pair (i, j) with left_hashes[i] == right_hashes[j], append i
// to out_left and j to out_right. Output order is row-major over the outer
// side (smaller of the two; the caller may swap).
//
// Complexity O(nl * nr). We use SIMD lane equality on the inner side to
// process 2 (NEON) or 4 (AVX2) right rows per iteration, then bit-scan
// the mask. Matches are expected to be sparse, so a single tzcnt loop
// per chunk is the right shape.
// ---------------------------------------------------------------------------

inline void nested_loop_match(
    const uint64_t* __restrict left_hashes,
    std::size_t nl,
    const uint64_t* __restrict right_hashes,
    std::size_t nr,
    std::vector<int32_t>& out_left,
    std::vector<int32_t>& out_right) noexcept
{
    if (nl == 0 || nr == 0) return;

    // Smaller side outer for L1 residency of the inner sweep.
    const bool left_outer = (nl <= nr);
    const uint64_t* outer = left_outer ? left_hashes : right_hashes;
    const uint64_t* inner = left_outer ? right_hashes : left_hashes;
    const std::size_t n_outer = left_outer ? nl : nr;
    const std::size_t n_inner = left_outer ? nr : nl;

    for (std::size_t o = 0; o < n_outer; ++o) {
        const uint64_t key = outer[o];
        std::size_t j = 0;

#if OPTERYX_LOOPJOIN_AVX2
        // 4 lanes per iteration. _mm256_cmpeq_epi64 produces 0xFFFF...FFFF
        // per matching lane; movemask collapses lane sign bits to 4 bits
        // when treated as pd. Use cast to keep it integer-only.
        const __m256i vkey = _mm256_set1_epi64x(static_cast<long long>(key));
        for (; j + 4 <= n_inner; j += 4) {
            __m256i v = _mm256_loadu_si256(
                reinterpret_cast<const __m256i*>(inner + j));
            __m256i eq = _mm256_cmpeq_epi64(v, vkey);
            // Convert per-lane mask to a 4-bit value (one bit per match).
            int mask = _mm256_movemask_pd(_mm256_castsi256_pd(eq));
            while (mask) {
                int lane = __builtin_ctz(static_cast<unsigned>(mask));
                std::size_t hit = j + static_cast<std::size_t>(lane);
                if (left_outer) {
                    out_left.push_back(static_cast<int32_t>(o));
                    out_right.push_back(static_cast<int32_t>(hit));
                } else {
                    out_left.push_back(static_cast<int32_t>(hit));
                    out_right.push_back(static_cast<int32_t>(o));
                }
                mask &= mask - 1;
            }
        }
#elif OPTERYX_LOOPJOIN_NEON
        // 2 lanes per iteration via uint64x2 equality.
        const uint64x2_t vkey = vdupq_n_u64(key);
        for (; j + 2 <= n_inner; j += 2) {
            uint64x2_t v = vld1q_u64(inner + j);
            uint64x2_t eq = vceqq_u64(v, vkey);
            uint64_t lo = vgetq_lane_u64(eq, 0);
            uint64_t hi = vgetq_lane_u64(eq, 1);
            if (lo) {
                if (left_outer) {
                    out_left.push_back(static_cast<int32_t>(o));
                    out_right.push_back(static_cast<int32_t>(j));
                } else {
                    out_left.push_back(static_cast<int32_t>(j));
                    out_right.push_back(static_cast<int32_t>(o));
                }
            }
            if (hi) {
                std::size_t hit = j + 1;
                if (left_outer) {
                    out_left.push_back(static_cast<int32_t>(o));
                    out_right.push_back(static_cast<int32_t>(hit));
                } else {
                    out_left.push_back(static_cast<int32_t>(hit));
                    out_right.push_back(static_cast<int32_t>(o));
                }
            }
        }
#endif
        // Scalar tail / fallback.
        for (; j < n_inner; ++j) {
            if (inner[j] == key) {
                if (left_outer) {
                    out_left.push_back(static_cast<int32_t>(o));
                    out_right.push_back(static_cast<int32_t>(j));
                } else {
                    out_left.push_back(static_cast<int32_t>(j));
                    out_right.push_back(static_cast<int32_t>(o));
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// non_equi_emit_indices
//
// Given a bit-packed BoolVector mask of length right_rows produced by the
// Draken typed comparison for one left row, emit (left_index, j) pairs for
// every j where mask is set (and, when present, the null bitmap marks j as
// valid). The bitmaps are LSB-first within each byte to match Draken.
//
// This walks the mask 64 bits at a time using a popcount-driven bit scan.
// For sparse masks this is dramatically faster than a per-bit branch.
// ---------------------------------------------------------------------------

inline void non_equi_emit_indices(
    int32_t left_index,
    const uint8_t* __restrict data_bits,
    const uint8_t* __restrict null_bits,   // may be NULL
    std::size_t right_rows,
    std::vector<int32_t>& out_left,
    std::vector<int32_t>& out_right) noexcept
{
    if (right_rows == 0 || data_bits == nullptr) return;

    std::size_t j = 0;
    const std::size_t whole = right_rows / 64;

    // 64-bit word loop. We read possibly-unaligned bytes and assemble a
    // 64-bit chunk. ANDing with the null mask (if present) gives the set
    // of emit-eligible bit positions; ctz scans them.
    for (std::size_t w = 0; w < whole; ++w) {
        uint64_t data_word = 0;
        uint64_t null_word = ~uint64_t(0);
        // Memcpy is the canonical way to do an unaligned 64-bit load
        // without triggering UB; modern compilers fold this to a mov.
        __builtin_memcpy(&data_word, data_bits + (j >> 3), 8);
        if (null_bits != nullptr) {
            __builtin_memcpy(&null_word, null_bits + (j >> 3), 8);
        }
        uint64_t hits = data_word & null_word;
        while (hits) {
            int bit = __builtin_ctzll(hits);
            out_left.push_back(left_index);
            out_right.push_back(static_cast<int32_t>(j + static_cast<std::size_t>(bit)));
            hits &= hits - 1;
        }
        j += 64;
    }

    // Tail. Walk remaining bits one by one — at most 63 of them.
    for (; j < right_rows; ++j) {
        const std::size_t byte = j >> 3;
        const unsigned bit = static_cast<unsigned>(j & 7);
        if (null_bits != nullptr) {
            if (((null_bits[byte] >> bit) & 1u) == 0u) continue;
        }
        if (((data_bits[byte] >> bit) & 1u) != 0u) {
            out_left.push_back(left_index);
            out_right.push_back(static_cast<int32_t>(j));
        }
    }
}

}}  // namespace opteryx::operators
