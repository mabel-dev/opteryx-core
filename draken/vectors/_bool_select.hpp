#pragma once
//
// Branchless BoolVector → row-index extraction.
//
// Converts a packed bit array (LSB-first within each byte) into a flat array
// of int32_t row indices for the rows where the bit is 1.
//
// The core loop uses the branchless "always-write, conditionally-advance"
// pattern: write unconditionally, increment the output pointer by 0 or 1
// based on the bit value. This eliminates the data-dependent branch that
// causes predictor thrash at ~50% selectivity, and the fixed 8-iteration
// inner loop allows the compiler to unroll and auto-vectorise.
//
// Compare to the LUT approach (256-entry offset table + variable inner loop):
//   LUT:         mispredicts `if byte == 0` skip at medium selectivity;
//                variable inner loop blocks auto-vectorisation.
//   Branchless:  no data-dependent branches; always 8 stores per byte;
//                compiler unrolls and may emit SIMD on NEON / AVX2.
//
// API
// ───
//   size_t draken::bool_select::extract_indices(
//       const uint8_t* bits,    // packed bit array, one bit per row, LSB-first
//       const uint8_t* valid,   // null bitmap, same layout; NULL = all valid
//       int32_t*       out,     // caller-allocated, capacity >= n_rows
//       size_t         n_rows
//   ) -> number of indices written to out
//
// Caller must allocate `out` with at least `n_rows` elements.
//

#include <stdint.h>
#include <stddef.h>

namespace draken { namespace bool_select {

static inline size_t extract_indices(
    const uint8_t* __restrict__ bits,
    const uint8_t* __restrict__ valid,
    int32_t*       __restrict__ out,
    size_t                      n_rows)
{
    size_t o           = 0;
    size_t n_full      = n_rows >> 3;

    if (valid == nullptr) {
        for (size_t b = 0; b < n_full; ++b) {
            const uint8_t  d    = bits[b];
            const int32_t  base = static_cast<int32_t>(b << 3);
            out[o] = base;     o += (d     ) & 1u;
            out[o] = base + 1; o += (d >> 1) & 1u;
            out[o] = base + 2; o += (d >> 2) & 1u;
            out[o] = base + 3; o += (d >> 3) & 1u;
            out[o] = base + 4; o += (d >> 4) & 1u;
            out[o] = base + 5; o += (d >> 5) & 1u;
            out[o] = base + 6; o += (d >> 6) & 1u;
            out[o] = base + 7; o += (d >> 7) & 1u;
        }
    } else {
        for (size_t b = 0; b < n_full; ++b) {
            const uint8_t  d    = bits[b] & valid[b];
            const int32_t  base = static_cast<int32_t>(b << 3);
            out[o] = base;     o += (d     ) & 1u;
            out[o] = base + 1; o += (d >> 1) & 1u;
            out[o] = base + 2; o += (d >> 2) & 1u;
            out[o] = base + 3; o += (d >> 3) & 1u;
            out[o] = base + 4; o += (d >> 4) & 1u;
            out[o] = base + 5; o += (d >> 5) & 1u;
            out[o] = base + 6; o += (d >> 6) & 1u;
            out[o] = base + 7; o += (d >> 7) & 1u;
        }
    }

    // Remainder rows (n_rows not a multiple of 8)
    const size_t rem = n_rows & 7;
    if (rem) {
        uint8_t d = bits[n_full];
        if (valid != nullptr) d &= valid[n_full];
        d &= static_cast<uint8_t>((1u << rem) - 1u);
        const int32_t base = static_cast<int32_t>(n_full << 3);
        for (size_t bit = 0; bit < rem; ++bit) {
            out[o] = base + static_cast<int32_t>(bit);
            o += (d >> bit) & 1u;
        }
    }

    return o;
}

}} // namespace draken::bool_select
