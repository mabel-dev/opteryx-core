#pragma once

#include <cstddef>
#include <cstdint>
#include <vector>
#include <atomic>

#ifdef __AVX2__
#include <immintrin.h>
#endif

#include "cpu_features.h"
#include "simd_dispatch.h"

// SIMD-accelerated validity bitmap building for parquet decoding.
//
// Pattern: Build null bitmap from definition levels via fast bit-packing.
// Use case: After parquet column decoding, create validity bitmap for Draken.
//
// Algorithm:
//   - Load 8 int32_t def_levels at a time
//   - Compare each to max_def (all equal → valid, else → null)
//   - Pack 8 boolean results into 1 byte
//   - Store byte to bitmap

namespace parquet_simd {

// ---------------------------------------------------------------------------
// Validity Bitmap Building from Definition Levels
// ---------------------------------------------------------------------------

// Scalar fallback: standard bit-by-bit approach
static inline void build_validity_bitmap_scalar(
    const int32_t* def_levels,
    size_t total_rows,
    int32_t max_def,
    std::vector<uint8_t>& bitmap)
{
    int32_t bitmap_bytes = (int32_t)((total_rows + 7) / 8);
    bitmap.resize(bitmap_bytes, 0);

    for (size_t i = 0; i < total_rows; ++i) {
        if (def_levels[i] == max_def) {
            bitmap[i / 8] |= (1 << (i % 8));
        }
    }
}

#ifdef __AVX2__
// AVX2: Process 8 definition levels per iteration
// Load 8 int32_t → compare each to max_def → pack 8 bits into 1 byte
static inline void build_validity_bitmap_avx2(
    const int32_t* def_levels,
    size_t total_rows,
    int32_t max_def,
    std::vector<uint8_t>& bitmap)
{
    int32_t bitmap_bytes = (int32_t)((total_rows + 7) / 8);
    bitmap.resize(bitmap_bytes, 0);

    // Create comparison vector with max_def replicated 8 times
    __m256i max_def_vec = _mm256_set1_epi32(max_def);

    size_t full_groups = total_rows / 8;

    for (size_t group = 0; group < full_groups; ++group) {
        size_t base_idx = group * 8;

        // Load 8 def_levels
        __m256i def_vec = _mm256_loadu_si256((__m256i*)(def_levels + base_idx));

        // Compare each def_level == max_def
        // Result: 0xFFFFFFFF (true) or 0x00000000 (false) per element
        __m256i cmp_result = _mm256_cmpeq_epi32(def_vec, max_def_vec);

        // Extract sign bit from each 32-bit comparison result
        // Sign bit is 1 when cmp_result == 0xFFFFFFFF
        int32_t mask_bits = _mm256_movemask_epi8(cmp_result);

        // movemask_epi8 extracts MSB from each byte (8 bits from 32 bytes)
        // We need only the MSBs of the 4 bytes corresponding to our 8 int32s
        // Rearrange: bytes [0,4,8,12,16,20,24,28] → bits [0,1,2,3,4,5,6,7]
        uint8_t bitmap_byte = 0;

        // Extract the MSB from each of the 8 int32_t comparison results
        // Each 4-byte group has MSB we care about
        for (int i = 0; i < 8; ++i) {
            // Extract byte at offset 4*i + 3 (MSB of i-th int32_t)
            int byte_offset = 4 * i + 3;
            uint8_t byte_val = (mask_bits >> (8 * (byte_offset / 4))) & 0xFF;
            // Check if MSB is set
            if (byte_val & 0x80) {
                bitmap_byte |= (1 << i);
            }
        }

        bitmap[group] = bitmap_byte;
    }

    // Handle remainder rows (less than 8)
    size_t remainder = total_rows % 8;
    if (remainder > 0) {
        size_t base_idx = full_groups * 8;
        uint8_t remainder_byte = 0;

        for (size_t i = 0; i < remainder; ++i) {
            if (def_levels[base_idx + i] == max_def) {
                remainder_byte |= (1 << i);
            }
        }

        bitmap[full_groups] = remainder_byte;
    }
}
#endif

// Dispatch
using build_validity_bitmap_fn_t = void(*)(const int32_t*, size_t, int32_t, std::vector<uint8_t>&);
static std::atomic<build_validity_bitmap_fn_t> s_build_bitmap_cache{nullptr};

static inline build_validity_bitmap_fn_t get_build_validity_bitmap_fn()
{
    return simd::select_dispatch<build_validity_bitmap_fn_t>(s_build_bitmap_cache, {
#if defined(__AVX2__)
        {&cpu_supports_avx2, build_validity_bitmap_avx2},
#endif
    }, build_validity_bitmap_scalar);
}

static inline void build_validity_bitmap(
    const int32_t* def_levels,
    size_t total_rows,
    int32_t max_def,
    std::vector<uint8_t>& bitmap)
{
    return get_build_validity_bitmap_fn()(def_levels, total_rows, max_def, bitmap);
}

} // namespace parquet_simd
