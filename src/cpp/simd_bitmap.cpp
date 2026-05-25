#include "simd_bitmap.h"

#include <cstring>

#if defined(__ARM_NEON) || defined(__ARM_NEON__)
#include <arm_neon.h>
#endif

#if defined(__AVX2__)
#include <immintrin.h>
#endif

#if defined(__riscv) && defined(__riscv_vector)
#include <riscv_vector.h>
#endif

namespace simd {

// Helper used by the partial-byte paths in expand_bitmap_to_u64_masks.
static inline uint64_t bit_to_mask(int bit) { return -(uint64_t)bit; }

// ============================================================================
// expand_bitmap_byte_to_u64_masks
//
// Converts 8 bits of a validity byte into 8 uint64_t masks, where each mask
// is all-zeros (null) or all-ones (valid).  Used as the inner step of
// expand_bitmap_to_u64_masks.
//
// SIMD strategy: isolate each bit into a byte lane, widen 0x00/0xFF to
// 0x00…00 / 0xFF…FF via sign-extension cascade.
//
// Compile-time dispatch — no select_dispatch overhead (called ~n/8 times per
// column; the atomic cache-check cost would dominate at that call rate).
// ============================================================================

#if defined(__ARM_NEON) || defined(__ARM_NEON__)

void expand_bitmap_byte_to_u64_masks(uint8_t bitmap_byte, uint64_t* out_mask) {
    // Bit isolators: bit_sels[i] = 1<<i
    static const uint8_t k_sels[8] = {0x01,0x02,0x04,0x08,0x10,0x20,0x40,0x80};
    uint8x8_t bcast  = vdup_n_u8(bitmap_byte);
    uint8x8_t sels   = vld1_u8(k_sels);
    uint8x8_t bits   = vand_u8(bcast, sels);            // isolate each bit in its lane
    uint8x8_t bytes8 = vcgt_u8(bits, vdup_n_u8(0));     // 0xFF where bit set, 0x00 where clear

    // Cascade-widen 8×u8 → 4×(u64x2) masks via sign-extension.
    // vmovl on 0xFF bytes: 0xFF(u8) → 0xFFFF(u16) → 0xFFFFFFFF(u32) → 0xFFFFFFFFFFFFFFFF(u64)
    uint16x8_t  w16    = vmovl_u8(bytes8);
    uint32x4_t  w32_lo = vmovl_u16(vget_low_u16(w16));
    uint32x4_t  w32_hi = vmovl_u16(vget_high_u16(w16));

    vst1q_u64(out_mask + 0, vmovl_u32(vget_low_u32(w32_lo)));
    vst1q_u64(out_mask + 2, vmovl_u32(vget_high_u32(w32_lo)));
    vst1q_u64(out_mask + 4, vmovl_u32(vget_low_u32(w32_hi)));
    vst1q_u64(out_mask + 6, vmovl_u32(vget_high_u32(w32_hi)));
}

#elif defined(__AVX2__)

void expand_bitmap_byte_to_u64_masks(uint8_t bitmap_byte, uint64_t* out_mask) {
    // Broadcast bitmap_byte to all 16 lanes, AND with per-bit isolators,
    // compare == 0 and flip to get 0xFF (bit set) or 0x00 (bit clear),
    // then sign-extend each int8 lane to int64 (0xFF=-1 → all-ones, 0x00=0 → all-zeros).
    __m128i bcast = _mm_set1_epi8(static_cast<char>(bitmap_byte));
    // Bit selectors for lanes 0-7 (lanes 8-15 are don't-care zeros)
    __m128i sels  = _mm_set_epi8(0,0,0,0,0,0,0,0,
                                 (char)0x80,(char)0x40,(char)0x20,(char)0x10,
                                 (char)0x08,(char)0x04,(char)0x02,(char)0x01);
    __m128i bits  = _mm_and_si128(bcast, sels);
    // eq_zero is 0xFF where bit was 0; flip to get 0xFF where bit was 1
    __m128i bytes8 = _mm_xor_si128(_mm_cmpeq_epi8(bits, _mm_setzero_si128()),
                                   _mm_set1_epi8(-1));
    // Sign-extend 4 bytes at a time to 4×int64 (0xFF=-1 → 0xFFFF…FF, 0x00 → 0x0000…00)
    _mm256_storeu_si256(reinterpret_cast<__m256i*>(out_mask + 0),
                        _mm256_cvtepi8_epi64(bytes8));
    _mm256_storeu_si256(reinterpret_cast<__m256i*>(out_mask + 4),
                        _mm256_cvtepi8_epi64(_mm_bsrli_si128(bytes8, 4)));
}

#else

// Scalar fallback (also the RVV path — 8-element fixed-size is too small for
// variable-VLEN vectorisation to be worthwhile).
void expand_bitmap_byte_to_u64_masks(uint8_t bitmap_byte, uint64_t* out_mask) {
    for (int i = 0; i < 8; ++i) {
        int bit = (bitmap_byte >> i) & 1;
        out_mask[i] = -(uint64_t)bit;
    }
}

#endif

void expand_bitmap_to_u64_masks(
    const uint8_t* bitmap,
    std::size_t bit_offset,
    uint64_t* out_mask,
    std::size_t count
) {
    if (bitmap == nullptr || out_mask == nullptr || count == 0) {
        return;
    }

    std::size_t byte_offset = bit_offset >> 3;    // bit_offset / 8
    std::size_t start_bit = bit_offset & 7;       // bit_offset % 8

    std::size_t i = 0;

    // Handle partial first byte if needed
    if (start_bit != 0) {
        uint8_t byte = bitmap[byte_offset];
        for (std::size_t j = start_bit; j < 8 && i < count; ++j, ++i) {
            int bit = (byte >> j) & 1;
            out_mask[i] = bit_to_mask(bit);
        }
        byte_offset++;
    }

    // Process full bytes
    while (i + 8 <= count) {
        uint8_t byte = bitmap[byte_offset];
        expand_bitmap_byte_to_u64_masks(byte, out_mask + i);
        i += 8;
        byte_offset++;
    }

    // Handle remaining bits
    if (i < count) {
        uint8_t byte = bitmap[byte_offset];
        std::size_t remaining = count - i;
        for (std::size_t j = 0; j < remaining; ++j) {
            int bit = (byte >> j) & 1;
            out_mask[i + j] = bit_to_mask(bit);
        }
    }
}

}  // namespace simd
