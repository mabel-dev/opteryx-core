#include "simd_bitmap.h"

#include <cstring>

#if defined(__ARM_NEON) || defined(__ARM_NEON__)
#include <arm_neon.h>
#endif

#if defined(__AVX2__)
#include <immintrin.h>
#endif

namespace simd {

// Helper: convert a single bit (0 or 1) to a mask (0x0000000000000000 or 0xFFFFFFFFFFFFFFFF)
static inline uint64_t bit_to_mask(int bit) {
    return -(uint64_t)bit;
}

void expand_bitmap_byte_to_u64_masks(uint8_t bitmap_byte, uint64_t* out_mask) {
    for (int i = 0; i < 8; ++i) {
        int bit = (bitmap_byte >> i) & 1;
        out_mask[i] = bit_to_mask(bit);
    }
}

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
