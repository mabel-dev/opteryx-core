#ifndef SIMD_BITMAP_H
#define SIMD_BITMAP_H

#include <cstddef>
#include <cstdint>

namespace simd {

// Expand a null bitmap byte into 8 uint64_t masks (one per bit).
// For each bit, if the bit is set (row is valid), the mask is all-ones (0xFFFFFFFFFFFFFFFF).
// If the bit is clear (row is null), the mask is all-zeros (0x0000000000000000).
//
// This allows branchless blending:
//   scratch[j] = (data[j] & mask[j]) | (null_sentinel & ~mask[j])
//
// args:
//   bitmap_byte: the uint8_t bitmap byte to expand
//   out_mask: pointer to array of 8 uint64_t values (caller must allocate)
void expand_bitmap_byte_to_u64_masks(uint8_t bitmap_byte, uint64_t* out_mask);

// Expand a range of a null bitmap into uint64 masks.
// Handles arbitrary bit offsets and counts using SIMD when available.
//
// args:
//   bitmap: pointer to uint8_t bitmap array
//   bit_offset: bit index to start from (can be non-byte-aligned)
//   out_mask: output array of uint64_t masks (caller allocates `count` elements)
//   count: number of rows to process
void expand_bitmap_to_u64_masks(
    const uint8_t* bitmap,
    std::size_t bit_offset,
    uint64_t* out_mask,
    std::size_t count
);

}  // namespace simd

#endif  // SIMD_BITMAP_H
