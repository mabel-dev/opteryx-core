// decode_encodings.cpp
// Non-template implementations of the Parquet encoding helpers declared in
// decode_encodings.hpp.

#include "decode_encodings.hpp"
#include <cstring>
#include <iostream>
#include <algorithm>
#include <atomic>

// SIMD dispatch (src/cpp/ is on the include path)
#include "simd_dispatch.h"
#include "cpu_features.h"

#if defined(__ARM_NEON) || defined(__ARM_NEON__)
#  include <arm_neon.h>
#endif
#if defined(__AVX2__)
#  include <immintrin.h>
#endif

// ---------------------------------------------------------------------------
// Bit-pack unpacking infrastructure
// ---------------------------------------------------------------------------
//
// Parquet RLE/bit-packing stores values in groups of 8.  For bit_width 1-8
// each group requires exactly bit_width bytes, so the entire group fits in a
// uint64_t — no inner byte-loop or division needed.
//
// Function signature:
//   void fn(const uint8_t* src, int32_t* dst, int num_groups, int bit_width)
// Unpacks num_groups * 8 values from the packed bit stream at src.
// ---------------------------------------------------------------------------

// ---- scalar helper (one group of 8) ----------------------------------------
// SAFE variant: never reads beyond this group's own byte span. Required for the
// LAST group of a run — see unpack_group_8_scalar_fast.
static inline void unpack_group_8_scalar(const uint8_t* __restrict__ src,
                                         int32_t* __restrict__ dst,
                                         int bit_width)
{
    if (__builtin_expect(bit_width <= 8, 1)) {
        // For bw 1-8, bytes_per_group == bit_width exactly.
        uint64_t word = 0;
        for (int i = 0; i < bit_width; i++)
            word |= (uint64_t)src[i] << (i * 8);
        const uint64_t mask = (1ULL << bit_width) - 1;
        for (int i = 0; i < 8; i++)
            dst[i] = (int32_t)((word >> (i * bit_width)) & mask);
    } else {
        // bit_width is 9-32 in this branch; use uint64_t to avoid shift-by-32 UB
        // when bit_width==32 the mask (1ULL<<32)-1 = 0xFFFFFFFF is correct.
        const uint64_t mask = (bit_width == 32) ? 0xFFFFFFFFULL : ((1ULL << bit_width) - 1ULL);
        const int bytes = (8 * bit_width + 7) / 8;
        for (int i = 0; i < 8; i++) {
            const int bit_pos  = i * bit_width;
            const int byte_pos = bit_pos / 8;
            const int bit_off  = bit_pos % 8;
            uint64_t val = 0;
            for (int b = 0; b < 5 && byte_pos + b < bytes; b++)
                val |= ((uint64_t)src[byte_pos + b]) << (b * 8);
            dst[i] = (int32_t)((val >> bit_off) & mask);
        }
    }
}

// FAST variant for bit_width 9-32: one unaligned 64-bit load per value instead
// of assembling it byte-by-byte in an inner loop. A value spans at most
// bit_off(≤7) + bit_width(≤32) = 39 bits, so a single 8-byte load starting at
// byte_pos always covers it.
//
// This deliberately reads up to 8 bytes from byte_pos, which for the last
// values of a group runs past the group's own span into the FOLLOWING group.
// That is safe for every group except the last one of a run, where it would
// read past the end of the caller's buffer — callers must route the final
// group through the safe unpack_group_8_scalar above.
static inline void unpack_group_8_scalar_fast(const uint8_t* __restrict__ src,
                                              int32_t* __restrict__ dst,
                                              int bit_width)
{
    const uint64_t mask = (bit_width == 32) ? 0xFFFFFFFFULL : ((1ULL << bit_width) - 1ULL);
    for (int i = 0; i < 8; i++) {
        const int bit_pos  = i * bit_width;
        uint64_t val;
        std::memcpy(&val, src + (bit_pos >> 3), sizeof(val));
        dst[i] = (int32_t)((val >> (bit_pos & 7)) & mask);
    }
}

// ---- scalar bulk (num_groups groups) ----------------------------------------
static void unpack_bitpacked_groups_scalar(const uint8_t* src, int32_t* dst,
                                           int num_groups, int bit_width)
{
    const int bpg = (bit_width <= 8) ? bit_width : (8 * bit_width + 7) / 8;
    int g = 0;
    if (bit_width > 8) {
        // All but the final group can use the overreading fast path; the final
        // group must not read past the buffer.
        for (; g + 1 < num_groups; g++)
            unpack_group_8_scalar_fast(src + g * bpg, dst + g * 8, bit_width);
    }
    for (; g < num_groups; g++)
        unpack_group_8_scalar(src + g * bpg, dst + g * 8, bit_width);
}

// ---- NEON -------------------------------------------------------------------
#if defined(__ARM_NEON) || defined(__ARM_NEON__)

static inline void unpack_group_8_neon(const uint8_t* __restrict__ src,
                                       int32_t* __restrict__ dst,
                                       int bit_width)
{
    switch (bit_width) {
    case 1: {
        // 8 values packed in 1 byte.
        // vshl_u8 with signed-int8 shift vector: negative = right-shift.
        static const int8x8_t lsh = {0, -1, -2, -3, -4, -5, -6, -7};
        uint8x8_t v   = vdup_n_u8(src[0]);
        uint8x8_t bits = vand_u8(vshl_u8(v, lsh), vdup_n_u8(1));
        uint16x8_t v16 = vmovl_u8(bits);
        vst1q_s32(dst,     vreinterpretq_s32_u32(vmovl_u16(vget_low_u16(v16))));
        vst1q_s32(dst + 4, vreinterpretq_s32_u32(vmovl_u16(vget_high_u16(v16))));
        break;
    }
    case 2: {
        // 8 values packed in 2 bytes.
        const uint16_t w = (uint16_t)src[0] | ((uint16_t)src[1] << 8);
        static const int16x8_t lsh = {0, -2, -4, -6, -8, -10, -12, -14};
        uint16x8_t v   = vshlq_u16(vdupq_n_u16(w), lsh);
        v = vandq_u16(v, vdupq_n_u16(3));
        vst1q_s32(dst,     vreinterpretq_s32_u32(vmovl_u16(vget_low_u16(v))));
        vst1q_s32(dst + 4, vreinterpretq_s32_u32(vmovl_u16(vget_high_u16(v))));
        break;
    }
    case 4: {
        // 8 values packed in 4 bytes.
        uint32_t w; memcpy(&w, src, 4);
        static const int32x4_t sh_lo = {0, -4, -8,  -12};
        static const int32x4_t sh_hi = {-16, -20, -24, -28};
        const uint32x4_t mask4 = vdupq_n_u32(0xF);
        vst1q_s32(dst,     vreinterpretq_s32_u32(vandq_u32(vshlq_u32(vdupq_n_u32(w), sh_lo), mask4)));
        vst1q_s32(dst + 4, vreinterpretq_s32_u32(vandq_u32(vshlq_u32(vdupq_n_u32(w), sh_hi), mask4)));
        break;
    }
    default:
        // bw=3,5,6,7,8+: fall back to the scalar uint64 path.
        unpack_group_8_scalar(src, dst, bit_width);
        break;
    }
}

// bit_width 9-16, one group of 8. Each value spans at most 7+16 = 23 bits, so a
// 32-bit lane per value is enough: gather eight overlapping 32-bit words at the
// values' byte offsets, then shift/mask in one vector op. Overreads past the
// group like unpack_group_8_scalar_fast — final group must use the safe path.
static inline void unpack_group_8_neon_wide(const uint8_t* __restrict__ src,
                                            int32_t* __restrict__ dst,
                                            int bit_width)
{
    uint32_t w[8];
    int32_t  sh[8];
    for (int i = 0; i < 8; i++) {
        const int bit_pos = i * bit_width;
        std::memcpy(&w[i], src + (bit_pos >> 3), sizeof(uint32_t));
        sh[i] = -(bit_pos & 7);  // negative shift = right shift for vshlq_u32
    }
    const uint32x4_t mask = vdupq_n_u32((bit_width == 32)
        ? 0xFFFFFFFFu : ((1u << bit_width) - 1u));
    vst1q_s32(dst, vreinterpretq_s32_u32(vandq_u32(
        vshlq_u32(vld1q_u32(w), vld1q_s32(sh)), mask)));
    vst1q_s32(dst + 4, vreinterpretq_s32_u32(vandq_u32(
        vshlq_u32(vld1q_u32(w + 4), vld1q_s32(sh + 4)), mask)));
}

static void unpack_bitpacked_groups_neon(const uint8_t* src, int32_t* dst,
                                         int num_groups, int bit_width)
{
    const int bpg = (bit_width <= 8) ? bit_width : (8 * bit_width + 7) / 8;
    int g = 0;
    if (bit_width >= 9 && bit_width <= 16) {
        for (; g + 1 < num_groups; g++)
            unpack_group_8_neon_wide(src + g * bpg, dst + g * 8, bit_width);
    } else if (bit_width > 16) {
        for (; g + 1 < num_groups; g++)
            unpack_group_8_scalar_fast(src + g * bpg, dst + g * 8, bit_width);
    }
    for (; g < num_groups; g++)
        unpack_group_8_neon(src + g * bpg, dst + g * 8, bit_width);
}
#endif  // __ARM_NEON

// ---- AVX2 -------------------------------------------------------------------
#if defined(__AVX2__)

static void unpack_bitpacked_groups_avx2(const uint8_t* src, int32_t* dst,
                                          int num_groups, int bit_width)
{
    if (bit_width == 8) {
        // bw=8: load 8 bytes, zero-extend each to int32.
        for (int g = 0; g < num_groups; g++) {
            __m128i v8  = _mm_loadl_epi64((const __m128i*)(src + g * 8));
            __m256i v32 = _mm256_cvtepu8_epi32(v8);
            _mm256_storeu_si256((__m256i*)(dst + g * 8), v32);
        }
        return;
    }
    if (bit_width == 4) {
        // bw=4: 8 nibbles from 4 bytes.
        for (int g = 0; g < num_groups; g++) {
            uint32_t w; memcpy(&w, src + g * 4, 4);
            const __m256i W  = _mm256_set1_epi32((int)w);
            const __m256i sh = _mm256_set_epi32(28, 24, 20, 16, 12, 8, 4, 0);
            const __m256i mk = _mm256_set1_epi32(0xF);
            _mm256_storeu_si256((__m256i*)(dst + g * 8),
                                _mm256_and_si256(_mm256_srlv_epi32(W, sh), mk));
        }
        return;
    }
    const int bpg = (bit_width <= 8) ? bit_width : (8 * bit_width + 7) / 8;
    int g = 0;
    if (bit_width >= 9 && bit_width <= 16) {
        // Each value spans at most 7+16 = 23 bits, so one 32-bit lane per value
        // suffices: gather eight overlapping 32-bit words at the values' byte
        // offsets, then variable-shift and mask in one pass. Overreads past the
        // group, so the final group takes the safe scalar path below.
        const __m256i mk = _mm256_set1_epi32((int)((1u << bit_width) - 1u));
        for (; g + 1 < num_groups; g++) {
            const uint8_t* s = src + g * bpg;
            int32_t w[8], sh[8];
            for (int i = 0; i < 8; i++) {
                const int bit_pos = i * bit_width;
                memcpy(&w[i], s + (bit_pos >> 3), sizeof(int32_t));
                sh[i] = bit_pos & 7;
            }
            const __m256i W = _mm256_loadu_si256((const __m256i*)w);
            const __m256i S = _mm256_loadu_si256((const __m256i*)sh);
            _mm256_storeu_si256((__m256i*)(dst + g * 8),
                                _mm256_and_si256(_mm256_srlv_epi32(W, S), mk));
        }
    } else if (bit_width > 16) {
        for (; g + 1 < num_groups; g++)
            unpack_group_8_scalar_fast(src + g * bpg, dst + g * 8, bit_width);
    }
    // Remaining widths (1,2,3,5,6,7) and every final group: safe scalar.
    for (; g < num_groups; g++)
        unpack_group_8_scalar(src + g * bpg, dst + g * 8, bit_width);
}
#endif  // __AVX2__

// ---- Dispatch ---------------------------------------------------------------
using unpack_groups_fn_t = void(*)(const uint8_t*, int32_t*, int, int);

static unpack_groups_fn_t get_unpack_fn()
{
    return SIMD_STATIC_SELECT(unpack_bitpacked_groups_avx2, unpack_bitpacked_groups_neon, unpack_bitpacked_groups_scalar, unpack_bitpacked_groups_scalar);
}

// Unpack all complete groups from a bit-packed run, then handle any leftover
// values with the scalar helper.  Updates `decoded` and `ptr`.
static inline void decode_bitpacked_run(
        const uint8_t* run_start, int32_t values_in_run, int32_t bytes_needed,
        int32_t num_values, int bit_width,
        draken::AppendBuffer<int32_t>& indices, int32_t& decoded)
{
    const int32_t to_decode   = std::min(values_in_run, num_values - decoded);
    const int32_t full_groups = to_decode / 8;
    const int32_t remainder   = to_decode - full_groups * 8;
    const int     bpg         = (bit_width <= 8) ? bit_width : (8 * bit_width + 7) / 8;

    const size_t old_sz = indices.size();
    indices.resize_uninit(old_sz + to_decode);
    int32_t* dst = indices.data() + old_sz;

    if (full_groups > 0)
        get_unpack_fn()(run_start, dst, full_groups, bit_width);

    if (remainder > 0) {
        int32_t tmp[8] = {};
        unpack_group_8_scalar(run_start + full_groups * bpg, tmp, bit_width);
        for (int i = 0; i < remainder; i++)
            dst[full_groups * 8 + i] = tmp[i];
    }

    decoded += to_decode;
    (void)bytes_needed;  // consumed by caller via ptr advance
}

// ---------------------------------------------------------------------------
// SkipRLEBitPackedLevels
// ---------------------------------------------------------------------------

size_t SkipRLEBitPackedLevels(const uint8_t *data, size_t max_size,
                              int max_level) {
  if (max_level <= 0) {
    // No levels encoded when max_level == 0.
    return 0;
  }

  // Level data is prefixed with a 4-byte little-endian length.
  if (max_size < 4) return 0;

  int32_t level_byte_length = ReadLE32(data);
  if (level_byte_length < 0 ||
      level_byte_length > (int32_t)max_size - 4) {
    return 0;
  }

  return 4 + (size_t)level_byte_length;
}

// ---------------------------------------------------------------------------
// DecodeRLEBitPackedIndices
// ---------------------------------------------------------------------------

int32_t DecodeRLEBitPackedIndices(const uint8_t *data, size_t data_size,
                                  int32_t num_values, int bit_width,
                                  draken::AppendBuffer<int32_t> &indices) {
  if (bit_width <= 0 || bit_width > 32 || data_size < 4) return -1;

  indices.clear();
  indices.reserve(num_values);

  // Skip the 4-byte length prefix.
  const uint8_t *ptr = data + 4;
  const uint8_t *end = data + data_size;

  int32_t decoded = 0;
  while (decoded < num_values && ptr < end) {
    // Read varint run header.
    uint32_t header = 0;
    int shift = 0;
    while (ptr < end && shift < 32) {
      uint8_t byte = *ptr++;
      header |= ((uint32_t)(byte & 0x7F)) << shift;
      if ((byte & 0x80) == 0) break;
      shift += 7;
    }

    if ((header & 1) == 1) {
      // Bit-packed run: (header >> 1) groups of 8 values.
      int32_t num_groups    = (int32_t)(header >> 1);
      int32_t values_in_run = num_groups * 8;
      int32_t bytes_needed  = (values_in_run * bit_width + 7) / 8;

      if (ptr + bytes_needed > end) break;

      decode_bitpacked_run(ptr, values_in_run, bytes_needed, num_values,
                           bit_width, indices, decoded);
      ptr += bytes_needed;

    } else {
      // RLE run: (header >> 1) repetitions of a single value.
      int32_t count        = (int32_t)(header >> 1);
      int     bytes_needed = (bit_width + 7) / 8;
      uint32_t value       = 0;

      for (int i = 0; i < bytes_needed && ptr < end; i++)
        value |= ((uint32_t)(*ptr++)) << (i * 8);
      value &= (1U << bit_width) - 1;

      // Bulk fill: resize+fill_n avoids per-element push_back overhead.
      const int32_t to_fill = std::min(count, num_values - decoded);
      const size_t  old_sz  = indices.size();
      indices.resize_fill(old_sz + to_fill, (int32_t)value);
      decoded += to_fill;
    }
  }

  return (decoded == num_values) ? decoded : -1;
}

// ---------------------------------------------------------------------------
// DecodeRLEBitPackedIndicesWithConsumption (variant with byte consumption tracking)
// ---------------------------------------------------------------------------

int32_t DecodeRLEBitPackedIndicesWithConsumption(const uint8_t *data, size_t data_size,
                                                 int32_t num_values, int bit_width,
                                                 draken::AppendBuffer<int32_t> &indices,
                                                 size_t &bytes_consumed) {
  if (bit_width <= 0 || bit_width > 32 || data_size < 4) return -1;

  indices.clear();
  indices.reserve(num_values);

  // Skip the 4-byte length prefix.
  const uint8_t *ptr = data + 4;
  const uint8_t *end = data + data_size;
  const uint8_t *start = data;

  int32_t decoded = 0;
  while (decoded < num_values && ptr < end) {
    // Read varint run header.
    uint32_t header = 0;
    int shift = 0;
    while (ptr < end && shift < 32) {
      uint8_t byte = *ptr++;
      header |= ((uint32_t)(byte & 0x7F)) << shift;
      if ((byte & 0x80) == 0) break;
      shift += 7;
    }

    if ((header & 1) == 1) {
      // Bit-packed run: (header >> 1) groups of 8 values.
      int32_t num_groups    = (int32_t)(header >> 1);
      int32_t values_in_run = num_groups * 8;
      int32_t bytes_needed  = (values_in_run * bit_width + 7) / 8;

      if (ptr + bytes_needed > end) break;

      decode_bitpacked_run(ptr, values_in_run, bytes_needed, num_values,
                           bit_width, indices, decoded);
      ptr += bytes_needed;

    } else {
      // RLE run: (header >> 1) repetitions of a single value.
      int32_t count        = (int32_t)(header >> 1);
      int     bytes_needed = (bit_width + 7) / 8;
      uint32_t value       = 0;

      for (int i = 0; i < bytes_needed && ptr < end; i++)
        value |= ((uint32_t)(*ptr++)) << (i * 8);
      value &= (1U << bit_width) - 1;

      const int32_t to_fill = std::min(count, num_values - decoded);
      const size_t  old_sz  = indices.size();
      indices.resize_fill(old_sz + to_fill, (int32_t)value);
      decoded += to_fill;
    }
  }

  // Track bytes consumed.
  bytes_consumed = (size_t)(ptr - start);

  return (decoded == num_values) ? decoded : -1;
}

// ---------------------------------------------------------------------------
// DecodeRLEBitPackedIndicesNoPrefix
// Same as DecodeRLEBitPackedIndices but data points directly at the RLE
// stream (no 4-byte length prefix).  The bit_width byte has already been
// consumed by the caller.
// ---------------------------------------------------------------------------

int32_t DecodeRLEBitPackedIndicesNoPrefix(const uint8_t *data, size_t data_size,
                                          int32_t num_values, int bit_width,
                                          draken::AppendBuffer<int32_t> &indices) {
  if (bit_width > 32) return -1;
  if (bit_width == 0) {
    // Single-entry dictionary: every present value is index 0, no bits on wire.
    indices.clear();
    indices.resize_fill((size_t)num_values, 0);
    return num_values;
  }

  indices.clear();
  indices.reserve(num_values);

  const uint8_t *ptr = data;
  const uint8_t *end = data + data_size;

  int32_t decoded = 0;
  while (decoded < num_values && ptr < end) {
    uint32_t header = 0;
    int shift = 0;
    while (ptr < end && shift < 32) {
      uint8_t byte = *ptr++;
      header |= ((uint32_t)(byte & 0x7F)) << shift;
      if ((byte & 0x80) == 0) break;
      shift += 7;
    }

    if ((header & 1) == 1) {
      int32_t num_groups    = (int32_t)(header >> 1);
      int32_t values_in_run = num_groups * 8;
      int32_t bytes_needed  = (values_in_run * bit_width + 7) / 8;
      if (ptr + bytes_needed > end) break;

      decode_bitpacked_run(ptr, values_in_run, bytes_needed, num_values,
                           bit_width, indices, decoded);
      ptr += bytes_needed;
    } else {
      int32_t count        = (int32_t)(header >> 1);
      int     bytes_needed = (bit_width + 7) / 8;
      uint32_t value       = 0;
      for (int i = 0; i < bytes_needed && ptr < end; i++)
        value |= ((uint32_t)(*ptr++)) << (i * 8);
      value &= (1U << bit_width) - 1;

      const int32_t to_fill = std::min(count, num_values - decoded);
      const size_t  old_sz  = indices.size();
      indices.resize_fill(old_sz + to_fill, (int32_t)value);
      decoded += to_fill;
    }
  }

  return (decoded == num_values) ? decoded : -1;
}

// ---------------------------------------------------------------------------
// DecodeRLEBitPackedCodesPacked
// The hybrid stream decoded straight into the packed per-row code array, in
// one pass (docs/PARQUET_PACKED_CODES_ONE_PASS_DESIGN.md §3a). Bit-packed
// groups unpack through the same per-arch group unpackers as the int32 decoder
// into an L1-resident tile, then narrow-store at the code width while keeping a
// running max; RLE runs are a fill at the code width. The width is a template
// parameter, so the store and the max reduction carry no per-value switch.
// ---------------------------------------------------------------------------

template <typename CodeT>
static int32_t decode_codes_packed(const uint8_t *data, size_t data_size,
                                   int32_t num_values, int bit_width,
                                   CodeT *__restrict__ dst, uint32_t *max_code) {
  // 64 groups = 512 values = 2 KiB of int32: stays in L1 between unpack and store.
  constexpr int32_t kTileGroups = 64;
  alignas(64) int32_t tile[kTileGroups * 8];
  const unpack_groups_fn_t unpack = get_unpack_fn();
  const int bpg = (bit_width <= 8) ? bit_width : (8 * bit_width + 7) / 8;
  const uint32_t value_mask = (bit_width == 32) ? 0xFFFFFFFFu : ((1u << bit_width) - 1u);

  const uint8_t *ptr = data;
  const uint8_t *end = data + data_size;
  int32_t decoded = 0;
  uint32_t mx = 0;

  while (decoded < num_values && ptr < end) {
    uint32_t header = 0;
    int shift = 0;
    while (ptr < end && shift < 32) {
      uint8_t byte = *ptr++;
      header |= ((uint32_t)(byte & 0x7F)) << shift;
      if ((byte & 0x80) == 0) break;
      shift += 7;
    }

    if ((header & 1) == 1) {
      const int32_t num_groups    = (int32_t)(header >> 1);
      const int32_t values_in_run = num_groups * 8;
      const int32_t bytes_needed  = (values_in_run * bit_width + 7) / 8;
      if (ptr + bytes_needed > end) break;

      const int32_t to_decode   = std::min(values_in_run, num_values - decoded);
      const int32_t full_groups = to_decode / 8;
      const int32_t remainder   = to_decode - full_groups * 8;
      CodeT *out = dst + decoded;
      for (int32_t g = 0; g < full_groups; g += kTileGroups) {
        const int32_t n_groups = std::min(kTileGroups, full_groups - g);
        unpack(ptr + (size_t)g * bpg, tile, n_groups, bit_width);
        const int32_t n = n_groups * 8;
        uint32_t tile_max = 0;
        for (int32_t i = 0; i < n; i++) {
          const uint32_t v = (uint32_t)tile[i];
          tile_max = v > tile_max ? v : tile_max;
          out[i] = (CodeT)v;
        }
        mx = tile_max > mx ? tile_max : mx;
        out += n;
      }
      if (remainder > 0) {
        int32_t tmp[8] = {};
        unpack_group_8_scalar(ptr + (size_t)full_groups * bpg, tmp, bit_width);
        for (int32_t i = 0; i < remainder; i++) {
          const uint32_t v = (uint32_t)tmp[i];
          mx = v > mx ? v : mx;
          out[i] = (CodeT)v;
        }
      }
      decoded += to_decode;
      ptr += bytes_needed;
    } else {
      const int32_t count        = (int32_t)(header >> 1);
      const int     value_bytes  = (bit_width + 7) / 8;
      uint32_t value = 0;
      for (int i = 0; i < value_bytes && ptr < end; i++)
        value |= ((uint32_t)(*ptr++)) << (i * 8);
      value &= value_mask;

      const int32_t to_fill = std::min(count, num_values - decoded);
      if (to_fill > 0) {
        std::fill_n(dst + decoded, to_fill, (CodeT)value);
        mx = value > mx ? value : mx;
        decoded += to_fill;
      }
    }
  }

  *max_code = mx;
  return (decoded == num_values) ? decoded : -1;
}

int32_t DecodeRLEBitPackedCodesPacked(const uint8_t *data, size_t data_size,
                                      int32_t num_values, int bit_width,
                                      uint8_t *dst, uint8_t code_width,
                                      uint32_t *max_code) {
  *max_code = 0;
  if (bit_width < 0 || bit_width > 32 || num_values < 0) return -1;
  if (code_width != 1 && code_width != 2 && code_width != 4) return -1;
  if (bit_width == 0) {
    // Single-entry dictionary: every value is code 0, no bits on the wire.
    std::memset(dst, 0, (size_t)num_values * code_width);
    return num_values;
  }
  switch (code_width) {
    case 1:
      return decode_codes_packed<uint8_t>(data, data_size, num_values, bit_width,
                                          dst, max_code);
    case 2:
      return decode_codes_packed<uint16_t>(data, data_size, num_values, bit_width,
                                           reinterpret_cast<uint16_t *>(dst), max_code);
    case 4:
      return decode_codes_packed<uint32_t>(data, data_size, num_values, bit_width,
                                           reinterpret_cast<uint32_t *>(dst), max_code);
  }
  return -1;
}

// ---------------------------------------------------------------------------
// DecodeRLEBitPackedVerdicts
// The per-value verdict of a per-dictionary-entry pass table, read straight off
// the hybrid stream (docs/PARQUET_RUN_DOMAIN_PREDICATE_DESIGN.md §11). The
// stream is walked once: an RLE run becomes one table lookup and a memset of
// its length, so a run-heavy page (ClickBench AdvEngineID: ~0.4 bits per value
// on the wire) is decided in a handful of operations; a bit-packed run is
// unpacked through the same per-arch group unpackers as the index decoders,
// into an L1-resident tile, and each code is looked up. The bounds check on
// codes is one compare per tile (its max) plus one per RLE run.
// ---------------------------------------------------------------------------

int32_t DecodeRLEBitPackedVerdicts(const uint8_t *data, size_t data_size,
                                   int32_t num_values, int bit_width,
                                   const uint8_t *entry_pass, uint32_t num_entries,
                                   uint8_t *vpass, uint32_t *bad_code,
                                   long long *runs_filled, long long *codes_unpacked) {
  *bad_code = 0;
  if (bit_width < 0 || bit_width > 32 || num_values < 0) return -1;
  if (num_values == 0) return 0;
  if (bit_width == 0) {
    // Single-entry dictionary: every value is code 0, no bits on the wire.
    if (num_entries == 0) { *bad_code = 0; return -2; }
    std::memset(vpass, entry_pass[0], (size_t)num_values);
    *runs_filled += num_values;
    return num_values;
  }
  constexpr int32_t kTileGroups = 64;
  alignas(64) int32_t tile[kTileGroups * 8];
  const unpack_groups_fn_t unpack = get_unpack_fn();
  const int bpg = (bit_width <= 8) ? bit_width : (8 * bit_width + 7) / 8;
  const uint32_t value_mask = (bit_width == 32) ? 0xFFFFFFFFu : ((1u << bit_width) - 1u);

  const uint8_t *ptr = data;
  const uint8_t *end = data + data_size;
  int32_t decoded = 0;
  long long runs = 0, unpacked = 0;

  while (decoded < num_values && ptr < end) {
    uint32_t header = 0;
    int shift = 0;
    while (ptr < end && shift < 32) {
      uint8_t byte = *ptr++;
      header |= ((uint32_t)(byte & 0x7F)) << shift;
      if ((byte & 0x80) == 0) break;
      shift += 7;
    }

    if ((header & 1) == 1) {
      const int32_t num_groups    = (int32_t)(header >> 1);
      const int32_t values_in_run = num_groups * 8;
      const int32_t bytes_needed  = (values_in_run * bit_width + 7) / 8;
      if (ptr + bytes_needed > end) break;

      const int32_t to_decode   = std::min(values_in_run, num_values - decoded);
      const int32_t full_groups = to_decode / 8;
      const int32_t remainder   = to_decode - full_groups * 8;
      uint8_t *out = vpass + decoded;
      for (int32_t g = 0; g < full_groups; g += kTileGroups) {
        const int32_t n_groups = std::min(kTileGroups, full_groups - g);
        unpack(ptr + (size_t)g * bpg, tile, n_groups, bit_width);
        const int32_t n = n_groups * 8;
        uint32_t tile_max = 0;
        for (int32_t i = 0; i < n; i++) {
          const uint32_t v = (uint32_t)tile[i];
          tile_max = v > tile_max ? v : tile_max;
        }
        if (tile_max >= num_entries) { *bad_code = tile_max; return -2; }
        for (int32_t i = 0; i < n; i++) out[i] = entry_pass[(uint32_t)tile[i]];
        out += n;
      }
      if (remainder > 0) {
        int32_t tmp[8] = {};
        unpack_group_8_scalar(ptr + (size_t)full_groups * bpg, tmp, bit_width);
        for (int32_t i = 0; i < remainder; i++) {
          const uint32_t v = (uint32_t)tmp[i];
          if (v >= num_entries) { *bad_code = v; return -2; }
          out[i] = entry_pass[v];
        }
      }
      unpacked += to_decode;
      decoded += to_decode;
      ptr += bytes_needed;
    } else {
      const int32_t count       = (int32_t)(header >> 1);
      const int     value_bytes = (bit_width + 7) / 8;
      if (ptr + value_bytes > end) break;
      uint32_t value = 0;
      for (int i = 0; i < value_bytes; i++) value |= ((uint32_t)(*ptr++)) << (i * 8);
      value &= value_mask;
      // A zero-length run consumes nothing: malformed, never absorbed.
      if (count <= 0) return -1;
      if (value >= num_entries) { *bad_code = value; return -2; }

      const int32_t to_fill = std::min(count, num_values - decoded);
      std::memset(vpass + decoded, entry_pass[value], (size_t)to_fill);
      runs += to_fill;
      decoded += to_fill;
    }
  }

  *runs_filled += runs;
  *codes_unpacked += unpacked;
  return (decoded == num_values) ? decoded : -1;
}

// ---------------------------------------------------------------------------
// DecodeRLEBitPackedIndicesSelected
// ---------------------------------------------------------------------------

// The selective decode is specialised PER ARCH (measured 2026-10-16): on ARM
// (NEON) the masked count tail, per-group NEON unpack and branchless compress
// below won (adv_wide wall 0.939); on x86 the same code regressed guard shapes
// (counter62 wall 1.010, cb_q12 CPU 1.007), so x86 keeps the plain scalar
// path. Re-measure on x86 before changing either side.
#if defined(__ARM_NEON) || defined(__ARM_NEON__)
#  define RUGO_SELECTED_DECODE_NEON 1
#endif

#ifdef RUGO_SELECTED_DECODE_NEON
// Number of set 0/1 bytes in sel[0..n). `avail` (>= n) is how many bytes from
// sel are readable: when 8 are, the 1-7 byte tail is one masked word load
// instead of a byte loop whose variable trip count mispredicts on the short
// RLE runs this is called per.
static inline int32_t count_selected(const uint8_t *sel, int32_t n, int32_t avail) {
  int32_t c = 0;
  int32_t i = 0;
  for (; i + 8 <= n; i += 8) {
    uint64_t w;
    std::memcpy(&w, sel + i, 8);
    c += __builtin_popcountll(w);
  }
  const int32_t rem = n - i;
  if (rem == 0) return c;
  if (i + 8 <= avail) {
    uint64_t w;
    std::memcpy(&w, sel + i, 8);
    return c + __builtin_popcountll(w & (~0ULL >> (64 - 8 * rem)));
  }
  for (; i < n; ++i) c += sel[i];
  return c;
}
#else
// Number of set 0/1 bytes in sel[0..n).
static inline int32_t count_selected(const uint8_t *sel, int32_t n) {
  int32_t c = 0;
  int32_t i = 0;
  for (; i + 8 <= n; i += 8) {
    uint64_t w;
    std::memcpy(&w, sel + i, 8);
    c += __builtin_popcountll(w);
  }
  for (; i < n; ++i) c += sel[i];
  return c;
}
#endif

#ifdef RUGO_SELECTED_DECODE_NEON
// Selected-lane compress of one unpacked group of 8: every lane is written at
// the cursor and the cursor advances by that lane's 0/1 selection byte, so
// there is no per-value branch. `dst` needs 8 slots of headroom past `n`.
static inline size_t compress_group_8(const int32_t *__restrict__ vals, uint64_t sel8,
                                      int32_t *__restrict__ dst, size_t n) {
  for (int i = 0; i < 8; ++i) {
    dst[n] = vals[i];
    n += (size_t)((sel8 >> (8 * i)) & 1u);
  }
  return n;
}

// `groups` whole groups of 8 that are NOT the last group of their run, so the
// over-reading group unpackers are safe. Groups with no selected value are
// never unpacked.
template <void (*Unpack)(const uint8_t *, int32_t *, int)>
static inline size_t select_groups_8(const uint8_t *src, int bpg, const uint8_t *sel,
                                     int32_t groups, int bit_width,
                                     int32_t *dst, size_t n) {
  for (int32_t g = 0; g < groups; ++g) {
    uint64_t w;
    std::memcpy(&w, sel + g * 8, 8);
    if (w == 0) continue;
    int32_t tmp[8];
    Unpack(src + g * bpg, tmp, bit_width);
    n = compress_group_8(tmp, w, dst, n);
  }
  return n;
}
#endif  // RUGO_SELECTED_DECODE_NEON

int32_t DecodeRLEBitPackedIndicesSelected(const uint8_t *data, size_t data_size,
                                          int32_t num_values, int bit_width,
                                          const uint8_t *sel,
                                          draken::AppendBuffer<int32_t> &out) {
  if (bit_width > 32) return -1;
  out.clear();
  if (bit_width == 0) {
    // Single-entry dictionary: every value is index 0, no bits on wire.
#ifdef RUGO_SELECTED_DECODE_NEON
    out.resize_fill((size_t)count_selected(sel, num_values, num_values), 0);
#else
    out.resize_fill((size_t)count_selected(sel, num_values), 0);
#endif
    return num_values;
  }
#ifdef RUGO_SELECTED_DECODE_NEON
  // +8: compress_group_8 writes all 8 lanes at the cursor.
  out.reserve((size_t)num_values + 8);
#else
  out.reserve((size_t)num_values);
#endif

  const uint8_t *ptr = data;
  const uint8_t *end = data + data_size;
  const int bpg = (bit_width <= 8) ? bit_width : (8 * bit_width + 7) / 8;

  int32_t walked = 0;
  while (walked < num_values && ptr < end) {
    uint32_t header = 0;
    int shift = 0;
    while (ptr < end && shift < 32) {
      uint8_t byte = *ptr++;
      header |= ((uint32_t)(byte & 0x7F)) << shift;
      if ((byte & 0x80) == 0) break;
      shift += 7;
    }

    if ((header & 1) == 1) {
      const int32_t num_groups    = (int32_t)(header >> 1);
      const int32_t values_in_run = num_groups * 8;
      const int32_t bytes_needed  = (values_in_run * bit_width + 7) / 8;
      if (ptr + bytes_needed > end) break;

      const int32_t to_walk = std::min(values_in_run, num_values - walked);
#ifdef RUGO_SELECTED_DECODE_NEON
      // Whole groups that are not the run's last take the over-reading
      // unpackers; the run's last group and a page-end partial group take the
      // safe scalar one below. Capacity was reserved up front, so no growth.
      const int32_t fast_groups = std::max(0, std::min(to_walk / 8, num_groups - 1));
      int32_t *const dst = out.data();
      size_t n = out.size();
      if (bit_width <= 8)
        n = select_groups_8<unpack_group_8_neon>(ptr, bpg, sel + walked, fast_groups, bit_width, dst, n);
      else if (bit_width <= 16)
        n = select_groups_8<unpack_group_8_neon_wide>(ptr, bpg, sel + walked, fast_groups, bit_width, dst, n);
      else
        n = select_groups_8<unpack_group_8_scalar_fast>(ptr, bpg, sel + walked, fast_groups, bit_width, dst, n);
      for (int32_t g = fast_groups; g * 8 < to_walk; ++g) {
        const int32_t in_group = std::min(8, to_walk - g * 8);
        uint64_t w = 0;  // lanes past in_group stay 0: written, never counted
        std::memcpy(&w, sel + walked + g * 8, (size_t)in_group);
        if (w == 0) continue;  // nothing selected: never unpacked
        int32_t tmp[8];
        unpack_group_8_scalar(ptr + g * bpg, tmp, bit_width);
        n = compress_group_8(tmp, w, dst, n);
      }
      out.resize_uninit(n);
#else
      for (int32_t g = 0; g * 8 < to_walk; ++g) {
        const int32_t base = walked + g * 8;
        const int32_t in_group = std::min(8, to_walk - g * 8);
        uint64_t w = 0;
        std::memcpy(&w, sel + base, (size_t)in_group);
        if (w == 0) continue;  // nothing selected: never unpacked
        int32_t tmp[8];
        unpack_group_8_scalar(ptr + g * bpg, tmp, bit_width);
        for (int32_t i = 0; i < in_group; ++i)
          if (sel[base + i]) out.push_back(tmp[i]);
      }
#endif
      ptr += bytes_needed;
      walked += to_walk;
    } else {
      const int32_t count        = (int32_t)(header >> 1);
      const int     bytes_needed = (bit_width + 7) / 8;
      uint32_t value = 0;
      for (int i = 0; i < bytes_needed && ptr < end; i++)
        value |= ((uint32_t)(*ptr++)) << (i * 8);
      if (bit_width < 32) value &= (1U << bit_width) - 1;
      if (count <= 0) return -1;
      const int32_t to_fill = std::min(count, num_values - walked);
#ifdef RUGO_SELECTED_DECODE_NEON
      const int32_t n_sel = count_selected(sel + walked, to_fill, num_values - walked);
#else
      const int32_t n_sel = count_selected(sel + walked, to_fill);
#endif
      out.resize_fill(out.size() + (size_t)n_sel, (int32_t)value);
      walked += to_fill;
    }
  }

  return (walked == num_values) ? walked : -1;
}

// ---------------------------------------------------------------------------
// LevelStreamIsSingleRunOf
// ---------------------------------------------------------------------------
// Decides the all-present question from the RLE header alone.  The hybrid
// encoding puts a varint run header first: bit 0 selects bit-packed (1) or RLE
// (0), and the remaining bits carry the count.  An RLE run is then followed by
// the repeated value in round-up-to-byte(bit_width) bytes, little-endian.  So a
// stream whose FIRST run is RLE, repeats `expect_value`, and is at least as
// long as the page is all-`expect_value` for every level the page consumes —
// whatever follows it is never read.
//
// Every failure mode returns false, which costs only the normal full decode.
bool LevelStreamIsSingleRunOf(const uint8_t *data, size_t data_size,
                              int32_t num_values, int bit_width,
                              int32_t expect_value) {
  if (bit_width <= 0 || bit_width > 32) return false;
  if (num_values <= 0) return false;

  const uint8_t *ptr = data;
  const uint8_t *end = data + data_size;

  // Varint run header. A header that runs off the end of the stream, or that
  // never terminates within 32 bits, is not something we can prove.
  uint32_t header    = 0;
  int      shift     = 0;
  bool     terminated = false;
  while (ptr < end && shift < 32) {
    const uint8_t byte = *ptr++;
    header |= ((uint32_t)(byte & 0x7F)) << shift;
    if ((byte & 0x80) == 0) { terminated = true; break; }
    shift += 7;
  }
  if (!terminated) return false;

  if ((header & 1) != 0) return false;            // bit-packed run: not provable
  const int32_t count = (int32_t)(header >> 1);   // header is uint32 ⇒ fits int32
  if (count < num_values) return false;           // run stops short of the page

  const int bytes_needed = (bit_width + 7) / 8;
  if ((size_t)(end - ptr) < (size_t)bytes_needed) return false;

  uint32_t value = 0;
  for (int i = 0; i < bytes_needed; i++)
    value |= ((uint32_t)ptr[i]) << (i * 8);
  // bit_width == 32 would make (1U << 32) undefined; mask only when it narrows.
  if (bit_width < 32) value &= (1U << bit_width) - 1;

  return (int32_t)value == expect_value;
}

// ---------------------------------------------------------------------------
// DecodeRLEBitPackedIndicesToRuns
// ---------------------------------------------------------------------------
// Skip-dense variant: emits run-level SoA arrays (code, count) with no dense
// intermediate.  RLE segments map directly to one (code, count) entry.  Bit-
// packed segments decode into a small per-segment scratch buffer and merge
// consecutive equal codes into runs.  The caller resolves dict codes to actual
// values (one lookup per run, not per row), then accumulates into the column's
// type-specific RLE output vectors.
//
// The bit-packed merge loop is BRANCHLESS.  Real columns routinely produce
// close to one run per value (measured on tpch_100 lineitem: l_shipdate 0.994,
// l_linenumber 0.964, l_shipmode 0.857 runs/value), which makes the "did this
// value continue the previous run" test a coin flip.  A data-dependent branch
// there costs a mispredict per value and dominates the whole stage — it is flat
// across bit width, so it is not the unpacking.  Writing the run cell
// unconditionally and advancing the cursor by (v != prev) removes the branch:
// measured 1.76 -> 0.83 ns/value on l_shipdate, 2.00 -> 1.52 on l_shipmode, and
// neutral (0.78 -> 0.80) on the genuinely run-heavy l_linestatus (0.137).
//
// The cost of that is pre-sizing both arrays to num_values (8 bytes per value,
// transient for the page, trimmed by the resize-down below) so the cursor can
// store without a capacity check.  The arrays are sized uninitialised: every
// cell is written before it is read.
// ---------------------------------------------------------------------------

int32_t DecodeRLEBitPackedIndicesToRuns(const uint8_t *data, size_t data_size,
                                        int32_t num_values, int bit_width,
                                        draken::AppendBuffer<int32_t> &run_codes,
                                        draken::AppendBuffer<int32_t> &run_counts) {
  if (bit_width > 32) return -1;
  if (bit_width == 0) {
    // Single-entry dictionary: every present value is index 0, no bits on wire.
    run_codes.clear();
    run_codes.push_back(0);
    run_counts.clear();
    run_counts.push_back(num_values);
    return num_values;
  }

  // Worst case is one run per value; size for it once so the merge loop can
  // store unconditionally.  Trimmed to the real run count before returning.
  // Uninitialised: every cell is written (cursor advance) before it is read,
  // and the tail is trimmed below.
  run_codes.clear();
  run_codes.resize_uninit((size_t)num_values);
  run_counts.clear();
  run_counts.resize_uninit((size_t)num_values);
  int32_t *__restrict__ out_codes  = run_codes.data();
  int32_t *__restrict__ out_counts = run_counts.data();

  // Cursor into the run arrays.  -1 means "no run open yet"; `prev` is only
  // read when cursor >= 0, so its initial value never decides anything.
  int64_t cursor  = -1;
  int32_t prev    = 0;

  const uint8_t *ptr = data;
  const uint8_t *end = data + data_size;

  // Reused across every bit-packed segment in this call — resize() only grows
  // the underlying allocation on the first (largest-so-far) segment; later
  // segments that are smaller or equal reuse the existing buffer with no
  // realloc, avoiding a fresh heap allocation per segment.
  draken::AppendBuffer<int32_t> scratch;

  int32_t decoded = 0;
  while (decoded < num_values && ptr < end) {
    uint32_t header = 0;
    int shift = 0;
    while (ptr < end && shift < 32) {
      uint8_t byte = *ptr++;
      header |= ((uint32_t)(byte & 0x7F)) << shift;
      if ((byte & 0x80) == 0) break;
      shift += 7;
    }

    if ((header & 1) == 1) {
      // Bit-packed run: decode into a per-segment scratch buffer, then fold
      // consecutive equal codes into runs.
      int32_t num_groups    = (int32_t)(header >> 1);
      int32_t values_in_run = num_groups * 8;
      int32_t bytes_needed  = (values_in_run * bit_width + 7) / 8;
      if (ptr + bytes_needed > end) break;

      const int32_t to_decode = std::min(values_in_run, num_values - decoded);
      // Decode into the shared scratch buffer (O(segment_size), not O(column_size)).
      scratch.resize_uninit(to_decode);
      {
        const int32_t full_groups = to_decode / 8;
        const int32_t remainder   = to_decode - full_groups * 8;
        const int     bpg         = (bit_width <= 8) ? bit_width
                                                      : (8 * bit_width + 7) / 8;
        if (full_groups > 0)
          get_unpack_fn()(ptr, scratch.data(), full_groups, bit_width);
        if (remainder > 0) {
          int32_t tmp[8] = {};
          unpack_group_8_scalar(ptr + full_groups * bpg, tmp, bit_width);
          for (int i = 0; i < remainder; i++)
            scratch[full_groups * 8 + i] = tmp[i];
        }
      }

      // Merge scratch values into run_codes/run_counts.  Branchless: advance
      // the cursor only when the value differs from the open run, then store
      // both cells unconditionally.  Identical output to the equivalent
      // if/else, without the per-value mispredict.
      {
        const int32_t *__restrict__ src = scratch.data();
        for (int32_t i = 0; i < to_decode; i++) {
          const int32_t v     = src[i];
          const int64_t opens = (int64_t)((v != prev) | (cursor < 0));
          cursor += opens;
          out_codes[cursor]  = v;
          out_counts[cursor] = opens ? 1 : out_counts[cursor] + 1;
          prev = v;
        }
      }

      ptr += bytes_needed;
      decoded += to_decode;

    } else {
      // RLE run: single code repeated count times → one entry.
      int32_t count        = (int32_t)(header >> 1);
      int     bytes_needed = (bit_width + 7) / 8;
      uint32_t value       = 0;
      for (int i = 0; i < bytes_needed && ptr < end; i++)
        value |= ((uint32_t)(*ptr++)) << (i * 8);
      value &= (1U << bit_width) - 1;

      // A zero-length RLE run consumes no values but would still open a run,
      // so a stream of them could advance the cursor past the pre-sized arrays.
      // It is malformed either way — reject it rather than absorb it.
      if (count <= 0) return -1;

      const int32_t to_fill = std::min(count, num_values - decoded);
      const int32_t code    = (int32_t)value;

      // Merge with previous run if same code (handles page-internal adjacency).
      // One test per RLE segment, not per value — left as a branch.
      if (cursor >= 0 && code == prev) {
        out_counts[cursor] += to_fill;
      } else {
        ++cursor;
        out_codes[cursor]  = code;
        out_counts[cursor] = to_fill;
        prev = code;
      }
      decoded += to_fill;
    }
  }

  // Trim to the runs actually emitted.  Done on the failure path too, so a
  // short/corrupt stream never leaves the caller looking at zero-filled tail
  // cells that would read as valid (code 0, count 0) runs.
  run_codes.resize_uninit((size_t)(cursor + 1));    // shrink only
  run_counts.resize_uninit((size_t)(cursor + 1));

  return (decoded == num_values) ? decoded : -1;
}

// ---------------------------------------------------------------------------
// SkipDeltaBinaryPacked  (internal helper -- not exposed in the header)
// ---------------------------------------------------------------------------

static const uint8_t *SkipDeltaBinaryPacked(const uint8_t *ptr,
                                            const uint8_t *end,
                                            int32_t num_values) {
  uint64_t block_size    = ReadUnsignedVarint(ptr, end);
  if (block_size == 0 || ptr >= end) return ptr;

  uint64_t num_miniblocks = ReadUnsignedVarint(ptr, end);
  if (num_miniblocks == 0 || ptr >= end) return ptr;

  ReadUnsignedVarint(ptr, end);  // total_value_count
  if (ptr >= end) return ptr;

  ReadZigZagVarint(ptr, end);   // first_value
  if (ptr > end) return ptr;

  uint32_t values_per_miniblock =
      static_cast<uint32_t>(block_size / num_miniblocks);
  int32_t skipped = 1;

  while (skipped < num_values && ptr < end) {
    ReadZigZagVarint(ptr, end);  // min_delta
    if (ptr > end) break;

    // Every block carries a bit-width byte for ALL num_miniblocks miniblocks,
    // even in the final (short) block where trailing miniblocks hold no values
    // (Parquet DELTA_BINARY_PACKED spec). Consume the full bit-width list up
    // front — exactly as DecodeDeltaBinaryPacked does — so ptr lands on the
    // true end of this stream; consuming only the used miniblocks' widths would
    // leave the following stream (e.g. DELTA_BYTE_ARRAY suffix lengths)
    // misaligned and its decode would fail.
    if (ptr + num_miniblocks > end) break;
    const uint8_t *bit_widths = ptr;
    ptr += num_miniblocks;

    // Only miniblocks that actually hold values have data bytes; a fully-unused
    // trailing miniblock in the last block writes no body (its width byte is
    // present but its body is omitted). Skip bodies until num_values is reached.
    for (uint64_t mb = 0; mb < num_miniblocks && skipped < num_values; mb++) {
      uint8_t bit_width = bit_widths[mb];
      if (bit_width > 0) {
        int32_t bytes_needed =
            ((int32_t)values_per_miniblock * bit_width + 7) / 8;
        if (ptr + bytes_needed > end) break;
        ptr += bytes_needed;
      }
      skipped += (int32_t)values_per_miniblock;
    }
  }

  return ptr;
}

// ---------------------------------------------------------------------------
// DecodeDeltaByteArray
// ---------------------------------------------------------------------------

int32_t DecodeDeltaByteArray(const uint8_t *data, size_t data_size,
                             int32_t num_values,
                             std::vector<std::string> &values) {
  if (data_size < 4) return -1;

  const uint8_t *ptr = data;
  const uint8_t *end = data + data_size;

  values.clear();
  values.reserve(num_values);

  // 1. Decode prefix lengths.
  std::vector<int32_t> prefix_lengths;
  int32_t prefix_decoded =
      DecodeDeltaBinaryPacked(ptr, end - ptr, num_values, prefix_lengths);
  if (prefix_decoded != num_values) return -1;

  // 2. Skip over the prefix-length stream to reach the suffix-length stream.
  ptr = SkipDeltaBinaryPacked(data, end, num_values);
  if (ptr >= end) return -1;

  // 3. Decode suffix lengths.
  const uint8_t *suffix_start = ptr;
  std::vector<int32_t> suffix_lengths;
  int32_t suffix_decoded =
      DecodeDeltaBinaryPacked(ptr, end - ptr, num_values, suffix_lengths);
  if (suffix_decoded != num_values) return -1;

  // 4. Skip over the suffix-length stream to reach the raw suffix bytes.
  ptr = SkipDeltaBinaryPacked(suffix_start, end, num_values);
  if (ptr > end) return -1;

  // 5. Reconstruct strings: prefix from previous value + new suffix.
  std::string previous;
  for (int32_t i = 0; i < num_values; i++) {
    int32_t prefix_len = prefix_lengths[i];
    int32_t suffix_len = suffix_lengths[i];

    if (prefix_len < 0 || suffix_len < 0) return -1;
    if (ptr + suffix_len > end) return -1;

    std::string value;
    if (prefix_len > 0) {
      if ((size_t)prefix_len > previous.size()) return -1;
      value = previous.substr(0, prefix_len);
    }
    if (suffix_len > 0) {
      value.append(reinterpret_cast<const char *>(ptr), suffix_len);
      ptr += suffix_len;
    }

    values.push_back(value);
    previous = value;
  }

  return (int32_t)values.size();
}

// ---------------------------------------------------------------------------
// DecodeByteStreamSplit
// ---------------------------------------------------------------------------

// The PLAIN-layout decoders below write native values as PLAIN's little-endian
// bytes; every target (ARM64, x86-64, RISC-V) is little-endian.
static_assert(__BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__,
              "PLAIN-layout decoders write native little-endian bytes");

template <size_t W>
static void UnsplitStreams(const uint8_t *data, size_t n, uint8_t *dst) {
  for (size_t i = 0; i < n; ++i)
    for (size_t j = 0; j < W; ++j) dst[i * W + j] = data[j * n + i];
}

std::string DecodeByteStreamSplit(const uint8_t *data, size_t data_size,
                                  int32_t num_values, size_t width, uint8_t *dst) {
  const size_t n = (size_t)num_values;
  if (num_values < 0 || width == 0 || data_size != n * width) {
    return "BYTE_STREAM_SPLIT page holds " + std::to_string(data_size) +
           " value bytes, expected " + std::to_string(n * width) + " (" +
           std::to_string(num_values) + " values of " + std::to_string(width) + " bytes)";
  }
  switch (width) {
  case 4: UnsplitStreams<4>(data, n, dst); break;
  case 8: UnsplitStreams<8>(data, n, dst); break;
  default:
    for (size_t j = 0; j < width; ++j) {
      const uint8_t *s = data + j * n;
      for (size_t i = 0; i < n; ++i) dst[i * width + j] = s[i];
    }
  }
  return std::string();
}

// ---------------------------------------------------------------------------
// DecodeAlp
// ---------------------------------------------------------------------------
// Normative decode (AlpEncoding.md "Decoding"): unpack the FOR deltas, add the
// frame of reference, multiply by 10^factor THEN by 10^-exponent in the value's
// own precision, then patch the exceptions in by raw bit pattern. The power-of-
// ten tables are the correctly rounded IEEE 754 values of the decimal literals,
// as the spec requires — compile-time literals, never pow().

namespace {

template <typename F> struct AlpTraits;
template <> struct AlpTraits<float> {
  using Signed = int32_t;
  using Unsigned = uint32_t;
  static constexpr uint8_t kMaxExponent = 10;
  static constexpr size_t kForBytes = 5;  // int32 frame_of_reference + bit_width
  static constexpr float kPow10[11] = {1e0f, 1e1f, 1e2f, 1e3f, 1e4f, 1e5f,
                                       1e6f, 1e7f, 1e8f, 1e9f, 1e10f};
  static constexpr float kNegPow10[11] = {1e0f,  1e-1f, 1e-2f, 1e-3f, 1e-4f, 1e-5f,
                                          1e-6f, 1e-7f, 1e-8f, 1e-9f, 1e-10f};
};
template <> struct AlpTraits<double> {
  using Signed = int64_t;
  using Unsigned = uint64_t;
  static constexpr uint8_t kMaxExponent = 18;
  static constexpr size_t kForBytes = 9;  // int64 frame_of_reference + bit_width
  static constexpr double kPow10[19] = {1e0,  1e1,  1e2,  1e3,  1e4,  1e5,  1e6,
                                        1e7,  1e8,  1e9,  1e10, 1e11, 1e12, 1e13,
                                        1e14, 1e15, 1e16, 1e17, 1e18};
  static constexpr double kNegPow10[19] = {1e0,   1e-1,  1e-2,  1e-3,  1e-4,
                                           1e-5,  1e-6,  1e-7,  1e-8,  1e-9,
                                           1e-10, 1e-11, 1e-12, 1e-13, 1e-14,
                                           1e-15, 1e-16, 1e-17, 1e-18};
};

std::string AlpError(const std::string &what) { return "ALP page: " + what; }

template <typename F>
std::string DecodeAlpTyped(const uint8_t *data, size_t data_size, int32_t num_values,
                           F *out) {
  using T = AlpTraits<F>;
  using U = typename T::Unsigned;
  using S = typename T::Signed;
  constexpr size_t kVecHeader = 4 + T::kForBytes;
  constexpr uint32_t kMaxBits = sizeof(U) * 8;

  if (data_size < 7) return AlpError("truncated header");
  if (data[0] != 0)
    return AlpError("compression_mode " + std::to_string(data[0]) +
                    " is not supported (only 0 = ALP)");
  if (data[1] != 0)
    return AlpError("integer_encoding " + std::to_string(data[1]) +
                    " is not supported (only 0 = FOR + bit-packing)");
  const uint32_t log_vs = data[2];
  if (log_vs < 3 || log_vs > 15)
    return AlpError("log_vector_size " + std::to_string(log_vs) + " is outside [3, 15]");
  const int32_t num_elements = ReadLE32(data + 3);
  if (num_elements != num_values)
    return AlpError("header declares " + std::to_string(num_elements) +
                    " values, the page holds " + std::to_string(num_values) +
                    " present values");

  const size_t n = (size_t)num_values;
  const size_t vs = (size_t)1 << log_vs;
  const size_t num_vectors = (n + vs - 1) / vs;
  const uint8_t *base = data + 7;  // offsets are relative to the offset array
  const size_t region = data_size - 7;
  if (num_vectors * 4 > region) return AlpError("truncated offset array");

  for (size_t v = 0; v < num_vectors; ++v) {
    const size_t off = ReadLE32U(base + v * 4);
    const size_t limit = v + 1 < num_vectors ? ReadLE32U(base + (v + 1) * 4) : region;
    if (off < num_vectors * 4 || off > limit || limit > region)
      return AlpError("vector " + std::to_string(v) + " offset is out of bounds");
    const uint8_t *p = base + off;
    const size_t avail = limit - off;
    const size_t count = std::min(vs, n - v * vs);
    if (avail < kVecHeader)
      return AlpError("vector " + std::to_string(v) + " header is truncated");

    const uint8_t exponent = p[0];
    const uint8_t factor = p[1];
    const size_t num_exceptions = (size_t)p[2] | ((size_t)p[3] << 8);
    U frame;
    std::memcpy(&frame, p + 4, sizeof(U));
    const uint32_t bit_width = p[4 + sizeof(U)];
    if (exponent > T::kMaxExponent || factor > exponent)
      return AlpError("vector " + std::to_string(v) + " exponent " +
                      std::to_string(exponent) + " / factor " + std::to_string(factor) +
                      " is out of range");
    if (bit_width > kMaxBits)
      return AlpError("vector " + std::to_string(v) + " bit_width " +
                      std::to_string(bit_width) + " exceeds " + std::to_string(kMaxBits));
    if (num_exceptions > count)
      return AlpError("vector " + std::to_string(v) + " declares " +
                      std::to_string(num_exceptions) + " exceptions for " +
                      std::to_string(count) + " values");
    const size_t packed_bytes = (count * bit_width + 7) / 8;
    if (kVecHeader + packed_bytes + num_exceptions * (2 + sizeof(F)) > avail)
      return AlpError("vector " + std::to_string(v) + " data is truncated");

    const uint8_t *packed = p + kVecHeader;
    const F mul_factor = T::kPow10[factor];
    const F mul_exponent = T::kNegPow10[exponent];
    F *dst = out + v * vs;
    if (bit_width == 0) {
      const F value = static_cast<F>(static_cast<S>(frame)) * mul_factor * mul_exponent;
      for (size_t i = 0; i < count; ++i) dst[i] = value;
    } else {
      for (size_t i = 0; i < count; ++i) {
        const U delta = (U)ReadPackedLSB(packed, packed_bytes, i, bit_width);
        dst[i] = static_cast<F>(static_cast<S>(delta + frame)) * mul_factor * mul_exponent;
      }
    }

    const uint8_t *positions = packed + packed_bytes;
    const uint8_t *exceptions = positions + num_exceptions * 2;
    for (size_t k = 0; k < num_exceptions; ++k) {
      const size_t pos = (size_t)positions[2 * k] | ((size_t)positions[2 * k + 1] << 8);
      if (pos >= count)
        return AlpError("vector " + std::to_string(v) + " exception position " +
                        std::to_string(pos) + " is outside its " +
                        std::to_string(count) + " values");
      std::memcpy(dst + pos, exceptions + k * sizeof(F), sizeof(F));
    }
  }
  return std::string();
}

}  // namespace

std::string DecodeAlp(const uint8_t *data, size_t data_size, int32_t num_values,
                      size_t width, uint8_t *dst) {
  if (num_values < 0) return AlpError("negative value count");
  if (width == 4)
    return DecodeAlpTyped<float>(data, data_size, num_values, reinterpret_cast<float *>(dst));
  if (width == 8)
    return DecodeAlpTyped<double>(data, data_size, num_values, reinterpret_cast<double *>(dst));
  return AlpError("value width " + std::to_string(width) + " is not FLOAT or DOUBLE");
}

// ---------------------------------------------------------------------------
// DecodeDeltaLengthByteArray
// ---------------------------------------------------------------------------

std::string DecodeDeltaLengthByteArray(const uint8_t *data, size_t data_size,
                                       int32_t num_values,
                                       draken::AppendBuffer<uint8_t> &out) {
  out.clear();
  if (num_values == 0) return std::string();
  const uint8_t *end = data + data_size;
  std::vector<int32_t> lengths;
  if (DecodeDeltaBinaryPacked(data, data_size, num_values, lengths) != num_values)
    return "DELTA_LENGTH_BYTE_ARRAY page: length stream holds fewer than " +
           std::to_string(num_values) + " lengths";
  const uint8_t *bytes = SkipDeltaBinaryPacked(data, end, num_values);
  if (bytes > end)
    return "DELTA_LENGTH_BYTE_ARRAY page: length stream runs past the page";
  size_t total = 0;
  for (int32_t len : lengths) {
    if (len < 0) return "DELTA_LENGTH_BYTE_ARRAY page: negative value length";
    total += (size_t)len;
  }
  if (total > (size_t)(end - bytes))
    return "DELTA_LENGTH_BYTE_ARRAY page: lengths total " + std::to_string(total) +
           " bytes, the page holds " + std::to_string((size_t)(end - bytes));
  out.resize_uninit((size_t)num_values * 4 + total);
  uint8_t *w = out.data();
  for (int32_t len : lengths) {
    std::memcpy(w, &len, 4);  // PLAIN length prefix
    std::memcpy(w + 4, bytes, (size_t)len);
    w += 4 + (size_t)len;
    bytes += len;
  }
  return std::string();
}
