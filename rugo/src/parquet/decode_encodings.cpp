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

// ---- scalar bulk (num_groups groups) ----------------------------------------
static void unpack_bitpacked_groups_scalar(const uint8_t* src, int32_t* dst,
                                           int num_groups, int bit_width)
{
    const int bpg = (bit_width <= 8) ? bit_width : (8 * bit_width + 7) / 8;
    for (int g = 0; g < num_groups; g++)
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

static void unpack_bitpacked_groups_neon(const uint8_t* src, int32_t* dst,
                                         int num_groups, int bit_width)
{
    const int bpg = (bit_width <= 8) ? bit_width : (8 * bit_width + 7) / 8;
    for (int g = 0; g < num_groups; g++)
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
    // Fallback for other widths.
    const int bpg = (bit_width <= 8) ? bit_width : (8 * bit_width + 7) / 8;
    for (int g = 0; g < num_groups; g++)
        unpack_group_8_scalar(src + g * bpg, dst + g * 8, bit_width);
}
#endif  // __AVX2__

// ---- Dispatch ---------------------------------------------------------------
using unpack_groups_fn_t = void(*)(const uint8_t*, int32_t*, int, int);
static std::atomic<unpack_groups_fn_t> s_unpack_cache{nullptr};

static unpack_groups_fn_t get_unpack_fn()
{
    return simd::select_dispatch<unpack_groups_fn_t>(s_unpack_cache, {
#if defined(__ARM_NEON) || defined(__ARM_NEON__)
        {&cpu_supports_neon, unpack_bitpacked_groups_neon},
#endif
#if defined(__AVX2__)
        {&cpu_supports_avx2, unpack_bitpacked_groups_avx2},
#endif
    }, unpack_bitpacked_groups_scalar);
}

// Unpack all complete groups from a bit-packed run, then handle any leftover
// values with the scalar helper.  Updates `decoded` and `ptr`.
static inline void decode_bitpacked_run(
        const uint8_t* run_start, int32_t values_in_run, int32_t bytes_needed,
        int32_t num_values, int bit_width,
        std::vector<int32_t>& indices, int32_t& decoded)
{
    const int32_t to_decode   = std::min(values_in_run, num_values - decoded);
    const int32_t full_groups = to_decode / 8;
    const int32_t remainder   = to_decode - full_groups * 8;
    const int     bpg         = (bit_width <= 8) ? bit_width : (8 * bit_width + 7) / 8;

    const size_t old_sz = indices.size();
    indices.resize(old_sz + to_decode);
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
                                  std::vector<int32_t> &indices) {
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
      indices.resize(old_sz + to_fill);
      std::fill_n(indices.data() + old_sz, to_fill, (int32_t)value);
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
                                                 std::vector<int32_t> &indices,
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
      indices.resize(old_sz + to_fill);
      std::fill_n(indices.data() + old_sz, to_fill, (int32_t)value);
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
                                          std::vector<int32_t> &indices) {
  if (bit_width > 32) return -1;
  if (bit_width == 0) {
    // Single-entry dictionary: every present value is index 0, no bits on wire.
    indices.assign(num_values, 0);
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
      indices.resize(old_sz + to_fill);
      std::fill_n(indices.data() + old_sz, to_fill, (int32_t)value);
      decoded += to_fill;
    }
  }

  return (decoded == num_values) ? decoded : -1;
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
// ---------------------------------------------------------------------------

int32_t DecodeRLEBitPackedIndicesToRuns(const uint8_t *data, size_t data_size,
                                        int32_t num_values, int bit_width,
                                        std::vector<int32_t> &run_codes,
                                        std::vector<int32_t> &run_counts) {
  if (bit_width > 32) return -1;
  if (bit_width == 0) {
    // Single-entry dictionary: every present value is index 0, no bits on wire.
    run_codes.assign(1, 0);
    run_counts.assign(1, num_values);
    return num_values;
  }

  run_codes.clear();
  run_counts.clear();

  const uint8_t *ptr = data;
  const uint8_t *end = data + data_size;

  // Reused across every bit-packed segment in this call — resize() only grows
  // the underlying allocation on the first (largest-so-far) segment; later
  // segments that are smaller or equal reuse the existing buffer with no
  // realloc, avoiding a fresh heap allocation per segment.
  std::vector<int32_t> scratch;

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
      scratch.resize(to_decode);
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

      // Merge scratch values into run_codes/run_counts.
      for (int32_t v : scratch) {
        if (!run_codes.empty() && run_codes.back() == v) {
          ++run_counts.back();
        } else {
          run_codes.push_back(v);
          run_counts.push_back(1);
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

      const int32_t to_fill = std::min(count, num_values - decoded);
      const int32_t code    = (int32_t)value;

      // Merge with previous run if same code (handles page-internal adjacency).
      if (!run_codes.empty() && run_codes.back() == code) {
        run_counts.back() += to_fill;
      } else {
        run_codes.push_back(code);
        run_counts.push_back(to_fill);
      }
      decoded += to_fill;
    }
  }

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
