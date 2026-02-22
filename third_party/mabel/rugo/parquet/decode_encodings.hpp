#pragma once
// decode_encodings.hpp
// Parquet encoding/decoding primitives:
//   - RLE/bit-packed level skipping & index decoding
//   - DELTA_BINARY_PACKED (int/long, template defined here)
//   - DELTA_BYTE_ARRAY (byte-array / string columns)
//
// Non-template implementations live in decode_encodings.cpp.

#include "decode_primitives.hpp"
#include <cstdint>
#include <vector>
#include <string>

// ---------------------------------------------------------------------------
// Definition levels / repetition levels
// ---------------------------------------------------------------------------

// Skip RLE/bit-packed encoded levels (repetition or definition).
// Returns the number of bytes to skip.
// Per the Parquet spec, for Data Page V1 levels are RLE-encoded with a 4-byte
// length prefix *only* when max_level > 0; when max_level == 0 nothing is
// written at all.
size_t SkipRLEBitPackedLevels(const uint8_t *data, size_t max_size,
                               int max_level);

// ---------------------------------------------------------------------------
// Dictionary index decoder (RLE / bit-packed hybrid)
// ---------------------------------------------------------------------------

// Decode RLE/bit-packed dictionary indices from `data`.
// Returns the number of indices decoded, or -1 on error.
// The caller must prepend a 4-byte little-endian length prefix to the raw
// on-disk data before calling this function (see DecodeColumnFromChunk).
int32_t DecodeRLEBitPackedIndices(const uint8_t *data, size_t data_size,
                                  int32_t num_values, int bit_width,
                                  std::vector<int32_t> &indices);

// Variant that also returns the number of bytes consumed (including the 4-byte prefix).
// On success, updates `bytes_consumed` with the total bytes read.
// Returns the number of indices decoded, or -1 on error.
int32_t DecodeRLEBitPackedIndicesWithConsumption(const uint8_t *data, size_t data_size,
                                                 int32_t num_values, int bit_width,
                                                 std::vector<int32_t> &indices,
                                                 size_t &bytes_consumed);

// Variant with no 4-byte length prefix: data points directly at the RLE stream.
int32_t DecodeRLEBitPackedIndicesNoPrefix(const uint8_t *data, size_t data_size,
                                          int32_t num_values, int bit_width,
                                          std::vector<int32_t> &indices);

// ---------------------------------------------------------------------------
// DELTA_BINARY_PACKED (encoding id 4) -- template; must live in header
// ---------------------------------------------------------------------------

// Decode DELTA_BINARY_PACKED for int32 or int64 columns.
// Returns the number of values decoded, or -1 on error.
template <typename T>
static int32_t DecodeDeltaBinaryPacked(const uint8_t *data, size_t data_size,
                                       int32_t num_values,
                                       std::vector<T> &values) {
  if (data_size < 4) return -1;

  const uint8_t *ptr = data;
  const uint8_t *end = data + data_size;

  values.clear();
  values.reserve(num_values);

  // Header
  uint64_t block_size = ReadUnsignedVarint(ptr, end);
  if (block_size == 0 || ptr >= end) return -1;

  uint64_t num_miniblocks = ReadUnsignedVarint(ptr, end);
  if (num_miniblocks == 0 || ptr >= end) return -1;

  /* total_value_count */ ReadUnsignedVarint(ptr, end);
  if (ptr >= end) return -1;

  int64_t first_value = ReadZigZagVarint(ptr, end);
  if (ptr > end) return -1;

  values.push_back(static_cast<T>(first_value));
  if (num_values == 1) return 1;

  int32_t decoded = 1;
  uint32_t values_per_miniblock = static_cast<uint32_t>(block_size / num_miniblocks);

  while (decoded < num_values && ptr < end) {
    int64_t min_delta = ReadZigZagVarint(ptr, end);
    if (ptr > end) break;

    std::vector<uint8_t> bit_widths;
    bit_widths.reserve(num_miniblocks);
    for (uint64_t mb = 0; mb < num_miniblocks && ptr < end; mb++) {
      bit_widths.push_back(*ptr++);
    }
    if (bit_widths.size() != num_miniblocks) break;

    for (uint64_t mb = 0; mb < num_miniblocks && decoded < num_values; mb++) {
      uint8_t bit_width = bit_widths[mb];

      if (bit_width == 0) {
        // All deltas in this miniblock equal min_delta; no data bytes needed.
        for (uint32_t i = 0; i < values_per_miniblock && decoded < num_values; i++) {
          T last = values.back();
          values.push_back(last + static_cast<T>(min_delta));
          decoded++;
        }
      } else {
        int32_t bytes_needed =
            (static_cast<int32_t>(values_per_miniblock) * bit_width + 7) / 8;
        if (ptr + bytes_needed > end) break;

        for (uint32_t i = 0; i < values_per_miniblock && decoded < num_values; i++) {
          uint64_t delta = 0;
          int bit_pos    = i * bit_width;
          int byte_pos   = bit_pos / 8;
          int bit_offset = bit_pos % 8;

          for (int b = 0; b < 9 && byte_pos + b < bytes_needed; b++) {
            delta |= ((uint64_t)ptr[byte_pos + b]) << (b * 8);
          }
          delta = (delta >> bit_offset) & ((1ULL << bit_width) - 1);

          T last = values.back();
          values.push_back(last + static_cast<T>(min_delta + (int64_t)delta));
          decoded++;
        }
        ptr += bytes_needed;
      }
    }
  }

  return decoded;
}

// ---------------------------------------------------------------------------
// DELTA_BYTE_ARRAY (encoding id 6)
// ---------------------------------------------------------------------------

// Decode DELTA_BYTE_ARRAY for byte-array (string) columns.
// Returns the number of values decoded, or -1 on error.
int32_t DecodeDeltaByteArray(const uint8_t *data, size_t data_size,
                             int32_t num_values,
                             std::vector<std::string> &values);
