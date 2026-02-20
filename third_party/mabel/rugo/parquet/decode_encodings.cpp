// decode_encodings.cpp
// Non-template implementations of the Parquet encoding helpers declared in
// decode_encodings.hpp.

#include "decode_encodings.hpp"
#include <cstring>
#include <iostream>

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

      for (int32_t i = 0; i < values_in_run && decoded < num_values; i++) {
        uint32_t value    = 0;
        int bit_pos       = i * bit_width;
        int byte_pos      = bit_pos / 8;
        int bit_offset    = bit_pos % 8;

        for (int b = 0; b < 5 && byte_pos + b < bytes_needed; b++) {
          value |= ((uint32_t)ptr[byte_pos + b]) << (b * 8);
        }
        value = (value >> bit_offset) & ((1U << bit_width) - 1);
        indices.push_back((int32_t)value);
        decoded++;
      }
      ptr += bytes_needed;

    } else {
      // RLE run: (header >> 1) repetitions of a single value.
      int32_t count         = (int32_t)(header >> 1);
      int     bytes_needed  = (bit_width + 7) / 8;
      uint32_t value        = 0;

      for (int i = 0; i < bytes_needed && ptr < end; i++) {
        value |= ((uint32_t)(*ptr++)) << (i * 8);
      }
      value &= (1U << bit_width) - 1;

      for (int32_t i = 0; i < count && decoded < num_values; i++) {
        indices.push_back((int32_t)value);
        decoded++;
      }
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

    for (uint64_t mb = 0; mb < num_miniblocks && ptr < end; mb++) {
      uint8_t bit_width = *ptr++;
      if (bit_width > 0) {
        int32_t bytes_needed =
            ((int32_t)values_per_miniblock * bit_width + 7) / 8;
        if (ptr + bytes_needed > end) break;
        ptr += bytes_needed;
      }
      skipped += (int32_t)values_per_miniblock;
      if (skipped >= num_values) break;
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
