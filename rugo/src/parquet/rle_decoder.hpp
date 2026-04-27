#pragma once

#include <cstdint>
#include <cstddef>
#include <vector>
#include <stdexcept>

namespace rugo::parquet {

class RleDecoder {
private:
  const uint8_t* data;
  size_t offset = 0;

public:
  explicit RleDecoder(const uint8_t* data_ptr) : data(data_ptr) {}

  // Read variable-length integer (little-endian base-128)
  uint32_t read_varint() {
    uint32_t result = 0;
    int shift = 0;
    uint8_t byte;

    do {
      if (offset >= 1000000) {
        throw std::runtime_error("RLE varint overflow");
      }
      byte = data[offset++];
      result |= (uint32_t)(byte & 0x7F) << shift;
      shift += 7;
    } while (byte & 0x80);

    return result;
  }

  // Decode definition/repetition levels (RLE/bit-packed format)
  void decode_levels(int bit_width, int num_values,
                     std::vector<int8_t>& out) {
    while ((int)out.size() < num_values) {
      uint32_t rle_count = read_varint();
      uint32_t rle_value = read_varint();

      // Check if RLE or bit-packed
      // RLE: (count << 1) | 1
      // Bit-packed: (byte_count << 1) | 0

      if (rle_count & 1) {
        // RLE: repeat value (rle_count >> 1) times
        uint32_t repeat_count = rle_count >> 1;
        for (uint32_t i = 0; i < repeat_count && (int)out.size() < num_values; ++i) {
          out.push_back((int8_t)rle_value);
        }
      } else {
        // Bit-packed: (rle_count >> 1) bytes of bit-packed data
        uint32_t byte_count = rle_count >> 1;
        decode_bit_packed(bit_width, byte_count, out, num_values);
      }
    }
  }

  // Get number of bytes consumed
  size_t bytes_consumed() const { return offset; }

private:
  void decode_bit_packed(int bit_width, uint32_t byte_count,
                        std::vector<int8_t>& out, int num_values) {
    const uint8_t* packed_data = data + offset;
    offset += byte_count;

    uint32_t bit_pos = 0;
    while ((int)out.size() < num_values && bit_pos < byte_count * 8) {
      uint32_t value = 0;

      // Extract bit_width bits
      for (int b = 0; b < bit_width; ++b) {
        uint32_t byte_idx = (bit_pos + b) / 8;
        uint32_t bit_in_byte = (bit_pos + b) % 8;

        if ((packed_data[byte_idx] >> bit_in_byte) & 1) {
          value |= (1U << b);
        }
      }

      out.push_back((int8_t)value);
      bit_pos += bit_width;
    }
  }
};

}  // namespace rugo::parquet
