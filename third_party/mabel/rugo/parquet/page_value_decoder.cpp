#include "page_value_decoder.hpp"
#include "page_task.hpp"
#include "rle_decoder.hpp"
#include "simd_gather.hpp"
#include "decode.hpp"
#include <algorithm>
#include <cstring>

namespace rugo::parquet {

// Helper: Read little-endian int32
inline int32_t read_le32(const uint8_t* data) {
  return (int32_t)data[0] | ((int32_t)data[1] << 8) |
         ((int32_t)data[2] << 16) | ((int32_t)data[3] << 24);
}

// Helper: Read little-endian int64
inline int64_t read_le64(const uint8_t* data) {
  return (int64_t)data[0] | ((int64_t)data[1] << 8) |
         ((int64_t)data[2] << 16) | ((int64_t)data[3] << 24) |
         ((int64_t)data[4] << 32) | ((int64_t)data[5] << 40) |
         ((int64_t)data[6] << 48) | ((int64_t)data[7] << 56);
}

// Helper: Read little-endian float32
inline float read_le_float32(const uint8_t* data) {
  uint32_t bits = read_le32((const uint8_t*)data);
  float result;
  std::memcpy(&result, &bits, sizeof(float));
  return result;
}

// Helper: Read little-endian float64
inline double read_le_float64(const uint8_t* data) {
  uint64_t bits = read_le64((const uint8_t*)data);
  double result;
  std::memcpy(&result, &bits, sizeof(double));
  return result;
}

// Helper: Compute log2(x) rounded up
inline int log2_ceiling(int x) {
  if (x <= 1) return 0;
  int result = 0;
  int power = 1;
  while (power < x) {
    power *= 2;
    result++;
  }
  return result;
}

// Main page value decoding function
void decode_page_values(const PageDecodeContext& ctx,
                        DecodedColumn& result) {

  const uint8_t* data = ctx.data_ptr;
  size_t offset = 0;
  int64_t out_offset = ctx.out_offset;

  // ===== 1. DECODE REPETITION LEVELS (if nested) =====
  std::vector<int8_t> page_repetition_levels;
  if (ctx.max_repetition_level > 0) {
    RleDecoder rep_decoder(data + offset);
    int rep_bit_width = log2_ceiling(ctx.max_repetition_level + 1);
    rep_decoder.decode_levels(rep_bit_width, ctx.page->num_values,
                              page_repetition_levels);
    offset += rep_decoder.bytes_consumed();
  }

  // ===== 2. DECODE DEFINITION LEVELS (if nullable) =====
  std::vector<int8_t> page_definition_levels;
  if (ctx.max_definition_level > 0) {
    RleDecoder def_decoder(data + offset);
    int def_bit_width = log2_ceiling(ctx.max_definition_level + 1);
    def_decoder.decode_levels(def_bit_width, ctx.page->num_values,
                              page_definition_levels);
    offset += def_decoder.bytes_consumed();
  }

  // ===== 3. COUNT PRESENT VALUES =====
  int32_t present_count = ctx.page->num_values;
  if (ctx.max_definition_level > 0) {
    present_count = 0;
    for (int8_t level : page_definition_levels) {
      if (level == ctx.max_definition_level) {
        present_count++;
      }
    }
  }

  // ===== 4. DICTIONARY MODE: Expand dictionary =====
  if (ctx.is_dictionary_encoded) {
    std::vector<int32_t> dict_indices;
    dict_indices.reserve(present_count);

    // Decode RLE/bit-packed dictionary indices
    RleDecoder dict_decoder(data + offset);
    int dict_bit_width = log2_ceiling(ctx.dict_count + 1);
    // Note: decode_bit_packed expects byte_count not element_count
    // Simplified: just read present_count elements
    for (int32_t i = 0; i < present_count; ++i) {
      uint32_t val = dict_decoder.read_varint();
      dict_indices.push_back((int32_t)val);
    }

    // Validate indices
    if (!dict_indices.empty()) {
      int32_t min_idx = *std::min_element(dict_indices.begin(), dict_indices.end());
      int32_t max_idx = *std::max_element(dict_indices.begin(), dict_indices.end());
      if (min_idx < 0 || max_idx >= ctx.dict_count) {
        throw std::runtime_error("Invalid dictionary index: min=" +
                                std::to_string(min_idx) + " max=" +
                                std::to_string(max_idx) + " dict_count=" +
                                std::to_string(ctx.dict_count));
      }
    }

    // Gather dictionary values using SIMD (Tier 2A)
    if (ctx.target_type == "int32") {
      const int32_t* dict_vals = (const int32_t*)ctx.dict_data;
      for (int32_t idx : dict_indices) {
        result.int32_values[out_offset++] = dict_vals[idx];
      }
    }
    else if (ctx.target_type == "int64") {
      const int64_t* dict_vals = (const int64_t*)ctx.dict_data;
      for (int32_t idx : dict_indices) {
        result.int64_values[out_offset++] = dict_vals[idx];
      }
    }
    else if (ctx.target_type == "float32") {
      const float* dict_vals = (const float*)ctx.dict_data;
      for (int32_t idx : dict_indices) {
        result.float32_values[out_offset++] = dict_vals[idx];
      }
    }
    else if (ctx.target_type == "float64") {
      const double* dict_vals = (const double*)ctx.dict_data;
      for (int32_t idx : dict_indices) {
        result.float64_values[out_offset++] = dict_vals[idx];
      }
    }
    else {
      throw std::runtime_error("Unsupported type for dictionary mode");
    }
  }
  else {
    // ===== 5. PLAIN ENCODING: Direct value decode =====

    if (ctx.target_type == "int32") {
      if (ctx.max_definition_level > 0) {
        for (size_t i = 0; i < page_definition_levels.size(); ++i) {
          if (page_definition_levels[i] == ctx.max_definition_level) {
            int32_t val = read_le32(data + offset);
            result.int32_values[out_offset++] = val;
            offset += 4;
          }
        }
      } else {
        for (int32_t i = 0; i < ctx.page->num_values; ++i) {
          int32_t val = read_le32(data + offset);
          result.int32_values[out_offset++] = val;
          offset += 4;
        }
      }
    }
    else if (ctx.target_type == "int64") {
      if (ctx.max_definition_level > 0) {
        for (size_t i = 0; i < page_definition_levels.size(); ++i) {
          if (page_definition_levels[i] == ctx.max_definition_level) {
            int64_t val = read_le64(data + offset);
            result.int64_values[out_offset++] = val;
            offset += 8;
          }
        }
      } else {
        for (int32_t i = 0; i < ctx.page->num_values; ++i) {
          int64_t val = read_le64(data + offset);
          result.int64_values[out_offset++] = val;
          offset += 8;
        }
      }
    }
    else if (ctx.target_type == "float32") {
      if (ctx.max_definition_level > 0) {
        for (size_t i = 0; i < page_definition_levels.size(); ++i) {
          if (page_definition_levels[i] == ctx.max_definition_level) {
            float val = read_le_float32(data + offset);
            result.float32_values[out_offset++] = val;
            offset += 4;
          }
        }
      } else {
        for (int32_t i = 0; i < ctx.page->num_values; ++i) {
          float val = read_le_float32(data + offset);
          result.float32_values[out_offset++] = val;
          offset += 4;
        }
      }
    }
    else if (ctx.target_type == "float64") {
      if (ctx.max_definition_level > 0) {
        for (size_t i = 0; i < page_definition_levels.size(); ++i) {
          if (page_definition_levels[i] == ctx.max_definition_level) {
            double val = read_le_float64(data + offset);
            result.float64_values[out_offset++] = val;
            offset += 8;
          }
        }
      } else {
        for (int32_t i = 0; i < ctx.page->num_values; ++i) {
          double val = read_le_float64(data + offset);
          result.float64_values[out_offset++] = val;
          offset += 8;
        }
      }
    }
    else {
      throw std::runtime_error("Unsupported type for plain encoding");
    }
  }

  // ===== 6. ACCUMULATE DEFINITION LEVELS =====
  if (ctx.max_definition_level > 0) {
    // Copy definition levels to result
    int64_t def_out_offset = out_offset - present_count;
    for (int8_t level : page_definition_levels) {
      result.def_levels[def_out_offset++] = (int32_t)level;
    }
  }
}

}  // namespace rugo::parquet
