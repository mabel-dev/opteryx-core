// decode_column.cpp
// Core column-level decoding:
//   DecodeColumnFromChunk  -- decodes a single column from a raw memory region
//                             (static; called only by DecodeColumnFromMemory)
//   DecodeColumnFromMemory -- public API: locates the column chunk inside a full
//                             file buffer and delegates to DecodeColumnFromChunk

#include "decode.hpp"
#include "decode_primitives.hpp"
#include "decode_encodings.hpp"
#include "decode_page.hpp"
#include "compression.hpp"
#include "metadata.hpp"
#include <algorithm>
#include <cstring>
#include <iostream>
#include <stdexcept>

// ---------------------------------------------------------------------------
// DecodeColumnFromChunk (internal)
// ---------------------------------------------------------------------------
// Decodes a single column starting at target_col->dictionary_page_offset (if
// present) and target_col->data_page_offset inside the supplied memory region.

static DecodedColumn DecodeColumnFromChunk(const uint8_t *file_data,
                                           size_t file_size,
                                           const ColumnStats *target_col) {
  DecodedColumn result;

  try {
    // Guard: only supported codecs.
    if (target_col->codec != 0 && target_col->codec != 1 &&
        target_col->codec != 6) {
      return result;
    }

    // Guard: at least one supported encoding.
    // IDs use Parquet spec values (post ZigZag-decode fix in metadata.cpp):
    //   0=PLAIN, 2=PLAIN_DICTIONARY, 3=RLE, 8=RLE_DICTIONARY
    bool has_supported_encoding = false;
    for (int32_t enc : target_col->encodings) {
      if (enc == 0 || enc == 2 || enc == 3 || enc == 8) {
        has_supported_encoding = true;
        break;
      }
    }
    if (!has_supported_encoding) return result;

    result.type = target_col->physical_type;

    // -----------------------------------------------------------------
    // Step 1: Load dictionary page (if present)
    // -----------------------------------------------------------------
    std::vector<int32_t>    dict_int32;
    std::vector<int64_t>    dict_int64;
    std::vector<std::string> dict_string;
    std::vector<float>       dict_float32;
    std::vector<double>      dict_float64;
    int32_t dict_size = 0;

    // Keep decompressed buffers alive across dictionary and data page decoding.
    std::vector<uint8_t> dict_decompressed_data;
    std::vector<uint8_t> page_decompressed_data;

    if (target_col->dictionary_page_offset >= 0 &&
        (uint64_t)target_col->dictionary_page_offset < file_size) {

      const uint8_t *dict_ptr     = file_data + target_col->dictionary_page_offset;
      const uint8_t *dict_end_ptr = file_data + file_size;

      TInput dict_header_in{dict_ptr, dict_end_ptr};
      PageHeader dict_page_header = ParsePageHeader(dict_header_in);

      if (dict_page_header.page_type == 2) {  // DICTIONARY_PAGE
        size_t dict_header_size = dict_header_in.p - dict_ptr;
        if (dict_header_size > (size_t)(dict_end_ptr - dict_ptr)) return result;

        size_t dict_compressed_size = dict_page_header.compressed_page_size;
        const uint8_t *dict_compressed_data = dict_ptr + dict_header_size;
        if (dict_compressed_size == 0 ||
            dict_compressed_size >
                (size_t)(dict_end_ptr - dict_compressed_data)) {
          dict_compressed_size = dict_end_ptr - dict_compressed_data;
        }

        const uint8_t *dict_data_ptr;
        size_t         dict_data_size;

        if (target_col->codec == 0) {
          dict_data_ptr  = dict_compressed_data;
          dict_data_size = dict_compressed_size;
        } else {
          try {
            auto codec = rugo::compression::CodecFromInt(target_col->codec);
            dict_decompressed_data = rugo::compression::DecompressData(
                dict_compressed_data, dict_compressed_size,
                dict_page_header.uncompressed_page_size, codec);
            dict_data_ptr  = dict_decompressed_data.data();
            dict_data_size = dict_decompressed_data.size();
          } catch (...) {
            return result;
          }
        }

        dict_size = dict_page_header.num_values;
        const uint8_t *dict_end = dict_data_ptr + dict_data_size;

        if (result.type == "int32") {
          dict_int32.reserve(dict_size);
          for (int32_t i = 0; i < dict_size && dict_data_ptr + 4 <= dict_end; i++) {
            dict_int32.push_back(ReadLE32(dict_data_ptr));
            dict_data_ptr += 4;
          }
        } else if (result.type == "int64") {
          dict_int64.reserve(dict_size);
          for (int32_t i = 0; i < dict_size && dict_data_ptr + 8 <= dict_end; i++) {
            dict_int64.push_back(ReadLE64(dict_data_ptr));
            dict_data_ptr += 8;
          }
        } else if (result.type == "byte_array") {
          dict_string.reserve(dict_size);
          for (int32_t i = 0; i < dict_size && dict_data_ptr + 4 <= dict_end; i++) {
            int32_t length = ReadLE32(dict_data_ptr);
            dict_data_ptr += 4;
            if (dict_data_ptr + length > dict_end) break;
            dict_string.emplace_back(
                reinterpret_cast<const char *>(dict_data_ptr), length);
            dict_data_ptr += length;
          }
        } else if (result.type == "float32") {
          dict_float32.reserve(dict_size);
          for (int32_t i = 0; i < dict_size && dict_data_ptr + 4 <= dict_end; i++) {
            dict_float32.push_back(ReadFloat32(dict_data_ptr));
            dict_data_ptr += 4;
          }
        } else if (result.type == "float64") {
          dict_float64.reserve(dict_size);
          for (int32_t i = 0; i < dict_size && dict_data_ptr + 8 <= dict_end; i++) {
            dict_float64.push_back(ReadFloat64(dict_data_ptr));
            dict_data_ptr += 8;
          }
        }
      }
    }

    // -----------------------------------------------------------------
    // Step 2: Locate and decompress the data page
    // -----------------------------------------------------------------
    if (target_col->data_page_offset < 0 ||
        (uint64_t)target_col->data_page_offset >= file_size) {
      return result;
    }

    const uint8_t *current_ptr = file_data + target_col->data_page_offset;
    size_t remaining_size;
    if (target_col->total_compressed_size > 0) {
      uint64_t max_size =
          file_size - (uint64_t)target_col->data_page_offset;
      remaining_size = (size_t)std::min<uint64_t>(
          target_col->total_compressed_size, max_size);
    } else {
      remaining_size = file_size - (uint64_t)target_col->data_page_offset;
    }

    TInput header_in{current_ptr, current_ptr + remaining_size};
    PageHeader page_header = ParsePageHeader(header_in);

    if (page_header.page_type != 0) return result;  // Not a DATA_PAGE

    size_t header_size = header_in.p - current_ptr;
    const uint8_t *compressed_data = current_ptr + header_size;
    size_t compressed_size = page_header.compressed_page_size;

    if (compressed_size == 0 || compressed_size > remaining_size - header_size) {
      compressed_size = remaining_size - header_size;
    }

    const uint8_t *data_ptr;
    size_t         data_size;

    if (target_col->codec == 0) {
      data_ptr  = compressed_data;
      data_size = compressed_size;
    } else {
      try {
        auto codec = rugo::compression::CodecFromInt(target_col->codec);
        page_decompressed_data = rugo::compression::DecompressData(
            compressed_data, compressed_size,
            page_header.uncompressed_page_size, codec);
        data_ptr  = page_decompressed_data.data();
        data_size = page_decompressed_data.size();
      } catch (const std::exception &) {
        return result;
      }
    }

    int32_t num_values = target_col->num_values;
    if (num_values <= 0) num_values = page_header.num_values;

    // -----------------------------------------------------------------
    // Step 3: Skip repetition and definition levels (Parquet Data Page V1)
    // -----------------------------------------------------------------
    if (target_col->max_repetition_level > 0) {
      size_t skip = SkipRLEBitPackedLevels(
          data_ptr, data_size, target_col->max_repetition_level);
      data_ptr  += skip;
      data_size -= skip;
    }
    if (target_col->max_definition_level > 0) {
      size_t skip = SkipRLEBitPackedLevels(
          data_ptr, data_size, target_col->max_definition_level);
      data_ptr  += skip;
      data_size -= skip;
    }

    const uint8_t *data_end = data_ptr + data_size;

    // -----------------------------------------------------------------
    // Step 4: Decode values
    // -----------------------------------------------------------------
    bool encoding_requires_dictionary =
        (page_header.encoding == 2 || page_header.encoding == 8);
    bool page_uses_dictionary = encoding_requires_dictionary && dict_size > 0;

    if (encoding_requires_dictionary && dict_size == 0) return result;

    if (page_uses_dictionary) {
      // The on-disk format for dictionary-indexed Data Pages is:
      //   1 byte: bit_width  (read directly from the page — do NOT recalculate)
      //   followed by: RLE/bit-packed data (no leading length prefix)
      // Our DecodeRLEBitPackedIndices expects a 4-byte length prefix, so we
      // construct a synthetic one.
      int bit_width = (int)data_ptr[0];
      data_ptr++;
      data_size--;

      std::vector<uint8_t> index_data_with_prefix(4 + data_size);
      uint32_t data_len = (uint32_t)data_size;
      index_data_with_prefix[0] = (data_len >>  0) & 0xFF;
      index_data_with_prefix[1] = (data_len >>  8) & 0xFF;
      index_data_with_prefix[2] = (data_len >> 16) & 0xFF;
      index_data_with_prefix[3] = (data_len >> 24) & 0xFF;
      std::memcpy(index_data_with_prefix.data() + 4, data_ptr, data_size);

      std::vector<int32_t> indices;
      int32_t decoded = DecodeRLEBitPackedIndices(
          index_data_with_prefix.data(), index_data_with_prefix.size(),
          num_values, bit_width, indices);

      if (decoded != num_values) return result;

      // Map indices to dictionary values.
      if (result.type == "int32") {
        result.int32_values.reserve(num_values);
        for (int32_t idx : indices) {
          if (idx < 0 || idx >= (int32_t)dict_int32.size()) return result;
          result.int32_values.push_back(dict_int32[idx]);
        }
        result.success = (result.int32_values.size() == (size_t)num_values);
      } else if (result.type == "int64") {
        result.int64_values.reserve(num_values);
        for (int32_t idx : indices) {
          if (idx < 0 || idx >= (int32_t)dict_int64.size()) return result;
          result.int64_values.push_back(dict_int64[idx]);
        }
        result.success = (result.int64_values.size() == (size_t)num_values);
      } else if (result.type == "byte_array") {
        result.string_values.reserve(num_values);
        for (int32_t idx : indices) {
          if (idx < 0 || idx >= (int32_t)dict_string.size()) return result;
          result.string_values.push_back(dict_string[idx]);
        }
        result.success = (result.string_values.size() == (size_t)num_values);
      } else if (result.type == "float32") {
        result.float32_values.reserve(num_values);
        for (int32_t idx : indices) {
          if (idx < 0 || idx >= (int32_t)dict_float32.size()) return result;
          result.float32_values.push_back(dict_float32[idx]);
        }
        result.success = (result.float32_values.size() == (size_t)num_values);
      } else if (result.type == "float64") {
        result.float64_values.reserve(num_values);
        for (int32_t idx : indices) {
          if (idx < 0 || idx >= (int32_t)dict_float64.size()) return result;
          result.float64_values.push_back(dict_float64[idx]);
        }
        result.success = (result.float64_values.size() == (size_t)num_values);
      }

    } else {
      // PLAIN or DELTA encoding (determined per-page from the page header).
      int32_t page_encoding = page_header.encoding;

      if (result.type == "int32") {
        if (page_encoding == 4) {  // DELTA_BINARY_PACKED
          int32_t decoded =
              DecodeDeltaBinaryPacked(data_ptr, data_size, num_values,
                                      result.int32_values);
          result.success = (decoded == num_values);
        } else {                   // PLAIN
          result.int32_values.reserve(num_values);
          for (int32_t i = 0; i < num_values && data_ptr + 4 <= data_end; i++) {
            result.int32_values.push_back(ReadLE32(data_ptr));
            data_ptr += 4;
          }
          result.success = (result.int32_values.size() == (size_t)num_values);
        }

      } else if (result.type == "int64") {
        if (page_encoding == 4) {  // DELTA_BINARY_PACKED
          int32_t decoded =
              DecodeDeltaBinaryPacked(data_ptr, data_size, num_values,
                                      result.int64_values);
          result.success = (decoded == num_values);
        } else {                   // PLAIN
          result.int64_values.reserve(num_values);
          for (int32_t i = 0; i < num_values && data_ptr + 8 <= data_end; i++) {
            result.int64_values.push_back(ReadLE64(data_ptr));
            data_ptr += 8;
          }
          result.success = (result.int64_values.size() == (size_t)num_values);
        }

      } else if (result.type == "byte_array") {
        if (page_encoding == 6) {  // DELTA_BYTE_ARRAY
          int32_t decoded =
              DecodeDeltaByteArray(data_ptr, data_size, num_values,
                                   result.string_values);
          result.success = (decoded == num_values);
        } else {                   // PLAIN: 4-byte length prefix per value
          result.string_values.reserve(num_values);
          for (int32_t i = 0; i < num_values && data_ptr + 4 <= data_end; i++) {
            int32_t length = ReadLE32(data_ptr);
            data_ptr += 4;
            if (data_ptr + length > data_end) break;
            result.string_values.emplace_back(
                reinterpret_cast<const char *>(data_ptr), length);
            data_ptr += length;
          }
          result.success = (result.string_values.size() == (size_t)num_values);
        }

      } else if (result.type == "boolean") {
        // PLAIN: 1 bit per value, LSB-first within each byte.
        result.boolean_values.reserve(num_values);
        for (int32_t i = 0; i < num_values && data_ptr < data_end; i++) {
          uint8_t byte_value = data_ptr[i / 8];
          result.boolean_values.push_back((byte_value >> (i % 8)) & 1);
          if ((i + 1) % 8 == 0) data_ptr++;
        }
        if (num_values % 8 != 0 && num_values > 0) data_ptr++;
        result.success = (result.boolean_values.size() == (size_t)num_values);

      } else if (result.type == "float32") {
        result.float32_values.reserve(num_values);
        for (int32_t i = 0; i < num_values && data_ptr + 4 <= data_end; i++) {
          result.float32_values.push_back(ReadFloat32(data_ptr));
          data_ptr += 4;
        }
        result.success = (result.float32_values.size() == (size_t)num_values);

      } else if (result.type == "float64") {
        result.float64_values.reserve(num_values);
        for (int32_t i = 0; i < num_values && data_ptr + 8 <= data_end; i++) {
          result.float64_values.push_back(ReadFloat64(data_ptr));
          data_ptr += 8;
        }
        result.success = (result.float64_values.size() == (size_t)num_values);
      }
    }

  } catch (const std::exception &) {
    result.success = false;
  } catch (...) {
    result.success = false;
  }

  return result;
}

// ---------------------------------------------------------------------------
// DecodeColumnFromMemory (public)
// ---------------------------------------------------------------------------

DecodedColumn DecodeColumnFromMemory(const uint8_t *data, size_t size,
                                     const std::string &column_name,
                                     const RowGroupStats &row_group,
                                     int row_group_index) {
  DecodedColumn result;

  try {
    const ColumnStats *target_col = nullptr;
    for (const auto &col : row_group.columns) {
      if (col.name == column_name) {
        target_col = &col;
        break;
      }
    }
    if (!target_col) return result;

    int64_t offset     = target_col->data_page_offset;
    int64_t total_size = target_col->total_compressed_size;
    if (offset < 0 || total_size <= 0) return result;
    if (offset >= (int64_t)size) return result;

    return DecodeColumnFromChunk(data, size, target_col);

  } catch (...) {
    result.success = false;
  }

  return result;
}
