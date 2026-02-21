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
    // Step 2: Iterate over all data pages in this column chunk
    // -----------------------------------------------------------------
    if (target_col->data_page_offset < 0 ||
        (uint64_t)target_col->data_page_offset >= file_size) {
      return result;
    }

    // Compute the end of the column chunk (upper bound for cursor).
    uint64_t chunk_end;
    {
      uint64_t chunk_start =
          (target_col->dictionary_page_offset >= 0 &&
           target_col->dictionary_page_offset < target_col->data_page_offset)
              ? (uint64_t)target_col->dictionary_page_offset
              : (uint64_t)target_col->data_page_offset;
      if (target_col->total_compressed_size > 0) {
        chunk_end = chunk_start + (uint64_t)target_col->total_compressed_size;
        if (chunk_end > file_size) chunk_end = file_size;
      } else {
        chunk_end = file_size;
      }
    }

    int32_t total_needed    = target_col->num_values;  // 0 means "accumulate all"

    // Accumulate definition levels across all pages to build validity bitmap later.
    std::vector<int32_t> all_def_levels;
    if (target_col->max_definition_level > 0) {
      all_def_levels.reserve(total_needed > 0 ? total_needed : 100000);
    }

    int32_t total_collected = 0;
    const uint8_t *cursor      = file_data + (uint64_t)target_col->data_page_offset;
    const uint8_t *chunk_limit = file_data + chunk_end;

    while (cursor < chunk_limit &&
           (total_needed <= 0 || total_collected < total_needed)) {

      // Parse the page header at current cursor position.
      TInput header_in{cursor, chunk_limit};
      PageHeader page_header = ParsePageHeader(header_in);
      size_t header_size = (size_t)(header_in.p - cursor);

      if (page_header.page_type == 2) {
        // DICTIONARY_PAGE in the data range – already loaded above; skip it.
        cursor += header_size + (size_t)page_header.compressed_page_size;
        continue;
      }
      if (page_header.page_type != 0) break;  // Not a DATA_PAGE – stop

      int32_t page_values = page_header.num_values;
      if (page_values <= 0) break;  // Corrupt or empty page

      // Locate compressed payload.
      const uint8_t *compressed_data = cursor + header_size;
      size_t compressed_size = (size_t)page_header.compressed_page_size;
      size_t avail = (size_t)(chunk_limit - compressed_data);
      if (compressed_size == 0 || compressed_size > avail)
        compressed_size = avail;

      // Decompress if needed.
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
        } catch (const std::exception &e) {
          break;  // Decompression failure: stop page loop, report partial success
        }
      }

      // Step 3: Skip repetition levels; decode definition levels
      
      // Skip repetition levels if present (we don't use them yet).
      if (target_col->max_repetition_level > 0) {
        size_t skip = SkipRLEBitPackedLevels(
            data_ptr, data_size, target_col->max_repetition_level);
        data_ptr  += skip;
        data_size -= skip;
      }

      // Decode definition levels to build validity bitmap.
      std::vector<int32_t> def_levels;
      if (target_col->max_definition_level > 0) {
        // Compute bit-width needed to encode levels 0..max_definition_level
        int def_bit_width = 0;
        int32_t max_level = target_col->max_definition_level;
        while (max_level > 0) { def_bit_width++; max_level >>= 1; }

        // Construct 4-byte length prefix for levels data (reuse DecodeRLEBitPackedIndicesWithConsumption).
        std::vector<uint8_t> level_data_with_prefix(4 + data_size);
        uint32_t data_len = (uint32_t)data_size;
        level_data_with_prefix[0] = (data_len >>  0) & 0xFF;
        level_data_with_prefix[1] = (data_len >>  8) & 0xFF;
        level_data_with_prefix[2] = (data_len >> 16) & 0xFF;
        level_data_with_prefix[3] = (data_len >> 24) & 0xFF;
        std::memcpy(level_data_with_prefix.data() + 4, data_ptr, data_size);

        size_t bytes_consumed = 0;
        int32_t decoded_levels = DecodeRLEBitPackedIndicesWithConsumption(
            level_data_with_prefix.data(), level_data_with_prefix.size(),
            page_values, def_bit_width, def_levels, bytes_consumed);

        if (decoded_levels != page_values) return result;

        // Advance data_ptr past the definition level bytes.
        data_ptr  += (bytes_consumed - 4);  // subtract the synthetic 4-byte prefix
        data_size -= (bytes_consumed - 4);
        
        // Accumulate definition levels for later validity bitmap construction.
        all_def_levels.insert(all_def_levels.end(), def_levels.begin(), def_levels.end());
      }

      const uint8_t *data_end = data_ptr + data_size;

      // Step 4: Decode page values
      bool encoding_requires_dictionary =
          (page_header.encoding == 2 || page_header.encoding == 8);
      bool page_uses_dictionary = encoding_requires_dictionary && dict_size > 0;

      if (encoding_requires_dictionary && dict_size == 0) return result;

      if (page_uses_dictionary) {
        // On-disk layout: 1 byte bit_width, then RLE/bit-packed indices
        // (no 4-byte length prefix). Construct synthetic prefix for decoder.
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
            page_values, bit_width, indices);

        if (decoded != page_values) return result;

        if (result.type == "int32") {
          for (int32_t idx : indices) {
            if (idx < 0 || idx >= (int32_t)dict_int32.size()) return result;
            result.int32_values.push_back(dict_int32[idx]);
          }
        } else if (result.type == "int64") {
          for (int32_t idx : indices) {
            if (idx < 0 || idx >= (int32_t)dict_int64.size()) return result;
            result.int64_values.push_back(dict_int64[idx]);
          }
        } else if (result.type == "byte_array") {
          for (int32_t idx : indices) {
            if (idx < 0 || idx >= (int32_t)dict_string.size()) return result;
            result.string_values.push_back(dict_string[idx]);
          }
        } else if (result.type == "float32") {
          for (int32_t idx : indices) {
            if (idx < 0 || idx >= (int32_t)dict_float32.size()) return result;
            result.float32_values.push_back(dict_float32[idx]);
          }
        } else if (result.type == "float64") {
          for (int32_t idx : indices) {
            if (idx < 0 || idx >= (int32_t)dict_float64.size()) return result;
            result.float64_values.push_back(dict_float64[idx]);
          }
        }

      } else {
        // PLAIN or DELTA encoding
        int32_t page_encoding = page_header.encoding;

        if (result.type == "int32") {
          if (page_encoding == 4) {
            std::vector<int32_t> page_ints;
            int32_t decoded = DecodeDeltaBinaryPacked(data_ptr, data_size,
                                                       page_values, page_ints);
            if (decoded != page_values) return result;
            result.int32_values.insert(result.int32_values.end(),
                                        page_ints.begin(), page_ints.end());
          } else {
            for (int32_t i = 0; i < page_values && data_ptr + 4 <= data_end; i++) {
              result.int32_values.push_back(ReadLE32(data_ptr));
              data_ptr += 4;
            }
          }
        } else if (result.type == "int64") {
          if (page_encoding == 4) {
            std::vector<int64_t> page_ints;
            int32_t decoded = DecodeDeltaBinaryPacked(data_ptr, data_size,
                                                       page_values, page_ints);
            if (decoded != page_values) return result;
            result.int64_values.insert(result.int64_values.end(),
                                        page_ints.begin(), page_ints.end());
          } else {
            for (int32_t i = 0; i < page_values && data_ptr + 8 <= data_end; i++) {
              result.int64_values.push_back(ReadLE64(data_ptr));
              data_ptr += 8;
            }
          }
        } else if (result.type == "byte_array") {
          if (page_encoding == 6) {
            std::vector<std::string> page_strs;
            int32_t decoded = DecodeDeltaByteArray(data_ptr, data_size,
                                                    page_values, page_strs);
            if (decoded != page_values) return result;
            result.string_values.insert(result.string_values.end(),
                                         page_strs.begin(), page_strs.end());
          } else {
            for (int32_t i = 0; i < page_values && data_ptr + 4 <= data_end; i++) {
              int32_t length = ReadLE32(data_ptr);
              data_ptr += 4;
              if (data_ptr + length > data_end) break;
              result.string_values.emplace_back(
                  reinterpret_cast<const char *>(data_ptr), length);
              data_ptr += length;
            }
          }
        } else if (result.type == "boolean") {
          // PLAIN: 1 bit per value, LSB-first; bit index resets per page.
          for (int32_t i = 0; i < page_values && data_ptr < data_end; i++) {
            uint8_t byte_value = data_ptr[i / 8];
            result.boolean_values.push_back((byte_value >> (i % 8)) & 1);
            if ((i + 1) % 8 == 0) data_ptr++;
          }
          if (page_values % 8 != 0 && page_values > 0) data_ptr++;
        } else if (result.type == "float32") {
          for (int32_t i = 0; i < page_values && data_ptr + 4 <= data_end; i++) {
            result.float32_values.push_back(ReadFloat32(data_ptr));
            data_ptr += 4;
          }
        } else if (result.type == "float64") {
          for (int32_t i = 0; i < page_values && data_ptr + 8 <= data_end; i++) {
            result.float64_values.push_back(ReadFloat64(data_ptr));
            data_ptr += 8;
          }
        }
      }

      total_collected += page_values;
      cursor = compressed_data + compressed_size;
    }  // end page loop

    // Build validity bitmap from accumulated definition levels.
    if (!all_def_levels.empty()) {
      int32_t total_rows = (int32_t)all_def_levels.size();
      int32_t bitmap_bytes = (total_rows + 7) / 8;
      result.valid_bits.resize(bitmap_bytes, 0);

      int32_t max_def = target_col->max_definition_level;
      for (int32_t i = 0; i < total_rows; i++) {
        // Validity: 1 if def_level == max_definition_level (value present), 0 otherwise (null)
        if (all_def_levels[i] == max_def) {
          result.valid_bits[i / 8] |= (1 << (i % 8));
        }
      }
    }

    // Success: all expected values collected (or at least some, if total unknown).
    if (total_needed > 0) {
      result.success = (total_collected == total_needed);
    } else {
      result.success = (total_collected > 0);
    }

  } catch (const std::exception &e) {
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
