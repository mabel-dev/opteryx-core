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
#include "telemetry.hpp"
#include <algorithm>
#include <cstring>
#include <iostream>
#include <stdexcept>
#include <string>
#include <unordered_map>

namespace {

inline uint8_t CodeWidthForDictSize(size_t dict_size) {
  if (dict_size <= 256) return 1;
  if (dict_size <= 65536) return 2;
  return 4;
}

inline int32_t InternByteArrayToDictionary(
    const char* value_ptr,
    int32_t value_len,
    std::unordered_map<std::string, int32_t>& dict_map,
    std::vector<uint8_t>& arena,
    std::vector<uint32_t>& offsets,
    std::vector<int32_t>& lens) {
  std::string key(value_ptr, static_cast<size_t>(value_len));
  auto it = dict_map.find(key);
  if (it != dict_map.end()) {
    return it->second;
  }

  int32_t code = static_cast<int32_t>(lens.size());
  uint32_t off = static_cast<uint32_t>(arena.size());
  arena.insert(arena.end(), key.begin(), key.end());
  offsets.push_back(off);
  lens.push_back(value_len);
  dict_map.emplace(std::move(key), code);
  return code;
}

inline void SeedDictionaryMapFromArena(
    std::unordered_map<std::string, int32_t>& dict_map,
    const std::vector<uint8_t>& arena,
    const std::vector<uint32_t>& offsets,
    const std::vector<int32_t>& lens) {
  dict_map.reserve(lens.size() * 2 + 1);
  for (size_t i = 0; i < lens.size(); ++i) {
    const char* ptr = reinterpret_cast<const char*>(arena.data() + offsets[i]);
    dict_map.emplace(std::string(ptr, static_cast<size_t>(lens[i])),
                     static_cast<int32_t>(i));
  }
}

template <typename T>
inline int32_t InternPrimitiveToDictionary(
    T value,
    std::unordered_map<T, int32_t>& dict_map,
    std::vector<T>& dict_values) {
  auto it = dict_map.find(value);
  if (it != dict_map.end()) {
    return it->second;
  }

  int32_t code = static_cast<int32_t>(dict_values.size());
  dict_values.push_back(value);
  dict_map.emplace(value, code);
  return code;
}

template <typename T>
inline void SeedPrimitiveDictionaryMap(
    std::unordered_map<T, int32_t>& dict_map,
    const std::vector<T>& dict_values) {
  dict_map.reserve(dict_values.size() * 2 + 1);
  for (size_t i = 0; i < dict_values.size(); ++i) {
    dict_map.emplace(dict_values[i], static_cast<int32_t>(i));
  }
}

inline uint32_t Float32Bits(float value) {
  uint32_t bits;
  std::memcpy(&bits, &value, sizeof(uint32_t));
  return bits;
}

inline uint64_t Float64Bits(double value) {
  uint64_t bits;
  std::memcpy(&bits, &value, sizeof(uint64_t));
  return bits;
}

}  // namespace

// ---------------------------------------------------------------------------
// DecodeColumnFromChunk (internal)
// ---------------------------------------------------------------------------
// Decodes a single column starting at target_col->dictionary_page_offset (if
// present) and target_col->data_page_offset inside the supplied memory region.

DecodedColumn DecodeColumnFromChunk(const uint8_t *file_data,
                                    size_t file_size,
                                    const ColumnStats *target_col,
                                    int64_t* ext_int64,
                                    double*  ext_float64,
                                    int32_t* ext_int32,
                                    float*   ext_float32,
                                    const uint8_t* row_mask) {
  DecodedColumn result;
  result.ext_int64   = ext_int64;
  result.ext_float64 = ext_float64;
  result.ext_int32   = ext_int32;
  result.ext_float32 = ext_float32;
  result.ext_written = 0;

  // When masking, disable zero-copy external buffers — force internal vectors
  // so the post-loop filter has a single consistent place to apply the mask.
  if (row_mask != nullptr) {
    ext_int64  = nullptr;
    ext_float64 = nullptr;
    ext_int32  = nullptr;
    ext_float32 = nullptr;
  }

  ++rugo_tel::calls;

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
    result.max_rep_level = target_col->max_repetition_level;
    result.max_def_level = target_col->max_definition_level;

    // -----------------------------------------------------------------
    // Step 1: Load dictionary page (if present)
    // -----------------------------------------------------------------
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
        result.dict_ordered = dict_page_header.dictionary_is_sorted;
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
            { RUGO_TEL_START(_dc_t0);
              rugo::compression::DecompressInto(
                  dict_compressed_data, dict_compressed_size,
                  dict_page_header.uncompressed_page_size, codec,
                  dict_decompressed_data);
              RUGO_TEL_ACCUM(rugo_tel::decompress_s, _dc_t0); }
            dict_data_ptr  = dict_decompressed_data.data();
            dict_data_size = dict_decompressed_data.size();
          } catch (...) {
            return result;
          }
        }

        dict_size = dict_page_header.num_values;
        if (dict_size > 0) {
          result.code_width = CodeWidthForDictSize(static_cast<size_t>(dict_size));
        }
        const uint8_t *dict_end = dict_data_ptr + dict_data_size;

        RUGO_TEL_START(_dp_t0);
        if (result.type == "int32") {
          result.dict_int32_values.reserve(dict_size);
          for (int32_t i = 0; i < dict_size && dict_data_ptr + 4 <= dict_end; i++) {
            result.dict_int32_values.push_back(ReadLE32(dict_data_ptr));
            dict_data_ptr += 4;
          }
        } else if (result.type == "int64") {
          result.dict_int64_values.reserve(dict_size);
          for (int32_t i = 0; i < dict_size && dict_data_ptr + 8 <= dict_end; i++) {
            result.dict_int64_values.push_back(ReadLE64(dict_data_ptr));
            dict_data_ptr += 8;
          }
        } else if (result.type == "byte_array") {
          // Build a flat arena: one allocation for all dict string bytes,
          // plus offset/length arrays — no per-entry std::string heap alloc.
          result.string_dict_arena.reserve(dict_data_size);
          result.string_dict_offsets.reserve(dict_size);
          result.string_dict_lens.reserve(dict_size);
          for (int32_t i = 0; i < dict_size && dict_data_ptr + 4 <= dict_end; i++) {
            int32_t length = ReadLE32(dict_data_ptr);
            dict_data_ptr += 4;
            if (dict_data_ptr + length > dict_end) break;
            uint32_t off = (uint32_t)result.string_dict_arena.size();
            result.string_dict_arena.insert(result.string_dict_arena.end(),
                                            dict_data_ptr, dict_data_ptr + length);
            result.string_dict_offsets.push_back(off);
            result.string_dict_lens.push_back(length);
            dict_data_ptr += length;
          }
        } else if (result.type == "float32") {
          result.dict_float32_values.reserve(dict_size);
          for (int32_t i = 0; i < dict_size && dict_data_ptr + 4 <= dict_end; i++) {
            result.dict_float32_values.push_back(ReadFloat32(dict_data_ptr));
            dict_data_ptr += 4;
          }
        } else if (result.type == "float64") {
          result.dict_float64_values.reserve(dict_size);
          for (int32_t i = 0; i < dict_size && dict_data_ptr + 8 <= dict_end; i++) {
            result.dict_float64_values.push_back(ReadFloat64(dict_data_ptr));
            dict_data_ptr += 8;
          }
        }
        RUGO_TEL_ACCUM(rugo_tel::dict_parse_s, _dp_t0);
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

    // Accumulate repetition and definition levels across all pages.
    // rep_levels: only populated when max_repetition_level > 0 (list columns).
    // def_levels: used both for validity bitmap (all nullable columns) and for
    //             list offset reconstruction (Step 10).
    std::vector<int32_t> all_rep_levels;
    std::vector<int32_t> all_def_levels;
    if (target_col->max_repetition_level > 0) {
      all_rep_levels.reserve(total_needed > 0 ? total_needed : 100000);
    }
    if (target_col->max_definition_level > 0) {
      all_def_levels.reserve(total_needed > 0 ? total_needed : 100000);
    }

    int32_t total_collected = 0;
    const uint8_t *cursor      = file_data + (uint64_t)target_col->data_page_offset;
    const uint8_t *chunk_limit = file_data + chunk_end;

    // When true, byte_array values are kept dictionary-encoded. Mixed dict/plain
    // pages are unified into a synthetic per-chunk dictionary via unified_dict_map.
    bool byte_array_dict_mode = (result.type == "byte_array" && dict_size > 0);
    bool int32_dict_mode = (result.type == "int32" && dict_size > 0);
    bool int64_dict_mode = (result.type == "int64" && dict_size > 0);
    bool float32_dict_mode = (result.type == "float32" && dict_size > 0);
    bool float64_dict_mode = (result.type == "float64" && dict_size > 0);
    std::unordered_map<std::string, int32_t> unified_dict_map;
    std::unordered_map<int32_t, int32_t> int32_dict_map;
    std::unordered_map<int64_t, int32_t> int64_dict_map;
    std::unordered_map<uint32_t, int32_t> float32_dict_map;
    std::unordered_map<uint64_t, int32_t> float64_dict_map;

    int32_t page_row_offset = 0;
    std::vector<uint8_t> decoded_row_mask;
    if (row_mask != nullptr) {
      decoded_row_mask.reserve(total_needed > 0 ? (size_t)total_needed : 65536u);
    }

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

      // ── Row-mask page skipping ──────────────────────────────────────
      if (row_mask != nullptr) {
        bool any_selected = false;
        for (int32_t _i = 0; _i < page_values && !any_selected; ++_i) {
          if (row_mask[page_row_offset + _i]) any_selected = true;
        }
        if (!any_selected) {
          // No selected rows in this page: skip decompression entirely.
          page_row_offset += page_values;
          total_collected += page_values;
          cursor = compressed_data + compressed_size;
          ++result.pages_skipped;
          continue;
        }
        // Page has selections: record which of its rows are wanted.
        for (int32_t _i = 0; _i < page_values; ++_i) {
          decoded_row_mask.push_back(row_mask[page_row_offset + _i]);
        }
        page_row_offset += page_values;
      }
      // ────────────────────────────────────────────────────────────────

      ++result.pages_decoded;  // this page survived the row_mask check (or no mask); will be decompressed

      // Decompress if needed.
      const uint8_t *data_ptr;
      size_t         data_size;

      if (target_col->codec == 0) {
        data_ptr  = compressed_data;
        data_size = compressed_size;
      } else {
        try {
          auto codec = rugo::compression::CodecFromInt(target_col->codec);
          { RUGO_TEL_START(_pg_t0);
            rugo::compression::DecompressInto(
                compressed_data, compressed_size,
                page_header.uncompressed_page_size, codec,
                page_decompressed_data);
            RUGO_TEL_ACCUM(rugo_tel::decompress_s, _pg_t0); }
          data_ptr  = page_decompressed_data.data();
          data_size = page_decompressed_data.size();
        } catch (const std::exception &e) {
          break;  // Decompression failure: stop page loop, report partial success
        }
      }

      // Step 3: Decode repetition levels; decode definition levels.

      // Repetition levels (V1 pages: 4-byte LE length prefix).
      // Per the Parquet spec encoding.md table: Data page v1 repetition levels
      // are RLE/bit-packed and prefixed with a 4-byte LE length.
      std::vector<int32_t> page_rep_levels;
      if (target_col->max_repetition_level > 0) {
        int rep_bit_width = 0;
        int32_t max_rep = target_col->max_repetition_level;
        while (max_rep > 0) { rep_bit_width++; max_rep >>= 1; }

        if (data_size < 4) return result;
        uint32_t rep_payload_bytes = ReadLE32(data_ptr);
        size_t   rep_slice_size    = 4 + (size_t)rep_payload_bytes;
        if (rep_slice_size > data_size) return result;

        size_t bytes_consumed_rep = 0;
        int32_t decoded_rep = DecodeRLEBitPackedIndicesWithConsumption(
            data_ptr, rep_slice_size,
            page_values, rep_bit_width, page_rep_levels, bytes_consumed_rep);

        if (decoded_rep != page_values) return result;

        data_ptr  += rep_slice_size;
        data_size -= rep_slice_size;

        all_rep_levels.insert(all_rep_levels.end(),
                              page_rep_levels.begin(), page_rep_levels.end());
      }

      // Decode definition levels to build validity bitmap.
      // Per the Parquet V1 spec, definition level data on disk is already
      // prefixed with a 4-byte LE length.  Pass data_ptr directly — bounded
      // to exactly 4 + real_length bytes — so DecodeRLEBitPackedIndices skips
      // the real prefix and reads only the level bytes, not the value data.
      std::vector<int32_t> def_levels;
      if (target_col->max_definition_level > 0) {
        // Compute bit-width needed to encode levels 0..max_definition_level
        int def_bit_width = 0;
        int32_t max_level = target_col->max_definition_level;
        while (max_level > 0) { def_bit_width++; max_level >>= 1; }

        // On-disk: [4-byte LE length][RLE level data …]
        if (data_size < 4) return result;
        uint32_t level_payload_bytes = ReadLE32(data_ptr);
        size_t   level_slice_size    = 4 + (size_t)level_payload_bytes;
        if (level_slice_size > data_size) return result;

        size_t bytes_consumed = 0;
        int32_t decoded_levels = DecodeRLEBitPackedIndicesWithConsumption(
            data_ptr, level_slice_size,
            page_values, def_bit_width, def_levels, bytes_consumed);

        if (decoded_levels != page_values) return result;

        // Advance data_ptr past exactly the level bytes.
        data_ptr  += level_slice_size;
        data_size -= level_slice_size;

        // Accumulate definition levels for later validity bitmap construction.
        all_def_levels.insert(all_def_levels.end(), def_levels.begin(), def_levels.end());
      }

      const uint8_t *data_end = data_ptr + data_size;

      // Compute the number of present (non-null) values in this page.
      // The value stream only contains entries for present slots; null slots
      // are represented solely in the validity bitmap built from def_levels.
      int32_t present_count = page_values;  // default: all values present
      if (!def_levels.empty()) {
        int32_t max_def = target_col->max_definition_level;
        present_count = 0;
        for (int32_t dl : def_levels)
          if (dl == max_def) ++present_count;
      }

      // Step 4: Decode page values
      bool encoding_requires_dictionary =
          (page_header.encoding == 2 || page_header.encoding == 8);
      bool page_uses_dictionary = encoding_requires_dictionary && dict_size > 0;

      // Some writers emit dictionary-encoded nullable pages where every row is
      // null: dictionary page has 0 entries and present_count is 0.
      // This is valid because no dictionary indices are consumed.
      if (encoding_requires_dictionary && dict_size == 0 && present_count > 0) {
        return result;
      }

      if (page_uses_dictionary) {
        // On-disk layout: 1 byte bit_width, then RLE/bit-packed indices with no
        // length prefix.  Read bit_width and decode directly — no synthetic copy.
        int bit_width = (int)data_ptr[0];
        data_ptr++;
        data_size--;

        std::vector<int32_t> indices;
        { RUGO_TEL_START(_rle_t0);
          int32_t decoded = DecodeRLEBitPackedIndicesNoPrefix(
              data_ptr, data_size, present_count, bit_width, indices);
          RUGO_TEL_ACCUM(rugo_tel::rle_s, _rle_t0);
          if (decoded != present_count) return result; }

        RUGO_TEL_START(_vx_t0);
        if (result.type == "int32") {
          if (int32_dict_mode) {
            result.dict_indices.reserve(result.dict_indices.size() + indices.size());
            for (int32_t idx : indices) {
              if (idx < 0 || idx >= (int32_t)result.dict_int32_values.size()) return result;
              result.dict_indices.push_back(idx);
            }
          } else {
            int32_t n = (int32_t)indices.size();
            if (result.ext_int32) {
              int32_t* edst = result.ext_int32 + result.ext_written;
              for (int32_t i = 0; i < n; ++i) {
                int32_t idx = indices[i];
                if (idx < 0 || idx >= (int32_t)result.dict_int32_values.size()) return result;
                edst[i] = result.dict_int32_values[idx];
              }
              result.ext_written += n;
            } else {
              size_t old_sz = result.int32_values.size();
              result.int32_values.resize(old_sz + n);
              int32_t* dst = result.int32_values.data() + old_sz;
              for (int32_t i = 0; i < n; ++i) {
                int32_t idx = indices[i];
                if (idx < 0 || idx >= (int32_t)result.dict_int32_values.size()) return result;
                dst[i] = result.dict_int32_values[idx];
              }
            }
          }
        } else if (result.type == "int64") {
          if (int64_dict_mode) {
            result.dict_indices.reserve(result.dict_indices.size() + indices.size());
            for (int32_t idx : indices) {
              if (idx < 0 || idx >= (int32_t)result.dict_int64_values.size()) return result;
              result.dict_indices.push_back(idx);
            }
          } else {
            int32_t n = (int32_t)indices.size();
            if (result.ext_int64) {
              int64_t* edst = result.ext_int64 + result.ext_written;
              for (int32_t i = 0; i < n; ++i) {
                int32_t idx = indices[i];
                if (idx < 0 || idx >= (int32_t)result.dict_int64_values.size()) return result;
                edst[i] = result.dict_int64_values[idx];
              }
              result.ext_written += n;
            } else {
              size_t old_sz = result.int64_values.size();
              result.int64_values.resize(old_sz + n);
              int64_t* dst = result.int64_values.data() + old_sz;
              for (int32_t i = 0; i < n; ++i) {
                int32_t idx = indices[i];
                if (idx < 0 || idx >= (int32_t)result.dict_int64_values.size()) return result;
                dst[i] = result.dict_int64_values[idx];
              }
            }
          }
        } else if (result.type == "byte_array") {
          // Store raw indices; arena dict is already in result.string_dict_*.
          result.dict_indices.reserve(result.dict_indices.size() + indices.size());
          for (int32_t idx : indices) {
            if (idx < 0 || idx >= (int32_t)result.string_dict_lens.size()) return result;
            result.dict_indices.push_back(idx);
          }
        } else if (result.type == "float32") {
          if (float32_dict_mode) {
            result.dict_indices.reserve(result.dict_indices.size() + indices.size());
            for (int32_t idx : indices) {
              if (idx < 0 || idx >= (int32_t)result.dict_float32_values.size()) return result;
              result.dict_indices.push_back(idx);
            }
          } else {
            int32_t n = (int32_t)indices.size();
            if (result.ext_float32) {
              float* edst = result.ext_float32 + result.ext_written;
              for (int32_t i = 0; i < n; ++i) {
                int32_t idx = indices[i];
                if (idx < 0 || idx >= (int32_t)result.dict_float32_values.size()) return result;
                edst[i] = result.dict_float32_values[idx];
              }
              result.ext_written += n;
            } else {
              size_t old_sz = result.float32_values.size();
              result.float32_values.resize(old_sz + n);
              float* dst = result.float32_values.data() + old_sz;
              for (int32_t i = 0; i < n; ++i) {
                int32_t idx = indices[i];
                if (idx < 0 || idx >= (int32_t)result.dict_float32_values.size()) return result;
                dst[i] = result.dict_float32_values[idx];
              }
            }
          }
        } else if (result.type == "float64") {
          if (float64_dict_mode) {
            result.dict_indices.reserve(result.dict_indices.size() + indices.size());
            for (int32_t idx : indices) {
              if (idx < 0 || idx >= (int32_t)result.dict_float64_values.size()) return result;
              result.dict_indices.push_back(idx);
            }
          } else {
            int32_t n = (int32_t)indices.size();
            if (result.ext_float64) {
              double* edst = result.ext_float64 + result.ext_written;
              for (int32_t i = 0; i < n; ++i) {
                int32_t idx = indices[i];
                if (idx < 0 || idx >= (int32_t)result.dict_float64_values.size()) return result;
                edst[i] = result.dict_float64_values[idx];
              }
              result.ext_written += n;
            } else {
              size_t old_sz = result.float64_values.size();
              result.float64_values.resize(old_sz + n);
              double* dst = result.float64_values.data() + old_sz;
              for (int32_t i = 0; i < n; ++i) {
                int32_t idx = indices[i];
                if (idx < 0 || idx >= (int32_t)result.dict_float64_values.size()) return result;
                dst[i] = result.dict_float64_values[idx];
              }
            }
          }
        }
        RUGO_TEL_ACCUM(rugo_tel::val_expand_s, _vx_t0);

      } else {
        // PLAIN or DELTA encoding
        int32_t page_encoding = page_header.encoding;

        if (result.type == "int32") {
          if (int32_dict_mode) {
            if (int32_dict_map.empty()) {
              SeedPrimitiveDictionaryMap(int32_dict_map, result.dict_int32_values);
            }
            if (page_encoding == 4) {
              std::vector<int32_t> page_ints;
              int32_t decoded =
                  DecodeDeltaBinaryPacked(data_ptr, data_size, present_count, page_ints);
              if (decoded != present_count) return result;
              result.dict_indices.reserve(result.dict_indices.size() + page_ints.size());
              for (int32_t value : page_ints) {
                int32_t code = InternPrimitiveToDictionary(value, int32_dict_map, result.dict_int32_values);
                result.dict_indices.push_back(code);
              }
            } else {
              result.dict_indices.reserve(result.dict_indices.size() + present_count);
              for (int32_t i = 0; i < present_count && data_ptr + 4 <= data_end; i++) {
                int32_t value = ReadLE32(data_ptr);
                data_ptr += 4;
                int32_t code = InternPrimitiveToDictionary(value, int32_dict_map, result.dict_int32_values);
                result.dict_indices.push_back(code);
              }
            }
          } else if (result.ext_int32) {
            if (page_encoding == 4) {
              std::vector<int32_t> page_ints;
              int32_t decoded = DecodeDeltaBinaryPacked(data_ptr, data_size, present_count, page_ints);
              if (decoded != present_count) return result;
              std::copy(page_ints.begin(), page_ints.end(), result.ext_int32 + result.ext_written);
              result.ext_written += (int32_t)page_ints.size();
            } else {
              int32_t* edst = result.ext_int32 + result.ext_written;
              for (int32_t i = 0; i < present_count && data_ptr + 4 <= data_end; i++) {
                *edst++ = ReadLE32(data_ptr);
                data_ptr += 4;
              }
              result.ext_written += present_count;
            }
          } else {
            if (page_encoding == 4) {
              std::vector<int32_t> page_ints;
              int32_t decoded = DecodeDeltaBinaryPacked(data_ptr, data_size,
                                                         present_count, page_ints);
              if (decoded != present_count) return result;
              result.int32_values.insert(result.int32_values.end(),
                                          page_ints.begin(), page_ints.end());
            } else {
              for (int32_t i = 0; i < present_count && data_ptr + 4 <= data_end; i++) {
                result.int32_values.push_back(ReadLE32(data_ptr));
                data_ptr += 4;
              }
            }
          }
        } else if (result.type == "int64") {
          if (int64_dict_mode) {
            if (int64_dict_map.empty()) {
              SeedPrimitiveDictionaryMap(int64_dict_map, result.dict_int64_values);
            }
            if (page_encoding == 4) {
              std::vector<int64_t> page_ints;
              int32_t decoded =
                  DecodeDeltaBinaryPacked(data_ptr, data_size, present_count, page_ints);
              if (decoded != present_count) return result;
              result.dict_indices.reserve(result.dict_indices.size() + page_ints.size());
              for (int64_t value : page_ints) {
                int32_t code = InternPrimitiveToDictionary(value, int64_dict_map, result.dict_int64_values);
                result.dict_indices.push_back(code);
              }
            } else {
              result.dict_indices.reserve(result.dict_indices.size() + present_count);
              for (int32_t i = 0; i < present_count && data_ptr + 8 <= data_end; i++) {
                int64_t value = ReadLE64(data_ptr);
                data_ptr += 8;
                int32_t code = InternPrimitiveToDictionary(value, int64_dict_map, result.dict_int64_values);
                result.dict_indices.push_back(code);
              }
            }
          } else if (result.ext_int64) {
            if (page_encoding == 4) {
              std::vector<int64_t> page_ints;
              int32_t decoded = DecodeDeltaBinaryPacked(data_ptr, data_size, present_count, page_ints);
              if (decoded != present_count) return result;
              std::copy(page_ints.begin(), page_ints.end(), result.ext_int64 + result.ext_written);
              result.ext_written += (int32_t)page_ints.size();
            } else {
              int64_t* edst = result.ext_int64 + result.ext_written;
              for (int32_t i = 0; i < present_count && data_ptr + 8 <= data_end; i++) {
                *edst++ = ReadLE64(data_ptr);
                data_ptr += 8;
              }
              result.ext_written += present_count;
            }
          } else {
            if (page_encoding == 4) {
              std::vector<int64_t> page_ints;
              int32_t decoded = DecodeDeltaBinaryPacked(data_ptr, data_size,
                                                         present_count, page_ints);
              if (decoded != present_count) return result;
              result.int64_values.insert(result.int64_values.end(),
                                          page_ints.begin(), page_ints.end());
            } else {
              for (int32_t i = 0; i < present_count && data_ptr + 8 <= data_end; i++) {
                result.int64_values.push_back(ReadLE64(data_ptr));
                data_ptr += 8;
              }
            }
          }
        } else if (result.type == "byte_array") {
          // Mixed dict/plain chunks stay dictionary-encoded by interning plain
          // values into a synthetic unified dictionary (owning string keys).
          if (byte_array_dict_mode && unified_dict_map.empty()) {
            SeedDictionaryMapFromArena(
                unified_dict_map,
                result.string_dict_arena,
                result.string_dict_offsets,
                result.string_dict_lens);
          }
          if (page_encoding == 6) {
            std::vector<std::string> page_strs;
            int32_t decoded = DecodeDeltaByteArray(data_ptr, data_size,
                                                    present_count, page_strs);
            if (decoded != present_count) return result;
            if (byte_array_dict_mode) {
              result.dict_indices.reserve(result.dict_indices.size() + page_strs.size());
              for (const auto& value : page_strs) {
                int32_t code = InternByteArrayToDictionary(
                    value.data(),
                    static_cast<int32_t>(value.size()),
                    unified_dict_map,
                    result.string_dict_arena,
                    result.string_dict_offsets,
                    result.string_dict_lens);
                result.dict_indices.push_back(code);
              }
            } else {
              result.string_values.insert(result.string_values.end(),
                                           page_strs.begin(), page_strs.end());
            }
          } else {
            for (int32_t i = 0; i < present_count && data_ptr + 4 <= data_end; i++) {
              int32_t length = ReadLE32(data_ptr);
              data_ptr += 4;
              if (data_ptr + length > data_end) break;
              if (byte_array_dict_mode) {
                int32_t code = InternByteArrayToDictionary(
                    reinterpret_cast<const char*>(data_ptr),
                    length,
                    unified_dict_map,
                    result.string_dict_arena,
                    result.string_dict_offsets,
                    result.string_dict_lens);
                result.dict_indices.push_back(code);
              } else {
                result.string_values.emplace_back(
                    reinterpret_cast<const char *>(data_ptr), length);
              }
              data_ptr += length;
            }
          }
        } else if (result.type == "boolean") {
          if (page_encoding == 3) {
            // RLE encoding (encoding id 3): 4-byte LE length prefix + RLE/bit-packed
            // data with bit_width=1.  The value stream contains only present values.
            std::vector<int32_t> rle_vals;
            int32_t decoded = DecodeRLEBitPackedIndices(
                data_ptr, data_size, present_count, 1, rle_vals);
            if (decoded != present_count) return result;
            for (auto v : rle_vals)
              result.boolean_values.push_back((uint8_t)(v & 1));
          } else {
            // PLAIN: 1 bit per present value, LSB-first; bit index resets per page.
            const uint8_t *bool_start = data_ptr;
            for (int32_t i = 0; i < present_count && bool_start + (i / 8) < data_end; i++) {
              uint8_t byte_value = bool_start[i / 8];
              result.boolean_values.push_back((byte_value >> (i % 8)) & 1);
            }
            data_ptr += (present_count + 7) / 8;
          }
        } else if (result.type == "float32") {
          if (float32_dict_mode) {
            if (float32_dict_map.empty()) {
              float32_dict_map.reserve(result.dict_float32_values.size() * 2 + 1);
              for (size_t i = 0; i < result.dict_float32_values.size(); ++i) {
                float32_dict_map.emplace(Float32Bits(result.dict_float32_values[i]), static_cast<int32_t>(i));
              }
            }
            result.dict_indices.reserve(result.dict_indices.size() + present_count);
            for (int32_t i = 0; i < present_count && data_ptr + 4 <= data_end; i++) {
              float value = ReadFloat32(data_ptr);
              data_ptr += 4;
              uint32_t key = Float32Bits(value);
              auto it = float32_dict_map.find(key);
              int32_t code;
              if (it == float32_dict_map.end()) {
                code = static_cast<int32_t>(result.dict_float32_values.size());
                float32_dict_map.emplace(key, code);
                result.dict_float32_values.push_back(value);
              } else {
                code = it->second;
              }
              result.dict_indices.push_back(code);
            }
          } else if (result.ext_float32) {
            float* edst = result.ext_float32 + result.ext_written;
            for (int32_t i = 0; i < present_count && data_ptr + 4 <= data_end; i++) {
              *edst++ = ReadFloat32(data_ptr);
              data_ptr += 4;
            }
            result.ext_written += present_count;
          } else {
            for (int32_t i = 0; i < present_count && data_ptr + 4 <= data_end; i++) {
              result.float32_values.push_back(ReadFloat32(data_ptr));
              data_ptr += 4;
            }
          }
        } else if (result.type == "float64") {
          if (float64_dict_mode) {
            if (float64_dict_map.empty()) {
              float64_dict_map.reserve(result.dict_float64_values.size() * 2 + 1);
              for (size_t i = 0; i < result.dict_float64_values.size(); ++i) {
                float64_dict_map.emplace(Float64Bits(result.dict_float64_values[i]), static_cast<int32_t>(i));
              }
            }
            result.dict_indices.reserve(result.dict_indices.size() + present_count);
            for (int32_t i = 0; i < present_count && data_ptr + 8 <= data_end; i++) {
              double value = ReadFloat64(data_ptr);
              data_ptr += 8;
              uint64_t key = Float64Bits(value);
              auto it = float64_dict_map.find(key);
              int32_t code;
              if (it == float64_dict_map.end()) {
                code = static_cast<int32_t>(result.dict_float64_values.size());
                float64_dict_map.emplace(key, code);
                result.dict_float64_values.push_back(value);
              } else {
                code = it->second;
              }
              result.dict_indices.push_back(code);
            }
          } else if (result.ext_float64) {
            double* edst = result.ext_float64 + result.ext_written;
            for (int32_t i = 0; i < present_count && data_ptr + 8 <= data_end; i++) {
              *edst++ = ReadFloat64(data_ptr);
              data_ptr += 8;
            }
            result.ext_written += present_count;
          } else {
            for (int32_t i = 0; i < present_count && data_ptr + 8 <= data_end; i++) {
              result.float64_values.push_back(ReadFloat64(data_ptr));
              data_ptr += 8;
            }
          }
        }
      }

      total_collected += page_values;
      cursor = compressed_data + compressed_size;
    }  // end page loop

    const int32_t total_rows_all_pages = total_collected;

    // ── Post-loop row-mask filter ──────────────────────────────────────────
    // If a row_mask was supplied, filter all accumulated output vectors down
    // to only the K selected rows.  Pages with zero selections were already
    // skipped above; here we handle partial selections within decoded pages.
    if (row_mask != nullptr && !decoded_row_mask.empty()) {
      const int32_t total_decoded = (int32_t)decoded_row_mask.size();
      int32_t K = 0;
      for (uint8_t m : decoded_row_mask) K += (int32_t)m;

      if (K < total_decoded) {
        const bool has_nulls =
            (target_col->max_definition_level > 0 && !all_def_levels.empty());

        if (!has_nulls) {
          // Non-nullable: one value per row — simple mask filter on each vector.
          auto filt_i32 = [&](std::vector<int32_t>& v) {
            if (v.empty()) return;
            std::vector<int32_t> o; o.reserve(K);
            for (int32_t i = 0; i < (int32_t)v.size() && i < total_decoded; ++i)
              if (decoded_row_mask[i]) o.push_back(v[i]);
            v = std::move(o);
          };
          auto filt_i64 = [&](std::vector<int64_t>& v) {
            if (v.empty()) return;
            std::vector<int64_t> o; o.reserve(K);
            for (int32_t i = 0; i < (int32_t)v.size() && i < total_decoded; ++i)
              if (decoded_row_mask[i]) o.push_back(v[i]);
            v = std::move(o);
          };
          auto filt_f32 = [&](std::vector<float>& v) {
            if (v.empty()) return;
            std::vector<float> o; o.reserve(K);
            for (int32_t i = 0; i < (int32_t)v.size() && i < total_decoded; ++i)
              if (decoded_row_mask[i]) o.push_back(v[i]);
            v = std::move(o);
          };
          auto filt_f64 = [&](std::vector<double>& v) {
            if (v.empty()) return;
            std::vector<double> o; o.reserve(K);
            for (int32_t i = 0; i < (int32_t)v.size() && i < total_decoded; ++i)
              if (decoded_row_mask[i]) o.push_back(v[i]);
            v = std::move(o);
          };
          filt_i32(result.int32_values);
          filt_i64(result.int64_values);
          filt_f32(result.float32_values);
          filt_f64(result.float64_values);
          filt_i32(result.dict_indices);
          if (!result.boolean_values.empty()) {
            std::vector<uint8_t> o; o.reserve(K);
            for (int32_t i = 0; i < (int32_t)result.boolean_values.size() && i < total_decoded; ++i)
              if (decoded_row_mask[i]) o.push_back(result.boolean_values[i]);
            result.boolean_values = std::move(o);
          }
        } else {
          // Nullable: use def_levels to map rows → value positions.
          // Values in the stream correspond only to non-null rows.
          const int32_t max_def = target_col->max_definition_level;
          const bool have_i32   = !result.int32_values.empty();
          const bool have_i64   = !result.int64_values.empty();
          const bool have_f32   = !result.float32_values.empty();
          const bool have_f64   = !result.float64_values.empty();
          const bool have_dict  = !result.dict_indices.empty();
          const bool have_bool  = !result.boolean_values.empty();

          std::vector<int32_t> o_i32;
          std::vector<int64_t> o_i64;
          std::vector<float>   o_f32;
          std::vector<double>  o_f64;
          std::vector<int32_t> o_dict;
          std::vector<uint8_t> o_bool;

          int32_t val_idx = 0;
          for (int32_t row_i = 0; row_i < total_decoded; ++row_i) {
            const bool non_null = (row_i < (int32_t)all_def_levels.size() &&
                                   all_def_levels[row_i] == max_def);
            const bool selected = decoded_row_mask[row_i] != 0;
            if (non_null) {
              if (selected) {
                if (have_i32  && val_idx < (int32_t)result.int32_values.size())
                  o_i32.push_back(result.int32_values[val_idx]);
                if (have_i64  && val_idx < (int32_t)result.int64_values.size())
                  o_i64.push_back(result.int64_values[val_idx]);
                if (have_f32  && val_idx < (int32_t)result.float32_values.size())
                  o_f32.push_back(result.float32_values[val_idx]);
                if (have_f64  && val_idx < (int32_t)result.float64_values.size())
                  o_f64.push_back(result.float64_values[val_idx]);
                if (have_dict && val_idx < (int32_t)result.dict_indices.size())
                  o_dict.push_back(result.dict_indices[val_idx]);
                if (have_bool && val_idx < (int32_t)result.boolean_values.size())
                  o_bool.push_back(result.boolean_values[val_idx]);
              }
              ++val_idx;
            }
          }
          if (have_i32)  result.int32_values   = std::move(o_i32);
          if (have_i64)  result.int64_values   = std::move(o_i64);
          if (have_f32)  result.float32_values = std::move(o_f32);
          if (have_f64)  result.float64_values = std::move(o_f64);
          if (have_dict) result.dict_indices   = std::move(o_dict);
          if (have_bool) result.boolean_values = std::move(o_bool);
        }

        // Filter def_levels to selected rows (valid_bits is built from this below).
        if (!all_def_levels.empty()) {
          std::vector<int32_t> od; od.reserve(K);
          for (int32_t i = 0; i < total_decoded && i < (int32_t)all_def_levels.size(); ++i)
            if (decoded_row_mask[i]) od.push_back(all_def_levels[i]);
          all_def_levels = std::move(od);
        }
      }
      total_collected = K;
    }
    // ── End post-loop row-mask filter ──────────────────────────────────────

    result.num_rows = total_collected;

    if (result.type == "byte_array" && !result.string_dict_lens.empty()) {
      result.code_width = CodeWidthForDictSize(result.string_dict_lens.size());
    } else if (result.type == "int32" && !result.dict_int32_values.empty()) {
      result.code_width = CodeWidthForDictSize(result.dict_int32_values.size());
    } else if (result.type == "int64" && !result.dict_int64_values.empty()) {
      result.code_width = CodeWidthForDictSize(result.dict_int64_values.size());
    } else if (result.type == "float32" && !result.dict_float32_values.empty()) {
      result.code_width = CodeWidthForDictSize(result.dict_float32_values.size());
    } else if (result.type == "float64" && !result.dict_float64_values.empty()) {
      result.code_width = CodeWidthForDictSize(result.dict_float64_values.size());
    }

    // If every byte_array page used dictionary encoding, string_values is still empty
    // and dict_indices holds all per-row lookup indices.  The compact dictionary is
    // already in result.string_dict_arena / string_dict_offsets / string_dict_lens;
    // no action needed here.

    // Store accumulated levels in result for use by the Cython binding.
    // rep_levels and def_levels are needed for list column reconstruction (Step 10).
    // valid_bits is built from def_levels for direct null-bitmap use by flat columns.
    result.rep_levels = std::move(all_rep_levels);
    result.def_levels = all_def_levels;  // keep a copy; move would invalidate the loop below

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
      result.success = (total_rows_all_pages == total_needed);
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
                                     int row_group_index,
                                     int64_t* ext_int64,
                                     double*  ext_float64,
                                     int32_t* ext_int32,
                                     float*   ext_float32) {
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

    return DecodeColumnFromChunk(data, size, target_col,
                                 ext_int64, ext_float64, ext_int32, ext_float32);

  } catch (...) {
    result.success = false;
  }

  return result;
}
