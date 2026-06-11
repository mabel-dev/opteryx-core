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
#include "simd_gather.hpp"
#include "simd_compact.hpp"
#include "simd_validity_bitmap.hpp"
#include "type_widening.hpp"
#include "thread_pool.hpp"
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

// Read a big-endian, two's-complement signed integer of `width` bytes
// (1..8) into int64_t. Used to widen FIXED_LEN_BYTE_ARRAY DECIMAL values
// to int64. The first byte is sign-extended.
inline int64_t ReadBESignExt(const uint8_t* p, int width) {
  int64_t v = static_cast<int64_t>(static_cast<int8_t>(p[0]));
  for (int i = 1; i < width; ++i) {
    v = (v << 8) | static_cast<int64_t>(p[i]);
  }
  return v;
}

// Big-endian two's-complement signed integer of `width` bytes (9..16) into
// __int128. The first byte is sign-extended. Used to widen FIXED_LEN_BYTE_ARRAY
// DECIMAL values with precision > 18 into the int128-backed DECIMAL128 tier.
inline __int128 ReadBESignExt128(const uint8_t* p, int width) {
  __int128 v = static_cast<__int128>(static_cast<int8_t>(p[0]));
  for (int i = 1; i < width; ++i) {
    v = (v << 8) | static_cast<__int128>(p[i]);
  }
  return v;
}

inline void WritePackedCode(uint8_t* codes_array, size_t row_index,
                            int32_t code, uint8_t code_width) {
  switch (code_width) {
    case 1:
      codes_array[row_index] = (uint8_t)code;
      break;
    case 2:
      *(uint16_t*)(codes_array + row_index * 2) = (uint16_t)code;
      break;
    case 4:
      *(uint32_t*)(codes_array + row_index * 4) = (uint32_t)code;
      break;
  }
}

// ─── Open-addressed string intern table ──────────────────────────────────────
// Replaces std::unordered_map<std::string, int32_t> for the per-column-chunk
// unified dictionary on byte_array columns.  Designed for the parquet decode
// hot path: PLAIN-encoded data pages call intern() per row, millions of times
// for URL-shaped columns.  The std::unordered_map version dominates decode
// CPU because each call does a heap-alloc'd std::string key + node lookup.
//
// Wins:
//   - No per-call heap allocation: arena bytes are the canonical key storage;
//     slots reference into the arena by (offset, length).
//   - Open addressing with linear probing → one cache line per probe.
//   - 8-byte-chunked FNV-style hash → ~3-5 ns per short URL-sized value.
struct StringInternSlot {
  uint64_t hash;
  uint32_t arena_off;
  int32_t  len;        // -1 = empty slot
  int32_t  code;
};

struct StringInternTable {
  std::vector<StringInternSlot> slots;
  size_t mask = 0;     // capacity - 1; capacity is always a power of two
  size_t used = 0;

  inline bool empty() const { return used == 0; }

  inline void clear() {
    slots.clear();
    mask = 0;
    used = 0;
  }

  inline void resize_to(size_t new_capacity) {
    std::vector<StringInternSlot> old = std::move(slots);
    slots.assign(new_capacity, StringInternSlot{0, 0, -1, 0});
    mask = new_capacity - 1;
    for (const auto& s : old) {
      if (s.len < 0) continue;
      size_t b = s.hash & mask;
      while (slots[b].len >= 0) b = (b + 1) & mask;
      slots[b] = s;
    }
  }

  // 8-byte-chunked FNV-1a–style hash; cheap and adequate for short keys.
  static inline uint64_t hash_bytes(const char* data, size_t len) {
    uint64_t h = 0xcbf29ce484222325ULL;
    size_t i = 0;
    while (i + 8 <= len) {
      uint64_t chunk;
      std::memcpy(&chunk, data + i, 8);
      h ^= chunk;
      h *= 0x100000001b3ULL;
      i += 8;
    }
    while (i < len) {
      h ^= static_cast<uint8_t>(data[i]);
      h *= 0x100000001b3ULL;
      ++i;
    }
    h ^= h >> 32;
    return h;
  }
};

inline int32_t InternByteArrayToDictionary(
    const char* value_ptr,
    int32_t value_len,
    StringInternTable& table,
    std::vector<uint8_t>& arena,
    std::vector<uint32_t>& offsets,
    std::vector<int32_t>& lens) {
  if (table.slots.empty()) table.resize_to(64);
  // Grow at 75% load factor.
  if ((table.used + 1) * 4 > (table.mask + 1) * 3) {
    table.resize_to((table.mask + 1) * 2);
  }
  const uint64_t h = StringInternTable::hash_bytes(value_ptr, (size_t)value_len);
  size_t b = h & table.mask;
  while (true) {
    auto& s = table.slots[b];
    if (s.len < 0) {
      // Empty: append to arena, register slot.
      const uint32_t off = static_cast<uint32_t>(arena.size());
      arena.insert(arena.end(),
                   reinterpret_cast<const uint8_t*>(value_ptr),
                   reinterpret_cast<const uint8_t*>(value_ptr) + value_len);
      const int32_t code = static_cast<int32_t>(lens.size());
      offsets.push_back(off);
      lens.push_back(value_len);
      s.hash = h;
      s.arena_off = off;
      s.len = value_len;
      s.code = code;
      ++table.used;
      return code;
    }
    if (s.hash == h && s.len == value_len &&
        std::memcmp(arena.data() + s.arena_off, value_ptr, (size_t)value_len) == 0) {
      return s.code;
    }
    b = (b + 1) & table.mask;
  }
}

inline void SeedDictionaryMapFromArena(
    StringInternTable& table,
    const std::vector<uint8_t>& arena,
    const std::vector<uint32_t>& offsets,
    const std::vector<int32_t>& lens) {
  if (lens.empty()) return;
  size_t cap = 64;
  while (cap < lens.size() * 2) cap *= 2;
  table.resize_to(cap);
  for (size_t i = 0; i < lens.size(); ++i) {
    const char* ptr = reinterpret_cast<const char*>(arena.data() + offsets[i]);
    const int32_t l = lens[i];
    const uint64_t h = StringInternTable::hash_bytes(ptr, (size_t)l);
    size_t b = h & table.mask;
    while (table.slots[b].len >= 0) b = (b + 1) & table.mask;
    table.slots[b] = {h, offsets[i], l, static_cast<int32_t>(i)};
    ++table.used;
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

// ─────────────────────────────────────────────────────────────────────────
// Tier 3A: Page-parallel decode structures
// ─────────────────────────────────────────────────────────────────────────

// PageTask captures metadata for a single data page, used for parallel decoding.
// During pre-scan phase, we collect these without decoding.
// Then in parallel phase, each task decodes its own page independently.
struct PageTask {
  const uint8_t* compressed_data;  // Points into file_data
  size_t         compressed_size;
  uint32_t       uncompressed_size;
  int32_t        num_values;       // page_header.num_values
  int32_t        encoding;         // page_header.encoding
  uint8_t        rep_bit_width;    // Computed from repetition levels (if present)
  uint8_t        def_bit_width;    // Computed from definition levels (if present)
  int32_t        page_row_offset;  // Offset into row_mask (for filtering)
  int32_t        out_offset;       // Where this page's values start in output vectors
  bool           skip_page = false; // Set to true if row_mask indicates no selected rows
};

// Decompression result for a single page (used in parallel decompression)
struct PageDecompressed {
  int page_index;
  std::vector<uint8_t> data;  // Decompressed data for this page
  bool success = true;         // Set to false if decompression failed
};

}  // namespace

// ---------------------------------------------------------------------------
// Pre-Scan Helper (Tier 3A)
// ---------------------------------------------------------------------------
// Walk through all pages and collect metadata without decoding.
// Returns total number of values across all data pages.

int32_t PreScanPages(
    const uint8_t* cursor,
    const uint8_t* chunk_limit,
    const uint8_t* row_mask,
    std::vector<PageTask>& pages_out)
{
  int32_t total_values = 0;
  int32_t page_row_offset = 0;

  while (cursor < chunk_limit) {
    // Parse page header
    TInput header_in{cursor, chunk_limit};
    PageHeader page_header = ParsePageHeader(header_in);
    size_t header_size = (size_t)(header_in.p - cursor);

    // Skip dictionary pages (already loaded)
    if (page_header.page_type == 2) {
      cursor += header_size + (size_t)page_header.compressed_page_size;
      continue;
    }

    // Stop at non-data pages
    if (page_header.page_type != 0) break;

    int32_t page_values = page_header.num_values;
    if (page_values <= 0) break;

    // Locate compressed payload
    const uint8_t* compressed_data = cursor + header_size;
    size_t compressed_size = (size_t)page_header.compressed_page_size;
    size_t avail = (size_t)(chunk_limit - compressed_data);
    if (compressed_size > avail) compressed_size = avail;

    // Check row_mask: determine if page should be skipped (Tier 1D)
    bool should_skip = false;
    if (row_mask != nullptr) {
      const uint8_t* mp = row_mask + page_row_offset;
      const size_t pv = static_cast<size_t>(page_values);
      bool any_selected = false;

      // Word-at-a-time scan (8 bytes at once)
      size_t si = 0;
      for (; si + 8 <= pv; si += 8) {
        uint64_t w;
        std::memcpy(&w, mp + si, 8);
        if (w) { any_selected = true; break; }
      }
      if (!any_selected) {
        for (; si < pv; ++si) {
          if (mp[si]) { any_selected = true; break; }
        }
      }
      should_skip = !any_selected;
    }

    // Record this page's metadata for later parallel decoding
    PageTask task;
    task.compressed_data = compressed_data;
    task.compressed_size = compressed_size;
    task.uncompressed_size = page_header.uncompressed_page_size;
    task.num_values = page_values;
    task.encoding = page_header.encoding;
    task.page_row_offset = page_row_offset;
    task.out_offset = total_values;
    task.skip_page = should_skip;
    pages_out.push_back(task);

    total_values += page_values;
    page_row_offset += page_values;
    cursor = compressed_data + compressed_size;
  }

  return total_values;
}

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

    // FIXED_LEN_BYTE_ARRAY DECIMAL is decoded big-endian sign-extended:
    //   width <= 8  → int64   (DECIMAL,  precision <= 18)
    //   width 9..16 → int128  (DECIMAL128, precision > 18) — type "int128"
    // flba_byte_width > 0 selects the BE-stride read path; flba_int128 picks the tier.
    int flba_byte_width = 0;
    bool flba_int128 = false;
    if (target_col->physical_type == "fixed_len_byte_array") {
      if (target_col->type_length <= 0 || target_col->type_length > 16 ||
          target_col->logical_type.rfind("decimal", 0) != 0) {
        // Caller should have been gated by CanDecode; defensive bail.
        return result;
      }
      flba_byte_width = target_col->type_length;
      if (flba_byte_width > 8) {
        flba_int128 = true;
        result.type = "int128";
      } else {
        result.type = "int64";
      }
    }

    // DECIMAL128 (FLBA width 9..16) is only supported for PLAIN-encoded data today.
    // A dictionary page would feed the int64 dict path (silent truncation) or an
    // unhandled int128 dict path — fail loud instead. (Width<=8 dict decimals are
    // unaffected.) The plain int128 path is handled in the serial decode below.
    if (flba_int128 && target_col->dictionary_page_offset >= 0 &&
        (uint64_t)target_col->dictionary_page_offset < file_size) {
      return result;  // success stays false → "Decode failed" (honest rejection)
    }

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
          int32_t safe_count = std::min(dict_size, (int32_t)((dict_end - dict_data_ptr) / 4));
#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
          result.dict_int32_values.resize(safe_count);
          std::memcpy(result.dict_int32_values.data(), dict_data_ptr, safe_count * sizeof(int32_t));
          dict_data_ptr += safe_count * 4;
#else
          result.dict_int32_values.reserve(dict_size);
          for (int32_t i = 0; i < safe_count; i++) {
            result.dict_int32_values.push_back(ReadLE32(dict_data_ptr));
            dict_data_ptr += 4;
          }
#endif
        } else if (result.type == "int64") {
          if (flba_byte_width > 0) {
            // FIXED_LEN_BYTE_ARRAY DECIMAL dict: each value is `flba_byte_width`
            // bytes, big-endian, signed. Sign-extend to int64.
            int32_t safe_count = std::min(
                dict_size,
                (int32_t)((dict_end - dict_data_ptr) / flba_byte_width));
            result.dict_int64_values.reserve(safe_count);
            for (int32_t i = 0; i < safe_count; i++) {
              result.dict_int64_values.push_back(
                  ReadBESignExt(dict_data_ptr, flba_byte_width));
              dict_data_ptr += flba_byte_width;
            }
          } else {
          int32_t safe_count = std::min(dict_size, (int32_t)((dict_end - dict_data_ptr) / 8));
#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
          result.dict_int64_values.resize(safe_count);
          std::memcpy(result.dict_int64_values.data(), dict_data_ptr, safe_count * sizeof(int64_t));
          dict_data_ptr += safe_count * 8;
#else
          result.dict_int64_values.reserve(dict_size);
          for (int32_t i = 0; i < safe_count; i++) {
            result.dict_int64_values.push_back(ReadLE64(dict_data_ptr));
            dict_data_ptr += 8;
          }
#endif
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
          int32_t safe_count = std::min(dict_size, (int32_t)((dict_end - dict_data_ptr) / 4));
#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
          result.dict_float32_values.resize(safe_count);
          std::memcpy(result.dict_float32_values.data(), dict_data_ptr, safe_count * sizeof(float));
          dict_data_ptr += safe_count * 4;
#else
          result.dict_float32_values.reserve(dict_size);
          for (int32_t i = 0; i < safe_count; i++) {
            result.dict_float32_values.push_back(ReadFloat32(dict_data_ptr));
            dict_data_ptr += 4;
          }
#endif
        } else if (result.type == "float64") {
          int32_t safe_count = std::min(dict_size, (int32_t)((dict_end - dict_data_ptr) / 8));
#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
          result.dict_float64_values.resize(safe_count);
          std::memcpy(result.dict_float64_values.data(), dict_data_ptr, safe_count * sizeof(double));
          dict_data_ptr += safe_count * 8;
#else
          result.dict_float64_values.reserve(dict_size);
          for (int32_t i = 0; i < safe_count; i++) {
            result.dict_float64_values.push_back(ReadFloat64(dict_data_ptr));
            dict_data_ptr += 8;
          }
#endif
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
    StringInternTable unified_dict_map;
    std::unordered_map<int32_t, int32_t> int32_dict_map;
    std::unordered_map<int64_t, int32_t> int64_dict_map;
    std::unordered_map<uint32_t, int32_t> float32_dict_map;
    std::unordered_map<uint64_t, int32_t> float64_dict_map;

    // ── RLE skip-dense path gate ────────────────────────────────────────────
    // For non-nullable dict columns (max_definition_level == 0) we bypass the
    // O(N) dict_indices dense array entirely.  C++ resolves one dict lookup
    // per run and accumulates directly into the rle_* output vectors.  The
    // Cython layer then calls from_rle_builder with no further scan needed.
    //
    // Nullable columns (max_definition_level > 0) continue to use the existing
    // dict_indices path because nulls are interleaved in the index stream.
    const bool rle_path =
        target_col->max_definition_level == 0 &&
        (byte_array_dict_mode || int32_dict_mode || int64_dict_mode ||
         float32_dict_mode || float64_dict_mode);

    // ── Tier 1A: Pre-reserve output vectors ────────────────────────────
    // total_needed is known from column metadata; reserve once to eliminate
    // incremental reallocation across pages.
    if (total_needed > 0) {
      const size_t tn = static_cast<size_t>(total_needed);
      if (rle_path) {
        // For RLE path: reserve run arrays with a conservative estimate.
        // Highly repetitive columns have O(1) runs; worst case is O(N) runs
        // (all unique values in sequence).  A /16 estimate is very conservative
        // but avoids over-allocating when the column is nearly random.
        const size_t est_runs = std::max<size_t>(1, tn / 16);
        if (int64_dict_mode || int32_dict_mode)
          result.rle_int64_values.reserve(est_runs);
        else if (float64_dict_mode || float32_dict_mode)
          result.rle_float64_values.reserve(est_runs);
        else if (byte_array_dict_mode) {
          result.rle_str_offsets.reserve(est_runs);
          result.rle_str_lens.reserve(est_runs);
          // Rough bytes estimate: assume 8 bytes average string length.
          result.rle_str_arena.reserve(est_runs * 8);
        }
        result.rle_run_lengths.reserve(est_runs);
      } else if (dict_size > 0 && !rle_path &&
                 (int32_dict_mode || int64_dict_mode ||
                  float32_dict_mode || float64_dict_mode) &&
                 target_col->max_definition_level > 0 &&
                 result.ext_int64 == nullptr && result.ext_int32 == nullptr &&
                 result.ext_float64 == nullptr && result.ext_float32 == nullptr &&
                 row_mask == nullptr) {
        // Nullable numeric dict column with no caller-provided dense buffer and no
        // row-mask: pre-allocate packed codes array (zero = null sentinel).
        // byte_array is excluded: mixed dict/plain chunks determine at the first
        // PLAIN page whether to intern (dict_indices) or materialise to dense.
        // When row_mask is active the dict_codes_array would not be filtered, so
        // fall through to the dict_indices path which IS correctly compacted.
        result.dict_codes_array.assign(
            static_cast<size_t>(tn) * result.code_width, 0);
      } else if (dict_size > 0) {
        result.dict_indices.reserve(tn);
      } else if (result.type == "int32") {
        result.int32_values.reserve(tn);
      } else if (result.type == "int64") {
        result.int64_values.reserve(tn);
      } else if (result.type == "float32") {
        result.float32_values.reserve(tn);
      } else if (result.type == "float64") {
        result.float64_values.reserve(tn);
      } else if (result.type == "boolean") {
        result.boolean_values.reserve(tn);
      }
    }

    int32_t page_row_offset = 0;
    std::vector<uint8_t> decoded_row_mask;
    if (row_mask != nullptr) {
      decoded_row_mask.reserve(total_needed > 0 ? (size_t)total_needed : 65536u);
    }

    // ── Tier 3: Parallel page decode ──────────────────────────────────────
    // For non-nullable, non-dict, non-nested, fixed-width columns with more
    // than 2 pages, pre-scan → pre-allocate → parallel decompress + decode.
    // This path is strictly additive: falls back to sequential on any error,
    // or when the column doesn't meet the eligibility criteria.
    //
    // Eligible types:
    //   - dict_size == 0          (no dictionary; no interning, pages independent)
    //   - max_definition_level == 0 (non-nullable; no def-level decode needed)
    //   - max_repetition_level == 0 (non-nested; no rep-level decode needed)
    //   - row_mask == nullptr      (no filtering; post-loop filter stays simple)
    //   - type in {int32,int64,float32,float64} (fixed-width; bulk copy per page)
    //   - page_count > 2           (not worth the overhead for 1-2 pages)

    bool used_parallel_path = false;

    {
      const bool tier3_eligible = (
          dict_size == 0 &&
          target_col->max_definition_level == 0 &&
          target_col->max_repetition_level == 0 &&
          row_mask == nullptr &&
          flba_byte_width == 0 &&
          (result.type == "int32" || result.type == "int64" ||
           result.type == "float32" || result.type == "float64")
      );

      if (tier3_eligible) {
        // Phase 1: Pre-scan (cheap: parses page headers only, no decompression)
        RUGO_TEL_START(_ps_t0);
        std::vector<PageTask> page_tasks;
        page_tasks.reserve(64);
        const int32_t prescan_total = PreScanPages(cursor, chunk_limit, nullptr, page_tasks);
        RUGO_TEL_ACCUM(rugo_tel::prescan_s, _ps_t0);

        if (page_tasks.size() > 2 && prescan_total > 0) {
          // Phase 2: Pre-allocate output buffers exactly (no growth during decode)
          // For ext_* paths (Tier 4A), the buffer is pre-allocated by the caller.
          const bool has_ext = (result.ext_int32 != nullptr || result.ext_int64 != nullptr ||
                                result.ext_float32 != nullptr || result.ext_float64 != nullptr);
          if (!has_ext) {
            const size_t tn = (size_t)prescan_total;
            if      (result.type == "int32")   result.int32_values.resize(tn);
            else if (result.type == "int64")   result.int64_values.resize(tn);
            else if (result.type == "float32") result.float32_values.resize(tn);
            else if (result.type == "float64") result.float64_values.resize(tn);
          }

          // Phase 3: Dispatch pages to module-level thread pool.
          // Module-level pool is created once and reused across all calls —
          // eliminates per-column thread creation overhead.
          // All hardware threads used; no artificial cap.
          PageDecodePool& pool = rugo_pool::get_page_decode_pool();

          std::atomic<bool>    any_error{false};
          std::atomic<int32_t> decoded_count{0};

          // Per-batch completion tracking.
          // Shared pool requires batch-scoped sync — cannot use pool's global wait
          // as multiple callers may submit concurrently to the same pool.
          int32_t active_task_count = 0;
          for (const auto& pt : page_tasks) { if (!pt.skip_page) ++active_task_count; }
          std::atomic<int32_t> batch_remaining{active_task_count};
          std::mutex batch_mutex;
          std::condition_variable batch_cv;

          // Snapshot read-only state for thread-safe lambda capture
          const std::string col_type  = result.type;
          const int32_t     col_codec = target_col->codec;
          int64_t*  xint64   = result.ext_int64;
          double*   xfloat64 = result.ext_float64;
          int32_t*  xint32   = result.ext_int32;
          float*    xfloat32 = result.ext_float32;
          // Pointers to pre-allocated internal vectors (resized above; stable address)
          int32_t* ivec_i32  = result.int32_values.empty()   ? nullptr : result.int32_values.data();
          int64_t* ivec_i64  = result.int64_values.empty()   ? nullptr : result.int64_values.data();
          float*   ivec_f32  = result.float32_values.empty() ? nullptr : result.float32_values.data();
          double*  ivec_f64  = result.float64_values.empty() ? nullptr : result.float64_values.data();

          RUGO_TEL_START(_pp_t0);
          for (const PageTask& ptask : page_tasks) {
            if (ptask.skip_page) { ++result.pages_skipped; continue; }

            pool.push_task([ptask, col_type, col_codec,
                            xint64, xfloat64, xint32, xfloat32,
                            ivec_i32, ivec_i64, ivec_f32, ivec_f64,
                            &any_error, &decoded_count,
                            &batch_remaining, &batch_cv]() {
              if (!any_error.load(std::memory_order_relaxed)) {

              // Per-task decompression buffer (each task owns this — no sharing)
              std::vector<uint8_t> decomp_buf;
              const uint8_t* dp;
              size_t         ds;

              if (col_codec == 0) {
                dp = ptask.compressed_data;
                ds = ptask.compressed_size;
              } else {
                try {
                  auto codec = rugo::compression::CodecFromInt(col_codec);
                  rugo::compression::DecompressInto(
                      ptask.compressed_data, ptask.compressed_size,
                      ptask.uncompressed_size, codec, decomp_buf);
                  dp = decomp_buf.data();
                  ds = decomp_buf.size();
                } catch (...) {
                  any_error.store(true, std::memory_order_relaxed);
                  if (batch_remaining.fetch_sub(1, std::memory_order_acq_rel) == 1) batch_cv.notify_one();
                  return;
                }
              }

              const int32_t nv  = ptask.num_values;
              const size_t  off = (size_t)ptask.out_offset;
              const uint8_t* dend = dp + ds;

              if (col_type == "int32") {
                int32_t* dst = xint32 ? (xint32 + off) : (ivec_i32 + off);
                if (ptask.encoding == 4) {  // DELTA
                  std::vector<int32_t> tmp;
                  if (DecodeDeltaBinaryPacked(dp, ds, nv, tmp) != nv) {
                    any_error.store(true, std::memory_order_relaxed);
                    if (batch_remaining.fetch_sub(1, std::memory_order_acq_rel) == 1) batch_cv.notify_one();
                    return;
                  }
                  std::copy(tmp.begin(), tmp.end(), dst);
                } else {
                  int32_t safe = std::min(nv, (int32_t)((dend - dp) / 4));
#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
                  std::memcpy(dst, dp, (size_t)safe * sizeof(int32_t));
#else
                  for (int32_t i = 0; i < safe; i++) dst[i] = ReadLE32(dp + i * 4);
#endif
                }
              } else if (col_type == "int64") {
                int64_t* dst = xint64 ? (xint64 + off) : (ivec_i64 + off);
                if (ptask.encoding == 4) {  // DELTA
                  std::vector<int64_t> tmp;
                  if (DecodeDeltaBinaryPacked(dp, ds, nv, tmp) != nv) {
                    any_error.store(true, std::memory_order_relaxed);
                    if (batch_remaining.fetch_sub(1, std::memory_order_acq_rel) == 1) batch_cv.notify_one();
                    return;
                  }
                  std::copy(tmp.begin(), tmp.end(), dst);
                } else {
                  int32_t safe = std::min(nv, (int32_t)((dend - dp) / 8));
#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
                  std::memcpy(dst, dp, (size_t)safe * sizeof(int64_t));
#else
                  for (int32_t i = 0; i < safe; i++) dst[i] = ReadLE64(dp + i * 8);
#endif
                }
              } else if (col_type == "float32") {
                float* dst = xfloat32 ? (xfloat32 + off) : (ivec_f32 + off);
                int32_t safe = std::min(nv, (int32_t)((dend - dp) / 4));
#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
                std::memcpy(dst, dp, (size_t)safe * sizeof(float));
#else
                for (int32_t i = 0; i < safe; i++) dst[i] = ReadFloat32(dp + i * 4);
#endif
              } else if (col_type == "float64") {
                double* dst = xfloat64 ? (xfloat64 + off) : (ivec_f64 + off);
                int32_t safe = std::min(nv, (int32_t)((dend - dp) / 8));
#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
                std::memcpy(dst, dp, (size_t)safe * sizeof(double));
#else
                for (int32_t i = 0; i < safe; i++) dst[i] = ReadFloat64(dp + i * 8);
#endif
              }

              decoded_count.fetch_add(1, std::memory_order_relaxed);
              } // end if !any_error

              // Signal this task complete — notify if last in batch
              if (batch_remaining.fetch_sub(1, std::memory_order_acq_rel) == 1) {
                batch_cv.notify_one();
              }
            });
          }

          // Wait for this batch specifically (not all pool tasks)
          if (active_task_count > 0) {
            std::unique_lock<std::mutex> lock(batch_mutex);
            batch_cv.wait(lock, [&batch_remaining]() {
              return batch_remaining.load(std::memory_order_acquire) == 0;
            });
          }
          RUGO_TEL_ACCUM(rugo_tel::page_parallel_s, _pp_t0);

          if (!any_error.load()) {
            total_collected = prescan_total;
            result.pages_decoded += decoded_count.load();
            if (has_ext) {
              result.ext_written = prescan_total;
            }
            used_parallel_path = true;
          }
          // If any_error: used_parallel_path stays false → fall through to sequential recovery
        }
      }
    }  // end Tier 3 parallel scope

    if (!used_parallel_path) {
    // ── Sequential page loop ──────────────────────────────────────────────
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
      // LIST columns (max_repetition_level > 0) skip this block: the caller
      // supplies a per-logical-row mask, but page_values counts physical slots.
      // Applying a per-slot mask to a per-row buffer reads past its end.
      // Logical-row masking for LIST columns is applied post-loop instead.
      if (row_mask != nullptr && target_col->max_repetition_level == 0) {
        // Tier 1D: Word-at-a-time scan for any selected row in this page.
        const uint8_t* mp = row_mask + page_row_offset;
        const size_t pv = static_cast<size_t>(page_values);
        bool any_selected = false;
        size_t si = 0;
        for (; si + 8 <= pv; si += 8) {
          uint64_t w;
          std::memcpy(&w, mp + si, 8);
          if (w) { any_selected = true; break; }
        }
        if (!any_selected) {
          for (; si < pv; ++si) {
            if (mp[si]) { any_selected = true; break; }
          }
        }
        if (!any_selected) {
          // No selected rows in this page: skip decompression entirely.
          page_row_offset += page_values;
          total_collected += page_values;
          cursor = compressed_data + compressed_size;
          ++result.pages_skipped;
          continue;
        }
        // Tier 1E: Bulk copy mask bytes instead of per-element push_back.
        size_t old_sz = decoded_row_mask.size();
        decoded_row_mask.resize(old_sz + pv);
        std::memcpy(decoded_row_mask.data() + old_sz, mp, pv);
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

        RUGO_TEL_START(_vx_t0);

        if (rle_path) {
          // ── Skip-dense RLE path ──────────────────────────────────────────
          // Decode RLE/bit-packed indices directly into per-page run arrays
          // (no O(N) dense intermediate).  For each run, resolve the dict code
          // to the actual value in C++ and accumulate with page-boundary merge.
          std::vector<int32_t> run_codes, run_counts;
          { RUGO_TEL_START(_rle_t0);
            int32_t decoded = DecodeRLEBitPackedIndicesToRuns(
                data_ptr, data_size, present_count, bit_width, run_codes, run_counts);
            RUGO_TEL_ACCUM(rugo_tel::rle_s, _rle_t0);
            if (decoded != present_count) return result; }

          if (int64_dict_mode) {
            const int32_t dict_sz = (int32_t)result.dict_int64_values.size();
            for (size_t r = 0; r < run_codes.size(); ++r) {
              const int32_t code = run_codes[r];
              if (code < 0 || code >= dict_sz) return result;
              const int64_t val = result.dict_int64_values[code];
              const int32_t cnt = run_counts[r];
              if (!result.rle_int64_values.empty() &&
                  result.rle_int64_values.back() == val) {
                result.rle_run_lengths.back() += cnt;  // page-boundary merge
              } else {
                result.rle_int64_values.push_back(val);
                result.rle_run_lengths.push_back(cnt);
              }
            }
          } else if (int32_dict_mode) {
            // Widen int32 → int64 in C++ to simplify the Cython binding.
            const int32_t dict_sz = (int32_t)result.dict_int32_values.size();
            for (size_t r = 0; r < run_codes.size(); ++r) {
              const int32_t code = run_codes[r];
              if (code < 0 || code >= dict_sz) return result;
              const int64_t val = (int64_t)result.dict_int32_values[code];
              const int32_t cnt = run_counts[r];
              if (!result.rle_int64_values.empty() &&
                  result.rle_int64_values.back() == val) {
                result.rle_run_lengths.back() += cnt;
              } else {
                result.rle_int64_values.push_back(val);
                result.rle_run_lengths.push_back(cnt);
              }
            }
          } else if (float64_dict_mode) {
            const int32_t dict_sz = (int32_t)result.dict_float64_values.size();
            for (size_t r = 0; r < run_codes.size(); ++r) {
              const int32_t code = run_codes[r];
              if (code < 0 || code >= dict_sz) return result;
              const double  val  = result.dict_float64_values[code];
              const int32_t cnt  = run_counts[r];
              if (!result.rle_float64_values.empty() &&
                  result.rle_float64_values.back() == val) {
                result.rle_run_lengths.back() += cnt;
              } else {
                result.rle_float64_values.push_back(val);
                result.rle_run_lengths.push_back(cnt);
              }
            }
          } else if (float32_dict_mode) {
            // Widen float32 → float64 in C++ to simplify the Cython binding.
            const int32_t dict_sz = (int32_t)result.dict_float32_values.size();
            for (size_t r = 0; r < run_codes.size(); ++r) {
              const int32_t code = run_codes[r];
              if (code < 0 || code >= dict_sz) return result;
              const double  val  = (double)result.dict_float32_values[code];
              const int32_t cnt  = run_counts[r];
              if (!result.rle_float64_values.empty() &&
                  result.rle_float64_values.back() == val) {
                result.rle_run_lengths.back() += cnt;
              } else {
                result.rle_float64_values.push_back(val);
                result.rle_run_lengths.push_back(cnt);
              }
            }
          } else if (byte_array_dict_mode) {
            const int32_t dict_sz = (int32_t)result.string_dict_lens.size();
            const uint8_t* str_arena = result.string_dict_arena.data();
            for (size_t r = 0; r < run_codes.size(); ++r) {
              const int32_t code = run_codes[r];
              if (code < 0 || code >= dict_sz) return result;
              const int32_t cnt  = run_counts[r];
              // Page-boundary merge via code comparison (avoids O(len) memcmp).
              if (!result.rle_str_lens.empty() && result.rle_last_code == code) {
                result.rle_run_lengths.back() += cnt;
              } else {
                const uint32_t off = result.string_dict_offsets[code];
                const int32_t  len = result.string_dict_lens[code];
                result.rle_str_offsets.push_back((uint32_t)result.rle_str_arena.size());
                result.rle_str_lens.push_back(len);
                result.rle_str_arena.insert(result.rle_str_arena.end(),
                    str_arena + off, str_arena + off + len);
                result.rle_run_lengths.push_back(cnt);
                result.rle_last_code = code;
              }
            }
          }
          result.rle_total_length += (size_t)present_count;

        } else {
          // ── Dense path: nullable columns or non-RLE-mode dict types ─────
          std::vector<int32_t> indices;
          { RUGO_TEL_START(_rle_t0);
            int32_t decoded = DecodeRLEBitPackedIndicesNoPrefix(
                data_ptr, data_size, present_count, bit_width, indices);
            RUGO_TEL_ACCUM(rugo_tel::rle_s, _rle_t0);
            if (decoded != present_count) { return result; } }

          // ── Scatter packed codes for nullable dict columns ──
          if (!result.dict_codes_array.empty() && page_uses_dictionary) {
            // Scatter sparse indices into full-width packed codes array
            if (indices.size() != (size_t)present_count) return result;
            int32_t max_def = target_col->max_definition_level;
            int32_t code_idx = 0;
            int32_t row_offset = total_collected;

            for (int32_t i = 0; i < page_values && i < (int32_t)def_levels.size(); ++i) {
              if (def_levels[i] == max_def) {
                if (code_idx >= (int32_t)indices.size()) return result;
                int32_t code = indices[code_idx++];
                if (code < 0 || code >= (int32_t)dict_size) return result;
                WritePackedCode(result.dict_codes_array.data(), row_offset + i,
                               code, result.code_width);
              }
              // Null rows already zero-initialized
            }
          }

          // ── Tier 1B helper: batch bounds-check + insert for dict indices ──
          // Hoists the per-element range check into a single min/max scan
          // (auto-vectorizable), then uses bulk insert instead of push_back.
          auto batch_append_dict_indices = [&](const std::vector<int32_t>& idx_vec,
                                               int32_t dict_sz) -> bool {
            if (idx_vec.empty()) return true;
            int32_t lo = idx_vec[0], hi = idx_vec[0];
            for (size_t i = 1; i < idx_vec.size(); ++i) {
              int32_t v = idx_vec[i];
              if (v < lo) lo = v;
              if (v > hi) hi = v;
            }
            if (lo < 0 || hi >= dict_sz) return false;
            result.dict_indices.insert(result.dict_indices.end(),
                                       idx_vec.begin(), idx_vec.end());
            return true;
          };

          // ── Tier 2A helper: validate indices then SIMD gather ──
          // Validates all indices upfront (min/max scan), then uses SIMD gather
          // if available, with scalar fallback.
          auto validate_and_gather_int32 = [&](const std::vector<int32_t>& idx_vec,
                                               std::vector<int32_t>& result_vec) -> bool {
            if (idx_vec.empty()) return true;
            int32_t lo = idx_vec[0], hi = idx_vec[0];
            for (size_t i = 1; i < idx_vec.size(); ++i) {
              int32_t v = idx_vec[i];
              if (v < lo) lo = v;
              if (v > hi) hi = v;
            }
            if (lo < 0 || hi >= (int32_t)result.dict_int32_values.size()) return false;
            parquet_simd::gather_int32(result.dict_int32_values.data(), idx_vec.data(), idx_vec.size(), result_vec);
            return true;
          };

          auto validate_and_gather_int64 = [&](const std::vector<int32_t>& idx_vec,
                                               std::vector<int64_t>& result_vec) -> bool {
            if (idx_vec.empty()) return true;
            int32_t lo = idx_vec[0], hi = idx_vec[0];
            for (size_t i = 1; i < idx_vec.size(); ++i) {
              int32_t v = idx_vec[i];
              if (v < lo) lo = v;
              if (v > hi) hi = v;
            }
            if (lo < 0 || hi >= (int32_t)result.dict_int64_values.size()) return false;
            parquet_simd::gather_int64(result.dict_int64_values.data(), idx_vec.data(), idx_vec.size(), result_vec);
            return true;
          };

          auto validate_and_gather_float32 = [&](const std::vector<int32_t>& idx_vec,
                                                 std::vector<float>& result_vec) -> bool {
            if (idx_vec.empty()) return true;
            int32_t lo = idx_vec[0], hi = idx_vec[0];
            for (size_t i = 1; i < idx_vec.size(); ++i) {
              int32_t v = idx_vec[i];
              if (v < lo) lo = v;
              if (v > hi) hi = v;
            }
            if (lo < 0 || hi >= (int32_t)result.dict_float32_values.size()) return false;
            parquet_simd::gather_float32(result.dict_float32_values.data(), idx_vec.data(), idx_vec.size(), result_vec);
            return true;
          };

          auto validate_and_gather_float64 = [&](const std::vector<int32_t>& idx_vec,
                                                 std::vector<double>& result_vec) -> bool {
            if (idx_vec.empty()) return true;
            int32_t lo = idx_vec[0], hi = idx_vec[0];
            for (size_t i = 1; i < idx_vec.size(); ++i) {
              int32_t v = idx_vec[i];
              if (v < lo) lo = v;
              if (v > hi) hi = v;
            }
            if (lo < 0 || hi >= (int32_t)result.dict_float64_values.size()) return false;
            parquet_simd::gather_float64(result.dict_float64_values.data(), idx_vec.data(), idx_vec.size(), result_vec);
            return true;
          };

          if (result.type == "int32") {
            if (!result.dict_codes_array.empty()) {
              // Codes already scattered into dict_codes_array; dict-only output.
            } else if (int32_dict_mode) {
              if (!batch_append_dict_indices(indices, (int32_t)result.dict_int32_values.size()))
                return result;
            } else {
              if (result.ext_int32) {
                int32_t lo = indices.empty() ? 0 : indices[0];
                int32_t hi = lo;
                for (int32_t idx : indices) {
                  if (idx < lo) lo = idx;
                  if (idx > hi) hi = idx;
                }
                if (lo < 0 || hi >= (int32_t)result.dict_int32_values.size()) return result;
                size_t n = indices.size();
                for (size_t i = 0; i < n; ++i)
                  result.ext_int32[result.ext_written + i] = result.dict_int32_values[indices[i]];
                result.ext_written += n;
              } else {
                if (!validate_and_gather_int32(indices, result.int32_values))
                  return result;
              }
            }
          } else if (result.type == "int64") {
            if (!result.dict_codes_array.empty()) {
              // Codes already scattered into dict_codes_array; dict-only output.
            } else if (int64_dict_mode) {
              if (!batch_append_dict_indices(indices, (int32_t)result.dict_int64_values.size()))
                return result;
            } else {
              if (result.ext_int64) {
                int32_t lo = indices.empty() ? 0 : indices[0];
                int32_t hi = lo;
                for (int32_t idx : indices) {
                  if (idx < lo) lo = idx;
                  if (idx > hi) hi = idx;
                }
                if (lo < 0 || hi >= (int32_t)result.dict_int64_values.size()) return result;
                size_t n = indices.size();
                for (size_t i = 0; i < n; ++i)
                  result.ext_int64[result.ext_written + i] = result.dict_int64_values[indices[i]];
                result.ext_written += n;
              } else {
                if (!validate_and_gather_int64(indices, result.int64_values))
                  return result;
              }
            }
          } else if (result.type == "byte_array") {
            if (result.dict_codes_array.empty()) {
              // Only append to dict_indices if not using packed codes
              if (!batch_append_dict_indices(indices, (int32_t)result.string_dict_lens.size()))
                return result;
            }
          } else if (result.type == "float32") {
            if (!result.dict_codes_array.empty()) {
              // Codes already scattered into dict_codes_array; dict-only output.
            } else if (float32_dict_mode) {
              if (!batch_append_dict_indices(indices, (int32_t)result.dict_float32_values.size()))
                return result;
            } else {
              if (result.ext_float32) {
                int32_t lo = indices.empty() ? 0 : indices[0];
                int32_t hi = lo;
                for (int32_t idx : indices) {
                  if (idx < lo) lo = idx;
                  if (idx > hi) hi = idx;
                }
                if (lo < 0 || hi >= (int32_t)result.dict_float32_values.size()) return result;
                size_t n = indices.size();
                for (size_t i = 0; i < n; ++i)
                  result.ext_float32[result.ext_written + i] = result.dict_float32_values[indices[i]];
                result.ext_written += n;
              } else {
                if (!validate_and_gather_float32(indices, result.float32_values))
                  return result;
              }
            }
          } else if (result.type == "float64") {
            if (!result.dict_codes_array.empty()) {
              // Codes already scattered into dict_codes_array; dict-only output.
            } else if (float64_dict_mode) {
              if (!batch_append_dict_indices(indices, (int32_t)result.dict_float64_values.size()))
                return result;
            } else {
              if (result.ext_float64) {
                int32_t lo = indices.empty() ? 0 : indices[0];
                int32_t hi = lo;
                for (int32_t idx : indices) {
                  if (idx < lo) lo = idx;
                  if (idx > hi) hi = idx;
                }
                if (lo < 0 || hi >= (int32_t)result.dict_float64_values.size()) return result;
                size_t n = indices.size();
                for (size_t i = 0; i < n; ++i)
                  result.ext_float64[result.ext_written + i] = result.dict_float64_values[indices[i]];
                result.ext_written += n;
              } else {
                if (!validate_and_gather_float64(indices, result.float64_values))
                  return result;
              }
            }
          }
        }  // end dense path

        RUGO_TEL_ACCUM(rugo_tel::val_expand_s, _vx_t0);

      } else {
        // PLAIN or DELTA encoding
        int32_t page_encoding = page_header.encoding;

        // Place a page's worth of dict codes (one per present value, in order)
        // into the active code representation.  When dict_codes_array is in use
        // (nullable numeric dict columns), codes must be written at their DENSE
        // row position (def-level aware) exactly like the dict-page scatter —
        // NOT appended to dict_indices.  Appending to dict_indices here while the
        // serializer prefers the (then-incomplete) dict_codes_array silently
        // dropped PLAIN-fallback rows in mixed dict+PLAIN chunks.
        const int32_t _pd_max_def = target_col->max_definition_level;
        const int32_t _pd_row_off  = total_collected;
        auto place_plain_dict_codes = [&](const std::vector<int32_t>& codes) {
          if (!result.dict_codes_array.empty()) {
            int32_t pc = 0;
            for (int32_t i = 0; i < page_values && i < (int32_t)def_levels.size(); ++i) {
              if (def_levels[i] == _pd_max_def) {
                if (pc >= (int32_t)codes.size()) break;
                WritePackedCode(result.dict_codes_array.data(),
                                (size_t)(_pd_row_off + i), codes[pc++],
                                result.code_width);
              }
            }
          } else {
            result.dict_indices.insert(result.dict_indices.end(),
                                       codes.begin(), codes.end());
          }
        };

        // ── Mixed-encoding transition ──────────────────────────────────────
        // When a column starts with dict-encoded pages and then switches to
        // PLAIN/DELTA pages mid-chunk, the rle_path has already accumulated
        // runs into rle_*_values / rle_run_lengths. The PLAIN branch below
        // writes into dict_indices (or *_values), so materialize the rle_*
        // accumulation into the same dense form before continuing.
        if (rle_path && !result.rle_run_lengths.empty()) {
          if (byte_array_dict_mode && !result.rle_str_lens.empty()) {
            if (unified_dict_map.empty()) {
              SeedDictionaryMapFromArena(
                  unified_dict_map,
                  result.string_dict_arena,
                  result.string_dict_offsets,
                  result.string_dict_lens);
            }
            const size_t n_runs = result.rle_run_lengths.size();
            result.dict_indices.reserve(result.dict_indices.size() +
                                        result.rle_total_length);
            for (size_t r = 0; r < n_runs; ++r) {
              const uint32_t off = result.rle_str_offsets[r];
              const int32_t  len = result.rle_str_lens[r];
              const int32_t  cnt = result.rle_run_lengths[r];
              const int32_t code = InternByteArrayToDictionary(
                  reinterpret_cast<const char*>(result.rle_str_arena.data() + off),
                  len,
                  unified_dict_map,
                  result.string_dict_arena,
                  result.string_dict_offsets,
                  result.string_dict_lens);
              for (int32_t j = 0; j < cnt; ++j) result.dict_indices.push_back(code);
            }
            result.rle_str_arena.clear();
            result.rle_str_offsets.clear();
            result.rle_str_lens.clear();
          } else if (int64_dict_mode && !result.rle_int64_values.empty() &&
                     result.rle_total_length > 0 &&
                     (static_cast<double>(result.dict_int64_values.size()) /
                      static_cast<double>(result.rle_total_length)) >= 0.8) {
            // WP-1: int64 dict worth-it gate (non-nullable path only; mirrors the
            // byte_array gate below). The dictionary has spilled to PLAIN and is
            // no longer paying for itself (savings < 20%): interning every PLAIN
            // value into an unordered_map dominates decode time on high-cardinality
            // columns. Materialise the RLE runs decoded so far to dense int64
            // values, abandon the dictionary, and let the PLAIN branch below append
            // straight to int64_values for the rest of the chunk.
            //
            // This branch is the rle_path (max_definition_level == 0, non-nullable),
            // so int64_values is row-aligned with no null scatter needed, and the
            // serializer takes the dense int64 path once dict state is cleared.
            // The nullable (dict_codes_array) path is intentionally NOT gated here:
            // abandoning a packed code array would require a def-level-aware gather
            // back to dense, which is out of scope for this change.
            //
            // Standard Parquet dictionary fallback is one-way per column chunk (all
            // dict pages precede all PLAIN pages), so no dict page follows this flip
            // — the same assumption the byte_array gate relies on.
            result.int64_values.reserve(result.int64_values.size() +
                                        result.rle_total_length);
            const size_t n_runs = result.rle_run_lengths.size();
            for (size_t r = 0; r < n_runs; ++r) {
              const int64_t val = result.rle_int64_values[r];
              const int32_t cnt = result.rle_run_lengths[r];
              for (int32_t j = 0; j < cnt; ++j) result.int64_values.push_back(val);
            }
            result.rle_int64_values.clear();
            result.dict_int64_values.clear();
            int64_dict_mode = false;
          } else if ((int32_dict_mode || int64_dict_mode) &&
                     !result.rle_int64_values.empty()) {
            const bool is_i32 = int32_dict_mode;
            if (is_i32) {
              if (int32_dict_map.empty()) {
                SeedPrimitiveDictionaryMap(int32_dict_map, result.dict_int32_values);
              }
            } else {
              if (int64_dict_map.empty()) {
                SeedPrimitiveDictionaryMap(int64_dict_map, result.dict_int64_values);
              }
            }
            const size_t n_runs = result.rle_run_lengths.size();
            result.dict_indices.reserve(result.dict_indices.size() +
                                        result.rle_total_length);
            for (size_t r = 0; r < n_runs; ++r) {
              const int64_t val = result.rle_int64_values[r];
              const int32_t cnt = result.rle_run_lengths[r];
              int32_t code;
              if (is_i32) {
                code = InternPrimitiveToDictionary(
                    static_cast<int32_t>(val), int32_dict_map,
                    result.dict_int32_values);
              } else {
                code = InternPrimitiveToDictionary(
                    val, int64_dict_map, result.dict_int64_values);
              }
              for (int32_t j = 0; j < cnt; ++j) result.dict_indices.push_back(code);
            }
            result.rle_int64_values.clear();
          } else if ((float32_dict_mode || float64_dict_mode) &&
                     !result.rle_float64_values.empty()) {
            // float32_dict_map / float64_dict_map are keyed by the float's
            // raw bit pattern (so NaNs, +0/-0 are interned per-bits-pattern,
            // matching the manual PLAIN-path code further down).
            if (float32_dict_mode) {
              if (float32_dict_map.empty()) {
                float32_dict_map.reserve(result.dict_float32_values.size() * 2 + 1);
                for (size_t i = 0; i < result.dict_float32_values.size(); ++i) {
                  float32_dict_map.emplace(
                      Float32Bits(result.dict_float32_values[i]),
                      static_cast<int32_t>(i));
                }
              }
            } else {
              if (float64_dict_map.empty()) {
                float64_dict_map.reserve(result.dict_float64_values.size() * 2 + 1);
                for (size_t i = 0; i < result.dict_float64_values.size(); ++i) {
                  float64_dict_map.emplace(
                      Float64Bits(result.dict_float64_values[i]),
                      static_cast<int32_t>(i));
                }
              }
            }
            const size_t n_runs = result.rle_run_lengths.size();
            result.dict_indices.reserve(result.dict_indices.size() +
                                        result.rle_total_length);
            for (size_t r = 0; r < n_runs; ++r) {
              const double  val = result.rle_float64_values[r];
              const int32_t cnt = result.rle_run_lengths[r];
              int32_t code;
              if (float32_dict_mode) {
                const float fv = static_cast<float>(val);
                const uint32_t key = Float32Bits(fv);
                auto it = float32_dict_map.find(key);
                if (it != float32_dict_map.end()) {
                  code = it->second;
                } else {
                  code = static_cast<int32_t>(result.dict_float32_values.size());
                  result.dict_float32_values.push_back(fv);
                  float32_dict_map.emplace(key, code);
                }
              } else {
                const uint64_t key = Float64Bits(val);
                auto it = float64_dict_map.find(key);
                if (it != float64_dict_map.end()) {
                  code = it->second;
                } else {
                  code = static_cast<int32_t>(result.dict_float64_values.size());
                  result.dict_float64_values.push_back(val);
                  float64_dict_map.emplace(key, code);
                }
              }
              for (int32_t j = 0; j < cnt; ++j) result.dict_indices.push_back(code);
            }
            result.rle_float64_values.clear();
          }
          result.rle_run_lengths.clear();
          result.rle_total_length = 0;
          result.rle_last_code = -1;
        }
        // ──────────────────────────────────────────────────────────────────

        if (result.type == "int32") {
          if (int32_dict_mode) {
            if (int32_dict_map.empty()) {
              SeedPrimitiveDictionaryMap(int32_dict_map, result.dict_int32_values);
            }
            std::vector<int32_t> _pd_codes;
            _pd_codes.reserve(present_count);
            if (page_encoding == 4) {
              std::vector<int32_t> page_ints;
              int32_t decoded =
                  DecodeDeltaBinaryPacked(data_ptr, data_size, present_count, page_ints);
              if (decoded != present_count) return result;
              for (int32_t value : page_ints)
                _pd_codes.push_back(InternPrimitiveToDictionary(value, int32_dict_map, result.dict_int32_values));
            } else {
              for (int32_t i = 0; i < present_count && data_ptr + 4 <= data_end; i++) {
                int32_t value = ReadLE32(data_ptr);
                data_ptr += 4;
                _pd_codes.push_back(InternPrimitiveToDictionary(value, int32_dict_map, result.dict_int32_values));
              }
            }
            place_plain_dict_codes(_pd_codes);
          } else if (result.ext_int32) {
            if (page_encoding == 4) {
              std::vector<int32_t> page_ints;
              int32_t decoded = DecodeDeltaBinaryPacked(data_ptr, data_size, present_count, page_ints);
              if (decoded != present_count) return result;
              std::copy(page_ints.begin(), page_ints.end(), result.ext_int32 + result.ext_written);
              result.ext_written += (int32_t)page_ints.size();
            } else {
              int32_t safe_count = std::min(present_count, (int32_t)((data_end - data_ptr) / 4));
#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
              std::memcpy(result.ext_int32 + result.ext_written, data_ptr, safe_count * sizeof(int32_t));
#else
              int32_t* edst = result.ext_int32 + result.ext_written;
              for (int32_t i = 0; i < safe_count; i++) {
                *edst++ = ReadLE32(data_ptr + i * 4);
              }
#endif
              data_ptr += safe_count * 4;
              result.ext_written += safe_count;
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
#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
              // LE bulk copy: on-disk int32 LE layout matches in-memory layout.
              int32_t safe_count = std::min(present_count, (int32_t)((data_end - data_ptr) / 4));
              size_t old_sz = result.int32_values.size();
              result.int32_values.resize(old_sz + safe_count);
              std::memcpy(result.int32_values.data() + old_sz, data_ptr, safe_count * sizeof(int32_t));
              data_ptr += safe_count * 4;
#else
              for (int32_t i = 0; i < present_count && data_ptr + 4 <= data_end; i++) {
                result.int32_values.push_back(ReadLE32(data_ptr));
                data_ptr += 4;
              }
#endif
            }
          }
        } else if (result.type == "int64") {
          if (int64_dict_mode) {
            if (int64_dict_map.empty()) {
              SeedPrimitiveDictionaryMap(int64_dict_map, result.dict_int64_values);
            }
            std::vector<int32_t> _pd_codes;
            _pd_codes.reserve(present_count);
            if (page_encoding == 4) {
              std::vector<int64_t> page_ints;
              int32_t decoded =
                  DecodeDeltaBinaryPacked(data_ptr, data_size, present_count, page_ints);
              if (decoded != present_count) return result;
              for (int64_t value : page_ints)
                _pd_codes.push_back(InternPrimitiveToDictionary(value, int64_dict_map, result.dict_int64_values));
            } else if (flba_byte_width > 0) {
              for (int32_t i = 0;
                   i < present_count && data_ptr + flba_byte_width <= data_end;
                   i++) {
                int64_t value = ReadBESignExt(data_ptr, flba_byte_width);
                data_ptr += flba_byte_width;
                _pd_codes.push_back(InternPrimitiveToDictionary(value, int64_dict_map, result.dict_int64_values));
              }
            } else {
              for (int32_t i = 0; i < present_count && data_ptr + 8 <= data_end; i++) {
                int64_t value = ReadLE64(data_ptr);
                data_ptr += 8;
                _pd_codes.push_back(InternPrimitiveToDictionary(value, int64_dict_map, result.dict_int64_values));
              }
            }
            place_plain_dict_codes(_pd_codes);
          } else if (result.ext_int64) {
            if (page_encoding == 4) {
              std::vector<int64_t> page_ints;
              int32_t decoded = DecodeDeltaBinaryPacked(data_ptr, data_size, present_count, page_ints);
              if (decoded != present_count) return result;
              std::copy(page_ints.begin(), page_ints.end(), result.ext_int64 + result.ext_written);
              result.ext_written += (int32_t)page_ints.size();
            } else if (flba_byte_width > 0) {
              int32_t safe_count = std::min(
                  present_count,
                  (int32_t)((data_end - data_ptr) / flba_byte_width));
              int64_t* edst = result.ext_int64 + result.ext_written;
              for (int32_t i = 0; i < safe_count; i++) {
                edst[i] = ReadBESignExt(data_ptr + i * flba_byte_width,
                                        flba_byte_width);
              }
              data_ptr += safe_count * flba_byte_width;
              result.ext_written += safe_count;
            } else {
              int32_t safe_count = std::min(present_count, (int32_t)((data_end - data_ptr) / 8));
#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
              std::memcpy(result.ext_int64 + result.ext_written, data_ptr, safe_count * sizeof(int64_t));
#else
              int64_t* edst = result.ext_int64 + result.ext_written;
              for (int32_t i = 0; i < safe_count; i++) {
                *edst++ = ReadLE64(data_ptr + i * 8);
              }
#endif
              data_ptr += safe_count * 8;
              result.ext_written += safe_count;
            }
          } else {
            if (page_encoding == 4) {
              std::vector<int64_t> page_ints;
              int32_t decoded = DecodeDeltaBinaryPacked(data_ptr, data_size,
                                                         present_count, page_ints);
              if (decoded != present_count) return result;
              result.int64_values.insert(result.int64_values.end(),
                                          page_ints.begin(), page_ints.end());
            } else if (flba_byte_width > 0) {
              int32_t safe_count = std::min(
                  present_count,
                  (int32_t)((data_end - data_ptr) / flba_byte_width));
              size_t old_sz = result.int64_values.size();
              result.int64_values.resize(old_sz + safe_count);
              int64_t* dst = result.int64_values.data() + old_sz;
              for (int32_t i = 0; i < safe_count; i++) {
                dst[i] = ReadBESignExt(data_ptr + i * flba_byte_width,
                                       flba_byte_width);
              }
              data_ptr += safe_count * flba_byte_width;
            } else {
#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
              int32_t safe_count = std::min(present_count, (int32_t)((data_end - data_ptr) / 8));
              size_t old_sz = result.int64_values.size();
              result.int64_values.resize(old_sz + safe_count);
              std::memcpy(result.int64_values.data() + old_sz, data_ptr, safe_count * sizeof(int64_t));
              data_ptr += safe_count * 8;
#else
              for (int32_t i = 0; i < present_count && data_ptr + 8 <= data_end; i++) {
                result.int64_values.push_back(ReadLE64(data_ptr));
                data_ptr += 8;
              }
#endif
            }
          }
        } else if (result.type == "int128") {
          // PLAIN FLBA width 9..16 → int128 (DECIMAL128). Dict pages are rejected at
          // setup and there is no ext_int128 buffer, so only the dense path applies;
          // values are appended compactly (present rows) exactly like the int64 dense
          // FLBA path — the shared null-expansion step scatters them by def-level.
          if (flba_byte_width > 0) {
            int32_t safe_count = std::min(
                present_count,
                (int32_t)((data_end - data_ptr) / flba_byte_width));
            size_t old_sz = result.int128_values.size();
            result.int128_values.resize(old_sz + safe_count);
            __int128* dst = result.int128_values.data() + old_sz;
            for (int32_t i = 0; i < safe_count; i++) {
              dst[i] = ReadBESignExt128(data_ptr + i * flba_byte_width,
                                        flba_byte_width);
            }
            data_ptr += safe_count * flba_byte_width;
          }
        } else if (result.type == "byte_array") {
          // PLAIN fallback page: decide at the first one whether to intern or go dense.
          //
          // dict page -> build/extend dict vector (dict_indices + string_dict_arena).
          // PLAIN page -> if dict_size / rows_so_far < 0.8 (≥20% saving), intern the
          //              plain values into the unified dictionary and keep dict mode.
          //              Otherwise materialise the already-decoded dict rows to
          //              string_values and switch to dense for all remaining pages.
          if (byte_array_dict_mode) {
            const size_t dict_size_now = result.string_dict_lens.size();
            const size_t rows_so_far   = result.dict_indices.size();
            const bool worth_interning =
                rows_so_far == 0 ||
                (static_cast<double>(dict_size_now) / static_cast<double>(rows_so_far)) < 0.8;
            if (!worth_interning) {
              // Savings < 20%: materialise dict rows decoded so far to dense strings,
              // clear dict state and switch to plain mode for all remaining pages.
              result.string_values.reserve(result.dict_indices.size());
              for (int32_t code : result.dict_indices) {
                if (code >= 0 && code < (int32_t)result.string_dict_lens.size()) {
                  const uint32_t off = result.string_dict_offsets[code];
                  const int32_t  len = result.string_dict_lens[code];
                  result.string_values.emplace_back(
                      reinterpret_cast<const char*>(result.string_dict_arena.data()) + off,
                      static_cast<size_t>(len));
                }
              }
              result.dict_indices.clear();
              result.string_dict_arena.clear();
              result.string_dict_offsets.clear();
              result.string_dict_lens.clear();
              byte_array_dict_mode = false;
            } else if (unified_dict_map.empty()) {
              SeedDictionaryMapFromArena(
                  unified_dict_map,
                  result.string_dict_arena,
                  result.string_dict_offsets,
                  result.string_dict_lens);
            }
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
            std::vector<int32_t> _pd_codes;
            _pd_codes.reserve(present_count);
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
              _pd_codes.push_back(code);
            }
            place_plain_dict_codes(_pd_codes);
          } else if (result.ext_float32) {
            int32_t safe_count = std::min(present_count, (int32_t)((data_end - data_ptr) / 4));
#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
            std::memcpy(result.ext_float32 + result.ext_written, data_ptr, safe_count * sizeof(float));
#else
            float* edst = result.ext_float32 + result.ext_written;
            for (int32_t i = 0; i < safe_count; i++) {
              *edst++ = ReadFloat32(data_ptr + i * 4);
            }
#endif
            data_ptr += safe_count * 4;
            result.ext_written += safe_count;
          } else {
#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
            int32_t safe_count = std::min(present_count, (int32_t)((data_end - data_ptr) / 4));
            size_t old_sz = result.float32_values.size();
            result.float32_values.resize(old_sz + safe_count);
            std::memcpy(result.float32_values.data() + old_sz, data_ptr, safe_count * sizeof(float));
            data_ptr += safe_count * 4;
#else
            for (int32_t i = 0; i < present_count && data_ptr + 4 <= data_end; i++) {
              result.float32_values.push_back(ReadFloat32(data_ptr));
              data_ptr += 4;
            }
#endif
          }
        } else if (result.type == "float64") {
          if (float64_dict_mode) {
            if (float64_dict_map.empty()) {
              float64_dict_map.reserve(result.dict_float64_values.size() * 2 + 1);
              for (size_t i = 0; i < result.dict_float64_values.size(); ++i) {
                float64_dict_map.emplace(Float64Bits(result.dict_float64_values[i]), static_cast<int32_t>(i));
              }
            }
            std::vector<int32_t> _pd_codes;
            _pd_codes.reserve(present_count);
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
              _pd_codes.push_back(code);
            }
            place_plain_dict_codes(_pd_codes);
          } else if (result.ext_float64) {
            int32_t safe_count = std::min(present_count, (int32_t)((data_end - data_ptr) / 8));
#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
            std::memcpy(result.ext_float64 + result.ext_written, data_ptr, safe_count * sizeof(double));
#else
            double* edst = result.ext_float64 + result.ext_written;
            for (int32_t i = 0; i < safe_count; i++) {
              *edst++ = ReadFloat64(data_ptr + i * 8);
            }
#endif
            data_ptr += safe_count * 8;
            result.ext_written += safe_count;
          } else {
#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
            int32_t safe_count = std::min(present_count, (int32_t)((data_end - data_ptr) / 8));
            size_t old_sz = result.float64_values.size();
            result.float64_values.resize(old_sz + safe_count);
            std::memcpy(result.float64_values.data() + old_sz, data_ptr, safe_count * sizeof(double));
            data_ptr += safe_count * 8;
#else
            for (int32_t i = 0; i < present_count && data_ptr + 8 <= data_end; i++) {
              result.float64_values.push_back(ReadFloat64(data_ptr));
              data_ptr += 8;
            }
#endif
          }
        }
      }

      total_collected += page_values;
      cursor = compressed_data + compressed_size;
    }  // end page loop
    }  // end if (!used_parallel_path) [sequential fallback]

    const int32_t total_rows_all_pages = total_collected;

    // ── Post-loop row-mask filter ──────────────────────────────────────────
    // If a row_mask was supplied, filter all accumulated output vectors down
    // to only the K selected rows.  Pages with zero selections were already
    // skipped above; here we handle partial selections within decoded pages.
    RUGO_TEL_START(_mf_t0);
    if (row_mask != nullptr && !decoded_row_mask.empty()) {
      // ── RLE → dense materialisation ──────────────────────────────────────
      // The row_mask filter compacts dense vectors; rle_*_values are run-
      // length encoded and would slip through unfiltered, leaving a column
      // with rle_total_length entries while the engine sees num_rows = K.
      // Expand rle runs into the type's natural dense vector first; the
      // compact pass below then filters them like any other dense vector.
      if (!result.rle_run_lengths.empty()) {
        const size_t total = result.rle_total_length;
        if (!result.rle_int64_values.empty()) {
          if (result.type == "int32") {
            result.int32_values.resize(total);
            int32_t* out = result.int32_values.data();
            size_t off = 0;
            for (size_t r = 0; r < result.rle_run_lengths.size(); ++r) {
              const int32_t v = static_cast<int32_t>(result.rle_int64_values[r]);
              const int32_t cnt = result.rle_run_lengths[r];
              for (int32_t j = 0; j < cnt; ++j) out[off + j] = v;
              off += cnt;
            }
          } else {
            result.int64_values.resize(total);
            int64_t* out = result.int64_values.data();
            size_t off = 0;
            for (size_t r = 0; r < result.rle_run_lengths.size(); ++r) {
              const int64_t v = result.rle_int64_values[r];
              const int32_t cnt = result.rle_run_lengths[r];
              for (int32_t j = 0; j < cnt; ++j) out[off + j] = v;
              off += cnt;
            }
          }
          result.rle_int64_values.clear();
        } else if (!result.rle_float64_values.empty()) {
          if (result.type == "float32") {
            result.float32_values.resize(total);
            float* out = result.float32_values.data();
            size_t off = 0;
            for (size_t r = 0; r < result.rle_run_lengths.size(); ++r) {
              const float v = static_cast<float>(result.rle_float64_values[r]);
              const int32_t cnt = result.rle_run_lengths[r];
              for (int32_t j = 0; j < cnt; ++j) out[off + j] = v;
              off += cnt;
            }
          } else {
            result.float64_values.resize(total);
            double* out = result.float64_values.data();
            size_t off = 0;
            for (size_t r = 0; r < result.rle_run_lengths.size(); ++r) {
              const double v = result.rle_float64_values[r];
              const int32_t cnt = result.rle_run_lengths[r];
              for (int32_t j = 0; j < cnt; ++j) out[off + j] = v;
              off += cnt;
            }
          }
          result.rle_float64_values.clear();
        } else if (!result.rle_str_lens.empty()) {
          // Materialise into the unified byte_array dict form: append run
          // strings into string_dict_arena (interning per run) and emit one
          // dict_indices entry per logical row.
          if (unified_dict_map.empty()) {
            SeedDictionaryMapFromArena(
                unified_dict_map,
                result.string_dict_arena,
                result.string_dict_offsets,
                result.string_dict_lens);
          }
          result.dict_indices.reserve(result.dict_indices.size() + total);
          for (size_t r = 0; r < result.rle_run_lengths.size(); ++r) {
            const uint32_t off = result.rle_str_offsets[r];
            const int32_t  len = result.rle_str_lens[r];
            const int32_t  cnt = result.rle_run_lengths[r];
            const int32_t  code = InternByteArrayToDictionary(
                reinterpret_cast<const char*>(result.rle_str_arena.data() + off),
                len,
                unified_dict_map,
                result.string_dict_arena,
                result.string_dict_offsets,
                result.string_dict_lens);
            for (int32_t j = 0; j < cnt; ++j) result.dict_indices.push_back(code);
          }
          result.rle_str_arena.clear();
          result.rle_str_offsets.clear();
          result.rle_str_lens.clear();
        }
        result.rle_run_lengths.clear();
        result.rle_total_length = 0;
        result.rle_last_code = -1;
      }

      const int32_t total_decoded = (int32_t)decoded_row_mask.size();
      int32_t K = 0;
      for (uint8_t m : decoded_row_mask) K += (int32_t)m;

      if (K < total_decoded) {
        const bool has_nulls =
            (target_col->max_definition_level > 0 && !all_def_levels.empty());

        if (!has_nulls) {
          // Non-nullable: one value per row — use SIMD-accelerated compaction.
          const size_t mask_len = std::min((size_t)total_decoded, decoded_row_mask.size());

          // Filter int32_values using SIMD compact
          if (!result.int32_values.empty()) {
            std::vector<int32_t> o; o.reserve(K);
            parquet_simd::compact_int32(result.int32_values.data(), decoded_row_mask.data(),
                                        std::min((size_t)result.int32_values.size(), mask_len), o);
            result.int32_values = std::move(o);
          }

          // Filter int64_values using SIMD compact
          if (!result.int64_values.empty()) {
            std::vector<int64_t> o; o.reserve(K);
            parquet_simd::compact_int64(result.int64_values.data(), decoded_row_mask.data(),
                                        std::min((size_t)result.int64_values.size(), mask_len), o);
            result.int64_values = std::move(o);
          }

          // Filter float32_values using SIMD compact
          if (!result.float32_values.empty()) {
            std::vector<float> o; o.reserve(K);
            parquet_simd::compact_float32(result.float32_values.data(), decoded_row_mask.data(),
                                          std::min((size_t)result.float32_values.size(), mask_len), o);
            result.float32_values = std::move(o);
          }

          // Filter float64_values using SIMD compact
          if (!result.float64_values.empty()) {
            std::vector<double> o; o.reserve(K);
            parquet_simd::compact_float64(result.float64_values.data(), decoded_row_mask.data(),
                                          std::min((size_t)result.float64_values.size(), mask_len), o);
            result.float64_values = std::move(o);
          }

          // Filter dict_indices using SIMD compact
          if (!result.dict_indices.empty()) {
            std::vector<int32_t> o; o.reserve(K);
            parquet_simd::compact_int32(result.dict_indices.data(), decoded_row_mask.data(),
                                        std::min((size_t)result.dict_indices.size(), mask_len), o);
            result.dict_indices = std::move(o);
          }

          // Filter boolean_values using scalar compact (treat uint8_t as int32_t via memcpy)
          if (!result.boolean_values.empty()) {
            std::vector<uint8_t> o; o.reserve(K);
            for (size_t i = 0; i < std::min((size_t)result.boolean_values.size(), mask_len); ++i)
              if (decoded_row_mask[i]) o.push_back(result.boolean_values[i]);
            result.boolean_values = std::move(o);
          }

          // Filter string_values (plain byte_array — high-cardinality columns that
          // fell out of dictionary encoding into dense strings). Without this they
          // slip through unfiltered, leaving the column at total_decoded length
          // while the engine sees num_rows = K.
          if (!result.string_values.empty()) {
            std::vector<std::string> o; o.reserve(K);
            for (size_t i = 0; i < std::min((size_t)result.string_values.size(), mask_len); ++i)
              if (decoded_row_mask[i]) o.push_back(std::move(result.string_values[i]));
            result.string_values = std::move(o);
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
          const bool have_str   = !result.string_values.empty();

          std::vector<int32_t> o_i32;
          std::vector<int64_t> o_i64;
          std::vector<float>   o_f32;
          std::vector<double>  o_f64;
          std::vector<int32_t> o_dict;
          std::vector<uint8_t> o_bool;
          std::vector<std::string> o_str;

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
                if (have_str  && val_idx < (int32_t)result.string_values.size())
                  o_str.push_back(std::move(result.string_values[val_idx]));
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
          if (have_str)  result.string_values  = std::move(o_str);
        }

        // Filter def_levels to selected rows (valid_bits is built from this below).
        if (!all_def_levels.empty()) {
          std::vector<int32_t> od; od.reserve(K);
          const size_t mask_len = std::min((size_t)total_decoded, decoded_row_mask.size());
          parquet_simd::compact_int32(all_def_levels.data(), decoded_row_mask.data(),
                                      std::min((size_t)all_def_levels.size(), mask_len), od);
          all_def_levels = std::move(od);
        }
      }
      total_collected = K;
    } else if (row_mask != nullptr && target_col->max_repetition_level > 0) {
      // LIST column: input row_mask is per-logical-row; apply by walking rep_levels.
      // rep_level == 0 marks the start of each new logical row.
      const int32_t max_def_lvl = target_col->max_definition_level;

      std::vector<int32_t> new_rep, new_def;
      std::vector<std::string> new_strings;
      std::vector<int32_t> new_dict_indices;
      const bool use_strings = !result.string_values.empty();
      const bool use_dicts   = !result.dict_indices.empty();

      int32_t logical_row = -1;
      bool     selected   = false;
      int32_t  value_idx  = 0;
      int32_t  out_rows   = 0;

      for (size_t i = 0; i < all_rep_levels.size(); ++i) {
        if (all_rep_levels[i] == 0) {
          ++logical_row;
          selected = (row_mask[logical_row] != 0);
          if (selected) ++out_rows;
        }
        const bool is_value = (!all_def_levels.empty() &&
                               i < all_def_levels.size() &&
                               all_def_levels[i] == max_def_lvl);
        if (selected) {
          new_rep.push_back(all_rep_levels[i]);
          if (i < all_def_levels.size())
            new_def.push_back(all_def_levels[i]);
          if (is_value) {
            if (use_strings && value_idx < (int32_t)result.string_values.size())
              new_strings.push_back(std::move(result.string_values[value_idx]));
            if (use_dicts && value_idx < (int32_t)result.dict_indices.size())
              new_dict_indices.push_back(result.dict_indices[value_idx]);
          }
        }
        if (is_value) ++value_idx;
      }

      all_rep_levels = std::move(new_rep);
      all_def_levels = std::move(new_def);
      if (use_strings) result.string_values = std::move(new_strings);
      if (use_dicts)   result.dict_indices   = std::move(new_dict_indices);
      total_collected = out_rows;
    }
    // ── End post-loop row-mask filter ──────────────────────────────────────
    RUGO_TEL_ACCUM(rugo_tel::mask_filter_s, _mf_t0);

    result.num_rows = total_collected;

    // dict_codes_array (nullable path) was allocated and packed with the initial code_width
    // set at dict-page decode time.  Updating code_width here when dict_codes_array is
    // non-empty would make the serialiser write codes_len = num_rows * new_cw, which
    // overreads the num_rows * old_cw buffer when new_cw > old_cw (buffer overread →
    // garbage codes in IPC → out-of-bounds offsets access → crash in materialize).
    // Only recalculate for the dict_indices path (rle_path, plain pages, or non-nullable),
    // where the final dict size is authoritative and no pre-packed array exists.
    if (result.dict_codes_array.empty()) {
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

    // Build validity bitmap from accumulated definition levels (Tier 2D: SIMD-accelerated).
    { RUGO_TEL_START(_vb_t0);
    if (!all_def_levels.empty()) {
      int32_t total_rows = (int32_t)all_def_levels.size();
      int32_t max_def = target_col->max_definition_level;
      // Use SIMD-accelerated bitmap building (AVX2: 8 rows/iteration, scalar fallback)
      parquet_simd::build_validity_bitmap(all_def_levels.data(), total_rows, max_def, result.valid_bits);
    }
    RUGO_TEL_ACCUM(rugo_tel::validity_bmp_s, _vb_t0); }

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
