#pragma once

#include <cstdint>
#include <cstddef>
#include <vector>
#include <string>
#include "page_task.hpp"

namespace rugo::parquet {

// Context passed to decode_page_values() for each page
struct PageDecodeContext {
  // ===== INPUT: Page Metadata =====
  const PageTask* page;
  size_t page_idx;
  const uint8_t* data_ptr;    // Decompressed or PLAIN encoding data
  size_t data_size;           // Size of data_ptr buffer

  // ===== OUTPUT: Where to write in result buffers =====
  int64_t out_offset;         // Offset in result.int32_values, etc.

  // ===== SHARED STATE (Read-only, safe for concurrent access) =====
  const uint8_t* dict_data;   // Dictionary values (read-only)
  int32_t dict_count;         // Number of dictionary entries
  const uint8_t* row_mask;    // Row selection mask (read-only)
  int32_t row_mask_offset;    // Offset in row_mask for this page

  // ===== COLUMN METADATA =====
  int max_definition_level;   // 0 = non-nullable, >0 = nullable
  int max_repetition_level;   // 0 = non-nested, >0 = nested
  std::string target_type;    // "int32", "int64", "float32", "float64", "byte_array", etc.
  bool is_dictionary_encoded; // true = dict mode, false = plain
  int codec;                  // Compression codec (0 = PLAIN)

  // ===== BOOLEAN FLAGS =====
  bool skip_page = false;     // Skip this page (no selected rows)
};

}  // namespace rugo::parquet
