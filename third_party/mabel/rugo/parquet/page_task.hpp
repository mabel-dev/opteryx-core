#pragma once

#include <cstdint>
#include <cstddef>

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
