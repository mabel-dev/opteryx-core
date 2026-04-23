#pragma once

#include <cstdint>
#include <string>
#include <vector>
#include <memory>

#include "markers.hpp"
#include "value_parser.hpp"

namespace rugo::_jsonl {

enum class ColumnType : uint8_t {
    Int64   = 0,
    Float64 = 1,
    Bool    = 2,
    String  = 3,
    Null    = 4,
};

// Result owns all buffers. Cython wraps pointers directly into Draken vectors.
struct ColumnResult {
    ColumnType col_type = ColumnType::Null;
    size_t     num_rows = 0;

    // Data buffer (owned). Contents depend on col_type:
    // - Int64/Float64: 8 bytes per row (little-endian native)
    // - Bool: 1 byte per row (0 or 1)
    // - String: unused
    // - Null: empty
    std::vector<uint8_t> data;

    // Null bitmap: byte i/8, bit j&7 = 1 (valid) or 0 (null)
    std::vector<uint8_t> null_bitmap;

    // String-only: flat data + per-row offsets/lengths
    std::vector<uint8_t>  str_data;
    std::vector<uint32_t> str_offsets;
    std::vector<uint32_t> str_lengths;

    // Direct access to buffer pointers for zero-copy Draken wrapping
    uint8_t*  data_ptr()   { return data.empty() ? nullptr : data.data(); }
    uint8_t*  bitmap_ptr() { return null_bitmap.empty() ? nullptr : null_bitmap.data(); }
    uint8_t*  str_ptr()    { return str_data.empty() ? nullptr : str_data.data(); }
};

// Extract one column from records. Fills buffers once; ready for Draken wrapping.
ColumnResult extract_column(
    const uint8_t*                            buffer,
    const std::vector<std::vector<FieldSpan>>& records,
    const std::string&                         column_name
);

// Merge results from multiple chunks (same column, different buffers)
void merge_column(ColumnResult& dest, ColumnResult& src);

}  // namespace rugo::_jsonl
