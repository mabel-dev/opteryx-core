#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "markers.hpp"
#include "value_parser.hpp"

namespace rugo::_jsonl {

// Output type for Draken column construction
enum class ColumnType : uint8_t {
    Int64   = 0,
    Float64 = 1,
    Bool    = 2,
    String  = 3,
    Null    = 4,  // all-null column
};

// Per-column result from one chunk.  Designed so results from multiple
// chunks can be trivially appended to produce a full-file column.
struct ColumnResult {
    ColumnType col_type = ColumnType::Null;
    size_t     num_rows = 0;

    // One byte per row: 1 = valid, 0 = null
    std::vector<uint8_t> null_flags;

    // Int64 / Float64: 8 bytes per row (little-endian native)
    // Bool:            1 byte per row (0 or 1)
    // String:          unused (see str_* below)
    std::vector<uint8_t> data;

    // String columns: flat buffer + per-row offset/length
    std::vector<uint8_t>  str_data;     // concatenated UTF-8 string bytes
    std::vector<uint32_t> str_offsets;  // start offset in str_data, per row
    std::vector<uint32_t> str_lengths;  // byte count,   per row  (0 = null)
};

// Extract one named column from a single chunk's records.
// buffer must remain valid for the duration of the call.
ColumnResult extract_column(
    const uint8_t*                            buffer,
    const std::vector<std::vector<FieldSpan>>& records,
    const std::string&                         column_name
);

// Append src onto dest (both must have the same col_type, or dest is Null).
void merge_column(ColumnResult& dest, ColumnResult&& src);

}  // namespace rugo::_jsonl
