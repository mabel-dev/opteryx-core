#pragma once

#include <cstdint>
#include <string>
#include <vector>
#include "csv_parse_context.hpp"

namespace rugo::_csv {

// Header parse.
//
// Scans from `data` to the first unquoted '\n', populating column_names_out
// from the field values. Returns the byte offset of the first data row (one
// past the header '\n'). If ctx.has_header == false, synthesises col_0…col_N
// by counting delimiters in the first row and returns 0.
size_t parse_csv_header(
    const uint8_t*            data,
    size_t                    length,
    const CsvParseContext&    ctx,
    std::vector<std::string>& column_names_out,
    uint32_t&                 num_cols_out);

}  // namespace rugo::_csv
