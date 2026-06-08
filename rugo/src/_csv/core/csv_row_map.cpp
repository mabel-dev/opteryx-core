#include "csv_row_map.hpp"

#include <cstring>
#include <stdexcept>

namespace rugo::_csv {

namespace {

inline void append_name_char(std::string& name, uint8_t c) {
    name += static_cast<char>(c);
}

size_t scan_header_row(
    const uint8_t*            data,
    size_t                    length,
    const CsvParseContext&    ctx,
    std::vector<std::string>& column_names_out)
{
    enum class H { START, UNQUOTED, QUOTED, ESCAPE_IN_QUOTED, DQ_PENDING };
    H state = H::START;
    std::string name;

    for (size_t i = 0; i < length; ++i) {
        const uint8_t c = data[i];
        switch (state) {
            case H::START:
                if (c == '"') {
                    state = H::QUOTED;
                } else if (c == ctx.delimiter) {
                    column_names_out.push_back(name); name.clear();
                } else if (c == '\n') {
                    column_names_out.push_back(name); return i + 1;
                } else if (c == '\r') {
                    if (i + 1 < length && data[i + 1] == '\n') {
                        column_names_out.push_back(name); return i + 2;
                    }
                    append_name_char(name, c); state = H::UNQUOTED;
                } else {
                    append_name_char(name, c); state = H::UNQUOTED;
                }
                break;
            case H::UNQUOTED:
                if (c == ctx.delimiter) {
                    column_names_out.push_back(name); name.clear(); state = H::START;
                } else if (c == '\n') {
                    column_names_out.push_back(name); return i + 1;
                } else if (c == '\r') {
                    if (i + 1 < length && data[i + 1] == '\n') {
                        column_names_out.push_back(name); return i + 2;
                    }
                    append_name_char(name, c);
                } else {
                    append_name_char(name, c);
                }
                break;
            case H::QUOTED:
                if (c == '\\')  { state = H::ESCAPE_IN_QUOTED; }
                else if (c == '"') { state = H::DQ_PENDING; }
                else { append_name_char(name, c); }
                break;
            case H::ESCAPE_IN_QUOTED:
                append_name_char(name, c); state = H::QUOTED;
                break;
            case H::DQ_PENDING:
                if (c == '"') { append_name_char(name, '"'); state = H::QUOTED; }
                else if (c == ctx.delimiter) { column_names_out.push_back(name); name.clear(); state = H::START; }
                else if (c == '\n') { column_names_out.push_back(name); return i + 1; }
                else if (c == '\r') {
                    if (i + 1 < length && data[i + 1] == '\n') {
                        column_names_out.push_back(name); return i + 2;
                    }
                    state = H::UNQUOTED; append_name_char(name, c);
                } else {
                    state = H::UNQUOTED; append_name_char(name, c);
                }
                break;
        }
    }
    column_names_out.push_back(name);
    return length;
}

uint32_t count_first_row_cols(const uint8_t* data, size_t length, const CsvParseContext& ctx) {
    if (length == 0) return 0;
    uint32_t cols = 1;
    bool in_quoted = false, escape_next = false, dq_pending = false;
    for (size_t i = 0; i < length; ++i) {
        const uint8_t c = data[i];
        if (escape_next) { escape_next = false; continue; }
        if (dq_pending) {
            if (c == '"')             { dq_pending = false; }
            else if (c == ctx.delimiter) { dq_pending = false; in_quoted = false; ++cols; }
            else if (c == '\n')       { break; }
            else                      { dq_pending = false; }
            continue;
        }
        if (in_quoted) {
            if (c == '\\')  escape_next = true;
            else if (c == '"') dq_pending = true;
        } else {
            if      (c == '"')           in_quoted = true;
            else if (c == ctx.delimiter) ++cols;
            else if (c == '\n')          break;
        }
    }
    return cols;
}

}  // namespace

size_t parse_csv_header(
    const uint8_t*            data,
    size_t                    length,
    const CsvParseContext&    ctx,
    std::vector<std::string>& column_names_out,
    uint32_t&                 num_cols_out)
{
    if (length == 0) { num_cols_out = 0; return 0; }
    if (!ctx.has_header) {
        num_cols_out = count_first_row_cols(data, length, ctx);
        for (uint32_t i = 0; i < num_cols_out; ++i)
            column_names_out.push_back("col_" + std::to_string(i));
        return 0;
    }
    const size_t data_start = scan_header_row(data, length, ctx, column_names_out);
    num_cols_out = static_cast<uint32_t>(column_names_out.size());
    return data_start;
}

}  // namespace rugo::_csv
