#include "column_builder.hpp"

#include <algorithm>
#include <cstring>

namespace rugo::_jsonl {

namespace {

static inline bool key_matches(
    const uint8_t* buf,
    uint32_t       key_start,
    uint32_t       key_end,
    const char*    name,
    size_t         name_len) noexcept
{
    size_t klen = static_cast<size_t>(key_end - key_start + 1);
    return klen == name_len &&
           std::memcmp(buf + key_start, name, name_len) == 0;
}

// Resolve the dominant numeric type for a column by sampling up to 64 rows.
static ColumnType infer_numeric_type(
    const uint8_t*                            buffer,
    const std::vector<std::vector<FieldSpan>>& records,
    const char*                               col_ptr,
    size_t                                    col_len)
{
    bool saw_float = false;
    bool saw_int   = false;
    size_t limit   = std::min(records.size(), static_cast<size_t>(64));

    for (size_t row = 0; row < limit; ++row) {
        for (const auto& f : records[row]) {
            if (!key_matches(buffer, f.key_start, f.key_end, col_ptr, col_len))
                continue;
            if (is_null(buffer, f.value_start, f.value_end))
                break;
            if (f.type == static_cast<uint8_t>(ValueType::Integer) ||
                f.type == static_cast<uint8_t>(ValueType::Double))
            {
                int64_t tmp_i = 0;
                if (parse_int64(buffer, f.value_start, f.value_end, tmp_i)) {
                    saw_int = true;
                } else {
                    double tmp_d = 0.0;
                    if (parse_float64(buffer, f.value_start, f.value_end, tmp_d))
                        saw_float = true;
                }
            }
            break;
        }
    }
    return saw_float ? ColumnType::Float64 : ColumnType::Int64;
}

}  // namespace

// ---------------------------------------------------------------------------
// extract_column
// ---------------------------------------------------------------------------
ColumnResult extract_column(
    const uint8_t*                            buffer,
    const std::vector<std::vector<FieldSpan>>& records,
    const std::string&                         column_name)
{
    const size_t num_rows = records.size();
    const char*  col_ptr  = column_name.data();
    const size_t col_len  = column_name.size();

    ColumnResult result;
    result.num_rows = num_rows;

    if (num_rows == 0) {
        result.col_type = ColumnType::Null;
        return result;
    }

    // --- Infer dominant type (scan first non-null row) ----------------------
    ColumnType dom = ColumnType::Null;
    for (size_t row = 0; row < num_rows && dom == ColumnType::Null; ++row) {
        for (const auto& f : records[row]) {
            if (!key_matches(buffer, f.key_start, f.key_end, col_ptr, col_len))
                continue;
            if (is_null(buffer, f.value_start, f.value_end))
                break;
            uint8_t vt = f.type;
            if (vt == static_cast<uint8_t>(ValueType::String))         dom = ColumnType::String;
            else if (vt == static_cast<uint8_t>(ValueType::Boolean))   dom = ColumnType::Bool;
            else if (vt == static_cast<uint8_t>(ValueType::Integer) ||
                     vt == static_cast<uint8_t>(ValueType::Double))
                dom = infer_numeric_type(buffer, records, col_ptr, col_len);
            break;
        }
    }

    result.col_type = dom;

    // --- Allocate output buffers --------------------------------------------
    result.null_flags.resize(num_rows, 1);  // default: all valid

    switch (dom) {
    case ColumnType::Int64:
    case ColumnType::Float64:
        result.data.resize(num_rows * 8, 0);
        break;
    case ColumnType::Bool:
        result.data.resize(num_rows, 0);
        break;
    case ColumnType::String:
        result.str_offsets.resize(num_rows, 0);
        result.str_lengths.resize(num_rows, 0);
        result.str_data.reserve(num_rows * 8);
        break;
    case ColumnType::Null:
        break;
    }

    // --- Fill buffers --------------------------------------------------------
    for (size_t row = 0; row < num_rows; ++row) {
        bool found = false;

        for (const auto& f : records[row]) {
            if (!key_matches(buffer, f.key_start, f.key_end, col_ptr, col_len))
                continue;

            found = true;

            if (is_null(buffer, f.value_start, f.value_end)) {
                result.null_flags[row] = 0;
                break;
            }

            switch (dom) {
            case ColumnType::Int64: {
                int64_t v = 0;
                if (parse_int64(buffer, f.value_start, f.value_end, v)) {
                    std::memcpy(result.data.data() + row * 8, &v, 8);
                } else {
                    double dv = 0.0;
                    if (parse_float64(buffer, f.value_start, f.value_end, dv)) {
                        v = static_cast<int64_t>(dv);
                        std::memcpy(result.data.data() + row * 8, &v, 8);
                    } else {
                        result.null_flags[row] = 0;
                    }
                }
                break;
            }
            case ColumnType::Float64: {
                double v = 0.0;
                bool parsed = parse_float64(buffer, f.value_start, f.value_end, v);
                if (!parsed) {
                    int64_t iv = 0;
                    if (parse_int64(buffer, f.value_start, f.value_end, iv)) {
                        v = static_cast<double>(iv);
                        parsed = true;
                    }
                }
                if (parsed) {
                    std::memcpy(result.data.data() + row * 8, &v, 8);
                } else {
                    result.null_flags[row] = 0;
                }
                break;
            }
            case ColumnType::Bool: {
                uint32_t len = f.value_end - f.value_start + 1;
                if (len == 4 && std::memcmp(buffer + f.value_start, "true", 4) == 0)
                    result.data[row] = 1;
                else if (len == 5 && std::memcmp(buffer + f.value_start, "false", 5) != 0)
                    result.null_flags[row] = 0;
                // false → data[row] stays 0
                break;
            }
            case ColumnType::String: {
                uint32_t start = f.value_start;
                uint32_t len   = (f.value_end >= f.value_start)
                                    ? f.value_end - f.value_start + 1
                                    : 0;
                result.str_offsets[row] = static_cast<uint32_t>(result.str_data.size());
                result.str_lengths[row] = len;
                if (len > 0)
                    result.str_data.insert(result.str_data.end(),
                                           buffer + start,
                                           buffer + start + len);
                break;
            }
            case ColumnType::Null:
                break;
            }
            break;  // matched field, move to next row
        }

        if (!found)
            result.null_flags[row] = 0;
    }

    return result;
}

// ---------------------------------------------------------------------------
// merge_column — append src onto dest
// ---------------------------------------------------------------------------
void merge_column(ColumnResult& dest, ColumnResult&& src) {
    if (src.num_rows == 0)
        return;

    if (dest.num_rows == 0) {
        dest = std::move(src);
        return;
    }

    // Type promotion: prefer non-Null type
    if (dest.col_type == ColumnType::Null && src.col_type != ColumnType::Null) {
        dest.col_type = src.col_type;
        // Retroactively allocate dest buffers for existing null rows
        size_t prev = dest.num_rows;
        switch (dest.col_type) {
        case ColumnType::Int64:
        case ColumnType::Float64:
            dest.data.resize(prev * 8, 0);
            break;
        case ColumnType::Bool:
            dest.data.resize(prev, 0);
            break;
        case ColumnType::String:
            dest.str_offsets.resize(prev, 0);
            dest.str_lengths.resize(prev, 0);
            break;
        default: break;
        }
    }

    // Append null_flags
    dest.null_flags.insert(dest.null_flags.end(),
                           src.null_flags.begin(), src.null_flags.end());

    // Append data
    if (dest.col_type == ColumnType::Int64 || dest.col_type == ColumnType::Float64) {
        dest.data.insert(dest.data.end(), src.data.begin(), src.data.end());
    } else if (dest.col_type == ColumnType::Bool) {
        dest.data.insert(dest.data.end(), src.data.begin(), src.data.end());
    } else if (dest.col_type == ColumnType::String) {
        uint32_t offset_base = static_cast<uint32_t>(dest.str_data.size());
        for (size_t i = 0; i < src.num_rows; ++i) {
            dest.str_offsets.push_back(src.str_offsets[i] + offset_base);
            dest.str_lengths.push_back(src.str_lengths[i]);
        }
        dest.str_data.insert(dest.str_data.end(),
                             src.str_data.begin(), src.str_data.end());
    }

    dest.num_rows += src.num_rows;
}

}  // namespace rugo::_jsonl
