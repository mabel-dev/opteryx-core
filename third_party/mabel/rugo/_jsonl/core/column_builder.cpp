#include "column_builder.hpp"
#include "fast_parsers.hpp"

#include <algorithm>
#include <cstring>

// LIVE: extract_column() — extracts one column as raw strings (StringColumnResult)
// DEAD: ColumnResult, merge_column(), type parsing functions — legacy implementation
// See ARCHITECTURE.md for current pipeline

namespace rugo::_jsonl {

namespace {

static inline bool key_matches(
    const uint8_t* buf,
    uint32_t       key_start,
    uint32_t       key_width,
    const char*    name,
    size_t         name_len) noexcept
{
    return static_cast<size_t>(key_width) == name_len &&
           std::memcmp(buf + key_start, name, name_len) == 0;
}

static ColumnType infer_numeric_type(
    const uint8_t*                            buffer,
    const std::vector<std::vector<FieldSpan>>& records,
    const char*                               col_ptr,
    size_t                                    col_len)
{
    bool saw_float = false;
    size_t limit   = std::min(records.size(), static_cast<size_t>(64));

    for (size_t row = 0; row < limit; ++row) {
        for (const auto& f : records[row]) {
            if (!key_matches(buffer, f.key_start, f.key_width, col_ptr, col_len))
                continue;
            if (is_null(buffer, f.value_start, f.value_start + f.value_width - 1))
                break;
            if (f.type == static_cast<uint8_t>(ValueType::Integer) ||
                f.type == static_cast<uint8_t>(ValueType::Double))
            {
                int64_t tmp_i = 0;
                if (!fast_parse_int64(buffer, f.value_start, f.value_start + f.value_width - 1, tmp_i)) {
                    double tmp_d = 0.0;
                    if (fast_parse_float64(buffer, f.value_start, f.value_start + f.value_width - 1, tmp_d))
                        saw_float = true;
                }
            }
            break;
        }
    }
    return saw_float ? ColumnType::Float64 : ColumnType::Int64;
}

}  // namespace

StringColumnResult extract_column(
    const uint8_t*                            buffer,
    const std::vector<std::vector<FieldSpan>>& records,
    const std::string&                         column_name,
    OrdinalPredictor&                         predictor)
{
    const size_t num_rows = records.size();
    const size_t col_len  = column_name.size();

    StringColumnResult result;
    result.num_rows = num_rows;

    if (num_rows == 0) {
        return result;
    }

    // Allocate null bitmap (all valid by default)
    const size_t bitmap_bytes = (num_rows + 7) >> 3;
    result.null_bitmap.assign(bitmap_bytes, 0xFF);

    // Infer type from first non-null value
    for (size_t row = 0; row < num_rows; ++row) {
        const auto& record = records[row];
        for (const auto& f : record) {
            if (f.key_width == col_len &&
                std::memcmp(buffer + f.key_start, column_name.data(), col_len) == 0) {
                if (!is_null(buffer, f.value_start, f.value_start + f.value_width - 1)) {
                    uint8_t vt = f.type;
                    if (vt == static_cast<uint8_t>(ValueType::String))
                        result.inferred_type = ColumnType::String;
                    else if (vt == static_cast<uint8_t>(ValueType::Boolean))
                        result.inferred_type = ColumnType::Bool;
                    else if (vt == static_cast<uint8_t>(ValueType::Integer))
                        result.inferred_type = ColumnType::Int64;
                    else if (vt == static_cast<uint8_t>(ValueType::Double))
                        result.inferred_type = ColumnType::Float64;
                    goto infer_done;
                }
                break;
            }
        }
    }
    infer_done:

    // Preallocate estimated string data (rough estimate)
    result.data.reserve(num_rows * 16);
    result.offsets.resize(num_rows);
    result.lengths.resize(num_rows);

    // Get ordinal prediction candidates for this column
    auto candidates = predictor.get_candidates(column_name);
    uint16_t last_seen = candidates.empty() ? 0xFFFF : candidates[0];

    for (size_t row = 0; row < num_rows; ++row) {
        const auto& record = records[row];
        bool found = false;

        // Fast path: try predicted ordinal first
        if (last_seen != 0xFFFF && last_seen < record.size()) {
            const auto& f = record[last_seen];
            if (f.key_width == col_len &&
                std::memcmp(buffer + f.key_start, column_name.data(), col_len) == 0) {
                // Match! Extract value
                if (is_null(buffer, f.value_start, f.value_start + f.value_width - 1)) {
                    result.null_bitmap[row >> 3] &= ~(uint8_t(1u << (row & 7u)));
                } else {
                    result.offsets[row] = static_cast<uint32_t>(result.data.size());
                    result.lengths[row] = f.value_width;
                    result.data.insert(result.data.end(),
                                      buffer + f.value_start,
                                      buffer + f.value_start + f.value_width);
                }
                found = true;
            }
        }

        // Slow path: linear scan (fallback if prediction missed)
        if (!found) {
            for (size_t i = 0; i < record.size(); ++i) {
                const auto& f = record[i];
                if (f.key_width == col_len &&
                    std::memcmp(buffer + f.key_start, column_name.data(), col_len) == 0) {
                    // Match! Update prediction and extract value
                    last_seen = static_cast<uint16_t>(i);
                    predictor.update_history(column_name, last_seen);

                    if (is_null(buffer, f.value_start, f.value_start + f.value_width - 1)) {
                        result.null_bitmap[row >> 3] &= ~(uint8_t(1u << (row & 7u)));
                    } else {
                        result.offsets[row] = static_cast<uint32_t>(result.data.size());
                        result.lengths[row] = f.value_width;
                        result.data.insert(result.data.end(),
                                          buffer + f.value_start,
                                          buffer + f.value_start + f.value_width);
                    }
                    found = true;
                    break;
                }
            }
        }

        // Column not found in this record
        if (!found) {
            result.null_bitmap[row >> 3] &= ~(uint8_t(1u << (row & 7u)));
        }
    }

    return result;
}

void merge_column(ColumnResult& dest, ColumnResult& src) {
    if (src.num_rows == 0)
        return;

    if (dest.num_rows == 0) {
        dest = src;
        return;
    }

    // Type promotion
    if (dest.col_type == ColumnType::Null && src.col_type != ColumnType::Null) {
        dest.col_type = src.col_type;
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

    // Append data
    dest.null_bitmap.insert(dest.null_bitmap.end(),
                            src.null_bitmap.begin(), src.null_bitmap.end());

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
