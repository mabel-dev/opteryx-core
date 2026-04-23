#include "value_parser.hpp"
#include <cstring>
#include <cstdlib>
#include <cmath>

namespace rugo::_jsonl {

bool parse_int64(const uint8_t* buffer, uint32_t start, uint32_t end, int64_t& out) {
    if (start > end) {
        return false;
    }

    const char* str = reinterpret_cast<const char*>(buffer + start);
    size_t len = end - start + 1;

    // Use strtoll for parsing
    char* endptr;
    long long value = std::strtoll(str, &endptr, 10);

    // Check if entire string was consumed
    if (endptr == str || (endptr - str) != static_cast<int>(len)) {
        return false;
    }

    out = value;
    return true;
}

bool parse_float64(const uint8_t* buffer, uint32_t start, uint32_t end, double& out) {
    if (start > end) {
        return false;
    }

    const char* str = reinterpret_cast<const char*>(buffer + start);
    size_t len = end - start + 1;

    // Use strtod for parsing
    char* endptr;
    double value = std::strtod(str, &endptr);

    // Check if entire string was consumed
    if (endptr == str || (endptr - str) != static_cast<int>(len)) {
        return false;
    }

    out = value;
    return true;
}

bool parse_bool(const uint8_t* buffer, uint32_t start, uint32_t end, bool& out) {
    if (start > end) {
        return false;
    }

    size_t len = end - start + 1;
    const char* str = reinterpret_cast<const char*>(buffer + start);

    if (len == 4 && std::strncmp(str, "true", 4) == 0) {
        out = true;
        return true;
    }

    if (len == 5 && std::strncmp(str, "false", 5) == 0) {
        out = false;
        return true;
    }

    return false;
}

std::string extract_string(const uint8_t* buffer, uint32_t start, uint32_t end) {
    // String value is between quotes; this returns raw bytes
    size_t len = end - start + 1;
    return std::string(reinterpret_cast<const char*>(buffer + start), len);
}

bool is_null(const uint8_t* buffer, uint32_t start, uint32_t end) {
    size_t len = end - start + 1;
    return (len == 4 && std::strncmp(reinterpret_cast<const char*>(buffer + start), "null", 4) == 0);
}

bool evaluate_predicate(
    const uint8_t* buffer,
    const FieldSpan& value_span,
    const Predicate& pred) {

    // Handle NULL values
    if (is_null(buffer, value_span.value_start, value_span.value_start + value_span.value_width - 1)) {
        // NULL comparisons: NULL op anything = false (SQL semantics)
        // except NULL != anything might be true in some systems, but we'll use SQL
        return false;
    }

    // Parse predicate value
    int64_t pred_int = 0;
    double pred_float = 0.0;
    bool pred_parsed_int = false;
    bool pred_parsed_float = false;

    // Try to parse predicate value as number
    pred_parsed_int = parse_int64(
        reinterpret_cast<const uint8_t*>(pred.value.c_str()),
        0,
        pred.value.length() - 1,
        pred_int
    );

    if (!pred_parsed_int) {
        pred_parsed_float = parse_float64(
            reinterpret_cast<const uint8_t*>(pred.value.c_str()),
            0,
            pred.value.length() - 1,
            pred_float
        );
    }

    // Parse field value based on type
    if (value_span.type == static_cast<uint8_t>(ValueType::Integer)) {
        int64_t val_int;
        if (!parse_int64(buffer, value_span.value_start, value_span.value_start + value_span.value_width - 1, val_int)) {
            return false;
        }

        int64_t cmp_val = pred_parsed_int ? pred_int : static_cast<int64_t>(pred_float);

        switch (pred.op) {
            case 0:  // EQ
                return val_int == cmp_val;
            case 1:  // NE
                return val_int != cmp_val;
            case 2:  // LT
                return val_int < cmp_val;
            case 3:  // LE
                return val_int <= cmp_val;
            case 4:  // GT
                return val_int > cmp_val;
            case 5:  // GE
                return val_int >= cmp_val;
        }
    } else if (value_span.type == static_cast<uint8_t>(ValueType::Double)) {
        double val_float;
        if (!parse_float64(buffer, value_span.value_start, value_span.value_start + value_span.value_width - 1, val_float)) {
            return false;
        }

        double cmp_val = pred_parsed_float ? pred_float : static_cast<double>(pred_int);

        switch (pred.op) {
            case 0:
                return std::fabs(val_float - cmp_val) < 1e-9;
            case 1:
                return std::fabs(val_float - cmp_val) >= 1e-9;
            case 2:
                return val_float < cmp_val;
            case 3:
                return val_float <= cmp_val;
            case 4:
                return val_float > cmp_val;
            case 5:
                return val_float >= cmp_val;
        }
    } else if (value_span.type == static_cast<uint8_t>(ValueType::String)) {
        std::string val_str = extract_string(buffer, value_span.value_start, value_span.value_start + value_span.value_width - 1);

        // String comparison
        int cmp = val_str.compare(pred.value);

        switch (pred.op) {
            case 0:
                return cmp == 0;
            case 1:
                return cmp != 0;
            case 2:
                return cmp < 0;
            case 3:
                return cmp <= 0;
            case 4:
                return cmp > 0;
            case 5:
                return cmp >= 0;
        }
    }

    // Type mismatch or unsupported type for predicate
    return false;
}

}  // namespace rugo::_jsonl
