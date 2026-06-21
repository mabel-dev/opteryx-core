#include "value_parser.hpp"
#include "fast_parsers.hpp"
#include <cstring>
#include <cmath>

namespace rugo::_jsonl {

// LIVE: is_null(), evaluate_predicate() — predicate pushdown.
// LIVE: parse_int64 / parse_float64 / parse_bool / extract_string — used by
//   evaluate_predicate; parse_bool also by the typed column builder.
// The numeric parsers delegate to the bounded fast_parse_* (fast_float-backed).
// No stdlib strtod/strtoll here: strtod has no end bound and over-reads past the
// value on separator-less buffers, and is locale-sensitive. fast_float is the
// vendored parser for this job.

bool parse_int64(const uint8_t* buffer, uint32_t start, uint32_t end, int64_t& out) {
    return fast_parse_int64(buffer, start, end, out);
}

bool parse_float64(const uint8_t* buffer, uint32_t start, uint32_t end, double& out) {
    return fast_parse_float64(buffer, start, end, out);
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

namespace {
// op codes: 0 EQ, 1 NE, 2 LT, 3 LE, 4 GT, 5 GE
inline bool apply_op_i64(uint8_t op, int64_t a, int64_t b) {
    switch (op) {
        case 0: return a == b;
        case 1: return a != b;
        case 2: return a <  b;
        case 3: return a <= b;
        case 4: return a >  b;
        case 5: return a >= b;
    }
    return false;
}
inline bool apply_op_f64(uint8_t op, double a, double b) {
    switch (op) {
        case 0: return std::fabs(a - b) <  1e-9;
        case 1: return std::fabs(a - b) >= 1e-9;
        case 2: return a <  b;
        case 3: return a <= b;
        case 4: return a >  b;
        case 5: return a >= b;
    }
    return false;
}
}  // namespace

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

    // Numeric field. The structural pass tags every number as Integer from its
    // first byte; a value like "3.5" only reveals itself as a float on parse. So
    // try int64 first, and compare in the float domain whenever either the field
    // or the predicate is fractional (avoids truncating "3.5" to 3).
    if (value_span.type == static_cast<uint8_t>(ValueType::Integer) ||
        value_span.type == static_cast<uint8_t>(ValueType::Double)) {

        if (!pred_parsed_int && !pred_parsed_float) {
            return false;  // predicate value is not numeric — no ordering against a number
        }

        const uint32_t fend = value_span.value_start + value_span.value_width - 1;
        int64_t val_int;
        const bool field_is_int = parse_int64(buffer, value_span.value_start, fend, val_int);

        if (field_is_int && pred_parsed_int) {
            return apply_op_i64(pred.op, val_int, pred_int);  // exact integer comparison
        }

        double val_float;
        if (field_is_int) {
            val_float = static_cast<double>(val_int);
        } else if (!parse_float64(buffer, value_span.value_start, fend, val_float)) {
            return false;  // field is neither int nor float
        }
        const double cmp_val = pred_parsed_float ? pred_float : static_cast<double>(pred_int);
        return apply_op_f64(pred.op, val_float, cmp_val);

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
