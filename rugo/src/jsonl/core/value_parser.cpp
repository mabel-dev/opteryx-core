#include "value_parser.hpp"
#include "fast_parsers.hpp"
#include "predicate_literal.hpp"
#include <cstring>
#include <cmath>
#include <stdexcept>

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

const char* json_value_kind_name(uint8_t value_type) {
    switch (static_cast<ValueType>(value_type)) {
        case ValueType::Null:    return "JSON null";
        case ValueType::Boolean: return "JSON boolean";
        case ValueType::Integer:
        case ValueType::Double:  return "JSON number";
        case ValueType::String:  return "JSON string";
        case ValueType::Array:   return "JSON array";
        case ValueType::Object:  return "JSON object";
        default:                 return "unrecognised JSON value";
    }
}

bool literal_fits_json_value(uint8_t value_type, uint8_t kind) {
    switch (static_cast<ValueType>(value_type)) {
        case ValueType::String:  return kind == rugo::LITERAL_STRING;
        case ValueType::Integer:
        case ValueType::Double:  return kind == rugo::LITERAL_INT || kind == rugo::LITERAL_FLOAT;
        case ValueType::Boolean: return kind == rugo::LITERAL_BOOL;
        default:                 return false;   // array / object / unknown: no comparison defined
    }
}

void prepare_predicate(Predicate& pred) {
    for (auto& m : pred.members) prepare_predicate(m);
    if (pred.op >= 6) return;   // IN / NOT IN (members prepared above), IS [NOT] NULL
    const uint8_t* v = reinterpret_cast<const uint8_t*>(pred.value.c_str());
    const uint32_t e = pred.value.empty() ? 0 : static_cast<uint32_t>(pred.value.length() - 1);
    // Parse ONLY as the literal's kind. Sniffing the text instead is what made the
    // string '1' compare as a number.
    switch (pred.kind) {
        case rugo::LITERAL_INT:
            pred.pred_parsed_int = !pred.value.empty() && parse_int64(v, 0, e, pred.pred_int);
            // An int beyond int64 still compares, in the float domain.
            if (!pred.pred_parsed_int)
                pred.pred_parsed_float = !pred.value.empty() && parse_float64(v, 0, e, pred.pred_float);
            break;
        case rugo::LITERAL_FLOAT:
            pred.pred_parsed_float = !pred.value.empty() && parse_float64(v, 0, e, pred.pred_float);
            break;
        case rugo::LITERAL_BOOL:
            pred.pred_parsed_bool = !pred.value.empty() && parse_bool(v, 0, e, pred.pred_bool);
            break;
        default:
            return;   // LITERAL_STRING: compared as bytes, nothing to parse
    }
    if (!pred.pred_parsed_int && !pred.pred_parsed_float && !pred.pred_parsed_bool)
        // The Cython edge renders every int/float/bool literal parseably; this is a
        // backstop for a non-Python caller that set `kind` inconsistently.
        throw std::invalid_argument(
            "predicate on column '" + pred.column + "': literal '" + pred.value +
            "' is marked " + rugo::literal_kind_name(pred.kind) + " but does not parse as one");
}

namespace {
// Scalar comparison (ops 0-5) of a NON-null field value against pred's literal. A
// field whose JSON kind cannot be compared with the literal's kind is an error, not
// a non-match (predicate_literal.hpp): `s = 1` against a JSON string raises.
bool evaluate_scalar(
    const uint8_t* buffer,
    const FieldSpan& value_span,
    const Predicate& pred) {

    if (!literal_fits_json_value(value_span.type, pred.kind))
        throw std::invalid_argument(rugo::literal_mismatch_message(
            pred.column, json_value_kind_name(value_span.type), pred.kind, pred.value));

    const uint32_t fend = value_span.value_start + value_span.value_width - 1;

    switch (static_cast<ValueType>(value_span.type)) {
        case ValueType::Integer:
        case ValueType::Double: {
            // The structural pass tags every number as Integer from its first byte; a
            // value like "3.5" only reveals itself as a float on parse. So try int64
            // first, and compare in the float domain whenever either the field or the
            // literal is fractional (avoids truncating "3.5" to 3).
            int64_t val_int;
            const bool field_is_int = parse_int64(buffer, value_span.value_start, fend, val_int);
            if (field_is_int && pred.pred_parsed_int)
                return apply_op_i64(pred.op, val_int, pred.pred_int);  // exact integer comparison
            double val_float;
            if (field_is_int) {
                val_float = static_cast<double>(val_int);
            } else if (!parse_float64(buffer, value_span.value_start, fend, val_float)) {
                throw std::invalid_argument(
                    "predicate on column '" + pred.column + "': JSON number '" +
                    std::string(reinterpret_cast<const char*>(buffer + value_span.value_start),
                                value_span.value_width) + "' does not parse");
            }
            const double cmp_val = pred.pred_parsed_int ? static_cast<double>(pred.pred_int)
                                                        : pred.pred_float;
            return apply_op_f64(pred.op, val_float, cmp_val);
        }
        case ValueType::Boolean: {
            bool field_bool;
            if (!parse_bool(buffer, value_span.value_start, fend, field_bool))
                throw std::invalid_argument(
                    "predicate on column '" + pred.column + "': JSON boolean does not parse");
            // false=0 < true=1 (SQL boolean ordering) — reuse the int comparator so all
            // six ops (EQ/NE/LT/LE/GT/GE) behave consistently with numeric predicates.
            return apply_op_i64(pred.op, field_bool ? 1 : 0, pred.pred_bool ? 1 : 0);
        }
        default: {  // String (the only other kind literal_fits_json_value admits)
            const std::string val_str = extract_string(buffer, value_span.value_start, fend);
            const int cmp = val_str.compare(pred.value);
            return apply_op_i64(pred.op, cmp, 0);
        }
    }
}
}  // namespace

bool evaluate_predicate(
    const uint8_t* buffer,
    const FieldSpan& value_span,
    const Predicate& pred) {

    const bool value_is_null = is_null(
        buffer, value_span.value_start, value_span.value_start + value_span.value_width - 1);

    switch (pred.op) {
        case 8: return value_is_null;    // IS NULL
        case 9: return !value_is_null;   // IS NOT NULL
        case 7:
            // `x NOT IN ()` asks no comparison, so it accepts every row, nulls included.
            if (pred.members.empty()) return true;
            break;
        default:
            break;
    }

    // NULL op anything is unknown => the row does not pass (SQL semantics) — for the six
    // comparisons and for a non-empty IN / NOT IN alike.
    if (value_is_null) return false;

    if (pred.op == 6) {  // IN: any member's EQ passes (empty list => false)
        for (const auto& m : pred.members)
            if (evaluate_scalar(buffer, value_span, m)) return true;
        return false;
    }
    if (pred.op == 7) {  // NOT IN: every member's NE passes
        for (const auto& m : pred.members)
            if (!evaluate_scalar(buffer, value_span, m)) return false;
        return true;
    }
    return evaluate_scalar(buffer, value_span, pred);
}

}  // namespace rugo::_jsonl
