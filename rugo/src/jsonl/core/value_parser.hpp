#ifndef _JSONL_VALUE_PARSER_HPP_
#define _JSONL_VALUE_PARSER_HPP_

#include <cstdint>
#include <string>
#include <optional>
#include "markers.hpp"
#include "parse_context.hpp"

namespace rugo::_jsonl {

// Parse value from buffer given FieldSpan bounds
// Returns true if parsing succeeded, false if malformed
bool parse_int64(const uint8_t* buffer, uint32_t start, uint32_t end, int64_t& out);
bool parse_float64(const uint8_t* buffer, uint32_t start, uint32_t end, double& out);
bool parse_bool(const uint8_t* buffer, uint32_t start, uint32_t end, bool& out);

// Extract string value (unescaping not performed; raw bytes between quotes)
std::string extract_string(const uint8_t* buffer, uint32_t start, uint32_t end);

// Check if value is null
bool is_null(const uint8_t* buffer, uint32_t start, uint32_t end);

// Parse bool with out parameter (Cython-friendly)
bool parse_bool_wrapper(const uint8_t* buffer, uint32_t start, uint32_t end, bool& out);
inline bool parse_bool_wrapper(const uint8_t* buffer, uint32_t start, uint32_t end, bool& out) {
    return parse_bool(buffer, start, end, out);
}

// Compare value with a predicate value (as string) using comparison op
bool evaluate_predicate(
    const uint8_t* buffer,
    const FieldSpan& value_span,
    const Predicate& pred
);

}  // namespace rugo::_jsonl

#endif  // _JSONL_VALUE_PARSER_HPP_
