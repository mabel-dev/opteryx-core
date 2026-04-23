#ifndef _JSONL_INTERPRETER_HPP_
#define _JSONL_INTERPRETER_HPP_

#include <vector>
#include <map>
#include <string>
#include <cstdint>
#include <optional>

#include "markers.hpp"
#include "parse_context.hpp"

namespace rugo::_jsonl {

// Helper for interpreting a single JSON record
class RecordInterpreter {
public:
    // Parse a single record given marker positions and byte range
    // Returns FieldSpans for all key-value pairs found in the record
    std::vector<FieldSpan> parse_record(
        const uint8_t* buffer,
        uint32_t record_start,
        uint32_t record_end,
        const std::vector<MarkerPosition>& markers,
        const std::map<std::string, uint32_t>& marker_index  // [position] -> index in markers
    );

private:
    // Find the closing quote for an opening quote, accounting for escapes
    uint32_t find_closing_quote(
        uint32_t open_quote_pos,
        const std::vector<MarkerPosition>& markers,
        const std::map<std::string, uint32_t>& marker_index,
        uint32_t record_end
    );

    // Classify value type by examining buffer at value_start
    ValueType classify_value_type(
        const uint8_t* buffer,
        uint32_t value_start
    );

    // Skip whitespace forward in buffer
    uint32_t skip_whitespace(const uint8_t* buffer, uint32_t pos, uint32_t limit);

    // Extract key string (unquoted)
    std::string extract_key(const uint8_t* buffer, uint32_t key_start, uint32_t key_end);
};

}  // namespace rugo::_jsonl

#endif  // _JSONL_INTERPRETER_HPP_
