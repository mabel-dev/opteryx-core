#include "interpreter.hpp"
#include "field_span.hpp"
#include <cstring>
#include <cctype>
#include <algorithm>

namespace rugo::_jsonl {

// Build document map from markers: walk markers sequentially, extract key-value boundaries
std::vector<std::vector<FieldSpan>> build_map(
    const uint8_t* buffer,
    size_t buffer_length,
    const std::vector<MarkerPosition>& markers) {

    std::vector<std::vector<FieldSpan>> records;
    std::vector<FieldSpan> current_record;

    enum State {
        EXPECT_RECORD_START,  // Looking for {
        EXPECT_KEY_QUOTE,     // Inside {, looking for opening "
        IN_KEY,               // Between opening and closing quote of key
        EXPECT_COLON,         // After key, looking for :
        EXPECT_VALUE,         // After :, looking for value start
        IN_STRING_VALUE,      // Inside quoted value
        IN_UNQUOTED_VALUE,    // Parsing number/bool/null
        EXPECT_SEPARATOR,     // After value, looking for , or }
    };

    State state = EXPECT_RECORD_START;
    uint32_t key_start = 0, key_end = 0, key_width = 0;
    uint32_t value_start = 0, value_end = 0, value_width = 0;
    ValueType value_type = ValueType::Unknown;
    uint32_t ordinal = 0;
    size_t marker_idx = 0;

    while (marker_idx < markers.size()) {
        const MarkerPosition& m = markers[marker_idx];
        uint8_t ch = buffer[m.position];

        switch (state) {
        case EXPECT_RECORD_START:
            if (ch == '{') {
                state = EXPECT_KEY_QUOTE;
                ordinal = 0;
            }
            break;

        case EXPECT_KEY_QUOTE:
            if (ch == '"') {
                key_start = m.position + 1;  // First char after opening quote
                state = IN_KEY;
            } else if (ch == '}') {
                // Empty record, record it and continue
                if (!current_record.empty()) {
                    records.push_back(current_record);
                    current_record.clear();
                }
                state = EXPECT_RECORD_START;
            } else if (ch == '\n') {
                // Unexpected newline, reset
                state = EXPECT_RECORD_START;
            }
            break;

        case IN_KEY:
            if (ch == '"') {
                key_end = m.position - 1;  // Last char before closing quote (inclusive)
                key_width = key_end - key_start + 1;
                state = EXPECT_COLON;
            }
            break;

        case EXPECT_COLON:
            if (ch == ':') {
                state = EXPECT_VALUE;
            }
            break;

        case EXPECT_VALUE:
            if (ch == '"') {
                // String value
                value_start = m.position + 1;  // First char after opening quote
                value_type = ValueType::String;
                state = IN_STRING_VALUE;
            } else if (ch == '{') {
                // Object value
                value_start = m.position;
                value_type = ValueType::Object;
                state = IN_UNQUOTED_VALUE;
            } else if (ch == '[') {
                // Array value
                value_start = m.position;
                value_type = ValueType::Array;
                state = IN_UNQUOTED_VALUE;
            } else if (ch == 't' || ch == 'f') {
                // Boolean
                value_start = m.position;
                value_type = ValueType::Boolean;
                state = IN_UNQUOTED_VALUE;
            } else if (ch == 'n') {
                // Null
                value_start = m.position;
                value_type = ValueType::Null;
                state = IN_UNQUOTED_VALUE;
            } else if (ch == '-' || (ch >= '0' && ch <= '9')) {
                // Number
                value_start = m.position;
                value_type = ValueType::Integer;
                state = IN_UNQUOTED_VALUE;
            }
            break;

        case IN_STRING_VALUE:
            if (ch == '"') {
                // Closing quote of string
                value_end = m.position - 1;  // Last char before closing quote (inclusive)
                value_width = value_end - value_start + 1;
                FieldSpan span(key_start, key_width, value_start, value_width, value_type, ordinal);
                current_record.push_back(span);
                ordinal++;
                state = EXPECT_SEPARATOR;
            }
            break;

        case IN_UNQUOTED_VALUE:
            if (ch == ',' || ch == '}' || ch == '\n') {
                // End of unquoted value
                value_end = m.position - 1;  // Char before current (inclusive)
                // Trim trailing whitespace
                while (value_end > value_start && std::isspace(buffer[value_end])) {
                    value_end--;
                }
                value_width = value_end - value_start + 1;
                FieldSpan span(key_start, key_width, value_start, value_width, value_type, ordinal);
                current_record.push_back(span);
                ordinal++;

                if (ch == '}') {
                    state = EXPECT_SEPARATOR;
                } else if (ch == ',') {
                    state = EXPECT_KEY_QUOTE;
                } else if (ch == '\n') {
                    // End of record
                    records.push_back(current_record);
                    current_record.clear();
                    state = EXPECT_RECORD_START;
                }
            }
            break;

        case EXPECT_SEPARATOR:
            if (ch == ',') {
                state = EXPECT_KEY_QUOTE;
            } else if (ch == '}') {
                // End of record
                records.push_back(current_record);
                current_record.clear();
                state = EXPECT_RECORD_START;
            } else if (ch == '\n') {
                // End of record
                records.push_back(current_record);
                current_record.clear();
                state = EXPECT_RECORD_START;
            }
            break;
        }

        marker_idx++;
    }

    // Handle incomplete final record
    if (!current_record.empty()) {
        records.push_back(current_record);
    }

    return records;
}

// Legacy: kept for compatibility, but not used
std::vector<FieldSpan> RecordInterpreter::parse_record(
    const uint8_t* buffer,
    uint32_t record_start,
    uint32_t record_end,
    const std::vector<MarkerPosition>& markers,
    const std::map<std::string, uint32_t>& marker_index) {
    // Deprecated: use build_map() instead
    return {};
}

uint32_t RecordInterpreter::find_closing_quote(
    uint32_t open_quote_pos,
    const std::vector<MarkerPosition>& markers,
    const std::map<std::string, uint32_t>& marker_index,
    uint32_t record_end) {
    return 0;  // Deprecated
}

ValueType RecordInterpreter::classify_value_type(
    const uint8_t* buffer,
    uint32_t value_start) {
    return ValueType::Unknown;  // Deprecated
}

uint32_t RecordInterpreter::skip_whitespace(
    const uint8_t* buffer,
    uint32_t pos,
    uint32_t limit) {
    return pos;  // Deprecated
}

std::string RecordInterpreter::extract_key(
    const uint8_t* buffer,
    uint32_t key_start,
    uint32_t key_end) {
    return "";  // Deprecated
}

}  // namespace rugo::_jsonl
