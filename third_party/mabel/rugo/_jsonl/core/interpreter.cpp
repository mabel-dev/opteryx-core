#include "interpreter.hpp"
#include "field_span.hpp"
#include <cstring>
#include <cctype>
#include <algorithm>

namespace rugo::_jsonl {

std::vector<FieldSpan> RecordInterpreter::parse_record(
    const uint8_t* buffer,
    uint32_t record_start,
    uint32_t record_end,
    const std::vector<MarkerPosition>& markers,
    const std::map<std::string, uint32_t>& marker_index) {

    std::vector<FieldSpan> fields;

    // Find the opening { of this record
    uint32_t brace_open = record_start;
    while (brace_open < record_end && buffer[brace_open] != '{') {
        brace_open++;
    }

    if (brace_open >= record_end) {
        return fields;  // No valid record found
    }

    uint16_t ordinal = 0;
    // Track the minimum position for the next key's opening quote.
    // This prevents re-consuming closing quotes of previously parsed fields.
    uint32_t next_key_pos = brace_open + 1;

    // Walk through the record looking for key-value pairs
    // We iterate through the markers, looking for quote pairs (keys) followed by colons
    for (size_t i = 0; i < markers.size(); ++i) {
        const MarkerPosition& marker = markers[i];

        // Stop if we've passed the record boundary
        if (marker.position >= record_end) {
            break;
        }

        // Skip if we haven't reached the record start yet
        if (marker.position < record_start) {
            continue;
        }

        // Skip markers belonging to already-processed fields
        if (marker.position < next_key_pos) {
            continue;
        }

        // Look for opening quote (key start)
        if (marker.marker_type != static_cast<uint8_t>(MarkerType::QUOTE)) {
            continue;
        }

        uint32_t key_open = marker.position;

        // Find the closing quote for the key
        uint32_t key_close = find_closing_quote(key_open, markers, marker_index, record_end);
        if (key_close >= record_end) {
            continue;
        }

        // Key is between key_open+1 and key_close-1 (unquoted)
        uint32_t key_start = key_open + 1;
        uint32_t key_end = key_close - 1;

        // Look for the colon after the key
        uint32_t colon_pos = key_close + 1;
        while (colon_pos < record_end && buffer[colon_pos] != ':') {
            colon_pos++;
        }

        if (colon_pos >= record_end) {
            continue;
        }

        // Find the value start (skip whitespace and opening delimiter)
        uint32_t value_start = colon_pos + 1;
        value_start = skip_whitespace(buffer, value_start, record_end);

        if (value_start >= record_end) {
            continue;
        }

        // Determine value type and find value end
        ValueType value_type = classify_value_type(buffer, value_start);
        uint32_t value_end = value_start;

        if (value_type == ValueType::String) {
            // String: value_start is at opening quote; advance past it per FieldSpan spec
            if (buffer[value_start] != '"') {
                continue;
            }
            uint32_t str_open = value_start;
            value_start = str_open + 1;  // First char after opening quote
            value_end = find_closing_quote(str_open, markers, marker_index, record_end);
            if (value_end >= record_end) {
                continue;
            }
            value_end--;  // Inclusive end is the last char before closing quote
        } else if (value_type == ValueType::Array || value_type == ValueType::Object) {
            // Array or Object: find closing ] or }
            char opening = buffer[value_start];
            char closing = (opening == '[') ? ']' : '}';

            uint32_t depth = 1;
            uint32_t pos = value_start + 1;
            while (pos < record_end && depth > 0) {
                if (buffer[pos] == opening) {
                    depth++;
                } else if (buffer[pos] == closing) {
                    depth--;
                } else if (buffer[pos] == '"') {
                    // Skip quoted strings to avoid counting braces inside strings
                    pos++;
                    while (pos < record_end && buffer[pos] != '"') {
                        if (buffer[pos] == '\\') {
                            pos += 2;  // Skip escaped char
                        } else {
                            pos++;
                        }
                    }
                }
                pos++;
            }

            if (depth != 0) {
                continue;  // Unbalanced brackets/braces
            }

            value_end = pos - 2;  // Inclusive end is before closing bracket/brace
        } else {
            // Null, Boolean, Number: find next comma or closing brace
            uint32_t pos = value_start;
            while (pos < record_end && buffer[pos] != ',' && buffer[pos] != '}') {
                pos++;
            }
            value_end = pos - 1;  // Inclusive, last non-whitespace char
            // Trim trailing whitespace
            while (value_end > value_start && std::isspace(buffer[value_end])) {
                value_end--;
            }
        }

        // Create FieldSpan with inclusive ranges
        FieldSpan span(key_start, key_end, value_start, value_end, value_type, ordinal);
        fields.push_back(span);
        ordinal++;

        // Advance next_key_pos past value_end and the trailing separator (comma or })
        // so the next iteration doesn't re-consume quotes inside the value.
        uint32_t after_value = value_end + 1;
        while (after_value < record_end &&
               buffer[after_value] != ',' &&
               buffer[after_value] != '}') {
            after_value++;
        }
        next_key_pos = after_value + 1;
    }

    return fields;
}

uint32_t RecordInterpreter::find_closing_quote(
    uint32_t open_quote_pos,
    const std::vector<MarkerPosition>& markers,
    const std::map<std::string, uint32_t>& marker_index,
    uint32_t record_end) {

    // Find QUOTE markers after open_quote_pos
    for (size_t i = 0; i < markers.size(); ++i) {
        const MarkerPosition& marker = markers[i];

        if (marker.position <= open_quote_pos) {
            continue;
        }

        if (marker.position >= record_end) {
            break;
        }

        if (marker.marker_type != static_cast<uint8_t>(MarkerType::QUOTE)) {
            continue;
        }

        // Check if this quote is escaped (preceded by backslash)
        if (i > 0 && markers[i - 1].marker_type == static_cast<uint8_t>(MarkerType::BACKSLASH) &&
            markers[i - 1].position == marker.position - 1) {
            // This quote is escaped, skip it
            continue;
        }

        // Found unescaped closing quote
        return marker.position;
    }

    return record_end;  // Not found
}

ValueType RecordInterpreter::classify_value_type(
    const uint8_t* buffer,
    uint32_t value_start) {

    if (value_start >= 1000000) {  // Safety check
        return ValueType::Unknown;
    }

    uint8_t ch = buffer[value_start];

    if (ch == '"') {
        return ValueType::String;
    } else if (ch == '{') {
        return ValueType::Object;
    } else if (ch == '[') {
        return ValueType::Array;
    } else if (ch == 't' || ch == 'f') {
        return ValueType::Boolean;
    } else if (ch == 'n') {
        return ValueType::Null;
    } else if (ch == '-' || (ch >= '0' && ch <= '9')) {
        // Could be int or double; classify later
        return ValueType::Integer;  // Default to integer for now
    }

    return ValueType::Unknown;
}

uint32_t RecordInterpreter::skip_whitespace(
    const uint8_t* buffer,
    uint32_t pos,
    uint32_t limit) {

    while (pos < limit && std::isspace(buffer[pos])) {
        pos++;
    }

    return pos;
}

std::string RecordInterpreter::extract_key(
    const uint8_t* buffer,
    uint32_t key_start,
    uint32_t key_end) {

    // key_end is inclusive
    size_t len = key_end - key_start + 1;
    return std::string(reinterpret_cast<const char*>(buffer + key_start), len);
}

}  // namespace rugo::_jsonl
