#ifndef _JSONL_MARKERS_HPP_
#define _JSONL_MARKERS_HPP_

#include <cstdint>
#include <vector>

namespace rugo::_jsonl {

// The 9 marker characters we scan for
enum class MarkerType : uint8_t {
    BRACE_OPEN = 0,    // {
    BRACE_CLOSE = 1,   // }
    BRACKET_OPEN = 2,  // [
    BRACKET_CLOSE = 3, // ]
    COLON = 4,         // :
    COMMA = 5,         // ,
    QUOTE = 6,         // "
    BACKSLASH = 7,     // Reverse solidus
    NEWLINE = 8        // Newline
};

struct MarkerPosition {
    uint32_t position;
    uint8_t marker_type;  // MarkerType enum value

    MarkerPosition() = default;
    MarkerPosition(uint32_t pos, MarkerType type)
        : position(pos), marker_type(static_cast<uint8_t>(type)) {}
};

// Value type classification (from marker and context)
enum class ValueType : uint8_t {
    Null = 0,
    Boolean = 1,
    Integer = 2,
    Double = 3,
    String = 4,
    Array = 5,
    Object = 6,
    Unknown = 7
};

// FieldSpan: location and type of a key-value pair in the source buffer
struct FieldSpan {
    uint32_t key_start;    // First char of unquoted key
    uint32_t key_width;    // Byte width of key
    uint32_t value_start;  // First char of value (after opening quote/brace/bracket)
    uint32_t value_width;  // Byte width of value
    uint8_t type;          // ValueType enum
    uint16_t ordinal;      // Position (key order) within object

    FieldSpan() = default;
    FieldSpan(uint32_t ks, uint32_t kw, uint32_t vs, uint32_t vw, ValueType t, uint16_t ord)
        : key_start(ks), key_width(kw), value_start(vs), value_width(vw),
          type(static_cast<uint8_t>(t)), ordinal(ord) {}

    // Byte length accessors
    inline uint32_t key_length() const { return key_width; }
    inline uint32_t value_length() const { return value_width; }
};

}  // namespace rugo::_jsonl

#endif  // _JSONL_MARKERS_HPP_
