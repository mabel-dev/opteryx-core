#pragma once

#include <cstdint>
#include <cstring>
#include <cmath>
#include <system_error>

#include "fast_float/fast_float.h"

namespace rugo::_jsonl {

// Fast integer parser (no exceptions, pure computation)
// Returns false if input is not a valid integer
inline bool fast_parse_int64(const uint8_t* data, uint32_t start, uint32_t end, int64_t& out) noexcept {
    if (start > end) return false;

    int64_t value = 0;
    int sign = 1;
    uint32_t i = start;

    if (data[i] == '-') {
        sign = -1;
        i++;
    } else if (data[i] == '+') {
        i++;
    }

    if (i > end) return false;

    while (i <= end) {
        uint8_t c = data[i];
        if (c < '0' || c > '9') return false;
        value = value * 10 + (c - '0');
        i++;
    }

    out = sign * value;
    return true;
}

// Fast float parser. Bounded by [start, end] via fast_float::from_chars — must NOT
// use strtod here: the slice buffer is concatenated values with no separators, so
// strtod would over-read past `end` into the next value. The whole slice must be
// consumed (answer.ptr == last) so trailing garbage (e.g. "3.5x") is rejected.
inline bool fast_parse_float64(const uint8_t* data, uint32_t start, uint32_t end, double& out) noexcept {
    if (start > end) return false;

    const char* first = reinterpret_cast<const char*>(data + start);
    const char* last  = reinterpret_cast<const char*>(data + end + 1);

    double value;
    auto answer = fast_float::from_chars(first, last, value);
    if (answer.ec != std::errc() || answer.ptr != last) {
        return false;
    }

    out = value;
    return true;
}

}  // namespace rugo::_jsonl
