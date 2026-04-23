#pragma once

#include <cstdint>
#include <cstring>
#include <cmath>

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

// Fast float parser using strtod (reasonably fast for JSON numbers)
inline bool fast_parse_float64(const uint8_t* data, uint32_t start, uint32_t end, double& out) noexcept {
    if (start > end) return false;

    const char* str = reinterpret_cast<const char*>(data + start);
    size_t len = end - start + 1;

    char* endptr;
    double value = std::strtod(str, &endptr);

    // Check if entire range was consumed
    if (endptr == str || (endptr - str) != static_cast<int>(len)) {
        return false;
    }

    out = value;
    return true;
}

}  // namespace rugo::_jsonl
