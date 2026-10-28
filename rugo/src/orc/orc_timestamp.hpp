#pragma once
// rugo/src/orc/orc_timestamp.hpp — ORC TIMESTAMP (DATA seconds + SECONDARY nanos) to epoch
// microseconds (docs/ORC_READER_DESIGN.md §4.4). Header-only so the native test
// (tests/rugo/orc_timestamp_test.cpp) can drive it with values no writer produces.

#include <cstddef>
#include <cstdint>
#include <string>

#include "orc_proto.hpp"

namespace rugo::orc {

// Seconds from 1970-01-01 to ORC's 2015-01-01 timestamp epoch.
constexpr int64_t kOrcEpochOffset = 1420070400;

// SECONDARY holds (mantissa << 3) | z: z == 0 is the nanos as written, otherwise the mantissa
// carries z + 1 trailing decimal zeros. Returns the nanos; above 999,999,999 fails, never wraps.
inline uint64_t timestamp_nanos(int64_t nanos, const std::string& where) {
    const uint64_t raw = static_cast<uint64_t>(nanos);
    uint64_t value = raw >> 3;
    const unsigned zeros = static_cast<unsigned>(raw & 7);
    // A mantissa below 2^30 times at most 10^8 cannot wrap; anything larger is out of range anyway.
    if (value >> 30) fail(where + "a timestamp's nanoseconds are out of range");
    if (zeros != 0) {
        uint64_t scale = 1;
        for (unsigned z = 0; z <= zeros; ++z) scale *= 10;
        value *= scale;
    }
    if (value > 999999999) fail(where + "a timestamp's nanoseconds are out of range");
    return value;
}

// DATA seconds for which the plain conversion (offset, pre-1970 borrow, * 1e6 + fraction)
// cannot leave int64.
constexpr int64_t kWindowSecondsLo = INT64_MIN / 1000000 + 1 - kOrcEpochOffset;
constexpr int64_t kWindowSecondsHi = INT64_MAX / 1000000 - 1 - kOrcEpochOffset;

// One branch-free pass per run: per-row range checks cost 17-44% of timestamp decode, this
// check is hoisted out of the row loop instead.
inline bool timestamp_seconds_in_window(const int64_t* seconds, size_t n) {
    constexpr uint64_t span = static_cast<uint64_t>(kWindowSecondsHi - kWindowSecondsLo);
    uint64_t outside = 0;
    for (size_t i = 0; i < n; ++i)
        outside |= static_cast<uint64_t>(static_cast<uint64_t>(seconds[i]) -
                                         static_cast<uint64_t>(kWindowSecondsLo) > span);
    return outside == 0;
}

// Seconds are truncated toward zero; match the Java reader's pre-1970 adjustment.
// Only for seconds inside the window (timestamp_seconds_in_window).
inline int64_t timestamp_micros_in_window(int64_t seconds, int64_t nanos, const std::string& where) {
    const uint64_t value = timestamp_nanos(nanos, where);
    int64_t epoch = seconds + kOrcEpochOffset;
    if (epoch < 0 && value > 999999) epoch -= 1;
    return epoch * 1000000 + static_cast<int64_t>(value / 1000);
}

// Any seconds: sub-microsecond digits are truncated; a result outside int64 microseconds fails.
inline int64_t timestamp_micros(int64_t seconds, int64_t nanos, const std::string& where) {
    const uint64_t value = timestamp_nanos(nanos, where);
    int64_t epoch;
    if (__builtin_add_overflow(seconds, kOrcEpochOffset, &epoch))
        fail(where + "a timestamp's seconds are out of range");
    // The borrowed second is taken from the fraction so the product cannot overflow where the
    // sum still fits (INT64_MIN itself).
    int64_t fraction = static_cast<int64_t>(value / 1000);
    if (epoch < 0 && value > 999999) fraction -= 1000000;
    int64_t micros;
    if (__builtin_mul_overflow(epoch, int64_t{1000000}, &micros) ||
        __builtin_add_overflow(micros, fraction, &micros))
        fail(where + "a timestamp is outside the microsecond range");
    return micros;
}

}  // namespace rugo::orc
