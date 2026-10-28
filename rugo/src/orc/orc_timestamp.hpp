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

// DATA seconds in [kWindowSecondsLo, kWindowSecondsLo + 2^44): a power-of-two window inside the
// range where the plain conversion (offset, pre-1970 borrow, * 1e6 + fraction) cannot leave
// int64 — Unix years about -292,000 to +265,000. Seconds outside it, valid or not, take the
// exact checked path.
constexpr int64_t kWindowSecondsLo = INT64_MIN / 1000000 + 1 - kOrcEpochOffset;
constexpr unsigned kWindowBits = 44;
static_assert(kWindowSecondsLo + (int64_t{1} << kWindowBits) - 1 <= INT64_MAX / 1000000 - 1 - kOrcEpochOffset,
              "the timestamp window must sit inside the range the plain conversion handles");

// Offset of seconds from the window floor. The row loop ORs these over a run (two ALU ops per
// row, no branch); timestamp_offsets_in_window then tests the run once. A separate pass over
// the run or a per-row range branch costs 10-44% of timestamp decode (~1.4 ns/row).
inline uint64_t timestamp_seconds_offset(int64_t seconds) {
    return static_cast<uint64_t>(seconds) - static_cast<uint64_t>(kWindowSecondsLo);
}

inline bool timestamp_offsets_in_window(uint64_t offsets) { return (offsets >> kWindowBits) == 0; }

// Seconds are truncated toward zero; match the Java reader's pre-1970 adjustment. Exact for
// seconds inside the window; outside it the result wraps (unsigned, defined) and the caller
// redoes the run with timestamp_micros_run_checked.
inline int64_t timestamp_micros_in_window(int64_t seconds, int64_t nanos, const std::string& where) {
    const uint64_t value = timestamp_nanos(nanos, where);
    uint64_t epoch = static_cast<uint64_t>(seconds) + static_cast<uint64_t>(kOrcEpochOffset);
    if (static_cast<int64_t>(epoch) < 0 && value > 999999) epoch -= 1;
    return static_cast<int64_t>(epoch * 1000000 + value / 1000);
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

// A run whose seconds leave the window: checked conversion into present rows,
// overwriting what timestamp_micros_in_window wrote. A null bitmap means all valid.
// Out of line and cold so its code stays out of the hot loop.
[[gnu::noinline, gnu::cold]] inline void timestamp_micros_run_checked(int64_t* values, size_t row,
                                                                        const uint8_t* validity,
                                                                        const int64_t* seconds,
                                                                        const int64_t* nanos, size_t count,
                                                                        const std::string& where) {
    for (size_t i = 0; i < count; ++i) {
        if (validity != nullptr)
            while ((validity[row >> 3] & (1u << (row & 7))) == 0) ++row;
        values[row++] = timestamp_micros(seconds[i], nanos[i], where);
    }
}

}  // namespace rugo::orc
