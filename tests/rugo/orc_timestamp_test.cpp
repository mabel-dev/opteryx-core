// Run with `make orc-timestamp-test`.
#include "rugo/src/orc/orc_timestamp.hpp"

#include <cstdint>
#include <exception>
#include <iostream>
#include <limits>
#include <stdexcept>
#include <string>

namespace {

using rugo::orc::kOrcEpochOffset;
using rugo::orc::timestamp_micros;

constexpr int64_t kMax = std::numeric_limits<int64_t>::max();
constexpr int64_t kMin = std::numeric_limits<int64_t>::min();

// SECONDARY encoding: mantissa << 3 | z, where z != 0 means z + 1 trailing decimal zeros.
int64_t nanos(uint64_t mantissa, unsigned z = 0) { return static_cast<int64_t>((mantissa << 3) | z); }

// ORC DATA seconds for a given Unix-epoch second.
int64_t orc_seconds(int64_t unix_seconds) { return unix_seconds - kOrcEpochOffset; }

int failures = 0;

void expect_value(const char* name, int64_t seconds, int64_t encoded, int64_t expected) {
    try {
        const int64_t got = timestamp_micros(seconds, encoded, "");
        if (got != expected) {
            std::cerr << name << ": expected " << expected << ", got " << got << "\n";
            ++failures;
        }
    } catch (const std::exception& e) {
        std::cerr << name << ": expected " << expected << ", raised: " << e.what() << "\n";
        ++failures;
    }
}

void expect_error(const char* name, int64_t seconds, int64_t encoded, const std::string& message) {
    try {
        const int64_t got = timestamp_micros(seconds, encoded, "column 't': ");
        std::cerr << name << ": expected an error, got " << got << "\n";
        ++failures;
    } catch (const std::runtime_error& e) {
        const std::string what = e.what();
        if (what != "ORC: column 't': " + message) {
            std::cerr << name << ": wrong error: " << what << "\n";
            ++failures;
        }
    }
}

}  // namespace

int main() {
    const std::string bad_nanos = "a timestamp's nanoseconds are out of range";
    const std::string bad_seconds = "a timestamp's seconds are out of range";
    const std::string bad_micros = "a timestamp is outside the microsecond range";

    // Valid values: existing semantics, including sub-microsecond truncation.
    expect_value("orc epoch", 0, nanos(0), kOrcEpochOffset * 1000000);
    expect_value("unix epoch", orc_seconds(0), nanos(0), 0);
    expect_value("sub-microsecond truncated", orc_seconds(0), nanos(1999), 1);
    expect_value("largest nanos", orc_seconds(0), nanos(999999999), 999999);
    expect_value("scaled nanos z=7", orc_seconds(0), nanos(9, 7), 900000);
    expect_value("scaled nanos z=2", orc_seconds(0), nanos(123, 2), 123);
    // Pre-1970: seconds truncate toward zero, so a fraction above 999,999 ns borrows a second.
    expect_value("pre-1970 fraction", orc_seconds(-1), nanos(5, 7), -1500000);
    expect_value("pre-1970 no borrow", orc_seconds(-1), nanos(999999), -1000000 + 999);

    // Representable microsecond endpoints and one microsecond past each.
    expect_value("max micros", orc_seconds(kMax / 1000000), nanos(775807999), kMax);
    expect_error("max + 1 us", orc_seconds(kMax / 1000000), nanos(775808000), bad_micros);
    // INT64_MIN = -9223372036855 s + 224192 us; the stored second is one above (truncated).
    expect_value("min micros", orc_seconds(-9223372036854), nanos(224192000), kMin);
    expect_error("min - 1 us", orc_seconds(-9223372036854), nanos(224191999), bad_micros);

    // Malformed nanos.
    expect_error("nanos 1e9", orc_seconds(0), nanos(1000000000), bad_nanos);
    expect_error("scaled nanos 1e9", orc_seconds(0), nanos(10, 7), bad_nanos);
    // 2^56 * 10^8 == 5^8 * 2^64: the scaled product wraps to zero.
    expect_error("wrapping scaled nanos", orc_seconds(0), static_cast<int64_t>((1ULL << 59) | 7), bad_nanos);
    expect_error("negative nanos", orc_seconds(0), -8, bad_nanos);

    // Malformed seconds.
    expect_error("seconds max", kMax, nanos(0), bad_seconds);
    expect_error("seconds past offset", kMax - kOrcEpochOffset + 1, nanos(0), bad_seconds);
    expect_error("seconds min", kMin, nanos(0), bad_micros);
    expect_error("seconds min with fraction", kMin, nanos(5, 7), bad_micros);
    expect_error("seconds too large for micros", orc_seconds(kMax / 1000000 + 1), nanos(0), bad_micros);

    // Around both int64 microsecond endpoints, every result matches exact 128-bit arithmetic
    // when it fits and fails when it does not.
    for (const int64_t edge : {orc_seconds(kMax / 1000000), orc_seconds(kMin / 1000000)}) {
        for (int64_t seconds = edge - 3; seconds <= edge + 3; ++seconds) {
            for (const uint64_t fraction : {0ULL, 999999ULL, 1000000ULL, 224191999ULL, 224192000ULL,
                                            775807999ULL, 775808000ULL, 999999999ULL}) {
                __int128 epoch = static_cast<__int128>(seconds) + kOrcEpochOffset;
                if (epoch < 0 && fraction > 999999) epoch -= 1;
                const __int128 exact = epoch * 1000000 + static_cast<__int128>(fraction / 1000);
                if (exact >= kMin && exact <= kMax)
                    expect_value("endpoint sweep", seconds, nanos(fraction), static_cast<int64_t>(exact));
                else
                    expect_error("endpoint sweep", seconds, nanos(fraction), bad_micros);
            }
        }
    }

    // The in-window conversion matches the checked one at and just inside both window edges,
    // and the window test admits exactly [kWindowSecondsLo, kWindowSecondsLo + 2^44).
    using rugo::orc::kWindowSecondsLo;
    using rugo::orc::timestamp_micros_in_window;
    using rugo::orc::timestamp_micros_run_checked;
    // The reader's per-run window test: OR the offsets, test once.
    auto timestamp_seconds_in_window = [](const int64_t* seconds, size_t n) {
        uint64_t offsets = 0;
        for (size_t i = 0; i < n; ++i) offsets |= rugo::orc::timestamp_seconds_offset(seconds[i]);
        return rugo::orc::timestamp_offsets_in_window(offsets);
    };
    constexpr int64_t kWindowSecondsHi = kWindowSecondsLo + (int64_t{1} << rugo::orc::kWindowBits) - 1;
    for (const int64_t seconds : {kWindowSecondsLo, kWindowSecondsLo + 1, orc_seconds(-1), orc_seconds(0),
                                  kWindowSecondsHi - 1, kWindowSecondsHi}) {
        for (const uint64_t fraction : {0ULL, 999999ULL, 1000000ULL, 999999999ULL}) {
            const int64_t checked = timestamp_micros(seconds, nanos(fraction), "");
            expect_value("in-window conversion", seconds, nanos(fraction), checked);
            if (timestamp_micros_in_window(seconds, nanos(fraction), "") != checked) {
                std::cerr << "in-window conversion differs at seconds " << seconds << "\n";
                ++failures;
            }
        }
    }
    const int64_t inside[] = {kWindowSecondsLo, 0, kWindowSecondsHi};
    const int64_t below[] = {0, kWindowSecondsLo - 1, 0};
    const int64_t above[] = {0, 0, kWindowSecondsHi + 1};
    const int64_t extremes[] = {kMin, kMax};
    if (!timestamp_seconds_in_window(inside, 3) || timestamp_seconds_in_window(below, 3) ||
        timestamp_seconds_in_window(above, 3) || timestamp_seconds_in_window(extremes, 1) ||
        timestamp_seconds_in_window(extremes + 1, 1) || !timestamp_seconds_in_window(inside, 0)) {
        std::cerr << "window test admits the wrong seconds\n";
        ++failures;
    }
    // Nanos are checked on the in-window path too, including the wrapping encoding.
    for (const int64_t bad : {static_cast<int64_t>((1ULL << 59) | 7), nanos(10, 7), nanos(1000000000)}) {
        try {
            timestamp_micros_in_window(orc_seconds(0), bad, "");
            std::cerr << "in-window conversion accepted bad nanos " << bad << "\n";
            ++failures;
        } catch (const std::runtime_error&) {
        }
    }

    // The checked rerun overwrites exactly the run's present rows, skipping absent ones.
    {
        const int64_t secs[] = {orc_seconds(0), orc_seconds(kMax / 1000000), orc_seconds(-1)};
        const int64_t fracs[] = {nanos(1000), nanos(775807000), nanos(5, 7)};
        const uint8_t validity[] = {0x1a}; // rows 1, 3 and 4
        int64_t values[6] = {-7, -7, -7, -7, -7, -7};
        timestamp_micros_run_checked(values, 0, validity, secs, fracs, 3, "");
        const int64_t expected[6] = {-7, 1, -7, kMax, -1500000, -7};
        for (int i = 0; i < 6; ++i)
            if (values[i] != expected[i]) {
                std::cerr << "checked rerun row " << i << ": expected " << expected[i] << ", got " << values[i] << "\n";
                ++failures;
            }
        int64_t dense[3] = {};
        timestamp_micros_run_checked(dense, 0, nullptr, secs, fracs, 3, "");
        if (dense[0] != 1 || dense[1] != kMax || dense[2] != -1500000) {
            std::cerr << "checked rerun without validity bitmap is wrong\n";
            ++failures;
        }
        const int64_t bad_secs[] = {orc_seconds(0), kMax};
        try {
            timestamp_micros_run_checked(dense, 0, nullptr, bad_secs, fracs, 2, "column 't': ");
            std::cerr << "checked rerun accepted out-of-range seconds\n";
            ++failures;
        } catch (const std::runtime_error& e) {
            if (std::string(e.what()) != "ORC: column 't': " + bad_seconds) {
                std::cerr << "checked rerun wrong error: " << e.what() << "\n";
                ++failures;
            }
        }
    }

    if (failures != 0) {
        std::cerr << failures << " ORC timestamp test(s) failed\n";
        return 1;
    }
    std::cout << "ORC timestamp native correctness tests passed\n";
    return 0;
}
