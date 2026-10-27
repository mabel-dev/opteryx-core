// Run with `make orc-rle-test`.
#include "rugo/src/orc/orc_rle.hpp"

#include <algorithm>
#include <array>
#include <cstdint>
#include <cstring>
#include <exception>
#include <iostream>
#include <limits>
#include <stdexcept>
#include <string>
#include <vector>

namespace {

using Bytes = std::vector<uint8_t>;
using Values = std::vector<int64_t>;

// This is the ORC width table, not a call into the decoder under test.
constexpr std::array<unsigned, 32> widths = {
    1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16,
    17, 18, 19, 20, 21, 22, 23, 24, 26, 28, 30, 32, 40, 48, 56, 64};
constexpr std::array<size_t, 16> counts = {
    1, 2, 3, 7, 8, 9, 15, 16, 31, 32, 127, 128, 255, 256, 511, 512};

struct Run {
    Bytes bytes;
    Values expected;
};

[[noreturn]] void error(const std::string& context, const std::string& message) {
    throw std::runtime_error(context + ": " + message);
}

uint64_t mask(unsigned width) {
    return width == 64 ? std::numeric_limits<uint64_t>::max() : (uint64_t{1} << width) - 1;
}

// Interpret the bits returned by the unsigned IntRle API, including its high bit.
int64_t as_signed(uint64_t bits) {
    int64_t value;
    std::memcpy(&value, &bits, sizeof(value));
    return value;
}

uint64_t zigzag(int64_t value) {
    if (value >= 0) return static_cast<uint64_t>(value) * 2;
    return static_cast<uint64_t>(-(value + 1)) * 2 + 1;
}

int64_t signed_reference(uint64_t bits) {
    const int64_t half = static_cast<int64_t>(bits >> 1);
    return (bits & 1) ? -1 - half : half;
}

uint64_t pattern(size_t index, unsigned width) {
    const uint64_t all = mask(width);
    switch (index % 8) {
        case 0: return 0;
        case 1: return all;
        case 2: return (uint64_t{1} << (width - 1));
        case 3: return 0xaaaaaaaaaaaaaaaaULL & all;
        case 4: return 0x5555555555555555ULL & all;
        case 5: return 1;
        default: return (uint64_t{0x9e3779b97f4a7c15ULL} * (index + 1)) & all;
    }
}

// Deliberately bit-at-a-time, independent of the decoder's accumulator/unpack code.
// Each call finishes on a byte boundary, as each ORC packed section does.
void pack(Bytes& out, const std::vector<uint64_t>& values, unsigned width) {
    uint8_t byte = 0;
    unsigned used = 0;
    for (uint64_t value : values) {
        if ((value & ~mask(width)) != 0) error("packer", "value exceeds width");
        for (unsigned bit = width; bit != 0; --bit) {
            byte = static_cast<uint8_t>((byte << 1) | ((value >> (bit - 1)) & 1));
            if (++used == 8) {
                out.push_back(byte);
                byte = 0;
                used = 0;
            }
        }
    }
    if (used != 0) out.push_back(static_cast<uint8_t>(byte << (8 - used)));
}

void varint(Bytes& out, uint64_t value) {
    while (value >= 128) {
        out.push_back(static_cast<uint8_t>((value & 127) | 128));
        value >>= 7;
    }
    out.push_back(static_cast<uint8_t>(value));
}

void header(Bytes& out, unsigned encoding, unsigned code, size_t count) {
    if (count < 1 || count > 512) error("header", "invalid run count");
    const size_t n = count - 1;
    out.push_back(static_cast<uint8_t>((encoding << 6) | (code << 1) | (n >> 8)));
    out.push_back(static_cast<uint8_t>(n));
}

Run direct(unsigned code, size_t count, bool is_signed) {
    Run run;
    header(run.bytes, 1, code, count);
    std::vector<uint64_t> data;
    for (size_t i = 0; i < count; ++i) {
        const uint64_t v = pattern(i, widths[code]);
        data.push_back(v);
        run.expected.push_back(is_signed ? signed_reference(v) : as_signed(v));
    }
    pack(run.bytes, data, widths[code]);
    return run;
}

Run delta(unsigned code, size_t count, bool is_signed, bool descending) {
    Run run;
    header(run.bytes, 3, code, count);
    const int64_t base = is_signed ? -12345 : (int64_t{1} << 60);
    const int64_t first_delta = descending ? -7 : 7;
    varint(run.bytes, is_signed ? zigzag(base) : static_cast<uint64_t>(base));
    varint(run.bytes, zigzag(first_delta));
    int64_t value = base;
    run.expected.push_back(value);
    if (count >= 2) {
        value += first_delta;
        run.expected.push_back(value);
    }
    std::vector<uint64_t> magnitudes;
    const uint64_t limit = static_cast<uint64_t>(std::numeric_limits<int64_t>::max()) / (8 * count);
    for (size_t i = 2; i < count; ++i) {
        const uint64_t magnitude = pattern(i - 2, widths[code]) % (limit + 1);
        magnitudes.push_back(magnitude);
        value += descending ? -static_cast<int64_t>(magnitude) : static_cast<int64_t>(magnitude);
        run.expected.push_back(value);
    }
    pack(run.bytes, magnitudes, widths[code]);
    return run;
}

// Large delta magnitudes exercise the high bits even for 64-bit encoded deltas,
// while keeping both endpoints representable as signed integers.
Run wide_delta(unsigned code, bool descending) {
    Run run;
    header(run.bytes, 3, code, 3);
    const int64_t base = descending ? std::numeric_limits<int64_t>::max()
                                    : std::numeric_limits<int64_t>::min();
    const int64_t first = descending ? -1 : 1;
    const uint64_t magnitude = uint64_t{1} << (widths[code] - 1);
    varint(run.bytes, zigzag(base));
    varint(run.bytes, zigzag(first));
    pack(run.bytes, {magnitude}, widths[code]);
    run.expected = {base, base + first,
                    as_signed(static_cast<uint64_t>(base + first) +
                              (descending ? uint64_t{0} - magnitude : magnitude))};
    return run;
}

unsigned entry_width(unsigned bits) {
    for (unsigned width : widths) if (width >= bits) return width;
    error("patch", "patch entry exceeds 64 bits");
}

struct Patch {
    unsigned gap;
    uint64_t value;
};

Run patched(unsigned data_code, size_t count, int64_t base, unsigned patch_code,
            unsigned gap_width, const std::vector<Patch>& patches) {
    Run run;
    const unsigned data_width = widths[data_code];
    const unsigned patch_width = widths[patch_code];
    if (patch_width + gap_width > 64 || patches.size() > 31 || base < -127 || base > 127)
        error("patch", "invalid test parameters");
    header(run.bytes, 2, data_code, count);
    run.bytes.push_back(static_cast<uint8_t>(patch_code)); // one-byte sign-magnitude base
    run.bytes.push_back(static_cast<uint8_t>(((gap_width - 1) << 5) | patches.size()));
    run.bytes.push_back(static_cast<uint8_t>(base < 0 ? 0x80 | -base : base));

    std::vector<uint64_t> data;
    for (size_t i = 0; i < count; ++i) data.push_back(pattern(i, data_width));
    pack(run.bytes, data, data_width);

    std::vector<uint64_t> entries;
    size_t index = 0;
    for (const Patch& patch : patches) {
        if (patch.gap > mask(gap_width) || patch.value > mask(patch_width))
            error("patch", "entry does not fit");
        entries.push_back((static_cast<uint64_t>(patch.gap) << patch_width) | patch.value);
        index += patch.gap;
        if (patch.gap == 255 && patch.value == 0) continue;
        if (index >= count || data_width == 64 || patch.value > mask(64 - data_width))
            error("patch", "patch position or value is invalid");
        data[index] |= patch.value << data_width;
    }
    pack(run.bytes, entries, entry_width(patch_width + gap_width));
    for (uint64_t v : data)
        run.expected.push_back(as_signed(static_cast<uint64_t>(base) + v));
    return run;
}

void verify(const Run& run, bool is_signed, const std::string& context) {
    // Offset the stream pointer, not just the first packed value. Allocate no
    // suffix so sanitizers can catch reads beyond the end of the stream.
    for (size_t offset = 0; offset < 16; ++offset) {
        Bytes storage(offset + run.bytes.size(), 0xa5);
        std::copy(run.bytes.begin(), run.bytes.end(), storage.begin() + offset);
        for (bool fragmented : {false, true}) {
            rugo::orc::IntRle decoder(storage.data() + offset, run.bytes.size(), true, is_signed);
            Values actual(run.expected.size());
            decoder.read(actual.data(), 0);
            if (fragmented) {
                constexpr std::array<size_t, 8> chunks = {1, 2, 7, 3, 16, 31, 5, 128};
                size_t at = 0;
                size_t chunk = 0;
                while (at < actual.size()) {
                    const size_t n = std::min(chunks[chunk++ % chunks.size()], actual.size() - at);
                    decoder.read(actual.data() + at, n);
                    at += n;
                }
            } else {
                decoder.read(actual.data(), actual.size());
            }
            for (size_t i = 0; i < actual.size(); ++i) {
                if (actual[i] != run.expected[i]) {
                    error(context, "offset=" + std::to_string(offset) +
                          " fragmented=" + std::to_string(fragmented) +
                          " index=" + std::to_string(i) +
                          " expected=" + std::to_string(run.expected[i]) +
                          " actual=" + std::to_string(actual[i]));
                }
            }
        }
    }
}

void check_packer() {
    Bytes b;
    pack(b, {1, 2, 7}, 3);
    if (b != Bytes{0x2b, 0x80}) error("packer", "3-bit MSB-first known vector failed");
    b.clear();
    pack(b, {0x0123456789abcdefULL, 0xfedcba9876543210ULL}, 64);
    if (b != Bytes{0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef,
                   0xfe, 0xdc, 0xba, 0x98, 0x76, 0x54, 0x32, 0x10})
        error("packer", "64-bit MSB-first known vector failed");
}

void truncated_at(const Run& run, size_t size, bool is_signed, const std::string& context) {
    Bytes input(run.bytes.begin(), run.bytes.begin() + size);
    const uint8_t empty = 0;
    rugo::orc::IntRle decoder(size ? input.data() : &empty, size, true, is_signed);
    Values actual(run.expected.size());
    bool threw = false;
    try {
        decoder.read(actual.data(), actual.size());
    } catch (const std::runtime_error&) {
        threw = true;
    }
    if (!threw) error(context, "truncated at byte " + std::to_string(size) + " did not throw");
}

void direct_tests() {
    for (unsigned code = 0; code < widths.size(); ++code) {
        for (size_t count : counts) {
            for (bool is_signed : {false, true}) {
                const Run run = direct(code, count, is_signed);
                const std::string context = "direct width=" + std::to_string(widths[code]) +
                    " count=" + std::to_string(count) + " signed=" + std::to_string(is_signed);
                verify(run, is_signed, context);
                if (!is_signed) truncated_at(run, run.bytes.size() - 1, is_signed, context);
            }
        }
    }
}

void delta_tests() {
    // Code zero means fixed delta, not a one-bit variable-delta payload.
    for (unsigned code = 1; code < widths.size(); ++code) {
        for (size_t count : counts) {
            for (bool is_signed : {false, true}) for (bool descending : {false, true}) {
                const Run run = delta(code, count, is_signed, descending);
                const std::string context = "delta width=" + std::to_string(widths[code]) +
                    " count=" + std::to_string(count) + " signed=" + std::to_string(is_signed) +
                    " descending=" + std::to_string(descending);
                verify(run, is_signed, context);
                if (is_signed && !descending)
                    truncated_at(run, run.bytes.size() - 1, is_signed, context);
            }
        }
        for (bool descending : {false, true})
            verify(wide_delta(code, descending), true,
                   "wide delta width=" + std::to_string(widths[code]) +
                   " descending=" + std::to_string(descending));
    }
}

void patched_tests() {
    for (unsigned code = 0; code < widths.size(); ++code) {
        for (size_t count : counts) {
            // A 64-bit data field has no bits left for a nonzero patch.
            std::vector<Patch> patches;
            if (widths[code] != 64) {
                size_t gap = count - 1;
                while (gap >= 255) {
                    patches.push_back({255, 0});
                    gap -= 255;
                }
                patches.push_back({static_cast<unsigned>(gap), 1});
            }
            for (int64_t base : {-37, 0, 37}) {
                const Run run = patched(code, count, base, 0, 8, patches);
                const std::string context = "patched data width=" + std::to_string(widths[code]) +
                    " count=" + std::to_string(count) + " base=" + std::to_string(base);
                verify(run, true, context);
                if (base == 0) truncated_at(run, run.bytes.size() - 1, true, context);
            }
        }
    }
    std::array<bool, 65> seen{};
    for (unsigned code = 0; code < widths.size(); ++code) {
        const unsigned pw = widths[code];
        for (unsigned gw = 1; gw <= 8 && pw + gw <= 64; ++gw) {
            const unsigned ew = entry_width(pw + gw);
            seen[ew] = true;
            const uint64_t max_patch = mask(pw);
            const unsigned last_gap = std::min<unsigned>(static_cast<unsigned>(mask(gw)), 6);
            const std::vector<Patch> patches = {
                {0, max_patch}, {1, max_patch / 2}, {1, 1}, {last_gap, max_patch}};
            const Run run = patched(0, 9, -11, code, gw, patches);
            const std::string context = "patch width=" + std::to_string(pw) +
                " gap width=" + std::to_string(gw) + " entry width=" + std::to_string(ew);
            verify(run, true, context);
            truncated_at(run, run.bytes.size() - 1, true, context);
        }
    }
    for (unsigned width : widths) {
        if (width >= 2 && !seen[width])
            error("patch coverage", "missing entry width " + std::to_string(width));
    }
}

void append(Run& target, const Run& part) {
    target.bytes.insert(target.bytes.end(), part.bytes.begin(), part.bytes.end());
    target.expected.insert(target.expected.end(), part.expected.begin(), part.expected.end());
}

void cross_run_tests() {
    verify(Run{Bytes{0x44, 0x02, 0x2b, 0x80}, Values{1, 2, 7}}, false,
           "hand-encoded direct");
    verify(Run{Bytes{0x44, 0x02, 0x2b, 0x80}, Values{-1, 1, -4}}, true,
           "hand-encoded signed direct");
    Run combined;
    // Unused bits in each packed section must not be consumed as part of the next run.
    append(combined, direct(2, 3, true));
    append(combined, delta(4, 9, true, true));
    append(combined, patched(2, 7, -37, 2, 3, {{0, 7}, {4, 3}}));
    append(combined, Run{Bytes{0x02, 0x09}, Values(5, -5)}); // short repeat, zigzag -5
    append(combined, direct(31, 16, true));
    append(combined, delta(7, 1, true, false));
    append(combined, direct(0, 511, true));
    verify(combined, true, "cross-run alignment");
}

void truncated(const Run& run, bool is_signed, const std::string& context) {
    for (size_t size = 0; size < run.bytes.size(); ++size)
        truncated_at(run, size, is_signed, context);
}

void truncation_tests() {
    truncated(direct(2, 9, true), true, "truncated direct, partial byte");
    truncated(direct(31, 16, false), false, "truncated direct, 64-bit");
    truncated(delta(30, 9, true, false), true, "truncated delta");
    truncated(delta(1, 1, true, true), true, "truncated delta single value");
    truncated(delta(1, 2, false, false), false, "truncated delta two values");
    truncated(patched(2, 9, -37, 10, 3, {{0, 1023}, {4, 1}}), true, "truncated patched base");
    truncated(Run{Bytes{0x0a, 0x01, 0x23}, Values(5, -146)}, true, "truncated short repeat");
    Run combined;
    append(combined, direct(2, 3, true));
    append(combined, delta(4, 9, true, false));
    append(combined, patched(2, 7, -37, 2, 3, {{0, 7}, {4, 3}}));
    truncated(combined, true, "truncated across runs");
}

} // namespace

int main() {
    try {
        check_packer();
        direct_tests();
        delta_tests();
        patched_tests();
        cross_run_tests();
        truncation_tests();
        std::cout << "ORC RLEv2 native correctness tests passed\n";
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "ORC RLEv2 native correctness test failed: " << e.what() << '\n';
        return 1;
    }
}
