// Run with `make orc-present-test`.
#include "rugo/src/orc/orc_rle.hpp"

#include <algorithm>
#include <array>
#include <cstdint>
#include <exception>
#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

namespace {

using Bytes = std::vector<uint8_t>;

[[noreturn]] void error(const std::string& context, const std::string& detail) {
    throw std::runtime_error(context + ": " + detail);
}

void literal(Bytes& encoded, Bytes& packed, const Bytes& values) {
    if (values.empty() || values.size() > 128) error("fixture", "invalid literal length");
    encoded.push_back(static_cast<uint8_t>(256 - values.size()));
    encoded.insert(encoded.end(), values.begin(), values.end());
    packed.insert(packed.end(), values.begin(), values.end());
}

void repeat(Bytes& encoded, Bytes& packed, uint8_t value, size_t count) {
    if (count < 3 || count > 130) error("fixture", "invalid repeat length");
    encoded.push_back(static_cast<uint8_t>(count - 3));
    encoded.push_back(value);
    packed.insert(packed.end(), count, value);
}

// A deliberately bit-at-a-time oracle: ORC reads the most significant bit
// first; the corresponding Draken row is the least significant output bit.
void verify(const Bytes& encoded, const Bytes& packed, size_t count, const std::string& context) {
    const size_t nbytes = (count + 7) / 8;
    if (nbytes > packed.size()) error(context, "fixture does not contain enough bits");
    Bytes expected(nbytes, 0);
    size_t nonnull = 0;
    for (size_t row = 0; row < count; ++row) {
        if ((packed[row / 8] & (0x80u >> (row % 8))) != 0) {
            expected[row / 8] |= static_cast<uint8_t>(1u << (row % 8));
            ++nonnull;
        }
    }

    // Exercise unaligned input/output and check both output guards. The source
    // ends at the allocation boundary so ASan can detect over-reads.
    for (size_t offset = 0; offset < 8; ++offset) {
        Bytes source(offset + encoded.size() + (encoded.empty() && offset == 0 ? 1 : 0), 0xa5);
        std::copy(encoded.begin(), encoded.end(), source.begin() + offset);
        Bytes output(offset + nbytes + 8, 0xa5);
        const size_t actual = rugo::orc::read_present(source.data() + offset, encoded.size(), count,
                                                       output.data() + offset);
        if (actual != nonnull) error(context, "nonnull count at bit length " + std::to_string(count));
        if (!std::equal(expected.begin(), expected.end(), output.begin() + offset))
            error(context, "validity bitmap at bit length " + std::to_string(count));
        if (!std::all_of(output.begin(), output.begin() + offset, [](uint8_t b) { return b == 0xa5; }) ||
            !std::all_of(output.begin() + offset + nbytes, output.end(), [](uint8_t b) { return b == 0xa5; }))
            error(context, "wrote outside output at bit length " + std::to_string(count));
        if (nbytes != 0) {
            // No suffix: ASan also detects reads beyond the output's last byte.
            Bytes tight(offset + nbytes, 0xa5);
            const size_t tight_count = rugo::orc::read_present(source.data() + offset, encoded.size(), count,
                                                                 tight.data() + offset);
            if (tight_count != nonnull || !std::equal(expected.begin(), expected.end(), tight.begin() + offset))
                error(context, "tight output at bit length " + std::to_string(count));
            if (!std::all_of(tight.begin(), tight.begin() + offset, [](uint8_t b) { return b == 0xa5; }))
                error(context, "wrote before tight output at bit length " + std::to_string(count));
        }
    }
}

void exhaustive_bytes_and_tails() {
    // Nine source bytes exercise the uint64 reversal, the scalar path, every
    // tail length, and every possible combination of unused padding bits.
    for (unsigned value = 0; value < 256; ++value) {
        Bytes repeated, repeated_packed;
        repeat(repeated, repeated_packed, static_cast<uint8_t>(value), 9);
        Bytes literals, literal_packed;
        literal(literals, literal_packed, Bytes(9, static_cast<uint8_t>(value)));
        for (size_t count = 0; count <= 72; ++count) {
            const std::string context = "byte " + std::to_string(value);
            verify(repeated, repeated_packed, count, context + " repeat");
            verify(literals, literal_packed, count, context + " literal");
        }
    }
}

void run_boundaries() {
    Bytes encoded, packed;
    std::vector<size_t> ends;
    auto add_literal = [&](size_t count, unsigned seed) {
        Bytes values;
        for (size_t i = 0; i < count; ++i)
            values.push_back(static_cast<uint8_t>((i * 73 + seed) & 0xff));
        literal(encoded, packed, values);
        ends.push_back(packed.size());
    };
    auto add_repeat = [&](size_t count, uint8_t value) {
        repeat(encoded, packed, value, count);
        ends.push_back(packed.size());
    };
    add_literal(1, 0x80);
    add_repeat(3, 0x01);
    add_literal(127, 0x25);
    add_repeat(130, 0x81);
    add_literal(128, 0x39);
    add_repeat(129, 0x55);
    add_literal(128, 0xf0);
    add_repeat(4, 0xff);

    constexpr std::array<size_t, 27> counts = {
        0, 1, 7, 8, 9, 63, 64, 65, 127, 128, 129, 255, 256, 257,
        511, 512, 513, 1023, 1024, 1025, 2047, 2048, 2049, 4095, 4096, 4097, 5200};
    for (size_t count : counts) verify(encoded, packed, count, "mixed runs");
    for (size_t end : ends) {
        const size_t bit = end * 8;
        verify(encoded, packed, bit - 1, "before run boundary");
        verify(encoded, packed, bit, "at run boundary");
        if (end < packed.size()) verify(encoded, packed, bit + 1, "after run boundary");
    }
}

void expect_failure(const Bytes& encoded, size_t count, const std::string& message) {
    // Keep an addressable input even for an empty stream; no reads are allowed.
    Bytes source(encoded.size() + (encoded.empty() ? 1 : 0));
    std::copy(encoded.begin(), encoded.end(), source.begin());
    Bytes output((count + 7) / 8 + 8, 0xa5);
    bool threw = false;
    try {
        rugo::orc::read_present(source.data(), encoded.size(), count, output.data());
    } catch (const std::runtime_error& e) {
        threw = true;
        if (std::string(e.what()).find(message) == std::string::npos)
            error("exception", std::string("unexpected message: ") + e.what());
    }
    if (!threw) error("exception", "decoder accepted " + message);
    const size_t nbytes = (count + 7) / 8;
    if (!std::all_of(output.begin() + nbytes, output.end(), [](uint8_t b) { return b == 0xa5; }))
        error("exception", "wrote beyond output");
}

void malformed_and_truncated() {
    verify({}, {}, 0, "empty input and output");
    verify({0x00}, {}, 0, "zero bits do not consume malformed input");
    expect_failure({}, 1, "ends before its values");
    expect_failure({0x00}, 1, "run is missing its value");
    expect_failure({0xff}, 1, "literal runs past its stream");
    expect_failure({0xfe, 0x80}, 1, "literal runs past its stream");
    expect_failure({0x80, 0x80, 0x01}, 1, "literal runs past its stream");
    expect_failure({0x00, 0xff}, 25, "ends before its values");

    Bytes encoded, packed;
    repeat(encoded, packed, 0x80, 3);
    literal(encoded, packed, {0xff, 0x01, 0x80, 0x42});
    repeat(encoded, packed, 0x55, 130);
    literal(encoded, packed, {0x13, 0x26, 0x4c});
    verify(encoded, packed, packed.size() * 8, "complete stream");
    for (size_t length = 0; length < encoded.size(); ++length) {
        const Bytes prefix(encoded.begin(), encoded.begin() + length);
        expect_failure(prefix, packed.size() * 8, "byte run-length");
    }
}

} // namespace

int main() {
    try {
        exhaustive_bytes_and_tails();
        run_boundaries();
        malformed_and_truncated();
        std::cout << "ORC PRESENT native correctness tests passed\n";
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "ORC PRESENT native correctness test failed: " << e.what() << '\n';
        return 1;
    }
}
