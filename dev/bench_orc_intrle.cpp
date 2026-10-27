// Native ORC RLEv2 IntRle benchmark; run `make bench-orc-rle` from the repository root.
// Override ORC_RLE_BENCH_ARGS="rounds iterations" for a different measurement budget.
// The target rebuilds the executable because orc_rle.hpp is inline.
//
// Deterministic, independently encoded complete runs; every output is checked
// before timing, including a separate sequence of partial reads. Timing includes
// decoder construction, run headers, unpack, reconstruction and read's memcpy;
// fixture generation, allocation and validation are outside the timed interval.
// Per iteration an opaque compiler barrier and a sampled checksum consume the
// output. Cases are interleaved in alternating order; results are per-case
// median/min/max of rounds, in ns per decoded value. Inputs and outputs are hot.
// This is not an ORC file, decompression, I/O, multi-thread, or query benchmark.

#include "rugo/src/orc/orc_rle.hpp"

#include <algorithm>
#include <array>
#include <bit>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <limits>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace {

using Clock = std::chrono::steady_clock;
constexpr std::array<unsigned, 32> widths = {
    1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16,
    17, 18, 19, 20, 21, 22, 23, 24, 26, 28, 30, 32, 40, 48, 56, 64};
constexpr std::array<size_t, 4> run_lengths = {512, 511, 128, 127};
constexpr size_t runs = 16;

struct Fixture {
    Fixture(std::string name_, bool is_signed_) : name(std::move(name_)), is_signed(is_signed_) {}

    std::string name;
    bool is_signed;
    std::vector<uint8_t> bytes;
    std::vector<int64_t> expected;
    std::vector<int64_t> output;
    std::vector<double> samples;
};

// Fixed algorithm rather than std::uniform_int_distribution, whose sequence is
// implementation-dependent. Unsigned overflow is intentional.
uint64_t random64(uint64_t& state) {
    state ^= state >> 12;
    state ^= state << 25;
    state ^= state >> 27;
    return state * UINT64_C(2685821657736338717);
}

uint64_t mask(unsigned width) {
    return width == 64 ? ~uint64_t{0} : (uint64_t{1} << width) - 1;
}

unsigned width_code(unsigned width) {
    for (unsigned i = 0; i < widths.size(); ++i)
        if (widths[i] == width) return i;
    throw std::runtime_error("unsupported fixture width");
}

void varint(std::vector<uint8_t>& out, uint64_t value) {
    while (value >= 128) {
        out.push_back(static_cast<uint8_t>(value | 0x80));
        value >>= 7;
    }
    out.push_back(static_cast<uint8_t>(value));
}

uint64_t zigzag(int64_t value) {
    return value < 0 ? (static_cast<uint64_t>(-(value + 1)) << 1) | 1
                     : static_cast<uint64_t>(value) << 1;
}

int64_t unzigzag_reference(uint64_t value) {
    const int64_t magnitude = static_cast<int64_t>(value >> 1);
    return value & 1 ? -magnitude - 1 : magnitude;
}

// Bit-at-a-time encoder, deliberately independent of the production unpacker.
void pack(std::vector<uint8_t>& out, const std::vector<uint64_t>& values, unsigned width) {
    uint8_t byte = 0;
    unsigned used = 0;
    for (uint64_t value : values) {
        if ((value & ~mask(width)) != 0) throw std::runtime_error("fixture value exceeds width");
        for (unsigned bit = width; bit > 0; --bit) {
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

void header(std::vector<uint8_t>& out, unsigned encoding, unsigned code, size_t count) {
    if (count < 1 || count > 512) throw std::runtime_error("invalid fixture run length");
    const size_t n = count - 1;
    out.push_back(static_cast<uint8_t>((encoding << 6) | (code << 1) | (n >> 8)));
    out.push_back(static_cast<uint8_t>(n));
}

Fixture direct(unsigned width, bool is_signed) {
    Fixture f{"DIRECT-" + std::to_string(width) + (is_signed ? "-signed" : "-unsigned"), is_signed};
    uint64_t state = UINT64_C(0x731a11) + width + (is_signed ? 0 : 100);
    for (size_t r = 0; r < runs; ++r) {
        const size_t count = run_lengths[r % run_lengths.size()];
        std::vector<uint64_t> data;
        data.reserve(count);
        for (size_t i = 0; i < count; ++i) {
            uint64_t v = random64(state) & mask(width);
            if (i == 0) v |= uint64_t{1} << (width - 1);
            data.push_back(v);
            f.expected.push_back(is_signed ? unzigzag_reference(v) : static_cast<int64_t>(v));
        }
        header(f.bytes, 1, width_code(width), count);
        pack(f.bytes, data, width);
    }
    return f;
}

Fixture delta(unsigned width, bool descending) {
    Fixture f{"DELTA-" + std::to_string(width) + (descending ? "-down" : "-up"), true};
    uint64_t state = UINT64_C(0xdec7a) + width + (descending ? 100 : 0);
    for (size_t r = 0; r < runs; ++r) {
        const size_t count = run_lengths[r % run_lengths.size()];
        const int64_t base = width == 64 ? std::numeric_limits<int64_t>::min() :
                             (descending ? INT64_C(5000000000000) : -1000);
        const int64_t first_delta = descending ? -3 : 3;
        header(f.bytes, 3, width == 0 ? 0 : width_code(width), count);
        varint(f.bytes, zigzag(base));
        varint(f.bytes, zigzag(first_delta));
        f.expected.push_back(base);
        uint64_t value = std::bit_cast<uint64_t>(base);
        value += static_cast<uint64_t>(first_delta);
        f.expected.push_back(std::bit_cast<int64_t>(value));
        std::vector<uint64_t> data;
        for (size_t i = 2; i < count; ++i) {
            uint64_t d;
            if (width == 0) {
                d = 3;
            } else {
                // Very wide deltas cannot repeatedly span their full range in a
                // monotone int64 run. Include one large delta, then small ones.
                d = random64(state) & mask(width > 48 ? 16 : width);
                if (i == 2) d |= uint64_t{1} << (width - 1);
                data.push_back(d);
            }
            value = descending ? value - d : value + d;
            f.expected.push_back(std::bit_cast<int64_t>(value));
        }
        if (width != 0) pack(f.bytes, data, width);
    }
    return f;
}

Fixture patched(unsigned width, unsigned patch_width) {
    if (width + patch_width > 63) throw std::runtime_error("fixture patch exceeds signed range");
    Fixture f{"PATCHED-" + std::to_string(width) + "-p" + std::to_string(patch_width), true};
    uint64_t state = UINT64_C(0xba5e) + width * 100 + patch_width;
    constexpr int64_t base = -1000;
    constexpr unsigned gap_width = 8;
    // Independent lookup of ORC's fixed-width table for patch entries.
    unsigned entry_width = 0;
    for (unsigned w : widths) if (w >= gap_width + patch_width) { entry_width = w; break; }
    if (entry_width == 0) throw std::runtime_error("invalid patch entry width");
    for (size_t r = 0; r < runs; ++r) {
        const size_t count = run_lengths[r % run_lengths.size()];
        const std::array<size_t, 4> positions = {count / 6, count / 3, count * 2 / 3, count - 1};
        std::vector<uint64_t> data, patches;
        size_t previous = 0, patch_index = 0;
        for (size_t i = 0; i < count; ++i) {
            uint64_t low = random64(state) & mask(width);
            if (i == 0) low |= uint64_t{1} << (width - 1);
            data.push_back(low);
            uint64_t full = low;
            if (patch_index < positions.size() && i == positions[patch_index]) {
                const uint64_t patch = (random64(state) & mask(patch_width)) | (uint64_t{1} << (patch_width - 1));
                const size_t gap = i - previous;
                if (gap > 254) throw std::runtime_error("fixture patch gap too large");
                patches.push_back((static_cast<uint64_t>(gap) << patch_width) | patch);
                full |= patch << width;
                previous = i;
                ++patch_index;
            }
            f.expected.push_back(base + static_cast<int64_t>(full));
        }
        header(f.bytes, 2, width_code(width), count);
        f.bytes.push_back(static_cast<uint8_t>((1 << 5) | width_code(patch_width))); // two-byte base
        f.bytes.push_back(static_cast<uint8_t>(((gap_width - 1) << 5) | patches.size()));
        f.bytes.push_back(0x83); // sign-magnitude -1000
        f.bytes.push_back(0xe8);
        pack(f.bytes, data, width);
        pack(f.bytes, patches, entry_width);
    }
    return f;
}

uint64_t fingerprint(const Fixture& f) {
    uint64_t h = UINT64_C(14695981039346656037);
    for (uint8_t b : f.bytes) h = (h ^ b) * UINT64_C(1099511628211);
    for (int64_t v : f.expected) {
        const uint64_t bits = std::bit_cast<uint64_t>(v);
        for (unsigned i = 0; i < 8; ++i) h = (h ^ ((bits >> (i * 8)) & 255)) * UINT64_C(1099511628211);
    }
    return h;
}

void validate(Fixture& f) {
    f.output.resize(f.expected.size());
    for (bool partial : {false, true}) {
        std::fill(f.output.begin(), f.output.end(), 0);
        rugo::orc::IntRle decoder(f.bytes.data(), f.bytes.size(), true, f.is_signed);
        size_t pos = 0, step = 0;
        constexpr std::array<size_t, 5> chunks = {1, 7, 509, 3, 257};
        while (pos < f.output.size()) {
            const size_t n = partial ? std::min(chunks[step++ % chunks.size()], f.output.size() - pos)
                                     : f.output.size() - pos;
            decoder.read(f.output.data() + pos, n);
            pos += n;
        }
        for (size_t i = 0; i < f.expected.size(); ++i) {
            if (f.output[i] != f.expected[i]) {
                std::fprintf(stderr, "%s %s mismatch at %zu: got %lld expected %lld\n", f.name.c_str(),
                             partial ? "partial" : "bulk", i, static_cast<long long>(f.output[i]),
                             static_cast<long long>(f.expected[i]));
                std::exit(1);
            }
        }
    }
}

unsigned parse_positive(const char* text) {
    char* end = nullptr;
    const unsigned long n = std::strtoul(text, &end, 10);
    if (*text == '\0' || *end != '\0' || n == 0 || n > 100000) {
        std::fprintf(stderr, "invalid positive integer: %s\n", text);
        std::exit(2);
    }
    return static_cast<unsigned>(n);
}

} // namespace

int main(int argc, char** argv) {
    if (argc > 3) {
        std::fprintf(stderr, "usage: %s [rounds=7] [iterations=128]\n", argv[0]);
        return 2;
    }
    const unsigned rounds = argc > 1 ? parse_positive(argv[1]) : 7;
    const unsigned iterations = argc > 2 ? parse_positive(argv[2]) : 128;
    std::vector<Fixture> cases;
    for (unsigned w : widths) cases.push_back(direct(w, true));
    for (unsigned w : {1u, 8u, 16u, 32u, 56u}) cases.push_back(direct(w, false));
    cases.push_back(delta(0, false));
    for (unsigned w : widths) if (w != 1) cases.push_back(delta(w, false));
    for (unsigned w : {2u, 8u, 16u, 32u}) cases.push_back(delta(w, true));
    for (unsigned w : widths) if (w <= 56) cases.push_back(patched(w, 4));
    for (unsigned w : {1u, 8u, 16u, 32u})
        for (unsigned p : {1u, 8u, 16u, 24u}) cases.push_back(patched(w, p));

    for (Fixture& f : cases) validate(f);
    std::printf("ORC IntRle v2: %zu cases, %zu runs/case, lengths 512/511/128/127, %u rounds, %u iterations/round\n",
                cases.size(), runs, rounds, iterations);
    std::printf("All bulk and partial-read outputs verified; times include full decode and memcpy.\n");
    std::printf("%-22s %8s %8s %16s %10s %10s %10s\n", "case", "values", "bytes", "fixture-fnv64", "median ns/v", "min ns/v", "max ns/v");

    uint64_t sink = 0;
    // Warm up every case before measurements; consume output on every iteration.
    for (unsigned round = 0; round <= rounds; ++round) {
        for (size_t k = 0; k < cases.size(); ++k) {
            Fixture& f = cases[round % 2 == 0 ? k : cases.size() - 1 - k];
            const auto start = Clock::now();
            for (unsigned i = 0; i < iterations; ++i) {
                rugo::orc::IntRle decoder(f.bytes.data(), f.bytes.size(), true, f.is_signed);
                decoder.read(f.output.data(), f.output.size());
                // Both supported production compilers honor a memory clobber.
                asm volatile("" : : "r"(f.output.data()) : "memory");
                sink += std::bit_cast<uint64_t>(f.output[i % f.output.size()]);
            }
            const double ns = std::chrono::duration<double, std::nano>(Clock::now() - start).count();
            if (round != 0) f.samples.push_back(ns / (static_cast<double>(iterations) * f.output.size()));
        }
    }
    for (Fixture& f : cases) {
        std::sort(f.samples.begin(), f.samples.end());
        const size_t mid = f.samples.size() / 2;
        const double median = f.samples.size() % 2 ? f.samples[mid] : (f.samples[mid - 1] + f.samples[mid]) / 2;
        std::printf("%-22s %8zu %8zu %016llx %10.3f %10.3f %10.3f\n", f.name.c_str(), f.expected.size(),
                    f.bytes.size(), static_cast<unsigned long long>(fingerprint(f)), median,
                    f.samples.front(), f.samples.back());
    }
    std::printf("sink=%llu\n", static_cast<unsigned long long>(sink));
}
