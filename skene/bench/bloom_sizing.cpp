// What does a bloom filter actually cost, and what does it actually deliver?
//
// Blooms are ~9-18% of a file once every column gets one, so their sizing is no
// longer a detail. Two things are worth knowing and neither was measured:
//
//   1. bits per distinct value against the theoretical figure. The sizing uses
//      the CLASSIC Bloom formula, bits = -n*ln(p)/ln(2)^2, then rounds the block
//      count UP TO A POWER OF TWO. That rounding is invisible in the formula and
//      overshoots by up to 2x.
//
//   2. the FALSE POSITIVE RATE actually delivered. A split-block filter is not a
//      classic Bloom filter — confining all of a key's bits to one 32-byte block
//      trades accuracy for cache behaviour, so classic sizing does not give the
//      classic rate. The existing test only bounds it at 10%, ten times the
//      target, so the real number is unknown.
//
// These interact: if the delivered rate is worse than requested, the power-of-two
// overshoot has been silently compensating, and removing it would make accuracy
// worse rather than just making files smaller.
//
// NOT part of libskene.a. See bench/README.md.

#include <cmath>
#include <cstdio>
#include <vector>

#include "bloom.h"

#include "core/buffers.h"

using namespace skene;

namespace {

// A dense int64 vector over `values`, borrowed — no ownership, this is a bench.
DrakenVector view_of(const std::vector<int64_t>& values,
                     const std::vector<uint32_t>& identity) {
    DrakenVector v{};
    v.data        = const_cast<int64_t*>(values.data());
    v.selection   = identity.data();
    v.data_length = static_cast<uint32_t>(values.size());
    v.length      = static_cast<uint32_t>(values.size());
    v.validity    = nullptr;
    v.type        = DRAKEN_INT64;
    return v;
}

void measure(uint32_t ndv, double target) {
    std::vector<int64_t> values(ndv);
    std::vector<uint32_t> identity(ndv);
    for (uint32_t i = 0; i < ndv; ++i) {
        values[i]   = static_cast<int64_t>(i) * 2;   // evens present
        identity[i] = i;
    }

    std::vector<uint8_t> filter;
    const DrakenVector v = view_of(values, identity);
    if (!bloom_build(v, target, &filter)) { std::printf("  build declined\n"); return; }

    // Odds are all absent, so every acceptance is a false positive.
    const uint32_t probes = ndv * 4u;
    uint32_t accepted = 0;
    for (uint32_t i = 0; i < probes; ++i) {
        const int64_t absent = static_cast<int64_t>(i) * 2 + 1;
        bool may = false;
        if (!bloom_probe(filter.data(), filter.size(), &absent, sizeof(absent), &may)
                 .is_ok()) {
            std::printf("  probe failed\n");
            return;
        }
        if (may) ++accepted;
    }

    // Every present value must still be accepted — a bloom that rejects a
    // present value is not a tuning problem, it is data loss.
    for (uint32_t i = 0; i < ndv; ++i) {
        bool may = false;
        bloom_probe(filter.data(), filter.size(), &values[i], sizeof(int64_t), &may);
        if (!may) { std::printf("  BROKEN: rejected a present value\n"); return; }
    }

    const double ln2 = 0.6931471805599453;
    const double ideal_bits = -static_cast<double>(ndv) * std::log(target) / (ln2 * ln2);
    const double actual_bits = 8.0 * static_cast<double>(filter.size());
    const double observed = static_cast<double>(accepted) / static_cast<double>(probes);

    std::printf("  %8u  %9zu  %7.1f  %7.1f  %6.2fx  %8.3f%%  %6.1fx\n",
                ndv, filter.size(),
                actual_bits / static_cast<double>(ndv),
                ideal_bits / static_cast<double>(ndv),
                actual_bits / ideal_bits,
                100.0 * observed,
                observed / target);
}

// ─── The join filter's construction, for comparison ─────────────────────────
//
// opteryx's join-side bloom (src/cpp/bloom_filter_ops.hpp) is a CLASSIC Bloom
// filter with k=2, the second bit position derived from the first by a golden
// ratio multiply rather than a second hash:
//
//   a = h & mask
//   b = (h * 0x9E3779B97F4A7C15) & mask
//
// Two differences from skene's split-block filter matter. It sets 2 bits, not 8,
// and it scatters them across the WHOLE array rather than confining them to one
// 32-byte block. The first governs bits-per-key; the second governs cache misses
// per probe — two scattered touches against one block.
constexpr uint64_t kGoldenRatio = 0x9E3779B97F4A7C15ULL;

// Bits/key needed for a target rate, from the classic k=2 model:
//   fpp = (1 - e^(-2n/m))^2   =>   m/n = -2 / ln(1 - sqrt(fpp))
double golden_bits_per_key(double fpp) {
    return -2.0 / std::log(1.0 - std::sqrt(fpp));
}

void measure_golden(uint32_t ndv, double target) {
    // Power-of-two bit array, as the join filter requires for its mask.
    uint64_t bits = static_cast<uint64_t>(
        std::ceil(static_cast<double>(ndv) * golden_bits_per_key(target)));
    uint64_t sized = 64;
    while (sized < bits) sized <<= 1;
    const uint64_t mask = sized - 1u;
    std::vector<uint64_t> array(static_cast<size_t>(sized / 64u), 0);

    auto touch = [&](int64_t value, bool insert) {
        uint64_t h = 0;
        bloom_hash_value(&value, sizeof(value), &h);
        const uint64_t a = h & mask;
        const uint64_t b = (h * kGoldenRatio) & mask;
        if (insert) {
            array[a >> 6] |= uint64_t(1) << (a & 63u);
            array[b >> 6] |= uint64_t(1) << (b & 63u);
            return true;
        }
        return ((array[a >> 6] >> (a & 63u)) & 1u)
            && ((array[b >> 6] >> (b & 63u)) & 1u);
    };

    for (uint32_t i = 0; i < ndv; ++i) touch(static_cast<int64_t>(i) * 2, true);

    const uint32_t probes = ndv * 4u;
    uint32_t accepted = 0;
    for (uint32_t i = 0; i < probes; ++i)
        if (touch(static_cast<int64_t>(i) * 2 + 1, false)) ++accepted;

    const double observed = static_cast<double>(accepted) / static_cast<double>(probes);
    std::printf("  %8u  %9llu  %7.1f  %8.3f%%\n", ndv,
                static_cast<unsigned long long>(sized / 8u),
                static_cast<double>(sized) / static_cast<double>(ndv),
                100.0 * observed);
}

}  // namespace

int main() {
    const double targets[] = {0.001, 0.005, 0.01, 0.02, 0.05, 0.10, 0.25};
    const uint32_t sizes[] = {1000, 5000, 10000, 40000, 100000, 300000};

    for (double target : targets) {
        std::printf("\ntarget FPR %.0f%%\n", 100.0 * target);
        std::printf("  %8s  %9s  %7s  %7s  %6s  %9s  %7s\n",
                    "ndv", "bytes", "bits/n", "ideal", "waste", "observed", "vs want");
        for (uint32_t ndv : sizes) measure(ndv, target);
    }

    std::printf("\n\n=== join-filter construction (classic, k=2, golden ratio) ===\n");
    for (double target : targets) {
        if (target != 0.01 && target != 0.05) continue;
        std::printf("\ntarget FPR %.0f%%   (skene SBBF needs %.1f bits/key)\n",
                    100.0 * target, target == 0.01 ? 10.7 : 7.3);
        std::printf("  %8s  %9s  %7s  %9s\n", "ndv", "bytes", "bits/n", "observed");
        for (uint32_t ndv : sizes) measure_golden(ndv, target);
    }
    return 0;
}
