// μ-bench 1: CarcharIndex find_or_insert_id throughput vs table size.
//
// Question: at what table size does ns/op start climbing, and how steeply?
// A flat curve = CPU-bound, radix partitioning can't help.
// A cliff at L2/L3 boundary = cache-bound, partitioning could in principle help.
//
// Build:
//   c++ -std=c++20 -O3 -march=native \
//       -I third_party/mabel/carchar \
//       scratch/_carchar_size_curve.cpp -o scratch/_carchar_size_curve
// Run:
//   scratch/_carchar_size_curve
//
// Method:
// - For each N in the size ladder: generate N random uint64 keys (deterministic
//   seed), insert them all into a fresh CarcharIndex via find_or_insert_id with
//   a monotonic group-id payload — exactly the operation the engine does.
// - Time only the insert loop. No pre-reservation; let the table grow exactly
//   like the engine does.
// - Repeat the run K times, report best wall-time and derived ns/op.
// - Print probe statistics from stats() so we can sanity-check that probe
//   length isn't itself climbing with N (which would mask cache effects).

#include "carchar_index.hpp"

#include <chrono>
#include <cstdint>
#include <cstdio>
#include <random>
#include <vector>

using opteryx::carchar::CarcharIndex;

static std::vector<std::uint64_t> make_keys(std::size_t n, std::uint64_t seed) {
    // splitmix64 — deterministic and fast, produces well-distributed uint64s.
    std::vector<std::uint64_t> keys(n);
    std::uint64_t x = seed;
    for (std::size_t i = 0; i < n; ++i) {
        x += 0x9E3779B97F4A7C15ULL;
        std::uint64_t z = x;
        z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ULL;
        z = (z ^ (z >> 27)) * 0x94D049BB133111EBULL;
        z = z ^ (z >> 31);
        keys[i] = z;
    }
    return keys;
}

struct RunResult {
    double ns_per_op = 0.0;
    std::size_t capacity = 0;
    std::size_t resize_count = 0;
    double avg_insert_probes = 0.0;
    std::size_t max_insert_probes = 0;
};

static RunResult run_once(const std::vector<std::uint64_t>& keys) {
    CarcharIndex idx{};
    const auto t0 = std::chrono::steady_clock::now();
    std::int64_t payload_out = -1;
    for (std::size_t i = 0; i < keys.size(); ++i) {
        idx.find_or_insert_id(keys[i], static_cast<std::int64_t>(i), payload_out);
    }
    const auto t1 = std::chrono::steady_clock::now();
    const auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count();

    const auto s = idx.stats();
    RunResult r;
    r.ns_per_op = static_cast<double>(ns) / static_cast<double>(keys.size());
    r.capacity = s.capacity;
    r.resize_count = s.resize_count;
    r.avg_insert_probes = s.insert_count > 0
        ? static_cast<double>(s.insert_total_probes) / static_cast<double>(s.insert_count)
        : 0.0;
    r.max_insert_probes = s.max_insert_probe_length;
    return r;
}

int main() {
    // Size ladder. We want to span L1 (~64-128KB private), L2 (Apple Silicon
    // ~16MB shared), and DRAM (>>L2). Slots are ~17 bytes each (1 tag + 8 hash
    // + 8 payload) plus the empty-tail overhead. 4M keys ≈ 5M slots at 0.80
    // load ≈ 85MB, comfortably out of L2.
    const std::size_t sizes[] = {
        1024, 4096, 16384, 65536, 262144,
        1u << 20, 1u << 22, 1u << 23, 1u << 24,
        1u << 25, 1u << 26,  // 33M, 67M — Q33 territory (51M unique groups)
    };
    const int reps = 3;

    std::printf("%10s  %10s  %10s  %10s  %10s  %s\n",
                "N", "ns/op", "capacity", "resizes", "avg_probes", "max_probes");
    for (auto n : sizes) {
        auto keys = make_keys(n, 0xC0FFEEULL);

        RunResult best;
        best.ns_per_op = 1e18;
        for (int r = 0; r < reps; ++r) {
            auto rr = run_once(keys);
            if (rr.ns_per_op < best.ns_per_op) {
                best = rr;
            }
        }
        std::printf("%10zu  %10.1f  %10zu  %10zu  %10.2f  %zu\n",
                    n, best.ns_per_op, best.capacity, best.resize_count,
                    best.avg_insert_probes, best.max_insert_probes);
    }
    return 0;
}
