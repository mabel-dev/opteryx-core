// μ-bench 2: ablation around CarcharIndex find_or_insert_id.
//
// Q33's engine inner loop (post hashes are computed) is morally:
//   for i in 0..n_rows:
//       _hot_is_new = _index.find_or_insert_id(hashes[i], num_groups, state_idx)
//       if _hot_is_new:
//           _new_row_scratch.push_back(i)
//           num_groups += 1
//       si_buf[i] = state_idx
//
// μ-bench 1 measured the bare hash-table call: ~30 ns/op at 16M keys → ~510ms
// predicted for Q33's 17M rows. Engine attributes 4783ms. So 4+ seconds live
// outside the bare hash table. This bench identifies how much of that 4
// seconds the rest of the loop (push_back + si_buf write + branch) can
// account for.
//
// Variants:
//   A: bare find_or_insert_id (no scratch, no si_buf)
//   B: + conditional push_back to vector<int64_t>
//   C: + si_buf write (int64_t* of length n_rows)
//   D: B + C — the engine's actual shape
//
// Build:
//   c++ -std=c++20 -O3 -march=native \
//       -I third_party/mabel/carchar -I src/cpp \
//       scratch/_carchar_loop_ablation.cpp -o scratch/_carchar_loop_ablation
// Run:
//   scratch/_carchar_loop_ablation

#include "carchar_index.hpp"

#include <chrono>
#include <cstdint>
#include <cstdio>
#include <vector>

using opteryx::carchar::CarcharIndex;

static std::vector<std::uint64_t> make_keys(std::size_t n, std::uint64_t seed) {
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

static double bench_A(const std::vector<std::uint64_t>& keys) {
    CarcharIndex idx{};
    std::int64_t payload_out = -1;
    const auto t0 = std::chrono::steady_clock::now();
    for (std::size_t i = 0; i < keys.size(); ++i) {
        idx.find_or_insert_id(keys[i], static_cast<std::int64_t>(i), payload_out);
    }
    const auto t1 = std::chrono::steady_clock::now();
    return static_cast<double>(std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count()) /
           static_cast<double>(keys.size());
}

static double bench_B(const std::vector<std::uint64_t>& keys) {
    CarcharIndex idx{};
    std::vector<std::int64_t> new_row_scratch;
    new_row_scratch.reserve(0);  // mirror engine — no pre-size
    std::int64_t payload_out = -1;
    std::int64_t num_groups = 0;
    const auto t0 = std::chrono::steady_clock::now();
    for (std::size_t i = 0; i < keys.size(); ++i) {
        const bool is_new = idx.find_or_insert_id(keys[i], num_groups, payload_out);
        if (is_new) {
            new_row_scratch.push_back(static_cast<std::int64_t>(i));
            ++num_groups;
        }
    }
    const auto t1 = std::chrono::steady_clock::now();
    return static_cast<double>(std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count()) /
           static_cast<double>(keys.size());
}

static double bench_C(const std::vector<std::uint64_t>& keys, std::vector<std::int64_t>& si_buf) {
    CarcharIndex idx{};
    std::int64_t payload_out = -1;
    const auto t0 = std::chrono::steady_clock::now();
    for (std::size_t i = 0; i < keys.size(); ++i) {
        idx.find_or_insert_id(keys[i], static_cast<std::int64_t>(i), payload_out);
        si_buf[i] = payload_out;
    }
    const auto t1 = std::chrono::steady_clock::now();
    return static_cast<double>(std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count()) /
           static_cast<double>(keys.size());
}

static double bench_D(const std::vector<std::uint64_t>& keys, std::vector<std::int64_t>& si_buf) {
    CarcharIndex idx{};
    std::vector<std::int64_t> new_row_scratch;
    new_row_scratch.reserve(0);
    std::int64_t payload_out = -1;
    std::int64_t num_groups = 0;
    const auto t0 = std::chrono::steady_clock::now();
    for (std::size_t i = 0; i < keys.size(); ++i) {
        const bool is_new = idx.find_or_insert_id(keys[i], num_groups, payload_out);
        if (is_new) {
            new_row_scratch.push_back(static_cast<std::int64_t>(i));
            ++num_groups;
        }
        si_buf[i] = payload_out;
    }
    const auto t1 = std::chrono::steady_clock::now();
    return static_cast<double>(std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count()) /
           static_cast<double>(keys.size());
}

template <typename Fn>
static double best_of(int reps, Fn fn) {
    double best = 1e18;
    for (int r = 0; r < reps; ++r) {
        double v = fn();
        if (v < best) best = v;
    }
    return best;
}

int main() {
    const std::size_t sizes[] = {1u << 20, 1u << 22, 1u << 24};  // 1M, 4M, 16M
    const int reps = 3;

    std::printf("%10s  %10s  %10s  %10s  %10s\n", "N", "A bare", "B +push", "C +si_buf", "D +both");
    for (auto n : sizes) {
        auto keys = make_keys(n, 0xC0FFEEULL);
        std::vector<std::int64_t> si_buf(n, 0);

        double a = best_of(reps, [&]() { return bench_A(keys); });
        double b = best_of(reps, [&]() { return bench_B(keys); });
        double c = best_of(reps, [&]() { return bench_C(keys, si_buf); });
        double d = best_of(reps, [&]() { return bench_D(keys, si_buf); });

        std::printf("%10zu  %10.1f  %10.1f  %10.1f  %10.1f\n", n, a, b, c, d);
    }
    return 0;
}
