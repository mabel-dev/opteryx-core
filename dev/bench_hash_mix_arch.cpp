// dev/bench_hash_mix_arch.cpp — per-architecture A/B of the hash mixer kernels.
//
// ARCH-AWARE PLAN Phase 1 (docs/ARCH_AWARE_PERFORMANCE_TEST_PLAN.md). On
// 2026-10-04 this measured the hand-written AVX2 mixers slower than the
// compiler-vectorized scalar loop on x86 (i5-8500, gcc 12), and they were
// deleted; NEON hand mixers had already gone the same way on M-series. What is
// left to measure is RVV against the scalar loop on riscv64 (and the scalar loop
// itself on any new host). Times every kernel compiled for the host, interleaved
// with alternating order, min of N, at an L2-resident and a DRAM working set,
// and checks the outputs are byte-identical.
//
// The kernels are pulled in by including the TU itself, so the bench times the
// exact production code (they are file-static). Build with -fno-tree-vectorize
// (gcc) / -fno-vectorize -fno-slp-vectorize (clang) to see the truly scalar loop.
//
// Build with the production flags for the host, e.g.
//   x86:     g++ -O3 -std=c++20 -march=haswell -mtune=generic -I. -Idraken/simd -Idraken
//                dev/bench_hash_mix_arch.cpp -o /tmp/bench_hash_mix && /tmp/bench_hash_mix
//   arm64:   clang++ -O3 -std=c++20 -I. -Idraken/simd -Idraken
//                dev/bench_hash_mix_arch.cpp -o /tmp/bench_hash_mix && /tmp/bench_hash_mix
//   riscv64: g++ -O3 -std=c++20 -march=rv64gcv -I. -Idraken/simd -Idraken
//                dev/bench_hash_mix_arch.cpp -o /tmp/bench_hash_mix && /tmp/bench_hash_mix

#include "draken/simd/simd_hash.cpp"

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <random>
#include <string>
#include <vector>

namespace {

using MixFn = void (*)(uint64_t*, const uint64_t*, std::size_t);
using HashFn = void (*)(const uint64_t*, uint64_t*, std::size_t);

struct MixArm { const char* name; MixFn fn; };
struct HashArm { const char* name; HashFn fn; };

double now_ns() {
    return static_cast<double>(std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count());
}

void bench_mix(const std::vector<MixArm>& arms, std::size_t n, int reps) {
    std::mt19937_64 rng(42);
    std::vector<uint64_t> values(n), seed(n), dest(n);
    for (auto& v : values) v = rng();
    for (auto& v : seed) v = rng();

    std::vector<std::vector<uint64_t>> outputs;
    for (const auto& arm : arms) {
        std::memcpy(dest.data(), seed.data(), n * 8);
        arm.fn(dest.data(), values.data(), n);
        outputs.push_back(dest);
    }
    for (std::size_t a = 1; a < arms.size(); ++a) {
        if (outputs[a] != outputs[0]) {
            std::fprintf(stderr, "MISMATCH: mix %s differs from %s\n", arms[a].name, arms[0].name);
            std::exit(1);
        }
    }

    std::vector<double> best(arms.size(), 1e300);
    for (int r = 0; r < reps; ++r) {
        for (std::size_t k = 0; k < arms.size(); ++k) {
            const std::size_t a = (r % 2 == 0) ? k : arms.size() - 1 - k;
            std::memcpy(dest.data(), seed.data(), n * 8);
            const double t0 = now_ns();
            arms[a].fn(dest.data(), values.data(), n);
            const double t = now_ns() - t0;
            if (t < best[a]) best[a] = t;
        }
    }
    for (std::size_t a = 0; a < arms.size(); ++a) {
        std::printf("  mix   n=%-9zu %-12s %7.3f ns/value  (x%.3f vs %s)\n", n, arms[a].name,
                    best[a] / n, best[a] / best[0], arms[0].name);
    }
}

void bench_hash(const std::vector<HashArm>& arms, std::size_t n, int reps) {
    std::mt19937_64 rng(7);
    std::vector<uint64_t> src(n), dst(n);
    for (auto& v : src) v = rng();

    std::vector<std::vector<uint64_t>> outputs;
    for (const auto& arm : arms) {
        arm.fn(src.data(), dst.data(), n);
        outputs.push_back(dst);
    }
    for (std::size_t a = 1; a < arms.size(); ++a) {
        if (outputs[a] != outputs[0]) {
            std::fprintf(stderr, "MISMATCH: hash_i64 %s differs from %s\n", arms[a].name, arms[0].name);
            std::exit(1);
        }
    }

    std::vector<double> best(arms.size(), 1e300);
    for (int r = 0; r < reps; ++r) {
        for (std::size_t k = 0; k < arms.size(); ++k) {
            const std::size_t a = (r % 2 == 0) ? k : arms.size() - 1 - k;
            const double t0 = now_ns();
            arms[a].fn(src.data(), dst.data(), n);
            const double t = now_ns() - t0;
            if (t < best[a]) best[a] = t;
        }
    }
    for (std::size_t a = 0; a < arms.size(); ++a) {
        std::printf("  hash  n=%-9zu %-12s %7.3f ns/value  (x%.3f vs %s)\n", n, arms[a].name,
                    best[a] / n, best[a] / best[0], arms[0].name);
    }
}

}  // namespace

int main(int argc, char** argv) {
    const int reps = argc > 1 ? std::atoi(argv[1]) : 15;

    std::vector<MixArm> mix_arms = {{"scalar_x8", simd_mix_hash_scalar}};
    std::vector<HashArm> hash_arms = {{"scalar_x8", simd_hash_i64_scalar}};
#if defined(__riscv) && defined(__riscv_vector)
    mix_arms.push_back({"rvv", simd_mix_hash_rvv});
    hash_arms.push_back({"rvv", simd_hash_i64_rvv});
#endif

    // 1 MiB of uint64 (L2-resident on every target) and 64 MiB (DRAM).
    for (std::size_t n : {std::size_t(1) << 17, std::size_t(1) << 23}) {
        bench_mix(mix_arms, n, reps);
        bench_hash(hash_arms, n, reps);
    }
    return 0;
}
