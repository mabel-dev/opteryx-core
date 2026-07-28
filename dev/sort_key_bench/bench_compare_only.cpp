// Isolate whether unsigned __int128 comparison itself is the expensive part,
// independent of sort/cache effects — random-access reads into a large array,
// summing comparison results (to prevent the optimizer discarding the loop).
#include <chrono>
#include <cstdio>
#include <cstdint>
#include <random>
#include <vector>

int main() {
    const size_t N = 5'000'000;
    std::mt19937_64 rng(42);
    std::vector<unsigned __int128> packed(N);
    std::vector<uint64_t> hi(N), lo(N);
    for (size_t i = 0; i < N; ++i) {
        uint64_t h = rng(), l = rng();
        hi[i] = h; lo[i] = l;
        packed[i] = (static_cast<unsigned __int128>(h) << 64) | l;
    }
    std::vector<uint32_t> idx_a(N), idx_b(N);
    for (size_t i = 0; i < N; ++i) { idx_a[i] = rng() % N; idx_b[i] = rng() % N; }

    auto t0 = std::chrono::steady_clock::now();
    size_t count128 = 0;
    for (size_t i = 0; i < N; ++i)
        if (packed[idx_a[i]] < packed[idx_b[i]]) ++count128;
    auto t1 = std::chrono::steady_clock::now();

    size_t count64 = 0;
    for (size_t i = 0; i < N; ++i) {
        uint32_t a = idx_a[i], b = idx_b[i];
        if (hi[a] != hi[b] ? hi[a] < hi[b] : lo[a] < lo[b]) ++count64;
    }
    auto t2 = std::chrono::steady_clock::now();

    printf("int128 compare:      %.3fms  (count=%zu)\n",
           std::chrono::duration<double, std::milli>(t1 - t0).count(), count128);
    printf("2x uint64 compare:   %.3fms  (count=%zu)\n",
           std::chrono::duration<double, std::milli>(t2 - t1).count(), count64);
    return 0;
}
