#include "memory_pool.hpp"
#include <chrono>
#include <cstring>
#include <cstdio>
#include <vector>
#include <random>

using namespace opteryx;
using hrclock = std::chrono::high_resolution_clock;

static double elapsed_ms(hrclock::time_point t0) {
    return std::chrono::duration<double, std::milli>(hrclock::now() - t0).count();
}

// ── helpers ──────────────────────────────────────────────────────────────────

static unsigned char g_data[4 << 20]; // 4 MiB scratch buffer

void fill_data(int64_t size) {
    std::memset(g_data, 0xAB, std::min(size, (int64_t)sizeof(g_data)));
}

// ── bench 1: sequential alloc / free (measures commit + release throughput) ──

void bench_sequential(int64_t pool_size, int64_t alloc_size, int iters) {
    MemoryPool pool(pool_size, "bench", true);
    fill_data(alloc_size);

    auto t0 = hrclock::now();
    for (int i = 0; i < iters; i++) {
        int64_t ref = pool.commit(g_data, alloc_size);
        pool.release(ref);
    }
    double ms = elapsed_ms(t0);
    printf("sequential  alloc=%6lld  iters=%7d  %.2f ms  (%.0f ops/s)\n",
           (long long)alloc_size, iters, ms, iters / (ms / 1000.0));
}

// ── bench 2: fragmentation churn — alloc many, free every other, alloc again ─
// This is what exercises coalesce-on-release hardest.

void bench_fragmentation(int64_t pool_size, int64_t alloc_size, int slots) {
    MemoryPool pool(pool_size, "bench", false);
    fill_data(alloc_size);

    std::vector<int64_t> refs(slots);
    for (int i = 0; i < slots; i++) {
        refs[i] = pool.commit(g_data, alloc_size);
    }

    // Free every other slot — creates alternating used/free pattern
    auto t0 = hrclock::now();
    for (int i = 0; i < slots; i += 2) {
        pool.release(refs[i]);
        refs[i] = -1;
    }
    // Re-fill the freed slots
    for (int i = 0; i < slots; i += 2) {
        refs[i] = pool.commit(g_data, alloc_size);
    }
    double ms = elapsed_ms(t0);

    // Cleanup
    for (int i = 0; i < slots; i++) {
        if (refs[i] != -1) pool.release(refs[i]);
    }

    printf("fragchurn   alloc=%6lld  slots=%5d  %.2f ms\n",
           (long long)alloc_size, slots, ms);
}

// ── bench 3: mixed sizes — simulates morsel workload (mostly large, some small)

void bench_mixed(int64_t pool_size, int iters) {
    MemoryPool pool(pool_size, "bench", true);

    std::mt19937 rng(42);
    // ~80% large (64KB–2MB), ~20% small (256B–4KB)
    std::uniform_int_distribution<int64_t> large_dist(64 << 10, 2 << 20);
    std::uniform_int_distribution<int64_t> small_dist(256, 4 << 10);
    std::uniform_int_distribution<int> kind(0, 4);

    std::vector<int64_t> live;
    live.reserve(64);

    auto t0 = hrclock::now();
    for (int i = 0; i < iters; i++) {
        int64_t sz = (kind(rng) == 0) ? small_dist(rng) : large_dist(rng);
        sz = std::min(sz, (int64_t)sizeof(g_data));
        fill_data(sz);
        int64_t ref = pool.commit(g_data, sz);
        if (ref >= 0) live.push_back(ref);

        // Release oldest when we have several live
        if (live.size() > 8) {
            pool.release(live.front());
            live.erase(live.begin());
        }
    }
    double ms = elapsed_ms(t0);
    for (auto r : live) pool.release(r);

    printf("mixed       pool=%6lldMB  iters=%7d  %.2f ms  (%.0f ops/s)\n",
           (long long)(pool_size >> 20), iters, ms, iters / (ms / 1000.0));
}

// ── bench 4: latch / read / unlatch throughput ────────────────────────────────

void bench_read(int64_t pool_size, int64_t alloc_size, int iters) {
    MemoryPool pool(pool_size, "bench", false);
    fill_data(alloc_size);
    int64_t ref = pool.commit(g_data, alloc_size);

    auto t0 = hrclock::now();
    for (int i = 0; i < iters; i++) {
        ReadResult r = pool.read(ref, false);
        (void)r;
    }
    double ms = elapsed_ms(t0);
    pool.release(ref);

    printf("read        alloc=%6lld  iters=%7d  %.2f ms  (%.0f ops/s)\n",
           (long long)alloc_size, iters, ms, iters / (ms / 1000.0));
}

int main() {
    printf("=== memory pool benchmark ===\n\n");

    printf("-- sequential commit/release --\n");
    bench_sequential(256 << 20,   1 << 10,  500000);  // 256MB pool, 1KB allocs
    bench_sequential(256 << 20,  64 << 10,   50000);  // 64KB allocs
    bench_sequential(256 << 20,   1 << 20,    5000);  // 1MB allocs

    printf("\n-- fragmentation churn (coalesce stress) --\n");
    bench_fragmentation(256 << 20,   4 << 10,  1000);  // 4KB, 1000 slots
    bench_fragmentation(256 << 20,  64 << 10,   500);  // 64KB, 500 slots
    bench_fragmentation(256 << 20,   1 << 20,   100);  // 1MB, 100 slots

    printf("\n-- mixed size morsel workload --\n");
    bench_mixed(512 << 20, 10000);
    bench_mixed(512 << 20, 50000);

    printf("\n-- read throughput (no segments access) --\n");
    bench_read(256 << 20,  64 << 10, 2000000);
    bench_read(256 << 20,   1 << 20,  500000);

    return 0;
}
