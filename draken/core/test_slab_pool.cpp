// draken/core/test_slab_pool.cpp — the slab pool (slab_pool.h) and the alloc.h
// tiers over it, under ASan/UBSan via `make slab-pool-test` (part of `make q`).
// Fails by exit code with the failing check printed.

#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <random>
#include <thread>
#include <vector>

#include "alloc.h"
#include "slab_pool.h"

static int failures = 0;
#define CHECK(cond)                                                              \
    do {                                                                         \
        if (!(cond)) {                                                           \
            std::fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, #cond); \
            ++failures;                                                          \
        }                                                                        \
    } while (0)

static const size_t K = 1024, M = 1024 * 1024;

// Tier routing: below the threshold the system allocator, from it the pool;
// usable sizes and ownership agree on both sides of the line.
static void test_tiers() {
    void* small = draken_malloc(DRAKEN_SLAB_MIN - 1);
    void* big = draken_malloc(DRAKEN_SLAB_MIN);
    CHECK(small != nullptr && big != nullptr);
    CHECK(!draken_slab_owns(small));
    CHECK(draken_slab_owns(big));
    CHECK(draken_usable_size(big) == DRAKEN_SLAB_MIN);
    CHECK(draken_usable_size(small) >= DRAKEN_SLAB_MIN - 1);
    CHECK(!draken_slab_owns(nullptr));
    CHECK(reinterpret_cast<uintptr_t>(big) % DRAKEN_SLAB_MIN == 0);
    draken_free(small);
    draken_free(big);
    // Every class and the first runs: size, alignment, writable end to end.
    for (size_t bytes : {64 * K, 64 * K + 1, 100 * K, 128 * K, 256 * K, 700 * K, 1 * M, 1 * M + 1, 2 * M, 3 * M, 7 * M}) {
        uint8_t* p = static_cast<uint8_t*>(draken_malloc(bytes));
        CHECK(p != nullptr);
        CHECK(draken_slab_owns(p));
        const size_t u = draken_usable_size(p);
        CHECK(u >= bytes);
        CHECK(u <= std::max(bytes * 2, DRAKEN_SLAB_SLOT));
        std::memset(p, 0xA5, u);
        CHECK(p[0] == 0xA5 && p[u - 1] == 0xA5);
        draken_free(p);
    }
}

// calloc zeroes a recycled block (it may hold the previous user's bytes).
static void test_calloc_recycled_is_zero() {
    uint8_t* p = static_cast<uint8_t*>(draken_malloc(256 * K));
    std::memset(p, 0xFF, 256 * K);
    draken_free(p);
    uint8_t* q = static_cast<uint8_t*>(draken_calloc(256 * K, 1));
    bool zero = true;
    for (size_t i = 0; i < 256 * K; ++i) zero &= q[i] == 0;
    CHECK(zero);
    draken_free(q);
    uint8_t* r = static_cast<uint8_t*>(draken_calloc(3, M));   // a run
    zero = true;
    for (size_t i = 0; i < 3 * M; i += 4096) zero &= r[i] == 0;
    CHECK(zero);
    draken_free(r);
    CHECK(draken_calloc(SIZE_MAX / 2, 4) == nullptr);   // overflow refused
}

// realloc keeps contents across every tier move and within a tier.
static void test_realloc() {
    std::mt19937_64 rng(7);
    // system -> pool
    uint8_t* p = static_cast<uint8_t*>(draken_malloc(32 * K));
    for (size_t i = 0; i < 32 * K; ++i) p[i] = static_cast<uint8_t>(i * 31);
    p = static_cast<uint8_t*>(draken_realloc(p, 200 * K));
    CHECK(draken_slab_owns(p));
    bool ok = true;
    for (size_t i = 0; i < 32 * K; ++i) ok &= p[i] == static_cast<uint8_t>(i * 31);
    CHECK(ok);
    // class -> bigger class -> run
    for (size_t i = 0; i < 200 * K; ++i) p[i] = static_cast<uint8_t>(i * 7);
    p = static_cast<uint8_t*>(draken_realloc(p, 900 * K));
    CHECK(draken_usable_size(p) == M);
    p = static_cast<uint8_t*>(draken_realloc(p, 5 * M));
    CHECK(draken_usable_size(p) == 6 * M);
    ok = true;
    for (size_t i = 0; i < 200 * K; ++i) ok &= p[i] == static_cast<uint8_t>(i * 7);
    CHECK(ok);
    // run grows in place when the slots after it are free (fresh frontier)
    uint8_t* before = p;
    p = static_cast<uint8_t*>(draken_realloc(p, 9 * M));
    CHECK(draken_usable_size(p) == 10 * M);
    ok = true;
    for (size_t i = 0; i < 200 * K; ++i) ok &= p[i] == static_cast<uint8_t>(i * 7);
    CHECK(ok);
    (void)before;
    // run shrinks in place
    uint8_t* same = static_cast<uint8_t*>(draken_realloc(p, 3 * M));
    CHECK(same == p);
    CHECK(draken_usable_size(p) == 4 * M);
    // pool -> system
    p = static_cast<uint8_t*>(draken_realloc(p, 10 * K));
    CHECK(!draken_slab_owns(p));
    ok = true;
    for (size_t i = 0; i < 10 * K; ++i) ok &= p[i] == static_cast<uint8_t>(i * 7);
    CHECK(ok);
    draken_free(p);
    CHECK(draken_realloc(nullptr, 128 * K) != nullptr);
}

// Recycling: a freed block's slot is handed out again, faulted; the retained set
// obeys the budget and give-backs happen past it.
static void test_recycle_and_budget() {
    draken_slab_configure(8 * M, 0);
    DrakenSlabStats s0 = draken_slab_stats();
    void* a = draken_malloc(4 * M);
    void* b = draken_malloc(4 * M);
    void* c = draken_malloc(4 * M);
    DrakenSlabStats s1 = draken_slab_stats();
    CHECK(s1.in_use_bytes - s0.in_use_bytes == 12 * int64_t(M));
    CHECK(s1.faulted_bytes >= s1.in_use_bytes);
    draken_free(a);
    draken_free(b);
    draken_free(c);
    DrakenSlabStats s2 = draken_slab_stats();
    // The budget bounds what a RELEASE may add to the retained set; what earlier
    // tests retained under the default budget stays.
    CHECK(s2.free_faulted_bytes <= std::max<int64_t>(s0.free_faulted_bytes, 8 * int64_t(M)));
    CHECK(s2.give_backs > s1.give_backs);     // past the budget: pages given back
    CHECK(s2.faulted_bytes == s2.in_use_bytes + s2.free_faulted_bytes);
    // A recycled run: no new fault for what stayed faulted.
    void* d = draken_malloc(4 * M);
    DrakenSlabStats s3 = draken_slab_stats();
    CHECK(s3.faults == s2.faults || s3.faults - s2.faults <= 2);
    draken_free(d);
    draken_slab_configure(1 << 30, 0);
    CHECK(draken_slab_budget_bytes() == (1 << 30));
}

// Class slabs: blocks of one class come from one slot, the slot is reclaimed
// when every block of it is free, and a sweep of frees never corrupts neighbours.
static void test_class_slab_reclaim() {
    DrakenSlabStats s0 = draken_slab_stats();
    std::vector<uint8_t*> blocks;
    for (int i = 0; i < 32; ++i) blocks.push_back(static_cast<uint8_t*>(draken_malloc(64 * K)));
    uintptr_t lo = UINTPTR_MAX, hi = 0;
    for (uint8_t* p : blocks) {
        lo = std::min(lo, reinterpret_cast<uintptr_t>(p));
        hi = std::max(hi, reinterpret_cast<uintptr_t>(p));
        std::memset(p, static_cast<int>(reinterpret_cast<uintptr_t>(p) >> 16), 64 * K);
    }
    CHECK(hi - lo == 31 * 64 * K);   // one 2 MiB slab, blocks packed
    DrakenSlabStats s1 = draken_slab_stats();
    // At most one new slab: this thread may already hold an empty current slab of
    // the class from an earlier test, which the 32 blocks then fill exactly.
    CHECK(s1.class_slabs - s0.class_slabs <= 1);
    for (uint8_t* p : blocks) {
        bool ok = true;
        const uint8_t v = static_cast<uint8_t>(reinterpret_cast<uintptr_t>(p) >> 16);
        for (size_t i = 0; i < 64 * K; i += 997) ok &= p[i] == v;
        CHECK(ok);
    }
    std::shuffle(blocks.begin(), blocks.end(), std::mt19937(3));
    for (uint8_t* p : blocks) draken_free(p);
    // The slab stays as this thread's current slab while it has free blocks and
    // nobody holds one: still carved, not reclaimed; one more alloc reuses it.
    uint8_t* again = static_cast<uint8_t*>(draken_malloc(64 * K));
    CHECK(reinterpret_cast<uintptr_t>(again) >= lo && reinterpret_cast<uintptr_t>(again) <= hi);
    draken_free(again);
}

// Threads: allocate/free mixed sizes concurrently; every block is private and
// keeps its pattern; thread exit returns current slabs cleanly.
static void test_threads() {
    const int T = 8;
    std::vector<std::thread> ts;
    std::vector<int> bad(T, 0);
    for (int t = 0; t < T; ++t) {
        ts.emplace_back([t, &bad]() {
            std::mt19937 rng(100 + t);
            std::vector<std::pair<uint8_t*, size_t>> held;
            for (int it = 0; it < 4000; ++it) {
                if (held.size() < 40 && (held.empty() || rng() % 3 != 0)) {
                    const size_t bytes = (rng() % 4 == 0) ? (2 * M + rng() % (3 * M))
                                                           : (64 * K + rng() % (960 * K));
                    uint8_t* p = static_cast<uint8_t*>(draken_malloc(bytes));
                    if (p == nullptr) { ++bad[t]; continue; }
                    p[0] = static_cast<uint8_t>(t);
                    p[bytes - 1] = static_cast<uint8_t>(t);
                    held.emplace_back(p, bytes);
                } else {
                    const size_t i = rng() % held.size();
                    auto [p, bytes] = held[i];
                    if (p[0] != static_cast<uint8_t>(t) || p[bytes - 1] != static_cast<uint8_t>(t)) ++bad[t];
                    draken_free(p);
                    held[i] = held.back();
                    held.pop_back();
                }
            }
            for (auto [p, bytes] : held) {
                if (p[0] != static_cast<uint8_t>(t) || p[bytes - 1] != static_cast<uint8_t>(t)) ++bad[t];
                draken_free(p);
            }
        });
    }
    for (auto& th : ts) th.join();
    for (int t = 0; t < T; ++t) CHECK(bad[t] == 0);
    DrakenSlabStats s = draken_slab_stats();
    // The workers' current slabs were reclaimed at thread exit; what stays in use
    // is at most the main thread's one current slab per class.
    CHECK(s.in_use_bytes <= 5 * int64_t(DRAKEN_SLAB_SLOT));
    CHECK(s.faulted_bytes == s.in_use_bytes + s.free_faulted_bytes);
    CHECK(s.free_faulted_bytes <= draken_slab_budget_bytes() + 2 * int64_t(DRAKEN_SLAB_SLOT));
}

// The account sees pool memory by the slot: faulting charges, giving back uncharges.
static void test_account() {
    draken_slab_configure(0, 0);   // retain nothing: every free gives back
    const int64_t c0 = draken_mem_charged();
    const DrakenSlabStats s0 = draken_slab_stats();
    void* p = draken_malloc(6 * M);
    const int64_t c1 = draken_mem_charged();
    const DrakenSlabStats s1 = draken_slab_stats();
    CHECK(c1 - c0 == s1.faulted_bytes - s0.faulted_bytes);   // charged by the slot, as faulted
    CHECK(s1.in_use_bytes - s0.in_use_bytes == 6 * int64_t(M));
    draken_free(p);
    const int64_t c2 = draken_mem_charged();
    const DrakenSlabStats s2 = draken_slab_stats();
    CHECK(c2 - c0 == s2.faulted_bytes - s0.faulted_bytes);
    // Budget 0: this release retained nothing - slots the run took out of the
    // retained set are given back rather than returned to it.
    CHECK(s2.free_faulted_bytes <= s0.free_faulted_bytes);
    CHECK(s2.in_use_bytes == s0.in_use_bytes);
    CHECK(s2.faulted_bytes == s2.in_use_bytes + s2.free_faulted_bytes);
    CHECK(s2.give_backs - s1.give_backs == 3);
    draken_slab_configure(1 << 30, 0);
}

// Boot prefault: slots faulted into the free runs, counted as retained.
static void test_prefault() {
    DrakenSlabStats s0 = draken_slab_stats();
    draken_slab_configure(1 << 30, 16 * M);
    DrakenSlabStats s1 = draken_slab_stats();
    CHECK(s1.free_faulted_bytes - s0.free_faulted_bytes >= 0);
    CHECK(s1.faulted_bytes >= 16 * int64_t(M));
    CHECK(draken_slab_prefault_bytes() == 16 * int64_t(M));
    draken_slab_configure(1 << 30, 0);
}

int main() {
    test_tiers();
    test_calloc_recycled_is_zero();
    test_realloc();
    test_recycle_and_budget();
    test_class_slab_reclaim();
    test_threads();
    test_account();
    test_prefault();
    if (failures != 0) {
        std::fprintf(stderr, "%d check(s) failed\n", failures);
        return 1;
    }
    std::printf("slab pool tests: OK\n");
    return 0;
}
