// src/cpp/engine/test_gb_block_store.cpp — GBBlockPool / GBSegArray / GBSegVec / GBArena
// (gb_block_store.hpp). Built TWICE by `make gb-block-store-test`: once per geometry
// (-DOPTERYX_GB_SEG_FIXED=0 and =1), because the two share every caller and must
// agree on every answer. Fails by exit code with the failing check printed.

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include "gb_block_store.hpp"

using namespace opteryx::engine;

static int failures = 0;
#define CHECK(cond)                                                              \
    do {                                                                         \
        if (!(cond)) {                                                           \
            std::fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, #cond); \
            ++failures;                                                          \
        }                                                                        \
    } while (0)

// Values survive growth across many segments; size and indexing agree.
static void test_push_and_index() {
    GBSegVec<uint64_t> v;
    const size_t n = 300000;
    for (size_t i = 0; i < n; ++i) v.push_back(i * 2654435761ULL);
    CHECK(v.size() == n);
    bool ok = true;
    for (size_t i = 0; i < n; ++i) ok &= v[i] == i * 2654435761ULL;
    CHECK(ok);
}

// resize() zero-fills new rows even when the segment is a recycled block full of
// garbage — the lanes' initial state depends on it.
static void test_resize_zeroes_recycled_blocks() {
    {
        GBSegVec<int64_t> dirty;
        dirty.resize(100000);
        for (size_t i = 0; i < 100000; ++i) dirty[i] = -1;
    }   // its blocks go back to the pool, still holding -1
    GBSegVec<int64_t> v;
    v.resize(70000);
    bool zero = true;
    for (size_t i = 0; i < 70000; ++i) zero &= v[i] == 0;
    CHECK(zero);
    for (size_t i = 0; i < 70000; ++i) v[i] = 7;
    v.resize(90000);   // growth keeps old rows, zeroes only the new ones
    bool ok = true;
    for (size_t i = 0; i < 70000; ++i) ok &= v[i] == 7;
    for (size_t i = 70000; i < 90000; ++i) ok &= v[i] == 0;
    CHECK(ok);
}

// A hinted column is one segment in the doubling geometry; the fixed geometry ignores
// the hint. Either way span() returns the right values.
static void test_hint_and_span() {
    GBSegVec<double> v;
    v.hint(50000);
    for (size_t i = 0; i < 50000; ++i) v.push_back(static_cast<double>(i) + 0.5);
#if OPTERYX_GB_SEG_FIXED
    CHECK(v.run_ptr(0, 50000) == nullptr);
#else
    CHECK(v.run_ptr(0, 50000) != nullptr);
#endif
    std::vector<double> scratch;
    const double* p = v.span(1234, 40000, scratch);
    bool ok = true;
    for (size_t i = 0; i < 40000; ++i) ok &= p[i] == static_cast<double>(1234 + i) + 0.5;
    CHECK(ok);
}

// for_each_run covers exactly [start, start+n), in order; copy_out agrees.
static void test_runs_cover_range() {
    GBSegVec<uint32_t> v;
    const size_t n = 200000;
    for (size_t i = 0; i < n; ++i) v.push_back(static_cast<uint32_t>(i));
    std::mt19937_64 rng(42);
    for (int t = 0; t < 2000; ++t) {
        size_t a = rng() % n, b = rng() % n;
        if (a > b) std::swap(a, b);
        size_t next = a, seen = 0;
        bool ok = true;
        v.for_each_run(a, b - a, [&](uint32_t* p, size_t rows) {
            for (size_t j = 0; j < rows; ++j) ok &= p[j] == next + j;
            next += rows;
            seen += rows;
        });
        CHECK(ok && seen == b - a);
        std::vector<uint32_t> out(b - a);
        v.copy_out(a, b - a, out.data());
        bool same = true;
        for (size_t j = 0; j < out.size(); ++j) same &= out[j] == a + j;
        CHECK(same);
    }
}

// Runtime-stride arrays (the key store's raw column) at odd and slot widths.
static void test_runtime_stride() {
    for (size_t stride : {size_t(1), size_t(3), size_t(16), size_t(24)}) {
        GBSegArray<0> a;
        a.set_stride(stride);
        const size_t n = 50000;
        for (size_t i = 0; i < n; ++i) {
            uint8_t* p = a.append();
            for (size_t b = 0; b < stride; ++b) p[b] = static_cast<uint8_t>(i + b);
        }
        bool ok = true;
        for (size_t i = 0; i < n; ++i)
            for (size_t b = 0; b < stride; ++b) ok &= a.at(i)[b] == static_cast<uint8_t>(i + b);
        CHECK(ok);
        std::vector<uint8_t> out(1000 * stride);
        a.copy_out(25000, 1000, out.data());
        CHECK(std::memcmp(out.data(), a.at(25000), stride) == 0);
        CHECK(out[999 * stride] == static_cast<uint8_t>(25999));
    }
    // Growth before set_stride is a bug, and fails loud.
    GBSegArray<0> bad;
    bool threw = false;
    try { bad.append(); } catch (const std::logic_error&) { threw = true; }
    CHECK(threw);
}

// Moves transfer ownership; clear() hands blocks back so a re-acquire is a pool hit.
static void test_move_and_recycle() {
    GBSegVec<int64_t> a;
    for (int i = 0; i < 5000; ++i) a.push_back(i);
    GBSegVec<int64_t> b(std::move(a));
    CHECK(a.size() == 0 && b.size() == 5000 && b[4999] == 4999);
    GBSegVec<int64_t> c;
    c = std::move(b);
    CHECK(b.empty() && c.size() == 5000 && c[123] == 123);
    c.clear();
    const long long hits0 = gb_pool_tel::hits.load();
    GBSegVec<int64_t> d;
    for (int i = 0; i < 5000; ++i) d.push_back(i);
    CHECK(gb_pool_tel::hits.load() > hits0);
}

// Arena: strings round-trip at every size class, including chunk-spanning oversize
// ones; a string never straddles a chunk; a window copy keeps relative offsets.
static void test_arena() {
    GBArena ar;
    std::mt19937_64 rng(7);
    std::vector<std::string> vals;
    std::vector<uint64_t> offs;
    for (int i = 0; i < 20000; ++i) {
        size_t len = (i % 997 == 0) ? (GBArena::kChunk + 1 + rng() % (3 * GBArena::kChunk))
                                    : (rng() % 300);
        std::string s(len, '\0');
        for (auto& ch : s) ch = static_cast<char>('a' + rng() % 26);
        offs.push_back(ar.put(reinterpret_cast<const uint8_t*>(s.data()), s.size()));
        vals.push_back(std::move(s));
    }
    bool ok = true, no_straddle = true;
    for (size_t i = 0; i < vals.size(); ++i) {
        ok &= std::memcmp(ar.resolve(offs[i]), vals[i].data(), vals[i].size()) == 0;
        if (!vals[i].empty() && vals[i].size() <= GBArena::kChunk)
            no_straddle &= (offs[i] >> GBArena::kChunkLog2)
                           == ((offs[i] + vals[i].size() - 1) >> GBArena::kChunkLog2);
    }
    CHECK(ok);
    CHECK(no_straddle);
    // Window copy over a range of strings: out[off - lo] holds each string.
    const size_t first = 5000, last = 9000;
    uint64_t lo = UINT64_MAX, hi = 0;
    for (size_t i = first; i < last; ++i) {
        if (vals[i].empty()) continue;
        lo = std::min<uint64_t>(lo, offs[i]);
        hi = std::max<uint64_t>(hi, offs[i] + vals[i].size());
    }
    std::vector<uint8_t> win(hi - lo);
    size_t pos = 0;
    ar.for_each_run(lo, hi, [&](const uint8_t* p, size_t n) {
        std::memcpy(win.data() + pos, p, n);
        pos += n;
    });
    CHECK(pos == hi - lo);
    bool wok = true;
    for (size_t i = first; i < last; ++i) {
        if (vals[i].empty()) continue;
        wok &= std::memcmp(win.data() + (offs[i] - lo), vals[i].data(), vals[i].size()) == 0;
    }
    CHECK(wok);
    GBArena moved(std::move(ar));
    CHECK(ar.empty() && !moved.empty());
    CHECK(std::memcmp(moved.resolve(offs[0]), vals[0].data(), vals[0].size()) == 0);
}

// Blocks cross threads (filled on one, freed on another — the sink/finalize split),
// and threads exit holding cached blocks; nothing may be lost or freed twice (ASan).
static void test_threads() {
    std::vector<GBSegVec<int64_t>> made(8);
    {
        std::vector<std::thread> ts;
        for (int t = 0; t < 8; ++t)
            ts.emplace_back([t, &made] {
                for (int round = 0; round < 50; ++round) {
                    GBSegVec<int64_t> tmp;
                    for (int i = 0; i < 3000 + round; ++i) tmp.push_back(i ^ t);
                }
                GBSegVec<int64_t> keep;
                for (int i = 0; i < 20000; ++i) keep.push_back(i + t);
                made[static_cast<size_t>(t)] = std::move(keep);
            });
        for (auto& th : ts) th.join();   // each exiting thread hands its cache back
    }
    bool ok = true;
    for (int t = 0; t < 8; ++t)
        for (int i = 0; i < 20000; ++i) ok &= made[static_cast<size_t>(t)][static_cast<size_t>(i)] == i + t;
    CHECK(ok);
    {
        std::vector<std::thread> ts;   // free on threads other than the allocators
        for (int t = 0; t < 8; ++t)
            ts.emplace_back([t, &made] { made[static_cast<size_t>(7 - t)].clear(); });
        for (auto& th : ts) th.join();
    }
    CHECK(GBBlockPool::instance().retained_bytes() <= GBBlockPool::kRetainBytes);
    const long long hits0 = gb_pool_tel::hits.load();
    GBSegVec<int64_t> again;
    for (int i = 0; i < 20000; ++i) again.push_back(i);
    CHECK(gb_pool_tel::hits.load() > hits0);   // served from blocks the threads returned
}

int main() {
    test_push_and_index();
    test_resize_zeroes_recycled_blocks();
    test_hint_and_span();
    test_runs_cover_range();
    test_runtime_stride();
    test_move_and_recycle();
    test_arena();
    test_threads();
    if (failures != 0) {
        std::fprintf(stderr, "gb_block_store (%s geometry): %d check(s) failed\n",
                     OPTERYX_GB_SEG_FIXED ? "fixed" : "doubling", failures);
        return 1;
    }
    std::printf("gb_block_store (%s geometry): all checks passed\n",
                OPTERYX_GB_SEG_FIXED ? "fixed" : "doubling");
    return 0;
}
