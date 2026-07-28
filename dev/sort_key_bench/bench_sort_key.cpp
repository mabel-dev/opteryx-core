// dev/sort_key_bench/bench_sort_key.cpp
//
// Matched-driver A/B microbench: production SortKeyCmp (branch-per-column loop
// over SortKeyColumn, src/cpp/engine/native_sort.hpp) vs a MOCK packed-128-bit
// composite-key comparator, over synthetic 2-column numeric ORDER BY morsels.
//
// Both arms:
//   - start from the SAME build_sort_keys() output (bit-identical normalized
//     per-column uint64 keys — no risk of the two arms silently comparing
//     different data)
//   - sort the SAME identity permutation array
//   - go through the IDENTICAL parallel-stable-sort scaffold (thread count,
//     200k threshold, explicit-buffer merge) — parallel_stable_sort_generic
//     below is a verbatim copy of native_sort.hpp's parallel_stable_sort_perm,
//     templated on the comparator type instead of hardcoded to SortKeyCmp, so
//     the ONLY thing that differs between arms is the comparator itself.
//
// Arm A calls the REAL production parallel_stable_sort_perm() directly (not a
// reimplementation) for maximum fidelity to what actually ships.
//
// Scope / known simplification: no NULLs in the synthetic data. The packed key
// here reserves no null sentinel (see conversation notes) — a production
// version would need one; this mock exists to measure the comparator-swap's
// raw cost delta, not to be a drop-in replacement.
//
// Build:
//   g++ -O3 -std=c++20 -I. -Idraken -Idraken/core -Isrc/cpp -pthread \
//       dev/sort_key_bench/bench_sort_key.cpp -o /tmp/bench_sort_key
// Run:
//   /tmp/bench_sort_key [n_rows]

#include <chrono>
#include <cinttypes>
#include <cstdio>
#include <cstdint>
#include <cstring>
#include <random>
#include <vector>

#include "engine/native_sort.hpp"

using namespace opteryx::engine;

// ---- verbatim copy of parallel_stable_sort_perm, templated on Cmp ----------------
// Kept as an exact structural mirror of native_sort.hpp's version (same thread
// count derivation, same 200k threshold, same explicit-buffer merge) so the mock
// arm gets identical parallelization treatment to the production arm — the
// matched-wrapper discipline applied to the *algorithm* as well as the data.
template <class Cmp>
void parallel_stable_sort_generic(Cmp cmp, std::vector<uint32_t>& perm) {
    const size_t n = perm.size();
    unsigned hw = std::thread::hardware_concurrency();
    unsigned nt = hw > 2 ? static_cast<unsigned>(hw - 2) : 1u;
    if (nt > 16) nt = 16;
    if (n < 200000) nt = 1;
    if (nt < 1) nt = 1;

    if (nt <= 1) {
        std::stable_sort(perm.begin(), perm.end(), cmp);
        return;
    }

    size_t chunk = (n + nt - 1) / nt;
    std::vector<std::pair<size_t, size_t>> ranges;
    for (size_t s = 0; s < n; s += chunk) ranges.emplace_back(s, std::min(s + chunk, n));

    std::vector<std::thread> threads;
    threads.reserve(ranges.size() - 1);
    for (size_t r = 1; r < ranges.size(); ++r) {
        threads.emplace_back([&perm, &cmp, &ranges, r]() {
            std::stable_sort(perm.begin() + static_cast<ptrdiff_t>(ranges[r].first),
                             perm.begin() + static_cast<ptrdiff_t>(ranges[r].second), cmp);
        });
    }
    std::stable_sort(perm.begin() + static_cast<ptrdiff_t>(ranges[0].first),
                     perm.begin() + static_cast<ptrdiff_t>(ranges[0].second), cmp);
    for (std::thread& t : threads) t.join();

    if (ranges.size() == 1) return;

    std::vector<uint32_t> scratch(n);
    uint32_t* src = perm.data();
    uint32_t* dst = scratch.data();
    while (ranges.size() > 1) {
        std::vector<std::pair<size_t, size_t>> next_ranges;
        for (size_t i = 0; i + 1 < ranges.size(); i += 2) {
            size_t a0 = ranges[i].first, a1 = ranges[i].second, a2 = ranges[i + 1].second;
            std::merge(src + a0, src + a1, src + a1, src + a2, dst + a0, cmp);
            next_ranges.emplace_back(a0, a2);
        }
        if (ranges.size() % 2 == 1) {
            const auto& last = ranges.back();
            std::copy(src + last.first, src + last.second, dst + last.first);
            next_ranges.push_back(last);
        }
        std::swap(src, dst);
        ranges = std::move(next_ranges);
    }
    if (src != perm.data()) std::copy(src, src + n, perm.data());
}

// ---- mock packed 128-bit composite key -------------------------------------------
// High 64 bits = col 0's normalized key (bit-flipped if DESC); low 64 bits =
// col 1's normalized key (bit-flipped if DESC). Both columns already arrive as
// correct-total-order uint64 (native_sort.hpp's sort_num_key), so a plain
// unsigned 128-bit compare on the concatenation reproduces the exact
// lexicographic (col0 first, col1 tiebreak) order for the non-null case.
struct PackedKeyCmp {
    const std::vector<unsigned __int128>& packed;
    bool operator()(uint32_t a, uint32_t b) const { return packed[a] < packed[b]; }
};

// Diagnostic only: wraps any comparator to count invocations, so we can check
// whether the two arms' comparator is called a similar number of times through
// the identical generic scaffold (rules in/out "different algorithmic work"
// as the explanation, separate from "different cost per call").
template <class Cmp>
struct CountingCmp {
    Cmp inner;
    std::atomic<uint64_t>* counter;
    bool operator()(uint32_t a, uint32_t b) const {
        counter->fetch_add(1, std::memory_order_relaxed);
        return inner(a, b);
    }
};

static std::vector<unsigned __int128> build_packed_keys(const std::vector<SortKeyColumn>& keys) {
    const SortKeyColumn& c0 = keys[0];
    const SortKeyColumn& c1 = keys[1];
    size_t n = c0.num.size();
    std::vector<unsigned __int128> packed(n);
    for (size_t i = 0; i < n; ++i) {
        uint64_t k0 = c0.asc ? c0.num[i] : ~c0.num[i];
        uint64_t k1 = c1.asc ? c1.num[i] : ~c1.num[i];
        packed[i] = (static_cast<unsigned __int128>(k0) << 64) | static_cast<unsigned __int128>(k1);
    }
    return packed;
}

// ---- synthetic morsel construction -------------------------------------------------

static CxxColumn make_dense_col(void* data, uint32_t n, DrakenType t) {
    DrakenVector v = draken_vector_from_dense(data, n, t, nullptr);
    CxxColumn c;
    c.own = std::make_shared<VectorOwner>(v, OwnedBuffer<void>(data), OwnedBuffer<uint8_t>(nullptr));
    c.view = c.own->vec;
    return c;
}

// Splits n rows across morsels of `morsel_size` rows each — mirrors how SortSink
// actually receives its input (many small morsels, not one giant buffer).
// price_buckets == 0: continuous random price (first column ~never ties, always
// decides the comparison). price_buckets > 0: price drawn from that many distinct
// values (forces frequent first-column ties, so the comparator must fall through
// to the second column) — lets us test whether short-circuiting on column 0 is
// what's driving the A/B result, not just assert it.
static std::vector<MorselPtr> build_morsels(size_t n, uint32_t morsel_size, unsigned seed,
                                            uint32_t price_buckets) {
    std::mt19937_64 rng(seed);
    std::uniform_real_distribution<double> price_dist(0.0, 100000.0);
    std::uniform_int_distribution<uint32_t> bucket_dist(0, price_buckets > 0 ? price_buckets - 1 : 0);
    std::uniform_int_distribution<int64_t> date_dist(0, 200000);   // deliberate collisions -> real tiebreaks

    std::vector<MorselPtr> morsels;
    for (size_t start = 0; start < n; start += morsel_size) {
        uint32_t rows = static_cast<uint32_t>(std::min<size_t>(morsel_size, n - start));
        auto* prices = static_cast<double*>(draken_malloc(sizeof(double) * (rows ? rows : 1)));
        auto* dates  = static_cast<int64_t*>(draken_malloc(sizeof(int64_t) * (rows ? rows : 1)));
        for (uint32_t i = 0; i < rows; ++i) {
            prices[i] = price_buckets > 0
                ? static_cast<double>(bucket_dist(rng))
                : price_dist(rng);
            dates[i]  = date_dist(rng);
        }
        auto m = std::make_shared<CxxMorsel>();
        m->columns.push_back(make_dense_col(prices, rows, DRAKEN_FLOAT64));
        m->columns.push_back(make_dense_col(dates, rows, DRAKEN_INT64));
        m->names = {"price", "orderdate"};
        morsels.push_back(std::move(m));
    }
    return morsels;
}

// ---- timing helper ------------------------------------------------------------------

template <class F>
static double time_ms(F&& f) {
    auto t0 = std::chrono::steady_clock::now();
    f();
    auto t1 = std::chrono::steady_clock::now();
    return std::chrono::duration<double, std::milli>(t1 - t0).count();
}

int main(int argc, char** argv) {
    size_t n = argc > 1 ? std::strtoull(argv[1], nullptr, 10) : 5'000'000ull;
    uint32_t price_buckets = argc > 2 ? static_cast<uint32_t>(std::strtoul(argv[2], nullptr, 10)) : 0;
    const uint32_t MORSEL_SIZE = 65536;
    const int REPEATS = 5;

    printf("bench_sort_key: n=%zu rows, %d repeats, morsel_size=%u, price_buckets=%u%s\n",
           n, REPEATS, MORSEL_SIZE, price_buckets, price_buckets == 0 ? " (continuous, col0 rarely ties)" : " (col0 ties often)");
    printf("ORDER BY price DESC, orderdate ASC  (FLOAT64 desc, INT64 asc)\n\n");

    std::vector<MorselPtr> morsels = build_morsels(n, MORSEL_SIZE, 12345, price_buckets);

    std::vector<SortKeySpec> spec = {{0, false}, {1, true}};   // price DESC, orderdate ASC
    ErrCtx err;
    std::vector<SortKeyColumn> keys;
    if (!build_sort_keys(morsels, spec, n, keys, err)) {
        fprintf(stderr, "build_sort_keys failed: %s\n", err.msg);
        return 1;
    }

    // ---- Arm A: production SortKeyCmp + production parallel_stable_sort_perm ----
    std::vector<double> times_a, times_b;
    std::vector<uint32_t> perm_a_final, perm_b_final;

    for (int r = 0; r < REPEATS; ++r) {
        std::vector<uint32_t> perm(n);
        for (size_t i = 0; i < n; ++i) perm[i] = static_cast<uint32_t>(i);
        double t = time_ms([&]() { parallel_stable_sort_perm(keys, perm); });
        times_a.push_back(t);
        if (r == REPEATS - 1) perm_a_final = std::move(perm);
    }

    // ---- Arm B: mock packed-128-bit key + identical parallel scaffold ----
    // Pack time measured once outside the repeat loop, reported separately —
    // it's the one-time O(n) cost the mock adds; the repeats isolate comparator
    // + sort cost, which is where the hypothesis lives.
    std::vector<unsigned __int128> packed;
    double pack_ms = time_ms([&]() { packed = build_packed_keys(keys); });

    for (int r = 0; r < REPEATS; ++r) {
        std::vector<uint32_t> perm(n);
        for (size_t i = 0; i < n; ++i) perm[i] = static_cast<uint32_t>(i);
        PackedKeyCmp cmp{packed};
        double t = time_ms([&]() { parallel_stable_sort_generic(cmp, perm); });
        times_b.push_back(t);
        if (r == REPEATS - 1) perm_b_final = std::move(perm);
    }

    // ---- correctness check: both arms must induce the identical row order ----
    bool same_order = (perm_a_final == perm_b_final);
    if (!same_order) {
        fprintf(stderr, "CORRECTNESS FAILURE: Arm A and Arm B produced different orderings — "
                        "mock is NOT a valid stand-in, timings below are meaningless.\n");
    }

    auto stats = [](std::vector<double>& v, double& min_out, double& med_out) {
        std::vector<double> s = v;
        std::sort(s.begin(), s.end());
        min_out = s.front();
        med_out = s[s.size() / 2];
    };
    double min_a, med_a, min_b, med_b;
    stats(times_a, min_a, med_a);
    stats(times_b, min_b, med_b);

    printf("Arm A (production SortKeyCmp, branch-per-column):\n");
    printf("  min=%.3fms  median=%.3fms   [%s]\n\n", min_a, med_a,
           [&]{ std::string s; for (double t : times_a) s += (s.empty()?"":", ") + std::to_string(t); return s; }().c_str());

    printf("Arm B (mock packed-128-bit key, single unsigned-128 compare):\n");
    printf("  pack (one-time, excluded from repeats above): %.3fms\n", pack_ms);
    printf("  min=%.3fms  median=%.3fms   [%s]\n\n", min_b, med_b,
           [&]{ std::string s; for (double t : times_b) s += (s.empty()?"":", ") + std::to_string(t); return s; }().c_str());

    printf("Correctness: %s\n", same_order ? "PASS (identical row order)" : "FAIL");
    printf("Speedup (sort-only, min):      %.3fx\n", min_a / min_b);
    printf("Speedup (sort-only, median):    %.3fx\n", med_a / med_b);
    printf("Speedup (sort + pack, median):  %.3fx\n", med_a / (med_b + pack_ms));

    // ---- diagnostic: comparator call counts through the IDENTICAL generic scaffold ----
    // (not the real production parallel_stable_sort_perm for arm A here — using the
    // mirrored generic template for both so the counter wrapper applies symmetrically)
    {
        std::atomic<uint64_t> calls_a{0}, calls_b{0};
        std::vector<uint32_t> perm(n);
        for (size_t i = 0; i < n; ++i) perm[i] = static_cast<uint32_t>(i);
        CountingCmp<SortKeyCmp> ccmp_a{SortKeyCmp{keys}, &calls_a};
        double dt_a = time_ms([&]() { parallel_stable_sort_generic(ccmp_a, perm); });

        for (size_t i = 0; i < n; ++i) perm[i] = static_cast<uint32_t>(i);
        CountingCmp<PackedKeyCmp> ccmp_b{PackedKeyCmp{packed}, &calls_b};
        double dt_b = time_ms([&]() { parallel_stable_sort_generic(ccmp_b, perm); });

        printf("\nDiagnostic (both arms via the SAME generic scaffold, counted):\n");
        printf("  SortKeyCmp:   %8" PRIu64 " calls, %.3fms  (%.2fns/call)\n",
               calls_a.load(), dt_a, dt_a * 1e6 / static_cast<double>(calls_a.load()));
        printf("  PackedKeyCmp: %8" PRIu64 " calls, %.3fms  (%.2fns/call)\n",
               calls_b.load(), dt_b, dt_b * 1e6 / static_cast<double>(calls_b.load()));
    }

    return same_order ? 0 : 1;
}
