// src/cpp/engine/bench_join_csr_lookup.cpp — cost of ONE JoinCsr bucket lookup.
//
// `L`: the time to look up a key that is NOT present, swept across build sizes so the
// L1/L2/L3/DRAM transitions are visible. This is the number that decides whether any
// scheme for AVOIDING a lookup can pay.
//
// It measured 5-13ns even at a 305 MiB table, which is far cheaper than a hash-table
// miss is usually assumed to cost. The reason is structural: a missing key hits an
// EMPTY bucket and stops at `off[]`, so the miss path's working set is 4 bytes/row,
// not the ~16 the full table implies. A build-side bloom prefilter was built and
// measured against this and could not beat it at either extreme of build size
// (100M build/1 probe: +15%; 1 build/100M probe: +4.7%) — proving absence is not
// cheaper than looking it up here. Kept because the same question will be asked
// again of the next scheme.
//
// Standalone assert()-based benchmark (same pattern as test_sort_unified.cpp — this
// repo has no C++ test framework). Not wired into CI; run by hand.
//
// Build & run:
//   g++ -O2 -std=c++20 -I. -Isrc/cpp -pthread \
//       src/cpp/engine/bench_join_csr_lookup.cpp -o /tmp/bench_join_csr && \
//       /tmp/bench_join_csr

#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <random>
#include <string>
#include <vector>


// ---------------------------------------------------------------------------
// A faithful copy of JoinCsr (src/cpp/engine/native_join2.hpp). Copied
// rather than included because native_join2.hpp pulls the whole executor; the
// three arrays and the bucket scan below are byte-for-byte the layout and the
// access pattern the real probe uses, which is all that governs the cost.
// If JoinCsr's layout changes, this copy must change with it.
// ---------------------------------------------------------------------------
struct JoinCsrModel {
    size_t mask = 0;
    std::vector<uint32_t> off;     // bucket offsets, size N+1
    std::vector<uint32_t> rows;    // build row ids, grouped by bucket
    std::vector<uint64_t> hashes;  // parallel to `rows`: the stored key hash

    // Mirrors JoinCsr::row_count_for — the SEMI/ANTI existence probe, and the
    // same memory traffic the INNER probe's append_probe_matches pays.
    size_t row_count_for(uint64_t key) const {
        const size_t b = static_cast<size_t>(key) & mask;
        size_t n = 0;
        for (uint32_t i = off[b]; i < off[b + 1]; ++i)
            if (hashes[i] == key) ++n;
        return n;
    }
};

// Build the CSR exactly as build_join_csr does: pow2 bucket count >= row count,
// histogram, prefix sum, scatter. Single-threaded here — the LAYOUT is identical,
// and layout is what the probe cost depends on.
static JoinCsrModel build_csr(const std::vector<uint64_t>& keys) {
    JoinCsrModel c;
    const size_t total = keys.size();
    size_t n = 1;
    while (n < total) n <<= 1;
    c.mask = n - 1;
    c.off.assign(n + 1, 0);
    c.rows.resize(total);
    c.hashes.resize(total);

    std::vector<uint32_t> counts(n, 0);
    for (uint64_t h : keys) ++counts[static_cast<size_t>(h) & c.mask];
    uint32_t run = 0;
    for (size_t b = 0; b < n; ++b) {
        c.off[b] = run;
        run += counts[b];
    }
    c.off[n] = run;

    std::vector<uint32_t> cursor(c.off.begin(), c.off.end() - 1);
    for (size_t r = 0; r < total; ++r) {
        const uint64_t h = keys[r];
        const size_t b = static_cast<size_t>(h) & c.mask;
        const uint32_t p = cursor[b]++;
        c.rows[p] = static_cast<uint32_t>(r);
        c.hashes[p] = h;
    }
    return c;
}

// ---------------------------------------------------------------------------
// Per-architecture probe-shape arms (2026-10-04, docs/ARCH_AWARE_PERFORMANCE_TEST_PLAN.md
// Phase 1: prefetch and bloom re-tests on ARM and x86). Every arm returns the
// same match count as the plain loop — checked, not assumed.
// ---------------------------------------------------------------------------

// The build-side filter layout the 2026-08-07 bloom experiment shipped and
// measured best: one 64-bit word per key, k=2 bits in that word, ~8 bits/key
// rounded up to a power of two words. Bits are taken from hash ranges the CSR
// bucket index (low bits) does not use.
struct WordBloom {
    std::vector<uint64_t> words;
    size_t mask = 0;
    explicit WordBloom(const std::vector<uint64_t>& keys) {
        size_t n = 1;
        while (n * 8 < keys.size()) n <<= 1;   // >= 8 bits per key
        words.assign(n, 0);
        mask = n - 1;
        for (uint64_t h : keys) words[(h >> 32) & mask] |= bits(h);
    }
    static uint64_t bits(uint64_t h) { return (1ull << ((h >> 20) & 63)) | (1ull << ((h >> 26) & 63)); }
    bool maybe(uint64_t h) const {
        const uint64_t b = bits(h);
        return (words[(h >> 32) & mask] & b) == b;
    }
};

static size_t probe_plain(const JoinCsrModel& c, const std::vector<uint64_t>& keys) {
    size_t acc = 0;
    for (uint64_t k : keys) acc += c.row_count_for(k);
    return acc;
}

static size_t probe_bloom(const JoinCsrModel& c, const WordBloom& f, const std::vector<uint64_t>& keys) {
    size_t acc = 0;
    for (uint64_t k : keys)
        if (f.maybe(k)) acc += c.row_count_for(k);
    return acc;
}

// Pivot-style pipelined probe: hash ahead, prefetch the bucket's off[] entry
// 2*D keys ahead, then (off[] now cached) prefetch the bucket's hashes[] range
// D keys ahead, then do the lookup. D is the knob.
template <size_t D>
static size_t probe_prefetch(const JoinCsrModel& c, const std::vector<uint64_t>& keys) {
    const size_t n = keys.size();
    size_t acc = 0;
    for (size_t i = 0; i < n; ++i) {
        if (i + 2 * D < n) __builtin_prefetch(&c.off[static_cast<size_t>(keys[i + 2 * D]) & c.mask]);
        if (i + D < n) {
            const size_t b = static_cast<size_t>(keys[i + D]) & c.mask;
            __builtin_prefetch(&c.hashes[c.off[b]]);
        }
        acc += c.row_count_for(keys[i]);
    }
    return acc;
}

static double ns_per(const std::chrono::steady_clock::time_point& t0,
                     const std::chrono::steady_clock::time_point& t1, size_t n) {
    return std::chrono::duration<double, std::nano>(t1 - t0).count() / n;
}

static void run_arms(const char* mix_name, const JoinCsrModel& csr, const WordBloom& bloom,
                     const std::vector<uint64_t>& keys, int reps) {
    struct Arm { const char* name; size_t (*fn)(const JoinCsrModel&, const WordBloom&, const std::vector<uint64_t>&); };
    static const Arm arms[] = {
        {"plain", [](const JoinCsrModel& c, const WordBloom&, const std::vector<uint64_t>& k) { return probe_plain(c, k); }},
        {"bloom", [](const JoinCsrModel& c, const WordBloom& f, const std::vector<uint64_t>& k) { return probe_bloom(c, f, k); }},
        {"pf8", [](const JoinCsrModel& c, const WordBloom&, const std::vector<uint64_t>& k) { return probe_prefetch<8>(c, k); }},
        {"pf16", [](const JoinCsrModel& c, const WordBloom&, const std::vector<uint64_t>& k) { return probe_prefetch<16>(c, k); }},
        {"pf32", [](const JoinCsrModel& c, const WordBloom&, const std::vector<uint64_t>& k) { return probe_prefetch<32>(c, k); }},
    };
    constexpr size_t kArms = sizeof(arms) / sizeof(arms[0]);
    const size_t expect = probe_plain(csr, keys);
    double best[kArms];
    for (size_t a = 0; a < kArms; ++a) best[a] = 1e300;
    volatile size_t sink = 0;
    for (int r = 0; r < reps; ++r) {
        for (size_t k = 0; k < kArms; ++k) {
            const size_t a = (r % 2 == 0) ? k : kArms - 1 - k;   // alternate arm order
            const auto t0 = std::chrono::steady_clock::now();
            const size_t got = arms[a].fn(csr, bloom, keys);
            const auto t1 = std::chrono::steady_clock::now();
            if (got != expect) {
                std::fprintf(stderr, "MISMATCH: arm %s returned %zu, plain %zu\n", arms[a].name, got, expect);
                std::exit(1);
            }
            sink += got;
            const double t = ns_per(t0, t1, keys.size());
            if (t < best[a]) best[a] = t;
        }
    }
    std::printf("   %-5s", mix_name);
    for (size_t a = 0; a < kArms; ++a) std::printf("  %s %6.2f (x%.2f)", arms[a].name, best[a], best[a] / best[0]);
    std::printf("\n");
}

static int arms_main(int reps) {
    std::printf("Probe-shape arms, ns/probe, min of %d, arm order alternating; x = vs plain\n", reps);
    const size_t kProbes = 4'000'000;
    std::mt19937_64 rng(0xA5C4A5C4A5C4ULL);
    for (size_t build_rows : {1'000ul, 100'000ul, 1'000'000ul, 4'000'000ul, 16'000'000ul, 64'000'000ul}) {
        std::vector<uint64_t> build(build_rows);
        for (auto& v : build) v = rng();
        JoinCsrModel csr = build_csr(build);
        WordBloom bloom(build);
        std::vector<uint64_t> miss(kProbes), hit(kProbes), half(kProbes);
        for (size_t i = 0; i < kProbes; ++i) {
            miss[i] = rng() | 1ull;
            hit[i] = build[rng() % build_rows];
            half[i] = (i & 1) ? miss[i] : hit[i];
        }
        std::printf("build %zu rows (CSR %.1f MiB, filter %.1f MiB)\n", build_rows, build_rows * 16.0 / 1048576.0,
                    bloom.words.size() * 8.0 / 1048576.0);
        run_arms("miss", csr, bloom, miss, reps);
        run_arms("half", csr, bloom, half, reps);
        run_arms("hit", csr, bloom, hit, reps);
    }
    return 0;
}

int main(int argc, char** argv) {
    if (argc > 1 && std::strcmp(argv[1], "--arms") == 0) return arms_main(argc > 2 ? std::atoi(argv[2]) : 5);
    std::printf("%s\n", std::string(96, '=').c_str());
    std::printf("JoinCsr lookup cost (L) — %s\n",
#if defined(__aarch64__)
                "arm64/NEON"
#elif defined(__x86_64__)
                "x86-64/SSE2"
#else
                "scalar"
#endif
    );
    std::printf("%s\n", std::string(96, '=').c_str());
    std::printf("CSR footprint is ~16 bytes/build row (off[] 4 + rows[] 4 + hashes[] 8)\n\n");
    std::printf("%12s %10s | %14s %14s\n", "build rows", "CSR MiB", "L miss (ns)",
                "L hit (ns)");
    std::printf("%s\n", std::string(96, '-').c_str());

    const size_t kProbes = 4'000'000;
    std::mt19937_64 rng(0x5EED5EED5EED5EEDULL);

    for (size_t build_rows : {1'000ul, 10'000ul, 100'000ul, 1'000'000ul, 4'000'000ul,
                              16'000'000ul}) {
        // Build keys and a DISJOINT probe set. Keys are drawn from a 64-bit PRNG,
        // which is what cxx_hash_c output looks like to the bucket index.
        std::vector<uint64_t> build(build_rows);
        for (size_t i = 0; i < build_rows; ++i) build[i] = rng();

        std::vector<uint64_t> miss(kProbes), hit(kProbes);
        for (size_t i = 0; i < kProbes; ++i) {
            miss[i] = rng() | 1ull;                 // overwhelmingly absent
            hit[i] = build[rng() % build_rows];     // present, random order
        }

        JoinCsrModel csr = build_csr(build);
        const double csr_mib = (build_rows * 16.0) / (1024.0 * 1024.0);

        volatile size_t sink = 0;

        auto time_lookup = [&](const std::vector<uint64_t>& keys) {
            const auto t0 = std::chrono::steady_clock::now();
            size_t acc = 0;
            for (uint64_t k : keys) acc += csr.row_count_for(k);
            const auto t1 = std::chrono::steady_clock::now();
            sink += acc;
            return std::chrono::duration<double, std::nano>(t1 - t0).count() / keys.size();
        };

        const double l_miss = time_lookup(miss);
        const double l_hit = time_lookup(hit);

        std::printf("%12zu %10.1f | %14.2f %14.2f\n", build_rows, csr_mib, l_miss, l_hit);
    }

    return 0;
}
