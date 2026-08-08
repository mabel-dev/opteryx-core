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

int main() {
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
