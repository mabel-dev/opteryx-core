// src/cpp/engine/bench_join_build_side.cpp — is the SEMI/ANTI build side on the wrong leg?
//
// compiler.py:3178 pins SEMI/ANTI to build the RIGHT leg ("the LEFT leg is the
// preserved/filtered side - it must be the PROBE"). At TPC-H SF10 that inverts every
// expensive one: Q21's semi join builds 59,986,052 rows to probe 734,523.
//
// Swapping is only worth a native left-build operator if
//
//     build(small) + probe_all(big)   <   build(big) + probe_all(small)
//
// which is NOT obvious: the same total number of rows passes through either way, so
// the win has to come from BUILD being dearer than PROBE per row, and from the small
// table staying cache-resident while the big one does not. If build and probe cost
// the same per row, swapping buys nothing and the operator is not worth writing.
// That is exactly the trap the reverted bloom prefilter fell into — spectacular
// elimination rates, net negative — so measure before building.
//
// Companion to bench_join_csr_lookup.cpp, which measures the probe half alone (L).
// This one adds the BUILD half and totals the two real arrangements.
//
// Standalone assert()-based benchmark (this repo has no C++ test framework). Not
// wired into CI; run by hand.
//
// Build & run:
//   g++ -O2 -std=c++20 -I. -Isrc/cpp -pthread \
//       src/cpp/engine/bench_join_build_side.cpp -o /tmp/bench_join_build && \
//       /tmp/bench_join_build

#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <random>
#include <string>
#include <vector>

// ---------------------------------------------------------------------------
// Faithful copy of JoinCsr (src/cpp/engine/native_join2.hpp), same as
// bench_join_csr_lookup.cpp copies it — native_join2.hpp pulls the whole executor,
// and the three arrays plus the bucket scan are what govern the cost.
// If JoinCsr's layout changes, both copies must change with it.
// ---------------------------------------------------------------------------
struct JoinCsrModel {
    size_t mask = 0;
    std::vector<uint32_t> off;
    std::vector<uint32_t> rows;
    std::vector<uint64_t> hashes;

    inline size_t row_count_for(uint64_t key) const {
        const size_t b = static_cast<size_t>(key) & mask;
        size_t n = 0;
        for (uint32_t i = off[b]; i < off[b + 1]; ++i)
            if (hashes[i] == key) ++n;
        return n;
    }
};

// Exactly build_join_csr: pow2 buckets >= rows, histogram, prefix sum, scatter.
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

static double seconds_since(std::chrono::steady_clock::time_point t0) {
    return std::chrono::duration<double>(std::chrono::steady_clock::now() - t0).count();
}

int main() {
    std::printf("%s\n", std::string(100, '=').c_str());
    std::printf("JoinCsr BUILD vs PROBE — %s\n",
#if defined(__aarch64__)
                "arm64/NEON"
#elif defined(__x86_64__)
                "x86-64/SSE2"
#else
                "scalar"
#endif
    );
    std::printf("%s\n\n", std::string(100, '=').c_str());

    std::mt19937_64 rng(0x5EED5EED5EED5EEDULL);

    // ---- Part 1: per-row cost of each half, swept across table size ----------
    std::printf("Part 1 — per-row cost of each half (probes are MISS-dominated, as in a\n");
    std::printf("         selective semi join: most probe keys are absent from the table)\n\n");
    std::printf("%12s %10s | %12s %12s | %10s\n", "table rows", "CSR MiB", "build ns/row",
                "probe ns/row", "build/probe");
    std::printf("%s\n", std::string(100, '-').c_str());

    const size_t kProbes = 4'000'000;
    for (size_t rows : {100'000ul, 750'000ul, 4'000'000ul, 16'000'000ul, 60'000'000ul}) {
        std::vector<uint64_t> keys(rows);
        for (size_t i = 0; i < rows; ++i) keys[i] = rng();

        auto t0 = std::chrono::steady_clock::now();
        JoinCsrModel csr = build_csr(keys);
        const double build_s = seconds_since(t0);

        std::vector<uint64_t> miss(kProbes);
        for (size_t i = 0; i < kProbes; ++i) miss[i] = rng() | 1ull;

        volatile size_t sink = 0;
        t0 = std::chrono::steady_clock::now();
        for (size_t i = 0; i < kProbes; ++i) sink += csr.row_count_for(miss[i]);
        const double probe_s = seconds_since(t0);
        (void)sink;

        const double build_ns = build_s * 1e9 / static_cast<double>(rows);
        const double probe_ns = probe_s * 1e9 / static_cast<double>(kProbes);
        std::printf("%12zu %10.1f | %12.2f %12.2f | %10.2fx\n", rows,
                    (rows * 16.0) / (1024.0 * 1024.0), build_ns, probe_ns,
                    build_ns / probe_ns);
    }

    // ---- Part 2: the two arrangements, at the real TPC-H SF10 cardinalities --
    std::printf("\nPart 2 — the two arrangements at real TPC-H SF10 cardinalities.\n");
    std::printf("         A = build(right) + probe(left)  <- what compiler.py does today\n");
    std::printf("         B = build(left)  + probe(right) <- a left-build semi/anti operator\n");
    std::printf("         B also pays a matched-bitmap write per probe HIT, included below.\n\n");
    std::printf("%-26s %12s %12s | %10s %10s | %8s\n", "join", "build side", "probe side",
                "A (ms)", "B (ms)", "B vs A");
    std::printf("%s\n", std::string(100, '-').c_str());

    struct Shape {
        const char* name;
        size_t big;    // the right leg today (the build side)
        size_t small;  // the left leg today (the probe side)
    };
    const Shape shapes[] = {
        {"Q21 left_semi (l2)", 59'986'052, 734'523},
        {"Q21 left_anti (l3)", 37'929'348, 707'593},
        {"Q04 left_semi (lineitem)", 37'929'348, 573'955},
    };

    for (const Shape& s : shapes) {
        // --- Arrangement A: build the big side, probe with the small side.
        std::vector<uint64_t> big(s.big);
        for (size_t i = 0; i < s.big; ++i) big[i] = rng();
        std::vector<uint64_t> small(s.small);
        for (size_t i = 0; i < s.small; ++i) small[i] = rng();

        auto t0 = std::chrono::steady_clock::now();
        JoinCsrModel a = build_csr(big);
        double a_ms = seconds_since(t0) * 1e3;
        volatile size_t sink = 0;
        t0 = std::chrono::steady_clock::now();
        for (size_t i = 0; i < s.small; ++i) sink += a.row_count_for(small[i]);
        a_ms += seconds_since(t0) * 1e3;
        a = JoinCsrModel{};  // release before B builds, so neither is measured under
                             // the other's memory pressure

        // --- Arrangement B: build the small side, stream the big side, mark hits.
        t0 = std::chrono::steady_clock::now();
        JoinCsrModel b = build_csr(small);
        double b_ms = seconds_since(t0) * 1e3;
        std::vector<uint64_t> matched((s.small + 63) / 64, 0);
        t0 = std::chrono::steady_clock::now();
        for (size_t i = 0; i < s.big; ++i) {
            const uint64_t key = big[i];
            const size_t bucket = static_cast<size_t>(key) & b.mask;
            for (uint32_t j = b.off[bucket]; j < b.off[bucket + 1]; ++j) {
                if (b.hashes[j] == key) {
                    const uint32_t row = b.rows[j];
                    matched[row >> 6] |= (1ull << (row & 63));
                }
            }
        }
        b_ms += seconds_since(t0) * 1e3;
        (void)sink;

        std::printf("%-26s %12zu %12zu | %10.1f %10.1f | %7.2fx\n", s.name, s.big, s.small,
                    a_ms, b_ms, a_ms / b_ms);
    }

    std::printf("\nB/A > 1 means the swap wins by that factor. Under ~1.2x it is not worth a\n");
    std::printf("new native operator: SEMI/ANTI also carry a correlated residual and\n");
    std::printf("NULL-aware NOT IN, both of which get harder when the sides are swapped.\n");
    return 0;
}
