// simd_find_test.cpp — oracle test for draken's substring search primitives and
// the parquet page search built on them.
//
// `make simd-find-test` (in `make q`). Every search result is compared with a
// naive scalar scan over haystacks drawn from a tiny alphabet, so candidates
// (anchor-byte coincidences that are not matches) are COMMON, and every anchor
// pair of every needle is exercised — not just the pair the heuristic picks.
// Lengths straddle the 64-byte and 16-byte loop boundaries and the scalar tail.

#include "simd_find.h"
#include "../../rugo/src/parquet/page_search.hpp"

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <random>
#include <string>
#include <vector>

static size_t naive_find(const uint8_t* h, size_t hl, const uint8_t* p, size_t pl) {
    if (pl == 0) return 0;
    if (hl < pl) return SIZE_MAX;
    for (size_t i = 0; i + pl <= hl; ++i)
        if (std::memcmp(h + i, p, pl) == 0) return i;
    return SIZE_MAX;
}

static int failures = 0;
#define CHECK(cond, ...) do { if (!(cond)) { ++failures; std::printf("FAIL %s:%d: ", __FILE__, __LINE__); std::printf(__VA_ARGS__); std::printf("\n"); if (failures > 20) std::exit(1); } } while (0)

int main() {
    std::mt19937 rng(20261022);
    const std::string alphabet = "ab ";
    // --- simd_find_cs / simd_find_cs_pair vs the naive scan ------------------
    const std::vector<std::string> needles = {
        "a", "ab", "ba", "b a", "aa", "bb ", "abab", "a b a", std::string(40, 'b'),
        "ab ab ab ", std::string(17, 'a') + "b", "b" + std::string(63, 'a') + "b"};
    const std::vector<size_t> hay_lens = {0, 1, 2, 3, 15, 16, 17, 31, 32, 33, 63, 64, 65,
                                          100, 127, 128, 129, 200, 255, 256, 257, 1000, 4096, 5000};
    long cases = 0;
    for (size_t hl : hay_lens) {
        for (int rep = 0; rep < 40; ++rep) {
            std::vector<uint8_t> hay(hl + 64);   // slack so over-reads would be visible under ASan only if real
            for (size_t i = 0; i < hl; ++i) hay[i] = static_cast<uint8_t>(alphabet[rng() % alphabet.size()]);
            // Occasionally plant a needle at the very end / start.
            for (const std::string& ns : needles) {
                const uint8_t* p = reinterpret_cast<const uint8_t*>(ns.data());
                const size_t pl = ns.size();
                if (rep % 7 == 1 && pl <= hl) std::memcpy(hay.data() + hl - pl, p, pl);
                if (rep % 7 == 2 && pl <= hl) std::memcpy(hay.data(), p, pl);
                const size_t want = naive_find(hay.data(), hl, p, pl);
                const size_t got = simd_find_cs(hay.data(), hl, p, pl);
                CHECK(got == want, "simd_find_cs hl=%zu needle='%s' want=%zu got=%zu", hl, ns.c_str(), want, got);
                CHECK(simd_contains_cs(hay.data(), hl, p, pl) == (want != SIZE_MAX), "contains hl=%zu", hl);
                ++cases;
                if (pl >= 2) {
                    for (size_t a = 0; a < pl; ++a)
                        for (size_t b = a + 1; b < pl; ++b) {
                            const size_t g2 = simd_find_cs_pair(hay.data(), hl, p, pl, a, b);
                            CHECK(g2 == want, "pair(%zu,%zu) hl=%zu needle='%s' want=%zu got=%zu", a, b, hl, ns.c_str(), want, g2);
                            const size_t g3 = simd_find_cs_pair(hay.data(), hl, p, pl, b, a);
                            CHECK(g3 == want, "pair swapped (%zu,%zu) hl=%zu", b, a, hl);
                            ++cases;
                        }
                    size_t pa = 0, pb = 0;
                    simd_pick_pair(hay.data(), hl, p, pl, &pa, &pb);
                    CHECK(pa < pb && pb < pl, "pick_pair gave (%zu,%zu) for pl=%zu", pa, pb, pl);
                    const size_t g4 = simd_find_cs_pair(hay.data(), hl, p, pl, pa, pb);
                    CHECK(g4 == want, "picked pair hl=%zu needle='%s'", hl, ns.c_str());
                }
            }
        }
    }
    // Rarest-byte choice: with 'x' absent from the sample, the pair must take
    // both 'x' positions; with everything equally common it stays first+last.
    {
        std::string sample;
        for (int i = 0; i < 500; ++i) sample += "ab";   // 'x' absent, 'a'/'b' equally common
        const uint8_t* s = reinterpret_cast<const uint8_t*>(sample.data());
        const std::string pat = "axbxa";
        size_t a = 9, b = 9;
        simd_pick_pair(s, sample.size(), reinterpret_cast<const uint8_t*>(pat.data()), pat.size(), &a, &b);
        CHECK(a == 1 && b == 3, "rare pair expected (1,3) got (%zu,%zu)", a, b);
        const std::string pat2 = "aaaa";
        simd_pick_pair(s, sample.size(), reinterpret_cast<const uint8_t*>(pat2.data()), pat2.size(), &a, &b);
        CHECK(a == 0 && b == 3, "tie pair expected (0,3) got (%zu,%zu)", a, b);
    }
    // --- page search over [len32][bytes]... regions vs per-value oracle ------
    const std::vector<size_t> vlens = {0, 1, 2, 3, 5, 32, 97, 98};   // 32/97/98: prefix bytes look like needle bytes
    for (int rep = 0; rep < 60; ++rep) {
        const size_t nvals = 1 + rng() % 400;
        std::vector<uint8_t> region;
        std::vector<std::string> vals;
        for (size_t v = 0; v < nvals; ++v) {
            const size_t n = vlens[rng() % vlens.size()];
            std::string s;
            for (size_t i = 0; i < n; ++i) s.push_back(alphabet[rng() % alphabet.size()]);
            vals.push_back(s);
            const int32_t len = static_cast<int32_t>(n);
            const uint8_t* lp = reinterpret_cast<const uint8_t*>(&len);
            region.insert(region.end(), lp, lp + 4);
            region.insert(region.end(), s.begin(), s.end());
        }
        // Pad large regions past the 4 KiB sampling threshold now and then.
        for (const std::string& ns : needles) {
            std::vector<uint8_t> hit(nvals, 0);
            const uint32_t marked = rugo::page_search::mark_values_containing(
                region.data(), region.size(), static_cast<uint32_t>(nvals),
                reinterpret_cast<const uint8_t*>(ns.data()), ns.size(), hit.data());
            uint32_t want_marked = 0;
            for (size_t v = 0; v < nvals; ++v) {
                const bool want = vals[v].find(ns) != std::string::npos;
                CHECK(hit[v] == (want ? 1 : 0), "page_search rep=%d value %zu needle='%s' want=%d got=%d", rep, v, ns.c_str(), (int)want, (int)hit[v]);
                want_marked += want;
            }
            CHECK(marked == want_marked, "page_search marked=%u want=%u", marked, want_marked);
            ++cases;
        }
    }
    if (failures) { std::printf("simd_find_test: %d failures\n", failures); return 1; }
    std::printf("simd_find_test: OK (%ld cases)\n", cases);
    return 0;
}
