// CarcharSet::mark_new (the batched two-phase probe) against an exact oracle.
//
// Every verdict mark_new hands back is checked against a std::unordered_set fed
// the same keys in the same order, interleaved with single insert_or_ignore calls
// so the two entry points share one table. Streams are built to hit the paths
// that matter: duplicates inside one block (the second copy must take the full
// probe after the first inserted), tag false-positives (keys that share home slot
// and tag but differ elsewhere), heavy repetition (phase-1 hits), wrap-around at
// the top of the table, runs that straddle several doublings, and a run that
// starts on a minimum-capacity table (block larger than the headroom).
//
// Build/run: make carchar-set-test (ASan + UBSan).
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <unordered_set>
#include <vector>

#include "carchar_set.hpp"

namespace {

uint64_t splitmix64(uint64_t& x) {
    uint64_t z = (x += 0x9E3779B97F4A7C15ULL);
    z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ULL;
    z = (z ^ (z >> 27)) * 0x94D049BB133111EBULL;
    return z ^ (z >> 31);
}

int failures = 0;
void check(bool ok, const char* what, size_t at) {
    if (!ok) {
        std::fprintf(stderr, "FAIL: %s (at %zu)\n", what, at);
        ++failures;
    }
}

// Feed `keys` through one CarcharSet: runs of random length via mark_new, every
// third run element-wise via insert_or_ignore; verdicts checked against the oracle.
void drive(const std::vector<uint64_t>& keys, uint64_t seed, const char* name) {
    opteryx::carchar::CarcharSet set;
    std::unordered_set<uint64_t> oracle;
    std::vector<uint8_t> verdict;
    size_t i = 0, run_no = 0;
    while (i < keys.size()) {
        size_t n = 1 + static_cast<size_t>(splitmix64(seed) % 5000);
        if (i + n > keys.size()) n = keys.size() - i;
        if (run_no % 3 == 2) {
            for (size_t k = 0; k < n; ++k) {
                const bool fresh = set.insert_or_ignore(keys[i + k]);
                check(fresh == oracle.insert(keys[i + k]).second, name, i + k);
            }
        } else {
            verdict.assign(n, 0xAA);
            const size_t got = set.mark_new(keys.data() + i, verdict.data(), n);
            size_t expect = 0;
            for (size_t k = 0; k < n; ++k) {
                const bool fresh = oracle.insert(keys[i + k]).second;
                expect += fresh;
                check(verdict[k] == static_cast<uint8_t>(fresh), name, i + k);
            }
            check(got == expect, name, i);
        }
        i += n;
        ++run_no;
    }
    check(set.size() == oracle.size(), name, set.size());
    for (uint64_t k : oracle) check(set.contains(k), name, static_cast<size_t>(k));
}

}  // namespace

int main() {
    uint64_t seed = 0x0123456789ABCDEFULL;

    // 1. 400k draws over a 60k-value universe: ~85% repeats, many inside one block.
    {
        std::vector<uint64_t> keys(400000);
        std::vector<uint64_t> universe(60000);
        for (auto& u : universe) u = splitmix64(seed);
        for (auto& k : keys) k = universe[splitmix64(seed) % universe.size()];
        drive(keys, seed, "repeats");
    }
    // 2. All distinct: every key a phase-2 miss; straddles ~20 doublings.
    {
        std::vector<uint64_t> keys(300000);
        for (auto& k : keys) k = splitmix64(seed);
        drive(keys, seed, "distinct");
    }
    // 3. Tag false-positives: same low 20 bits (home slot for every table size up
    //    to 1M slots) and same top 7 bits (tag), different middle bits.
    {
        std::vector<uint64_t> keys;
        const uint64_t low = splitmix64(seed) & 0xFFFFFULL;
        const uint64_t top = splitmix64(seed) & (0x7FULL << 57);
        for (int rep = 0; rep < 3; ++rep)
            for (uint64_t m = 0; m < 20000; ++m) {
                const uint64_t mid = (splitmix64(seed) & 0x01FFFFFFFFF00000ULL);
                keys.push_back(top | mid | low);
            }
        drive(keys, seed, "tag-collisions");
    }
    // 4. Wrap-around: home slots in the last 16 lanes of each power-of-two table.
    {
        std::vector<uint64_t> keys;
        for (uint64_t m = 0; m < 50000; ++m) {
            const uint64_t hi = splitmix64(seed) & ~0xFFFFULL;
            keys.push_back(hi | (0xFFF0ULL + (m & 15)));
            if (m % 4 == 0) keys.push_back(keys[keys.size() - 1 - (m % 7)]);
        }
        drive(keys, seed, "wrap");
    }
    // 5. One block on a minimum-capacity table, then a single key, then a block of
    //    exactly kBatch duplicates of one value.
    {
        opteryx::carchar::CarcharSet set;
        uint64_t keys[64];
        uint8_t v[64];
        for (auto& k : keys) k = splitmix64(seed);
        check(set.mark_new(keys, v, 40) == 40, "min-cap block", 0);
        for (int j = 0; j < 40; ++j) check(v[j] == 1, "min-cap verdict", j);
        check(set.mark_new(keys, v, 40) == 0, "min-cap repeat", 0);
        for (int j = 0; j < 40; ++j) check(v[j] == 0, "min-cap repeat verdict", j);
        for (auto& k : keys) k = 0xDEADBEEFULL;
        check(set.mark_new(keys, v, 32) == 1, "dup block", 0);
        check(v[0] == 1, "dup block first", 0);
        for (int j = 1; j < 32; ++j) check(v[j] == 0, "dup block rest", j);
        check(set.size() == 41, "size after dup block", set.size());
        check(set.mark_new(nullptr, v, 3) == 0, "null keys", 0);
        check(set.mark_new(keys, v, 0) == 0, "empty run", 0);
    }

    if (failures != 0) {
        std::fprintf(stderr, "%d failures\n", failures);
        return 1;
    }
    std::printf("carchar_set batch: all checks passed\n");
    return 0;
}
