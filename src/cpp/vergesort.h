#pragma once
// Vergesort: run-detection pre-pass for LSD radix sort.
//
// Scans keys[perm[i]] for ascending/descending runs. Descending runs are
// reversed in-place. If the number of runs is <= sqrt(n), the runs are merged
// using tmp[] as scratch and the function returns true (sorted, skip radix).
// Otherwise returns false and perm[] is left in identity order for radix sort.
//
// This exploits pre-existing order in time-series or nearly-sorted data,
// giving O(n) best case instead of O(8n) radix passes.

#include <cstddef>
#include <cstdint>
#include <cmath>
#include <cstring>

// Stats counters for benchmarking and testing.
// Not thread-safe — intended for single-threaded profiling scripts only.
static uint64_t _vgs_hits   = 0;  // returned true  (handled; radix skipped)
static uint64_t _vgs_misses = 0;  // returned false (fell through to radix)

static inline void vergesort_reset_stats() {
    _vgs_hits = _vgs_misses = 0;
}

static inline void vergesort_get_stats(uint64_t* hits, uint64_t* misses) {
    *hits   = _vgs_hits;
    *misses = _vgs_misses;
}

// Merge two adjacent sorted runs [lo, mid) and [mid, hi) in perm[],
// using scratch buffer tmp[]. Compares via keys[perm[i]].
static inline void _vgs_merge(
    uint32_t* perm,
    uint32_t* tmp,
    const uint64_t* keys,
    uint32_t lo, uint32_t mid, uint32_t hi
) {
    uint32_t i = lo, j = mid, k = 0;
    uint32_t len = hi - lo;
    while (i < mid && j < hi) {
        if (keys[perm[i]] <= keys[perm[j]])
            tmp[k++] = perm[i++];
        else
            tmp[k++] = perm[j++];
    }
    while (i < mid) tmp[k++] = perm[i++];
    while (j < hi)  tmp[k++] = perm[j++];
    std::memcpy(perm + lo, tmp, (size_t)len * sizeof(uint32_t));
}

// Bottom-up merge sort over an array of run boundaries.
// runs[r] = start index of run r; runs[num_runs] = n (sentinel).
static inline void _vgs_merge_runs(
    uint32_t* perm,
    uint32_t* tmp,
    const uint64_t* keys,
    uint32_t* runs,
    uint32_t num_runs,
    uint32_t n
) {
    while (num_runs > 1) {
        uint32_t out = 0;
        for (uint32_t r = 0; r < num_runs; r += 2) {
            uint32_t lo  = runs[r];
            uint32_t mid = (r + 1 < num_runs) ? runs[r + 1] : n;
            uint32_t hi  = (r + 2 < num_runs) ? runs[r + 2] : n;
            if (r + 1 < num_runs)
                _vgs_merge(perm, tmp, keys, lo, mid, hi);
            runs[out++] = lo;
        }
        num_runs = out;
    }
}

// Main entry point.
// Returns true  → perm[] is now sorted, caller skips radix sort.
// Returns false → run coverage insufficient, caller should use radix sort.
//                 perm[] is unchanged (identity permutation still holds from
//                 the radix init, or partially refined from prior LSD passes).
static inline bool vergesort_u64(
    uint32_t* perm,
    uint32_t* tmp,
    const uint64_t* keys,
    size_t n
) {
    if (n < 2)
        return true;

    // Threshold: only merge if num_runs <= THRESHOLD.
    //
    // DuckDB uses sqrt(n) because its fallback is pdqsort (O(n log n)).
    // Our fallback is radix sort (O(8n)), which is much cheaper. Empirically
    // on this codebase, merge cost exceeds radix cost around k=64 runs
    // (merge does ceil(log2(k)) passes vs 8 radix passes, with a ~1.2x higher
    // constant per pass due to branching). k=32 is safely below the crossover
    // on both ARM (dev) and x86 (prod).
    const uint32_t THRESHOLD = 32;

    // Fixed-size stack array — 34 entries, 136 bytes total.
    const uint32_t MAX_RUNS = THRESHOLD + 2;
    uint32_t stack_runs[34];
    uint32_t* runs = stack_runs;

    uint32_t num_runs = 0;
    uint32_t i = 0;
    const uint32_t nn = (uint32_t)n;

    while (i < nn) {
        uint32_t run_start = i;

        if (__builtin_expect(i + 1 >= nn, 0)) {
            if (num_runs < MAX_RUNS)
                runs[num_runs++] = run_start;
            break;
        }

        if (keys[perm[i + 1]] < keys[perm[i]]) {
            // Descending run: extend and reverse in-place.
            while (i + 1 < nn && keys[perm[i + 1]] <= keys[perm[i]])
                ++i;
            uint32_t lo = run_start, hi = i;
            while (lo < hi) {
                uint32_t t = perm[lo]; perm[lo] = perm[hi]; perm[hi] = t;
                ++lo; --hi;
            }
        } else {
            // Ascending run (common case): extend.
            while (__builtin_expect(i + 1 < nn, 1) &&
                   keys[perm[i + 1]] >= keys[perm[i]])
                ++i;
        }

        if (__builtin_expect(num_runs >= MAX_RUNS, 0)) {
            ++_vgs_misses;
            return false;
        }
        runs[num_runs++] = run_start;
        ++i;
    }
    runs[num_runs] = nn; // sentinel

    if (num_runs > THRESHOLD) {
        ++_vgs_misses;
        return false;
    }

    // Already sorted: skip merge entirely.
    if (__builtin_expect(num_runs == 1, 0)) {
        ++_vgs_hits;
        return true;
    }

    _vgs_merge_runs(perm, tmp, keys, runs, num_runs, nn);
    ++_vgs_hits;
    return true;
}
