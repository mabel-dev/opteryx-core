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

// ── Comparator-driven variant ────────────────────────────────────────────────
//
// Same algorithm as vergesort_u64 below, but ordered by a caller-supplied
// comparator instead of a flat uint64 key array. This is what the unified sort
// (draken/morsels/sort.hpp) uses: its comparator orders ALL key columns
// most-significant-first, so a hit here means the input is genuinely sorted by the
// whole multi-column ORDER BY — not merely by the leading column, which ties would
// make an unsafe proxy.
//
// `cmp(a, b)` is a strict weak ordering: true iff row a sorts before row b.
// `runs_scratch` must hold at least `threshold + 3` entries; it is the caller's so
// the timed path allocates nothing. NOTE the +3, not +2: `num_runs` can reach
// `MAX_RUNS` (= threshold + 2) when the scan exits by exhausting the input rather
// than by tripping the run-count guard, and the sentinel `runs[num_runs] = nn`
// then needs one slot BEYOND that. Sizing this +2 writes one element past the end
// — a live stack-buffer overflow that vergesort_u64 below still has (its
// stack_runs[34] with MAX_RUNS 34); flagged, not copied.
//
// The `threshold` is a parameter rather than the fixed THRESHOLD below because the
// crossover depends on what the caller falls back TO — see SORT_VERGESORT_THRESHOLD
// in draken/morsels/sort.hpp, which is 16 against a comparison-sort fallback, versus
// the 32 below tuned against radix.

template <class Cmp>
static inline void _vgs_merge_cmp(
    uint32_t* perm, uint32_t* tmp, Cmp cmp,
    uint32_t lo, uint32_t mid, uint32_t hi
) {
    uint32_t i = lo, j = mid, k = 0;
    uint32_t len = hi - lo;
    while (i < mid && j < hi) {
        // `!cmp(right, left)` — NOT `cmp(left, right)` — so EQUAL elements take the
        // left branch. That is what makes the merge stable.
        if (!cmp(perm[j], perm[i]))
            tmp[k++] = perm[i++];
        else
            tmp[k++] = perm[j++];
    }
    while (i < mid) tmp[k++] = perm[i++];
    while (j < hi)  tmp[k++] = perm[j++];
    std::memcpy(perm + lo, tmp, (size_t)len * sizeof(uint32_t));
}

template <class Cmp>
static inline void _vgs_merge_runs_cmp(
    uint32_t* perm, uint32_t* tmp, Cmp cmp,
    uint32_t* runs, uint32_t num_runs, uint32_t n
) {
    while (num_runs > 1) {
        uint32_t out = 0;
        for (uint32_t r = 0; r < num_runs; r += 2) {
            uint32_t lo  = runs[r];
            uint32_t mid = (r + 1 < num_runs) ? runs[r + 1] : n;
            uint32_t hi  = (r + 2 < num_runs) ? runs[r + 2] : n;
            if (r + 1 < num_runs)
                _vgs_merge_cmp(perm, tmp, cmp, lo, mid, hi);
            runs[out++] = lo;
        }
        num_runs = out;
    }
}

// Returns true  → perm[] is now sorted, caller skips its fallback sort.
// Returns false → too many runs; caller should run its fallback over perm[].
//                 NOTE perm[] may have had descending runs reversed in place before
//                 the bail. That does not disturb a stable fallback: runs are
//                 extended with STRICT `<`, so a reversed run holds no equal keys,
//                 and reversal keeps every element within its own run's index range
//                 — so the relative order of equal keys is untouched and a stable
//                 sort yields exactly what it would from the identity permutation.
template <class Cmp>
static inline bool vergesort_generic(
    uint32_t* perm,
    uint32_t* tmp,
    Cmp cmp,
    size_t n,
    uint32_t threshold,
    uint32_t* runs_scratch
) {
    if (n < 2)
        return true;

    const uint32_t MAX_RUNS = threshold + 2;
    uint32_t* runs = runs_scratch;
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

        if (cmp(perm[i + 1], perm[i])) {
            // STRICTLY descending run — see the stability note above.
            while (i + 1 < nn && cmp(perm[i + 1], perm[i]))
                ++i;
            uint32_t lo = run_start, hi = i;
            while (lo < hi) {
                uint32_t t = perm[lo]; perm[lo] = perm[hi]; perm[hi] = t;
                ++lo; --hi;
            }
        } else {
            while (__builtin_expect(i + 1 < nn, 1) && !cmp(perm[i + 1], perm[i]))
                ++i;
        }

        if (__builtin_expect(num_runs >= MAX_RUNS, 0))
            return false;
        runs[num_runs++] = run_start;
        ++i;
    }
    runs[num_runs] = nn;   // sentinel

    if (num_runs > threshold)
        return false;
    if (__builtin_expect(num_runs == 1, 0))
        return true;

    _vgs_merge_runs_cmp(perm, tmp, cmp, runs, num_runs, nn);
    return true;
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
            // STRICTLY descending run: extend with `<` (NOT `<=`) and reverse
            // in-place. Using `<` excludes trailing EQUAL elements from the run,
            // so the in-place reversal never reverses the relative order of equal
            // keys — the pre-pass stays STABLE (equal keys keep input order). This
            // makes the whole sort a deterministic input-order-stable TOTAL order,
            // the precondition the parallel k-way merge relies on to be
            // byte-identical to serial (its global-index tiebreak == this input
            // order). `<=` here would reverse ties and no merge could reproduce it.
            while (i + 1 < nn && keys[perm[i + 1]] < keys[perm[i]])
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
