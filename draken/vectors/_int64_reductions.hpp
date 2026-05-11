#pragma once
//
// Templated reduction kernels for Int64Vector — sum / min / max.
//
// Same shape as _int64_compare.hpp: tight inner loops with `__restrict__`
// pointers and no per-row Python or virtual dispatch, so the C++ compiler is
// free to auto-vectorise to NEON / AVX2.
//
// Three null shapes:
//   - *_nonnull           : caller has already established there are no nulls
//   - *_nullable_branchless : null bitmap present; the lane is masked out via
//                             a bitmask, no branch in the hot loop
//   - *_nullable_branching  : null bitmap present; branch on validity. Useful
//                             only at high null density (>~70%) where the
//                             branch is well-predicted and most rows are
//                             skipped.
//
// All functions are inline. Callers select the variant once at the morsel
// boundary and the inner loop has zero language overhead.
//

#include <stdint.h>
#include <stddef.h>

namespace draken { namespace int64_red {

// ---------------------------------------------------------------------------
// SUM
// ---------------------------------------------------------------------------

// Plain sum of `n` int64 values. Auto-vectorises trivially.
static inline int64_t sum_nonnull(const int64_t* __restrict__ data, size_t n) {
    int64_t total = 0;
    for (size_t i = 0; i < n; ++i) total += data[i];
    return total;
}

// Branchless null-aware sum: nulls contribute 0 via a per-lane AND mask.
// The bitmap is Arrow-style LSB-first (bit set = valid).
static inline int64_t sum_nullable_branchless(
    const int64_t* __restrict__ data,
    const uint8_t* __restrict__ nulls,
    size_t n)
{
    int64_t total = 0;
    for (size_t i = 0; i < n; ++i) {
        // mask is 0x0 for null rows, 0xFF..FF for valid rows
        const int64_t mask = -static_cast<int64_t>(
            (nulls[i >> 3] >> (i & 7)) & 1u);
        total += data[i] & mask;
    }
    return total;
}

// Branching null-aware sum: skip null rows entirely. Use only when nulls are
// known to be dense (predictor learns the pattern and the branch is cheap).
static inline int64_t sum_nullable_branching(
    const int64_t* __restrict__ data,
    const uint8_t* __restrict__ nulls,
    size_t n)
{
    int64_t total = 0;
    for (size_t i = 0; i < n; ++i) {
        if ((nulls[i >> 3] >> (i & 7)) & 1u) {
            total += data[i];
        }
    }
    return total;
}

// ---------------------------------------------------------------------------
// MIN / MAX
//
// Branchless min/max use `value = cond ? a : b` which most compilers lower
// to cmov/csel — no auto-vectorised lane reduction without arch-specific
// intrinsics, but scalar throughput is still far above the Cython equivalent
// because the inner loop has no Python frame / GIL / type plumbing.
//
// The nullable variants return the count of valid rows; the caller raises
// "all-null" when count == 0. This keeps the kernel free of exception
// signalling on the hot path.
// ---------------------------------------------------------------------------

static inline int64_t min_nonnull(const int64_t* __restrict__ data, size_t n) {
    // Precondition: n > 0 — caller is responsible.
    int64_t m = data[0];
    for (size_t i = 1; i < n; ++i) {
        m = data[i] < m ? data[i] : m;
    }
    return m;
}

static inline int64_t max_nonnull(const int64_t* __restrict__ data, size_t n) {
    int64_t m = data[0];
    for (size_t i = 1; i < n; ++i) {
        m = data[i] > m ? data[i] : m;
    }
    return m;
}

// Branchless nullable min: seeds the running minimum with INT64_MAX so any
// real value beats it. Tracks valid count via a popcount-style add.
static inline size_t min_nullable_branchless(
    const int64_t* __restrict__ data,
    const uint8_t* __restrict__ nulls,
    size_t n,
    int64_t* __restrict__ out_min)
{
    int64_t m = INT64_MAX;
    size_t count = 0;
    for (size_t i = 0; i < n; ++i) {
        const uint8_t v = (nulls[i >> 3] >> (i & 7)) & 1u;
        // pick `data[i]` only on valid rows; null rows leave `m` unchanged.
        const int64_t candidate = v ? data[i] : INT64_MAX;
        m = candidate < m ? candidate : m;
        count += v;
    }
    *out_min = m;
    return count;
}

static inline size_t max_nullable_branchless(
    const int64_t* __restrict__ data,
    const uint8_t* __restrict__ nulls,
    size_t n,
    int64_t* __restrict__ out_max)
{
    int64_t m = INT64_MIN;
    size_t count = 0;
    for (size_t i = 0; i < n; ++i) {
        const uint8_t v = (nulls[i >> 3] >> (i & 7)) & 1u;
        const int64_t candidate = v ? data[i] : INT64_MIN;
        m = candidate > m ? candidate : m;
        count += v;
    }
    *out_max = m;
    return count;
}

static inline size_t min_nullable_branching(
    const int64_t* __restrict__ data,
    const uint8_t* __restrict__ nulls,
    size_t n,
    int64_t* __restrict__ out_min)
{
    int64_t m = INT64_MAX;
    size_t count = 0;
    for (size_t i = 0; i < n; ++i) {
        if ((nulls[i >> 3] >> (i & 7)) & 1u) {
            if (data[i] < m) m = data[i];
            ++count;
        }
    }
    *out_min = m;
    return count;
}

static inline size_t max_nullable_branching(
    const int64_t* __restrict__ data,
    const uint8_t* __restrict__ nulls,
    size_t n,
    int64_t* __restrict__ out_max)
{
    int64_t m = INT64_MIN;
    size_t count = 0;
    for (size_t i = 0; i < n; ++i) {
        if ((nulls[i >> 3] >> (i & 7)) & 1u) {
            if (data[i] > m) m = data[i];
            ++count;
        }
    }
    *out_max = m;
    return count;
}

}} // namespace draken::int64_red
