#pragma once
//
// Templated reduction kernels for Float64Vector — sum / min / max.
//
// Mirrors _int64_reductions.hpp. NaN handling follows IEEE-754: comparisons
// with NaN return false, so the running min/max never adopts a NaN. SUM of
// any NaN value yields NaN — matches PyArrow / SQL standard semantics.
//

#include <stdint.h>
#include <stddef.h>
#include <float.h>

namespace draken { namespace float64_red {

static inline double sum_nonnull(const double* __restrict__ data, size_t n) {
    double total = 0.0;
    for (size_t i = 0; i < n; ++i) total += data[i];
    return total;
}

static inline double sum_nullable_branchless(
    const double* __restrict__ data,
    const uint8_t* __restrict__ nulls,
    size_t n)
{
    double total = 0.0;
    for (size_t i = 0; i < n; ++i) {
        const uint8_t v = (nulls[i >> 3] >> (i & 7)) & 1u;
        // Multiplying by (double)v keeps the loop branch-free and avoids
        // any path-dependent NaN propagation: a null row contributes 0.0.
        total += data[i] * static_cast<double>(v);
    }
    return total;
}

static inline double sum_nullable_branching(
    const double* __restrict__ data,
    const uint8_t* __restrict__ nulls,
    size_t n)
{
    double total = 0.0;
    for (size_t i = 0; i < n; ++i) {
        if ((nulls[i >> 3] >> (i & 7)) & 1u) {
            total += data[i];
        }
    }
    return total;
}

// ---------------------------------------------------------------------------
// MIN / MAX
// ---------------------------------------------------------------------------

static inline double min_nonnull(const double* __restrict__ data, size_t n) {
    // Precondition: n > 0.
    double m = data[0];
    for (size_t i = 1; i < n; ++i) {
        m = data[i] < m ? data[i] : m;
    }
    return m;
}

static inline double max_nonnull(const double* __restrict__ data, size_t n) {
    double m = data[0];
    for (size_t i = 1; i < n; ++i) {
        m = data[i] > m ? data[i] : m;
    }
    return m;
}

static inline size_t min_nullable_branchless(
    const double* __restrict__ data,
    const uint8_t* __restrict__ nulls,
    size_t n,
    double* __restrict__ out_min)
{
    double m = DBL_MAX;
    size_t count = 0;
    for (size_t i = 0; i < n; ++i) {
        const uint8_t v = (nulls[i >> 3] >> (i & 7)) & 1u;
        const double candidate = v ? data[i] : DBL_MAX;
        m = candidate < m ? candidate : m;
        count += v;
    }
    *out_min = m;
    return count;
}

static inline size_t max_nullable_branchless(
    const double* __restrict__ data,
    const uint8_t* __restrict__ nulls,
    size_t n,
    double* __restrict__ out_max)
{
    double m = -DBL_MAX;
    size_t count = 0;
    for (size_t i = 0; i < n; ++i) {
        const uint8_t v = (nulls[i >> 3] >> (i & 7)) & 1u;
        const double candidate = v ? data[i] : -DBL_MAX;
        m = candidate > m ? candidate : m;
        count += v;
    }
    *out_max = m;
    return count;
}

static inline size_t min_nullable_branching(
    const double* __restrict__ data,
    const uint8_t* __restrict__ nulls,
    size_t n,
    double* __restrict__ out_min)
{
    double m = DBL_MAX;
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
    const double* __restrict__ data,
    const uint8_t* __restrict__ nulls,
    size_t n,
    double* __restrict__ out_max)
{
    double m = -DBL_MAX;
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

}} // namespace draken::float64_red
