#pragma once
// draken/ops/int64_reductions.h — sum / min / max over a DrakenVector<int64>.
//
// All functions take a DrakenVector and operate via the uniform
// data[selection[i]] access pattern with an identity fast-path (DRAKEN_SEL_IDENTITY).
//
// SIMD note: the identity path is structured for auto-vectorisation by -O2.
// Manual NEON / AVX2 intrinsics are a follow-up; a time-boxed performance waiver
// per 09_delivery.md §3 covers the gap until that lands.
//
// Semantics match draken_old/vectors/integer64_vector.pyx:
//   sum:  empty or all-null → returns 0, count = 0.
//   min:  empty or all-null → count = 0; caller must raise ValueError.
//   max:  empty or all-null → count = 0; caller must raise ValueError.
//   sum overflow: wraps (signed C arithmetic; matches draken_old's cdivision=True).

#include <stdint.h>
#include <stddef.h>
#include <limits.h>
#include "core/buffers.h"
#include "core/alloc.h"

namespace draken { namespace ops {

// ---------------------------------------------------------------------------
// Internal: query the validity bit for logical row i (Arrow convention: 1=valid).
// ---------------------------------------------------------------------------
static inline bool row_valid(const uint8_t* validity, uint32_t i) noexcept {
    return (validity[i >> 3] >> (i & 7)) & 1u;
}

// ---------------------------------------------------------------------------
// SUM
//
// Returns count of non-null contributing rows. out_value is always set:
//   all-valid:  out_value = sum of all rows.
//   some nulls: out_value = sum of valid rows (null rows contribute 0).
//   all null / empty: out_value = 0, return = 0.
// Overflow wraps (C signed — same as draken_old with cdivision=True).
// ---------------------------------------------------------------------------
static inline uint32_t i64_sum(const DrakenVector& v, int64_t* out_value) noexcept {
    const uint32_t n = v.length;
    const int64_t* data = static_cast<const int64_t*>(v.data);
    const uint8_t* validity = v.validity;

    int64_t total = 0;
    uint32_t count = 0;

    if (validity == nullptr) {
        count = n;
        if (v.flags & DRAKEN_SEL_IDENTITY) {
            for (uint32_t i = 0; i < n; ++i) total += data[i];
        } else {
            for (uint32_t i = 0; i < n; ++i) total += data[v.selection[i]];
        }
    } else {
        // Branchless: mask to 0 for null rows so auto-vectoriser can run.
        if (v.flags & DRAKEN_SEL_IDENTITY) {
            for (uint32_t i = 0; i < n; ++i) {
                const uint32_t valid = row_valid(validity, i) ? 1u : 0u;
                total += data[i] * static_cast<int64_t>(valid);
                count += valid;
            }
        } else {
            for (uint32_t i = 0; i < n; ++i) {
                const uint32_t valid = row_valid(validity, i) ? 1u : 0u;
                total += data[v.selection[i]] * static_cast<int64_t>(valid);
                count += valid;
            }
        }
    }

    *out_value = total;
    return count;
}

// ---------------------------------------------------------------------------
// MIN
//
// Returns count of valid rows; *out_min is set only when count > 0.
// If count == 0 (empty or all-null), *out_min is undefined — caller raises.
// ---------------------------------------------------------------------------
static inline uint32_t i64_min(const DrakenVector& v, int64_t* out_min) noexcept {
    const uint32_t n = v.length;
    const int64_t* data = static_cast<const int64_t*>(v.data);
    const uint8_t* validity = v.validity;

    int64_t m = INT64_MAX;
    uint32_t count = 0;

    if (validity == nullptr) {
        count = n;
        if (v.flags & DRAKEN_SEL_IDENTITY) {
            for (uint32_t i = 0; i < n; ++i)
                m = data[i] < m ? data[i] : m;
        } else {
            for (uint32_t i = 0; i < n; ++i) {
                const int64_t val = data[v.selection[i]];
                m = val < m ? val : m;
            }
        }
    } else {
        if (v.flags & DRAKEN_SEL_IDENTITY) {
            for (uint32_t i = 0; i < n; ++i) {
                if (row_valid(validity, i)) {
                    m = data[i] < m ? data[i] : m;
                    ++count;
                }
            }
        } else {
            for (uint32_t i = 0; i < n; ++i) {
                if (row_valid(validity, i)) {
                    const int64_t val = data[v.selection[i]];
                    m = val < m ? val : m;
                    ++count;
                }
            }
        }
    }

    *out_min = m;
    return count;
}

// ---------------------------------------------------------------------------
// MAX
//
// Returns count of valid rows; *out_max set only when count > 0.
// ---------------------------------------------------------------------------
static inline uint32_t i64_max(const DrakenVector& v, int64_t* out_max) noexcept {
    const uint32_t n = v.length;
    const int64_t* data = static_cast<const int64_t*>(v.data);
    const uint8_t* validity = v.validity;

    int64_t m = INT64_MIN;
    uint32_t count = 0;

    if (validity == nullptr) {
        count = n;
        if (v.flags & DRAKEN_SEL_IDENTITY) {
            for (uint32_t i = 0; i < n; ++i)
                m = data[i] > m ? data[i] : m;
        } else {
            for (uint32_t i = 0; i < n; ++i) {
                const int64_t val = data[v.selection[i]];
                m = val > m ? val : m;
            }
        }
    } else {
        if (v.flags & DRAKEN_SEL_IDENTITY) {
            for (uint32_t i = 0; i < n; ++i) {
                if (row_valid(validity, i)) {
                    m = data[i] > m ? data[i] : m;
                    ++count;
                }
            }
        } else {
            for (uint32_t i = 0; i < n; ++i) {
                if (row_valid(validity, i)) {
                    const int64_t val = data[v.selection[i]];
                    m = val > m ? val : m;
                    ++count;
                }
            }
        }
    }

    *out_max = m;
    return count;
}

}} // namespace draken::ops
