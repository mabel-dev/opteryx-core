// Hand-written C++ header for ungrouped aggregate state structs.
// Underscore prefix distinguishes from Cython-generated .cpp files.
//
// All types are in namespace opteryx::ungrouped.
// All methods are inline in the header — no separate .cpp needed.

#pragma once

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <limits>

#include "engine/agg_budgets.hpp"   // kMedianBytes — shared with variables.py's report
#include "ops/float_ops.h"          // draken::ops::fp_total_lt — THE ratified float order,
                                    // imported not re-derived (see finalize_median)

#if defined(__ARM_NEON) || defined(__ARM_NEON__)
#  include <arm_neon.h>
#  define UA_NEON 1
#elif defined(__SSE2__)
#  include <emmintrin.h>
#  define UA_SSE2 1
#endif

namespace opteryx { namespace ungrouped {

// ---------------------------------------------------------------------------
// Null-bitmap helper
// ---------------------------------------------------------------------------
static inline bool bitmap_valid(const uint8_t* b, size_t i) noexcept {
    if (b == nullptr) return true;
    return ((b[i >> 3] >> (i & 7)) & 1) != 0;
}


// ---------------------------------------------------------------------------
// SIMD lexicographic byte comparison
// Returns <0 if (a,la) < (b,lb), 0 if equal, >0 if (a,la) > (b,lb).
// No Python, no malloc.
// ---------------------------------------------------------------------------
static inline int compare_bytes(
    const char* a, size_t la,
    const char* b, size_t lb
) noexcept {
    size_t min_len = la < lb ? la : lb;
    size_t pos = 0;

#ifdef UA_NEON
    while (pos + 16 <= min_len) {
        uint8x16_t va = vld1q_u8((const uint8_t*)a + pos);
        uint8x16_t vb = vld1q_u8((const uint8_t*)b + pos);
        uint8x16_t neq = veorq_u8(va, vb);
        // Check if any lane differs
        uint64x2_t neq64 = vreinterpretq_u64_u8(neq);
        if (vgetq_lane_u64(neq64, 0) || vgetq_lane_u64(neq64, 1)) {
            // Find first differing byte
            uint8_t av[16], bv[16];
            vst1q_u8(av, va);
            vst1q_u8(bv, vb);
            for (int k = 0; k < 16; ++k) {
                if (av[k] != bv[k]) return (int)av[k] - (int)bv[k];
            }
        }
        pos += 16;
    }
#elif defined(UA_SSE2)
    while (pos + 16 <= min_len) {
        __m128i va = _mm_loadu_si128((const __m128i*)(a + pos));
        __m128i vb = _mm_loadu_si128((const __m128i*)(b + pos));
        int mask = _mm_movemask_epi8(_mm_cmpeq_epi8(va, vb));
        if (mask != 0xFFFF) {
            // Find first differing byte
            int first_diff = __builtin_ctz(~mask);
            return (int)(uint8_t)a[pos + first_diff] - (int)(uint8_t)b[pos + first_diff];
        }
        pos += 16;
    }
#endif

    // Scalar tail
    while (pos < min_len) {
        int diff = (int)(uint8_t)a[pos] - (int)(uint8_t)b[pos];
        if (diff != 0) return diff;
        ++pos;
    }

    if (la < lb) return -1;
    if (la > lb) return  1;
    return 0;
}


// ---------------------------------------------------------------------------
// SumState<T>
// ---------------------------------------------------------------------------
template<typename T>
struct SumState {
    T     total = T(0);
    bool  seen  = false;

    inline void apply(const T* data, const uint8_t* nulls, size_t n) noexcept {
        for (size_t i = 0; i < n; ++i) {
            if (bitmap_valid(nulls, i)) {
                total += data[i];
                seen = true;
            }
        }
    }

    inline void apply_const(T val, size_t n) noexcept {
        total += val * static_cast<T>(n);
        seen = true;
    }
};


// ---------------------------------------------------------------------------
// MinState<T>
// ---------------------------------------------------------------------------
template<typename T>
struct MinState {
    T    result = std::numeric_limits<T>::max();
    bool seen   = false;

    inline void apply(const T* data, const uint8_t* nulls, size_t n) noexcept {
        for (size_t i = 0; i < n; ++i) {
            if (bitmap_valid(nulls, i)) {
                if (!seen || data[i] < result) {
                    result = data[i];
                    seen = true;
                }
            }
        }
    }

    inline void apply_const(T val) noexcept {
        if (!seen || val < result) {
            result = val;
            seen = true;
        }
    }
};


// ---------------------------------------------------------------------------
// MaxState<T>
// ---------------------------------------------------------------------------
template<typename T>
struct MaxState {
    T    result = std::numeric_limits<T>::lowest();
    bool seen   = false;

    inline void apply(const T* data, const uint8_t* nulls, size_t n) noexcept {
        for (size_t i = 0; i < n; ++i) {
            if (bitmap_valid(nulls, i)) {
                if (!seen || data[i] > result) {
                    result = data[i];
                    seen = true;
                }
            }
        }
    }

    inline void apply_const(T val) noexcept {
        if (!seen || val > result) {
            result = val;
            seen = true;
        }
    }
};


// ---------------------------------------------------------------------------
// MinBytesState — tracks the lexicographically smallest byte sequence.
// Stores a copy of the winning bytes in an inline buffer (up to 256 bytes)
// or heap-allocated for longer strings.
// ---------------------------------------------------------------------------
struct MinBytesState {
    char*  buf     = nullptr;
    size_t length  = 0;
    size_t cap     = 0;
    bool   seen    = false;

    MinBytesState() noexcept = default;

    // No copy — this struct owns its buffer
    MinBytesState(const MinBytesState&) = delete;
    MinBytesState& operator=(const MinBytesState&) = delete;

    ~MinBytesState() noexcept {
        if (buf) { free(buf); buf = nullptr; }
    }

    inline void _store(const char* data, size_t len) noexcept {
        if (len > cap) {
            if (buf) free(buf);
            buf = (char*)malloc(len);
            cap = buf ? len : 0;
        }
        if (buf) {
            memcpy(buf, data, len);
            length = len;
        }
    }

    inline void apply_one(const char* data, size_t len) noexcept {
        if (!seen || compare_bytes(data, len, buf, length) < 0) {
            _store(data, len);
            seen = true;
        }
    }
};


// ---------------------------------------------------------------------------
// MaxBytesState
// ---------------------------------------------------------------------------
struct MaxBytesState {
    char*  buf     = nullptr;
    size_t length  = 0;
    size_t cap     = 0;
    bool   seen    = false;

    MaxBytesState() noexcept = default;
    MaxBytesState(const MaxBytesState&) = delete;
    MaxBytesState& operator=(const MaxBytesState&) = delete;

    ~MaxBytesState() noexcept {
        if (buf) { free(buf); buf = nullptr; }
    }

    inline void _store(const char* data, size_t len) noexcept {
        if (len > cap) {
            if (buf) free(buf);
            buf = (char*)malloc(len);
            cap = buf ? len : 0;
        }
        if (buf) {
            memcpy(buf, data, len);
            length = len;
        }
    }

    inline void apply_one(const char* data, size_t len) noexcept {
        if (!seen || compare_bytes(data, len, buf, length) > 0) {
            _store(data, len);
            seen = true;
        }
    }
};


// ---------------------------------------------------------------------------
// CountState
// ---------------------------------------------------------------------------
struct CountState {
    int64_t count = 0;

    inline void apply(const uint8_t* nulls, size_t n) noexcept {
        if (nulls == nullptr) {
            count += static_cast<int64_t>(n);
        } else {
            for (size_t i = 0; i < n; ++i) {
                if (bitmap_valid(nulls, i)) ++count;
            }
        }
    }

    inline void apply_star(size_t n) noexcept {
        count += static_cast<int64_t>(n);
    }
};

// ---------------------------------------------------------------------------
// MedianState — buffers non-null doubles, computes exact median via
// std::nth_element on finalize.
//
// Memory guard: a GLOBAL byte budget across every MedianState buffer, not a
// per-group value cap. Exact MEDIAN inherently buffers every non-null input
// value, so the real OOM risk is the TOTAL across all groups — a per-group
// cap bounded nothing (group count is unbounded) while refusing ordinary
// group sizes. The budget is charged on capacity growth (amortized — one
// atomic op per doubling, never per append) and released on free. Past the
// budget, `overflowed` latches and appends are refused — the engine must
// check and raise (fail loud; a query author who wants approximate opts in
// by name via APPROX_PERCENTILE, the budget never silently decides).
//
// The counter is a per-shared-object static (inline function local). The
// native engine is the only thing that compiles this header, so there is
// exactly one instance and every MEDIAN buffer in the process accounts
// against it. A second .so including this header would get its own counter
// and its own budget — check that before adding an includer.
// ---------------------------------------------------------------------------
constexpr int64_t kMedianBudgetFloorBytes = opteryx::agg_budgets::kMedianFloorBytes;  // 256MB
constexpr int64_t kMedianBudgetBytes      = opteryx::agg_budgets::kMedianBytes;       // 2GB

inline std::atomic<int64_t>& median_budget_used() noexcept {
    static std::atomic<int64_t> used{0};
    return used;
}

// The CURRENT ceiling, which starts at the floor and ratchets on demand.
//
// Process-wide, like the counter it guards — the two must have the same scope
// or the guard means nothing. That has a consequence worth knowing: while two
// MEDIAN queries overlap they share both the pool and the escalation, so a
// large one raises the ceiling for a small one, and the small one is what dies
// if the total blows 2GB. It resets to the floor whenever the counter returns
// to zero (see _release), so the escalation is effectively per-query for
// non-overlapping queries and shared only for genuinely concurrent ones.
inline std::atomic<int64_t>& median_budget_ceiling() noexcept {
    static std::atomic<int64_t> ceiling{kMedianBudgetFloorBytes};
    return ceiling;
}

// Raise the ceiling by doubling until it covers `needed`, or refuse. Returns
// false only when the hard ceiling genuinely cannot cover the demand — that is
// the fail-loud path, and the caller latches `overflowed` on it.
inline bool median_budget_escalate(int64_t needed) noexcept {
    std::atomic<int64_t>& ceiling = median_budget_ceiling();
    int64_t current = ceiling.load(std::memory_order_relaxed);
    while (current < needed) {
        if (current >= kMedianBudgetBytes) return false;
        int64_t next = current * 2;
        if (next > kMedianBudgetBytes) next = kMedianBudgetBytes;
        // compare_exchange_weak reloads `current` on failure, so a losing
        // racer re-tests against whatever the winner installed rather than
        // doubling a second time on top of it.
        ceiling.compare_exchange_weak(current, next, std::memory_order_relaxed);
    }
    return true;
}

struct MedianState {
    double* buf = nullptr;
    size_t  size = 0;
    size_t  cap = 0;
    bool    overflowed = false;

    MedianState() noexcept = default;
    MedianState(const MedianState&) = delete;
    MedianState& operator=(const MedianState&) = delete;

    MedianState(MedianState&& o) noexcept
        : buf(o.buf), size(o.size), cap(o.cap), overflowed(o.overflowed) {
        o.buf = nullptr; o.size = 0; o.cap = 0;
    }

    MedianState& operator=(MedianState&& o) noexcept {
        if (this != &o) {
            _release();
            buf = o.buf; size = o.size; cap = o.cap;
            overflowed = o.overflowed;
            o.buf = nullptr; o.size = 0; o.cap = 0;
        }
        return *this;
    }

    ~MedianState() noexcept { _release(); }

    inline void _release() noexcept {
        if (buf) {
            std::free(buf);
            const int64_t bytes =
                static_cast<int64_t>(cap) * static_cast<int64_t>(sizeof(double));
            if (median_budget_used().fetch_sub(bytes) - bytes == 0) {
                // Last buffer gone: hand the next query the floor rather than
                // whatever ceiling this one ratcheted to.
                median_budget_ceiling().store(kMedianBudgetFloorBytes,
                                              std::memory_order_relaxed);
            }
            buf = nullptr; size = 0; cap = 0;
        }
    }

    // Capacity growth. The budget charges CAPACITY, not values, so the growth
    // curve IS the memory profile — and a 64-slot first allocation made the
    // floor, not the data, the dominant cost whenever groups are small.
    // GROUP BY over a high-cardinality key is the ordinary case that broke:
    // 1e8 rows in 1e8 groups buffers 763MB of doubles and charged 47.7GB, a
    // 64x multiplier on a query holding one value per group.
    //
    // So: start at ONE slot (a singleton group is charged for exactly what it
    // holds), jump straight to 8 on the first growth to skip the churn a
    // 1,2,4 ramp would cost every small group, double while small, then grow
    // by a quarter past kMedianGeoLimit. Doubling is what overshoots large
    // groups — h2o g6's ~10,000-value groups each took a 16,384-slot buffer,
    // 39% waste — and a 1.25x tail bounds that without making small groups
    // pay realloc traffic for it.
    //
    // Worst case across the shapes measured (g6, GROUP BY user_id at 500k/10M
    // /100M distinct, one big group, 200 groups): 1.60x raw, against 64x for
    // the previous curve. Still geometric, so appends stay amortized O(1);
    // a 10-value group now takes 3 reallocs rather than 1, and a query that
    // used to be refused outright now runs.
    static constexpr size_t kMedianGeoLimit = 4096;

    inline size_t _next_cap(size_t current) const noexcept {
        if (current == 0) return 1;
        if (current == 1) return 8;
        return current < kMedianGeoLimit ? current * 2 : current + (current >> 2);
    }

    inline bool _grow(size_t need) noexcept {
        size_t new_cap = _next_cap(cap);
        while (new_cap < need) new_cap = _next_cap(new_cap);
        int64_t delta = static_cast<int64_t>(new_cap - cap)
                        * static_cast<int64_t>(sizeof(double));
        const int64_t used = median_budget_used().fetch_add(delta) + delta;
        if (used > median_budget_ceiling().load(std::memory_order_relaxed)
                && !median_budget_escalate(used)) {
            median_budget_used().fetch_sub(delta);
            overflowed = true;
            return false;
        }
        double* nb = (double*)std::realloc(buf, new_cap * sizeof(double));
        if (!nb) {
            median_budget_used().fetch_sub(delta);
            return false;
        }
        buf = nb;
        cap = new_cap;
        return true;
    }

    inline bool append(double v) noexcept {
        if (size >= cap && !_grow(size + 1)) return false;
        buf[size++] = v;
        return true;
    }

    // Compute median in place. Mutates buf via std::nth_element.
    // Returns 0.0 if size==0; caller must check size first.
    //
    // MEDIAN OVER NaN — RATIFIED SEMANTICS.
    // MEDIAN is an ORDER STATISTIC, and the order is the one float_ops.h
    // locked on 2026-05-22 and MIN/MAX already answer under: NaN is a VALUE
    // (validity bit set, not a null) that ranks above every other value,
    // including +inf. So a NaN in the input does NOT poison the result — it
    // sorts to the top and reaches the answer only when it is genuinely one
    // of the middle elements. 2000 values of which 24 are NaN: the 24 sit at
    // the top and the median is the mean of the 1000th and 1001st smallest,
    // exactly as if they were any other large values. For an even count where
    // exactly one of the two middles is NaN, the mean of the two selected
    // values is NaN — that is IEEE arithmetic on the selected pair, not NaN
    // propagation through the selection.
    //
    // The comparator MUST be fp_total_lt. Raw `<` is not a strict weak
    // ordering in the presence of NaN (every comparison with NaN is false),
    // so std::nth_element's precondition is violated and the partition
    // depends on the order values happened to arrive in — which varies with
    // morsel scheduling. That produced a silently wrong AND unstable MEDIAN.
    inline double finalize_median() noexcept {
        if (size == 0) return 0.0;
        constexpr auto total_lt = [](double a, double b) noexcept {
            return draken::ops::fp_total_lt<double>(a, b);
        };
        size_t mid = size / 2;
        std::nth_element(buf, buf + mid, buf + size, total_lt);
        double upper = buf[mid];
        if (size % 2 == 1) return upper;
        // Even count: lower middle is the max of the lower partition.
        // Partition [0, mid) was left after the first nth_element; the
        // largest element in it is the (mid-1)-th order statistic.
        std::nth_element(buf, buf + mid - 1, buf + mid, total_lt);
        double lower = buf[mid - 1];
        return (lower + upper) * 0.5;
    }
};

}} // namespace opteryx::ungrouped
