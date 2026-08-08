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
// The counter is a per-shared-object static (inline function local): the one
// extension that actually executes MEDIAN (the native engine) accounts
// against a single instance; the legacy Cython spec-carrier modules never
// append.
// ---------------------------------------------------------------------------
constexpr int64_t kMedianBudgetBytes = opteryx::agg_budgets::kMedianBytes;   // 512MB

inline std::atomic<int64_t>& median_budget_used() noexcept {
    static std::atomic<int64_t> used{0};
    return used;
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
            median_budget_used().fetch_sub(
                static_cast<int64_t>(cap) * static_cast<int64_t>(sizeof(double)));
            buf = nullptr; size = 0; cap = 0;
        }
    }

    inline bool _grow(size_t need) noexcept {
        size_t new_cap = cap == 0 ? 64 : cap * 2;
        while (new_cap < need) new_cap *= 2;
        int64_t delta = static_cast<int64_t>(new_cap - cap)
                        * static_cast<int64_t>(sizeof(double));
        if (median_budget_used().fetch_add(delta) + delta > kMedianBudgetBytes) {
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
    inline double finalize_median() noexcept {
        if (size == 0) return 0.0;
        size_t mid = size / 2;
        std::nth_element(buf, buf + mid, buf + size);
        double upper = buf[mid];
        if (size % 2 == 1) return upper;
        // Even count: lower middle is the max of the lower partition.
        // Partition [0, mid) was left after the first nth_element; the
        // largest element in it is the (mid-1)-th order statistic.
        std::nth_element(buf, buf + mid - 1, buf + mid);
        double lower = buf[mid - 1];
        return (lower + upper) * 0.5;
    }
};

}} // namespace opteryx::ungrouped
