// Hand-written C++ header for ungrouped aggregate state structs.
// Underscore prefix distinguishes from Cython-generated .cpp files.
//
// All types are in namespace opteryx::ungrouped.
// All methods are inline in the header — no separate .cpp needed.

#pragma once

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>

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

}} // namespace opteryx::ungrouped
