#pragma once

#include <cstdint>
#include <cstddef>

// Minimal inline implementations for timestamp casting
// Provides 4-unrolled scalar loops (SIMD dispatch can be added later)

extern "C" {
    // Multiply int64 array by factor
    inline void multiply_int64_simd(const int64_t* src, int64_t* dst, int64_t factor, size_t n) {
        size_t i = 0;
        for (; i + 3 < n; i += 4) {
            dst[i]     = src[i]     * factor;
            dst[i + 1] = src[i + 1] * factor;
            dst[i + 2] = src[i + 2] * factor;
            dst[i + 3] = src[i + 3] * factor;
        }
        for (; i < n; ++i) {
            dst[i] = src[i] * factor;
        }
    }

    // Divide int64 array by divisor
    inline void divide_int64_simd(const int64_t* src, int64_t* dst, int64_t divisor, size_t n) {
        size_t i = 0;
        for (; i + 3 < n; i += 4) {
            dst[i]     = src[i]     / divisor;
            dst[i + 1] = src[i + 1] / divisor;
            dst[i + 2] = src[i + 2] / divisor;
            dst[i + 3] = src[i + 3] / divisor;
        }
        for (; i < n; ++i) {
            dst[i] = src[i] / divisor;
        }
    }
}
