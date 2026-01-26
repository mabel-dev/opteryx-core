#include "simd_string_ops.h"
#include <cstdint>
#include <cstring>
#include <atomic>

#include "simd_dispatch.h"
#include "cpu_features.h"

#if defined(__AVX2__)
#include <immintrin.h>
#endif

#if defined(__ARM_NEON) || defined(__ARM_NEON__)
#include <arm_neon.h>
#endif

// SIMD-accelerated ASCII case conversion
// Handles only ASCII characters (0-127); non-ASCII bytes are left unchanged

// Constants for case conversion
static const uint8_t LOWER_A = 'a';
static const uint8_t LOWER_Z = 'z';
static const uint8_t UPPER_A = 'A';
static const uint8_t UPPER_Z = 'Z';
static const uint8_t CASE_DIFF = 'a' - 'A';  // 32

// We always provide a scalar fallback implementation. SIMD variants are compiled when
// the compiler supports them, and we select the best implementation at runtime via
// `simd::select_dispatch` so SIMD code is not executed on CPUs that lack support.

// Scalar fallback for to_upper
static void simd_to_upper_scalar(char* data, size_t length) {
    for (size_t i = 0; i < length; i++) {
        if (data[i] >= LOWER_A && data[i] <= LOWER_Z) {
            data[i] -= CASE_DIFF;
        }
    }
}

#if defined(__AVX2__)
static void simd_to_upper_avx2(char* data, size_t length) {
    size_t i = 0;
    __m256i lower_a_vec = _mm256_set1_epi8(LOWER_A);
    __m256i lower_z_vec = _mm256_set1_epi8(LOWER_Z);
    __m256i case_diff_vec = _mm256_set1_epi8(CASE_DIFF);

    for (; i + 32 <= length; i += 32) {
        __m256i chunk = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(data + i));
        __m256i is_ge_a = _mm256_cmpgt_epi8(chunk, _mm256_sub_epi8(lower_a_vec, _mm256_set1_epi8(1)));
        __m256i is_le_z = _mm256_cmpgt_epi8(_mm256_add_epi8(lower_z_vec, _mm256_set1_epi8(1)), chunk);
        __m256i is_lower = _mm256_and_si256(is_ge_a, is_le_z);
        __m256i to_subtract = _mm256_and_si256(is_lower, case_diff_vec);
        __m256i converted = _mm256_sub_epi8(chunk, to_subtract);
        _mm256_storeu_si256(reinterpret_cast<__m256i*>(data + i), converted);
    }

    for (; i < length; i++) {
        if (data[i] >= LOWER_A && data[i] <= LOWER_Z) {
            data[i] -= CASE_DIFF;
        }
    }
}
#endif

// Public wrapper that dispatches at runtime
void simd_to_upper(char* data, size_t length) {
    using fn_t = void(*)(char*, size_t);
    static std::atomic<fn_t> cache{nullptr};

    fn_t fn = simd::select_dispatch<fn_t>(cache, {
#if defined(__AVX2__)
        { &cpu_supports_avx2, simd_to_upper_avx2 },
#endif
#if defined(__ARM_NEON) || defined(__ARM_NEON__)
        { &cpu_supports_neon, simd_to_upper_scalar }, // NEON variant not implemented; fall back to scalar
#endif
    }, simd_to_upper_scalar);

    return fn(data, length);
}

// Scalar fallback for to_lower
static void simd_to_lower_scalar(char* data, size_t length) {
    for (size_t i = 0; i < length; i++) {
        if (data[i] >= UPPER_A && data[i] <= UPPER_Z) {
            data[i] += CASE_DIFF;
        }
    }
}

#if defined(__AVX2__)
static void simd_to_lower_avx2(char* data, size_t length) {
    size_t i = 0;
    __m256i upper_a_vec = _mm256_set1_epi8(UPPER_A);
    __m256i upper_z_vec = _mm256_set1_epi8(UPPER_Z);
    __m256i case_diff_vec = _mm256_set1_epi8(CASE_DIFF);

    for (; i + 32 <= length; i += 32) {
        __m256i chunk = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(data + i));
        __m256i is_ge_a = _mm256_cmpgt_epi8(chunk, _mm256_sub_epi8(upper_a_vec, _mm256_set1_epi8(1)));
        __m256i is_le_z = _mm256_cmpgt_epi8(_mm256_add_epi8(upper_z_vec, _mm256_set1_epi8(1)), chunk);
        __m256i is_upper = _mm256_and_si256(is_ge_a, is_le_z);
        __m256i to_add = _mm256_and_si256(is_upper, case_diff_vec);
        __m256i converted = _mm256_add_epi8(chunk, to_add);
        _mm256_storeu_si256(reinterpret_cast<__m256i*>(data + i), converted);
    }

    for (; i < length; i++) {
        if (data[i] >= UPPER_A && data[i] <= UPPER_Z) {
            data[i] += CASE_DIFF;
        }
    }
}
#endif

// Public wrapper that dispatches at runtime
void simd_to_lower(char* data, size_t length) {
    using fn_t = void(*)(char*, size_t);
    static std::atomic<fn_t> cache{nullptr};

    fn_t fn = simd::select_dispatch<fn_t>(cache, {
#if defined(__AVX2__)
        { &cpu_supports_avx2, simd_to_lower_avx2 },
#endif
#if defined(__ARM_NEON) || defined(__ARM_NEON__)
        { &cpu_supports_neon, simd_to_lower_scalar }, // NEON variant not implemented; fall back to scalar
#endif
    }, simd_to_lower_scalar);

    return fn(data, length);
}

// Scalar fallback for case-insensitive equals
static bool simd_equals_ci_scalar(const char* a, const char* b, size_t length) {
    for (size_t i = 0; i < length; i++) {
        unsigned char c1 = static_cast<unsigned char>(a[i]);
        unsigned char c2 = static_cast<unsigned char>(b[i]);
        
        // Convert c1 to lowercase if uppercase ASCII
        if (c1 >= UPPER_A && c1 <= UPPER_Z) {
            c1 += CASE_DIFF;
        }
        
        // Compare with b (assumed to be pre-lowercased)
        if (c1 != c2) {
            return false;
        }
    }
    return true;
}

#if defined(__AVX2__)
static bool simd_equals_ci_avx2(const char* a, const char* b, size_t length) {
    size_t i = 0;
    __m256i upper_a_vec = _mm256_set1_epi8(UPPER_A);
    __m256i threshold_vec = _mm256_set1_epi8(25);  // 'Z' - 'A'
    __m256i case_diff_vec = _mm256_set1_epi8(CASE_DIFF);

    // Process 32-byte chunks with SIMD
    for (; i + 32 <= length; i += 32) {
        // Load 32 bytes from each string
        __m256i chunk_a = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(a + i));
        __m256i chunk_b = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(b + i));
        
        // Detect uppercase ASCII letters in chunk_a using single comparison: (c - 'A') <= 25
        // Subtract 'A' from each byte
        __m256i offset = _mm256_sub_epi8(chunk_a, upper_a_vec);
        // Check if offset <= 25 using unsigned min: min(offset, 25) == offset means offset <= 25
        __m256i clamped = _mm256_min_epu8(offset, threshold_vec);
        __m256i is_upper = _mm256_cmpeq_epi8(offset, clamped);
        
        // Apply lowercase conversion: add 32 to uppercase letters
        __m256i to_add = _mm256_and_si256(is_upper, case_diff_vec);
        __m256i converted_a = _mm256_add_epi8(chunk_a, to_add);
        
        // Compare converted chunk_a with chunk_b
        __m256i cmp = _mm256_cmpeq_epi8(converted_a, chunk_b);
        
        // Check if all bytes matched
        int mask = _mm256_movemask_epi8(cmp);
        if (mask != -1) {  // Not all bits set means mismatch found
            return false;
        }
    }

    // Handle remaining bytes with scalar code
    for (; i < length; i++) {
        unsigned char c1 = static_cast<unsigned char>(a[i]);
        unsigned char c2 = static_cast<unsigned char>(b[i]);
        
        if (c1 >= UPPER_A && c1 <= UPPER_Z) {
            c1 += CASE_DIFF;
        }
        
        if (c1 != c2) {
            return false;
        }
    }
    
    return true;
}
#endif

#if defined(__ARM_NEON) || defined(__ARM_NEON__)
static bool simd_equals_ci_neon(const char* a, const char* b, size_t length) {
    size_t i = 0;
    const uint8x16_t upper_a_vec = vdupq_n_u8(UPPER_A);
    const uint8x16_t threshold_vec = vdupq_n_u8(25);  // 'Z' - 'A'
    const uint8x16_t case_diff_vec = vdupq_n_u8(CASE_DIFF);

    // Process 16-byte chunks with NEON
    for (; i + 16 <= length; i += 16) {
        // Load 16 bytes from each string
        uint8x16_t chunk_a = vld1q_u8(reinterpret_cast<const uint8_t*>(a + i));
        uint8x16_t chunk_b = vld1q_u8(reinterpret_cast<const uint8_t*>(b + i));
        
        // Detect uppercase ASCII letters in chunk_a using single comparison: (c - 'A') <= 25
        // Subtract 'A' from each byte
        uint8x16_t offset = vsubq_u8(chunk_a, upper_a_vec);
        // Check if offset <= 25 using vcleq_u8
        uint8x16_t is_upper = vcleq_u8(offset, threshold_vec);
        
        // Apply lowercase conversion: add 32 to uppercase letters
        // to_add will be 32 for uppercase, 0 otherwise
        uint8x16_t to_add = vandq_u8(is_upper, case_diff_vec);
        uint8x16_t converted_a = vaddq_u8(chunk_a, to_add);
        
        // Compare converted chunk_a with chunk_b
        uint8x16_t cmp = vceqq_u8(converted_a, chunk_b);
        
        // Check if all bytes matched
        // If all bytes are equal, all lanes will be 0xFF
        // We can check by reducing with AND and seeing if result is all 1s
        uint64x2_t cmp64 = vreinterpretq_u64_u8(cmp);
        uint64_t low = vgetq_lane_u64(cmp64, 0);
        uint64_t high = vgetq_lane_u64(cmp64, 1);
        
        if ((low & high) != 0xFFFFFFFFFFFFFFFFULL) {
            return false;  // Mismatch found
        }
    }

    // Handle remaining bytes with scalar code
    for (; i < length; i++) {
        unsigned char c1 = static_cast<unsigned char>(a[i]);
        unsigned char c2 = static_cast<unsigned char>(b[i]);
        
        if (c1 >= UPPER_A && c1 <= UPPER_Z) {
            c1 += CASE_DIFF;
        }
        
        if (c1 != c2) {
            return false;
        }
    }
    
    return true;
}
#endif

// Public wrapper that dispatches at runtime
bool simd_equals_ci(const char* a, const char* b, size_t length) {
    using fn_t = bool(*)(const char*, const char*, size_t);
    static std::atomic<fn_t> cache{nullptr};

    fn_t fn = simd::select_dispatch<fn_t>(cache, {
#if defined(__AVX2__)
        { &cpu_supports_avx2, simd_equals_ci_avx2 },
#endif
#if defined(__ARM_NEON) || defined(__ARM_NEON__)
        { &cpu_supports_neon, simd_equals_ci_neon },
#endif
    }, simd_equals_ci_scalar);

    return fn(a, b, length);
}