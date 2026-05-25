#include "simd_search.h"
#include "cpu_features.h"
#include <cstdint>
#include <vector>
#include <cstring>

// Estimated match ratio for vector pre-allocation (1%)
static const size_t EXPECTED_MATCH_RATIO = 100;

#if defined(__ARM_NEON) || defined(__ARM_NEON__)
#include <arm_neon.h>
#endif

#if defined(__AVX2__)
#include <immintrin.h>
#endif

#if defined(__riscv) && defined(__riscv_vector)
#include <riscv_vector.h>
#endif

// SIMD substring search (up to 16 bytes pattern)
// Returns index of first occurrence or -1 if not found
// We'll provide a scalar implementation always and SIMD implementations
// conditionally. At runtime we dispatch to the best supported implementation.

static int simd_search_substring_scalar(const char* data, size_t length, const char* pattern, size_t pattern_len) {
    if (pattern_len == 0 || pattern_len > 16) return -1;
    for (size_t i = 0; i + pattern_len <= length; ++i) {
        if (memcmp(data + i, pattern, pattern_len) == 0) {
            return static_cast<int>(i);
        }
    }
    return -1;
}

#if defined(__AVX2__)
static int simd_search_substring_avx2(const char* data, size_t length, const char* pattern, size_t pattern_len) {
    if (pattern_len == 0 || pattern_len > 16) return -1;

    // Broadcast the first byte of the pattern into a 256-bit vector.
    // Scan 32 bytes at a time for first-byte candidates (O(N/32) SIMD ops),
    // then confirm each candidate with memcmp.  This replaces the original
    // sliding-window loop which advanced by 1 each iteration (O(N) SIMD ops).
    const __m256i first_vec = _mm256_set1_epi8(static_cast<char>(pattern[0]));
    size_t i = 0;

    for (; i + 32 <= length; i += 32) {
        const __m256i chunk = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(data + i));
        const __m256i cmp   = _mm256_cmpeq_epi8(chunk, first_vec);
        // movemask gives 1 bit per byte: bit k set → data[i+k] == pattern[0].
        uint32_t mask = static_cast<uint32_t>(_mm256_movemask_epi8(cmp));

        while (mask) {
            const int    bit = __builtin_ctz(mask);
            const size_t pos = i + static_cast<size_t>(bit);
            if (pos + pattern_len <= length &&
                memcmp(data + pos, pattern, pattern_len) == 0) {
                return static_cast<int>(pos);
            }
            mask &= mask - 1;  // clear lowest set bit
        }
    }

    // Scalar tail: fewer than 32 bytes remain.
    for (; i + pattern_len <= length; i++) {
        if (memcmp(data + i, pattern, pattern_len) == 0)
            return static_cast<int>(i);
    }
    return -1;
}
#endif

#if defined(__ARM_NEON) || defined(__ARM_NEON__)
static int simd_search_substring_neon(const char* data, size_t length, const char* pattern, size_t pattern_len) {
    if (pattern_len == 0 || pattern_len > 16) return -1;

    // Broadcast the first byte of the pattern into a NEON vector.
    // We scan 16 bytes at a time for first-byte candidates, then confirm
    // the full pattern with memcmp.  This gives O(N/16) NEON operations
    // plus O(hits) memcmp calls — much better than a scalar sliding window.
    const uint8x16_t first_vec = vdupq_n_u8(static_cast<uint8_t>(pattern[0]));
    size_t i = 0;

    for (; i + 16 <= length; i += 16) {
        // Load 16 bytes of the haystack and compare each byte to pattern[0].
        const uint8x16_t chunk = vld1q_u8(reinterpret_cast<const uint8_t*>(data + i));
        const uint8x16_t cmp   = vceqq_u8(chunk, first_vec);

        // Reinterpret the 16-byte comparison mask as two 64-bit words.
        // Each byte of cmp is 0xFF (match) or 0x00 (no match), so in the
        // 64-bit view a matching byte contributes 8 set bits.
        uint64_t lo = vgetq_lane_u64(vreinterpretq_u64_u8(cmp), 0);
        uint64_t hi = vgetq_lane_u64(vreinterpretq_u64_u8(cmp), 1);

        // Fast-skip chunks with no first-byte hits.
        if ((lo | hi) == 0) continue;

        // Iterate over every first-byte hit in the low 8 bytes.
        while (lo) {
            // __builtin_ctzll always returns a multiple of 8 here because
            // matching bytes are 0xFF (8 trailing zeros from the preceding
            // 0x00 byte boundary).
            const int    bit = __builtin_ctzll(lo);
            const size_t pos = i + static_cast<size_t>(bit >> 3);
            if (pos + pattern_len <= length &&
                memcmp(data + pos, pattern, pattern_len) == 0) {
                return static_cast<int>(pos);
            }
            // Clear this byte's bits so the next iteration finds the next hit.
            lo &= ~(static_cast<uint64_t>(0xFF) << bit);
        }

        // Iterate over every first-byte hit in the high 8 bytes.
        while (hi) {
            const int    bit = __builtin_ctzll(hi);
            const size_t pos = i + 8 + static_cast<size_t>(bit >> 3);
            if (pos + pattern_len <= length &&
                memcmp(data + pos, pattern, pattern_len) == 0) {
                return static_cast<int>(pos);
            }
            hi &= ~(static_cast<uint64_t>(0xFF) << bit);
        }
    }

    // Scalar tail: fewer than 16 bytes remain.
    for (; i + pattern_len <= length; i++) {
        if (memcmp(data + i, pattern, pattern_len) == 0)
            return static_cast<int>(i);
    }
    return -1;
}
#endif

#if defined(__riscv) && defined(__riscv_vector)
// ============================================================================
// RVV substring search.
//
// Fast-path: scan (length - i) bytes per iteration for the first byte of
// pattern using vmseq + vfirst.  Most iterations have no candidate (vfirst
// returns -1) and advance by vl bytes at O(1) cost.  When a candidate is
// found we confirm from that position with scalar memcmp up to the end of the
// loaded chunk.  Pattern matches are rare enough that the scalar tail does not
// dominate.
//
// LMUL=m8 maximises bytes scanned per vsetvl/vle cycle.
// ============================================================================
static int simd_search_substring_rvv(const char* data, size_t length,
                                     const char* pattern, size_t pattern_len) {
    if (pattern_len == 0 || pattern_len > 16) return -1;

    const uint8_t first = static_cast<uint8_t>(pattern[0]);
    size_t i = 0;

    while (i + pattern_len <= length) {
        size_t vl = __riscv_vsetvl_e8m8(length - i);
        vuint8m8_t chunk = __riscv_vle8_v_u8m8((const uint8_t*)(data + i), vl);
        vbool1_t   cmp   = __riscv_vmseq_vx_u8m8_b1(chunk, first, vl);
        long hit = __riscv_vfirst_m_b1(cmp, vl);

        if (hit >= 0) {
            // Scalar confirmation: scan from hit to end of this chunk.
            // j + pattern_len <= length ensures we don't read past the buffer.
            size_t end = i + vl;
            for (size_t j = i + (size_t)hit; j < end && j + pattern_len <= length; j++) {
                if (memcmp(data + j, pattern, pattern_len) == 0)
                    return static_cast<int>(j);
            }
        }

        i += vl;
    }
    return -1;
}
#endif  // __riscv_vector

using search_sub_fn_t = int (*)(const char*, size_t, const char*, size_t);

#include "simd_dispatch.h"

int simd_search_substring(const char* data, size_t length, const char* pattern, size_t pattern_len) {
    static std::atomic<search_sub_fn_t> cache{nullptr};
    search_sub_fn_t fn = simd::select_dispatch<search_sub_fn_t>(
        cache,
        {
#if defined(__AVX2__)
            { &cpu_supports_avx2, simd_search_substring_avx2 },
#endif
#if defined(__ARM_NEON) || defined(__ARM_NEON__)
            { &cpu_supports_neon, simd_search_substring_neon },
#endif
#if defined(__riscv) && defined(__riscv_vector)
            { &cpu_supports_rvv, simd_search_substring_rvv },
#endif
        },
        simd_search_substring_scalar
    );
    return fn(data, length, pattern, pattern_len);
}

#ifdef __AVX2__
#include <immintrin.h>
#endif

// NEON implementation for ARM
#if defined(__ARM_NEON) || defined(__ARM_NEON__)
int neon_search(const char* data, size_t length, char target) {
    size_t i = 0;
    // Create a vector with the target repeated.
    uint8x16_t target_vec = vdupq_n_u8(static_cast<uint8_t>(target));
    for (; i + 16 <= length; i += 16) {
        // Load 16 bytes.
        uint8x16_t chunk = vld1q_u8(reinterpret_cast<const uint8_t*>(data + i));
        // Compare each byte to the target.
        uint8x16_t cmp = vceqq_u8(chunk, target_vec);
        // Extract two 64-bit lanes; ctzll finds the first matching byte in 2 comparisons.
        uint64_t lo = vgetq_lane_u64(vreinterpretq_u64_u8(cmp), 0);
        uint64_t hi = vgetq_lane_u64(vreinterpretq_u64_u8(cmp), 1);
        if (lo != 0) return static_cast<int>(i + __builtin_ctzll(lo) / 8);
        if (hi != 0) return static_cast<int>(i + 8 + __builtin_ctzll(hi) / 8);
    }
    // Process any leftover bytes.
    for (; i < length; i++) {
        if (data[i] == target) {
            return static_cast<int>(i);
        }
    }
    return -1;
}
#elif defined(__riscv) && defined(__riscv_vector)
int neon_search(const char* data, size_t length, char target) {
    const uint8_t t = static_cast<uint8_t>(target);
    size_t i = 0;
    while (i < length) {
        size_t vl = __riscv_vsetvl_e8m8(length - i);
        vuint8m8_t chunk = __riscv_vle8_v_u8m8((const uint8_t*)(data + i), vl);
        vbool1_t cmp = __riscv_vmseq_vx_u8m8_b1(chunk, t, vl);
        long hit = __riscv_vfirst_m_b1(cmp, vl);
        if (hit >= 0) return static_cast<int>(i + (size_t)hit);
        i += vl;
    }
    return -1;
}
#else
// Fallback scalar implementation if NEON is not available.
int neon_search(const char* data, size_t length, char target) {
    for (size_t i = 0; i < length; i++) {
        if (data[i] == target)
            return static_cast<int>(i);
    }
    return -1;
}
#endif

// NEON find_all implementation for ARM
#if defined(__ARM_NEON) || defined(__ARM_NEON__)
std::vector<size_t> neon_find_all(const char* data, size_t length, char target) {
    std::vector<size_t> results;
    results.reserve(length / EXPECTED_MATCH_RATIO);  // Reserve space for ~1% matches as a reasonable estimate

    size_t i = 0;
    // Create a vector with the target repeated.
    uint8x16_t target_vec = vdupq_n_u8(static_cast<uint8_t>(target));

    for (; i + 16 <= length; i += 16) {
        // Load 16 bytes.
        uint8x16_t chunk = vld1q_u8(reinterpret_cast<const uint8_t*>(data + i));
        // Compare each byte to the target.
        uint8x16_t cmp = vceqq_u8(chunk, target_vec);
        // Store the comparison results into an array.
        uint8_t mask[16];
        vst1q_u8(mask, cmp);
        for (int j = 0; j < 16; j++) {
            if (mask[j] == 0xFF) {
                results.push_back(i + j);
            }
        }
    }

    // Process any leftover bytes.
    for (; i < length; i++) {
        if (data[i] == target) {
            results.push_back(i);
        }
    }

    return results;
}
#elif defined(__riscv) && defined(__riscv_vector)
// RVV: skip chunks with no matches via vcpop; extract positions with scalar scan.
// Fast-path eliminates push_back overhead for the common no-match case.
std::vector<size_t> neon_find_all(const char* data, size_t length, char target) {
    std::vector<size_t> results;
    results.reserve(length / EXPECTED_MATCH_RATIO);
    const uint8_t t = static_cast<uint8_t>(target);
    size_t i = 0;
    while (i < length) {
        size_t vl = __riscv_vsetvl_e8m8(length - i);
        vuint8m8_t chunk = __riscv_vle8_v_u8m8((const uint8_t*)(data + i), vl);
        vbool1_t cmp = __riscv_vmseq_vx_u8m8_b1(chunk, t, vl);
        if (__riscv_vcpop_m_b1(cmp, vl) > 0) {
            for (size_t j = 0; j < vl; j++) {
                if ((uint8_t)data[i + j] == t) results.push_back(i + j);
            }
        }
        i += vl;
    }
    return results;
}
#else
// Fallback scalar implementation if NEON is not available.
std::vector<size_t> neon_find_all(const char* data, size_t length, char target) {
    std::vector<size_t> results;
    for (size_t i = 0; i < length; i++) {
        if (data[i] == target) {
            results.push_back(i);
        }
    }
    return results;
}
#endif

// NEON count implementation for ARM
#if defined(__ARM_NEON) || defined(__ARM_NEON__)
size_t neon_count(const char* data, size_t length, char target) {
    size_t count = 0;
    size_t i = 0;
    // Create a vector with the target repeated.
    uint8x16_t target_vec = vdupq_n_u8(static_cast<uint8_t>(target));

    for (; i + 16 <= length; i += 16) {
        // Load 16 bytes.
        uint8x16_t chunk = vld1q_u8(reinterpret_cast<const uint8_t*>(data + i));
        // Compare each byte to the target.
        uint8x16_t cmp = vceqq_u8(chunk, target_vec);
        // Store the comparison results into an array.
        uint8_t mask[16];
        vst1q_u8(mask, cmp);
        for (int j = 0; j < 16; j++) {
            if (mask[j] == 0xFF) {
                count++;
            }
        }
    }

    // Process any leftover bytes.
    for (; i < length; i++) {
        if (data[i] == target) {
            count++;
        }
    }

    return count;
}
#elif defined(__riscv) && defined(__riscv_vector)
// RVV: vcpop counts matching bits per iteration — no per-element branch.
size_t neon_count(const char* data, size_t length, char target) {
    const uint8_t t = static_cast<uint8_t>(target);
    size_t count = 0, i = 0;
    while (i < length) {
        size_t vl = __riscv_vsetvl_e8m8(length - i);
        vuint8m8_t chunk = __riscv_vle8_v_u8m8((const uint8_t*)(data + i), vl);
        vbool1_t cmp = __riscv_vmseq_vx_u8m8_b1(chunk, t, vl);
        count += __riscv_vcpop_m_b1(cmp, vl);
        i += vl;
    }
    return count;
}
#else
// Fallback scalar implementation if NEON is not available.
size_t neon_count(const char* data, size_t length, char target) {
    size_t count = 0;
    for (size_t i = 0; i < length; i++) {
        if (data[i] == target) {
            count++;
        }
    }
    return count;
}
#endif

#if defined(__riscv) && defined(__riscv_vector)
// ============================================================================
// RVV implementations for the dispatched search/count/find_all/delimiter
// functions.  All use LMUL=m8 for maximum byte throughput per iteration.
// ============================================================================

static int avx_search_rvv(const char* data, size_t length, char target) {
    const uint8_t t = static_cast<uint8_t>(target);
    size_t i = 0;
    while (i < length) {
        size_t vl = __riscv_vsetvl_e8m8(length - i);
        vuint8m8_t chunk = __riscv_vle8_v_u8m8((const uint8_t*)(data + i), vl);
        vbool1_t cmp = __riscv_vmseq_vx_u8m8_b1(chunk, t, vl);
        long hit = __riscv_vfirst_m_b1(cmp, vl);
        if (hit >= 0) return static_cast<int>(i + (size_t)hit);
        i += vl;
    }
    return -1;
}

static size_t avx_count_rvv(const char* data, size_t length, char target) {
    const uint8_t t = static_cast<uint8_t>(target);
    size_t count = 0, i = 0;
    while (i < length) {
        size_t vl = __riscv_vsetvl_e8m8(length - i);
        vuint8m8_t chunk = __riscv_vle8_v_u8m8((const uint8_t*)(data + i), vl);
        vbool1_t cmp = __riscv_vmseq_vx_u8m8_b1(chunk, t, vl);
        count += __riscv_vcpop_m_b1(cmp, vl);
        i += vl;
    }
    return count;
}

static std::vector<size_t> simd_find_all_rvv(const char* data, size_t length, char target) {
    std::vector<size_t> results;
    results.reserve(length / EXPECTED_MATCH_RATIO);
    const uint8_t t = static_cast<uint8_t>(target);
    size_t i = 0;
    while (i < length) {
        size_t vl = __riscv_vsetvl_e8m8(length - i);
        vuint8m8_t chunk = __riscv_vle8_v_u8m8((const uint8_t*)(data + i), vl);
        vbool1_t cmp = __riscv_vmseq_vx_u8m8_b1(chunk, t, vl);
        // Skip chunks with no matches — eliminates push_back for the common case.
        if (__riscv_vcpop_m_b1(cmp, vl) > 0) {
            for (size_t j = 0; j < vl; j++) {
                if ((uint8_t)data[i + j] == t) results.push_back(i + j);
            }
        }
        i += vl;
    }
    return results;
}

// Delimiters: space (32), comma (44), '}' (125), tab (9)
static int avx_find_delimiter_rvv(const char* data, size_t length) {
    size_t i = 0;
    while (i < length) {
        size_t vl = __riscv_vsetvl_e8m8(length - i);
        vuint8m8_t chunk = __riscv_vle8_v_u8m8((const uint8_t*)(data + i), vl);
        vbool1_t m1 = __riscv_vmseq_vx_u8m8_b1(chunk,  32, vl);  // ' '
        vbool1_t m2 = __riscv_vmseq_vx_u8m8_b1(chunk,  44, vl);  // ','
        vbool1_t m3 = __riscv_vmseq_vx_u8m8_b1(chunk, 125, vl);  // '}'
        vbool1_t m4 = __riscv_vmseq_vx_u8m8_b1(chunk,   9, vl);  // '\t'
        vbool1_t any = __riscv_vmor_mm_b1(
            __riscv_vmor_mm_b1(m1, m2, vl),
            __riscv_vmor_mm_b1(m3, m4, vl), vl);
        long hit = __riscv_vfirst_m_b1(any, vl);
        if (hit >= 0) return static_cast<int>(i + (size_t)hit);
        i += vl;
    }
    return -1;
}
#endif  // __riscv_vector

// AVX2 implementation for x86 (if available)
// Always provide a scalar fallback implementation for avx_search so we can
// dispatch to it at runtime even when compiling with AVX flags.
static int avx_search_scalar(const char* data, size_t length, char target) {
    for (size_t i = 0; i < length; i++) {
        if (data[i] == target)
            return static_cast<int>(i);
    }
    return -1;
}

#if defined(__AVX2__)
static int avx_search_avx2(const char* data, size_t length, char target) {
    size_t i = 0;
    __m256i target_vec = _mm256_set1_epi8(target);
    for (; i + 32 <= length; i += 32) {
        __m256i chunk = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(data + i));
        __m256i cmp = _mm256_cmpeq_epi8(chunk, target_vec);
        int mask = _mm256_movemask_epi8(cmp);
        if (mask != 0) {
            // Use __builtin_ctz to find the lowest set bit.
            int offset = __builtin_ctz(mask);
            return static_cast<int>(i + offset);
        }
    }
    // Process remaining bytes.
    for (; i < length; i++) {
        if (data[i] == target)
            return static_cast<int>(i);
    }
    return -1;
}
#else
// If AVX2 support is compiled in, keep the scalar impl as
// the only available code.
// avx_search_avx2 won't be defined; use scalar only.
#endif

// Wrapper for avx_search that dispatches to available implementation
int avx_search(const char* data, size_t length, char target) {
    using fn_t = int (*)(const char*, size_t, char);
    static std::atomic<fn_t> cache{nullptr};
    fn_t fn = simd::select_dispatch<fn_t>(
        cache,
        {
#if defined(__AVX2__)
            { &cpu_supports_avx2, avx_search_avx2 },
#endif
#if defined(__riscv) && defined(__riscv_vector)
            { &cpu_supports_rvv, avx_search_rvv },
#endif
        },
        avx_search_scalar
    );
    return fn(data, length, target);
}

// Scalar fallback for find_all (always compiled)
static std::vector<size_t> avx_find_all_scalar(const char* data, size_t length, char target) {
    std::vector<size_t> results;
    for (size_t i = 0; i < length; i++) {
        if (data[i] == target) {
            results.push_back(i);
        }
    }
    return results;
}

#if defined(__AVX2__)
static std::vector<size_t> avx_find_all_avx2(const char* data, size_t length, char target) {
    std::vector<size_t> results;
    results.reserve(length / EXPECTED_MATCH_RATIO);  // Reserve space for ~1% matches as a reasonable estimate
    
    size_t i = 0;
    __m256i target_vec = _mm256_set1_epi8(target);
    
    for (; i + 32 <= length; i += 32) {
        __m256i chunk = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(data + i));
        __m256i cmp = _mm256_cmpeq_epi8(chunk, target_vec);
        int mask = _mm256_movemask_epi8(cmp);
        
        // Process all matches in this chunk
        while (mask != 0) {
            int offset = __builtin_ctz(mask);
            results.push_back(i + offset);
            mask &= (mask - 1);  // Clear the lowest set bit
        }
    }
    
    // Process remaining bytes.
    for (; i < length; i++) {
        if (data[i] == target) {
            results.push_back(i);
        }
    }
    
    return results;
}
#else
// If compiled without AVX2, avx_find_all_avx2 won't exist.
#endif

// Architecture-neutral dispatcher for finding all occurrences.
// Automatically selects the best SIMD implementation at runtime:
// - AVX2 (32 bytes/iteration) on x86 with AVX2 support
// - NEON (16 bytes/iteration) on ARM with NEON support  
// - Scalar fallback on other architectures
std::vector<size_t> simd_find_all(const char* data, size_t length, char target) {
    using fn_t = std::vector<size_t> (*)(const char*, size_t, char);
    static std::atomic<fn_t> cache{nullptr};
    fn_t fn = simd::select_dispatch<fn_t>(
        cache,
        {
#if defined(__AVX2__)
            { &cpu_supports_avx2, avx_find_all_avx2 },
#endif
#if defined(__ARM_NEON) || defined(__ARM_NEON__)
            { &cpu_supports_neon, neon_find_all },
#endif
#if defined(__riscv) && defined(__riscv_vector)
            { &cpu_supports_rvv, simd_find_all_rvv },
#endif
        },
        avx_find_all_scalar
    );
    return fn(data, length, target);
}

// Legacy wrapper for backward compatibility (deprecated)
std::vector<size_t> avx_find_all(const char* data, size_t length, char target) {
    return simd_find_all(data, length, target);
}

// Scalar fallback for avx_count
static size_t avx_count_scalar(const char* data, size_t length, char target) {
    size_t count = 0;
    for (size_t i = 0; i < length; i++) {
        if (data[i] == target) count++;
    }
    return count;
}

#if defined(__AVX2__)
static size_t avx_count_avx2(const char* data, size_t length, char target) {
    size_t count = 0;
    size_t i = 0;
    __m256i target_vec = _mm256_set1_epi8(target);
    
    for (; i + 32 <= length; i += 32) {
        __m256i chunk = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(data + i));
        __m256i cmp = _mm256_cmpeq_epi8(chunk, target_vec);
        int mask = _mm256_movemask_epi8(cmp);
        
        // Count the number of set bits in the mask
        count += __builtin_popcount(mask);
    }
    
    // Process remaining bytes.
    for (; i < length; i++) {
        if (data[i] == target) {
            count++;
        }
    }
    
    return count;
}
#else
// If not AVX2 compiled in, avx_count_avx2 won't exist.
#endif

// Wrapper that dispatches for avx_count
size_t avx_count(const char* data, size_t length, char target) {
    using fn_t = size_t (*)(const char*, size_t, char);
    static std::atomic<fn_t> cache{nullptr};
    fn_t fn = simd::select_dispatch<fn_t>(
        cache,
        {
#if defined(__AVX2__)
            { &cpu_supports_avx2, avx_count_avx2 },
#endif
#if defined(__riscv) && defined(__riscv_vector)
            { &cpu_supports_rvv, avx_count_rvv },
#endif
        },
        avx_count_scalar
    );
    return fn(data, length, target);
}


// NEON delimiter search for ARM
// Delimiters: space (32), comma (44), '}' (125), tab (9)
#if defined(__ARM_NEON) || defined(__ARM_NEON__)
int neon_find_delimiter(const char* data, size_t length) {
    size_t i = 0;

    // Create comparison vectors for all delimiters
    uint8x16_t space_vec = vdupq_n_u8(32);   // ' '
    uint8x16_t comma_vec = vdupq_n_u8(44);   // ','
    uint8x16_t brace_vec = vdupq_n_u8(125);  // '}'
    uint8x16_t tab_vec = vdupq_n_u8(9);      // '\t'

    for (; i + 16 <= length; i += 16) {
        // Load 16 bytes
        uint8x16_t chunk = vld1q_u8(reinterpret_cast<const uint8_t*>(data + i));

        // Compare with all delimiters
        uint8x16_t cmp_space = vceqq_u8(chunk, space_vec);
        uint8x16_t cmp_comma = vceqq_u8(chunk, comma_vec);
        uint8x16_t cmp_brace = vceqq_u8(chunk, brace_vec);
        uint8x16_t cmp_tab = vceqq_u8(chunk, tab_vec);

        // OR all comparisons together
        uint8x16_t result = vorrq_u8(cmp_space, cmp_comma);
        result = vorrq_u8(result, cmp_brace);
        result = vorrq_u8(result, cmp_tab);

        // Check if any match found
        uint8_t mask[16];
        vst1q_u8(mask, result);
        for (int j = 0; j < 16; j++) {
            if (mask[j] == 0xFF) {
                return static_cast<int>(i + j);
            }
        }
    }

    // Process any leftover bytes
    for (; i < length; i++) {
        char c = data[i];
        if (c == 32 || c == 44 || c == 125 || c == 9) {
            return static_cast<int>(i);
        }
    }

    return -1;
}
#elif defined(__riscv) && defined(__riscv_vector)
int neon_find_delimiter(const char* data, size_t length) {
    size_t i = 0;
    while (i < length) {
        size_t vl = __riscv_vsetvl_e8m8(length - i);
        vuint8m8_t chunk = __riscv_vle8_v_u8m8((const uint8_t*)(data + i), vl);
        vbool1_t m1 = __riscv_vmseq_vx_u8m8_b1(chunk,  32, vl);
        vbool1_t m2 = __riscv_vmseq_vx_u8m8_b1(chunk,  44, vl);
        vbool1_t m3 = __riscv_vmseq_vx_u8m8_b1(chunk, 125, vl);
        vbool1_t m4 = __riscv_vmseq_vx_u8m8_b1(chunk,   9, vl);
        vbool1_t any = __riscv_vmor_mm_b1(
            __riscv_vmor_mm_b1(m1, m2, vl),
            __riscv_vmor_mm_b1(m3, m4, vl), vl);
        long hit = __riscv_vfirst_m_b1(any, vl);
        if (hit >= 0) return static_cast<int>(i + (size_t)hit);
        i += vl;
    }
    return -1;
}
#else
// Fallback scalar implementation if NEON is not available
int neon_find_delimiter(const char* data, size_t length) {
    for (size_t i = 0; i < length; i++) {
        char c = data[i];
        if (c == 32 || c == 44 || c == 125 || c == 9) {
            return static_cast<int>(i);
        }
    }
    return -1;
}
#endif

// Delimiters: space (32), comma (44), '}' (125), tab (9)
// Scalar fallback for avx_find_delimiter
static int avx_find_delimiter_scalar(const char* data, size_t length) {
    for (size_t i = 0; i < length; i++) {
        char c = data[i];
        if (c == 32 || c == 44 || c == 125 || c == 9) {
            return static_cast<int>(i);
        }
    }
    return -1;
}

#if defined(__AVX2__)
// AVX2 delimiter search for x86 (fallback)
// Delimiters: space (32), comma (44), '}' (125), tab (9)
static int avx_find_delimiter_avx2(const char* data, size_t length) {
    size_t i = 0;
    
    // Create comparison vectors for all delimiters
    __m256i space_vec = _mm256_set1_epi8(32);   // ' '
    __m256i comma_vec = _mm256_set1_epi8(44);   // ','
    __m256i brace_vec = _mm256_set1_epi8(125);  // '}'
    __m256i tab_vec = _mm256_set1_epi8(9);      // '\t'
    
    for (; i + 32 <= length; i += 32) {
        // Load 32 bytes
        __m256i chunk = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(data + i));
        
        // Compare with all delimiters
        __m256i cmp_space = _mm256_cmpeq_epi8(chunk, space_vec);
        __m256i cmp_comma = _mm256_cmpeq_epi8(chunk, comma_vec);
        __m256i cmp_brace = _mm256_cmpeq_epi8(chunk, brace_vec);
        __m256i cmp_tab = _mm256_cmpeq_epi8(chunk, tab_vec);
        
        // OR all comparisons together
        __m256i result = _mm256_or_si256(cmp_space, cmp_comma);
        result = _mm256_or_si256(result, cmp_brace);
        result = _mm256_or_si256(result, cmp_tab);
        
        // Get mask of matching bytes
        int mask = _mm256_movemask_epi8(result);
        if (mask != 0) {
            // Find the first set bit
            int offset = __builtin_ctz(mask);
            return static_cast<int>(i + offset);
        }
    }
    
    // Process remaining bytes
    for (; i < length; i++) {
        char c = data[i];
        if (c == 32 || c == 44 || c == 125 || c == 9) {
            return static_cast<int>(i);
        }
    }
    
    return -1;
}
#endif

// Wrapper that dispatches to the best delimiter finder
int avx_find_delimiter(const char* data, size_t length) {
    using fn_t = int (*)(const char*, size_t);
    static std::atomic<fn_t> cache{nullptr};
    fn_t fn = simd::select_dispatch<fn_t>(
        cache,
        {
#if defined(__AVX2__)
            { &cpu_supports_avx2, avx_find_delimiter_avx2 },
#endif
#if defined(__riscv) && defined(__riscv_vector)
            { &cpu_supports_rvv, avx_find_delimiter_rvv },
#endif
        },
        avx_find_delimiter_scalar
    );
    return fn(data, length);
}
