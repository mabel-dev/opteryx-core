// Copied from jsonl_src/simd_helpers.hpp
#pragma once

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <atomic>

// Include CPU feature detection and dispatch
#include "cpu_features.h"
#include "simd_dispatch.h"

// Conditional SIMD header includes
#if defined(__AVX2__)
#include <immintrin.h>  // AVX2
#endif

#if defined(__ARM_NEON) || defined(__ARM_NEON__)
#include <arm_neon.h>  // NEON
#endif

namespace simd {

// ============================================================================
// FindNewline implementations
// ============================================================================

static const char* FindNewline_scalar(const char* data, size_t size) {
    const char* ptr = data;
    const char* end = data + size;
    while (ptr < end) {
        if (*ptr == '\n') {
            return ptr;
        }
        ptr++;
    }
    return nullptr;
}

#if defined(__AVX2__)
static const char* FindNewline_avx2(const char* data, size_t size) {
    const char* ptr = data;
    const char* end = data + size;
    
    if (size >= 32) {
        __m256i newline_vec = _mm256_set1_epi8('\n');
        const char* avx_end = end - 31;
        
        while (ptr < avx_end) {
            __m256i chunk = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(ptr));
            __m256i cmp = _mm256_cmpeq_epi8(chunk, newline_vec);
            int mask = _mm256_movemask_epi8(cmp);
            
            if (mask != 0) {
                int offset = __builtin_ctz(mask);
                return ptr + offset;
            }
            ptr += 32;
        }
    }
    
    // Scalar fallback for remaining bytes
    while (ptr < end) {
        if (*ptr == '\n') {
            return ptr;
        }
        ptr++;
    }
    return nullptr;
}
#endif

#if defined(__ARM_NEON) || defined(__ARM_NEON__)
static const char* FindNewline_neon(const char* data, size_t size) {
    const char* ptr = data;
    const char* end = data + size;
    
    if (size >= 16) {
        uint8x16_t newline_vec = vdupq_n_u8('\n');
        const char* neon_end = end - 15;
        
        while (ptr < neon_end) {
            uint8x16_t chunk = vld1q_u8(reinterpret_cast<const uint8_t*>(ptr));
            uint8x16_t cmp = vceqq_u8(chunk, newline_vec);
            
            uint64x2_t cmp64 = vreinterpretq_u64_u8(cmp);
            uint64_t low = vgetq_lane_u64(cmp64, 0);
            uint64_t high = vgetq_lane_u64(cmp64, 1);
            
            if (low != 0 || high != 0) {
                for (int i = 0; i < 16; i++) {
                    if (ptr[i] == '\n') {
                        return ptr + i;
                    }
                }
            }
            ptr += 16;
        }
    }
    
    // Scalar fallback for remaining bytes
    while (ptr < end) {
        if (*ptr == '\n') {
            return ptr;
        }
        ptr++;
    }
    return nullptr;
}
#endif

inline const char* FindNewline(const char* data, size_t size) {
    using fn_t = const char* (*)(const char*, size_t);
    static std::atomic<fn_t> cache{nullptr};
    
    fn_t fn = ::simd::select_dispatch<fn_t>(
        cache,
        {
#if defined(__AVX2__)
            { &cpu_supports_avx2, FindNewline_avx2 },
#endif
#if defined(__ARM_NEON) || defined(__ARM_NEON__)
            { &cpu_supports_neon, FindNewline_neon },
#endif
        },
        FindNewline_scalar
    );
    return fn(data, size);
}

// ============================================================================
// SkipWhitespace implementations
// ============================================================================

static const char* SkipWhitespace_scalar(const char* data, size_t size) {
    const char* ptr = data;
    const char* end = data + size;
    while (ptr < end && (*ptr == ' ' || *ptr == '\t' || *ptr == '\r')) {
        ptr++;
    }
    return ptr;
}

#if defined(__AVX2__)
static const char* SkipWhitespace_avx2(const char* data, size_t size) {
    const char* ptr = data;
    const char* end = data + size;
    
    if (size >= 32) {
        __m256i space_vec = _mm256_set1_epi8(' ');
        __m256i tab_vec = _mm256_set1_epi8('\t');
        __m256i cr_vec = _mm256_set1_epi8('\r');
        const char* avx_end = end - 31;
        
        while (ptr < avx_end) {
            __m256i chunk = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(ptr));
            __m256i is_space = _mm256_cmpeq_epi8(chunk, space_vec);
            __m256i is_tab = _mm256_cmpeq_epi8(chunk, tab_vec);
            __m256i is_cr = _mm256_cmpeq_epi8(chunk, cr_vec);
            __m256i is_ws = _mm256_or_si256(_mm256_or_si256(is_space, is_tab), is_cr);
            int mask = _mm256_movemask_epi8(is_ws);
            
            if (mask != 0xFFFFFFFF) {
                int offset = __builtin_ctz(~mask);
                return ptr + offset;
            }
            ptr += 32;
        }
    }
    
    while (ptr < end && (*ptr == ' ' || *ptr == '\t' || *ptr == '\r')) {
        ptr++;
    }
    return ptr;
}
#endif

#if defined(__ARM_NEON) || defined(__ARM_NEON__)
static const char* SkipWhitespace_neon(const char* data, size_t size) {
    const char* ptr = data;
    const char* end = data + size;
    
    if (size >= 16) {
        uint8x16_t space_vec = vdupq_n_u8(' ');
        uint8x16_t tab_vec = vdupq_n_u8('\t');
        uint8x16_t cr_vec = vdupq_n_u8('\r');
        const char* neon_end = end - 15;
        
        while (ptr < neon_end) {
            uint8x16_t chunk = vld1q_u8(reinterpret_cast<const uint8_t*>(ptr));
            uint8x16_t is_space = vceqq_u8(chunk, space_vec);
            uint8x16_t is_tab = vceqq_u8(chunk, tab_vec);
            uint8x16_t is_cr = vceqq_u8(chunk, cr_vec);
            uint8x16_t is_ws = vorrq_u8(vorrq_u8(is_space, is_tab), is_cr);
            
            uint64x2_t ws64 = vreinterpretq_u64_u8(is_ws);
            uint64_t low = vgetq_lane_u64(ws64, 0);
            uint64_t high = vgetq_lane_u64(ws64, 1);
            
            if (low != 0xFFFFFFFFFFFFFFFFULL || high != 0xFFFFFFFFFFFFFFFFULL) {
                for (int i = 0; i < 16; i++) {
                    if (ptr[i] != ' ' && ptr[i] != '\t' && ptr[i] != '\r') {
                        return ptr + i;
                    }
                }
            }
            ptr += 16;
        }
    }
    
    while (ptr < end && (*ptr == ' ' || *ptr == '\t' || *ptr == '\r')) {
        ptr++;
    }
    return ptr;
}
#endif

inline const char* SkipWhitespace(const char* data, size_t size) {
    using fn_t = const char* (*)(const char*, size_t);
    static std::atomic<fn_t> cache{nullptr};
    
    fn_t fn = ::simd::select_dispatch<fn_t>(
        cache,
        {
#if defined(__AVX2__)
            { &cpu_supports_avx2, SkipWhitespace_avx2 },
#endif
#if defined(__ARM_NEON) || defined(__ARM_NEON__)
            { &cpu_supports_neon, SkipWhitespace_neon },
#endif
        },
        SkipWhitespace_scalar
    );
    return fn(data, size);
}

// ============================================================================
// FindQuote implementations
// ============================================================================

static const char* FindQuote_scalar(const char* data, size_t size) {
    const char* ptr = data;
    const char* end = data + size;
    bool escaped = false;
    
    while (ptr < end) {
        if (escaped) {
            escaped = false;
        } else if (*ptr == '\\') {
            escaped = true;
        } else if (*ptr == '"') {
            return ptr;
        }
        ptr++;
    }
    return nullptr;
}

// Note: FindQuote SIMD implementations fall back to scalar for escape handling
// so we just use scalar for all architectures
inline const char* FindQuote(const char* data, size_t size) {
    return FindQuote_scalar(data, size);
}

// ============================================================================
// FindChar implementations
// ============================================================================

static const char* FindChar_scalar(const char* data, size_t size, char target) {
    const char* ptr = data;
    const char* end = data + size;
    while (ptr < end) {
        if (*ptr == target) {
            return ptr;
        }
        ptr++;
    }
    return nullptr;
}

#if defined(__AVX2__)
static const char* FindChar_avx2(const char* data, size_t size, char target) {
    const char* ptr = data;
    const char* end = data + size;
    
    if (size >= 32) {
        __m256i target_vec = _mm256_set1_epi8(target);
        const char* avx_end = end - 31;
        
        while (ptr < avx_end) {
            __m256i chunk = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(ptr));
            __m256i cmp = _mm256_cmpeq_epi8(chunk, target_vec);
            int mask = _mm256_movemask_epi8(cmp);
            
            if (mask != 0) {
                int offset = __builtin_ctz(mask);
                return ptr + offset;
            }
            ptr += 32;
        }
    }
    
    while (ptr < end) {
        if (*ptr == target) {
            return ptr;
        }
        ptr++;
    }
    return nullptr;
}
#endif

#if defined(__ARM_NEON) || defined(__ARM_NEON__)
static const char* FindChar_neon(const char* data, size_t size, char target) {
    const char* ptr = data;
    const char* end = data + size;
    
    if (size >= 16) {
        uint8x16_t target_vec = vdupq_n_u8(target);
        const char* neon_end = end - 15;
        
        while (ptr < neon_end) {
            uint8x16_t chunk = vld1q_u8(reinterpret_cast<const uint8_t*>(ptr));
            uint8x16_t cmp = vceqq_u8(chunk, target_vec);
            
            uint64x2_t cmp64 = vreinterpretq_u64_u8(cmp);
            uint64_t low = vgetq_lane_u64(cmp64, 0);
            uint64_t high = vgetq_lane_u64(cmp64, 1);
            
            if (low != 0 || high != 0) {
                for (int i = 0; i < 16; i++) {
                    if (ptr[i] == target) {
                        return ptr + i;
                    }
                }
            }
            ptr += 16;
        }
    }
    
    while (ptr < end) {
        if (*ptr == target) {
            return ptr;
        }
        ptr++;
    }
    return nullptr;
}
#endif

inline const char* FindChar(const char* data, size_t size, char target) {
    using fn_t = const char* (*)(const char*, size_t, char);
    static std::atomic<fn_t> cache{nullptr};
    
    fn_t fn = ::simd::select_dispatch<fn_t>(
        cache,
        {
#if defined(__AVX2__)
            { &cpu_supports_avx2, FindChar_avx2 },
#endif
#if defined(__ARM_NEON) || defined(__ARM_NEON__)
            { &cpu_supports_neon, FindChar_neon },
#endif
        },
        FindChar_scalar
    );
    return fn(data, size, target);
}

// ============================================================================
// CountNewlines implementations
// ============================================================================

static size_t CountNewlines_scalar(const char* data, size_t size) {
    const char* ptr = data;
    const char* end = data + size;
    size_t count = 0;
    
    while (ptr < end) {
        if (*ptr == '\n') {
            count++;
        }
        ptr++;
    }
    return count;
}

#if defined(__AVX2__)
static size_t CountNewlines_avx2(const char* data, size_t size) {
    const char* ptr = data;
    const char* end = data + size;
    size_t count = 0;
    
    if (size >= 32) {
        __m256i newline_vec = _mm256_set1_epi8('\n');
        const char* avx_end = end - 31;
        
        while (ptr < avx_end) {
            __m256i chunk = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(ptr));
            __m256i cmp = _mm256_cmpeq_epi8(chunk, newline_vec);
            int mask = _mm256_movemask_epi8(cmp);
            count += __builtin_popcount(mask);
            ptr += 32;
        }
    }
    
    while (ptr < end) {
        if (*ptr == '\n') {
            count++;
        }
        ptr++;
    }
    return count;
}
#endif

#if defined(__ARM_NEON) || defined(__ARM_NEON__)
static size_t CountNewlines_neon(const char* data, size_t size) {
    const char* ptr = data;
    const char* end = data + size;
    size_t count = 0;
    
    if (size >= 16) {
        uint8x16_t newline_vec = vdupq_n_u8('\n');
        const char* neon_end = end - 15;
        
        while (ptr < neon_end) {
            uint8x16_t chunk = vld1q_u8(reinterpret_cast<const uint8_t*>(ptr));
            uint8x16_t cmp = vceqq_u8(chunk, newline_vec);
            uint64x2_t cmp64 = vpaddlq_u32(vpaddlq_u16(vpaddlq_u8(cmp)));
            count += (vgetq_lane_u64(cmp64, 0) + vgetq_lane_u64(cmp64, 1)) / 255;
            ptr += 16;
        }
    }
    
    while (ptr < end) {
        if (*ptr == '\n') {
            count++;
        }
        ptr++;
    }
    return count;
}
#endif

inline size_t CountNewlines(const char* data, size_t size) {
    using fn_t = size_t (*)(const char*, size_t);
    static std::atomic<fn_t> cache{nullptr};
    
    fn_t fn = ::simd::select_dispatch<fn_t>(
        cache,
        {
#if defined(__AVX2__)
            { &cpu_supports_avx2, CountNewlines_avx2 },
#endif
#if defined(__ARM_NEON) || defined(__ARM_NEON__)
            { &cpu_supports_neon, CountNewlines_neon },
#endif
        },
        CountNewlines_scalar
    );
    return fn(data, size);
}

} // namespace simd
