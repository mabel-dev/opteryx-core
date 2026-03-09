/**
 * simd_datepart.cpp — Fast date-part extraction for int64 timestamp vectors.
 *
 * Design notes
 * ─────────────
 *  The critical optimisation here is NOT SIMD load/store but constant-divisor
 *  arithmetic: when the divisor is a compile-time literal the compiler replaces
 *  the expensive SDIV/UDIV instruction with a UMULH+shift sequence (ARM64) or
 *  MULQ+shift (x86-64).  On Apple Silicon this saves ~5 cycles per division.
 *
 *  NEON / AVX2 cannot vectorise 64-bit integer division — there is no
 *  vdivq_s64 or _mm256_div_epi64.  Attempts to use load/extract/repack
 *  wrappers around scalar arithmetic add overhead and are net slower.
 *
 *  The scalar path below gains the full constant-divisor benefit via a
 *  switch-outside-loop pattern: each loop body sees a literal constant so
 *  the compiler applies magic-number division.  The loop itself is 4-unrolled
 *  which provides enough independent operations for the OOO scheduler.
 *
 *  The simd::select_dispatch infrastructure is retained for API consistency
 *  and so a genuine SIMD path can be dropped in later (e.g. float-conversion
 *  trick for narrow timestamp ranges), but no NEON/AVX2 specialisation is
 *  registered here.
 *
 * unit_code encoding (matches vector_date_part.pyx):
 *   0 = seconds
 *   1 = milliseconds
 *   2 = microseconds  (Q19 hot path, 60 000 000 µs / minute)
 *   3 = nanoseconds
 */

#include "simd_datepart.h"
#include "simd_dispatch.h"
#include "cpu_features.h"

#include <atomic>
#include <cstdint>
#include <cstddef>

// ===========================================================================
// 4-unrolled scalar loop with a compile-time (div, mod) pair.
// The compiler generates UMULH+shift instead of SDIV for each literal divisor.
// ===========================================================================
#define EXTRACT_LOOP_4(div, mod)                                                    \
    do {                                                                            \
        size_t _i = 0;                                                              \
        for (; _i + 3 < n; _i += 4) {                                              \
            dst[_i]   = (int64_t)((uint64_t)src[_i]   / (uint64_t)(div) % (uint64_t)(mod)); \
            dst[_i+1] = (int64_t)((uint64_t)src[_i+1] / (uint64_t)(div) % (uint64_t)(mod)); \
            dst[_i+2] = (int64_t)((uint64_t)src[_i+2] / (uint64_t)(div) % (uint64_t)(mod)); \
            dst[_i+3] = (int64_t)((uint64_t)src[_i+3] / (uint64_t)(div) % (uint64_t)(mod)); \
        }                                                                           \
        for (; _i < n; ++_i)                                                        \
            dst[_i] = (int64_t)((uint64_t)src[_i] / (uint64_t)(div) % (uint64_t)(mod)); \
    } while (0)

// ===========================================================================
// MINUTE  (modulus 60)
// ===========================================================================

static void minute_scalar(const int64_t* src, int64_t* dst, size_t n, int unit_code) {
    switch (unit_code) {
        case 0:  EXTRACT_LOOP_4(60LL,            60LL); break;
        case 1:  EXTRACT_LOOP_4(60000LL,         60LL); break;
        case 2:  EXTRACT_LOOP_4(60000000LL,      60LL); break;
        case 3:  EXTRACT_LOOP_4(60000000000LL,   60LL); break;
        default: EXTRACT_LOOP_4(60000000LL,      60LL); break;
    }
}

// ===========================================================================
// HOUR  (modulus 24)
// ===========================================================================

static void hour_scalar(const int64_t* src, int64_t* dst, size_t n, int unit_code) {
    switch (unit_code) {
        case 0:  EXTRACT_LOOP_4(3600LL,            24LL); break;
        case 1:  EXTRACT_LOOP_4(3600000LL,         24LL); break;
        case 2:  EXTRACT_LOOP_4(3600000000LL,      24LL); break;
        case 3:  EXTRACT_LOOP_4(3600000000000LL,   24LL); break;
        default: EXTRACT_LOOP_4(3600000000LL,      24LL); break;
    }
}

// ===========================================================================
// SECOND  (modulus 60)
// ===========================================================================

static void second_scalar(const int64_t* src, int64_t* dst, size_t n, int unit_code) {
    switch (unit_code) {
        case 0:  EXTRACT_LOOP_4(1LL,          60LL); break;
        case 1:  EXTRACT_LOOP_4(1000LL,       60LL); break;
        case 2:  EXTRACT_LOOP_4(1000000LL,    60LL); break;
        case 3:  EXTRACT_LOOP_4(1000000000LL, 60LL); break;
        default: EXTRACT_LOOP_4(1000000LL,    60LL); break;
    }
}

// ===========================================================================
// Public dispatch functions
// No NEON/AVX2 candidates: 64-bit integer division cannot be vectorised via
// NEON (no vdivq_s64) or AVX2 (no _mm256_div_epi64).  The constant-divisor
// scalar path is the optimal implementation on all current targets.
// ===========================================================================

using dp_fn_t = void (*)(const int64_t*, int64_t*, size_t, int);

void simd_datepart_minute(const int64_t* src, int64_t* dst, size_t n, int unit_code) {
    static std::atomic<dp_fn_t> cache{nullptr};
    dp_fn_t fn = simd::select_dispatch<dp_fn_t>(cache, {}, minute_scalar);
    fn(src, dst, n, unit_code);
}

void simd_datepart_hour(const int64_t* src, int64_t* dst, size_t n, int unit_code) {
    static std::atomic<dp_fn_t> cache{nullptr};
    dp_fn_t fn = simd::select_dispatch<dp_fn_t>(cache, {}, hour_scalar);
    fn(src, dst, n, unit_code);
}

void simd_datepart_second(const int64_t* src, int64_t* dst, size_t n, int unit_code) {
    static std::atomic<dp_fn_t> cache{nullptr};
    dp_fn_t fn = simd::select_dispatch<dp_fn_t>(cache, {}, second_scalar);
    fn(src, dst, n, unit_code);
}

