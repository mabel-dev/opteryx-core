#pragma once

#include <cstddef>
#include <cstdint>
#include <vector>
#include "csv_parse_context.hpp"

#if defined(__ARM_NEON) || defined(__ARM_NEON__)
#  include <arm_neon.h>
#elif defined(__AVX2__)
#  include <immintrin.h>
#endif

namespace rugo::_csv {

// ---------------------------------------------------------------------------
// Single-pass structural scan.
//
// Calls emit(uint32_t position, CsvMarkerType type) for every structural byte
// in the buffer in ascending position order. The NEON/AVX2 kernel finds
// candidates 16/32 bytes at a time; emit is inlined so the caller can either
// materialise markers or drive a state machine without an intermediate vector.
//
// `ctx.lut` encodes the five structural bytes; `ctx.delimiter` is used directly
// to load the NEON/AVX2 comparison register. Both must be consistent (i.e. the
// context was not mutated after construction without calling rebuild_lut()).
// ---------------------------------------------------------------------------
template <class Emit>
inline void scan_structural_csv(
    const uint8_t*        data,
    size_t                length,
    const CsvParseContext& ctx,
    Emit&&                emit)
{
    const uint8_t* lut = ctx.lut;

#if defined(__ARM_NEON) || defined(__ARM_NEON__)

    const uint8x16_t m_nl = vdupq_n_u8('\n');
    const uint8x16_t m_cr = vdupq_n_u8('\r');
    const uint8x16_t m_dl = vdupq_n_u8(ctx.delimiter);
    const uint8x16_t m_qu = vdupq_n_u8('"');
    const uint8x16_t m_bs = vdupq_n_u8('\\');

    size_t i = 0;
    for (; i + 16 <= length; i += 16) {
        const uint8x16_t v = vld1q_u8(data + i);
        const uint8x16_t any = vorrq_u8(
            vorrq_u8(
                vorrq_u8(vceqq_u8(v, m_nl), vceqq_u8(v, m_cr)),
                vorrq_u8(vceqq_u8(v, m_dl), vceqq_u8(v, m_qu))),
            vceqq_u8(v, m_bs));

        // Compress 16 byte-compare results to a 16-bit nibble mask
        uint64_t nibble_mask = vget_lane_u64(
            vreinterpret_u64_u8(vshrn_n_u16(vreinterpretq_u16_u8(any), 4)), 0);

        if (nibble_mask == 0) continue;

        while (nibble_mask) {
            const int b = __builtin_ctzll(nibble_mask) >> 2;
            const uint32_t pos = static_cast<uint32_t>(i + b);
            const uint8_t  raw = lut[data[pos]];
            if (raw) emit(pos, static_cast<CsvMarkerType>(raw - 1));
            nibble_mask &= ~(static_cast<uint64_t>(0xF) << (b << 2));
        }
    }
    // Scalar tail
    for (; i < length; ++i) {
        const uint8_t raw = lut[data[i]];
        if (raw) emit(static_cast<uint32_t>(i), static_cast<CsvMarkerType>(raw - 1));
    }

#elif defined(__AVX2__)

    const __m256i m_nl = _mm256_set1_epi8('\n');
    const __m256i m_cr = _mm256_set1_epi8('\r');
    const __m256i m_dl = _mm256_set1_epi8(static_cast<char>(ctx.delimiter));
    const __m256i m_qu = _mm256_set1_epi8('"');
    const __m256i m_bs = _mm256_set1_epi8('\\');

    size_t i = 0;
    for (; i + 32 <= length; i += 32) {
        const __m256i v = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(data + i));
        const __m256i any = _mm256_or_si256(
            _mm256_or_si256(
                _mm256_or_si256(_mm256_cmpeq_epi8(v, m_nl), _mm256_cmpeq_epi8(v, m_cr)),
                _mm256_or_si256(_mm256_cmpeq_epi8(v, m_dl), _mm256_cmpeq_epi8(v, m_qu))),
            _mm256_cmpeq_epi8(v, m_bs));

        uint32_t mask = static_cast<uint32_t>(_mm256_movemask_epi8(any));
        while (mask) {
            const int b = __builtin_ctz(mask);
            const uint32_t pos = static_cast<uint32_t>(i + b);
            const uint8_t  raw = lut[data[pos]];
            if (raw) emit(pos, static_cast<CsvMarkerType>(raw - 1));
            mask &= mask - 1;
        }
    }
    // Scalar tail
    for (; i < length; ++i) {
        const uint8_t raw = lut[data[i]];
        if (raw) emit(static_cast<uint32_t>(i), static_cast<CsvMarkerType>(raw - 1));
    }

#else

    // Portable scalar fallback
    for (size_t i = 0; i < length; ++i) {
        const uint8_t raw = lut[data[i]];
        if (raw) emit(static_cast<uint32_t>(i), static_cast<CsvMarkerType>(raw - 1));
    }

#endif
}

// ---------------------------------------------------------------------------
// Materialise all structural marker positions into a vector.
// Prefer scan_structural_csv() + a callback when materialisation is avoidable.
// ---------------------------------------------------------------------------
std::vector<CsvMarkerPosition> scan_csv_markers(
    const uint8_t*        data,
    size_t                length,
    const CsvParseContext& ctx);

// ---------------------------------------------------------------------------
// Safe-split discovery (serial).
//
// Walks a pre-scanned marker list (or rescans if markers is empty) tracking
// quote FSM state. Returns the byte offset of every '\n' that occurs outside
// a quoted field — these are safe points to split the buffer between threads.
//
// The returned list does NOT include offset 0 (the start) or `length` (the end).
// Callers splice adjacent offsets into ranges: [0, offsets[0]], [offsets[0]+1,
// offsets[1]], … and hand each range to a thread.
// ---------------------------------------------------------------------------
std::vector<uint32_t> find_safe_splits(
    const uint8_t*                        data,
    size_t                                length,
    const CsvParseContext&                ctx,
    const std::vector<CsvMarkerPosition>& markers = {});

// ---------------------------------------------------------------------------
// Safe-split discovery (parallel, prefix-sum FSM).
//
// Divides the buffer into `nt` equal chunks. Each chunk is independently
// scanned (SIMD) and run through the 4-state quote FSM four times — once
// per possible starting state. A tiny O(nt) serial composition step resolves
// the true initial state for each chunk, then safe \n positions are collected
// in order. This eliminates the serial scan bottleneck and scales to hardware
// concurrency for the dominant SIMD + FSM phase.
//
// Falls back to the serial version when nt <= 1.
// ---------------------------------------------------------------------------
std::vector<uint32_t> find_safe_splits_parallel(
    const uint8_t*         data,
    size_t                 length,
    const CsvParseContext& ctx,
    size_t                 nt);

}  // namespace rugo::_csv
