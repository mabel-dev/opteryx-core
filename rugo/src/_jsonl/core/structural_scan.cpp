#include "structural_scan.hpp"
#include <cstring>
#include <cstdint>

#if defined(__ARM_NEON) || defined(__ARM_NEON__)
#include <arm_neon.h>
#endif

namespace rugo::_jsonl {

namespace {

struct MarkerLut {
    uint8_t table[256];
    MarkerLut() {
        std::memset(table, 0, 256);
        table[static_cast<uint8_t>('{')]  = static_cast<uint8_t>(MarkerType::BRACE_OPEN)   + 1;
        table[static_cast<uint8_t>('}')]  = static_cast<uint8_t>(MarkerType::BRACE_CLOSE)  + 1;
        table[static_cast<uint8_t>('[')]  = static_cast<uint8_t>(MarkerType::BRACKET_OPEN) + 1;
        table[static_cast<uint8_t>(']')]  = static_cast<uint8_t>(MarkerType::BRACKET_CLOSE)+ 1;
        table[static_cast<uint8_t>(':')]  = static_cast<uint8_t>(MarkerType::COLON)        + 1;
        table[static_cast<uint8_t>(',')]  = static_cast<uint8_t>(MarkerType::COMMA)        + 1;
        table[static_cast<uint8_t>('"')]  = static_cast<uint8_t>(MarkerType::QUOTE)        + 1;
        table[static_cast<uint8_t>('\\')] = static_cast<uint8_t>(MarkerType::BACKSLASH)    + 1;
        table[static_cast<uint8_t>('\n')] = static_cast<uint8_t>(MarkerType::NEWLINE)      + 1;
    }
};
static const MarkerLut g_lut;

static void scan_scalar(const uint8_t* data, size_t length, std::vector<MarkerPosition>& out) {
    const uint8_t* lut = g_lut.table;
    for (size_t i = 0; i < length; ++i) {
        uint8_t t = lut[data[i]];
        if (t) {
            out.push_back(MarkerPosition(static_cast<uint32_t>(i), static_cast<MarkerType>(t - 1)));
        }
    }
}

#if defined(__ARM_NEON) || defined(__ARM_NEON__)
static void scan_neon(const uint8_t* data, size_t length, std::vector<MarkerPosition>& out) {
    const uint8_t* lut = g_lut.table;

    const uint8x16_t m_bo = vdupq_n_u8('{');
    const uint8x16_t m_bc = vdupq_n_u8('}');
    const uint8x16_t m_so = vdupq_n_u8('[');
    const uint8x16_t m_sc = vdupq_n_u8(']');
    const uint8x16_t m_cl = vdupq_n_u8(':');
    const uint8x16_t m_cm = vdupq_n_u8(',');
    const uint8x16_t m_qu = vdupq_n_u8('"');
    const uint8x16_t m_bs = vdupq_n_u8('\\');
    const uint8x16_t m_nl = vdupq_n_u8('\n');

    size_t i = 0;
    for (; i + 16 <= length; i += 16) {
        uint8x16_t v = vld1q_u8(data + i);
        uint8x16_t any = vorrq_u8(
            vorrq_u8(
                vorrq_u8(vceqq_u8(v, m_bo), vceqq_u8(v, m_bc)),
                vorrq_u8(vceqq_u8(v, m_so), vceqq_u8(v, m_sc))),
            vorrq_u8(
                vorrq_u8(
                    vorrq_u8(vceqq_u8(v, m_cl), vceqq_u8(v, m_cm)),
                    vorrq_u8(vceqq_u8(v, m_qu), vceqq_u8(v, m_bs))),
                vceqq_u8(v, m_nl)));

        uint64_t nibble_mask = vget_lane_u64(
            vreinterpret_u64_u8(vshrn_n_u16(vreinterpretq_u16_u8(any), 4)), 0);

        if (nibble_mask == 0) continue;

        while (nibble_mask) {
            int b = __builtin_ctzll(nibble_mask) >> 2;
            out.push_back(MarkerPosition(static_cast<uint32_t>(i + b), static_cast<MarkerType>(lut[data[i + b]] - 1)));
            nibble_mask &= ~(static_cast<uint64_t>(0xF) << (b << 2));
        }
    }
    // Tail
    for (; i < length; ++i) {
        uint8_t t = lut[data[i]];
        if (t) {
            out.push_back(MarkerPosition(static_cast<uint32_t>(i), static_cast<MarkerType>(t - 1)));
        }
    }
}
#endif

}  // namespace

std::vector<MarkerPosition> scan_structural_markers(
    const uint8_t* data,
    size_t length,
    bool use_simd) {

    std::vector<MarkerPosition> result;
    if (length == 0) return result;

    result.reserve(length / 12);

    if (!use_simd) {
        scan_scalar(data, length, result);
        return result;
    }

#if defined(__ARM_NEON) || defined(__ARM_NEON__)
    scan_neon(data, length, result);
#else
    scan_scalar(data, length, result);
#endif

    return result;
}

}  // namespace rugo::_jsonl
