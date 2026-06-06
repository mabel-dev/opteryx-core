#include "structural_scan.hpp"

namespace rugo::_jsonl {

std::vector<MarkerPosition> scan_structural_markers(
    const uint8_t* data,
    size_t length,
    bool /*use_simd*/) {

    std::vector<MarkerPosition> result;
    if (length == 0) return result;

    // Object-style JSON runs ~1 structural marker per 3 bytes; reserve for that
    // density so the vector does not reallocate (each realloc copies the whole grown
    // vector mid-scan).
    result.reserve(length / 3);

    const uint8_t* lut = structural_lut();
    scan_structural(data, length, [&](uint32_t pos, uint8_t ch) {
        result.push_back(MarkerPosition(pos, static_cast<MarkerType>(lut[ch] - 1)));
    });
    return result;
}

#if defined(__ARM_NEON) || defined(__ARM_NEON__)
// Compress a NEON byte-compare result (each lane 0x00 or 0xFF) to a 16-bit mask.
static inline uint16_t neon_movemask(uint8x16_t cmp) {
    static const uint8_t kBits[16] = {1,2,4,8,16,32,64,128, 1,2,4,8,16,32,64,128};
    const uint8x16_t bitmask = vld1q_u8(kBits);
    const uint8x16_t m = vandq_u8(cmp, bitmask);
    return static_cast<uint16_t>(vaddv_u8(vget_low_u8(m)))
         | (static_cast<uint16_t>(vaddv_u8(vget_high_u8(m))) << 8);
}
#endif

std::vector<uint64_t> scan_structural_bitmap(const uint8_t* data, size_t length) {
    std::vector<uint64_t> bm(length == 0 ? 0 : (length + 63) / 64, 0ull);
    if (length == 0) return bm;
    const uint8_t* lut = structural_lut();

#if defined(__ARM_NEON) || defined(__ARM_NEON__)
    const uint8x16_t m_bo = vdupq_n_u8('{'), m_bc = vdupq_n_u8('}');
    const uint8x16_t m_so = vdupq_n_u8('['), m_sc = vdupq_n_u8(']');
    const uint8x16_t m_cl = vdupq_n_u8(':'), m_cm = vdupq_n_u8(',');
    const uint8x16_t m_qu = vdupq_n_u8('"'), m_bs = vdupq_n_u8('\\');
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
        uint16_t mask = neon_movemask(any);
        // i is a multiple of 16, so these 16 bits land cleanly within one 64-bit word.
        if (mask) bm[i >> 6] |= static_cast<uint64_t>(mask) << (i & 63);
    }
    for (; i < length; ++i)
        if (lut[data[i]]) bm[i >> 6] |= (1ull << (i & 63));
#else
    for (size_t i = 0; i < length; ++i)
        if (lut[data[i]]) bm[i >> 6] |= (1ull << (i & 63));
#endif
    return bm;
}

}  // namespace rugo::_jsonl
