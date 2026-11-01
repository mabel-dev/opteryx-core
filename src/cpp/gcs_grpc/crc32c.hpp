// CRC32C (Castagnoli), as GCS's ChecksummedData.crc32c carries it.
//
// Compile-time dispatch like draken/simd/cpu_features.h: the hardware path is
// taken when the build's ISA floor guarantees the instruction, else the
// slice-by-8 table path. x86_64 builds (-march=haswell) imply SSE4.2. AArch64
// has CRC32 only where the floor enables it: Apple clang does; Linux
// `-march=armv8-a+aes` does NOT, so Linux ARM takes the table path until the
// floor gains `+crc`.
#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>

#if defined(__SSE4_2__)
#include <nmmintrin.h>
#elif defined(__ARM_FEATURE_CRC32)
#include <arm_acle.h>
#endif

namespace gcs_grpc {

namespace detail {
constexpr uint32_t kCrc32cPoly = 0x82F63B78u;  // reflected Castagnoli

constexpr std::array<std::array<uint32_t, 256>, 8> make_crc32c_tables() {
    std::array<std::array<uint32_t, 256>, 8> t{};
    for (uint32_t i = 0; i < 256; ++i) {
        uint32_t c = i;
        for (int k = 0; k < 8; ++k) c = (c >> 1) ^ ((c & 1u) ? kCrc32cPoly : 0u);
        t[0][i] = c;
    }
    for (uint32_t i = 0; i < 256; ++i)
        for (int s = 1; s < 8; ++s) t[s][i] = (t[s - 1][i] >> 8) ^ t[0][t[s - 1][i] & 0xFFu];
    return t;
}
inline constexpr auto kCrc32cTables = make_crc32c_tables();
}  // namespace detail

// Extends `crc` (0 for a fresh checksum) over [p, p + n).
inline uint32_t crc32c_extend(uint32_t crc, const uint8_t* p, size_t n) {
    uint32_t c = ~crc;
#if defined(__SSE4_2__) || defined(__ARM_FEATURE_CRC32)
    uint64_t c64 = c;
    for (; n >= 8; n -= 8, p += 8) {
        uint64_t w;
        std::memcpy(&w, p, 8);
#if defined(__SSE4_2__)
        c64 = _mm_crc32_u64(c64, w);
#else
        c64 = __crc32cd(static_cast<uint32_t>(c64), w);
#endif
    }
    c = static_cast<uint32_t>(c64);
    for (; n; --n, ++p) {
#if defined(__SSE4_2__)
        c = _mm_crc32_u8(c, *p);
#else
        c = __crc32cb(c, *p);
#endif
    }
#else
    const auto& t = detail::kCrc32cTables;
    for (; n >= 8; n -= 8, p += 8) {
        uint32_t lo, hi;
        std::memcpy(&lo, p, 4);
        std::memcpy(&hi, p + 4, 4);
        lo ^= c;
        c = t[7][lo & 0xFF] ^ t[6][(lo >> 8) & 0xFF] ^ t[5][(lo >> 16) & 0xFF] ^ t[4][lo >> 24] ^
            t[3][hi & 0xFF] ^ t[2][(hi >> 8) & 0xFF] ^ t[1][(hi >> 16) & 0xFF] ^ t[0][hi >> 24];
    }
    for (; n; --n, ++p) c = (c >> 8) ^ t[0][(c ^ *p) & 0xFF];
#endif
    return ~c;
}

inline uint32_t crc32c(const uint8_t* p, size_t n) { return crc32c_extend(0, p, n); }

}  // namespace gcs_grpc
