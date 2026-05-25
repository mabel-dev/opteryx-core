#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>

#include "simd_dispatch.h"

#if defined(__AVX2__)
#include <immintrin.h>
#endif

#if defined(__ARM_NEON) || defined(__ARM_NEON__)
#include <arm_neon.h>
#endif

#if defined(__riscv) && defined(__riscv_vector)
#include <riscv_vector.h>
#endif

namespace opteryx::carchar::detail {

#if defined(__AVX2__) || defined(__ARM_NEON) || defined(__ARM_NEON__) || \
    (defined(__riscv) && defined(__riscv_vector))
constexpr std::size_t kProbeGroupWidth = 16;
#else
constexpr std::size_t kProbeGroupWidth = 8;
#endif

struct ProbeResult {
    std::size_t slot = 0;
    bool found = false;
    std::size_t probes = 0;
};

using ProbeFn = ProbeResult (*)(const std::uint8_t*, const std::uint64_t*, std::size_t, std::uint64_t, std::uint8_t);

constexpr std::uint64_t kByteOnes64 = 0x0101010101010101ULL;
constexpr std::uint64_t kByteHighBits64 = 0x8080808080808080ULL;

inline std::size_t first_mask_index(std::uint32_t mask) noexcept {
    return static_cast<std::size_t>(__builtin_ctz(mask));
}

inline std::size_t first_group_index64(std::uint64_t mask) noexcept {
    return static_cast<std::size_t>(__builtin_ctzll(mask) >> 3U);
}

inline std::uint64_t load_u64(const std::uint8_t* control) noexcept {
    std::uint64_t value = 0;
    std::memcpy(&value, control, sizeof(value));
    return value;
}

inline std::uint64_t match_mask64(std::uint64_t group, std::uint8_t tag) noexcept {
    const std::uint64_t tag_group = kByteOnes64 * static_cast<std::uint64_t>(tag);
    const std::uint64_t comparison = group ^ tag_group;
    return (comparison - kByteOnes64) & ~comparison & kByteHighBits64;
}

inline ProbeResult probe_find_slot_scalar(
    const std::uint8_t* control,
    const std::uint64_t* hashes,
    std::size_t capacity,
    std::uint64_t key,
    std::uint8_t tag
) noexcept {
    const std::size_t mask = capacity - 1U;
    std::size_t slot = static_cast<std::size_t>(key & mask);
    std::size_t probes = 0;

    while (probes < capacity) {
        const std::uint64_t group_lo = load_u64(control + slot);
        std::uint64_t matches_lo = match_mask64(group_lo, tag);
        const std::uint64_t empties_lo = group_lo & kByteHighBits64;

        while (matches_lo != 0U) {
            const std::size_t index = first_group_index64(matches_lo);
            if (empties_lo != 0U && index >= first_group_index64(empties_lo)) {
                break;
            }
            const std::size_t candidate = (slot + index) & mask;
            if (hashes[candidate] == key) {
                return {candidate, true, probes + index + 1U};
            }
            matches_lo &= (matches_lo - 1U);
        }

        if (empties_lo != 0U) {
            const std::size_t stop = first_group_index64(empties_lo);
            return {(slot + stop) & mask, false, probes + stop + 1U};
        }

#if defined(__AVX2__) || defined(__ARM_NEON) || defined(__ARM_NEON__) || \
    (defined(__riscv) && defined(__riscv_vector))
        const std::uint64_t group_hi = load_u64(control + slot + 8U);
        std::uint64_t matches_hi = match_mask64(group_hi, tag);
        const std::uint64_t empties_hi = group_hi & kByteHighBits64;

        while (matches_hi != 0U) {
            const std::size_t index = first_group_index64(matches_hi);
            if (empties_hi != 0U && index >= first_group_index64(empties_hi)) {
                break;
            }
            const std::size_t candidate = (slot + 8U + index) & mask;
            if (hashes[candidate] == key) {
                return {candidate, true, probes + 8U + index + 1U};
            }
            matches_hi &= (matches_hi - 1U);
        }

        if (empties_hi != 0U) {
            const std::size_t stop = 8U + first_group_index64(empties_hi);
            return {(slot + stop) & mask, false, probes + stop + 1U};
        }
#endif

        slot = (slot + kProbeGroupWidth) & mask;
        probes += kProbeGroupWidth;
    }

    return {0, false, capacity};
}

inline ProbeResult probe_find_bucket_scalar(
    const std::uint8_t* control,
    const std::uint64_t* hashes,
    std::size_t capacity,
    std::uint64_t key,
    std::uint8_t tag
) noexcept {
    const std::size_t bucket_count = capacity / kProbeGroupWidth;
    const std::size_t bucket_mask = bucket_count - 1U;
    std::size_t bucket_index = static_cast<std::size_t>(key) & bucket_mask;
    std::size_t probes = 0;

    while (probes < capacity) {
        const std::size_t slot = bucket_index * kProbeGroupWidth;
        const std::uint64_t group_lo = load_u64(control + slot);
        std::uint64_t matches_lo = match_mask64(group_lo, tag);
        const std::uint64_t empties_lo = group_lo & kByteHighBits64;

        while (matches_lo != 0U) {
            const std::size_t index = first_group_index64(matches_lo);
            if (empties_lo != 0U && index >= first_group_index64(empties_lo)) {
                break;
            }
            const std::size_t candidate = slot + index;
            if (hashes[candidate] == key) {
                return {candidate, true, probes + index + 1U};
            }
            matches_lo &= (matches_lo - 1U);
        }

        if (empties_lo != 0U) {
            const std::size_t stop = first_group_index64(empties_lo);
            return {slot + stop, false, probes + stop + 1U};
        }

#if defined(__AVX2__) || defined(__ARM_NEON) || defined(__ARM_NEON__) || \
    (defined(__riscv) && defined(__riscv_vector))
        const std::uint64_t group_hi = load_u64(control + slot + 8U);
        std::uint64_t matches_hi = match_mask64(group_hi, tag);
        const std::uint64_t empties_hi = group_hi & kByteHighBits64;

        while (matches_hi != 0U) {
            const std::size_t index = first_group_index64(matches_hi);
            if (empties_hi != 0U && index >= first_group_index64(empties_hi)) {
                break;
            }
            const std::size_t candidate = slot + 8U + index;
            if (hashes[candidate] == key) {
                return {candidate, true, probes + 8U + index + 1U};
            }
            matches_hi &= (matches_hi - 1U);
        }

        if (empties_hi != 0U) {
            const std::size_t stop = 8U + first_group_index64(empties_hi);
            return {slot + stop, false, probes + stop + 1U};
        }
#endif

        bucket_index = (bucket_index + 1U) & bucket_mask;
        probes += kProbeGroupWidth;
    }

    return {0, false, capacity};
}

#if defined(__AVX2__)
inline ProbeResult probe_find_slot_avx2(
    const std::uint8_t* control,
    const std::uint64_t* hashes,
    std::size_t capacity,
    std::uint64_t key,
    std::uint8_t tag
) noexcept {
    const std::size_t mask = capacity - 1U;
    std::size_t slot = static_cast<std::size_t>(key & mask);
    std::size_t probes = 0;
    const __m128i tag_vec = _mm_set1_epi8(static_cast<char>(tag));
    const __m128i empty_vec = _mm_set1_epi8(static_cast<char>(0x80U));

    while (probes < capacity) {
        const __m128i group = _mm_loadu_si128(reinterpret_cast<const __m128i*>(control + slot));
        std::uint32_t matches = static_cast<std::uint32_t>(
            _mm_movemask_epi8(_mm_cmpeq_epi8(group, tag_vec))
        );
        const std::uint32_t empties = static_cast<std::uint32_t>(
            _mm_movemask_epi8(_mm_cmpeq_epi8(group, empty_vec))
        );
        const std::size_t group_stop = empties == 0U ? 16U : first_mask_index(empties);

        while (matches != 0U) {
            const std::size_t index = first_mask_index(matches);
            if (index >= group_stop) {
                break;
            }
            const std::size_t candidate = (slot + index) & mask;
            if (hashes[candidate] == key) {
                return {candidate, true, probes + index + 1U};
            }
            matches &= (matches - 1U);
        }

        if (group_stop < 16U) {
            return {(slot + group_stop) & mask, false, probes + group_stop + 1U};
        }

        slot = (slot + 16U) & mask;
        probes += 16U;
    }

    return {0, false, capacity};
}

inline ProbeResult probe_find_bucket_avx2(
    const std::uint8_t* control,
    const std::uint64_t* hashes,
    std::size_t capacity,
    std::uint64_t key,
    std::uint8_t tag
) noexcept {
    const std::size_t bucket_count = capacity / 16U;
    const std::size_t bucket_mask = bucket_count - 1U;
    std::size_t bucket_index = static_cast<std::size_t>(key) & bucket_mask;
    std::size_t probes = 0;
    const __m128i tag_vec = _mm_set1_epi8(static_cast<char>(tag));
    const __m128i empty_vec = _mm_set1_epi8(static_cast<char>(0x80U));

    while (probes < capacity) {
        const std::size_t slot = bucket_index * 16U;
        const __m128i group = _mm_loadu_si128(reinterpret_cast<const __m128i*>(control + slot));
        std::uint32_t matches = static_cast<std::uint32_t>(
            _mm_movemask_epi8(_mm_cmpeq_epi8(group, tag_vec))
        );
        const std::uint32_t empties = static_cast<std::uint32_t>(
            _mm_movemask_epi8(_mm_cmpeq_epi8(group, empty_vec))
        );
        const std::size_t group_stop = empties == 0U ? 16U : first_mask_index(empties);

        while (matches != 0U) {
            const std::size_t index = first_mask_index(matches);
            if (index >= group_stop) {
                break;
            }
            const std::size_t candidate = slot + index;
            if (hashes[candidate] == key) {
                return {candidate, true, probes + index + 1U};
            }
            matches &= (matches - 1U);
        }

        if (group_stop < 16U) {
            return {slot + group_stop, false, probes + group_stop + 1U};
        }

        bucket_index = (bucket_index + 1U) & bucket_mask;
        probes += 16U;
    }

    return {0, false, capacity};
}
#endif

#if defined(__ARM_NEON) || defined(__ARM_NEON__)
inline ProbeResult probe_find_slot_neon(
    const std::uint8_t* control,
    const std::uint64_t* hashes,
    std::size_t capacity,
    std::uint64_t key,
    std::uint8_t tag
) noexcept {
    const std::size_t mask = capacity - 1U;
    std::size_t slot = static_cast<std::size_t>(key & mask);
    std::size_t probes = 0;
    const uint8x16_t tag_vec = vdupq_n_u8(tag);
    const uint8x16_t empty_vec = vdupq_n_u8(0x80U);

    while (probes < capacity) {
        const uint8x16_t group = vld1q_u8(control + slot);
        const uint8x16_t interesting =
            vorrq_u8(vceqq_u8(group, tag_vec), vceqq_u8(group, empty_vec));
        const uint64x2_t interesting_lanes = vreinterpretq_u64_u8(interesting);
        const std::uint64_t interesting_lo = vgetq_lane_u64(interesting_lanes, 0);
        const std::uint64_t interesting_hi = vgetq_lane_u64(interesting_lanes, 1);

        if ((interesting_lo | interesting_hi) == 0U) {
            slot = (slot + 16U) & mask;
            probes += 16U;
            continue;
        }

        if (interesting_lo != 0U) {
            const std::uint64_t group_lo = load_u64(control + slot);
            std::uint64_t matches_lo = match_mask64(group_lo, tag);
            const std::uint64_t empties_lo = group_lo & kByteHighBits64;

            while (matches_lo != 0U) {
                const std::size_t index = first_group_index64(matches_lo);
                if (empties_lo != 0U && index >= first_group_index64(empties_lo)) {
                    break;
                }
                const std::size_t candidate = (slot + index) & mask;
                if (hashes[candidate] == key) {
                    return {candidate, true, probes + index + 1U};
                }
                matches_lo &= (matches_lo - 1U);
            }

            if (empties_lo != 0U) {
                const std::size_t stop = first_group_index64(empties_lo);
                return {(slot + stop) & mask, false, probes + stop + 1U};
            }
        }

        if (interesting_hi != 0U) {
            const std::uint64_t group_hi = load_u64(control + slot + 8U);
            std::uint64_t matches_hi = match_mask64(group_hi, tag);
            const std::uint64_t empties_hi = group_hi & kByteHighBits64;

            while (matches_hi != 0U) {
                const std::size_t index = first_group_index64(matches_hi);
                if (empties_hi != 0U && index >= first_group_index64(empties_hi)) {
                    break;
                }
                const std::size_t candidate = (slot + 8U + index) & mask;
                if (hashes[candidate] == key) {
                    return {candidate, true, probes + 8U + index + 1U};
                }
                matches_hi &= (matches_hi - 1U);
            }

            if (empties_hi != 0U) {
                const std::size_t stop = 8U + first_group_index64(empties_hi);
                return {(slot + stop) & mask, false, probes + stop + 1U};
            }
        }

        slot = (slot + 16U) & mask;
        probes += 16U;
    }

    return {0, false, capacity};
}

inline ProbeResult probe_find_bucket_neon(
    const std::uint8_t* control,
    const std::uint64_t* hashes,
    std::size_t capacity,
    std::uint64_t key,
    std::uint8_t tag
) noexcept {
    const std::size_t bucket_count = capacity / 16U;
    const std::size_t bucket_mask = bucket_count - 1U;
    std::size_t bucket_index = static_cast<std::size_t>(key) & bucket_mask;
    std::size_t probes = 0;
    const uint8x16_t tag_vec = vdupq_n_u8(tag);
    const uint8x16_t empty_vec = vdupq_n_u8(0x80U);

    while (probes < capacity) {
        const std::size_t slot = bucket_index * 16U;
        const uint8x16_t group = vld1q_u8(control + slot);
        const uint8x16_t interesting =
            vorrq_u8(vceqq_u8(group, tag_vec), vceqq_u8(group, empty_vec));
        const uint64x2_t interesting_lanes = vreinterpretq_u64_u8(interesting);
        const std::uint64_t interesting_lo = vgetq_lane_u64(interesting_lanes, 0);
        const std::uint64_t interesting_hi = vgetq_lane_u64(interesting_lanes, 1);

        if ((interesting_lo | interesting_hi) == 0U) {
            bucket_index = (bucket_index + 1U) & bucket_mask;
            probes += 16U;
            continue;
        }

        if (interesting_lo != 0U) {
            const std::uint64_t group_lo = load_u64(control + slot);
            std::uint64_t matches_lo = match_mask64(group_lo, tag);
            const std::uint64_t empties_lo = group_lo & kByteHighBits64;

            while (matches_lo != 0U) {
                const std::size_t index = first_group_index64(matches_lo);
                if (empties_lo != 0U && index >= first_group_index64(empties_lo)) {
                    break;
                }
                const std::size_t candidate = slot + index;
                if (hashes[candidate] == key) {
                    return {candidate, true, probes + index + 1U};
                }
                matches_lo &= (matches_lo - 1U);
            }

            if (empties_lo != 0U) {
                const std::size_t stop = first_group_index64(empties_lo);
                return {slot + stop, false, probes + stop + 1U};
            }
        }

        if (interesting_hi != 0U) {
            const std::uint64_t group_hi = load_u64(control + slot + 8U);
            std::uint64_t matches_hi = match_mask64(group_hi, tag);
            const std::uint64_t empties_hi = group_hi & kByteHighBits64;

            while (matches_hi != 0U) {
                const std::size_t index = first_group_index64(matches_hi);
                if (empties_hi != 0U && index >= first_group_index64(empties_hi)) {
                    break;
                }
                const std::size_t candidate = slot + 8U + index;
                if (hashes[candidate] == key) {
                    return {candidate, true, probes + 8U + index + 1U};
                }
                matches_hi &= (matches_hi - 1U);
            }

            if (empties_hi != 0U) {
                const std::size_t stop = 8U + first_group_index64(empties_hi);
                return {slot + stop, false, probes + stop + 1U};
            }
        }

        bucket_index = (bucket_index + 1U) & bucket_mask;
        probes += 16U;
    }

    return {0, false, capacity};
}
#endif

#if defined(__riscv) && defined(__riscv_vector)
inline bool group_has_tag_or_empty_rvv(const std::uint8_t* control, std::uint8_t tag) noexcept {
    const std::size_t vl = __riscv_vsetvl_e8m4(16U);
    if (vl < 16U) {
        const std::uint64_t group_lo = load_u64(control);
        const std::uint64_t group_hi = load_u64(control + 8U);
        return ((match_mask64(group_lo, tag) | (group_lo & kByteHighBits64)) |
                (match_mask64(group_hi, tag) | (group_hi & kByteHighBits64))) != 0U;
    }
    const vuint8m4_t group = __riscv_vle8_v_u8m4(control, vl);
    const vbool2_t tag_matches = __riscv_vmseq_vx_u8m4_b2(group, tag, vl);
    const vbool2_t empty_matches = __riscv_vmseq_vx_u8m4_b2(group, 0x80U, vl);
    const vbool2_t interesting = __riscv_vmor_mm_b2(tag_matches, empty_matches, vl);
    return __riscv_vfirst_m_b2(interesting, vl) >= 0;
}

inline ProbeResult probe_find_slot_rvv(
    const std::uint8_t* control,
    const std::uint64_t* hashes,
    std::size_t capacity,
    std::uint64_t key,
    std::uint8_t tag
) noexcept {
    const std::size_t mask = capacity - 1U;
    std::size_t slot = static_cast<std::size_t>(key & mask);
    std::size_t probes = 0;

    while (probes < capacity) {
        if (!group_has_tag_or_empty_rvv(control + slot, tag)) {
            slot = (slot + 16U) & mask;
            probes += 16U;
            continue;
        }

        const std::uint64_t group_lo = load_u64(control + slot);
        std::uint64_t matches_lo = match_mask64(group_lo, tag);
        const std::uint64_t empties_lo = group_lo & kByteHighBits64;

        while (matches_lo != 0U) {
            const std::size_t index = first_group_index64(matches_lo);
            if (empties_lo != 0U && index >= first_group_index64(empties_lo)) {
                break;
            }
            const std::size_t candidate = (slot + index) & mask;
            if (hashes[candidate] == key) {
                return {candidate, true, probes + index + 1U};
            }
            matches_lo &= (matches_lo - 1U);
        }

        if (empties_lo != 0U) {
            const std::size_t stop = first_group_index64(empties_lo);
            return {(slot + stop) & mask, false, probes + stop + 1U};
        }

        const std::uint64_t group_hi = load_u64(control + slot + 8U);
        std::uint64_t matches_hi = match_mask64(group_hi, tag);
        const std::uint64_t empties_hi = group_hi & kByteHighBits64;

        while (matches_hi != 0U) {
            const std::size_t index = first_group_index64(matches_hi);
            if (empties_hi != 0U && index >= first_group_index64(empties_hi)) {
                break;
            }
            const std::size_t candidate = (slot + 8U + index) & mask;
            if (hashes[candidate] == key) {
                return {candidate, true, probes + 8U + index + 1U};
            }
            matches_hi &= (matches_hi - 1U);
        }

        if (empties_hi != 0U) {
            const std::size_t stop = 8U + first_group_index64(empties_hi);
            return {(slot + stop) & mask, false, probes + stop + 1U};
        }

        slot = (slot + 16U) & mask;
        probes += 16U;
    }

    return {0, false, capacity};
}

inline ProbeResult probe_find_bucket_rvv(
    const std::uint8_t* control,
    const std::uint64_t* hashes,
    std::size_t capacity,
    std::uint64_t key,
    std::uint8_t tag
) noexcept {
    const std::size_t bucket_count = capacity / 16U;
    const std::size_t bucket_mask = bucket_count - 1U;
    std::size_t bucket_index = static_cast<std::size_t>(key) & bucket_mask;
    std::size_t probes = 0;

    while (probes < capacity) {
        const std::size_t slot = bucket_index * 16U;
        if (!group_has_tag_or_empty_rvv(control + slot, tag)) {
            bucket_index = (bucket_index + 1U) & bucket_mask;
            probes += 16U;
            continue;
        }

        const std::uint64_t group_lo = load_u64(control + slot);
        std::uint64_t matches_lo = match_mask64(group_lo, tag);
        const std::uint64_t empties_lo = group_lo & kByteHighBits64;

        while (matches_lo != 0U) {
            const std::size_t index = first_group_index64(matches_lo);
            if (empties_lo != 0U && index >= first_group_index64(empties_lo)) {
                break;
            }
            const std::size_t candidate = slot + index;
            if (hashes[candidate] == key) {
                return {candidate, true, probes + index + 1U};
            }
            matches_lo &= (matches_lo - 1U);
        }

        if (empties_lo != 0U) {
            const std::size_t stop = first_group_index64(empties_lo);
            return {slot + stop, false, probes + stop + 1U};
        }

        const std::uint64_t group_hi = load_u64(control + slot + 8U);
        std::uint64_t matches_hi = match_mask64(group_hi, tag);
        const std::uint64_t empties_hi = group_hi & kByteHighBits64;

        while (matches_hi != 0U) {
            const std::size_t index = first_group_index64(matches_hi);
            if (empties_hi != 0U && index >= first_group_index64(empties_hi)) {
                break;
            }
            const std::size_t candidate = slot + 8U + index;
            if (hashes[candidate] == key) {
                return {candidate, true, probes + 8U + index + 1U};
            }
            matches_hi &= (matches_hi - 1U);
        }

        if (empties_hi != 0U) {
            const std::size_t stop = 8U + first_group_index64(empties_hi);
            return {slot + stop, false, probes + stop + 1U};
        }

        bucket_index = (bucket_index + 1U) & bucket_mask;
        probes += 16U;
    }

    return {0, false, capacity};
}
#endif

inline ProbeFn select_probe_finder() noexcept {
    using fn_t = ProbeFn;
    static std::atomic<fn_t> cache{nullptr};
    return simd::select_dispatch<fn_t>(
        cache,
        {
#if defined(__AVX2__)
            {&cpu_supports_avx2, probe_find_slot_avx2},
#endif
#if defined(__ARM_NEON) || defined(__ARM_NEON__)
            {&cpu_supports_neon, probe_find_slot_neon},
#endif
#if defined(__riscv) && defined(__riscv_vector)
            {&cpu_supports_rvv, probe_find_slot_rvv},
#endif
        },
        probe_find_slot_scalar
    );
}

inline ProbeFn select_bucket_probe_finder() noexcept {
    using fn_t = ProbeFn;
    static std::atomic<fn_t> cache{nullptr};
    return simd::select_dispatch<fn_t>(
        cache,
        {
#if defined(__AVX2__)
            {&cpu_supports_avx2, probe_find_bucket_avx2},
#endif
#if defined(__ARM_NEON) || defined(__ARM_NEON__)
            {&cpu_supports_neon, probe_find_bucket_neon},
#endif
#if defined(__riscv) && defined(__riscv_vector)
            {&cpu_supports_rvv, probe_find_bucket_rvv},
#endif
        },
        probe_find_bucket_scalar
    );
}

}  // namespace opteryx::carchar::detail
