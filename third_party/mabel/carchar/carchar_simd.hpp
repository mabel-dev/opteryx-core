#pragma once

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

template <std::size_t HStride>
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
            if (hashes[candidate * HStride] == key) {
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
            if (hashes[candidate * HStride] == key) {
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

#if defined(__AVX2__)
template <std::size_t HStride>
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
            if (hashes[candidate * HStride] == key) {
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
#endif

#if defined(__ARM_NEON) || defined(__ARM_NEON__)
// One bit per lane out of a 16-lane byte compare: bit 4*i+3 is set iff lane i of
// `eq` is all-ones. The narrowing shift packs each lane into a nibble, so a whole
// 16-byte group becomes ONE 64-bit word (lane_index = ctz(mask) >> 2; clear the
// lowest match with m & (m - 1)). Replaces the older two-getlane + SWAR byte
// arithmetic, which did the same job in a longer dependent chain
// (scratch/carchar_contest: 1.2-2.8x on the hit path, ARM).
inline std::uint64_t neon_lane_mask(uint8x16_t eq) noexcept {
    return vget_lane_u64(vreinterpret_u64_u8(vshrn_n_u16(vreinterpretq_u16_u8(eq), 4)), 0) &
           0x8888888888888888ULL;
}

template <std::size_t HStride>
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
        std::uint64_t matches = neon_lane_mask(vceqq_u8(group, tag_vec));
        const std::uint64_t empties = neon_lane_mask(vceqq_u8(group, empty_vec));
        // Same contract as the AVX2 probe: a tag match at or after the first empty
        // lane is never a candidate (nothing is inserted past an empty slot).
        const std::size_t group_stop =
            empties == 0U ? 16U : static_cast<std::size_t>(__builtin_ctzll(empties) >> 2U);

        while (matches != 0U) {
            const std::size_t index = static_cast<std::size_t>(__builtin_ctzll(matches) >> 2U);
            if (index >= group_stop) {
                break;
            }
            const std::size_t candidate = (slot + index) & mask;
            if (hashes[candidate * HStride] == key) {
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

template <std::size_t HStride>
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
            if (hashes[candidate * HStride] == key) {
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
            if (hashes[candidate * HStride] == key) {
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
#endif

// Direct, INLINABLE probe. The ISA variant is fixed at compile time
// (SIMD_STATIC_SELECT), so routing through the ProbeFn pointer bought nothing
// and cost everything: an indirect call the optimiser cannot see through, so
// the 16-way tag compare could never be fused into the caller's row loop and
// the control-array base could not stay in a register across rows. These call
// the chosen implementation by name — same code, no indirection.
template <std::size_t HStride = 1>
inline ProbeResult probe_find_slot_direct(const std::uint8_t* control,
                                          const std::uint64_t* hashes,
                                          std::size_t capacity, std::uint64_t key,
                                          std::uint8_t tag) noexcept {
    return SIMD_STATIC_SELECT(probe_find_slot_avx2<HStride>, probe_find_slot_neon<HStride>,
                              probe_find_slot_rvv<HStride>, probe_find_slot_scalar<HStride>)(
        control, hashes, capacity, key, tag);
}

// Phase-1 step of a BATCHED probe (CarcharSet::mark_new): the first lane of the
// group at `control` whose tag equals `tag` and that lies before the group's
// first empty lane — kProbeGroupWidth when there is none. A hint, not a verdict:
// the caller confirms the candidate's stored hash against the key, and anything
// else falls through to the full probe above. Reads kProbeGroupWidth bytes, so it
// stays inside the control array's (kGroupWidth - 1)-byte mirror tail exactly as
// the probes do. RVV builds take the scalar form (8 of the 16 lanes): fewer
// phase-1 hits, never a wrong one.
#if defined(__AVX2__)
inline std::size_t group_first_candidate(const std::uint8_t* control, std::uint8_t tag) noexcept {
    const __m128i group = _mm_loadu_si128(reinterpret_cast<const __m128i*>(control));
    const std::uint32_t matches = static_cast<std::uint32_t>(
        _mm_movemask_epi8(_mm_cmpeq_epi8(group, _mm_set1_epi8(static_cast<char>(tag)))));
    if (matches == 0U) return kProbeGroupWidth;
    const std::uint32_t empties = static_cast<std::uint32_t>(
        _mm_movemask_epi8(_mm_cmpeq_epi8(group, _mm_set1_epi8(static_cast<char>(0x80U)))));
    const std::size_t index = first_mask_index(matches);
    const std::size_t stop = empties == 0U ? 16U : first_mask_index(empties);
    return index < stop ? index : kProbeGroupWidth;
}
#elif defined(__ARM_NEON) || defined(__ARM_NEON__)
inline std::size_t group_first_candidate(const std::uint8_t* control, std::uint8_t tag) noexcept {
    const uint8x16_t group = vld1q_u8(control);
    const std::uint64_t matches = neon_lane_mask(vceqq_u8(group, vdupq_n_u8(tag)));
    if (matches == 0U) return kProbeGroupWidth;
    const std::uint64_t empties = neon_lane_mask(vceqq_u8(group, vdupq_n_u8(0x80U)));
    const std::size_t index = static_cast<std::size_t>(__builtin_ctzll(matches) >> 2U);
    const std::size_t stop =
        empties == 0U ? 16U : static_cast<std::size_t>(__builtin_ctzll(empties) >> 2U);
    return index < stop ? index : kProbeGroupWidth;
}
#else
inline std::size_t group_first_candidate(const std::uint8_t* control, std::uint8_t tag) noexcept {
    const std::uint64_t group = load_u64(control);
    const std::uint64_t matches = match_mask64(group, tag);
    if (matches == 0U) return kProbeGroupWidth;
    const std::uint64_t empties = group & kByteHighBits64;
    const std::size_t index = first_group_index64(matches);
    const std::size_t stop = empties == 0U ? 8U : first_group_index64(empties);
    return index < stop ? index : kProbeGroupWidth;
}
#endif

}  // namespace opteryx::carchar::detail
