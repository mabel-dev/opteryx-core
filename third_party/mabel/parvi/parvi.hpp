#pragma once

// Parvi: a tiny fixed-capacity hash map for <= 16 entries.
//
// Design goals
// ------------
//   * Zero heap allocations — all storage is inline.
//   * Single SIMD-group probe: the whole table is one 16-byte control group,
//     so every lookup is one compare + one empty-check, no iterative probing.
//   * API-compatible with opteryx::carchar::CarcharIndex for the hot path
//     (lookup_fast / insert_new / find_or_insert / items) so callers can
//     promote to Carchar in one step when the "<= 16 items" estimate was
//     wrong.
//
// Promotion
// ---------
//   * `full()` returns true once capacity is exhausted.
//   * `drain_into(CarcharIndex&)` copies the live entries into a Carchar
//     using insert_new, preserving keys and payload refs.
//   * `insert_new` / `find_or_insert` return false/overflow when the table
//     is full so callers can swap to Carchar without silent corruption.
//
// Layout (linear, 16 slots):
//   control_[i]    : kEmpty (0x80) or tag (top 7 bits of key)
//   hashes_[i]     : full 64-bit key
//   payload_refs_[i] : int64 payload
//
// Keys must not use 0x80 as their top control byte — we reuse carchar's
// key_tag() which masks the top bit, so this is handled for us.

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <utility>
#include <vector>

#include "../carchar/carchar_common.hpp"
#include "../carchar/carchar_index.hpp"
#include "../carchar/carchar_simd.hpp"

#if defined(__AVX2__)
#include <immintrin.h>
#endif
#if defined(__ARM_NEON) || defined(__ARM_NEON__)
#include <arm_neon.h>
#endif

namespace opteryx::parvi {

using ::opteryx::carchar::kEmpty;
using ::opteryx::carchar::key_tag;

inline constexpr std::size_t kCapacity = 16;

struct ParviResult {
    std::size_t slot = 0;
    bool        found = false;
};

// Return a 16-bit mask: bit i set iff control_[i] == needle.
// Needle is either the tag or kEmpty (0x80).
inline std::uint32_t group_match_mask(const std::uint8_t* control, std::uint8_t needle) noexcept {
#if defined(__AVX2__)
    const __m128i group    = _mm_loadu_si128(reinterpret_cast<const __m128i*>(control));
    const __m128i needle_v = _mm_set1_epi8(static_cast<char>(needle));
    return static_cast<std::uint32_t>(_mm_movemask_epi8(_mm_cmpeq_epi8(group, needle_v)));
#elif defined(__ARM_NEON) || defined(__ARM_NEON__)
    // Each byte of `eq` is 0x00 or 0xFF. Multiply by a per-lane power of two
    // and horizontally sum each half to produce two 8-bit mask bytes.
    static const uint8_t kPowersArr[16] = {1, 2, 4, 8, 16, 32, 64, 128,
                                           1, 2, 4, 8, 16, 32, 64, 128};
    const uint8x16_t group   = vld1q_u8(control);
    const uint8x16_t eq      = vceqq_u8(group, vdupq_n_u8(needle));
    const uint8x16_t powers  = vld1q_u8(kPowersArr);
    const uint8x16_t masked  = vandq_u8(eq, powers);
    const std::uint8_t lo    = vaddv_u8(vget_low_u8(masked));
    const std::uint8_t hi    = vaddv_u8(vget_high_u8(masked));
    return static_cast<std::uint32_t>(lo) | (static_cast<std::uint32_t>(hi) << 8U);
#else
    // Scalar SWAR fallback using carchar's 64-bit match helpers.
    using ::opteryx::carchar::detail::kByteHighBits64;
    using ::opteryx::carchar::detail::load_u64;
    using ::opteryx::carchar::detail::match_mask64;
    const std::uint64_t lo = load_u64(control);
    const std::uint64_t hi = load_u64(control + 8U);
    std::uint64_t m_lo, m_hi;
    if (needle == kEmpty) {
        m_lo = lo & kByteHighBits64;
        m_hi = hi & kByteHighBits64;
    } else {
        m_lo = match_mask64(lo, needle);
        m_hi = match_mask64(hi, needle);
    }
    std::uint32_t mask = 0;
    for (std::size_t i = 0; i < 8; ++i) {
        if ((m_lo >> (i * 8U + 7U)) & 1U) mask |= 1U << i;
        if ((m_hi >> (i * 8U + 7U)) & 1U) mask |= 1U << (i + 8U);
    }
    return mask;
#endif
}

class ParviMap {
   public:
    ParviMap() noexcept {
        control_.fill(kEmpty);
    }

    static constexpr std::size_t capacity() noexcept { return kCapacity; }
    std::size_t size() const noexcept { return size_; }
    bool empty() const noexcept { return size_ == 0; }
    bool full() const noexcept { return size_ == kCapacity; }

    // Fast read path. Single SIMD group compare, then key verification on
    // any tag matches. Returns true iff key is present.
    bool lookup_fast(std::uint64_t key, std::int64_t& payload_ref_out) const noexcept {
        const std::uint8_t tag = key_tag(key);
        std::uint32_t matches = group_match_mask(control_.data(), tag);
        while (matches != 0U) {
            const std::size_t idx = static_cast<std::size_t>(__builtin_ctz(matches));
            if (hashes_[idx] == key) {
                payload_ref_out = payload_refs_[idx];
                return true;
            }
            matches &= (matches - 1U);
        }
        return false;
    }

    // Insert a new key with the given payload.
    //   * If the key already exists → returns {existing_slot, false}.
    //   * If the table is full       → returns {kCapacity, false}; caller
    //                                    should promote to CarcharIndex.
    //   * Otherwise                  → inserts and returns {new_slot, true}.
    ParviResult insert_new(std::uint64_t key, std::int64_t payload_ref) noexcept {
        const std::uint8_t tag = key_tag(key);
        std::uint32_t matches = group_match_mask(control_.data(), tag);
        while (matches != 0U) {
            const std::size_t idx = static_cast<std::size_t>(__builtin_ctz(matches));
            if (hashes_[idx] == key) {
                return {idx, false};
            }
            matches &= (matches - 1U);
        }
        if (size_ >= kCapacity) {
            return {kCapacity, false};
        }
        const std::uint32_t empties = group_match_mask(control_.data(), kEmpty);
        const std::size_t   slot    = static_cast<std::size_t>(__builtin_ctz(empties));
        control_[slot]               = tag;
        hashes_[slot]                = key;
        payload_refs_[slot]          = payload_ref;
        ++size_;
        return {slot, true};
    }

    // find_or_insert: returns (payload, inserted_bool).
    // On overflow (table full and key absent) the out-param `overflow` is
    // set to true and the returned payload is the factory's fresh value
    // which the caller is responsible for routing to the promoted map.
    template <typename PayloadFactory>
    std::pair<std::int64_t, bool> find_or_insert(
        std::uint64_t key, PayloadFactory&& payload_factory, bool& overflow
    ) {
        overflow = false;
        const std::uint8_t tag = key_tag(key);
        std::uint32_t matches = group_match_mask(control_.data(), tag);
        while (matches != 0U) {
            const std::size_t idx = static_cast<std::size_t>(__builtin_ctz(matches));
            if (hashes_[idx] == key) {
                return {payload_refs_[idx], false};
            }
            matches &= (matches - 1U);
        }
        const std::int64_t payload_ref = std::forward<PayloadFactory>(payload_factory)();
        if (size_ >= kCapacity) {
            overflow = true;
            return {payload_ref, true};
        }
        const std::uint32_t empties = group_match_mask(control_.data(), kEmpty);
        const std::size_t   slot    = static_cast<std::size_t>(__builtin_ctz(empties));
        control_[slot]               = tag;
        hashes_[slot]                = key;
        payload_refs_[slot]          = payload_ref;
        ++size_;
        return {payload_ref, true};
    }

    // Copy live entries into a CarcharIndex — use when overflow occurs or
    // the caller decides to promote early. The Carchar is reserved to hold
    // the existing entries plus headroom.
    void drain_into(::opteryx::carchar::CarcharIndex& target) const {
        target.reserve(size_);
        for (std::size_t i = 0; i < kCapacity; ++i) {
            if (control_[i] != kEmpty) {
                target.insert_new(hashes_[i], payload_refs_[i]);
            }
        }
    }

    std::vector<std::pair<std::uint64_t, std::int64_t>> items() const {
        std::vector<std::pair<std::uint64_t, std::int64_t>> out;
        out.reserve(size_);
        for (std::size_t i = 0; i < kCapacity; ++i) {
            if (control_[i] != kEmpty) {
                out.emplace_back(hashes_[i], payload_refs_[i]);
            }
        }
        return out;
    }

    void clear() noexcept {
        control_.fill(kEmpty);
        size_ = 0;
    }

   private:
    // 16-byte aligned control array for aligned SIMD loads if desired.
    alignas(16) std::array<std::uint8_t, kCapacity>  control_{};
    std::array<std::uint64_t, kCapacity>             hashes_{};
    std::array<std::int64_t, kCapacity>              payload_refs_{};
    std::size_t                                      size_ = 0;
};

}  // namespace opteryx::parvi
