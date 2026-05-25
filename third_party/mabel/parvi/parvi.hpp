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
#include "../carchar/carchar_set.hpp"
#include "../carchar/carchar_simd.hpp"

#if defined(__AVX2__)
#include <immintrin.h>
#endif
#if defined(__ARM_NEON) || defined(__ARM_NEON__)
#include <arm_neon.h>
#endif
#if defined(__riscv) && defined(__riscv_vector)
#include <riscv_vector.h>
#endif

namespace opteryx::parvi {

using ::opteryx::carchar::kEmpty;
using ::opteryx::carchar::key_tag;

inline constexpr std::size_t kCapacity = 16;

struct ParviResult {
    std::size_t slot = 0;
    bool        found = false;
};

// ------------------------------------------------------------------
// Performance improvement #1: faster scalar mask extraction
//
// The old scalar fallback looped over 8 bytes twice. Now we use a
// 64-bit multiply to gather high bits into a single byte per 8-byte
// chunk. This is branchless and reduces ~16 iterations to 2 multiplications.
// ------------------------------------------------------------------
inline std::uint32_t group_match_mask(const std::uint8_t* control, std::uint8_t needle) noexcept {
#if defined(__AVX2__)
    const __m128i group    = _mm_loadu_si128(reinterpret_cast<const __m128i*>(control));
    const __m128i needle_v = _mm_set1_epi8(static_cast<char>(needle));
    return static_cast<std::uint32_t>(_mm_movemask_epi8(_mm_cmpeq_epi8(group, needle_v)));
#elif defined(__riscv) && defined(__riscv_vector)
    static constexpr std::uint8_t kPowersArr[16] = {1, 2, 4, 8, 16, 32, 64, 128,
                                                    1, 2, 4, 8, 16, 32, 64, 128};
    alignas(16) std::uint8_t masked_bytes[16] = {};
    const std::size_t vl = __riscv_vsetvl_e8m4(kCapacity);
    if (vl < kCapacity) {
        using ::opteryx::carchar::detail::kByteHighBits64;
        using ::opteryx::carchar::detail::load_u64;
        using ::opteryx::carchar::detail::match_mask64;
        const std::uint64_t lo = load_u64(control);
        const std::uint64_t hi = load_u64(control + 8U);
        const std::uint64_t m_lo = needle == kEmpty ? (lo & kByteHighBits64) : match_mask64(lo, needle);
        const std::uint64_t m_hi = needle == kEmpty ? (hi & kByteHighBits64) : match_mask64(hi, needle);
        constexpr std::uint64_t kHighBitMask = 0x8080808080808080ULL;
        constexpr std::uint64_t kGather = 0x8040201008040201ULL;
        const auto to_mask = [](std::uint64_t bits) -> std::uint8_t {
            return static_cast<std::uint8_t>(((bits & kHighBitMask) * kGather) >> 56);
        };
        return static_cast<std::uint32_t>(to_mask(m_lo)) |
               (static_cast<std::uint32_t>(to_mask(m_hi)) << 8U);
    }
    const vuint8m4_t group = __riscv_vle8_v_u8m4(control, vl);
    const vuint8m4_t powers = __riscv_vle8_v_u8m4(kPowersArr, vl);
    const vuint8m4_t zero = __riscv_vmv_v_x_u8m4(0U, vl);
    const vbool2_t eq = __riscv_vmseq_vx_u8m4_b2(group, needle, vl);
    const vuint8m4_t masked = __riscv_vmerge_vvm_u8m4(zero, powers, eq, vl);
    __riscv_vse8_v_u8m4(masked_bytes, masked, vl);
    return static_cast<std::uint32_t>(masked_bytes[0] | masked_bytes[1] | masked_bytes[2] |
                                      masked_bytes[3] | masked_bytes[4] | masked_bytes[5] |
                                      masked_bytes[6] | masked_bytes[7]) |
           (static_cast<std::uint32_t>(masked_bytes[8] | masked_bytes[9] | masked_bytes[10] |
                                       masked_bytes[11] | masked_bytes[12] | masked_bytes[13] |
                                       masked_bytes[14] | masked_bytes[15])
            << 8U);
#elif defined(__ARM_NEON) || defined(__ARM_NEON__)
    // Performance improvement #2: static constexpr table avoids re‑initializing
    // the power‑of‑two array on every call.
    static constexpr uint8_t kPowersArr[16] = {1, 2, 4, 8, 16, 32, 64, 128,
                                               1, 2, 4, 8, 16, 32, 64, 128};
    const uint8x16_t group   = vld1q_u8(control);
    const uint8x16_t eq      = vceqq_u8(group, vdupq_n_u8(needle));
    const uint8x16_t powers  = vld1q_u8(kPowersArr);
    const uint8x16_t masked  = vandq_u8(eq, powers);
    const std::uint8_t lo    = vaddv_u8(vget_low_u8(masked));
    const std::uint8_t hi    = vaddv_u8(vget_high_u8(masked));
    return static_cast<std::uint32_t>(lo) | (static_cast<std::uint32_t>(hi) << 8U);
#else
    // Scalar SWAR fallback using multiplication instead of loops.
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
    // Isolate only the high bit of each byte.
    constexpr std::uint64_t kHighBitMask = 0x8080808080808080ULL;
    std::uint64_t bits_lo = m_lo & kHighBitMask;
    std::uint64_t bits_hi = m_hi & kHighBitMask;
    // Multiply by a magic constant that spreads the 8 high bits into the
    // topmost byte. Then shift right by 56 to extract an 8-bit mask.
    constexpr std::uint64_t kGather = 0x8040201008040201ULL;
    std::uint8_t mask_lo = static_cast<std::uint8_t>((bits_lo * kGather) >> 56);
    std::uint8_t mask_hi = static_cast<std::uint8_t>((bits_hi * kGather) >> 56);
    return static_cast<std::uint32_t>(mask_lo) | (static_cast<std::uint32_t>(mask_hi) << 8U);
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
        std::uint32_t tag_matches = group_match_mask(control_.data(), tag);
        // Check for existing key.
        while (tag_matches != 0U) {
            const std::size_t idx = static_cast<std::size_t>(__builtin_ctz(tag_matches));
            if (hashes_[idx] == key) {
                return {idx, false};
            }
            tag_matches &= (tag_matches - 1U);
        }
        // No existing key: insert if space available.
        if (size_ >= kCapacity) {
            return {kCapacity, false};
        }
        const std::uint32_t empty_matches = group_match_mask(control_.data(), kEmpty);
        const std::size_t slot = static_cast<std::size_t>(__builtin_ctz(empty_matches));
        control_[slot]          = tag;
        hashes_[slot]           = key;
        payload_refs_[slot]     = payload_ref;
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
        std::uint32_t tag_matches = group_match_mask(control_.data(), tag);
        while (tag_matches != 0U) {
            const std::size_t idx = static_cast<std::size_t>(__builtin_ctz(tag_matches));
            if (hashes_[idx] == key) {
                return {payload_refs_[idx], false};
            }
            tag_matches &= (tag_matches - 1U);
        }
        const std::int64_t payload_ref = std::forward<PayloadFactory>(payload_factory)();
        if (size_ >= kCapacity) {
            overflow = true;
            return {payload_ref, true};
        }
        const std::uint32_t empty_matches = group_match_mask(control_.data(), kEmpty);
        const std::size_t slot = static_cast<std::size_t>(__builtin_ctz(empty_matches));
        control_[slot]          = tag;
        hashes_[slot]           = key;
        payload_refs_[slot]     = payload_ref;
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
    // Helper that returns both tag-match mask and empty-slot mask
    // from a single load of the control array.
    // Marked always_inline to avoid function-call overhead.
    std::pair<std::uint32_t, std::uint32_t> get_masks(std::uint8_t tag) const noexcept
#if defined(__GNUC__) || defined(__clang__)
        __attribute__((always_inline))
#endif
    {
#if defined(__AVX2__)
        const __m128i ctrl = _mm_load_si128(reinterpret_cast<const __m128i*>(control_.data()));
        const __m128i tag_v = _mm_set1_epi8(static_cast<char>(tag));
        const __m128i empty_v = _mm_set1_epi8(static_cast<char>(kEmpty));
        const uint32_t tag_mask = static_cast<uint32_t>(_mm_movemask_epi8(_mm_cmpeq_epi8(ctrl, tag_v)));
        const uint32_t empty_mask = static_cast<uint32_t>(_mm_movemask_epi8(_mm_cmpeq_epi8(ctrl, empty_v)));
        return {tag_mask, empty_mask};
#elif defined(__riscv) && defined(__riscv_vector)
        return {group_match_mask(control_.data(), tag), group_match_mask(control_.data(), kEmpty)};
#elif defined(__ARM_NEON) || defined(__ARM_NEON__)
        const uint8x16_t ctrl = vld1q_u8(control_.data());
        const uint8x16_t tag_eq = vceqq_u8(ctrl, vdupq_n_u8(tag));
        const uint8x16_t empty_eq = vceqq_u8(ctrl, vdupq_n_u8(kEmpty));
        // Use the same fast mask conversion as group_match_mask.
        auto neon_mask = [](uint8x16_t eq) -> uint32_t {
            static constexpr uint8_t kPowersArr[16] = {1,2,4,8,16,32,64,128,
                                                       1,2,4,8,16,32,64,128};
            const uint8x16_t powers = vld1q_u8(kPowersArr);
            const uint8x16_t masked = vandq_u8(eq, powers);
            const uint8_t lo = vaddv_u8(vget_low_u8(masked));
            const uint8_t hi = vaddv_u8(vget_high_u8(masked));
            return static_cast<uint32_t>(lo) | (static_cast<uint32_t>(hi) << 8U);
        };
        return {neon_mask(tag_eq), neon_mask(empty_eq)};
#else
        // Scalar fallback: load 64-bit halves and compute both masks at once.
        using ::opteryx::carchar::detail::kByteHighBits64;
        using ::opteryx::carchar::detail::load_u64;
        using ::opteryx::carchar::detail::match_mask64;
        const uint64_t lo = load_u64(control_.data());
        const uint64_t hi = load_u64(control_.data() + 8U);
        // Compute tag-match masks.
        uint64_t tag_lo, tag_hi;
        if (tag == kEmpty) {
            tag_lo = lo & kByteHighBits64;
            tag_hi = hi & kByteHighBits64;
        } else {
            tag_lo = match_mask64(lo, tag);
            tag_hi = match_mask64(hi, tag);
        }
        // Compute empty masks (kEmpty is 0x80).
        const uint64_t empty_lo = lo & kByteHighBits64;
        const uint64_t empty_hi = hi & kByteHighBits64;
        // Convert each to 8-bit mask using multiplication.
        constexpr uint64_t kHighBitMask = 0x8080808080808080ULL;
        constexpr uint64_t kGather = 0x8040201008040201ULL;
        auto to_mask = [](uint64_t bits) -> uint8_t {
            return static_cast<uint8_t>(((bits & kHighBitMask) * kGather) >> 56);
        };
        uint8_t tag_mask_lo = to_mask(tag_lo);
        uint8_t tag_mask_hi = to_mask(tag_hi);
        uint8_t empty_mask_lo = to_mask(empty_lo);
        uint8_t empty_mask_hi = to_mask(empty_hi);
        uint32_t tag_mask = static_cast<uint32_t>(tag_mask_lo) | (static_cast<uint32_t>(tag_mask_hi) << 8U);
        uint32_t empty_mask = static_cast<uint32_t>(empty_mask_lo) | (static_cast<uint32_t>(empty_mask_hi) << 8U);
        return {tag_mask, empty_mask};
#endif
    }

    // Performance improvement #4: align control array to 64-byte cache line
    // to avoid false sharing when multiple ParviMaps are used concurrently
    // (e.g., in different threads). The other arrays are kept close together
    // for better cache locality.
    alignas(64) std::array<std::uint8_t, kCapacity>  control_{};
    std::array<std::uint64_t, kCapacity>             hashes_{};
    std::array<std::int64_t, kCapacity>              payload_refs_{};
    std::size_t                                      size_ = 0;
};

// ------------------------------------------------------------------
// ParviSet: like ParviMap but for set membership (no payloads)
// ------------------------------------------------------------------

struct ParviSetResult {
    bool is_new = false;
    bool overflow = false;
};

class ParviSet {
public:
    ParviSet() noexcept {
        control_.fill(kEmpty);
    }

    static constexpr std::size_t capacity() noexcept { return kCapacity; }
    std::size_t size() const noexcept { return size_; }
    bool empty() const noexcept { return size_ == 0; }
    bool full() const noexcept { return size_ == kCapacity; }

    // Fast contains: single SIMD group compare, then key verification.
    bool contains(std::uint64_t key) const noexcept {
        const std::uint8_t tag = key_tag(key);
        std::uint32_t matches = group_match_mask(control_.data(), tag);
        while (matches != 0U) {
            const std::size_t idx = static_cast<std::size_t>(__builtin_ctz(matches));
            if (hashes_[idx] == key) {
                return true;
            }
            matches &= (matches - 1U);
        }
        return false;
    }

    // Insert a new key.
    //   * If the key already exists → returns {is_new: false, overflow: false}.
    //   * If the table is full and key is absent
    //                              → returns {is_new: false, overflow: true}.
    //   * Otherwise                 → inserts and returns {is_new: true, overflow: false}.
    ParviSetResult insert_or_ignore(std::uint64_t key) noexcept {
        const std::uint8_t tag = key_tag(key);
        std::uint32_t tag_matches = group_match_mask(control_.data(), tag);
        // Check for existing key.
        while (tag_matches != 0U) {
            const std::size_t idx = static_cast<std::size_t>(__builtin_ctz(tag_matches));
            if (hashes_[idx] == key) {
                return {false, false};
            }
            tag_matches &= (tag_matches - 1U);
        }
        // No existing key: insert if space available.
        if (size_ >= kCapacity) {
            return {false, true};
        }
        const std::uint32_t empty_matches = group_match_mask(control_.data(), kEmpty);
        const std::size_t slot = static_cast<std::size_t>(__builtin_ctz(empty_matches));
        control_[slot]  = tag;
        hashes_[slot]   = key;
        ++size_;
        return {true, false};
    }

    // Bulk mark-new-indices operation: like CarcharSet::mark_new_indices_32 but
    // with overflow handling. Returns count of newly-inserted entries and writes
    // their indices into out_indices.
    //
    // overflow=true means we encountered an unseen key that could not be inserted
    // because capacity was already exhausted.
    template <typename IndexT>
    std::pair<std::size_t, bool> mark_new_indices(
        const std::uint64_t* keys, IndexT* out_indices, std::size_t length
    ) noexcept {
        std::size_t count = 0;
        for (std::size_t i = 0; i < length; ++i) {
            auto result = insert_or_ignore(keys[i]);
            if (result.is_new) {
                out_indices[count++] = static_cast<IndexT>(i);
                continue;
            }
            // Early exit only on true overflow (unseen key when full).
            if (result.overflow) {
                return {count, true};
            }
        }
        return {count, false};
    }

    // Copy live entries into a CarcharSet for promotion.
    void drain_into(::opteryx::carchar::CarcharSet& target) const {
        target.reserve(size_);
        for (std::size_t i = 0; i < kCapacity; ++i) {
            if (control_[i] != kEmpty) {
                target.insert_or_ignore(hashes_[i]);
            }
        }
    }

    void clear() noexcept {
        control_.fill(kEmpty);
        size_ = 0;
    }

private:
    // Same helper as ParviMap: compute tag-match and empty-slot masks in one pass.
    std::pair<std::uint32_t, std::uint32_t> get_masks(std::uint8_t tag) const noexcept
#if defined(__GNUC__) || defined(__clang__)
        __attribute__((always_inline))
#endif
    {
#if defined(__AVX2__)
        const __m128i ctrl = _mm_load_si128(reinterpret_cast<const __m128i*>(control_.data()));
        const __m128i tag_v = _mm_set1_epi8(static_cast<char>(tag));
        const __m128i empty_v = _mm_set1_epi8(static_cast<char>(kEmpty));
        const uint32_t tag_mask = static_cast<uint32_t>(_mm_movemask_epi8(_mm_cmpeq_epi8(ctrl, tag_v)));
        const uint32_t empty_mask = static_cast<uint32_t>(_mm_movemask_epi8(_mm_cmpeq_epi8(ctrl, empty_v)));
        return {tag_mask, empty_mask};
#elif defined(__riscv) && defined(__riscv_vector)
        return {group_match_mask(control_.data(), tag), group_match_mask(control_.data(), kEmpty)};
#elif defined(__ARM_NEON) || defined(__ARM_NEON__)
        const uint8x16_t ctrl = vld1q_u8(control_.data());
        const uint8x16_t tag_eq = vceqq_u8(ctrl, vdupq_n_u8(tag));
        const uint8x16_t empty_eq = vceqq_u8(ctrl, vdupq_n_u8(kEmpty));
        auto neon_mask = [](uint8x16_t eq) -> uint32_t {
            static constexpr uint8_t kPowersArr[16] = {1,2,4,8,16,32,64,128,
                                                       1,2,4,8,16,32,64,128};
            const uint8x16_t powers = vld1q_u8(kPowersArr);
            const uint8x16_t masked = vandq_u8(eq, powers);
            const uint8_t lo = vaddv_u8(vget_low_u8(masked));
            const uint8_t hi = vaddv_u8(vget_high_u8(masked));
            return static_cast<uint32_t>(lo) | (static_cast<uint32_t>(hi) << 8U);
        };
        return {neon_mask(tag_eq), neon_mask(empty_eq)};
#else
        using ::opteryx::carchar::detail::kByteHighBits64;
        using ::opteryx::carchar::detail::load_u64;
        using ::opteryx::carchar::detail::match_mask64;
        const uint64_t lo = load_u64(control_.data());
        const uint64_t hi = load_u64(control_.data() + 8U);
        uint64_t tag_lo, tag_hi;
        if (tag == kEmpty) {
            tag_lo = lo & kByteHighBits64;
            tag_hi = hi & kByteHighBits64;
        } else {
            tag_lo = match_mask64(lo, tag);
            tag_hi = match_mask64(hi, tag);
        }
        const uint64_t empty_lo = lo & kByteHighBits64;
        const uint64_t empty_hi = hi & kByteHighBits64;
        constexpr uint64_t kHighBitMask = 0x8080808080808080ULL;
        constexpr uint64_t kGather = 0x8040201008040201ULL;
        auto to_mask = [](uint64_t bits) -> uint8_t {
            return static_cast<uint8_t>(((bits & kHighBitMask) * kGather) >> 56);
        };
        uint8_t tag_mask_lo = to_mask(tag_lo);
        uint8_t tag_mask_hi = to_mask(tag_hi);
        uint8_t empty_mask_lo = to_mask(empty_lo);
        uint8_t empty_mask_hi = to_mask(empty_hi);
        uint32_t tag_mask = static_cast<uint32_t>(tag_mask_lo) | (static_cast<uint32_t>(tag_mask_hi) << 8U);
        uint32_t empty_mask = static_cast<uint32_t>(empty_mask_lo) | (static_cast<uint32_t>(empty_mask_hi) << 8U);
        return {tag_mask, empty_mask};
#endif
    }

    alignas(64) std::array<std::uint8_t, kCapacity>  control_{};
    std::array<std::uint64_t, kCapacity>             hashes_{};
    std::size_t                                      size_ = 0;
};

}  // namespace opteryx::parvi
